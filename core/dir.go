// Copyright 2015 - 2017 Ka-Hing Cheung
// Copyright 2021 Yandex LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jacobsa/fuse/fuseops"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

type SlurpGap struct {
	// Gap is (start < key <= end)
	start, end string
	loadTime   time.Time
}

type DirInodeData struct {
	cloud       StorageBackend
	mountPrefix string

	// lastOpenDirIdx refers to readdir of the Children
	lastOpenDirIdx  int
	seqOpenDirScore uint8
	DirTime         time.Time
	ImplicitDir     bool

	listMarker       string
	lastFromCloud    *string
	listDone         bool
	forgetDuringList bool
	// Time at which we started fetching child entries
	// from cloud for this handle.
	refreshStartTime time.Time

	ModifiedChildren int64

	Children        []*Inode
	DeletedChildren map[string]*Inode
	Gaps            []*SlurpGap
	handles         []*DirHandle
}

// Returns the position of first char < '/' in `inp` after prefixLen + any continued '/' characters.
// This should work for unicode also: unicode chars are all greater than 128.
// See TestHasCharLtSlash for examples.
func locateLtSlash(inp string, prefixLen int) int {
	i := prefixLen
	for i < len(inp) && inp[i] == '/' {
		i++
	}
	// first character < / doesn't matter, because directory names can't start with /
	i++
	for ; i < len(inp); i++ {
		if inp[i] < '/' {
			return i
		}
	}
	return -1
}

type DirHandle struct {
	inode *Inode
	mu    sync.Mutex // everything below is protected by mu
	// readdir() is allowed either at zero (restart from the beginning)
	// or from the previous offset
	lastExternalOffset fuseops.DirOffset
	lastInternalOffset int
	lastName           string
}

func NewDirHandle(inode *Inode) (dh *DirHandle) {
	dh = &DirHandle{inode: inode}
	return
}

func (inode *Inode) OpenDir() (dh *DirHandle) {
	var isS3 bool

	parent := inode.Parent
	cloud, _ := inode.cloud()

	// in test we sometimes set cloud to nil to ensure we are not
	// talking to the cloud
	if cloud != nil {
		_, isS3 = cloud.Delegate().(*S3Backend)
	}

	dir := inode.dir
	if dir == nil {
		panic(fmt.Sprintf("%v is not a directory", inode.FullName()))
	}

	if isS3 && parent != nil && inode.fs.flags.StatCacheTTL != 0 {
		parent.mu.Lock()
		defer parent.mu.Unlock()

		numChildren := len(parent.dir.Children)
		dirIdx := -1
		seqMode := -1
		firstDir := false

		if parent.dir.lastOpenDirIdx < 0 {
			// check if we are opening the first child
			// cap the search to 1000
			// peers to bound the time. If the next dir is
			// more than 1000 away, slurping isn't going
			// to be helpful anyway
			for i := 0; i < MinInt(numChildren, 1000); i++ {
				c := parent.dir.Children[i]
				if c.isDir() {
					if c.Name == inode.Name {
						dirIdx = i
						seqMode = 1
						firstDir = true
					}
					break
				}
			}
		} else if parent.dir.lastOpenDirIdx < numChildren &&
			parent.dir.Children[parent.dir.lastOpenDirIdx].isDir() &&
			parent.dir.Children[parent.dir.lastOpenDirIdx].Name == inode.Name {
			// allow to read the last directory again, don't reset, but don't bump seqOpenDirScore too
			seqMode = 0
		} else {
			// check if we are reading the next one as expected
			for i := parent.dir.lastOpenDirIdx + 1; i < MinInt(numChildren, parent.dir.lastOpenDirIdx+1000); i++ {
				c := parent.dir.Children[i]
				if c.isDir() {
					if c.Name == inode.Name {
						dirIdx = i
						seqMode = 1
					}
					break
				}
			}
		}

		if seqMode == 0 {
			// same directory again
		} else if seqMode == 1 {
			if parent.dir.seqOpenDirScore < 255 {
				parent.dir.seqOpenDirScore++
			}
			if parent.dir.seqOpenDirScore == 2 {
				fuseLog.Debugf("%v in readdir mode", parent.FullName())
			}
			parent.dir.lastOpenDirIdx = dirIdx
			if firstDir {
				// 1) if I open a/, root's score = 1
				// (a is the first dir), so make a/'s
				// count at 1 too this allows us to
				// propagate down the score for
				// depth-first search case
				wasSeqMode := dir.seqOpenDirScore >= 2
				dir.seqOpenDirScore = parent.dir.seqOpenDirScore
				if !wasSeqMode && dir.seqOpenDirScore >= 2 {
					fuseLog.Debugf("%v in readdir mode", inode.FullName())
				}
			}
		} else {
			parent.dir.seqOpenDirScore = 0
			if dirIdx == -1 {
				dirIdx = parent.findChildIdxUnlocked(inode.Name)
			}
			parent.dir.lastOpenDirIdx = dirIdx
		}
	}

	dh = NewDirHandle(inode)
	inode.mu.Lock()
	inode.dir.handles = append(inode.dir.handles, dh)
	n := atomic.AddInt32(&inode.fileHandles, 1)
	if n == 1 {
		inode.Parent.addModified(1)
	}
	inode.mu.Unlock()
	return
}

// Slurp is always done:
// - at the uppermost possible level (usually root if not using the nested mount perversion)
// - at some directory boundary
// - only at the beginning of readdir()
// Slurp can seal some directories - namely, it's safe to seal either the requested directory
// or directories that are not a parent of the requested one.
// I.e. if we're preloading at 00/05/06/01/ then it's safe to seal 00/05/06/01/, 01/*, 00/06/*,
// but not 00/ itself, because it's likely that the slurp wasn't started at the beginning of 00/.
// Slurp can be used multiple times by passing returned nextStartAfter as an argument the next time.
func (inode *Inode) slurpOnce(lock bool) (done bool, err error) {
	parent := inode
	for parent != nil && parent.dir.cloud == nil {
		parent = parent.Parent
	}
	next, err := parent.listObjectsSlurp(inode, "", true, lock)
	return next == "", err
}

func isInvalidName(name string) bool {
	return name == "" || name[0] == '/' ||
		len(name) >= 2 && (name[0:2] == "./" || name[len(name)-2:] == "/.") ||
		len(name) >= 3 && (name[0:3] == "../" || name[len(name)-3:] == "/..") ||
		strings.Index(name, "//") >= 0 ||
		strings.Index(name, "/./") >= 0 ||
		strings.Index(name, "/../") >= 0
}

func RetryListBlobs(flags *cfg.FlagStorage, cloud StorageBackend, req *ListBlobsInput) (resp *ListBlobsOutput, err error) {
	ReadBackoff(flags, func(attempt int) error {
		resp, err = cloud.ListBlobs(req)
		if err != nil && shouldRetry(err) {
			s3Log.Warnf("Error listing objects with prefix=%v delimiter=%v start-after=%v max-keys=%v (attempt %v): %v\n",
				NilStr(req.Prefix), NilStr(req.Delimiter), NilStr(req.StartAfter), NilUInt32(req.MaxKeys), attempt, err)
		}
		return err
	})
	return
}

func (parent *Inode) listObjectsSlurp(inode *Inode, startAfter string, sealEnd bool, lock bool) (nextStartAfter string, err error) {
	// Prefix is for insertSubTree
	cloud, prefix := parent.cloud()
	if prefix != "" {
		prefix += "/"
	}

	_, key := inode.cloud()
	var startWith *string
	if startAfter != "" {
		startWith = &startAfter
	} else if key != "" {
		startWith = PString(key + "/")
	}

	myList := parent.fs.addInflightListing()

	params := &ListBlobsInput{
		Prefix:     &prefix,
		StartAfter: startWith,
	}
	resp, err := RetryListBlobs(parent.fs.flags, cloud, params)
	if err != nil {
		parent.fs.completeInflightListing(myList)
		return
	}
	s3Log.Debug(resp)

	if lock {
		parent.mu.Lock()
	}
	skipListing := parent.fs.completeInflightListing(myList)
	dirs := make(map[*Inode]bool)
	for _, obj := range resp.Items {
		if skipListing != nil && skipListing[*obj.Key] {
			continue
		}
		baseName := (*obj.Key)[len(prefix):]
		if !isInvalidName(baseName) {
			parent.insertSubTree(baseName, &obj, dirs)
		}
	}

	for d, sealed := range dirs {
		// It's not safe to seal upper directories, we're not slurping at their start
		if (sealed || !resp.IsTruncated) && !d.isParentOf(inode) {
			if d != parent {
				d.mu.Lock()
			}
			d.sealDir()
			if d != parent {
				d.mu.Unlock()
			}
		}
	}

	var obj *BlobItemOutput
	if len(resp.Items) > 0 {
		obj = &resp.Items[len(resp.Items)-1]
	}
	seal := false
	if sealEnd {
		// if we are done listing prefix, we are good
		if prefix != "" && obj != nil && !strings.HasPrefix(*obj.Key, prefix) {
			if *obj.Key > prefix {
				seal = true
			}
		} else if !resp.IsTruncated {
			seal = true
		}
	}

	if seal {
		if inode != parent {
			inode.mu.Lock()
		}
		inode.sealDir()
		if inode != parent {
			inode.mu.Unlock()
		}
		nextStartAfter = ""
	} else if obj != nil {
		// NextContinuationToken is not returned when delimiter is empty, so use obj.Key
		nextStartAfter = *obj.Key
	}

	// Remember this range as already loaded
	parent.dir.markGapLoaded(NilStr(startWith), nextStartAfter)

	if lock {
		parent.mu.Unlock()
	}

	return
}

func (dir *DirInodeData) markGapLoaded(start, end string) {
	pos := 0
	if start != "" {
		pos = sort.Search(len(dir.Gaps), func(i int) bool {
			return dir.Gaps[i].start >= start
		})
	}
	for pos > 0 && dir.Gaps[pos-1].end > start {
		pos--
	}
	endPos := sort.Search(len(dir.Gaps), func(i int) bool {
		return dir.Gaps[i].start >= end
	})
	if pos < len(dir.Gaps) && dir.Gaps[pos].start < start {
		dir.Gaps[pos].end = start
		pos++
	}
	if endPos > 0 && dir.Gaps[endPos-1].end > end {
		dir.Gaps[endPos-1].start = end
		endPos--
	}
	l := len(dir.Gaps) - (endPos - pos)
	if pos == endPos {
		dir.Gaps = append(dir.Gaps, nil)
	}
	copy(dir.Gaps[pos+1:], dir.Gaps[endPos:])
	dir.Gaps[pos] = &SlurpGap{
		start:    start,
		end:      end,
		loadTime: time.Now(),
	}
	dir.Gaps = dir.Gaps[0:l]
}

// LOCKS_REQUIRED(inode.mu)
func (dir *DirInodeData) checkGapLoaded(key string, newerThan time.Time) bool {
	pos := sort.Search(len(dir.Gaps), func(i int) bool {
		return dir.Gaps[i].end >= key
	})
	if pos < len(dir.Gaps) && dir.Gaps[pos].start < key {
		if dir.Gaps[pos].loadTime.After(newerThan) {
			return true
		} else {
			copy(dir.Gaps[pos:], dir.Gaps[pos+1:])
			dir.Gaps = dir.Gaps[0 : len(dir.Gaps)-1]
		}
	}
	return false
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) sealDir() {
	inode.dir.listMarker = ""
	inode.dir.listDone = true
	inode.dir.lastFromCloud = nil
	inode.dir.DirTime = time.Now()
	if inode.fs.flags.EnableMtime && inode.userMetadata != nil &&
		inode.userMetadata[inode.fs.flags.MtimeAttr] != nil {
		_, inode.Attributes.Ctime = inode.findChildMaxTime()
	} else {
		inode.Attributes.Mtime, inode.Attributes.Ctime = inode.findChildMaxTime()
	}
	inode.removeExpired("")
}

// LOCKS_REQUIRED(dh.inode.mu)
// LOCKS_EXCLUDED(dh.inode.fs.mu)
func (dh *DirHandle) handleListResult(resp *ListBlobsOutput, prefix string, skipListing map[string]bool) {
	parent := dh.inode
	fs := parent.fs

	for _, dir := range resp.Prefixes {
		if skipListing != nil && skipListing[*dir.Prefix] {
			continue
		}
		// strip trailing /
		dirName := (*dir.Prefix)[0 : len(*dir.Prefix)-1]
		// strip previous prefix
		dirName = dirName[len(prefix):]
		if isInvalidName(dirName) {
			continue
		}

		if inode := parent.findChildUnlocked(dirName); inode != nil {
			now := time.Now()
			// don't want to update time if this
			// inode is setup to never expire
			if inode.AttrTime.Before(now) {
				inode.SetAttrTime(now)
			}
		} else if _, deleted := parent.dir.DeletedChildren[dirName]; !deleted {
			// don't revive deleted items
			inode := NewInode(fs, parent, dirName)
			inode.ToDir()
			fs.insertInode(parent, inode)
		}

		if dh.inode.dir.lastFromCloud == nil ||
			strings.Compare(*dh.inode.dir.lastFromCloud, dirName) < 0 {
			dh.inode.dir.lastFromCloud = &dirName
		}
	}

	for _, obj := range resp.Items {
		if skipListing != nil && skipListing[*obj.Key] {
			continue
		}
		baseName := (*obj.Key)[len(prefix):]
		if isInvalidName(baseName) {
			continue
		}

		slash := strings.Index(baseName, "/")
		if slash == -1 {
			inode := parent.findChildUnlocked(baseName)
			if inode != nil {
				inode.SetFromBlobItem(&obj)
			} else {
				// don't revive deleted items
				_, deleted := parent.dir.DeletedChildren[baseName]
				if !deleted {
					inode = NewInode(fs, parent, baseName)
					fs.insertInode(parent, inode)
					inode.SetFromBlobItem(&obj)
				}
			}
		} else {
			// this is a slurped up object which
			// was already cached
			baseName = baseName[:slash]
		}

		if dh.inode.dir.lastFromCloud == nil ||
			strings.Compare(*dh.inode.dir.lastFromCloud, baseName) < 0 {
			dh.inode.dir.lastFromCloud = &baseName
		}
	}
}

func maxName(resp *ListBlobsOutput, itemPos, prefixPos int) (string, int) {
	if itemPos > 0 && (prefixPos == 0 || *resp.Prefixes[prefixPos-1].Prefix < *resp.Items[itemPos-1].Key) {
		return *resp.Items[itemPos-1].Key, 0
	}
	if prefixPos == 0 {
		return "", 0
	}
	return *resp.Prefixes[prefixPos-1].Prefix, 1
}

func prefixLarger(s1, s2 string, pos int) bool {
	if len(s2) > pos && s1[0:pos] < s2[0:pos] || len(s2) <= pos && s1[0:pos] < s2 {
		return true
	} else if len(s2) >= pos+1 && s1[0:pos] == s2[0:pos] && s2[pos] > '/' {
		return true
	}
	return false
}

// In s3 & azblob, prefixes are returned with '/' => the prefix "2019" is
// returned as "2019/". So the list api for these backends returns "2019/" after
// "2019-0001/" because ascii("/") > ascii("-"). This is problematic for us if
// "2019/" is returned in x+1'th batch and "2019-0001/" is returned in x'th
// because GeeseFS returns results to the client in the sorted order.
//
// To overcome this issue, previously we continued to next listing pages
// until the last item of the listing didn't contain characters less than '/'
// anymore. But it was bad because '.' also precedes '/' and this made GeeseFS
// fully load almost all directories, because most directories contain files
// with extensions.
//
// So now we intelligently cut the result array so that the last item
// either doesn't contain characters before '/' or is followed by another item
// with a larger prefix (i.e. 'abc-def', then 'abd-' or 'abca-'). And if it's
// impossible, i.e. if the whole listing contains items with the same prefix,
// we issues an additional HEAD request and check for the presence of an item
// with slash ('abc-def' is in listing, then we check 'abc/').
//
// Relevant test case: TestReadDirDash
func intelligentListCut(resp *ListBlobsOutput, flags *cfg.FlagStorage, cloud StorageBackend, prefix string) (lastName string, err error) {
	if !resp.IsTruncated {
		return
	}
	isPrefix := 0
	itemPos, prefixPos := len(resp.Items), len(resp.Prefixes)
	count := itemPos + prefixPos
	lastName, isPrefix = maxName(resp, itemPos, prefixPos)
	lastLtPos := locateLtSlash(lastName, len(prefix))
	if lastLtPos >= 0 {
		prev, name := "", lastName
		ok := false
		for itemPos+prefixPos > count/2 {
			prefixPos -= isPrefix
			itemPos -= (1 - isPrefix)
			prev, isPrefix = maxName(resp, itemPos, prefixPos)
			ltPos := locateLtSlash(prev, len(prefix))
			if ltPos < 0 || prefixLarger(prev, name, ltPos) {
				// No characters less than '/' => OK, stop
				ok = true
				break
			}
			name = prev
		}
		if ok {
			resp.Items = resp.Items[0:itemPos]
			resp.Prefixes = resp.Prefixes[0:prefixPos]
			lastName = prev
		} else {
			// Can't intelligently cut the list as more than 50% of it has the same prefix
			// So, check for existence of the offending directory separately
			// Simulate '>=' operator with start-after
			// '.' = '/'-1
			// \xF4\x8F\xBF\xBF = 0x10FFFF in UTF-8 = largest code point of 3-byte UTF-8
			// \xEF\xBF\xBD = 0xFFFD in UTF-8 = largest valid symbol of 2-byte UTF-8
			// So, > xxx.\xEF\xBF\xBF is the same as >= xxx/
			dirobj, err := RetryListBlobs(flags, cloud, &ListBlobsInput{
				StartAfter: PString(lastName[0:lastLtPos] + ".\xEF\xBF\xBD"),
				MaxKeys:    PUInt32(1),
			})
			if err != nil {
				return "", err
			}
			if len(dirobj.Items) > 0 {
				checkedName := *dirobj.Items[0].Key
				if len(checkedName) >= lastLtPos+1 && checkedName[0:lastLtPos+1] == lastName[0:lastLtPos]+"/" {
					resp.Prefixes = append(resp.Prefixes, BlobPrefixOutput{Prefix: PString(checkedName[0 : lastLtPos+1])})
				}
			}
		}
	}
	return
}

func (dh *DirHandle) listObjectsFlat() (start string, err error) {
	dh.inode.mu.Lock()
	cloud, prefix := dh.inode.cloud()
	if cloud == nil {
		// Stale inode
		dh.inode.mu.Unlock()
		return "", syscall.ESTALE
	}
	if dh.inode.oldParent != nil {
		_, prefix = dh.inode.oldParent.cloud()
		prefix = appendChildName(prefix, dh.inode.oldName)
	}
	if len(prefix) != 0 {
		prefix += "/"
	}
	if len(dh.inode.dir.listMarker) >= len(prefix) && dh.inode.dir.listMarker[0:len(prefix)] == prefix {
		start = dh.inode.dir.listMarker[len(prefix):]
	}
	params := &ListBlobsInput{
		Delimiter:  PString("/"),
		StartAfter: PString(dh.inode.dir.listMarker),
		Prefix:     &prefix,
	}
	dh.inode.mu.Unlock()

	myList := dh.inode.fs.addInflightListing()

	dh.mu.Unlock()
	resp, err := RetryListBlobs(dh.inode.fs.flags, cloud, params)
	dh.mu.Lock()

	if err != nil {
		dh.inode.fs.completeInflightListing(myList)
		return
	}

	s3Log.Debug(resp)

	// See comment to intelligentListCut above
	lastName, err := intelligentListCut(resp, dh.inode.fs.flags, cloud, prefix)
	if err != nil {
		dh.inode.fs.completeInflightListing(myList)
		return
	}

	dh.inode.mu.Lock()
	dh.handleListResult(resp, prefix, dh.inode.fs.completeInflightListing(myList))

	if resp.IsTruncated {
		// :-X for history: aws-sdk with its idiotic string pointers was leading
		// to a huge memory leak when we were saving NextContinuationToken as is
		if dh.inode.dir.listMarker == "" || dh.inode.dir.listMarker < lastName {
			dh.inode.dir.listMarker = lastName
		}
	} else {
		dh.inode.sealDir()
	}

	dh.inode.mu.Unlock()

	return
}

// LOCKS_REQUIRED(dh.mu)
// LOCKS_REQUIRED(dh.inode.mu)
func (dh *DirHandle) checkDirPosition() {
	if dh.lastInternalOffset < 0 {
		parent := dh.inode
		// Directory position invalidated, try to find it again using lastName
		if dh.lastName == "." {
			dh.lastInternalOffset = 1
		} else if dh.lastName == ".." {
			dh.lastInternalOffset = 2
		} else {
			dh.lastInternalOffset = sort.Search(len(parent.dir.Children), parent.findInodeFunc(dh.lastName))
			if dh.lastInternalOffset < len(parent.dir.Children) && parent.dir.Children[dh.lastInternalOffset].Name == dh.lastName {
				dh.lastInternalOffset++
			}
			dh.lastInternalOffset += 2
		}
	}
}

// LOCKS_REQUIRED(dh.mu)
// LOCKS_REQUIRED(dh.inode.mu)
// LOCKS_EXCLUDED(dh.inode.fs)
func (dh *DirHandle) loadListing() error {
	parent := dh.inode

	if !parent.dir.listDone && parent.dir.listMarker == "" {
		// listMarker is nil => We just started refreshing this directory
		parent.dir.listDone = false
		parent.dir.lastFromCloud = nil
		parent.dir.refreshStartTime = time.Now()
		parent.dir.Gaps = nil
		parent.dir.forgetDuringList = false
	}

	// We don't want to wait for the whole slurp to finish when we just do 'ls ./dir/subdir'
	// because subdir may be very large. So we only use slurp at the beginning of the directory.
	// It will fill and seal some adjacent directories if they're small and they'll be served from cache.
	// However, if a single slurp isn't enough to serve sufficient amount of directory entries,
	// we immediately switch to regular listings.
	// Original implementation in Goofys in fact was similar in this aspect
	// but it was ugly in several places, so ... sorry, it's reworked. O:-)
	useSlurp := parent.dir.listMarker == "" && parent.fs.flags.StatCacheTTL != 0

	// the dir expired, so we need to fetch from the cloud. there
	// may be static directories that we want to keep, so cloud
	// listing should not overwrite them. here's what we do:
	//
	// 1. list from cloud and add them all to the tree, remember
	//    which one we added last
	//
	// 2. serve from cache
	//
	// 3. when we serve the entry we added last, signal that next
	//    time we need to list from cloud again with continuation
	//    token

	if useSlurp {
		parent.mu.Unlock()
		dh.mu.Unlock()
		done, err := parent.slurpOnce(true)
		dh.mu.Lock()
		parent.mu.Lock()
		if err != nil {
			return err
		}
		if done && !parent.dir.listDone {
			// Usually subdirs are sealed by slurp
			// However, it is possible that sometimes they're not
			// For example, in case of a nested mount...
			parent.sealDir()
		}
	}

	loaded, startMarker := false, ""
	for parent.dir.lastFromCloud == nil && !parent.dir.listDone {
		parent.mu.Unlock()
		start, err := dh.listObjectsFlat()
		if !loaded {
			loaded, startMarker = true, start
		}
		parent.mu.Lock()
		if err != nil {
			return err
		}
	}

	if loaded || parent.dir.listDone {
		parent.removeExpired(startMarker)
	}

	return nil
}

// LOCKS_REQUIRED(dh.mu)
func (dh *DirHandle) Seek(newOffset fuseops.DirOffset) {
	if newOffset != 0 && newOffset != dh.lastExternalOffset {
		// Do our best to support directory seeks even though we can't guarantee
		// consistent listings in this case (i.e. files may be duplicated or skipped on changes)
		// 'Normal' software doesn't seek within the directory.
		// nfs-kernel-server, though, does: it closes the dir between paged listing calls.
		fuseLog.Debugf("Directory seek from %v to %v in %v", newOffset, dh.lastExternalOffset, dh.inode.FullName())
		dh.inode.mu.Lock()
		dh.lastExternalOffset = newOffset
		dh.lastInternalOffset = int(newOffset)
		if dh.lastInternalOffset > 2+len(dh.inode.dir.Children) {
			dh.lastInternalOffset = 2 + len(dh.inode.dir.Children)
		}
		if dh.lastInternalOffset == 1 {
			dh.lastName = "."
		} else if dh.lastInternalOffset == 2 {
			dh.lastName = ".."
		} else {
			dh.inode.dir.Children[dh.lastInternalOffset-3].mu.Lock()
			dh.lastName = dh.inode.dir.Children[dh.lastInternalOffset-3].Name
			dh.inode.dir.Children[dh.lastInternalOffset-3].mu.Unlock()
		}
		dh.inode.mu.Unlock()
	} else if newOffset == 0 {
		dh.lastExternalOffset = 0
		dh.lastInternalOffset = 0
		dh.lastName = ""
	}
}

// LOCKS_REQUIRED(dh.mu)
func (dh *DirHandle) Next(name string) {
	if dh.lastInternalOffset >= 0 {
		dh.lastInternalOffset++
	}
	dh.lastExternalOffset++
	dh.lastName = name
}

func (parent *Inode) removeExpired(from string) {
	// Skip stale inodes
	var notifications []interface{}
	var pos int
	if from != "" && from != "." && from != ".." {
		pos = sort.Search(len(parent.dir.Children), parent.findInodeFunc(from))
	}
	for i := pos; i < len(parent.dir.Children); i++ {
		// Note on locking: See comments at Inode::AttrTime, Inode::Parent.
		childTmp := parent.dir.Children[i]
		if parent.dir.lastFromCloud != nil && childTmp.Name >= *parent.dir.lastFromCloud {
			break
		}
		if childTmp.AttrTime.Before(parent.dir.refreshStartTime) &&
			atomic.LoadInt32(&childTmp.fileHandles) == 0 &&
			atomic.LoadInt32(&childTmp.CacheState) <= ST_DEAD &&
			(!childTmp.isDir() || atomic.LoadInt64(&childTmp.dir.ModifiedChildren) == 0) {
			childTmp.mu.Lock()
			childTmp.resetCache()
			childTmp.SetCacheState(ST_DEAD)
			notifications = append(notifications, &fuseops.NotifyDelete{
				Parent: parent.Id,
				Child:  childTmp.Id,
				Name:   childTmp.Name,
			})
			if childTmp.isDir() {
				childTmp.removeAllChildrenUnlocked()
			}
			parent.removeChildUnlocked(childTmp)
			childTmp.mu.Unlock()
			i--
		}
	}
	if len(notifications) > 0 && parent.fs.NotifyCallback != nil {
		parent.fs.NotifyCallback(notifications)
	}
}

// LOCKS_REQUIRED(dh.mu)
// LOCKS_EXCLUDED(dh.inode.mu)
// LOCKS_EXCLUDED(dh.inode.fs)
func (dh *DirHandle) ReadDir() (inode *Inode, err error) {
	parent := dh.inode
	if parent.dir == nil {
		panic("ReadDir non-directory " + parent.FullName())
	}
	parent.mu.Lock()
	defer parent.mu.Unlock()

	dh.checkDirPosition()
	if dh.lastInternalOffset == 0 {
		// "."
		return parent, nil
	} else if dh.lastInternalOffset == 1 {
		// ".."
		if parent.Parent != nil {
			return parent.Parent, nil
		} else {
			return parent, nil
		}
	}

	if expired(dh.inode.dir.DirTime, dh.inode.fs.flags.StatCacheTTL) {
		err = dh.loadListing()
		if err != nil {
			return nil, err
		}
		// May be -1 if we remove inodes in loadListing
		dh.checkDirPosition()
	}

	if dh.lastInternalOffset-2 >= len(dh.inode.dir.Children) {
		// we've reached the end
		parent.dir.listDone = false
		if parent.dir.forgetDuringList {
			parent.dir.DirTime = time.Time{}
			parent.dir.Gaps = nil
		}
		return
	}

	child := dh.inode.dir.Children[dh.lastInternalOffset-2]
	if dh.inode.dir.lastFromCloud != nil && child.Name == *dh.inode.dir.lastFromCloud {
		dh.inode.dir.lastFromCloud = nil
	}

	return child, nil
}

func (dh *DirHandle) CloseDir() error {
	dh.inode.mu.Lock()
	i := 0
	for ; i < len(dh.inode.dir.handles) && dh.inode.dir.handles[i] != dh; i++ {
	}
	if i < len(dh.inode.dir.handles) {
		dh.inode.dir.handles = append(dh.inode.dir.handles[0:i], dh.inode.dir.handles[i+1:]...)
		n := atomic.AddInt32(&dh.inode.fileHandles, -1)
		if n == 0 {
			dh.inode.Parent.addModified(-1)
		}
	}
	dh.inode.mu.Unlock()
	return nil
}

// Recursively resets the DirTime for child directories.
// ACQUIRES_LOCK(inode.mu)
func (inode *Inode) resetDirTimeRec() {
	inode.mu.Lock()
	inode.SetAttrTime(time.Time{})
	if inode.dir == nil {
		inode.mu.Unlock()
		return
	}
	inode.dir.listDone = false
	inode.dir.DirTime = time.Time{}
	// Make a copy of the child nodes before giving up the lock.
	// This protects us from any addition/removal of child nodes
	// under this node.
	children := make([]*Inode, len(inode.dir.Children))
	copy(children, inode.dir.Children)
	inode.mu.Unlock()
	for _, child := range children {
		child.resetDirTimeRec()
	}
}

// ResetForUnmount resets the Inode as part of unmounting a storage backend
// mounted at the given inode.
// ACQUIRES_LOCK(inode.mu)
func (inode *Inode) ResetForUnmount() {
	if inode.dir == nil {
		panic(fmt.Sprintf("ResetForUnmount called on a non-directory. name:%v",
			inode.Name))
	}

	inode.mu.Lock()
	// First reset the cloud info for this directory. After that, any read and
	// write operations under this directory will not know about this cloud.
	inode.dir.cloud = nil
	inode.dir.mountPrefix = ""

	// Clear metadata.
	// Set the metadata values to nil instead of deleting them so that
	// we know to fetch them again next time instead of thinking there's
	// no metadata
	inode.userMetadata = nil
	inode.s3Metadata = nil
	inode.Attributes = InodeAttributes{}
	inode.dir.ImplicitDir = false
	inode.mu.Unlock()
	// Reset DirTime for recursively for this node and all its child nodes.
	// Note: resetDirTimeRec should be called without holding the lock.
	inode.resetDirTimeRec()

}

func (parent *Inode) findPath(path string) (inode *Inode) {
	dir := parent

	for dir != nil {
		if !dir.isDir() {
			return nil
		}

		idx := strings.Index(path, "/")
		if idx == -1 {
			return dir.findChild(path)
		}
		dirName := path[0:idx]
		path = path[idx+1:]

		dir = dir.findChild(dirName)
	}

	return nil
}

func (parent *Inode) findChild(name string) (inode *Inode) {
	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode = parent.findChildUnlocked(name)
	return
}

func (parent *Inode) findInodeFunc(name string) func(i int) bool {
	if name == "" {
		return func(i int) bool {
			return true
		}
	}
	return func(i int) bool {
		return parent.dir.Children[i].Name >= name
	}
}

func (parent *Inode) findChildUnlocked(name string) (inode *Inode) {
	l := len(parent.dir.Children)
	if l == 0 {
		return
	}
	i := sort.Search(l, parent.findInodeFunc(name))
	if i < l {
		// found
		if parent.dir.Children[i].Name == name {
			inode = parent.dir.Children[i]
		}
	}
	return
}

func (parent *Inode) findChildIdxUnlocked(name string) int {
	l := len(parent.dir.Children)
	if l == 0 {
		return -1
	}
	i := sort.Search(l, parent.findInodeFunc(name))
	if i < l && parent.dir.Children[i].Name == name {
		return i
	}
	return -1
}

// LOCKS_REQUIRED(parent.mu)
// LOCKS_REQUIRED(inode.mu)
// LOCKS_EXCLUDED(parent.fs.mu)
func (parent *Inode) removeChildUnlocked(inode *Inode) {
	l := len(parent.dir.Children)
	if l == 0 {
		return
	}
	i := sort.Search(l, parent.findInodeFunc(inode.Name))
	if i >= l || parent.dir.Children[i].Name != inode.Name {
		panic(fmt.Sprintf("%v.removeName(%v) but child not found: %v",
			parent.FullName(), inode.Name, i))
	}

	// POSIX allows parallel readdir() and modifications,
	// so preserve position of all directory handles
	for _, dh := range parent.dir.handles {
		dh.lastInternalOffset = -1
	}
	// >= because we use the "last open dir" as the "next" one
	if parent.dir.lastOpenDirIdx >= i {
		parent.dir.lastOpenDirIdx--
	}
	copy(parent.dir.Children[i:], parent.dir.Children[i+1:])
	parent.dir.Children[l-1] = nil
	parent.dir.Children = parent.dir.Children[:l-1]

	if cap(parent.dir.Children) >= len(parent.dir.Children)*2 {
		tmp := make([]*Inode, len(parent.dir.Children))
		copy(tmp, parent.dir.Children)
		parent.dir.Children = tmp
	}

	inode.DeRef(1)
}

// LOCKS_REQUIRED(parent.mu)
// LOCKS_EXCLUDED(parent.fs.mu)
func (parent *Inode) removeAllChildrenUnlocked() {
	for i := 0; i < len(parent.dir.Children); i++ {
		child := parent.dir.Children[i]
		child.mu.Lock()
		if child.isDir() {
			child.removeAllChildrenUnlocked()
		}
		child.DeRef(1)
		child.mu.Unlock()
	}
	// POSIX allows parallel readdir() and modifications,
	// so reset position of all directory handles
	for _, dh := range parent.dir.handles {
		dh.lastInternalOffset = -1
	}
	parent.dir.Children = nil
}

// LOCKS_EXCLUDED(parent.fs.mu)
// LOCKS_EXCLUDED(parent.mu)
// LOCKS_EXCLUDED(inode.mu)
func (parent *Inode) removeChild(inode *Inode) {
	parent.mu.Lock()
	defer parent.mu.Unlock()

	l := len(parent.dir.Children)
	i := sort.Search(l, parent.findInodeFunc(inode.Name))
	if i >= l || parent.dir.Children[i] != inode {
		return
	}

	inode.mu.Lock()
	defer inode.mu.Unlock()

	parent.removeChildUnlocked(inode)
	return
}

func (parent *Inode) insertChild(inode *Inode) {
	parent.mu.Lock()
	defer parent.mu.Unlock()

	parent.insertChildUnlocked(inode)
}

// LOCKS_REQUIRED(parent.mu)
func (parent *Inode) insertChildUnlocked(inode *Inode) {
	inode.Ref()

	l := len(parent.dir.Children)
	if l == 0 {
		parent.dir.Children = []*Inode{inode}
		return
	}

	i := sort.Search(l, parent.findInodeFunc(inode.Name))
	if i == l {
		// not found = new value is the biggest
		parent.dir.Children = append(parent.dir.Children, inode)
	} else {
		if parent.dir.Children[i].Name == inode.Name {
			panic(fmt.Sprintf("double insert of %v", parent.getChildName(inode.Name)))
		}

		// POSIX allows parallel readdir() and modifications,
		// so preserve position of all directory handles
		for _, dh := range parent.dir.handles {
			dh.lastInternalOffset = -1
		}
		if parent.dir.lastOpenDirIdx >= i {
			parent.dir.lastOpenDirIdx++
		}
		parent.dir.Children = append(parent.dir.Children, nil)
		copy(parent.dir.Children[i+1:], parent.dir.Children[i:])
		parent.dir.Children[i] = inode
	}
}

func (parent *Inode) getChildName(name string) string {
	if parent.Id == fuseops.RootInodeID {
		return name
	} else {
		return fmt.Sprintf("%v/%v", parent.FullName(), name)
	}
}

func (parent *Inode) Unlink(name string) (err error) {
	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode := parent.findChildUnlocked(name)
	if inode != nil {
		fuseLog.Debugf("Unlink %v", inode.FullName())
		inode.mu.Lock()
		inode.doUnlink()
		inode.mu.Unlock()
		inode.fs.WakeupFlusher()
	}

	return
}

func (inode *Inode) SendDelete() {
	cloud, key := inode.Parent.cloud()
	key = appendChildName(key, inode.Name)
	oldParent := inode.oldParent
	oldName := inode.oldName
	if oldParent != nil {
		_, key = oldParent.cloud()
		key = appendChildName(key, oldName)
		inode.oldParent = nil
		inode.oldName = ""
	}
	implicit := false
	if inode.isDir() {
		implicit = inode.dir.ImplicitDir
		if !cloud.Capabilities().DirBlob {
			key += "/"
		}
	}
	atomic.AddInt64(&inode.Parent.fs.activeFlushers, 1)
	inode.IsFlushing += inode.fs.flags.MaxParallelParts
	go func() {
		// Delete may race with a parallel listing
		var err error
		if !implicit {
			inode.fs.addInflightChange(key)
			_, err = cloud.DeleteBlob(&DeleteBlobInput{
				Key: key,
			})
			inode.fs.completeInflightChange(key)
		}
		inode.mu.Lock()
		atomic.AddInt64(&inode.Parent.fs.activeFlushers, -1)
		inode.IsFlushing -= inode.fs.flags.MaxParallelParts
		if mapAwsError(err) == syscall.ENOENT {
			// object is already deleted
			err = nil
		}
		inode.recordFlushError(err)
		if err != nil {
			log.Warnf("Failed to delete object %v: %v", key, err)
			inode.mu.Unlock()
			inode.fs.WakeupFlusher()
			return
		}
		forget := false
		if inode.CacheState == ST_DELETED {
			inode.resetCache()
			inode.SetCacheState(ST_DEAD)
			// We don't remove directories until all children are deleted
			// So that we don't revive the directory after removing it
			// by fetching a list of files not all of which are actually deleted
			if inode.refcnt == 0 {
				// Don't call forget with inode locks taken ... :-X
				forget = true
			}
		}
		inode.mu.Unlock()
		if oldParent != nil {
			oldParent.mu.Lock()
			delete(oldParent.dir.DeletedChildren, oldName)
			oldParent.addModified(-1)
			oldParent.mu.Unlock()
		}
		inode.Parent.mu.Lock()
		delete(inode.Parent.dir.DeletedChildren, inode.Name)
		inode.Parent.mu.Unlock()
		if forget {
			inode.mu.Lock()
			inode.DeRef(0)
			inode.mu.Unlock()
		}
		inode.fs.WakeupFlusher()
	}()
}

func (parent *Inode) Create(name string) (*Inode, *FileHandle, error) {
	return parent.CreateOrOpen(name, false)
}

func (parent *Inode) CreateOrOpen(name string, open bool) (inode *Inode, fh *FileHandle, err error) {

	parent.logFuse("Create", name, open)

	fs := parent.fs

	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode = parent.findChildUnlocked(name)
	if inode != nil {
		if open {
			fh, err := inode.OpenFile()
			return inode, fh, err
		}
		return nil, nil, syscall.EEXIST
	}

	now := time.Now()
	inode = NewInode(fs, parent, name)
	inode.userMetadata = make(map[string][]byte)
	inode.mu.Lock()
	defer inode.mu.Unlock()
	inode.Attributes = InodeAttributes{
		Size:  0,
		Ctime: now,
		Mtime: now,
		Uid:   fs.flags.Uid,
		Gid:   fs.flags.Gid,
		Mode:  fs.flags.FileMode,
	}
	// one ref is for lookup
	inode.Ref()
	// another ref is for being in Children
	fs.insertInode(parent, inode)
	inode.SetCacheState(ST_CREATED)
	fs.WakeupFlusher()

	fh = NewFileHandle(inode)
	inode.fileHandles = 1
	// protect directories with open files from eviction
	parent.addModified(1)

	parent.touch()

	return
}

func (parent *Inode) MkDir(
	name string) (inode *Inode, err error) {

	parent.logFuse("MkDir", name)

	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode = parent.findChildUnlocked(name)
	if inode != nil {
		return nil, syscall.EEXIST
	}

	inode = parent.doMkDir(name)
	inode.mu.Unlock()
	parent.fs.WakeupFlusher()

	return
}

// LOCKS_REQUIRED(parent.mu)
// LOCKS_EXCLUDED(parent.fs.mu)
// Returns locked inode (!)
func (parent *Inode) doMkDir(name string) (inode *Inode) {
	if parent.dir.DeletedChildren != nil {
		if oldInode, ok := parent.dir.DeletedChildren[name]; ok {
			if oldInode.isDir() {
				// We should resurrect the old directory when creating a directory
				// over a removed directory instead of recreating it.
				//
				// Because otherwise the following race may become possible:
				// - A large directory with files is removed
				// - We don't have time to flush all changes so some files remain in S3
				// - We create a new directory in place of the removed one
				// - And we recreate some files in it
				// - The new directory won't be flushed until the old one is removed
				//   because flusher checks "overDeleted"
				// - However, the files in it don't check if they are created in
				//   a directory that is created over a removed one
				// - So we can first flush some new files to S3
				// - ...and then delete some of them as we flush "older" deletes with the same key
				delete(parent.dir.DeletedChildren, name)
				inode = NewInode(parent.fs, parent, name)
				inode.ToDir()
				inode.Id = oldInode.Id
				// We leave the older inode in place only for forget() calls
				inode.refcnt = oldInode.refcnt
				oldInode.mu.Lock()
				parent.fs.mu.Lock()
				parent.fs.inodes[oldInode.Id] = inode
				oldInode.Id = parent.fs.allocateInodeId()
				parent.fs.inodes[oldInode.Id] = oldInode
				parent.fs.mu.Unlock()
				oldInode.userMetadataDirty = 0
				oldInode.userMetadata = make(map[string][]byte)
				oldInode.touch()
				oldInode.refcnt = 0
				oldInode.Ref()
				oldInode.SetCacheState(ST_MODIFIED)
				oldInode.Attributes.Ctime = time.Now()
				if parent.Attributes.Ctime.Before(oldInode.Attributes.Ctime) {
					parent.Attributes.Ctime = oldInode.Attributes.Ctime
				}
				oldInode.Attributes.Mtime = time.Now()
				if parent.Attributes.Mtime.Before(oldInode.Attributes.Mtime) {
					parent.Attributes.Mtime = oldInode.Attributes.Mtime
				}
				parent.insertChildUnlocked(oldInode)
				return oldInode
			}
		}
	}
	inode = NewInode(parent.fs, parent, name)
	inode.mu.Lock()
	inode.userMetadata = make(map[string][]byte)
	inode.ToDir()
	inode.touch()
	// Record dir as actual
	inode.dir.DirTime = inode.Attributes.Ctime
	if parent.Attributes.Ctime.Before(inode.Attributes.Ctime) {
		parent.Attributes.Ctime = inode.Attributes.Ctime
	}
	if parent.Attributes.Mtime.Before(inode.Attributes.Mtime) {
		parent.Attributes.Mtime = inode.Attributes.Mtime
	}
	// one ref is for lookup
	inode.Ref()
	// another ref is for being in Children
	parent.fs.insertInode(parent, inode)
	if !parent.fs.flags.NoDirObject {
		inode.SetCacheState(ST_CREATED)
	} else {
		inode.dir.ImplicitDir = true
	}
	inode.SetAttrTime(time.Now())
	return
}

func (parent *Inode) CreateSymlink(
	name string, target string) (inode *Inode, err error) {

	parent.logFuse("CreateSymlink", name)

	fs := parent.fs

	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode = parent.findChildUnlocked(name)
	if inode != nil {
		return nil, syscall.EEXIST
	}

	now := time.Now()
	inode = NewInode(fs, parent, name)
	inode.userMetadata = make(map[string][]byte)
	inode.userMetadata[inode.fs.flags.SymlinkAttr] = []byte(target)
	inode.userMetadataDirty = 2
	inode.mu.Lock()
	defer inode.mu.Unlock()
	inode.Attributes = InodeAttributes{
		Size:  0,
		Mtime: now,
		Ctime: now,
		Uid:   fs.flags.Uid,
		Gid:   fs.flags.Gid,
		Mode:  fs.flags.FileMode,
	}
	// one ref is for lookup
	inode.Ref()
	// another ref is for being in Children
	fs.insertInode(parent, inode)
	inode.SetCacheState(ST_CREATED)
	fs.WakeupFlusher()

	parent.touch()

	return inode, nil
}

func (inode *Inode) ReadSymlink() (target string, err error) {
	inode.mu.Lock()
	defer inode.mu.Unlock()

	if inode.userMetadata[inode.fs.flags.SymlinkAttr] == nil {
		return "", syscall.EIO
	}

	return string(inode.userMetadata[inode.fs.flags.SymlinkAttr]), nil
}

func (dir *Inode) SendMkDir() {
	cloud, key := dir.Parent.cloud()
	key = appendChildName(key, dir.Name)
	if !cloud.Capabilities().DirBlob {
		key += "/"
	}
	params := &PutBlobInput{
		Key:      key,
		Body:     nil,
		DirBlob:  true,
		Metadata: escapeMetadata(dir.userMetadata),
	}
	dir.dir.ImplicitDir = false
	dir.IsFlushing += dir.fs.flags.MaxParallelParts
	atomic.AddInt64(&dir.fs.activeFlushers, 1)
	go func() {
		_, err := cloud.PutBlob(params)
		dir.mu.Lock()
		defer dir.mu.Unlock()
		atomic.AddInt64(&dir.fs.activeFlushers, -1)
		dir.IsFlushing -= dir.fs.flags.MaxParallelParts
		dir.recordFlushError(err)
		if err != nil {
			log.Warnf("Failed to create directory object %v: %v", key, err)
			dir.fs.WakeupFlusher()
			return
		}
		if dir.CacheState == ST_CREATED || dir.CacheState == ST_MODIFIED {
			dir.SetCacheState(ST_CACHED)
			dir.SetAttrTime(time.Now())
		}
		dir.fs.WakeupFlusher()
	}()
}

func appendChildName(parent, child string) string {
	if len(parent) != 0 {
		parent += "/"
	}
	return parent + child
}

func (inode *Inode) isEmptyDir() (bool, error) {
	dh := NewDirHandle(inode)
	dh.mu.Lock()
	dh.Seek(2)
	en, err := dh.ReadDir()
	dh.mu.Unlock()
	return en == nil, err
}

// LOCKS_REQUIRED(inode.Parent.mu)
// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) doUnlink() {
	parent := inode.Parent

	if inode.oldParent != nil && !inode.renamingTo {
		inode.resetCache()
		inode.SetCacheState(ST_DELETED)
	} else if inode.CacheState != ST_CREATED || inode.IsFlushing > 0 {
		// resetCache will clear all buffers and abort the multipart upload
		inode.resetCache()
		if parent.dir.DeletedChildren == nil || parent.dir.DeletedChildren[inode.Name] == nil {
			inode.SetCacheState(ST_DELETED)
			if parent.dir.DeletedChildren == nil {
				parent.dir.DeletedChildren = make(map[string]*Inode)
			}
			parent.dir.DeletedChildren[inode.Name] = inode
		} else {
			// A deleted file is already present, we can just reset the cache
			inode.SetCacheState(ST_DEAD)
		}
	} else {
		inode.resetCache()
		inode.SetCacheState(ST_DEAD)
	}
	inode.Attributes.Size = 0

	parent.removeChildUnlocked(inode)
}

func (parent *Inode) RmDir(name string) (err error) {
	parent.logFuse("Rmdir", name)

	// we know this entry is gone
	parent.mu.Lock()

	// rmdir assumes that <name> was previously looked up
	inode := parent.findChildUnlocked(name)
	parent.mu.Unlock()
	if inode != nil {
		if !inode.isDir() {
			return syscall.ENOTDIR
		}

		dh := NewDirHandle(inode)
		dh.mu.Lock()
		dh.Seek(2)
		en, err := dh.ReadDir()
		dh.mu.Unlock()
		if err != nil {
			return err
		}
		if en != nil {
			fuseLog.Debugf("Directory %v not empty: still has entry \"%v\"", inode.FullName(), en.Name)
			return syscall.ENOTEMPTY
		}

		parent.mu.Lock()
		inode := parent.findChildUnlocked(name)
		if inode != nil {
			inode.mu.Lock()
			inode.doUnlink()
			inode.mu.Unlock()
		}
		parent.mu.Unlock()
		inode.fs.WakeupFlusher()
	}

	return
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) SetCacheState(state int32) {
	wasModified := inode.CacheState == ST_CREATED || inode.CacheState == ST_DELETED || inode.CacheState == ST_MODIFIED
	willBeModified := state == ST_CREATED || state == ST_DELETED || state == ST_MODIFIED
	atomic.StoreInt32(&inode.CacheState, state)
	if wasModified != willBeModified {
		inc := int64(1)
		if wasModified {
			inc = -1
			inode.fs.inodeQueue.Delete(inode.dirtyQueueId)
		} else {
			inode.dirtyQueueId = inode.fs.inodeQueue.Add(uint64(inode.Id))
		}
		inode.Parent.addModified(inc)
	}
}

func (parent *Inode) addModified(inc int64) {
	for parent != nil {
		n := atomic.AddInt64(&parent.dir.ModifiedChildren, inc)
		if n < 0 {
			log.Errorf("BUG: ModifiedChildren of %v < 0", parent.FullName())
		}
		parent = parent.Parent
	}
}

// semantic of rename:
// rename("any", "not_exists") = ok
// rename("file1", "file2") = ok
// rename("empty_dir1", "empty_dir2") = ok
// rename("nonempty_dir1", "empty_dir2") = ok
// rename("nonempty_dir1", "nonempty_dir2") = ENOTEMPTY
// rename("file", "dir") = EISDIR
// rename("dir", "file") = ENOTDIR
// LOCKS_EXCLUDED(parent.mu)
// LOCKS_EXCLUDED(newParent.mu)
func (parent *Inode) Rename(from string, newParent *Inode, to string) (err error) {
	if parent == newParent {
		parent.mu.Lock()
		defer parent.mu.Unlock()
	} else {
		// lock ordering to prevent deadlock
		if parent.Id < newParent.Id {
			parent.mu.Lock()
			newParent.mu.Lock()
		} else {
			newParent.mu.Lock()
			parent.mu.Lock()
		}
		defer parent.mu.Unlock()
		defer newParent.mu.Unlock()
	}

	fromCloud, fromPath := parent.cloud()
	toCloud, toPath := newParent.cloud()
	if fromCloud != toCloud {
		// cannot rename across cloud backend
		err = syscall.EINVAL
		return
	}

	// We rely on lookup() again, cache must be already populated here
	fromInode := parent.findChildUnlocked(from)
	toInode := newParent.findChildUnlocked(to)
	if fromInode == nil {
		return syscall.ENOENT
	}
	fromInode.mu.Lock()
	defer fromInode.mu.Unlock()
	if toInode != nil {
		if fromInode.isDir() {
			if !toInode.isDir() {
				return syscall.ENOTDIR
			}
			toEmpty, err := toInode.isEmptyDir()
			if err != nil {
				return err
			}
			if !toEmpty {
				return syscall.ENOTEMPTY
			}
		} else if toInode.isDir() {
			return syscall.EISDIR
		}
	}

	fromFullName := appendChildName(fromPath, from)
	toFullName := appendChildName(toPath, to)

	if toInode != nil {
		// this file's been overwritten, it's been detached but we can't delete
		// it just yet, because the kernel will still send forget ops to us
		toInode.mu.Lock()
		if toInode.isDir() {
			toInode.doUnlink()
		} else {
			// Do not unlink target file if it's a file to make situation where the old
			// file is already deleted, but the new one is not uploaded yet, impossible
			newParent.removeChildUnlocked(toInode)
			toInode.resetCache()
			toInode.SetCacheState(ST_DEAD)
		}
		toInode.mu.Unlock()
	}

	if fromInode.isDir() {
		fromFullName += "/"
		toFullName += "/"

		// List all objects and rename them in cache (keeping the lock)
		var next string
		var err error
		fromInode.dir.listDone = false
		for !fromInode.dir.listDone {
			next, err = fromInode.listObjectsSlurp(fromInode, next, true, false)
			if err != nil {
				return mapAwsError(err)
			}
		}

		renameRecursive(fromInode, newParent, to)
	} else {
		// Handle staged file renames
		if fromInode.StagedFile != nil && fromInode.StagedFile.FD != nil {
			fs := fromInode.fs

			oldStagedPath := fromInode.StagedFile.FD.Name()
			newStagedDir := fs.flags.StagedWritePath + "/" + newParent.FullName()
			newStagedPath := appendChildName(newStagedDir, to)

			if err := os.MkdirAll(newStagedDir, fs.flags.DirMode); err == nil {
				err := os.Rename(oldStagedPath, newStagedPath)
				if err == nil {
					// Reopen the file descriptor at the new path
					newFD, openErr := os.OpenFile(newStagedPath, os.O_RDWR, fs.flags.FileMode)
					if openErr == nil {
						oldFD := fromInode.StagedFile.FD
						fromInode.StagedFile.FD = newFD
						oldFD.Close()
					}
				}
			}
		}

		renameInCache(fromInode, newParent, to)
	}

	fromInode.fs.WakeupFlusher()

	return
}

func renameRecursive(fromInode *Inode, newParent *Inode, to string) {
	toDir := newParent.doMkDir(to)
	toDir.userMetadata = fromInode.userMetadata
	toDir.dir.ImplicitDir = fromInode.dir.ImplicitDir
	// Trick IDs
	oldId := fromInode.Id
	newId := toDir.Id
	fromInode.Id = newId
	toDir.Id = oldId
	fs := fromInode.fs
	fs.mu.Lock()
	fs.inodes[newId] = fromInode
	fs.inodes[oldId] = toDir
	fs.mu.Unlock()
	// Swap reference counts - the kernel will still send forget ops for the new inode
	fromInode.refcnt, toDir.refcnt = toDir.refcnt, fromInode.refcnt
	for len(fromInode.dir.Children) > 0 {
		child := fromInode.dir.Children[0]
		child.mu.Lock()
		if child.isDir() {
			renameRecursive(child, toDir, child.Name)
		} else {
			renameInCache(child, toDir, child.Name)
		}
		child.mu.Unlock()
	}
	toDir.mu.Unlock()
	fromInode.doUnlink()
}

func renameInCache(fromInode *Inode, newParent *Inode, to string) {
	fuseLog.Debugf("Rename %v to %v", fromInode.FullName(), newParent.getChildName(to))
	// There's a lot of edge cases with the asynchronous rename to handle:
	// 1) rename a new file => we can just upload it with the new name
	// 2) rename a new file that's already being flushed => rename after flush
	// 3) rename a modified file => rename after flush
	// 4) create a new file in place of a renamed one => don't flush until rename completes
	// 5) second rename while rename is already in progress => rename again after the first rename finishes
	// 6) rename then modify then rename => either rename then modify or modify then rename
	// and etc...
	parent := fromInode.Parent
	if fromInode.CacheState == ST_CREATED && fromInode.IsFlushing == 0 && fromInode.mpu == nil ||
		fromInode.oldParent != nil {
		// File is either just created or already renamed
		// In both cases we can move it without changing oldParent
		// ...and, in fact, we CAN'T change oldParent the second time
		if fromInode.renamingTo {
			// File is already being copied to the new name
			// So it may appear in an extra place if we just change the location
			if parent.dir.DeletedChildren == nil {
				parent.dir.DeletedChildren = make(map[string]*Inode)
			}
			if parent.dir.DeletedChildren[fromInode.Name] == nil {
				parent.dir.DeletedChildren[fromInode.Name] = fromInode
			}
			fromInode.renamingTo = false
		} else {
			parent.addModified(-1)
			if fromInode.oldParent == newParent && fromInode.oldName == fromInode.Name {
				// Moved back. Unrename! :D
				fromInode.oldParent = nil
				fromInode.oldName = ""
			}
		}
	} else {
		// Remember that the original file is "deleted"
		// We can skip this step if the file is new and isn't being flushed yet
		if parent.dir.DeletedChildren == nil {
			parent.dir.DeletedChildren = make(map[string]*Inode)
		}
		if parent.dir.DeletedChildren[fromInode.Name] == nil {
			parent.dir.DeletedChildren[fromInode.Name] = fromInode
		}
		if fromInode.CacheState == ST_CACHED {
			// Was not modified and we remove it from current parent => add modified
			parent.addModified(1)
		}
		fromInode.oldParent = parent
		fromInode.oldName = fromInode.Name
	}
	if newParent.dir.DeletedChildren != nil &&
		newParent.dir.DeletedChildren[to] == fromInode {
		// Moved back. Undelete!
		delete(newParent.dir.DeletedChildren, to)
	}
	// Rename on-disk cache entry
	if fromInode.OnDisk {
		fs := fromInode.fs
		oldFileName := fs.flags.CachePath + "/" + fromInode.FullName()
		newDirName := fs.flags.CachePath + "/" + newParent.FullName()
		newFileName := appendChildName(newDirName, to)
		err := os.MkdirAll(newDirName, fs.flags.CacheFileMode|((fs.flags.CacheFileMode&0777)>>2))
		if err == nil {
			err = os.Rename(oldFileName, newFileName)
		}
		if err != nil {
			log.Warnf("Error renaming %v to %v: %v", oldFileName, newFileName, err)
			if fromInode.DiskCacheFD != nil {
				fromInode.DiskCacheFD.Close()
				fromInode.DiskCacheFD = nil
				fromInode.fs.diskFdQueue.DeleteFD(fromInode)
			}
		}
	}
	fromInode.Ref()
	parent.removeChildUnlocked(fromInode)
	if fromInode.fileHandles > 0 {
		// Move filehandle modification protection
		parent.addModified(-1)
		newParent.addModified(1)
	}
	fromInode.Name = to
	fromInode.Parent = newParent
	if fromInode.CacheState == ST_CACHED {
		// Was not modified => we make it modified
		fromInode.SetCacheState(ST_MODIFIED)
	} else {
		// Was already modified => stays modified
		newParent.addModified(1)
	}
	newParent.insertChildUnlocked(fromInode)
	fromInode.DeRef(1)
}

// if I had seen a/ and a/b, and now I get a/c, that means a/b is
// done, but not a/
func (parent *Inode) isParentOf(inode *Inode) bool {
	return inode.Parent != nil && (parent == inode.Parent || parent.isParentOf(inode.Parent))
}

func sealPastDirs(dirs map[*Inode]bool, d *Inode) {
	for p, sealed := range dirs {
		if p != d && !sealed && !p.isParentOf(d) {
			dirs[p] = true
		}
	}
	// I just read something in d, obviously it's not done yet
	dirs[d] = false
}

// LOCKS_REQUIRED(parent.mu)
// LOCKS_EXCLUDED(parent.fs.mu)
func (parent *Inode) insertSubTree(path string, obj *BlobItemOutput, dirs map[*Inode]bool) {
	fs := parent.fs
	slash := strings.Index(path, "/")
	if slash == -1 {
		inode := parent.findChildUnlocked(path)
		if inode == nil {
			// don't revive deleted items
			_, deleted := parent.dir.DeletedChildren[path]
			if !deleted {
				inode = NewInode(fs, parent, path)
				// our locking order is parent before child, inode before fs. try to respect it
				fs.insertInode(parent, inode)
				inode.SetFromBlobItem(obj)
			}
		} else {
			inode.SetFromBlobItem(obj)
		}
		sealPastDirs(dirs, parent)
	} else {
		dir := path[:slash]
		path = path[slash+1:]

		// ensure that the potentially implicit dir is added
		inode := parent.findChildUnlocked(dir)
		if inode == nil {
			// don't revive deleted items
			_, deleted := parent.dir.DeletedChildren[dir]
			if !deleted {
				inode = NewInode(fs, parent, dir)
				inode.ToDir()
				fs.insertInode(parent, inode)
			}
		} else if !inode.isDir() {
			// replace unmodified file item with a directory
			if atomic.LoadInt32(&inode.CacheState) <= ST_DEAD {
				inode.mu.Lock()
				inode.resetCache()
				inode.SetCacheState(ST_DEAD)
				parent.removeChildUnlocked(inode)
				inode.mu.Unlock()
				// create a directory inode instead
				inode = NewInode(fs, parent, dir)
				inode.ToDir()
				fs.insertInode(parent, inode)
			} else {
				inode = nil
			}
		}

		if inode != nil {
			isDirBlob := len(path) == 0
			if isDirBlob {
				inode.SetFromBlobItem(obj)
				sealPastDirs(dirs, inode)
			} else {
				now := time.Now()
				if inode.AttrTime.Before(now) {
					inode.SetAttrTime(now)
				}
			}

			// mark this dir but don't seal anything else
			// until we get to the leaf
			dirs[inode] = false

			if !isDirBlob {
				inode.mu.Lock()
				inode.insertSubTree(path, obj, dirs)
				inode.mu.Unlock()
			}
		}
	}
}

func (parent *Inode) findChildMaxTime() (maxMtime, maxCtime time.Time) {
	maxCtime = parent.Attributes.Ctime
	maxMtime = parent.Attributes.Mtime

	for _, c := range parent.dir.Children {
		if c.Attributes.Ctime.After(maxCtime) {
			maxCtime = c.Attributes.Ctime
		}
		if c.Attributes.Mtime.After(maxMtime) {
			maxMtime = c.Attributes.Mtime
		}
	}

	return
}

func (parent *Inode) LookUpCached(name string) (inode *Inode, err error) {
	parent.mu.Lock()
	ok := false
	inode = parent.findChildUnlocked(name)
	if inode != nil {
		ok = true
		if expired(inode.AttrTime, parent.fs.flags.StatCacheTTL) {
			ok = false
			if inode.CacheState != ST_CACHED ||
				inode.isDir() && atomic.LoadInt64(&inode.dir.ModifiedChildren) > 0 {
				// we have an open file handle, object
				// in S3 may not represent the true
				// state of the file anyway, so just
				// return what we know which is
				// potentially more accurate
				ok = true
			} else {
				inode.logFuse("lookup expired")
			}
		}
	} else {
		ok = false
		if parent.dir.DeletedChildren != nil {
			if _, ok := parent.dir.DeletedChildren[name]; ok {
				// File is deleted locally
				parent.mu.Unlock()
				return nil, syscall.ENOENT
			}
		}
		if !expired(parent.dir.DirTime, parent.fs.flags.StatCacheTTL) {
			// Don't recheck from the server if directory cache is actual
			parent.mu.Unlock()
			return nil, syscall.ENOENT
		}
	}
	parent.mu.Unlock()
	if !ok {
		inode, err = parent.recheckInode(inode, name)
		err = mapAwsError(err)
		if err != nil {
			return nil, err
		}
		if inode == nil {
			return nil, syscall.ENOENT
		}
	}
	return inode, nil
}

func (parent *Inode) recheckInode(inode *Inode, name string) (newInode *Inode, err error) {
	newInode, err = parent.LookUp(name, inode == nil && !parent.fs.flags.NoPreloadDir)
	if err != nil {
		if inode != nil {
			parent.removeChild(inode)
		}
		return nil, err
	}
	return newInode, nil
}

func (parent *Inode) LookUp(name string, doSlurp bool) (*Inode, error) {
	_, parentKey := parent.cloud()
	key := appendChildName(parentKey, name)
	root := parent
	for root != nil && root.dir.cloud == nil {
		root = root.Parent
	}
	expire := time.Now().Add(-parent.fs.flags.StatCacheTTL)
	root.mu.Lock()
	loaded := root.dir.checkGapLoaded(key, expire) && root.dir.checkGapLoaded(key+"/", expire)
	root.mu.Unlock()
	if loaded {
		parent.mu.Lock()
		inode := parent.findChildUnlocked(name)
		parent.mu.Unlock()
		return inode, nil
	}
	if doSlurp {
		// 99% of time it's impractical to do 2 HEAD requests per file when looking it up
		// So we first try to preload a whole batch of files starting with our key
		// If the file/directory is there, the listing result will highly likely contain it
		// The only case where it may be missing from the listing is when it's a directory
		// and there's a lot of (more than 1000) files named "<file>[\x20-\x2E]...", because
		// these names will come before "file/".
		_, err := root.listObjectsSlurp(&Inode{Parent: parent}, key, false, true)
		if err != nil {
			return nil, err
		}
		parent.mu.Lock()
		inode := parent.findChildUnlocked(name)
		parent.mu.Unlock()
		root.mu.Lock()
		loaded := root.dir.checkGapLoaded(key, expire) && root.dir.checkGapLoaded(key+"/", expire)
		root.mu.Unlock()
		if inode != nil || loaded {
			return inode, nil
		}
	}
	myList := parent.fs.addInflightListing()
	blob, err := parent.LookUpInodeMaybeDir(name)
	if err != nil {
		parent.fs.completeInflightListing(myList)
		return nil, err
	}
	dirs := make(map[*Inode]bool)
	prefixLen := len(parentKey)
	if prefixLen > 0 {
		prefixLen++
	}
	parent.mu.Lock()
	skipListing := parent.fs.completeInflightListing(myList)
	if skipListing == nil || !skipListing[*blob.Key] {
		parent.insertSubTree((*blob.Key)[prefixLen:], blob, dirs)
	}
	inode := parent.findChildUnlocked(name)
	parent.mu.Unlock()
	return inode, nil
}

func (parent *Inode) LookUpInodeMaybeDir(name string) (*BlobItemOutput, error) {
	cloud, parentKey := parent.cloud()
	if cloud == nil {
		panic("s3 disabled")
	}
	key := appendChildName(parentKey, name)
	parent.logFuse("Inode.LookUp", key)

	var object, dirObject *HeadBlobOutput
	var prefixList *ListBlobsOutput
	var objectError, dirError, prefixError error
	results := make(chan int, 3)
	n := 0

	for {
		n++
		go func() {
			object, objectError = cloud.HeadBlob(&HeadBlobInput{Key: key})
			results <- 1
		}()
		if cloud.Capabilities().DirBlob {
			<-results
			break
		}
		if parent.fs.flags.Cheap {
			<-results
			if mapAwsError(objectError) != syscall.ENOENT {
				break
			}
		}

		if !parent.fs.flags.NoDirObject {
			n++
			go func() {
				dirObject, dirError = cloud.HeadBlob(&HeadBlobInput{Key: key + "/"})
				results <- 2
			}()
			if parent.fs.flags.Cheap {
				<-results
				if mapAwsError(dirError) != syscall.ENOENT {
					break
				}
			}
		}

		if !parent.fs.flags.ExplicitDir {
			n++
			go func() {
				prefixList, prefixError = RetryListBlobs(parent.fs.flags, cloud, &ListBlobsInput{
					Delimiter: PString("/"),
					MaxKeys:   PUInt32(1),
					Prefix:    PString(key + "/"),
				})
				results <- 3
			}()
			if parent.fs.flags.Cheap {
				<-results
			}
		}

		break
	}

	for n > 0 {
		n--
		if !cloud.Capabilities().DirBlob && !parent.fs.flags.Cheap {
			<-results
		}
		if object != nil {
			return &object.BlobItemOutput, nil
		}
		if dirObject != nil {
			return &dirObject.BlobItemOutput, nil
		}
		if prefixList != nil && (len(prefixList.Prefixes) != 0 || len(prefixList.Items) != 0) {
			if len(prefixList.Items) != 0 && (*prefixList.Items[0].Key == key ||
				(*prefixList.Items[0].Key)[0:len(key)+1] == key+"/") {
				return &prefixList.Items[0], nil
			}
			return &BlobItemOutput{
				Key: PString(key + "/"),
			}, nil
		}
	}

	if objectError != nil && mapAwsError(objectError) != syscall.ENOENT {
		return nil, objectError
	}
	if dirError != nil && mapAwsError(dirError) != syscall.ENOENT {
		return nil, dirError
	}
	if prefixError != nil && mapAwsError(prefixError) != syscall.ENOENT {
		return nil, prefixError
	}
	return nil, syscall.ENOENT
}
