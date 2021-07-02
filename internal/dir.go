// Copyright 2015 - 2017 Ka-Hing Cheung
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

package internal

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
)

type DirInodeData struct {
	cloud       StorageBackend
	mountPrefix string

	// these 2 refer to readdir of the Children
	lastOpenDir     *DirInodeData
	lastOpenDirIdx  int
	seqOpenDirScore uint8
	DirTime         time.Time

	slurpMarker *string
	listMarker *string
	lastFromCloud *string
	listDone bool

	ModifiedChildren int64

	Children []*Inode
	DeletedChildren map[string]*Inode
	handles []*DirHandle
}

type DirHandleEntry struct {
	Name   string
	Inode  fuseops.InodeID
	Type   fuseutil.DirentType
	Offset fuseops.DirOffset
}

// Returns true if any char in `inp` has a value < '/'.
// This should work for unicode also: unicode chars are all greater than 128.
// See TestHasCharLtSlash for examples.
func hasCharLtSlash(inp string) bool {
	for _, c := range inp {
		if c < '/' {
			return true
		}
	}
	return false
}

// Gets the name of the blob/prefix from a full cloud path.
// See TestCloudPathToName for examples.
func cloudPathToName(inp string) string {
	inp = strings.TrimRight(inp, "/")
	split := strings.Split(inp, "/")
	return split[len(split)-1]
}

// Returns true if the last prefix's name or last item's name from the given
// ListBlobsOutput has a character less than '/'
// See TestShouldFetchNextListBlobsPage for examples.
func shouldFetchNextListBlobsPage(resp *ListBlobsOutput) bool {
	if !resp.IsTruncated {
		// There is no next page.
		return false
	}
	numPrefixes := len(resp.Prefixes)
	numItems := len(resp.Items)
	if numPrefixes > 0 &&
		hasCharLtSlash(cloudPathToName(*resp.Prefixes[numPrefixes-1].Prefix)) {
		return true
	} else if numItems > 0 &&
		hasCharLtSlash(cloudPathToName(*resp.Items[numItems-1].Key)) {
		return true
	}
	return false
}

type DirHandle struct {
	inode *Inode
	mu sync.Mutex // everything below is protected by mu
	// readdir() is allowed either at zero (restart from the beginning)
	// or from the previous offset
	lastExternalOffset fuseops.DirOffset
	lastInternalOffset int
}

func NewDirHandle(inode *Inode) (dh *DirHandle) {
	dh = &DirHandle{inode: inode}
	return
}

func (inode *Inode) OpenDir() (dh *DirHandle) {
	inode.logFuse("OpenDir")
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

	if isS3 && parent != nil && inode.fs.flags.TypeCacheTTL != 0 {
		parent.mu.Lock()
		defer parent.mu.Unlock()

		numChildren := len(parent.dir.Children)
		dirIdx := -1
		seqMode := false
		firstDir := false

		if parent.dir.lastOpenDir == nil {
			// check if we are opening the first child
			// (after . and ..)  cap the search to 1000
			// peers to bound the time. If the next dir is
			// more than 1000 away, slurping isn't going
			// to be helpful anyway
			for i := 2; i < MinInt(numChildren, 1000); i++ {
				c := parent.dir.Children[i]
				if c.isDir() {
					if *c.Name == *inode.Name {
						dirIdx = i
						seqMode = true
						firstDir = true
					}
					break
				}
			}
		} else {
			// check if we are reading the next one as expected
			for i := parent.dir.lastOpenDirIdx + 1; i < MinInt(numChildren, 1000); i++ {
				c := parent.dir.Children[i]
				if c.isDir() {
					if *c.Name == *inode.Name {
						dirIdx = i
						seqMode = true
					}
					break
				}
			}
		}

		if seqMode {
			if parent.dir.seqOpenDirScore < 255 {
				parent.dir.seqOpenDirScore++
			}
			if parent.dir.seqOpenDirScore == 2 {
				fuseLog.Debugf("%v in readdir mode", *parent.FullName())
			}
			parent.dir.lastOpenDir = dir
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
					fuseLog.Debugf("%v in readdir mode", *inode.FullName())
				}
			}
		} else {
			parent.dir.seqOpenDirScore = 0
			if dirIdx == -1 {
				dirIdx = parent.findChildIdxUnlocked(*inode.Name)
			}
			if dirIdx != -1 {
				parent.dir.lastOpenDir = dir
				parent.dir.lastOpenDirIdx = dirIdx
			}
		}
	}

	dh = NewDirHandle(inode)
	inode.mu.Lock()
	inode.dir.handles = append(inode.dir.handles, dh)
	atomic.AddInt32(&inode.fileHandles, 1)
	inode.mu.Unlock()
	return
}

func (inode *Inode) listObjectsSlurp(lock bool) (err error) {
	cloud, key := inode.cloud()
	if inode.oldParent != nil {
		_, key = inode.oldParent.cloud()
		key = appendChildName(key, *inode.oldName)
	}
	prefix := key
	if len(prefix) != 0 {
		prefix += "/"
	}

	params := &ListBlobsInput{
		Prefix:     &prefix,
		StartAfter: inode.dir.slurpMarker,
	}

	resp, err := cloud.ListBlobs(params)
	if err != nil {
		s3Log.Errorf("ListObjects %v = %v", params, err)
		return
	}
	s3Log.Debug(resp)

	if lock {
		inode.mu.Lock()
	}
	inode.fs.mu.Lock()
	dirs := make(map[*Inode]bool)
	for _, obj := range resp.Items {
		baseName := (*obj.Key)[len(prefix):]
		if baseName != "" {
			inode.insertSubTree(baseName, &obj, dirs)
		}
	}
	inode.fs.mu.Unlock()
	if lock {
		inode.mu.Unlock()
	}

	for d, sealed := range dirs {
		if d == inode {
			// never seal the current dir because that's
			// handled at upper layer
			continue
		}

		if sealed || !resp.IsTruncated {
			d.dir.listDone = true
			d.dir.DirTime = time.Now()
			d.Attributes.Mtime = d.findChildMaxTime()
		}
	}

	obj := resp.Items[len(resp.Items)-1]
	seal := false
	// if we are done listing prefix, we are good
	if !strings.HasPrefix(*obj.Key, prefix) {
		if *obj.Key > prefix {
			seal = true
		}
	} else if !resp.IsTruncated {
		seal = true
	}

	if seal {
		inode.dir.slurpMarker = nil
		inode.dir.listMarker = nil
		inode.dir.listDone = true
		inode.dir.lastFromCloud = nil
		inode.dir.DirTime = time.Now()
	} else {
		// NextContinuationToken is not returned when delimiter is empty, so use obj.Key
		inode.dir.slurpMarker = obj.Key
		last := (*obj.Key)[len(prefix):]
		p := strings.Index(last, "/")
		if p > 0 {
			last = last[0 : p]
		}
		inode.dir.lastFromCloud = &last
	}

	return
}

// Sorting order of entries in directories is slightly inconsistent between goofys
// and azblob, s3. This inconsistency can be a problem if the listing involves
// multiple pagination results. Call this instead of `cloud.ListBlobs` if you are
// paginating.
//
// Problem: In s3 & azblob, prefixes are returned with '/' => the prefix "2019" is
// returned as "2019/". So the list api for these backends returns "2019/" after
// "2019-0001/" because ascii("/") > ascii("-"). This is problematic for goofys if
// "2019/" is returned in x+1'th batch and "2019-0001/" is returned in x'th; Goofys
// stores the results as they arrive in a sorted array and expects backends to return
// entries in a sorted order.
// We cant just use ordering of s3/azblob because different cloud providers have
// different sorting strategies when it involes directories. In s3 "a/" > "a-b/".
// In adlv2 it is opposite.
//
// Solution: To deal with this our solution with follows (for all backends). For
// a single call of ListBlobs, we keep requesting multiple list batches until there
// is nothing left to list or the last listed entry has all characters > "/"
// Relavant test case: TestReadDirDash
func listBlobsSafe(cloud StorageBackend, param *ListBlobsInput) (*ListBlobsOutput, error) {
	res, err := cloud.ListBlobs(param)
	if err != nil {
		return nil, err
	}

	for shouldFetchNextListBlobsPage(res) {
		nextReq := &ListBlobsInput{
			// Inherit Prefix, Delimiter, MaxKeys from original request.
			Prefix:    param.Prefix,
			Delimiter: param.Delimiter,
			MaxKeys:   param.MaxKeys,
			// Get the continuation token from the result.
			ContinuationToken: res.NextContinuationToken,
		}
		nextRes, err := cloud.ListBlobs(nextReq)
		if err != nil {
			return nil, err
		}

		res = &ListBlobsOutput{
			// Add new items and prefixes.
			Prefixes: append(res.Prefixes, nextRes.Prefixes...),
			Items:    append(res.Items, nextRes.Items...),
			// Inherit NextContinuationToken, IsTruncated from nextRes.
			NextContinuationToken: nextRes.NextContinuationToken,
			IsTruncated:           nextRes.IsTruncated,
			// We no longer have a single request. This is composite request. Concatenate
			// new request id to exiting.
			RequestId: res.RequestId + ", " + nextRes.RequestId,
		}
	}
	return res, nil
}

func (dh *DirHandle) handleListResult(resp *ListBlobsOutput, prefix string) {
	parent := dh.inode
	fs := parent.fs

	for _, dir := range resp.Prefixes {
		// strip trailing /
		dirName := (*dir.Prefix)[0 : len(*dir.Prefix)-1]
		// strip previous prefix
		dirName = dirName[len(prefix):]
		if len(dirName) == 0 {
			continue
		}

		if inode := parent.findChildUnlocked(dirName); inode != nil {
			now := time.Now()
			// don't want to update time if this
			// inode is setup to never expire
			if inode.AttrTime.Before(now) {
				inode.AttrTime = now
			}
		} else if _, deleted := parent.dir.DeletedChildren[dirName]; !deleted {
			// don't revive deleted items
			inode := NewInode(fs, parent, &dirName)
			inode.ToDir()
			fs.insertInode(parent, inode)
		}

		dh.inode.dir.lastFromCloud = &dirName
	}

	for _, obj := range resp.Items {
		baseName := (*obj.Key)[len(prefix):]

		slash := strings.Index(baseName, "/")
		if slash == -1 {
			if len(baseName) == 0 {
				// shouldn't happen
				continue
			}

			inode := parent.findChildUnlocked(baseName)
			if inode != nil {
				// don't update modified items
				if inode.CacheState == ST_CACHED {
					inode.SetFromBlobItem(&obj)
				}
			} else {
				// don't revive deleted items
				_, deleted := parent.dir.DeletedChildren[baseName]
				if !deleted {
					inode = NewInode(fs, parent, &baseName)
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

func (dh *DirHandle) listObjectsFlat() (err error) {
	cloud, prefix := dh.inode.cloud()
	if cloud == nil {
		// Stale inode
		return fuse.ENOENT
	}
	if dh.inode.oldParent != nil {
		_, prefix = dh.inode.oldParent.cloud()
		prefix = appendChildName(prefix, *dh.inode.oldName)
	}
	if len(prefix) != 0 {
		prefix += "/"
	}

	params := &ListBlobsInput{
		Delimiter:         aws.String("/"),
		ContinuationToken: dh.inode.dir.listMarker,
		Prefix:            &prefix,
	}
	dh.mu.Unlock()
	resp, err := listBlobsSafe(cloud, params)
	dh.mu.Lock()
	if err != nil {
		return
	}

	s3Log.Debug(resp)

	if resp.IsTruncated {
		dh.inode.dir.listMarker = resp.NextContinuationToken
	} else {
		dh.inode.dir.listMarker = nil
		dh.inode.dir.listDone = true
		dh.inode.dir.DirTime = time.Now()
	}

	dh.inode.mu.Lock()
	dh.inode.fs.mu.Lock()

	dh.handleListResult(resp, prefix)

	dh.inode.fs.mu.Unlock()
	dh.inode.mu.Unlock()

	return
}

func (dh *DirHandle) readDirFromCache(internalOffset int, offset fuseops.DirOffset) (en *DirHandleEntry, ok bool) {
	dh.inode.mu.Lock()
	defer dh.inode.mu.Unlock()

	if dh.inode.dir == nil {
		panic(*dh.inode.FullName())
	}
	if !expired(dh.inode.dir.DirTime, dh.inode.fs.flags.TypeCacheTTL) {
		ok = true

		if int(internalOffset) >= len(dh.inode.dir.Children) {
			return
		}
		child := dh.inode.dir.Children[internalOffset]

		en = &DirHandleEntry{
			Name:   *child.Name,
			Inode:  child.Id,
			Offset: offset + 1,
		}
		if child.isDir() {
			en.Type = fuseutil.DT_Directory
		} else {
			en.Type = fuseutil.DT_File
		}

	}
	return
}

// LOCKS_REQUIRED(dh.mu)
// LOCKS_EXCLUDED(dh.inode.mu)
// LOCKS_EXCLUDED(dh.inode.fs)
func (dh *DirHandle) ReadDir(internalOffset int, offset fuseops.DirOffset) (en *DirHandleEntry, err error) {
	en, ok := dh.readDirFromCache(internalOffset, offset)
	if ok {
		return
	}

	parent := dh.inode
	fs := parent.fs

	parent.mu.Lock()

	// FIXME Allow to use slurp more than 1 directory level above

	useSlurp := fs.flags.TypeCacheTTL != 0 &&
		(dh.inode.Parent != nil && dh.inode.Parent.dir.seqOpenDirScore >= 2)

	if !useSlurp && dh.inode.dir.listMarker == nil ||
		useSlurp && dh.inode.Parent.dir.listMarker == nil {
		// listMarker is nil => We just started refreshing this directory
		dh.inode.dir.listDone = false
		dh.inode.dir.lastFromCloud = nil
		// Remove unmodified stale inodes when we start listing
		for i := 2; i < len(parent.dir.Children); i++ {
			// Note on locking: See comments at Inode::AttrTime, Inode::Parent.
			childTmp := parent.dir.Children[i]
			// FIXME: Check if the kernel may still access removed inodes by their cached numbers
			if atomic.LoadInt32(&childTmp.fileHandles) == 0 &&
				atomic.LoadInt32(&childTmp.CacheState) == ST_CACHED &&
				(!childTmp.isDir() || atomic.LoadInt64(&childTmp.dir.ModifiedChildren) == 0) {
				childTmp.mu.Lock()
				parent.removeChildUnlocked(childTmp)
				childTmp.mu.Unlock()
				i--
			}
		}
	}

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
	for dh.inode.dir.lastFromCloud == nil && !dh.inode.dir.listDone {
		// FIXME Maybe allow to skip some objects deep inside the previous directory
		// by resetting slurp position when it's used after a non-sealed directory
		parent.mu.Unlock()
		if useSlurp {
			dh.mu.Unlock()
			err = dh.inode.Parent.listObjectsSlurp(true)
			dh.mu.Lock()
		} else {
			err = dh.listObjectsFlat()
		}
		if err != nil {
			return nil, err
		}
		parent.mu.Lock()
	}

	if int(internalOffset) >= len(parent.dir.Children) {
		// we've reached the end
		parent.Attributes.Mtime = parent.findChildMaxTime()
		parent.mu.Unlock()
		return nil, nil
	}

	child := parent.dir.Children[internalOffset]
	en = &DirHandleEntry{
		Name:   *child.Name,
		Inode:  child.Id,
		Offset: offset + 1,
	}
	if child.isDir() {
		en.Type = fuseutil.DT_Directory
	} else {
		en.Type = fuseutil.DT_File
	}

	if parent.dir.lastFromCloud != nil && en.Name == *parent.dir.lastFromCloud {
		parent.dir.lastFromCloud = nil
	}

	parent.mu.Unlock()

	return en, nil
}

func (dh *DirHandle) CloseDir() error {
	dh.inode.mu.Lock()
	i := 0
	for ; i < len(dh.inode.dir.handles) && dh.inode.dir.handles[i] != dh; i++ {
	}
	if i < len(dh.inode.dir.handles) {
		dh.inode.dir.handles = append(dh.inode.dir.handles[0 : i], dh.inode.dir.handles[i+1 : ]...)
		atomic.AddInt32(&dh.inode.fileHandles, -1)
	}
	dh.inode.mu.Unlock()
	return nil
}

// Recursively resets the DirTime for child directories.
// ACQUIRES_LOCK(inode.mu)
func (inode *Inode) resetDirTimeRec() {
	inode.mu.Lock()
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
	inode.ImplicitDir = false
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
	return func(i int) bool {
		return (*parent.dir.Children[i].Name) >= name
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
		if *parent.dir.Children[i].Name == name {
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
	if i < l && *parent.dir.Children[i].Name == name {
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
	i := sort.Search(l, parent.findInodeFunc(*inode.Name))
	if i >= l || *parent.dir.Children[i].Name != *inode.Name {
		panic(fmt.Sprintf("%v.removeName(%v) but child not found: %v",
			*parent.FullName(), *inode.Name, i))
	}

	// POSIX allows parallel readdir() and modifications,
	// so preserve position of all directory handles
	for _, dh := range parent.dir.handles {
		if dh.lastInternalOffset > i {
			dh.lastInternalOffset--
		}
	}
	copy(parent.dir.Children[i:], parent.dir.Children[i+1:])
	parent.dir.Children[l-1] = nil
	parent.dir.Children = parent.dir.Children[:l-1]

	if cap(parent.dir.Children)-len(parent.dir.Children) > 20 {
		tmp := make([]*Inode, len(parent.dir.Children))
		copy(tmp, parent.dir.Children)
		parent.dir.Children = tmp
	}

	inode.DeRef(1)
}

// LOCKS_EXCLUDED(parent.fs.mu)
func (parent *Inode) removeChild(inode *Inode) {
	parent.mu.Lock()
	defer parent.mu.Unlock()

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

	i := sort.Search(l, parent.findInodeFunc(*inode.Name))
	if i == l {
		// not found = new value is the biggest
		parent.dir.Children = append(parent.dir.Children, inode)
	} else {
		if *parent.dir.Children[i].Name == *inode.Name {
			panic(fmt.Sprintf("double insert of %v", parent.getChildName(*inode.Name)))
		}

		// POSIX allows parallel readdir() and modifications,
		// so preserve position of all directory handles
		for _, dh := range parent.dir.handles {
			if dh.lastInternalOffset > i {
				dh.lastInternalOffset++
			}
		}
		parent.dir.Children = append(parent.dir.Children, nil)
		copy(parent.dir.Children[i+1:], parent.dir.Children[i:])
		parent.dir.Children[i] = inode
	}
}

func (parent *Inode) LookUp(name string) (inode *Inode, err error) {
	parent.logFuse("Inode.LookUp", name)

	inode, err = parent.LookUpInodeMaybeDir(name, parent.getChildName(name))
	if err != nil {
		return nil, err
	}

	return
}

func (parent *Inode) getChildName(name string) string {
	if parent.Id == fuseops.RootInodeID {
		return name
	} else {
		return fmt.Sprintf("%v/%v", *parent.FullName(), name)
	}
}

func (parent *Inode) Unlink(name string) (err error) {
	parent.logFuse("Unlink", name)

	parent.mu.Lock()
	defer parent.mu.Unlock()

	inode := parent.findChildUnlocked(name)
	if inode != nil {
		inode.mu.Lock()
		inode.doUnlink()
		inode.mu.Unlock()
		inode.fs.WakeupFlusher()
	}

	return
}

func (inode *Inode) SendDelete() {
	cloud, key := inode.Parent.cloud()
	key = appendChildName(key, *inode.Name)
	if inode.isDir() && !cloud.Capabilities().DirBlob {
		key += "/"
	}
	atomic.AddInt64(&inode.Parent.fs.activeFlushers, 1)
	inode.IsFlushing += inode.fs.flags.MaxParallelParts
	implicit := inode.ImplicitDir
	go func() {
		var err error
		if !implicit {
			_, err = cloud.DeleteBlob(&DeleteBlobInput{
				Key: key,
			})
		}
		inode.mu.Lock()
		atomic.AddInt64(&inode.Parent.fs.activeFlushers, -1)
		inode.IsFlushing -= inode.fs.flags.MaxParallelParts
		if err == fuse.ENOENT {
			// object is already deleted
			err = nil
		}
		inode.recordFlushError(err)
		if err != nil {
			log.Errorf("Failed to delete object %v: %v", key, err)
			inode.mu.Unlock()
			return
		}
		forget := false
		if inode.CacheState == ST_DELETED {
			inode.SetCacheState(ST_CACHED)
			// We don't remove directories until all children are deleted
			// So that we don't revive the directory after removing it
			// by fetching a list of files not all of which are actually deleted
			if inode.refcnt == 0 {
				// Don't call forget with inode locks taken ... :-X
				forget = true
			}
		}
		inode.mu.Unlock()
		inode.Parent.mu.Lock()
		delete(inode.Parent.dir.DeletedChildren, *inode.Name)
		inode.Parent.mu.Unlock()
		if forget {
			inode.mu.Lock()
			inode.DeRef(0)
			inode.mu.Unlock()
		}
		inode.fs.WakeupFlusher()
	}()
}

func (parent *Inode) Create(
	name string, metadata fuseops.OpMetadata) (inode *Inode, fh *FileHandle) {

	parent.logFuse("Create", name)

	fs := parent.fs

	parent.mu.Lock()
	defer parent.mu.Unlock()

	fs.mu.Lock()
	defer fs.mu.Unlock()

	now := time.Now()
	inode = NewInode(fs, parent, &name)
	inode.userMetadata = make(map[string][]byte)
	inode.mu.Lock()
	defer inode.mu.Unlock()
	inode.Attributes = InodeAttributes{
		Size:  0,
		Mtime: now,
	}
	// one ref is for lookup
	inode.Ref()
	// another ref is for being in Children
	fs.insertInode(parent, inode)
	inode.SetCacheState(ST_CREATED)
	fs.WakeupFlusher()

	fh = NewFileHandle(inode, metadata)
	inode.fileHandles = 1
	inode.fileHandle = fh

	parent.touch()

	return
}

func (parent *Inode) MkDir(
	name string) (inode *Inode, err error) {

	parent.logFuse("MkDir", name)

	parent.mu.Lock()
	defer parent.mu.Unlock()

	parent.fs.mu.Lock()
	defer parent.fs.mu.Unlock()

	inode = parent.doMkDir(name)
	inode.mu.Unlock()
	parent.fs.WakeupFlusher()

	return
}

func (parent *Inode) doMkDir(name string) (inode *Inode) {
	inode = NewInode(parent.fs, parent, &name)
	inode.mu.Lock()
	inode.userMetadata = make(map[string][]byte)
	inode.ToDir()
	inode.touch()
	// Record dir as actual
	inode.dir.DirTime = inode.Attributes.Mtime
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
		inode.ImplicitDir = true
	}
	return
}

func (dir *Inode) SendMkDir() {
	cloud, key := dir.Parent.cloud()
	key = appendChildName(key, *dir.Name)
	if !cloud.Capabilities().DirBlob {
		key += "/"
	}
	params := &PutBlobInput{
		Key:     key,
		Body:    nil,
		DirBlob: true,
	}
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
			log.Errorf("Failed to create directory object %v: %v", key, err)
			return
		}
		if dir.CacheState == ST_CREATED {
			dir.SetCacheState(ST_CACHED)
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
	en, err := dh.ReadDir(2, 2)
	dh.mu.Unlock()
	return en == nil, err
}

// LOCKS_REQUIRED(inode.Parent.mu)
// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) doUnlink() {
	parent := inode.Parent

	if inode.CacheState != ST_CREATED || inode.IsFlushing > 0 {
		inode.SetCacheState(ST_DELETED)
		if parent.dir.DeletedChildren == nil {
			parent.dir.DeletedChildren = make(map[string]*Inode)
		}
		parent.dir.DeletedChildren[*inode.Name] = inode
	} else {
		inode.SetCacheState(ST_CACHED)
	}

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
			return fuse.ENOTDIR
		}

		dh := NewDirHandle(inode)
		dh.mu.Lock()
		en, err := dh.ReadDir(2, 2)
		dh.mu.Unlock()
		if err != nil {
			return err
		}
		if en != nil {
			fuseLog.Debugf("Directory %v not empty: still has entry \"%v\"", *inode.FullName(), en.Name)
			return fuse.ENOTEMPTY
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
		}
		inode.Parent.addModified(inc)
	}
}

func (parent *Inode) addModified(inc int64) {
	for parent != nil {
		atomic.AddInt64(&parent.dir.ModifiedChildren, inc)
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
func (parent *Inode) Rename(from string, newParent *Inode, to string) (err error) {
	parent.logFuse("Rename", from, newParent.getChildName(to))

	fromCloud, fromPath := parent.cloud()
	toCloud, toPath := newParent.cloud()
	if fromCloud != toCloud {
		// cannot rename across cloud backend
		err = fuse.EINVAL
		return
	}

	// We rely on lookup() again, cache must be already populated here
	fromInode := parent.findChildUnlocked(from)
	toInode := newParent.findChildUnlocked(to)
	if fromInode == nil {
		return fuse.ENOENT
	}
	fromInode.mu.Lock()
	defer fromInode.mu.Unlock()
	if toInode != nil {
		toInode.mu.Lock()
		defer toInode.mu.Unlock()
		if fromInode.isDir() {
			if !toInode.isDir() {
				return fuse.ENOTDIR
			}
			toEmpty, err := toInode.isEmptyDir()
			if err != nil {
				return err
			}
			if !toEmpty {
				return fuse.ENOTEMPTY
			}
		} else if toInode.isDir() {
			return syscall.EISDIR
		}
	}

	fromFullName := appendChildName(fromPath, from)
	toFullName := appendChildName(toPath, to)

	if toInode != nil {
		// this file's been overwritten, it's
		// been detached but we can't delete
		// it just yet, because the kernel
		// will still send forget ops to us
		toInode.SetCacheState(ST_CACHED)
		newParent.removeChildUnlocked(toInode)
	}

	if fromInode.isDir() {
		fromFullName += "/"
		toFullName += "/"
		// List all objects and rename them in cache (keeping the lock)
		fromInode.dir.listDone = false
		fromInode.dir.slurpMarker = nil
		for !fromInode.dir.listDone {
			err := fromInode.listObjectsSlurp(false)
			if err != nil {
				return err
			}
		}
		renameRecursive(fromInode, newParent, to)
	} else {
		renameInCache(fromInode, newParent, to)
	}

	fromInode.fs.WakeupFlusher()

	return
}

func renameRecursive(fromInode *Inode, newParent *Inode, to string) {
	toDir := newParent.doMkDir(to)
	isNew := fromInode.CacheState == ST_CREATED && fromInode.IsFlushing == 0
	if isNew {
		// Brand new directory, not yet on the server, nothing to rename
	} else {
		toDir.SetCacheState(ST_MODIFIED)
		toDir.userMetadata = fromInode.userMetadata
		toDir.ImplicitDir = fromInode.ImplicitDir
		toDir.oldParent = fromInode.Parent
		toDir.oldName = fromInode.Name
	}
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
	// 2 is to skip . and ..
	for len(fromInode.dir.Children) > 2 {
		child := fromInode.dir.Children[2]
		child.mu.Lock()
		if child.isDir() {
			renameRecursive(child, toDir, *child.Name)
		} else {
			renameInCache(child, toDir, *child.Name)
		}
		child.mu.Unlock()
	}
	toDir.mu.Unlock()
	if isNew {
		// Unlink will be immediate
		fromInode.doUnlink()
	} else {
		// Removing the object will be done during the flush of rename
		parent := fromInode.Parent
		parent.removeChildUnlocked(fromInode)
		if parent.dir.DeletedChildren == nil {
			parent.dir.DeletedChildren = make(map[string]*Inode)
		}
		parent.dir.DeletedChildren[*fromInode.Name] = toDir
		parent.addModified(1)
	}
}

func renameInCache(fromInode *Inode, newParent *Inode, to string) {
	// There's a lot of edge cases with the asynchronous rename to handle:
	// 1) rename a new file => we can just upload it with the new name
	// 2) rename a new file that's already being flushed => rename after flush
	// 3) rename a modified file => rename after flush
	// 4) create a new file in place of a renamed one => don't flush until rename completes
	// 5) second rename while rename is already in progress => rename again after the first rename finishes
	// 6) rename then modify then rename => either rename then modify or modify then rename
	// and etc...
	parent := fromInode.Parent
	if (fromInode.CacheState != ST_CREATED || fromInode.IsFlushing > 0 || fromInode.mpu != nil) &&
		fromInode.oldParent == nil {
		// Remember that the original file is "deleted"
		// We can skip this step if the file is new and isn't being flushed yet
		if parent.dir.DeletedChildren == nil {
			parent.dir.DeletedChildren = make(map[string]*Inode)
		}
		parent.dir.DeletedChildren[*fromInode.Name] = fromInode
		if fromInode.CacheState == ST_CACHED {
			parent.addModified(1)
		}
		fromInode.oldParent = parent
		fromInode.oldName = fromInode.Name
	}
	fromInode.Name = &to
	fromInode.Parent = newParent
	if fromInode.CacheState == ST_CACHED {
		fromInode.SetCacheState(ST_MODIFIED)
	} else {
		newParent.addModified(1)
	}
	newParent.insertChildUnlocked(fromInode)
	parent.removeChildUnlocked(fromInode)
}

func RenameObject(cloud StorageBackend, fromFullName string, toFullName string, size *uint64) (err error) {
	_, err = cloud.RenameBlob(&RenameBlobInput{
		Source:      fromFullName,
		Destination: toFullName,
	})
	if err == nil || err != syscall.ENOTSUP {
		return
	}

	_, err = cloud.CopyBlob(&CopyBlobInput{
		Source:      fromFullName,
		Destination: toFullName,
		Size:        size,
	})
	if err != nil {
		return
	}

	_, err = cloud.DeleteBlob(&DeleteBlobInput{
		Key: fromFullName,
	})
	if err != nil {
		return
	}
	s3Log.Debugf("Deleted %v", fromFullName)

	return
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

// LOCKS_REQUIRED(fs.mu)
// LOCKS_REQUIRED(parent.mu)
// LOCKS_REQUIRED(parent.fs.mu)
func (parent *Inode) insertSubTree(path string, obj *BlobItemOutput, dirs map[*Inode]bool) {
	fs := parent.fs
	slash := strings.Index(path, "/")
	if slash == -1 {
		inode := parent.findChildUnlocked(path)
		if inode == nil {
			// don't revive deleted items
			_, deleted := parent.dir.DeletedChildren[path]
			if !deleted {
				inode = NewInode(fs, parent, &path)
				fs.insertInode(parent, inode)
				inode.SetFromBlobItem(obj)
			}
		} else {
			// our locking order is most specific lock
			// first, ie: lock a/b before a/. But here we
			// already have a/ and also global lock. For
			// new inode we don't care about that
			// violation because no one else will take
			// that lock anyway
			fs.mu.Unlock()
			parent.mu.Unlock()
			// don't update modified items
			if inode.CacheState == ST_CACHED {
				inode.SetFromBlobItem(obj)
			}
			parent.mu.Lock()
			fs.mu.Lock()
		}
		sealPastDirs(dirs, parent)
	} else {
		dir := path[:slash]
		path = path[slash+1:]

		if len(path) == 0 {
			inode := parent.findChildUnlocked(dir)
			if inode == nil {
				// don't revive deleted items
				_, deleted := parent.dir.DeletedChildren[path]
				if !deleted {
					inode = NewInode(fs, parent, &dir)
					inode.ToDir()
					fs.insertInode(parent, inode)
					inode.SetFromBlobItem(obj)
				}
			} else if inode.CacheState == ST_CACHED {
				// don't update modified items
				if !inode.isDir() {
					inode.ToDir()
					fs.addDotAndDotDot(inode)
				} else {
					fs.mu.Unlock()
					parent.mu.Unlock()
					inode.SetFromBlobItem(obj)
					parent.mu.Lock()
					fs.mu.Lock()
				}
			}
			sealPastDirs(dirs, inode)
		} else {
			// ensure that the potentially implicit dir is added
			inode := parent.findChildUnlocked(dir)
			if inode == nil {
				// don't revive deleted items
				_, deleted := parent.dir.DeletedChildren[path]
				if !deleted {
					inode = NewInode(fs, parent, &dir)
					inode.ToDir()
					fs.insertInode(parent, inode)
					now := time.Now()
					if inode.AttrTime.Before(now) {
						inode.AttrTime = now
					}
				}
			} else if inode.CacheState == ST_CACHED {
				// don't update modified items
				if !inode.isDir() {
					inode.ToDir()
					fs.addDotAndDotDot(inode)
				}
				now := time.Now()
				if inode.AttrTime.Before(now) {
					inode.AttrTime = now
				}
			}

			if inode.isDir() {
				// mark this dir but don't seal anything else
				// until we get to the leaf
				dirs[inode] = false

				fs.mu.Unlock()
				parent.mu.Unlock()
				inode.mu.Lock()
				fs.mu.Lock()
				inode.insertSubTree(path, obj, dirs)
				inode.mu.Unlock()
				fs.mu.Unlock()
				parent.mu.Lock()
				fs.mu.Lock()
			}
		}
	}
}

func (parent *Inode) findChildMaxTime() time.Time {
	maxTime := parent.Attributes.Mtime

	for i, c := range parent.dir.Children {
		if i < 2 {
			// skip . and ..
			continue
		}
		if c.Attributes.Mtime.After(maxTime) {
			maxTime = c.Attributes.Mtime
		}
	}

	return maxTime
}

func (parent *Inode) LookUpInodeNotDir(name string, c chan HeadBlobOutput, errc chan error) {
	cloud, key := parent.cloud()
	key = appendChildName(key, name)
	params := &HeadBlobInput{Key: key}
	resp, err := cloud.HeadBlob(params)
	if err != nil {
		errc <- mapAwsError(err)
		return
	}

	s3Log.Debug(resp)
	c <- *resp
}

func (parent *Inode) LookUpInodeDir(name string, c chan ListBlobsOutput, errc chan error) {
	cloud, key := parent.cloud()
	key = appendChildName(key, name) + "/"

	resp, err := cloud.ListBlobs(&ListBlobsInput{
		Delimiter: aws.String("/"),
		MaxKeys:   PUInt32(1),
		Prefix:    &key,
	})

	if err != nil {
		errc <- err
		return
	}

	s3Log.Debug(resp)
	c <- *resp
}

// returned inode has nil Id
func (parent *Inode) LookUpInodeMaybeDir(name string, fullName string) (inode *Inode, err error) {
	errObjectChan := make(chan error, 1)
	objectChan := make(chan HeadBlobOutput, 2)
	errDirBlobChan := make(chan error, 1)
	var errDirChan chan error
	var dirChan chan ListBlobsOutput

	checking := 3
	var checkErr [3]error

	cloud, _ := parent.cloud()
	if cloud == nil {
		panic("s3 disabled")
	}

	go parent.LookUpInodeNotDir(name, objectChan, errObjectChan)
	if !cloud.Capabilities().DirBlob && !parent.fs.flags.Cheap {
		go parent.LookUpInodeNotDir(name+"/", objectChan, errDirBlobChan)
		if !parent.fs.flags.ExplicitDir {
			errDirChan = make(chan error, 1)
			dirChan = make(chan ListBlobsOutput, 1)
			go parent.LookUpInodeDir(name, dirChan, errDirChan)
		}
	}

	for {
		select {
		case resp := <-objectChan:
			err = nil
			inode = NewInode(parent.fs, parent, &name)
			if !resp.IsDirBlob {
				// XXX/TODO if both object and object/ exists, return dir
				inode.SetFromBlobItem(&resp.BlobItemOutput)
			} else {
				inode.ToDir()
				if resp.LastModified != nil {
					inode.Attributes.Mtime = *resp.LastModified
				}
			}
			inode.fillXattrFromHead(&resp)
			return
		case err = <-errObjectChan:
			checking--
			checkErr[0] = err
			s3Log.Debugf("HEAD %v = %v", fullName, err)
		case resp := <-dirChan:
			err = nil
			if len(resp.Prefixes) != 0 || len(resp.Items) != 0 {
				inode = NewInode(parent.fs, parent, &name)
				inode.ToDir()
				if len(resp.Items) != 0 && *resp.Items[0].Key == name+"/" {
					// it's actually a dir blob
					entry := resp.Items[0]
					if entry.ETag != nil {
						inode.s3Metadata["etag"] = []byte(*entry.ETag)
					}
					if entry.StorageClass != nil {
						inode.s3Metadata["storage-class"] = []byte(*entry.StorageClass)
					}

				}
				// if cheap is not on, the dir blob
				// could exist but this returned first
				if inode.fs.flags.Cheap {
					inode.ImplicitDir = true
				}
				return
			} else {
				checkErr[2] = fuse.ENOENT
				checking--
			}
		case err = <-errDirChan:
			checking--
			checkErr[2] = err
			s3Log.Debugf("LIST %v/ = %v", fullName, err)
		case err = <-errDirBlobChan:
			checking--
			checkErr[1] = err
			s3Log.Debugf("HEAD %v/ = %v", fullName, err)
		}

		if cloud.Capabilities().DirBlob {
			return
		}

		switch checking {
		case 2:
			if parent.fs.flags.Cheap {
				go parent.LookUpInodeNotDir(name+"/", objectChan, errDirBlobChan)
			}
		case 1:
			if parent.fs.flags.ExplicitDir {
				checkErr[2] = fuse.ENOENT
				goto doneCase
			} else if parent.fs.flags.Cheap {
				errDirChan = make(chan error, 1)
				dirChan = make(chan ListBlobsOutput, 1)
				go parent.LookUpInodeDir(name, dirChan, errDirChan)
			}
			break
		doneCase:
			fallthrough
		case 0:
			for _, e := range checkErr {
				if e != fuse.ENOENT {
					err = e
					return
				}
			}

			err = fuse.ENOENT
			return
		}
	}
}
