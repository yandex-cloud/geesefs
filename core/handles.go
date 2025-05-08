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
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"hash"
	"io"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jacobsa/fuse/fuseops"

	"github.com/sirupsen/logrus"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

const (
	ST_CACHED   int32 = 0
	ST_DEAD     int32 = 1
	ST_CREATED  int32 = 2
	ST_MODIFIED int32 = 3
	ST_DELETED  int32 = 4
)

type NodeId uint64

type Joinable interface {
	Join(ctx context.Context) error
}

type MountedFS interface {
	Join(ctx context.Context) error
	Unmount() error
}

type InodeAttributes struct {
	Size  uint64
	Mtime time.Time
	Ctime time.Time
	Uid   uint32
	Gid   uint32
	Rdev  uint32
	Mode  os.FileMode
}

type ReadRange struct {
	Offset   uint64
	Size     uint64
	Flushing bool
}

type MPUPart struct {
	Num    uint32
	Offset uint64
	Size   uint64
	ETag   string
}

type StagedFile struct {
	FH          *FileHandle
	FD          *os.File
	mu          sync.Mutex
	lastWriteAt time.Time
	lastReadAt  time.Time
	shouldFlush bool
	flushing    bool
	debounce    time.Duration
}

func (stagedFile *StagedFile) ReadyToFlush() bool {
	stagedFile.mu.Lock()
	defer stagedFile.mu.Unlock()

	if stagedFile.flushing {
		return false
	}

	if time.Since(stagedFile.lastWriteAt) < stagedFile.debounce {
		return false
	}

	if time.Since(stagedFile.lastReadAt) < stagedFile.debounce {
		return false
	}

	return true
}

func (stagedFile *StagedFile) Cleanup() {
	log.Debugf("Cleaning up staged file: %s", stagedFile.FH.inode.FullName())

	stagedFile.mu.Lock()
	fh := stagedFile.FH
	fullPath := stagedFile.FD.Name()
	stagedFile.FD.Close()
	stagedFile.mu.Unlock()

	err := os.RemoveAll(fullPath)
	if err != nil {
		log.Warnf("Failed to remove staging file: %v", err)
	}

	if fh.inode.fs.flags.StagedWriteUploadCallback != nil {
		fh.inode.fs.flags.StagedWriteUploadCallback(fh.inode.FullName(), int64(fh.inode.Attributes.Size))
	}

	log.Debugf("Removed staged file: %s", fh.inode.FullName())
}

type Inode struct {
	Id         fuseops.InodeID
	Name       string
	fs         *Goofys
	Attributes InodeAttributes
	// It is generally safe to read `AttrTime` without locking because if some other
	// operation is modifying `AttrTime`, in most cases the reader is okay with working with
	// stale data. But Time is a struct and modifying it is not atomic. However
	// in practice (until the year 2157) we should be okay because
	// - Almost all uses of AttrTime will be about comparisons (AttrTime < x, AttrTime > x)
	// - Time object will have Time::monotonic bit set (until the year 2157) => the time
	//   comparision just compares Time::ext field
	// Ref: https://github.com/golang/go/blob/e42ae65a8507/src/time/time.go#L12:L56
	AttrTime   time.Time
	ExpireTime time.Time

	mu           sync.Mutex // everything below is protected by mu
	readCond     *sync.Cond
	pauseWriters int

	// We are not very consistent about enforcing locks for `Parent` because, the
	// parent field very very rarely changes and it is generally fine to operate on
	// stale parent information
	Parent *Inode

	dir *DirInodeData

	fileHandles  int32
	lastWriteEnd uint64

	// cached/buffered data
	CacheState     int32
	dirtyQueueId   uint64
	buffers        BufferList
	readRanges     []ReadRange
	DiskFDQueueID  uint64
	DiskCacheFD    *os.File
	StagedFile     *StagedFile
	OnDisk         bool
	forceFlush     bool
	IsFlushing     int
	flushError     error
	flushErrorTime time.Time
	readError      error
	// renamed from: parent, name
	oldParent *Inode
	oldName   string
	// is already being renamed to the current name
	renamingTo bool

	// multipart upload state
	mpu *MultipartBlobCommitInput

	userMetadataDirty int
	userMetadata      map[string][]byte
	s3Metadata        map[string][]byte

	// last known size and etag from the cloud
	knownSize uint64
	knownETag string

	// the refcnt is an exception, it's protected with atomic access
	// being part of parent.dir.Children increases refcnt by 1
	refcnt int64

	// Cluster Mode

	ownerMu    sync.RWMutex
	ownerTerm  uint64
	owner      NodeId
	readyOwner bool

	// hash state for multipart uploads
	hashLock         sync.Mutex
	hashInProgress   hash.Hash
	hashOffset       uint64
	pendingHashParts map[uint64][]byte // key: offset, value: data
}

func NewInode(fs *Goofys, parent *Inode, name string) (inode *Inode) {
	if strings.Index(name, "/") != -1 {
		fuseLog.Errorf("%v is not a valid name", name)
	}

	inode = &Inode{
		Name: name,
		fs:   fs,
		Attributes: InodeAttributes{
			Uid:  fs.flags.Uid,
			Gid:  fs.flags.Gid,
			Mode: fs.flags.FileMode,
		},
		AttrTime:   time.Now(),
		Parent:     parent,
		s3Metadata: make(map[string][]byte),
		refcnt:     0,
		StagedFile: nil,
	}

	inode.buffers.helpers = inode

	return
}

// For BufferListHelpers
func (inode *Inode) PartNum(offset uint64) uint64 {
	return inode.fs.partNum(offset)
}

// For BufferListHelpers
func (inode *Inode) QueueCleanBuffer(buf *FileBuffer) {
	inode.fs.cleanQueue.Add(inode, buf)
}

// For BufferListHelpers
func (inode *Inode) UnqueueCleanBuffer(buf *FileBuffer) {
	inode.fs.cleanQueue.Delete(buf)
}

// LOCKS_EXCLUDED(inode.mu)
func (inode *Inode) SetFromBlobItem(item *BlobItemOutput) {
	inode.mu.Lock()
	defer inode.mu.Unlock()

	// We always just drop our local cache when inode size or etag changes remotely
	// It's the simplest method of conflict resolution
	// Otherwise we may not be able to make a correct object version

	// If ongoing patch requests exist, then concurrent etag changes is normal. In current implementation
	// it is hard to reliably distinguish actual data conflicts from concurrent patch updates.
	patchInProgress := inode.fs.flags.UsePatch && inode.mpu == nil && inode.CacheState == ST_MODIFIED && inode.IsFlushing > 0

	// If a file is renamed from a different file then we also don't know its server-side
	// ETag or Size for sure, so the simplest fix is to also ignore this check
	renameInProgress := inode.oldName != ""

	if (item.ETag != nil && inode.knownETag != *item.ETag || item.Size != inode.knownSize) &&
		!patchInProgress && !renameInProgress {
		if inode.CacheState != ST_CACHED && (inode.knownETag != "" || inode.knownSize > 0) {
			s3Log.Warnf("Conflict detected (inode %v): server-side ETag or size of %v"+
				" (%v, %v) differs from local (%v, %v). File is changed remotely, dropping cache",
				inode.Id, inode.FullName(), NilStr(item.ETag), item.Size, inode.knownETag, inode.knownSize)
		}
		inode.resetCache()
		inode.Attributes.Size = item.Size
		inode.knownSize = item.Size
		if item.LastModified != nil {
			inode.Attributes.Mtime = *item.LastModified
			inode.Attributes.Ctime = *item.LastModified
		} else {
			inode.Attributes.Mtime = inode.fs.rootAttrs.Ctime
			inode.Attributes.Ctime = inode.fs.rootAttrs.Ctime
		}
		if item.Metadata != nil {
			inode.setMetadata(item.Metadata)
			inode.userMetadataDirty = 0
		}
	}
	if item.ETag != nil {
		inode.s3Metadata["etag"] = []byte(*item.ETag)
		inode.knownETag = *item.ETag
	} else {
		delete(inode.s3Metadata, "etag")
	}
	if item.StorageClass != nil {
		inode.s3Metadata["storage-class"] = []byte(*item.StorageClass)
	} else {
		delete(inode.s3Metadata, "storage-class")
	}
	now := time.Now()
	// don't want to update time if this inode is setup to never expire
	if inode.AttrTime.Before(now) {
		inode.SetAttrTime(now)
	}
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) cloud() (cloud StorageBackend, path string) {
	var prefix string
	var dir *Inode

	if inode.dir == nil {
		path = inode.Name
		dir = inode.Parent
	} else {
		dir = inode
	}

	for p := dir; p != nil; p = p.Parent {
		if p.dir.cloud != nil {
			cloud = p.dir.cloud
			// the error backend produces a mount.err file
			// at the root and is not aware of prefix
			_, isErr := cloud.(StorageBackendInitError)
			if !isErr {
				// we call init here instead of
				// relying on the wrapper to call init
				// because we want to return the right
				// prefix
				if c, ok := cloud.(*StorageBackendInitWrapper); ok {
					err := c.Init("")
					isErr = err != nil
				}
			}

			if !isErr {
				prefix = p.dir.mountPrefix
			}
			break
		}

		if path == "" {
			path = p.Name
		} else if p.Parent != nil {
			// don't prepend if I am already the root node
			path = p.Name + "/" + path
		}
	}

	if path == "" {
		path = strings.TrimRight(prefix, "/")
	} else {
		path = prefix + path
	}
	return
}

func (inode *Inode) FullName() string {
	if inode.Parent == nil {
		return inode.Name
	} else {
		return inode.Parent.getChildName(inode.Name)
	}
}

func (inode *Inode) touch() {
	inode.Attributes.Mtime = time.Now()
	inode.Attributes.Ctime = time.Now()
}

func (inode *Inode) InflateAttributes() (attr fuseops.InodeAttributes) {
	mtime := inode.Attributes.Mtime
	if mtime.IsZero() {
		mtime = inode.fs.rootAttrs.Mtime
	}

	attr = fuseops.InodeAttributes{
		Size:   inode.Attributes.Size,
		Atime:  inode.Attributes.Ctime,
		Mtime:  mtime,
		Ctime:  inode.Attributes.Ctime,
		Crtime: mtime,
		Uid:    inode.Attributes.Uid,
		Gid:    inode.Attributes.Gid,
		Mode:   inode.Attributes.Mode,
		Rdev:   inode.Attributes.Rdev,
	}

	if inode.dir != nil {
		attr.Nlink = 2
		attr.Mode = attr.Mode&os.ModePerm | os.ModeDir
	} else if inode.userMetadata != nil && inode.userMetadata[inode.fs.flags.SymlinkAttr] != nil {
		attr.Nlink = 1
		attr.Mode = attr.Mode&os.ModePerm | os.ModeSymlink
	} else {
		attr.Nlink = 1
	}

	return
}

func (inode *Inode) logFuse(op string, args ...interface{}) {
	if fuseLog.Level >= logrus.DebugLevel {
		fuseLog.Debugln(op, inode.Id, inode.FullName(), args)
	}
}

func (inode *Inode) errFuse(op string, args ...interface{}) {
	fuseLog.Errorln(op, inode.Id, inode.FullName(), args)
}

func (inode *Inode) ToDir() {
	if inode.dir == nil {
		inode.Attributes = InodeAttributes{
			Size: 4096,
			Uid:  inode.Attributes.Uid,
			Gid:  inode.Attributes.Gid,
			Mode: inode.fs.flags.DirMode | os.ModeDir,
			// Ctime, Mtime intentionally not initialized
		}
		inode.dir = &DirInodeData{
			lastOpenDirIdx: -1,
		}
	}
}

func (inode *Inode) Ref() {
	res := atomic.AddInt64(&inode.refcnt, 1)
	inode.logFuse("Ref", res)
	return
}

// LOCKS_REQUIRED(inode.mu)
// LOCKS_EXCLUDED(fs.mu)
func (inode *Inode) DeRef(n int64) (stale bool) {
	res := atomic.AddInt64(&inode.refcnt, -n)
	if res < 0 {
		fuseLog.Errorf("Deref underflow: deref inode %v (%v) by %v from %v", inode.Id, inode.FullName(), n, res+n)
		atomic.StoreInt64(&inode.refcnt, 0)
		res = 0
	} else {
		inode.logFuse("DeRef", n, res)
	}
	if res == 0 && inode.CacheState <= ST_DEAD {
		inode.resetCache()
		inode.fs.mu.Lock()
		inode.resetExpireTime()
		delete(inode.fs.inodes, inode.Id)
		inode.fs.forgotCnt += 1
		inode.fs.mu.Unlock()
	}
	return res == 0
}

// LOCKS_REQUIRED(inode.mu)
// LOCKS_EXCLUDED(inode.fs.mu)
func (inode *Inode) SetAttrTime(tm time.Time) {
	inode.AttrTime = tm
	// Expire when at least both AttrTime+TTL & ExpireTime pass
	// AttrTime is required for Windows where we don't use SetExpireTime()
	inode.SetExpireTime(tm.Add(inode.fs.flags.StatCacheTTL))
}

// LOCKS_REQUIRED(inode.mu)
// LOCKS_EXCLUDED(inode.fs.mu)
func (inode *Inode) SetExpireTime(tm time.Time) {
	// Only rewind expire time forward. I.e. it's more ExtendExpireTime than SetExpireTime
	if inode.ExpireTime.After(tm) {
		return
	}
	oldTime := inode.ExpireTime.Unix()
	newTime := tm.Unix()
	inode.ExpireTime = tm
	inode.fs.mu.Lock()
	oldMap := inode.fs.inodesByTime[oldTime]
	if oldMap != nil {
		delete(oldMap, inode.Id)
		if len(oldMap) == 0 {
			delete(inode.fs.inodesByTime, oldTime)
		}
	}
	if !tm.IsZero() {
		newMap := inode.fs.inodesByTime[newTime]
		if newMap == nil {
			newMap = make(map[fuseops.InodeID]bool)
			inode.fs.inodesByTime[newTime] = newMap
		}
		newMap[inode.Id] = true
	}
	inode.fs.mu.Unlock()
}

// LOCKS_REQUIRED(inode.mu)
// LOCKS_REQUIRED(inode.fs.mu)
func (inode *Inode) resetExpireTime() {
	oldTime := inode.ExpireTime.Unix()
	inode.ExpireTime = time.Time{}
	oldMap := inode.fs.inodesByTime[oldTime]
	if oldMap != nil {
		delete(oldMap, inode.Id)
		if len(oldMap) == 0 {
			delete(inode.fs.inodesByTime, oldTime)
		}
	}
}

// LOCKS_EXCLUDED(inode.mu)
// LOCKS_EXCLUDED(inode.fs.mu)
func (inode *Inode) SetExpireLocked(tm time.Time) {
	inode.mu.Lock()
	inode.SetExpireTime(tm)
	inode.mu.Unlock()
}

// LOCKS_EXCLUDED(inode.mu)
func (inode *Inode) GetAttributes() *fuseops.InodeAttributes {
	inode.mu.Lock()
	attr := inode.InflateAttributes()
	inode.mu.Unlock()
	return &attr
}

func (inode *Inode) isDir() bool {
	return inode.dir != nil
}

func RetryHeadBlob(flags *cfg.FlagStorage, cloud StorageBackend, req *HeadBlobInput) (resp *HeadBlobOutput, err error) {
	ReadBackoff(flags, func(attempt int) error {
		resp, err = cloud.HeadBlob(req)
		if err != nil && shouldRetry(err) {
			s3Log.Warnf("Error getting metadata of %v (attempt %v): %v\n", req.Key, attempt, err)
		}
		return err
	})
	return
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) fillXattrFromHead(resp *HeadBlobOutput) {
	if resp.ETag != nil {
		inode.s3Metadata["etag"] = []byte(*resp.ETag)
	}
	if resp.StorageClass != nil {
		inode.s3Metadata["storage-class"] = []byte(*resp.StorageClass)
	} else {
		inode.s3Metadata["storage-class"] = []byte("STANDARD")
	}

	inode.setMetadata(resp.Metadata)
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) setUserMeta(key string, value []byte) error {
	if inode.userMetadata == nil {
		if value == nil {
			return nil
		}
		err := inode.fillXattr()
		if err != nil {
			return err
		}
	}
	oldValue, exists := inode.userMetadata[key]
	if value == nil {
		if !exists {
			return nil
		}
		delete(inode.userMetadata, key)
	} else {
		if exists && bytes.Compare(oldValue, value) == 0 {
			return nil
		}
		inode.userMetadata[key] = value
	}
	inode.userMetadataDirty = 2
	return nil
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) setMetadata(metadata map[string]*string) {
	inode.userMetadata = unescapeMetadata(metadata)
	if inode.userMetadata != nil {
		if inode.fs.flags.EnableMtime {
			mtimeStr := inode.userMetadata[inode.fs.flags.MtimeAttr]
			if mtimeStr != nil {
				i, err := strconv.ParseUint(string(mtimeStr), 0, 64)
				if err == nil {
					inode.Attributes.Mtime = time.Unix(int64(i), 0)
				}
			}
		}
		if inode.fs.flags.EnablePerms {
			uidStr := inode.userMetadata[inode.fs.flags.UidAttr]
			if uidStr != nil {
				i, err := strconv.ParseUint(string(uidStr), 0, 32)
				if err == nil {
					inode.Attributes.Uid = uint32(i)
				}
			}
			gidStr := inode.userMetadata[inode.fs.flags.GidAttr]
			if gidStr != nil {
				i, err := strconv.ParseUint(string(gidStr), 0, 32)
				if err == nil {
					inode.Attributes.Gid = uint32(i)
				}
			}
		}
		if inode.fs.flags.EnablePerms || inode.fs.flags.EnableSpecials {
			modeStr := inode.userMetadata[inode.fs.flags.FileModeAttr]
			if modeStr != nil {
				i, err := strconv.ParseUint(string(modeStr), 0, 32)
				if err == nil {
					fm := fuseops.ConvertFileMode(uint32(i))
					var mask os.FileMode
					if inode.fs.flags.EnablePerms {
						mask = os.ModePerm
					}
					if inode.fs.flags.EnableSpecials && (inode.Attributes.Mode&os.ModeType) == 0 {
						mask = mask | os.ModeType
					}
					rmMask := (os.ModePerm | os.ModeType) ^ mask
					inode.Attributes.Mode = inode.Attributes.Mode&rmMask | (fm & mask)
					if (inode.Attributes.Mode & os.ModeDevice) != 0 {
						rdev, _ := strconv.ParseUint(string(inode.userMetadata[inode.fs.flags.RdevAttr]), 0, 32)
						inode.Attributes.Rdev = uint32(rdev)
					}
				}
			}
		}
	}
}

func (inode *Inode) setFileMode(newMode os.FileMode) (changed bool, err error) {
	prevMode := inode.Attributes.Mode
	if inode.fs.flags.EnableSpecials {
		if (newMode & os.ModeDir) != (inode.Attributes.Mode & os.ModeDir) {
			if (newMode & os.ModeDir) != 0 {
				return false, syscall.ENOTDIR
			} else {
				return false, syscall.EISDIR
			}
		}
		inode.Attributes.Mode = (inode.Attributes.Mode & os.ModePerm) | (newMode & os.ModeType)
	}
	if inode.fs.flags.EnablePerms {
		inode.Attributes.Mode = (inode.Attributes.Mode & os.ModeType) | (newMode & os.ModePerm)
	}
	changed = (prevMode != inode.Attributes.Mode)
	var defaultMode os.FileMode
	if inode.dir != nil {
		defaultMode = inode.fs.flags.DirMode | os.ModeDir
	} else {
		defaultMode = inode.fs.flags.FileMode
	}
	if (inode.Attributes.Mode & os.ModeDevice) != 0 {
		err = inode.setUserMeta(inode.fs.flags.RdevAttr, []byte(fmt.Sprintf("%d", inode.Attributes.Rdev)))
		if err != nil {
			return
		}
	}
	if inode.Attributes.Mode != defaultMode {
		err = inode.setUserMeta(inode.fs.flags.FileModeAttr, []byte(fmt.Sprintf("%d", fuseops.ConvertGoMode(inode.Attributes.Mode))))
	} else {
		err = inode.setUserMeta(inode.fs.flags.FileModeAttr, nil)
	}
	return
}

// FIXME: Move all these xattr-related functions to file.go

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) fillXattr() (err error) {
	if inode.userMetadata != nil {
		return nil
	}
	if inode.dir != nil && inode.dir.ImplicitDir {
		inode.userMetadata = make(map[string][]byte)
		return nil
	}
	cloud, key := inode.cloud()
	if inode.oldParent != nil {
		_, key = inode.oldParent.cloud()
		key = appendChildName(key, inode.oldName)
	}
	if inode.isDir() {
		key += "/"
	}
	inode.mu.Unlock()
	resp, err := RetryHeadBlob(inode.fs.flags, cloud, &HeadBlobInput{Key: key})
	inode.mu.Lock()
	if err != nil {
		err = mapAwsError(err)
		if err == syscall.ENOENT {
			err = nil
			if inode.isDir() {
				inode.dir.ImplicitDir = true
			}
		}
		return err
	} else if inode.userMetadata == nil {
		inode.fillXattrFromHead(resp)
	}
	return
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) getXattrMap(name string, userOnly bool) (
	meta map[string][]byte, newName string, err error) {

	cloud, _ := inode.cloud()
	xattrPrefix := cloud.Capabilities().Name + "."

	if strings.HasPrefix(name, xattrPrefix) {
		if userOnly {
			return nil, "", syscall.EPERM
		}

		newName = name[len(xattrPrefix):]
		meta = inode.s3Metadata
	} else if strings.HasPrefix(name, "user.") && name != "user."+inode.fs.flags.SymlinkAttr {
		err = inode.fillXattr()
		if err != nil {
			return nil, "", err
		}

		newName = name[5:]
		meta = inode.userMetadata
	} else {
		if userOnly {
			return nil, "", syscall.EPERM
		} else {
			return nil, "", ENOATTR
		}
	}

	if meta == nil {
		return nil, "", ENOATTR
	}

	return
}

func escapeMetadata(meta map[string][]byte) (metadata map[string]*string) {
	if meta == nil {
		return
	}
	metadata = make(map[string]*string)
	for k, v := range meta {
		k = strings.ToLower(xattrEscape(k))
		metadata[k] = PString(xattrEscape(string(v)))
	}
	return
}

func unescapeMetadata(meta map[string]*string) map[string][]byte {
	unescaped := make(map[string][]byte)
	for k, v := range meta {
		uk, err := url.PathUnescape(strings.ToLower(k))
		if err == nil {
			uv, err := url.PathUnescape(*v)
			if err == nil {
				unescaped[uk] = []byte(uv)
			}
		}
	}
	return unescaped
}

func (inode *Inode) SetXattr(name string, value []byte, flags uint32) error {
	inode.logFuse("SetXattr", name)

	if name == "debug" {
		inode.DumpTree(string(value) == "buffers")
		return nil
	}

	inode.mu.Lock()
	defer inode.mu.Unlock()

	if inode.CacheState == ST_DELETED || inode.CacheState == ST_DEAD {
		// Oops, it's a deleted file. We don't support changing invisible files
		return syscall.ENOENT
	}

	meta, name, err := inode.getXattrMap(name, true)
	if err == syscall.EPERM {
		// Silently ignore forbidden xattr operations
		return nil
	}
	if err != nil {
		return err
	}

	if flags != 0x0 {
		_, ok := meta[name]
		if flags == XATTR_CREATE {
			if ok {
				return syscall.EEXIST
			}
		} else if flags == XATTR_REPLACE {
			if !ok {
				return ENOATTR
			}
		}
	}

	meta[name] = Dup(value)
	inode.userMetadataDirty = 2
	if inode.CacheState == ST_CACHED {
		inode.SetCacheState(ST_MODIFIED)
		inode.fs.WakeupFlusher()
	}
	return nil
}

func (inode *Inode) RemoveXattr(name string) error {
	inode.logFuse("RemoveXattr", name)

	inode.mu.Lock()
	defer inode.mu.Unlock()

	if inode.CacheState == ST_DELETED || inode.CacheState == ST_DEAD {
		// Oops, it's a deleted file. We don't support changing invisible files
		return syscall.ENOENT
	}

	meta, name, err := inode.getXattrMap(name, true)
	if err == syscall.EPERM {
		// Silently ignore forbidden xattr operations
		return nil
	}
	if err != nil {
		return err
	}

	if _, ok := meta[name]; ok {
		delete(meta, name)
		inode.userMetadataDirty = 2
		if inode.CacheState == ST_CACHED {
			inode.SetCacheState(ST_MODIFIED)
			inode.fs.WakeupFlusher()
		}
		return err
	} else {
		return ENOATTR
	}
}

func (inode *Inode) GetXattr(name string) ([]byte, error) {
	inode.logFuse("GetXattr", name)
	if name == "geesefs" {
		return []byte(cfg.GEESEFS_VERSION), nil
	}

	inode.mu.Lock()
	defer inode.mu.Unlock()

	meta, name, err := inode.getXattrMap(name, false)
	if err != nil {
		return nil, err
	}

	value, ok := meta[name]
	if ok {
		return value, nil
	} else {
		return nil, ENOATTR
	}
}

func (inode *Inode) ListXattr() ([]string, error) {
	inode.mu.Lock()
	defer inode.mu.Unlock()

	var xattrs []string

	err := inode.fillXattr()
	if err != nil {
		return nil, err
	}

	cloud, _ := inode.cloud()
	cloudXattrPrefix := cloud.Capabilities().Name + "."

	for k, _ := range inode.s3Metadata {
		xattrs = append(xattrs, cloudXattrPrefix+k)
	}

	for k, _ := range inode.userMetadata {
		xattrs = append(xattrs, "user."+k)
	}

	sort.Strings(xattrs)

	return xattrs, nil
}

func (inode *Inode) OpenFile() (fh *FileHandle, err error) {
	inode.mu.Lock()
	defer inode.mu.Unlock()

	fh = NewFileHandle(inode)

	n := atomic.AddInt32(&inode.fileHandles, 1)
	if n == 1 {
		// This is done to try to protect directories with open files
		inode.Parent.addModified(1)
	}
	return
}

func (inode *Inode) DumpTree(withBuffers bool) {
	children := inode.DumpThis(withBuffers)
	for _, child := range children {
		child.DumpThis(withBuffers)
	}
}

func (inode *Inode) DumpThis(withBuffers bool) (children []*Inode) {
	inode.mu.Lock()
	defer inode.mu.Unlock()

	fs := inode.fs

	dataMap := make(map[string]interface{})
	dataMap["id"] = inode.Id
	dataMap["path"] = inode.FullName()
	if inode.CacheState == ST_DEAD {
		dataMap["state"] = "dead"
	} else if inode.CacheState == ST_CREATED {
		dataMap["state"] = "created"
	} else if inode.CacheState == ST_MODIFIED {
		dataMap["state"] = "modified"
	} else if inode.CacheState == ST_DELETED {
		dataMap["state"] = "deleted"
	}

	dataMap["size"] = inode.Attributes.Size
	dataMap["mtime"] = inode.Attributes.Mtime.Unix()
	dataMap["ctime"] = inode.Attributes.Ctime.Unix()
	if inode.Attributes.Uid != fs.flags.Uid {
		dataMap["uid"] = inode.Attributes.Uid
	}
	if inode.Attributes.Gid != fs.flags.Gid {
		dataMap["gid"] = inode.Attributes.Gid
	}
	if inode.Attributes.Rdev != 0 {
		dataMap["rdev"] = inode.Attributes.Rdev
	}
	if inode.isDir() && inode.Attributes.Mode != (os.ModeDir|fs.flags.DirMode) ||
		!inode.isDir() && inode.Attributes.Mode != fs.flags.FileMode {
		dataMap["mode"] = fuseops.ConvertGoMode(inode.Attributes.Mode)
	}

	dataMap["attrTime"] = inode.AttrTime.Unix()
	dataMap["expireTime"] = inode.ExpireTime.Unix()
	if inode.fileHandles != 0 {
		dataMap["fileHandles"] = inode.fileHandles
	}
	if inode.oldParent != nil {
		oldPath := inode.oldName
		if inode.oldParent.Id != fuseops.RootInodeID {
			oldPath = inode.oldParent.FullName() + "/" + oldPath
		}
		dataMap["oldPath"] = oldPath
		if inode.renamingTo {
			dataMap["renameStarted"] = true
		}
	}
	if len(inode.userMetadata) != 0 {
		dataMap["userMetadata"] = inode.userMetadata
	}
	if inode.userMetadataDirty != 0 {
		dataMap["userMetadataDirty"] = inode.userMetadataDirty
	}
	dataMap["knownSize"] = inode.knownSize
	dataMap["knownETag"] = inode.knownETag
	dataMap["refcnt"] = inode.refcnt
	if inode.pauseWriters != 0 {
		dataMap["pauseWriters"] = inode.pauseWriters
	}
	if inode.forceFlush {
		dataMap["forceFlush"] = true
	}
	if inode.IsFlushing != 0 {
		dataMap["flushing"] = inode.IsFlushing
	}
	if inode.OnDisk {
		dataMap["onDisk"] = inode.OnDisk
	}
	if inode.flushError != nil {
		dataMap["flushError"] = inode.flushError.Error()
		dataMap["flushErrorTime"] = inode.flushErrorTime.Unix()
	}
	if inode.readError != nil {
		dataMap["readError"] = inode.readError.Error()
	}

	if withBuffers && len(inode.readRanges) > 0 {
		var ranges [][]uint64
		for _, r := range inode.readRanges {
			fl := uint64(0)
			if r.Flushing {
				fl = 1
			}
			ranges = append(ranges, []uint64{fl, r.Offset, r.Size})
		}
		dataMap["readRanges"] = ranges
	}
	if withBuffers && inode.mpu != nil {
		var mpu []string
		for i := uint32(0); i < inode.mpu.NumParts; i++ {
			mpu = append(mpu, NilStr(inode.mpu.Parts[i]))
		}
		dataMap["uploadId"] = *inode.mpu.UploadId
		dataMap["uploadParts"] = mpu
	}

	if inode.isDir() {
		dirData := make(map[string]interface{})
		dirData["dirTime"] = inode.dir.DirTime.Unix()
		if inode.dir.ModifiedChildren != 0 {
			dirData["modifiedChildren"] = inode.dir.ModifiedChildren
		}
		if inode.dir.ImplicitDir {
			dirData["implicit"] = true
		}
		if inode.dir.listMarker != "" {
			dirData["listMarker"] = inode.dir.listMarker
		}
		if inode.dir.lastFromCloud != nil {
			dirData["lastFromCloud"] = *inode.dir.lastFromCloud
		}
		if !inode.dir.listDone {
			dirData["listing"] = true
		}
		if inode.dir.forgetDuringList {
			dirData["forgetDuringList"] = true
		}
		if inode.dir.lastOpenDirIdx != 0 {
			dirData["lastOpenDirIdx"] = inode.dir.lastOpenDirIdx
		}
		if inode.dir.seqOpenDirScore >= 2 {
			dirData["seqOpenDir"] = true
		}
		if !inode.dir.refreshStartTime.IsZero() {
			dirData["refreshStartTime"] = inode.dir.refreshStartTime.Unix()
		}
		if len(inode.dir.handles) > 0 {
			dirData["dirHandles"] = len(inode.dir.handles)
		}
		if len(inode.dir.Gaps) > 0 {
			var gaps []map[string]interface{}
			for _, gap := range inode.dir.Gaps {
				m := make(map[string]interface{})
				m["start"] = gap.start
				m["end"] = gap.end
				m["loadTime"] = gap.loadTime
			}
			dirData["gaps"] = gaps
		}
		if len(inode.dir.DeletedChildren) > 0 {
			var deletedNames []string
			for key := range inode.dir.DeletedChildren {
				deletedNames = append(deletedNames, key)
			}
			dirData["deletedChildren"] = deletedNames
		}
		dataMap["dir"] = dirData
		for _, child := range inode.dir.Children {
			children = append(children, child)
		}
	}

	dumpBuf, _ := json.Marshal(dataMap)
	dump := string(dumpBuf)
	if withBuffers && inode.buffers.Count() > 0 {
		b := inode.buffers.Dump(0, 0xffffffffffffffff)
		b = b[0 : len(b)-1]
		dump += "\n" + b
	}
	log.Error(dump)

	return children
}

func (inode *Inode) waitForHashToComplete(size uint64) (string, error) {
	timeout := time.After(inode.fs.flags.HashTimeout)

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	// Clean up
	defer func() {
		inode.hashInProgress = nil
		inode.pendingHashParts = nil
	}()

	for {
		select {
		case <-timeout:
			return "", fmt.Errorf("timeout waiting for hash to complete")
		case <-ticker.C:
			if inode.hashOffset == size && len(inode.pendingHashParts) == 0 {
				hash := hex.EncodeToString(inode.hashInProgress.Sum(nil))
				return hash, nil
			}
		}
	}
}

func (inode *Inode) hashFlushedPart(partOffset, partSize uint64) error {
	bufReader, _, err := inode.getMultiReader(partOffset, partSize)
	if err != nil {
		return err
	}
	data, err := io.ReadAll(bufReader)
	if err != nil {
		return err
	}

	inode.hashLock.Lock()
	defer inode.hashLock.Unlock()

	if inode.hashInProgress == nil {
		inode.hashInProgress = sha256.New()
		inode.hashOffset = 0
		inode.pendingHashParts = make(map[uint64][]byte)
	}

	if partOffset == inode.hashOffset {
		inode.hashInProgress.Write(data)
		inode.hashOffset += uint64(len(data))
		for {
			next, ok := inode.pendingHashParts[inode.hashOffset]
			if !ok {
				break
			}
			inode.hashInProgress.Write(next)
			inode.hashOffset += uint64(len(next))
			delete(inode.pendingHashParts, inode.hashOffset-uint64(len(next)))
		}
	} else {
		// out of order, buffer part until the next caller aligns with this offset
		inode.pendingHashParts[partOffset] = data
	}

	return nil
}
