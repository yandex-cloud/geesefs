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

package internal

import (
	"context"
	"fmt"
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
)

const (
	ST_CACHED int32 = 0
	ST_DEAD int32 = 1
	ST_CREATED int32 = 2
	ST_MODIFIED int32 = 3
	ST_DELETED int32 = 4
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
	Offset uint64
	Size uint64
	Flushing bool
}

type MPUPart struct {
	Num uint32
	Offset uint64
	Size uint64
	ETag string
}

const (
	// Buffer is clean
	BUF_CLEAN int16 = 1
	// Buffer is modified locally
	BUF_DIRTY int16 = 2
	// Buffer is flushed to the server as a full part, but multipart upload is not finalized yet
	BUF_FLUSHED_FULL int16 = 3
	// Buffer is flushed to the server as an undersized part
	// (and multipart upload is not finalized yet)
	BUF_FLUSHED_CUT int16 = 4
	// Buffer is flushed to the server and then removed from memory
	// (which is only possible for BUF_FLUSHED_FULL buffers)
	// (and multipart upload is not finalized yet)
	BUF_FL_CLEARED int16 = 5
)

type FileBuffer struct {
	offset uint64
	length uint64
	// Chunk state: 1 = clean. 2 = dirty. 3 = part flushed, but not finalized
	// 4 = flushed, not finalized, but removed from memory
	state int16
	// Loading from server or from disk
	loading bool
	// Latest chunk data is written to the disk cache
	onDisk bool
	// Chunk only contains zeroes, data and ptr are nil
	zero bool
	// Memory allocation recency counter
	recency uint64
	// Unmodified chunks (equal to the current server-side object state) have dirtyID = 0.
	// Every write or split assigns a new unique chunk ID.
	// Flusher tracks IDs that are currently being flushed to the server,
	// which allows to do flush and write in parallel.
	dirtyID uint64
	// Data
	data []byte
	ptr *BufferPointer
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
	AttrTime time.Time
	ExpireTime time.Time

	mu sync.Mutex // everything below is protected by mu
	readCond *sync.Cond
	pauseWriters int

	// We are not very consistent about enforcing locks for `Parent` because, the
	// parent field very very rarely changes and it is generally fine to operate on
	// stale parent information
	Parent *Inode

	dir *DirInodeData

	fileHandles int32
	lastWriteEnd uint64

	// cached/buffered data
	CacheState int32
	buffers []*FileBuffer
	readRanges []ReadRange
	DiskCacheFD *os.File
	OnDisk bool
	forceFlush bool
	IsFlushing int
	flushError error
	flushErrorTime time.Time
	readError error
	// renamed from: parent, name
	oldParent *Inode
	oldName string
	// is already being renamed to the current name
	renamingTo bool

	// multipart upload state
	mpu *MultipartBlobCommitInput

	userMetadataDirty int
	userMetadata map[string][]byte
	s3Metadata   map[string][]byte

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
}

func NewInode(fs *Goofys, parent *Inode, name string) (inode *Inode) {
	if strings.Index(name, "/") != -1 {
		fuseLog.Errorf("%v is not a valid name", name)
	}

	inode = &Inode{
		Name:       name,
		fs:         fs,
		Attributes: InodeAttributes{
			Uid:    fs.flags.Uid,
			Gid:    fs.flags.Gid,
			Mode:   fs.flags.FileMode,
		},
		AttrTime:   time.Now(),
		Parent:     parent,
		s3Metadata: make(map[string][]byte),
		refcnt:     0,
	}

	return
}

// LOCKS_EXCLUDED(inode.mu)
func (inode *Inode) SetFromBlobItem(item *BlobItemOutput) {
	inode.mu.Lock()
	defer inode.mu.Unlock()

	// We always just drop our local cache when inode size or etag changes remotely
	// It's the simplest method of conflict resolution
	// Otherwise we may not be able to make a correct object version
	if item.ETag != nil && inode.knownETag != *item.ETag || item.Size != inode.knownSize {
		if inode.CacheState != ST_CACHED && (inode.knownETag != "" || inode.knownSize > 0) {
			s3Log.Warnf("Conflict detected (inode %v): server-side ETag or size of %v"+
				" (%v, %v) differs from local (%v, %v). File is changed remotely, dropping cache",
				inode.Id, inode.FullName(), NilStr(item.ETag), item.Size, inode.knownETag, inode.knownSize)
		}
		inode.resetCache()
		inode.ResizeUnlocked(item.Size, false, false)
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
		attr.Mode = attr.Mode & os.ModePerm | os.ModeDir
	} else if inode.userMetadata != nil && inode.userMetadata[inode.fs.flags.SymlinkAttr] != nil {
		attr.Nlink = 1
		attr.Mode = attr.Mode & os.ModePerm | os.ModeSymlink
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
		delete(inode.fs.inodes, inode.Id)
		inode.fs.forgotCnt += 1
		inode.fs.mu.Unlock()
		// Remove from LFRU tracker
		inode.fs.lfru.Forget(inode.Id)
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
func (inode *Inode) setUserMeta(key string, value []byte) {
	if inode.userMetadata == nil {
		inode.userMetadata = make(map[string][]byte)
	}
	if value == nil {
		delete(inode.userMetadata, key)
	} else {
		inode.userMetadata[key] = value
	}
	inode.userMetadataDirty = 2
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
					if inode.fs.flags.EnableSpecials && (inode.Attributes.Mode & os.ModeType) == 0 {
						mask = mask | os.ModeType
					}
					rmMask := (os.ModePerm | os.ModeType) ^ mask
					inode.Attributes.Mode = inode.Attributes.Mode & rmMask | (fm & mask)
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
	var defaultMode os.FileMode
	if inode.dir != nil {
		defaultMode = inode.fs.flags.DirMode | os.ModeDir
	} else {
		defaultMode = inode.fs.flags.FileMode
	}
	if (inode.Attributes.Mode & os.ModeDevice) != 0 {
		inode.setUserMeta(inode.fs.flags.RdevAttr, []byte(fmt.Sprintf("%d", inode.Attributes.Rdev)))
	}
	if inode.Attributes.Mode != defaultMode {
		inode.setUserMeta(inode.fs.flags.FileModeAttr, []byte(fmt.Sprintf("%d", fuseops.ConvertGolangMode(inode.Attributes.Mode))))
	} else {
		inode.setUserMeta(inode.fs.flags.FileModeAttr, nil)
	}
	return prevMode != inode.Attributes.Mode, nil
}

// FIXME: Move all these xattr-related functions to file.go

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) fillXattr() (err error) {
	if inode.userMetadata == nil && (inode.dir == nil || !inode.dir.ImplicitDir) {
		cloud, key := inode.cloud()
		if inode.oldParent != nil {
			_, key = inode.oldParent.cloud()
			key = appendChildName(key, inode.oldName)
		}
		if inode.isDir() {
			key += "/"
		}
		inode.mu.Unlock()
		resp, err := cloud.HeadBlob(&HeadBlobInput{Key: key})
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
