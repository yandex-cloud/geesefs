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
	. "github.com/yandex-cloud/geesefs/api/common"

	"context"
	"fmt"
	"math/rand"
	"net/url"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/sirupsen/logrus"
	"net/http"
)

// goofys is a Filey System written in Go. All the backend data is
// stored on S3 as is. It's a Filey System instead of a File System
// because it makes minimal effort at being POSIX
// compliant. Particularly things that are difficult to support on S3
// or would translate into more than one round-trip would either fail
// (rename non-empty dir) or faked (no per-file permission). goofys
// does not have a on disk data cache, and consistency model is
// close-to-open.

type Goofys struct {
	fuseutil.NotImplementedFileSystem
	bucket string

	flags *FlagStorage

	umask uint32

	gcs       bool
	rootAttrs InodeAttributes

	bufferPool *BufferPool

	// A lock protecting the state of the file system struct itself (distinct
	// from per-inode locks). Make sure to see the notes on lock ordering above.
	mu sync.RWMutex

	flusherMu sync.Mutex
	flusherCond *sync.Cond
	flushPending int32

	// The next inode ID to hand out. We assume that this will never overflow,
	// since even if we were handing out inode IDs at 4 GHz, it would still take
	// over a century to do so.
	//
	// GUARDED_BY(mu)
	nextInodeID fuseops.InodeID

	// The collection of live inodes, keyed by inode ID. No ID less than
	// fuseops.RootInodeID is ever used.
	//
	// INVARIANT: For all keys k, fuseops.RootInodeID <= k < nextInodeID
	// INVARIANT: For all keys k, inodes[k].ID() == k
	// INVARIANT: inodes[fuseops.RootInodeID] is missing or of type inode.DirInode
	// INVARIANT: For all v, if IsDirName(v.Name()) then v is inode.DirInode
	//
	// GUARDED_BY(mu)
	inodes map[fuseops.InodeID]*Inode

	nextHandleID fuseops.HandleID
	dirHandles   map[fuseops.HandleID]*DirHandle

	fileHandles map[fuseops.HandleID]*FileHandle

	activeFlushers int64
	flushRetrySet int32

	forgotCnt uint32

	zeroBuf []byte
	lfru *LFRU
	diskFdMu sync.Mutex
	diskFdCond *sync.Cond
	diskFdCount int64
}

var s3Log = GetLogger("s3")
var log = GetLogger("main")
var fuseLog = GetLogger("fuse")

func NewBackend(bucket string, flags *FlagStorage) (cloud StorageBackend, err error) {
	if flags.Backend == nil {
		flags.Backend = (&S3Config{}).Init()
	}

	if config, ok := flags.Backend.(*AZBlobConfig); ok {
		cloud, err = NewAZBlob(bucket, config)
	} else if config, ok := flags.Backend.(*ADLv1Config); ok {
		cloud, err = NewADLv1(bucket, flags, config)
	} else if config, ok := flags.Backend.(*ADLv2Config); ok {
		cloud, err = NewADLv2(bucket, flags, config)
	} else if config, ok := flags.Backend.(*S3Config); ok {
		if strings.HasSuffix(flags.Endpoint, "/storage.googleapis.com") {
			cloud, err = NewGCS3(bucket, flags, config)
		} else {
			cloud, err = NewS3(bucket, flags, config)
		}
	} else {
		err = fmt.Errorf("Unknown backend config: %T", flags.Backend)
	}

	return
}

type BucketSpec struct {
	Scheme string
	Bucket string
	Prefix string
}

func ParseBucketSpec(bucket string) (spec BucketSpec, err error) {
	if strings.Index(bucket, "://") != -1 {
		var u *url.URL
		u, err = url.Parse(bucket)
		if err != nil {
			return
		}

		spec.Scheme = u.Scheme
		spec.Bucket = u.Host
		if u.User != nil {
			// wasb url can be wasb://container@storage-end-point
			// we want to return the entire thing as bucket
			spec.Bucket = u.User.String() + "@" + u.Host
		}
		spec.Prefix = u.Path
	} else {
		spec.Scheme = "s3"

		colon := strings.Index(bucket, ":")
		if colon != -1 {
			spec.Prefix = bucket[colon+1:]
			spec.Bucket = bucket[0:colon]
		} else {
			spec.Bucket = bucket
		}
	}

	spec.Prefix = strings.Trim(spec.Prefix, "/")
	if spec.Prefix != "" {
		spec.Prefix += "/"
	}
	return
}

func NewGoofys(ctx context.Context, bucket string, flags *FlagStorage) *Goofys {
	return newGoofys(ctx, bucket, flags, NewBackend)
}

func newGoofys(ctx context.Context, bucket string, flags *FlagStorage,
	newBackend func(string, *FlagStorage) (StorageBackend, error)) *Goofys {
	// Set up the basic struct.
	fs := &Goofys{
		bucket: bucket,
		flags:  flags,
		umask:  0122,
		lfru:   NewLFRU(flags.CachePopularThreshold, flags.CacheMaxHits, flags.CacheAgeInterval, flags.CacheAgeDecrement),
		zeroBuf: make([]byte, 1048576),
	}

	var prefix string
	colon := strings.Index(bucket, ":")
	if colon != -1 {
		prefix = bucket[colon+1:]
		prefix = strings.Trim(prefix, "/")
		if prefix != "" {
			prefix += "/"
		}

		fs.bucket = bucket[0:colon]
		bucket = fs.bucket
	}

	if flags.DebugS3 {
		s3Log.Level = logrus.DebugLevel
	}

	cloud, err := newBackend(bucket, flags)
	if err != nil {
		log.Errorf("Unable to setup backend: %v", err)
		return nil
	}
	_, fs.gcs = cloud.Delegate().(*GCS3)

	randomObjectName := prefix + (RandStringBytesMaskImprSrc(32))
	err = cloud.Init(randomObjectName)
	if err != nil {
		log.Errorf("Unable to access '%v': %v", bucket, err)
		return nil
	}
	go cloud.MultipartExpire(&MultipartExpireInput{})

	now := time.Now()
	fs.rootAttrs = InodeAttributes{
		Size:  4096,
		Mtime: now,
	}

	fs.bufferPool = NewBufferPool(int64(flags.MemoryLimit))
	fs.bufferPool.FreeSomeCleanBuffers = func(size int64) (int64, bool) {
		return fs.FreeSomeCleanBuffers(size)
	}

	fs.nextInodeID = fuseops.RootInodeID + 1
	fs.inodes = make(map[fuseops.InodeID]*Inode)
	root := NewInode(fs, nil, "")
	root.refcnt = 1
	root.Id = fuseops.RootInodeID
	root.ToDir()
	root.dir.cloud = cloud
	root.dir.mountPrefix = prefix
	root.userMetadata = make(map[string][]byte)
	root.Attributes.Mtime = fs.rootAttrs.Mtime

	fs.inodes[fuseops.RootInodeID] = root
	fs.addDotAndDotDot(root)

	fs.nextHandleID = 1
	fs.dirHandles = make(map[fuseops.HandleID]*DirHandle)

	fs.fileHandles = make(map[fuseops.HandleID]*FileHandle)

	fs.flusherCond = sync.NewCond(&fs.flusherMu)
	go fs.Flusher()

	if fs.flags.CachePath != "" && fs.flags.MaxDiskCacheFD > 0 {
		fs.diskFdCond = sync.NewCond(&fs.diskFdMu)
		go fs.FDCloser()
	}

	return fs
}

// from https://stackoverflow.com/questions/22892120/how-to-generate-a-random-string-of-a-fixed-length-in-golang
func RandStringBytesMaskImprSrc(n int) string {
	const letterBytes = "abcdefghijklmnopqrstuvwxyz0123456789"
	const (
		letterIdxBits = 6                    // 6 bits to represent a letter index
		letterIdxMask = 1<<letterIdxBits - 1 // All 1-bits, as many as letterIdxBits
		letterIdxMax  = 63 / letterIdxBits   // # of letter indices fitting in 63 bits
	)
	src := rand.NewSource(time.Now().UnixNano())
	b := make([]byte, n)
	// A src.Int63() generates 63 random bits, enough for letterIdxMax characters!
	for i, cache, remain := n-1, src.Int63(), letterIdxMax; i >= 0; {
		if remain == 0 {
			cache, remain = src.Int63(), letterIdxMax
		}
		if idx := int(cache & letterIdxMask); idx < len(letterBytes) {
			b[i] = letterBytes[idx]
			i--
		}
		cache >>= letterIdxBits
		remain--
	}

	return string(b)
}

func (fs *Goofys) SigUsr1() {
	fs.mu.RLock()

	log.Infof("forgot %v inodes", fs.forgotCnt)
	log.Infof("%v inodes", len(fs.inodes))
	fs.mu.RUnlock()
	debug.FreeOSMemory()
}

// Find the given inode. Panic if it doesn't exist.
//
// RLOCKS_REQUIRED(fs.mu)
func (fs *Goofys) getInodeOrDie(id fuseops.InodeID) (inode *Inode) {
	inode = fs.inodes[id]
	if inode == nil {
		panic(fmt.Sprintf("Unknown inode: %v", id))
	}

	return
}

// Close unneeded cache FDs
func (fs *Goofys) FDCloser() {
	fs.diskFdMu.Lock()
	for {
		rmFdItem := fs.lfru.Pick(nil)
		for fs.flags.MaxDiskCacheFD > 0 && fs.diskFdCount > fs.flags.MaxDiskCacheFD {
			for rmFdItem != nil {
				fs.mu.RLock()
				rmFdInode := fs.inodes[rmFdItem.Id()]
				fs.mu.RUnlock()
				if rmFdInode != nil {
					rmFdInode.mu.Lock()
					if rmFdInode.DiskCacheFD != nil {
						rmFdInode.DiskCacheFD.Close()
						rmFdInode.DiskCacheFD = nil
						fs.diskFdCount--
						break
					}
					rmFdInode.mu.Unlock()
				}
				rmFdItem = fs.lfru.Pick(rmFdItem)
			}
		}
		fs.diskFdCond.Wait()
	}
	fs.diskFdMu.Unlock()
}

// Try to reclaim some clean buffers
func (fs *Goofys) FreeSomeCleanBuffers(size int64) (int64, bool) {
	freed := int64(0)
	haveDirty := false
	var cacheItem *LFRUItem
	for {
		cacheItem = fs.lfru.Pick(cacheItem)
		if cacheItem == nil {
			break
		}
		inodeId := cacheItem.Id()
		fs.mu.RLock()
		inode := fs.inodes[inodeId]
		fs.mu.RUnlock()
		if inode == nil {
			continue
		}
		toFs := -1
		inode.mu.Lock()
		del := -1
		for i := 0; i < len(inode.buffers); i++ {
			buf := inode.buffers[i]
			if buf.dirtyID == 0 || buf.state == BUF_FLUSHED {
				if freed < size && buf.ptr != nil && !inode.IsRangeLocked(buf.offset, buf.length, false) {
					if fs.flags.CachePath != "" && !buf.onDisk {
						if toFs == -1 {
							toFs = 0
							if fs.lfru.GetHits(inode.Id) >= fs.flags.CacheToDiskHits {
								toFs = 1
							}
						}
						if toFs > 0 {
							// Evict to disk
							err := inode.OpenCacheFD()
							if err != nil {
								toFs = 0
							} else {
								_, err := inode.DiskCacheFD.WriteAt(buf.data, int64(buf.offset))
								if err != nil {
									toFs = 0
									log.Errorf("Couldn't write %v bytes at offset %v to %v: %v",
										len(buf.data), buf.offset, fs.flags.CachePath+"/"+inode.FullName(), err)
								} else {
									buf.onDisk = true
								}
							}
						}
					}
					// Release memory
					buf.ptr.refs--
					if buf.ptr.refs == 0 {
						freed += int64(len(buf.ptr.mem))
						fs.bufferPool.UseUnlocked(-int64(len(buf.ptr.mem)), false)
					}
					buf.ptr = nil
					buf.data = nil
					if buf.dirtyID == 0 && !buf.onDisk {
						if del == -1 {
							del = i
						}
						continue
					} else if buf.state == BUF_FLUSHED {
						// A flushed buffer can be removed at a cost of finalizing multipart upload
						// to read it back later. However it's likely not a problem if we're uploading
						// a large file because we may never need to read it back.
						prev := del
						if prev < 0 {
							prev = i-1
						}
						if prev > 0 && inode.buffers[prev].state == BUF_FL_CLEARED &&
							buf.offset == (inode.buffers[prev].offset + inode.buffers[prev].length) {
							inode.buffers[prev].length += buf.length
							if del == -1 {
								del = i
							}
							continue
						} else {
							buf.state = BUF_FL_CLEARED
						}
					}
				}
			} else {
				haveDirty = true
			}
			if del >= 0 {
				inode.buffers = append(inode.buffers[0 : del], inode.buffers[i : ]...)
				i = del
				del = -1
			}
		}
		if del >= 0 {
			inode.buffers = inode.buffers[0 : del]
			del = -1
		}
		inode.mu.Unlock()
		if freed >= size {
			break
		}
	}
	if haveDirty {
		fs.WakeupFlusher()
	}
	return freed, haveDirty
}

func (fs *Goofys) WakeupFlusher() {
	fs.flusherMu.Lock()
	if fs.flushPending == 0 {
		fs.flushPending = 1
		fs.flusherCond.Broadcast()
	}
	fs.flusherMu.Unlock()
}

// Flusher goroutine.
// Overall algorithm:
// 1) File opened => reads and writes just populate cache
// 2) File closed => flush it
//    Created or fully overwritten =>
//      Less than 5 MB => upload in a single part
//      More than 5 MB => upload using multipart
//    Updated =>
//      CURRENTLY:
//      Less than 5 MB => upload in a single part
//      More than 5 MB => update using multipart copy
//      Also we can't update less than 5 MB because it's the minimal part size
// 3) Fsync triggered => intermediate full flush (same algorithm)
// 4) Dirty memory limit reached => without on-disk cache we have to flush the whole object.
//    With on-disk cache we can unload some dirty buffers to disk.
func (fs *Goofys) Flusher() {
	var inodes []fuseops.InodeID
	again := false
	for true {
		if !again {
			fs.flusherMu.Lock()
			if fs.flushPending == 0 {
				fs.flusherCond.Wait()
			}
			fs.flushPending = 0
			fs.flusherMu.Unlock()
			// Repeat one more time after wakeup to scan all inodes
			again = true
		}
		if atomic.LoadInt64(&fs.activeFlushers) < fs.flags.MaxFlushers {
			if len(inodes) == 0 {
				again = false
				fs.mu.RLock()
				inodes = make([]fuseops.InodeID, 0, len(fs.inodes))
				for id := range fs.inodes {
					inodes = append(inodes, id)
				}
				fs.mu.RUnlock()
			}
			for len(inodes) > 0 {
				// pop id
				id := inodes[len(inodes)-1]
				inodes = inodes[0 : len(inodes)-1]
				fs.mu.RLock()
				inode := fs.inodes[id]
				fs.mu.RUnlock()
				if inode != nil {
					inode.TryFlush()
					if atomic.LoadInt64(&fs.activeFlushers) >= fs.flags.MaxFlushers {
						break
					}
				}
			}
		} else {
			again = false
		}
	}
}

type Mount struct {
	// Mount Point relative to goofys's root mount.
	name    string
	cloud   StorageBackend
	prefix  string
	mounted bool
}

func (fs *Goofys) mount(mp *Inode, b *Mount) {
	if b.mounted {
		return
	}

	name := strings.Trim(b.name, "/")

	// create path for the mount. AttrTime is set to TIME_MAX so
	// they will never expire and be removed. But DirTime is not
	// so we will still consult the underlining cloud for listing
	// (which will then be merged with the cached result)

	for {
		idx := strings.Index(name, "/")
		if idx == -1 {
			break
		}
		dirName := name[0:idx]
		name = name[idx+1:]

		mp.mu.Lock()
		dirInode := mp.findChildUnlocked(dirName)
		if dirInode == nil {
			fs.mu.Lock()

			dirInode = NewInode(fs, mp, dirName)
			dirInode.ToDir()
			dirInode.AttrTime = TIME_MAX
			dirInode.userMetadata = make(map[string][]byte)

			fs.insertInode(mp, dirInode)
			fs.mu.Unlock()
		}
		mp.mu.Unlock()
		mp = dirInode
	}

	mp.mu.Lock()
	defer mp.mu.Unlock()

	prev := mp.findChildUnlocked(name)
	if prev == nil {
		mountInode := NewInode(fs, mp, name)
		mountInode.ToDir()
		mountInode.dir.cloud = b.cloud
		mountInode.dir.mountPrefix = b.prefix
		mountInode.AttrTime = TIME_MAX
		mountInode.userMetadata = make(map[string][]byte)

		fs.mu.Lock()
		defer fs.mu.Unlock()

		fs.insertInode(mp, mountInode)
		prev = mountInode
	} else {
		if !prev.isDir() {
			panic(fmt.Sprintf("inode %v is not a directory", prev.FullName()))
		}

		// This inode might have some cached data from a parent mount.
		// Clear this cache by resetting the DirTime.
		// Note: resetDirTimeRec should be called without holding the lock.
		prev.resetDirTimeRec()
		prev.mu.Lock()
		defer prev.mu.Unlock()
		prev.dir.cloud = b.cloud
		prev.dir.mountPrefix = b.prefix
		prev.AttrTime = TIME_MAX

	}
	prev.addModified(1)
	fuseLog.Infof("mounted /%v", prev.FullName())
	b.mounted = true
}

func (fs *Goofys) MountAll(mounts []*Mount) {
	fs.mu.RLock()
	root := fs.getInodeOrDie(fuseops.RootInodeID)
	fs.mu.RUnlock()

	for _, m := range mounts {
		fs.mount(root, m)
	}
}

func (fs *Goofys) Mount(mount *Mount) {
	fs.mu.RLock()
	root := fs.getInodeOrDie(fuseops.RootInodeID)
	fs.mu.RUnlock()
	fs.mount(root, mount)
}

func (fs *Goofys) Unmount(mountPoint string) {
	fs.mu.RLock()
	mp := fs.getInodeOrDie(fuseops.RootInodeID)
	fs.mu.RUnlock()

	fuseLog.Infof("Attempting to unmount %v", mountPoint)
	path := strings.Split(strings.Trim(mountPoint, "/"), "/")
	for _, localName := range path {
		dirInode := mp.findChild(localName)
		if dirInode == nil || !dirInode.isDir() {
			fuseLog.Errorf("Failed to find directory:%v while unmounting %v. "+
				"Ignoring the unmount operation.", localName, mountPoint)
			return
		}
		mp = dirInode
	}
	mp.addModified(-1)
	mp.ResetForUnmount()
	return
}

func (fs *Goofys) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {

	const BLOCK_SIZE = 4096
	const TOTAL_SPACE = 1 * 1024 * 1024 * 1024 * 1024 * 1024 // 1PB
	const TOTAL_BLOCKS = TOTAL_SPACE / BLOCK_SIZE
	const INODES = 1 * 1000 * 1000 * 1000 // 1 billion
	op.BlockSize = BLOCK_SIZE
	op.Blocks = TOTAL_BLOCKS
	op.BlocksFree = TOTAL_BLOCKS
	op.BlocksAvailable = TOTAL_BLOCKS
	op.IoSize = 1 * 1024 * 1024 // 1MB
	op.Inodes = INODES
	op.InodesFree = INODES
	return
}

func (fs *Goofys) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	attr, err := inode.GetAttributes()
	err = mapAwsError(err)
	if err == nil {
		op.Attributes = *attr
		op.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	}

	return
}

func (fs *Goofys) GetXattr(ctx context.Context,
	op *fuseops.GetXattrOp) (err error) {
	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	value, err := inode.GetXattr(op.Name)
	err = mapAwsError(err)
	if err != nil {
		return err
	}

	op.BytesRead = len(value)

	if len(op.Dst) != 0 {
		if len(op.Dst) < op.BytesRead {
			return syscall.ERANGE
		}

		copy(op.Dst, value)
	}
	return
}

func (fs *Goofys) ListXattr(ctx context.Context,
	op *fuseops.ListXattrOp) (err error) {
	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	xattrs, err := inode.ListXattr()
	err = mapAwsError(err)

	ncopied := 0

	for _, name := range xattrs {
		buf := op.Dst[ncopied:]
		nlen := len(name) + 1

		if nlen <= len(buf) {
			copy(buf, name)
			ncopied += nlen
			buf[nlen-1] = '\x00'
		}

		op.BytesRead += nlen
	}

	if len(op.Dst) != 0 && ncopied < op.BytesRead {
		err = syscall.ERANGE
	}

	return
}

func (fs *Goofys) RemoveXattr(ctx context.Context,
	op *fuseops.RemoveXattrOp) (err error) {
	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	err = inode.RemoveXattr(op.Name)
	err = mapAwsError(err)
	if err == syscall.EPERM {
		// Silently ignore forbidden xattr operations
		err = nil
	}

	return
}

func (fs *Goofys) SetXattr(ctx context.Context,
	op *fuseops.SetXattrOp) (err error) {
	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	err = inode.SetXattr(op.Name, op.Value, op.Flags)
	err = mapAwsError(err)
	if err == syscall.EPERM {
		// Silently ignore forbidden xattr operations
		err = nil
	}
	return
}

func (fs *Goofys) CreateSymlink(ctx context.Context,
	op *fuseops.CreateSymlinkOp) (err error) {
	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	inode := parent.CreateSymlink(op.Name, op.Target)
	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	return
}

func (fs *Goofys) ReadSymlink(ctx context.Context,
	op *fuseops.ReadSymlinkOp) (err error) {
	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	op.Target, err = inode.ReadSymlink()
	err = mapAwsError(err)
	return
}

func mapHttpError(status int) error {
	switch status {
	case 400:
		return fuse.EINVAL
	case 401:
		return syscall.EACCES
	case 403:
		return syscall.EACCES
	case 404:
		return fuse.ENOENT
	case 405:
		return syscall.ENOTSUP
	case http.StatusConflict:
		return syscall.EINTR
	case http.StatusRequestedRangeNotSatisfiable:
		return syscall.ERANGE
	case 429:
		return syscall.EAGAIN
	case 500:
		return syscall.EAGAIN
	default:
		return nil
	}
}

func mapAwsError(err error) error {
	if err == nil {
		return nil
	}

	if awsErr, ok := err.(awserr.Error); ok {
		switch awsErr.Code() {
		case "BucketRegionError":
			// don't need to log anything, we should detect region after
			return err
		case "NoSuchBucket":
			return syscall.ENXIO
		case "BucketAlreadyOwnedByYou":
			return fuse.EEXIST
		}

		if reqErr, ok := err.(awserr.RequestFailure); ok {
			// A service error occurred
			err = mapHttpError(reqErr.StatusCode())
			if err != nil {
				return err
			} else {
				s3Log.Errorf("http=%v %v s3=%v request=%v\n",
					reqErr.StatusCode(), reqErr.Message(),
					awsErr.Code(), reqErr.RequestID())
				return reqErr
			}
		} else {
			// Generic AWS Error with Code, Message, and original error (if any)
			s3Log.Errorf("code=%v msg=%v, err=%v\n", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
			return awsErr
		}
	} else {
		return err
	}
}

// note that this is NOT the same as url.PathEscape in golang 1.8,
// as this preserves / and url.PathEscape converts / to %2F
func pathEscape(path string) string {
	u := url.URL{Path: path}
	return u.EscapedPath()
}

func (fs *Goofys) allocateInodeId() (id fuseops.InodeID) {
	id = fs.nextInodeID
	fs.nextInodeID++
	return
}

func expired(cache time.Time, ttl time.Duration) bool {
	now := time.Now()
	if cache.After(now) {
		return false
	}
	return !cache.Add(ttl).After(now)
}

func (fs *Goofys) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {

	var inode *Inode
	var ok bool
	defer func() { fuseLog.Debugf("<-- LookUpInode %v %v %v", op.Parent, op.Name, err) }()

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	parent.mu.Lock()
	inode = parent.findChildUnlocked(op.Name)
	if inode != nil {
		ok = true
		if expired(inode.AttrTime, fs.flags.StatCacheTTL) {
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
			if _, ok := parent.dir.DeletedChildren[op.Name]; ok {
				// File is deleted locally
				parent.mu.Unlock()
				return fuse.ENOENT
			}
		}
		if !expired(parent.dir.DirTime, fs.flags.StatCacheTTL) {
			// Don't recheck from the server if directory cache is actual
			parent.mu.Unlock()
			return fuse.ENOENT
		}
	}
	parent.mu.Unlock()

	if !ok {
		inode, err = fs.recheckInode(parent, inode, op.Name)
		err = mapAwsError(err)
		if err != nil {
			return
		}
	}

	inode.Ref()
	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	return
}

func (fs *Goofys) recheckInode(parent *Inode, inode *Inode, name string) (*Inode, error) {
	newInode, err := parent.LookUp(name)
	if err != nil {
		if inode != nil {
			parent.removeChild(inode)
		}
		return nil, err
	}
	return newInode, nil
}

// LOCKS_REQUIRED(fs.mu)
// LOCKS_REQUIRED(parent.mu)
func (fs *Goofys) insertInode(parent *Inode, inode *Inode) {
	addInode := false
	if inode.Name == "." {
		inode.Id = parent.Id
	} else if inode.Name == ".." {
		inode.Id = fuseops.InodeID(fuseops.RootInodeID)
		if parent.Parent != nil {
			inode.Id = parent.Parent.Id
		}
	} else {
		if inode.Id != 0 {
			panic(fmt.Sprintf("inode id is set: %v %v", inode.Name, inode.Id))
		}
		inode.Id = fs.allocateInodeId()
		addInode = true
	}
	parent.insertChildUnlocked(inode)
	if addInode {
		fs.inodes[inode.Id] = inode

		// if we are inserting a new directory, also create
		// the child . and ..
		if inode.isDir() {
			fs.addDotAndDotDot(inode)
		}
	}
}

func (fs *Goofys) addDotAndDotDot(dir *Inode) {
	dot := NewInode(fs, dir, ".")
	dot.ToDir()
	dot.AttrTime = TIME_MAX
	dot.userMetadata = make(map[string][]byte)
	fs.insertInode(dir, dot)

	dot = NewInode(fs, dir, "..")
	dot.ToDir()
	dot.AttrTime = TIME_MAX
	dot.userMetadata = make(map[string][]byte)
	fs.insertInode(dir, dot)
}

func (fs *Goofys) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) (err error) {

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	inode.mu.Lock()
	inode.DeRef(int64(op.N))
	inode.mu.Unlock()

	return
}

func (fs *Goofys) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {
	fs.mu.Lock()

	handleID := fs.nextHandleID
	fs.nextHandleID++

	in := fs.getInodeOrDie(op.Inode)
	fs.mu.Unlock()

	dh := in.OpenDir()

	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.dirHandles[handleID] = dh
	op.Handle = handleID

	return
}

func makeDirEntry(en *DirHandleEntry) fuseutil.Dirent {
	return fuseutil.Dirent{
		Name:   en.Name,
		Type:   en.Type,
		Inode:  en.Inode,
		Offset: en.Offset,
	}
}

func (fs *Goofys) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) (err error) {

	// Find the handle.
	fs.mu.RLock()
	dh := fs.dirHandles[op.Handle]
	fs.mu.RUnlock()

	if dh == nil {
		panic(fmt.Sprintf("can't find dh=%v", op.Handle))
	}

	inode := dh.inode
	inode.logFuse("ReadDir", op.Offset)

	dh.mu.Lock()

	if op.Offset != 0 && op.Offset != dh.lastExternalOffset {
		// seekdir() to an arbitrary offset is not supported
		// jacobsa/fuse says it's not a problem
		err = syscall.ENOTSUP
		dh.mu.Unlock()
		return
	}

	dh.lastExternalOffset = op.Offset
	if dh.lastExternalOffset == 0 {
		dh.lastInternalOffset = 0
	}

	for true {
		e, err := dh.ReadDir(dh.lastInternalOffset, dh.lastExternalOffset)
		if err != nil {
			dh.mu.Unlock()
			err = mapAwsError(err)
			return err
		}
		if e == nil {
			break
		}

		if e.Inode == 0 {
			panic(fmt.Sprintf("unset inode %v", e.Name))
		}

		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], makeDirEntry(e))
		if n == 0 {
			break
		}

		dh.inode.logFuse("<-- ReadDir", e.Name, e.Offset)

		op.BytesRead += n
		dh.lastInternalOffset++
		dh.lastExternalOffset++
	}

	dh.mu.Unlock()
	return
}

func (fs *Goofys) ReleaseDirHandle(
	ctx context.Context,
	op *fuseops.ReleaseDirHandleOp) (err error) {

	fs.mu.Lock()
	defer fs.mu.Unlock()

	dh := fs.dirHandles[op.Handle]
	dh.CloseDir()

	fuseLog.Debugln("ReleaseDirHandle", dh.inode.FullName())

	delete(fs.dirHandles, op.Handle)

	return
}

func (fs *Goofys) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	fs.mu.RLock()
	in := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	fh, err := in.OpenFile()
	if err != nil {
		err = mapAwsError(err)
		return
	}

	fs.mu.Lock()

	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = fh
	fs.mu.Unlock()

	op.Handle = handleID

	in.mu.Lock()
	defer in.mu.Unlock()

	// this flag appears to tell the kernel if this open should
	// use the page cache or not. "use" here means:
	//
	// read will read from cache
	// write will populate cache

	// We have our own in-memory cache, kernel page cache is redundant
	op.KeepPageCache = false

	return
}

func (fs *Goofys) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {

	fs.mu.RLock()
	fh := fs.fileHandles[op.Handle]
	fs.mu.RUnlock()

	op.Data, op.BytesRead, err = fh.ReadFile(op.Offset, op.Size)
	err = mapAwsError(err)

	return
}

func (fs *Goofys) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) (err error) {

	if !fs.flags.IgnoreFsync {
		fs.mu.RLock()
		in := fs.getInodeOrDie(op.Inode)
		fs.mu.RUnlock()

		if in.Id == fuseops.RootInodeID {
			err = fs.SyncFS(nil)
		} else if in.isDir() {
			err = fs.SyncFS(in)
		} else {
			err = in.SyncFile()
		}
		err = mapAwsError(err)
	}

	return
}

func (fs *Goofys) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {

	// FlushFile is a no-op because we flush changes to the server asynchronously
	// If the user really wants to persist a file to the server he should call fsync()

	return
}

func (fs *Goofys) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fh := fs.fileHandles[op.Handle]
	fh.Release()

	fuseLog.Debugln("ReleaseFileHandle", fh.inode.FullName(), op.Handle, fh.inode.Id)

	delete(fs.fileHandles, op.Handle)

	// try to compact heap
	//fs.bufferPool.MaybeGC()
	return
}

func (fs *Goofys) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) (err error) {

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	inode, fh := parent.Create(op.Name)

	// Always take inode locks after fs lock if you need both...
	fs.mu.Lock()
	defer fs.mu.Unlock()

	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	// Allocate a handle.
	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = fh

	op.Handle = handleID

	inode.logFuse("<-- CreateFile")

	return
}

func (fs *Goofys) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) (err error) {

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	// ignore op.Mode for now
	inode, err := parent.MkDir(op.Name)
	if err != nil {
		err = mapAwsError(err)
		return err
	}

	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	return
}

func (fs *Goofys) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) (err error) {

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	err = parent.RmDir(op.Name)
	err = mapAwsError(err)
	parent.logFuse("<-- RmDir", op.Name, err)
	return
}

func (fs *Goofys) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	if op.Size != nil {
		inode.mu.Lock()
		inode.ResizeUnlocked(*op.Size)
		if inode.CacheState == ST_CACHED {
			inode.SetCacheState(ST_MODIFIED)
			inode.fs.WakeupFlusher()
		}
		inode.mu.Unlock()
	}

	attr, err := inode.GetAttributes()
	err = mapAwsError(err)
	if err == nil {
		op.Attributes = *attr
		op.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	}
	return
}

func (fs *Goofys) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {

	fs.mu.RLock()

	fh, ok := fs.fileHandles[op.Handle]
	if !ok {
		panic(fmt.Sprintf("WriteFile: can't find handle %v", op.Handle))
	}
	fs.mu.RUnlock()

	err = fh.WriteFile(op.Offset, op.Data)
	err = mapAwsError(err)

	return
}

func (fs *Goofys) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) (err error) {

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	err = parent.Unlink(op.Name)
	err = mapAwsError(err)
	return
}

// rename("from", "to") causes the kernel to send lookup of "from" and
// "to" prior to sending rename to us
func (fs *Goofys) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) (err error) {

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.OldParent)
	newParent := fs.getInodeOrDie(op.NewParent)
	fs.mu.RUnlock()

	if op.OldParent == op.NewParent {
		parent.mu.Lock()
		defer parent.mu.Unlock()
	} else {
		// lock ordering to prevent deadlock
		if op.OldParent < op.NewParent {
			parent.mu.Lock()
			newParent.mu.Lock()
		} else {
			newParent.mu.Lock()
			parent.mu.Lock()
		}
		defer parent.mu.Unlock()
		defer newParent.mu.Unlock()
	}

	err = parent.Rename(op.OldName, newParent, op.NewName)
	err = mapAwsError(err)

	return
}

func (fs *Goofys) SyncFS(parent *Inode) (err error) {
	if parent == nil {
		log.Infof("Flushing all changes")
	} else {
		log.Infof("Flushing all changes under %v", parent.FullName())
	}
	fs.mu.RLock()
	inodes := make([]fuseops.InodeID, 0, len(fs.inodes))
	for id, inode := range fs.inodes {
		if parent == nil || parent.isParentOf(inode) {
			inodes = append(inodes, id)
		}
	}
	fs.mu.RUnlock()
	for i := 0; i < len(inodes); i++ {
		id := inodes[i]
		fs.mu.RLock()
		inode := fs.inodes[id]
		fs.mu.RUnlock()
		if inode != nil {
			inode.SyncFile()
		}
	}
	return
}
