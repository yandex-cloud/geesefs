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
	"os"
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
	connection *fuse.Connection

	bucket string

	flags *FlagStorage

	umask uint32

	gcs       bool
	rootAttrs InodeAttributes

	bufferPool *BufferPool

	// A lock protecting the state of the file system struct itself (distinct
	// from per-inode locks). Should be always taken after any inode locks.
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

	// Inflight changes are tracked to skip them in parallel listings
	// Required because we don't have guarantees about listing & change ordering
	inflightListingId int
	inflightListings map[int]map[string]bool
	inflightChanges map[string]int

	nextHandleID fuseops.HandleID
	dirHandles   map[fuseops.HandleID]*DirHandle

	fileHandles map[fuseops.HandleID]*FileHandle

	activeFlushers int64
	flushRetrySet int32
	memRecency uint64

	forgotCnt uint32

	zeroBuf []byte
	lfru *LFRU
	diskFdMu sync.Mutex
	diskFdCond *sync.Cond
	diskFdCount int64

	stats OpStats
}

type OpStats struct {
	reads int64
	readHits int64
	writes int64
	flushes int64
	metadataReads int64
	metadataWrites int64
	noops int64
	ts time.Time
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
		inflightChanges: make(map[string]int),
		inflightListings: make(map[int]map[string]bool),
		stats: OpStats{
			ts: time.Now(),
		},
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
	cloud.MultipartExpire(&MultipartExpireInput{})

	now := time.Now()
	fs.rootAttrs = InodeAttributes{
		Size:  4096,
		Ctime: now,
		Mtime: now,
	}

	fs.bufferPool = NewBufferPool(int64(flags.MemoryLimit), uint64(flags.GCInterval) << 20)
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
	root.Attributes.Ctime = fs.rootAttrs.Ctime

	fs.inodes[fuseops.RootInodeID] = root
	fs.addDotAndDotDot(root)

	fs.nextHandleID = 1
	fs.dirHandles = make(map[fuseops.HandleID]*DirHandle)

	fs.fileHandles = make(map[fuseops.HandleID]*FileHandle)

	fs.flusherCond = sync.NewCond(&fs.flusherMu)
	go fs.Flusher()
	if fs.flags.StatsInterval > 0 {
		go fs.StatPrinter()
	}

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

func (fs *Goofys) StatPrinter() {
	for {
		time.Sleep(fs.flags.StatsInterval)
		now := time.Now()
		d := now.Sub(fs.stats.ts).Seconds()
		reads := atomic.SwapInt64(&fs.stats.reads, 0)
		readHits := atomic.SwapInt64(&fs.stats.readHits, 0)
		writes := atomic.SwapInt64(&fs.stats.writes, 0)
		flushes := atomic.SwapInt64(&fs.stats.flushes, 0)
		metadataReads := atomic.SwapInt64(&fs.stats.metadataReads, 0)
		metadataWrites := atomic.SwapInt64(&fs.stats.metadataWrites, 0)
		noops := atomic.SwapInt64(&fs.stats.noops, 0)
		fs.stats.ts = now
		readsOr1 := float64(reads)
		if reads == 0 {
			readsOr1 = 1
		}
		fmt.Fprintf(
			os.Stderr,
			"%v I/O: %.2f read/s, %.2f %% hits, %.2f write/s; metadata: %.2f read/s, %.2f write/s; %.2f noop/s; %.2f flush/s\n",
			now.Format("2006/01/02 15:04:05.000000"),
			float64(reads) / d,
			float64(readHits)/readsOr1*100,
			float64(writes) / d,
			float64(metadataReads) / d,
			float64(metadataWrites) / d,
			float64(noops) / d,
			float64(flushes) / d,
		)
	}
}

// Close unneeded cache FDs
func (fs *Goofys) FDCloser() {
	fs.diskFdMu.Lock()
	for {
		rmFdItem := fs.lfru.Pick(nil)
		for fs.flags.MaxDiskCacheFD > 0 && fs.diskFdCount > fs.flags.MaxDiskCacheFD && rmFdItem != nil {
			fs.diskFdMu.Unlock()
			fs.mu.RLock()
			rmFdInode := fs.inodes[rmFdItem.Id()]
			fs.mu.RUnlock()
			if rmFdInode != nil {
				rmFdInode.mu.Lock()
				if rmFdInode.DiskCacheFD != nil {
					rmFdInode.DiskCacheFD.Close()
					rmFdInode.DiskCacheFD = nil
					rmFdInode.mu.Unlock()
					fs.diskFdMu.Lock()
					fs.diskFdCount--
				} else {
					rmFdInode.mu.Unlock()
					fs.diskFdMu.Lock()
				}
			} else {
				fs.diskFdMu.Lock()
			}
			rmFdItem = fs.lfru.Pick(rmFdItem)
		}
		fs.diskFdCond.Wait()
	}
	fs.diskFdMu.Unlock()
}

// Try to reclaim some clean buffers
func (fs *Goofys) FreeSomeCleanBuffers(size int64) (int64, bool) {
	freed := int64(0)
	haveDirty := false
	// Free at least 5 MB
	if size < 5*1024*1024 {
		size = 5*1024*1024
	}
	skipRecent := atomic.LoadUint64(&fs.memRecency)
	// Avoid evicting at least 1/4 of recent memory allocations
	if skipRecent > fs.flags.MemoryLimit/4 {
		skipRecent -= fs.flags.MemoryLimit/4
	} else {
		skipRecent = 0
	}
	var cacheItem *LFRUItem
	for {
		cacheItem = fs.lfru.Pick(cacheItem)
		if cacheItem == nil {
			if skipRecent != 0 {
				// Rescan without "skipRecent"
				skipRecent = 0
				cacheItem = fs.lfru.Pick(cacheItem)
				if cacheItem == nil {
					break
				}
			} else {
				break
			}
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
		i := 0
		for ; i < len(inode.buffers); i++ {
			if freed >= size {
				break
			}
			buf := inode.buffers[i]
			// Never evict buffers flushed in an incomplete (last) part
			if buf.dirtyID == 0 || buf.state == BUF_FLUSHED_FULL {
				if buf.ptr != nil && !inode.IsRangeLocked(buf.offset, buf.length, false) &&
					// Skip recent buffers when possible
					(skipRecent == 0 || buf.recency <= skipRecent) {
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
					} else if buf.state == BUF_FLUSHED_FULL {
						// A flushed buffer can be removed at a cost of finalizing multipart upload
						// to read it back later. However it's likely not a problem if we're uploading
						// a large file because we may never need to read it back.
						prev := del-1
						if prev < 0 {
							prev = i-1
						}
						if prev >= 0 && inode.buffers[prev].state == BUF_FL_CLEARED &&
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
			inode.buffers = append(inode.buffers[0 : del], inode.buffers[i : ]...)
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

func (fs *Goofys) ScheduleRetryFlush() {
	if atomic.CompareAndSwapInt32(&fs.flushRetrySet, 0, 1) {
		time.AfterFunc(fs.flags.RetryInterval, func() {
			atomic.StoreInt32(&fs.flushRetrySet, 0)
			// Wakeup flusher after retry interval
			fs.WakeupFlusher()
		})
	}
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
//
// FIXME There's still room for optimisations: now flusher scans all inodes
// and tries to flush any of them and it wastes a lot of time scanning.
// A queue of "ready to flush" items could be much better.
func (fs *Goofys) Flusher() {
	var inodes []fuseops.InodeID
	again := false
	for {
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
					sent := inode.TryFlush()
					if sent {
						atomic.AddInt64(&fs.stats.flushes, 1)
					}
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
			dirInode = NewInode(fs, mp, dirName)
			dirInode.ToDir()
			dirInode.AttrTime = TIME_MAX
			dirInode.userMetadata = make(map[string][]byte)

			fs.insertInode(mp, dirInode)
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

	atomic.AddInt64(&fs.stats.metadataReads, 1)

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

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

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

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

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

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

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

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

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

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	if op.Name == fs.flags.RefreshAttr {
		// Setting xattr with special name (.invalidate) refreshes the inode's cache
		inode.mu.Lock()
		parent := inode.Parent
		parentId := fuseops.InodeID(0)
		if parent != nil {
			parentId = parent.Id
		}
		name := inode.Name
		inodeId := inode.Id
		inode.mu.Unlock()
		inode.resetDirTimeRec()
		var mappedErr error
		if parent == nil {
			// For regular directories it's enough to send one invalidation
			// message, the kernel will send forgets for their children and
			// everything will be refreshed just fine.
			// But root directory is a special case: we should invalidate all
			// inodes in it ourselves. Basically this means that we have to do
			// a listing and notify the kernel about every file in a root
			// directory.
			dh := inode.OpenDir()
			dh.mu.Lock()
			var notifications []interface{}
			for {
				en, err := dh.ReadDir(dh.lastInternalOffset, dh.lastExternalOffset)
				if err != nil {
					mappedErr = mapAwsError(err)
					break
				}
				if en == nil {
					break
				}
				if dh.lastInternalOffset >= 2 {
					// Delete notifications are send by ReadDir() itself
					notifications = append(notifications, &fuseops.NotifyInvalEntry{
						Parent: inode.Id,
						Name: en.Name,
					})
				}
				dh.lastInternalOffset++
				dh.lastExternalOffset++
			}
			dh.CloseDir()
			dh.mu.Unlock()
			// Send notifications from another goroutine to prevent deadlocks
			if fs.connection != nil {
				go func() {
					for _, n := range notifications {
						fs.connection.Notify(n)
					}
				}()
			}
			return mappedErr
		}
		inode, err = fs.recheckInode(parent, inode, name)
		mappedErr = mapAwsError(err)
		if fs.connection != nil {
			// Send notifications from another goroutine to prevent deadlocks
			go func() {
				if mappedErr == fuse.ENOENT {
					fs.connection.Notify(&fuseops.NotifyDelete{
						Parent: parentId,
						Child: inodeId,
						Name: name,
					})
				} else {
					fs.connection.Notify(&fuseops.NotifyInvalEntry{
						Parent: parentId,
						Name: name,
					})
				}
			}()
		}
		if mappedErr == fuse.ENOENT {
			// We don't mind if the file disappeared
			return nil
		}
		return mappedErr
	}

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

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

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

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

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

// LOCKS_REQUIRED(fs.mu)
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

	atomic.AddInt64(&fs.stats.metadataReads, 1)

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
		if inode == nil {
			return fuse.ENOENT
		}
	}

	inode.Ref()
	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	return
}

func (fs *Goofys) recheckInode(parent *Inode, inode *Inode, name string) (newInode *Inode, err error) {
	newInode, err = parent.LookUp(name, inode == nil)
	if err != nil {
		if inode != nil {
			parent.removeChild(inode)
		}
		return nil, err
	}
	return newInode, nil
}

// LOCKS_REQUIRED(parent.mu)
// LOCKS_EXCLUDED(fs.mu)
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
		fs.mu.Lock()
		inode.Id = fs.allocateInodeId()
		addInode = true
	}
	parent.insertChildUnlocked(inode)
	if addInode {
		fs.inodes[inode.Id] = inode
		fs.mu.Unlock()

		// if we are inserting a new directory, also create
		// the child . and ..
		if inode.isDir() {
			fs.addDotAndDotDot(inode)
		}
	}
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) addDotAndDotDot(dir *Inode) {
	dot := NewInode(fs, dir, ".")
	dot.ToDir()
	dot.AttrTime = TIME_MAX
	fs.insertInode(dir, dot)

	dot = NewInode(fs, dir, "..")
	dot.ToDir()
	dot.AttrTime = TIME_MAX
	fs.insertInode(dir, dot)
}

func (fs *Goofys) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataReads, 1)

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

	atomic.AddInt64(&fs.stats.noops, 1)

	fs.mu.Lock()
	in := fs.getInodeOrDie(op.Inode)
	if atomic.LoadInt32(&in.refreshed) == -1 {
		// Stale inode
		fs.mu.Unlock()
		return syscall.ESTALE
	}
	handleID := fs.nextHandleID
	fs.nextHandleID++
	fs.mu.Unlock()

	if atomic.LoadInt32(&in.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

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

	atomic.AddInt64(&fs.stats.metadataReads, 1)

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
		// Do our best to support directory seeks even though we can't guarantee
		// consistent listings in this case (i.e. files may be duplicated or skipped on changes)
		// 'Normal' software doesn't seek within the directory.
		// nfs-kernel-server, though, does: it closes the dir between paged listing calls.
		fuseLog.Debugf("Directory seek from %v to %v in %v", op.Offset, dh.lastExternalOffset, inode.FullName())
		inode.mu.Lock()
		dh.lastExternalOffset = op.Offset
		dh.lastInternalOffset = int(op.Offset)
		if dh.lastInternalOffset > len(inode.dir.Children) {
			dh.lastInternalOffset = len(inode.dir.Children)
		}
		if len(inode.dir.Children) > 0 {
			inode.dir.Children[dh.lastInternalOffset-1].mu.Lock()
			dh.lastName = inode.dir.Children[dh.lastInternalOffset-1].Name
			inode.dir.Children[dh.lastInternalOffset-1].mu.Unlock()
		} else {
			dh.lastName = ""
		}
		inode.mu.Unlock()
	} else if op.Offset == 0 {
		dh.lastExternalOffset = 0
		dh.lastInternalOffset = 0
		dh.lastName = ""
	}

	for {
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
		// We have to modify it here because WriteDirent MAY not send the entry
		if dh.lastInternalOffset >= 0 {
			dh.lastInternalOffset++
		}
		dh.lastExternalOffset++
		dh.lastName = e.Name
	}

	dh.mu.Unlock()
	return
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) addInflightChange(key string) {
	fs.mu.Lock()
	fs.inflightChanges[key]++
	for _, v := range fs.inflightListings {
		v[key] = true
	}
	fs.mu.Unlock()
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) completeInflightChange(key string) {
	fs.mu.Lock()
	fs.inflightChanges[key]--
	if fs.inflightChanges[key] <= 0 {
		delete(fs.inflightChanges, key)
	}
	fs.mu.Unlock()
}

// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) addInflightListing() int {
	fs.mu.Lock()
	fs.inflightListingId++
	id := fs.inflightListingId
	m := make(map[string]bool)
	for k, _ := range fs.inflightChanges {
		m[k] = true
	}
	fs.inflightListings[id] = m
	fs.mu.Unlock()
	return id
}

// For any listing, we forcibly exclude all objects modifications of which were
// started before the completion of the listing, but were not completed before
// the beginning of the listing.
// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) completeInflightListing(id int) map[string]bool {
	fs.mu.Lock()
	m := fs.inflightListings[id]
	delete(fs.inflightListings, id)
	fs.mu.Unlock()
	return m
}

func (fs *Goofys) ReleaseDirHandle(
	ctx context.Context,
	op *fuseops.ReleaseDirHandleOp) (err error) {

	atomic.AddInt64(&fs.stats.noops, 1)

	fs.mu.RLock()
	dh := fs.dirHandles[op.Handle]
	fs.mu.RUnlock()

	dh.CloseDir()

	fuseLog.Debugln("ReleaseDirHandle", dh.inode.FullName())

	fs.mu.Lock()
	delete(fs.dirHandles, op.Handle)
	fs.mu.Unlock()

	return
}

func (fs *Goofys) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	fs.mu.RLock()
	in := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	atomic.AddInt64(&fs.stats.noops, 1)

	if atomic.LoadInt32(&in.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

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

	atomic.AddInt64(&fs.stats.reads, 1)

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

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

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

	atomic.AddInt64(&fs.stats.noops, 1)

	return
}

func (fs *Goofys) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fh := fs.fileHandles[op.Handle]
	fh.Release()

	atomic.AddInt64(&fs.stats.noops, 1)

	fuseLog.Debugln("ReleaseFileHandle", fh.inode.FullName(), op.Handle, fh.inode.Id)

	delete(fs.fileHandles, op.Handle)

	// try to compact heap
	//fs.bufferPool.MaybeGC()
	return
}

func (fs *Goofys) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	inode, fh := parent.Create(op.Name)

	// Always take inode locks after fs lock if you need both...
	fs.mu.Lock()
	defer fs.mu.Unlock()

	inode.setFileMode(op.Mode)

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

// MkNode is required for NFS even with regular files
// because kernel nfsd uses vfs_create() -> fuse_create() -> fuse_mknod()
// and then separate fuse_open() for file creation instead of fuse_create_open()
func (fs *Goofys) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if (op.Mode & os.ModeType) != os.ModeDir &&
		(op.Mode & os.ModeType) != 0 &&
		!fs.flags.EnableSpecials {
		return syscall.ENOTSUP
	}

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	var inode *Inode
	if (op.Mode & os.ModeDir) != 0 {
		inode, err = parent.MkDir(op.Name)
		if err != nil {
			err = mapAwsError(err)
			return err
		}
	} else {
		var fh *FileHandle
		inode, fh = parent.Create(op.Name)
		fh.Release()
	}
	inode.Attributes.Rdev = op.Rdev
	inode.setFileMode(op.Mode)

	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	return
}

func (fs *Goofys) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	// ignore op.Mode for now
	inode, err := parent.MkDir(op.Name)
	if err != nil {
		err = mapAwsError(err)
		return err
	}
	if fs.flags.EnablePerms {
		inode.Attributes.Mode = os.ModeDir | (op.Mode & os.ModePerm)
	} else {
		inode.Attributes.Mode = os.ModeDir | fs.flags.DirMode
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

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	err = parent.RmDir(op.Name)
	err = mapAwsError(err)
	parent.logFuse("<-- RmDir", op.Name, err)
	return
}

func (fs *Goofys) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	if inode.Parent == nil {
		// chmod/chown on the root directory of mountpoint is not supported
		return syscall.ENOTSUP
	}

	if op.Size != nil || op.Mode != nil || op.Mtime != nil || op.Uid != nil || op.Gid != nil {
		inode.mu.Lock()
	}

	modified := false

	if op.Size != nil && inode.Attributes.Size != *op.Size {
		if *op.Size > fs.getMaxFileSize() {
			// File size too large
			log.Warnf(
				"Maximum file size exceeded when trying to truncate %v to %v bytes",
				inode.FullName(), *op.Size,
			)
			inode.mu.Unlock()
			return syscall.EFBIG
		}
		inode.ResizeUnlocked(*op.Size, true, true)
		modified = true
	}

	if op.Mode != nil {
		m, err := inode.setFileMode(*op.Mode)
		if err != nil {
			inode.mu.Unlock()
			return err
		}
		modified = modified || m
	}

	if op.Mtime != nil && fs.flags.EnableMtime && inode.Attributes.Mtime != *op.Mtime {
		inode.Attributes.Mtime = *op.Mtime
		inode.setUserMeta(fs.flags.MtimeAttr, []byte(fmt.Sprintf("%d", inode.Attributes.Mtime.Unix())))
		modified = true
	}

	if op.Uid != nil && fs.flags.EnablePerms && inode.Attributes.Uid != *op.Uid {
		inode.Attributes.Uid = *op.Uid
		if inode.Attributes.Uid != fs.flags.Uid {
			inode.setUserMeta(fs.flags.UidAttr, []byte(fmt.Sprintf("%d", inode.Attributes.Uid)))
		} else {
			inode.setUserMeta(fs.flags.UidAttr, nil)
		}
		modified = true
	}

	if op.Gid != nil && fs.flags.EnablePerms && inode.Attributes.Gid != *op.Gid {
		inode.Attributes.Gid = *op.Gid
		if inode.Attributes.Gid != fs.flags.Gid {
			inode.setUserMeta(fs.flags.GidAttr, []byte(fmt.Sprintf("%d", inode.Attributes.Gid)))
		} else {
			inode.setUserMeta(fs.flags.GidAttr, nil)
		}
		modified = true
	}

	if modified && inode.CacheState == ST_CACHED {
		inode.SetCacheState(ST_MODIFIED)
		inode.fs.WakeupFlusher()
	}

	if op.Size != nil || op.Mode != nil || op.Mtime != nil || op.Uid != nil || op.Gid != nil {
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

	atomic.AddInt64(&fs.stats.writes, 1)

	fs.mu.RLock()

	fh, ok := fs.fileHandles[op.Handle]
	if !ok {
		panic(fmt.Sprintf("WriteFile: can't find handle %v", op.Handle))
	}
	fs.mu.RUnlock()

	// fuse binding leaves extra room for header, so we
	// account for it when we decide whether to do "zero-copy" write
	copyData := len(op.Data) < cap(op.Data)-4096
	err = fh.WriteFile(op.Offset, op.Data, copyData)
	err = mapAwsError(err)
	op.SuppressReuse = !copyData

	return
}

func (fs *Goofys) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	err = parent.Unlink(op.Name)
	err = mapAwsError(err)
	return
}

// rename("from", "to") causes the kernel to send lookup of "from" and
// "to" prior to sending rename to us
func (fs *Goofys) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.OldParent)
	newParent := fs.getInodeOrDie(op.NewParent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 ||
		atomic.LoadInt32(&newParent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

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

const (
	FALLOC_FL_KEEP_SIZE      = uint32(0x01)
	FALLOC_FL_PUNCH_HOLE     = uint32(0x02)
	FALLOC_FL_COLLAPSE_RANGE = uint32(0x08)
	FALLOC_FL_ZERO_RANGE     = uint32(0x10)
	FALLOC_FL_INSERT_RANGE   = uint32(0x20)
)

func (fs *Goofys) Fallocate(
	ctx context.Context,
	op *fuseops.FallocateOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	if op.Length == 0 {
		return nil
	}

	inode.mu.Lock()

	modified := false

	if (op.Mode & (FALLOC_FL_COLLAPSE_RANGE | FALLOC_FL_INSERT_RANGE)) != 0 {
		// Insert range/remove range operations are not supported
		// It's possible to support them, but it will require buffer remapping support.
		// I.e. if you open a file, insert/collapse a range and then read past the
		// affected offset you should get data from the old offset! And it's probably
		// wise to use UploadPartCopy with the corresponding ranges to optimize copying
		// on the server side in this case. Some day we might even be able to preserve
		// multipart part IDs if cutting a non-finalized upload across part boundaries,
		// but now we can't - part offsets are always fixed.
		inode.mu.Unlock()
		return syscall.ENOTSUP
	}

	if op.Offset+op.Length > inode.Attributes.Size {
		if (op.Mode & FALLOC_FL_KEEP_SIZE) == 0 {
			// Resize
			if op.Offset+op.Length > fs.getMaxFileSize() {
				// File size too large
				log.Warnf(
					"Maximum file size exceeded when trying to extend %v to %v bytes using fallocate",
					inode.FullName(), op.Offset+op.Length,
				)
				inode.mu.Unlock()
				return syscall.EFBIG
			}
			inode.ResizeUnlocked(op.Offset+op.Length, true, true)
			modified = true
		} else {
			if op.Offset > inode.Attributes.Size {
				op.Offset = inode.Attributes.Size
			}
			op.Length = inode.Attributes.Size-op.Offset
		}
	}

	if (op.Mode & (FALLOC_FL_PUNCH_HOLE | FALLOC_FL_ZERO_RANGE)) != 0 {
		// Zero fill
		mod, _ := inode.zeroRange(op.Offset, op.Length)
		modified = modified || mod
	}

	if modified && inode.CacheState == ST_CACHED {
		inode.SetCacheState(ST_MODIFIED)
		inode.fs.WakeupFlusher()
	}

	inode.mu.Unlock()

	return
}

func (fs *Goofys) SetConnection(conn *fuse.Connection) {
	fs.connection = conn
}
