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
	"github.com/yandex-cloud/geesefs/core/cfg"

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

	"github.com/jacobsa/fuse/fuseops"

	"net/http"

	"github.com/sirupsen/logrus"
)

// goofys is a Filey System written in Go. All the backend data is
// stored on S3 as is. It's a Filey System instead of a File System
// because it makes minimal effort at being POSIX
// compliant. Particularly things that are difficult to support on S3
// or would translate into more than one round-trip would either fail
// (rename non-empty dir) or faked (no per-file permission). goofys
// does not have a on disk data cache, and consistency model is
// close-to-open.

type cacheEvent struct {
	inode *Inode
}

type Goofys struct {
	ctx    context.Context
	bucket string

	flags *cfg.FlagStorage

	umask uint32

	rootAttrs InodeAttributes

	bufferPool *BufferPool
	wantFree   int32

	shutdown   int32
	shutdownCh chan struct{}

	// A lock protecting the state of the file system struct itself (distinct
	// from per-inode locks). Should be always taken after any inode locks.
	mu sync.RWMutex

	flusherMu    sync.Mutex
	flusherCond  *sync.Cond
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

	inodesByTime map[int64]map[fuseops.InodeID]bool

	// Inflight changes are tracked to skip them in parallel listings
	// Required because we don't have guarantees about listing & change ordering
	inflightListingId int
	inflightListings  map[int]map[string]bool
	inflightChanges   map[string]int

	nextHandleID fuseops.HandleID
	dirHandles   map[fuseops.HandleID]*DirHandle

	fileHandles map[fuseops.HandleID]*FileHandle

	activeFlushers  int64
	flushRetrySet   int32
	hasNewWrites    uint64
	flushPriorities []int64

	forgotCnt uint32

	cleanQueue BufferQueue
	inodeQueue InodeQueue

	zeroBuf []byte

	diskFdQueue *FDQueue

	stats OpStats

	NotifyCallback func(notifications []interface{})

	cacheEventChan  chan cacheEvent
	cachingStatus   map[string]bool
	cachingStatusMu sync.Mutex
}

type OpStats struct {
	reads          int64
	readHits       int64
	writes         int64
	flushes        int64
	metadataReads  int64
	metadataWrites int64
	noops          int64
	evicts         int64
	ts             time.Time
}

var s3Log = cfg.GetLogger("s3")
var log = cfg.GetLogger("main")
var fuseLog = cfg.GetLogger("fuse")

func NewBackend(bucket string, flags *cfg.FlagStorage) (cloud StorageBackend, err error) {
	if flags.Backend == nil {
		flags.Backend = (&cfg.S3Config{}).Init()
	}

	if config, ok := flags.Backend.(*cfg.AZBlobConfig); ok {
		cloud, err = NewAZBlob(bucket, config)
	} else if config, ok := flags.Backend.(*cfg.ADLv1Config); ok {
		cloud, err = NewADLv1(bucket, flags, config)
	} else if config, ok := flags.Backend.(*cfg.ADLv2Config); ok {
		cloud, err = NewADLv2(bucket, flags, config)
	} else if config, ok := flags.Backend.(*cfg.S3Config); ok {
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

func NewGoofys(ctx context.Context, bucketName string, flags *cfg.FlagStorage) (*Goofys, error) {
	if flags.DebugFuse || flags.DebugMain {
		log.Level = logrus.DebugLevel
	}
	if flags.DebugFuse {
		fuseLog.Level = logrus.DebugLevel
	}
	if flags.DebugS3 {
		cfg.SetCloudLogLevel(logrus.DebugLevel)
	}

	if flags.Backend == nil {
		if spec, err := ParseBucketSpec(bucketName); err == nil {
			switch spec.Scheme {
			case "adl":
				auth, err := cfg.AzureAuthorizerConfig{
					Log: cfg.GetLogger("adlv1"),
				}.Authorizer()
				if err != nil {
					err = fmt.Errorf("couldn't load azure credentials: %v",
						err)
					return nil, err
				}
				flags.Backend = &cfg.ADLv1Config{
					Endpoint:   spec.Bucket,
					Authorizer: auth,
				}
				// adlv1 doesn't really have bucket
				// names, but we will rebuild the
				// prefix
				bucketName = ""
				if spec.Prefix != "" {
					bucketName = ":" + spec.Prefix
				}
			case "wasb":
				config, err := cfg.AzureBlobConfig(flags.Endpoint, spec.Bucket, "blob")
				if err != nil {
					return nil, err
				}
				flags.Backend = &config
				if config.Container != "" {
					bucketName = config.Container
				} else {
					bucketName = spec.Bucket
				}
				if config.Prefix != "" {
					spec.Prefix = config.Prefix
				}
				if spec.Prefix != "" {
					bucketName += ":" + spec.Prefix
				}
			case "abfs":
				config, err := cfg.AzureBlobConfig(flags.Endpoint, spec.Bucket, "dfs")
				if err != nil {
					return nil, err
				}
				flags.Backend = &config
				if config.Container != "" {
					bucketName = config.Container
				} else {
					bucketName = spec.Bucket
				}
				if config.Prefix != "" {
					spec.Prefix = config.Prefix
				}
				if spec.Prefix != "" {
					bucketName += ":" + spec.Prefix
				}

				flags.Backend = &cfg.ADLv2Config{
					Endpoint:   config.Endpoint,
					Authorizer: &config,
				}
				bucketName = spec.Bucket
				if spec.Prefix != "" {
					bucketName += ":" + spec.Prefix
				}
			}
		}
	}
	return newGoofys(ctx, bucketName, flags, NewBackend)
}

func newGoofys(ctx context.Context, bucket string, flags *cfg.FlagStorage,
	newBackend func(string, *cfg.FlagStorage) (StorageBackend, error)) (*Goofys, error) {

	// Set up the basic struct.
	fs := &Goofys{
		ctx:              ctx,
		bucket:           bucket,
		flags:            flags,
		umask:            0122,
		shutdownCh:       make(chan struct{}),
		zeroBuf:          make([]byte, 1048576),
		inflightChanges:  make(map[string]int),
		inflightListings: make(map[int]map[string]bool),
		stats: OpStats{
			ts: time.Now(),
		},
		flushPriorities: make([]int64, MAX_FLUSH_PRIORITY+1),
		cacheEventChan:  make(chan cacheEvent, 10000),
		cachingStatus:   make(map[string]bool),
		cachingStatusMu: sync.Mutex{},
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
		return nil, fmt.Errorf("Unable to setup backend: %v", err)
	}

	randomObjectName := prefix + (RandStringBytesMaskImprSrc(32))
	err = cloud.Init(randomObjectName)
	if err != nil {
		return nil, fmt.Errorf("Unable to access '%v': %v", bucket, err)
	}
	cloud.MultipartExpire(&MultipartExpireInput{})

	now := time.Now()
	fs.rootAttrs = InodeAttributes{
		Size:  4096,
		Ctime: now,
		Mtime: now,
	}

	if os.Getenv("GOGC") == "" {
		// Set garbage collection ratio to 20 instead of 100 by default.
		debug.SetGCPercent(20)
	}

	fs.bufferPool = NewBufferPool(int64(flags.MemoryLimit), uint64(flags.GCInterval)<<20)
	fs.bufferPool.FreeSomeCleanBuffers = func(size int64) (int64, bool) {
		return fs.FreeSomeCleanBuffers(size)
	}

	fs.nextInodeID = fuseops.RootInodeID + 1
	fs.inodes = make(map[fuseops.InodeID]*Inode)
	fs.inodesByTime = make(map[int64]map[fuseops.InodeID]bool)
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

	fs.nextHandleID = 1
	fs.dirHandles = make(map[fuseops.HandleID]*DirHandle)

	fs.fileHandles = make(map[fuseops.HandleID]*FileHandle)

	fs.flusherCond = sync.NewCond(&fs.flusherMu)
	go fs.Flusher()
	if fs.flags.StatsInterval > 0 {
		go fs.StatPrinter()
	}

	if fs.flags.CachePath != "" {
		fs.diskFdQueue = NewFDQueue(int(fs.flags.MaxDiskCacheFD))
		if fs.flags.MaxDiskCacheFD > 0 {
			go fs.FDCloser()
		}
	}

	go fs.MetaEvictor()
	go fs.processCacheEvents()

	return fs, nil
}

func (fs *Goofys) processCacheEvents() {
	for {
		select {
		case <-fs.shutdownCh:
			return
		case cacheEvent := <-fs.cacheEventChan:
			log.Debugf("Processing cache event for inode: %v", cacheEvent.inode.FullName())

			inode := cacheEvent.inode
			flags := inode.fs.flags

			knownHash, ok := inode.userMetadata[flags.HashAttr]
			if !ok {
				log.Errorf("No hash found for inode, not caching inode in external cache: %v", inode.FullName())
				continue
			}

			s3, ok := flags.Backend.(*cfg.S3Config)
			if !ok {
				log.Errorf("Backend is not S3, not caching inode in external cache: %v", inode.FullName())
				continue
			}

			if inode.Attributes.Size > 0 {
				hash, err := fs.flags.ExternalCacheClient.StoreContentFromS3(struct {
					Path        string
					BucketName  string
					Region      string
					EndpointURL string
					AccessKey   string
					SecretKey   string
				}{
					Path:        inode.FullName(),
					BucketName:  fs.bucket,
					Region:      s3.Region,
					EndpointURL: flags.Endpoint,
					AccessKey:   s3.AccessKey,
					SecretKey:   s3.SecretKey,
				}, struct {
					RoutingKey string
					Lock       bool
				}{RoutingKey: string(knownHash), Lock: true})
				if err != nil {
					log.Debugf("Failed to store content from source: %v", err)
				} else if hash != string(knownHash) {
					log.Debugf("Hash mismatch for inode: %v", inode.FullName())
					continue
				}

				fs.clearCachingStatus(inode.FullName())
				log.Debugf("Successfully cached inode: %v", inode.FullName())
			}
		}
	}
}

func (fs *Goofys) CacheFileInExternalCache(inode *Inode) {
	hash, ok := inode.userMetadata[fs.flags.HashAttr]
	if !ok {
		log.Errorf("No hash found for inode, not caching inode in external cache: %v", inode.FullName())
		return
	}

	// Check and update caching status
	fs.cachingStatusMu.Lock()
	if fs.cachingStatus[string(hash)] {
		fs.cachingStatusMu.Unlock()
		return // File is already being cached or has been cached
	}
	fs.cachingStatus[string(hash)] = true
	fs.cachingStatusMu.Unlock()

	// Submit cache event
	fs.cacheEventChan <- cacheEvent{inode: inode}
}

func (fs *Goofys) clearCachingStatus(path string) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	delete(fs.cachingStatus, path)
}

func (fs *Goofys) Shutdown() {
	atomic.StoreInt32(&fs.shutdown, 1)
	close(fs.shutdownCh)
	fs.WakeupFlusher()
	if fs.diskFdQueue != nil {
		fs.diskFdQueue.cond.Broadcast()
	}
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
// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) getInodeOrDie(id fuseops.InodeID) (inode *Inode) {
	fs.mu.RLock()
	inode = fs.inodes[id]
	fs.mu.RUnlock()
	if inode == nil {
		panic(fmt.Sprintf("Unknown inode: %v", id))
	}

	return
}

func (fs *Goofys) AddDirHandle(dh *DirHandle) fuseops.HandleID {
	fs.mu.Lock()
	handleID := fs.nextHandleID
	fs.nextHandleID++
	fs.dirHandles[handleID] = dh
	fs.mu.Unlock()
	return handleID
}

func (fs *Goofys) AddFileHandle(fh *FileHandle) fuseops.HandleID {
	fs.mu.Lock()
	handleID := fs.nextHandleID
	fs.nextHandleID++
	fs.fileHandles[handleID] = fh
	fs.mu.Unlock()
	return handleID
}

func (fs *Goofys) StatPrinter() {
	for atomic.LoadInt32(&fs.shutdown) == 0 {
		select {
		case <-time.After(fs.flags.StatsInterval):
		case <-fs.shutdownCh:
			return
		}
		now := time.Now()
		d := now.Sub(fs.stats.ts).Seconds()
		reads := atomic.SwapInt64(&fs.stats.reads, 0)
		readHits := atomic.SwapInt64(&fs.stats.readHits, 0)
		writes := atomic.SwapInt64(&fs.stats.writes, 0)
		flushes := atomic.SwapInt64(&fs.stats.flushes, 0)
		metadataReads := atomic.SwapInt64(&fs.stats.metadataReads, 0)
		metadataWrites := atomic.SwapInt64(&fs.stats.metadataWrites, 0)
		noops := atomic.SwapInt64(&fs.stats.noops, 0)
		evicts := atomic.SwapInt64(&fs.stats.evicts, 0)
		fs.mu.RLock()
		inodeCount := len(fs.inodes)
		fs.mu.RUnlock()
		fs.stats.ts = now
		readsOr1 := float64(reads)
		if reads == 0 {
			readsOr1 = 1
		}
		log.Infof(
			"I/O: %.2f read/s, %.2f %% hits, %.2f write/s; metadata: %.2f read/s, %.2f write/s, %.2f noop/s, %v alive, %.2f evict/s; %.2f flush/s",
			float64(reads)/d,
			float64(readHits)/readsOr1*100,
			float64(writes)/d,
			float64(metadataReads)/d,
			float64(metadataWrites)/d,
			float64(noops)/d,
			inodeCount,
			float64(evicts)/d,
			float64(flushes)/d,
		)
	}
}

// Close unneeded cache FDs
func (fs *Goofys) FDCloser() {
	for atomic.LoadInt32(&fs.shutdown) == 0 {
		fs.diskFdQueue.CloseExtra()
	}
}

// Try to reclaim some clean buffers
func (fs *Goofys) FreeSomeCleanBuffers(origSize int64) (int64, bool) {
	freed := int64(0)
	// Free at least 5 MB
	size := origSize
	if size < 5*1024*1024 {
		size = 5 * 1024 * 1024
	}
	var inode *Inode
	var cleanEnd, cleanQueueID uint64
	for freed < size {
		inode, cleanEnd, cleanQueueID = fs.cleanQueue.NextClean(cleanQueueID)
		if cleanQueueID == 0 {
			break
		}
		inode.mu.Lock()
		toFs := -1
		buf := inode.buffers.Get(cleanEnd)
		// Never evict buffers flushed in an incomplete (last) part
		if buf != nil && (buf.state == BUF_CLEAN || buf.state == BUF_FLUSHED_FULL) &&
			buf.ptr != nil && !inode.IsRangeLocked(buf.offset, buf.length, false) {
			fs.tryEvictToDisk(inode, buf, &toFs)
			allocated, _ := inode.buffers.EvictFromMemory(buf)
			if allocated != 0 {
				fs.bufferPool.UseUnlocked(allocated, false)
				freed -= allocated
			}
		}
		inode.mu.Unlock()
		if freed >= size {
			break
		}
	}
	haveDirty := fs.inodeQueue.Size() > 0
	if freed < origSize && haveDirty {
		fs.bufferPool.mu.Unlock()
		atomic.AddInt32(&fs.wantFree, 1)
		fs.WakeupFlusherAndWait(true)
		atomic.AddInt32(&fs.wantFree, -1)
		fs.bufferPool.mu.Lock()
	}
	return freed, haveDirty
}

// FIXME: Implement disk cache size limit, add another btree.Map-based
// "LRU" queue to delete old files from the disk.
func (fs *Goofys) tryEvictToDisk(inode *Inode, buf *FileBuffer, toFs *int) {
	if fs.flags.CachePath != "" && !buf.onDisk {
		if *toFs == -1 {
			*toFs = 1
		}
		if *toFs > 0 {
			// Evict to disk
			err := inode.OpenCacheFD()
			if err != nil {
				*toFs = 0
			} else {
				_, err := inode.DiskCacheFD.WriteAt(buf.data, int64(buf.offset))
				if err != nil {
					*toFs = 0
					log.Errorf("Couldn't write %v bytes at offset %v to %v: %v",
						len(buf.data), buf.offset, fs.flags.CachePath+"/"+inode.FullName(), err)
				} else {
					buf.onDisk = true
				}
			}
		}
	}
}

func (fs *Goofys) WakeupFlusherAndWait(wait bool) {
	fs.flusherMu.Lock()
	if fs.flushPending == 0 {
		fs.flushPending = 1
		fs.flusherCond.Broadcast()
	}
	if wait {
		// Wait for any result
		fs.flusherCond.Wait()
	}
	fs.flusherMu.Unlock()
}

func (fs *Goofys) WakeupFlusher() {
	fs.WakeupFlusherAndWait(false)
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
//  1. File opened => reads and writes just populate cache
//  2. File closed => flush it
//     Created or fully overwritten =>
//     => Less than 5 MB => upload in a single part
//     => More than 5 MB => upload using multipart
//     Updated => CURRENTLY:
//     => Less than 5 MB => upload in a single part
//     => More than 5 MB => update using multipart copy
//     Also we can't update less than 5 MB because it's the minimal part size
//  3. Fsync triggered => intermediate full flush (same algorithm)
//  4. Dirty memory limit reached => without on-disk cache we have to flush the whole object.
//     With on-disk cache we can unload some dirty buffers to disk.
func (fs *Goofys) Flusher() {
	var inodeID, nextQueueID uint64
	priority := 1
	for atomic.LoadInt32(&fs.shutdown) == 0 {
		fs.flusherMu.Lock()
		if fs.flushPending == 0 {
			fs.flusherCond.Wait()
		}
		fs.flushPending = 0
		fs.flusherMu.Unlock()
		attempts := 1
		if priority > 1 || priority == 1 && nextQueueID != 0 {
			attempts = 2
		}
		curPriorityOk := false
		for i := 1; i <= priority; i++ {
			curPriorityOk = curPriorityOk || atomic.LoadInt64(&fs.flushPriorities[priority]) > 0
		}
		for attempts > 0 && atomic.LoadInt64(&fs.activeFlushers) < fs.flags.MaxFlushers {
			inodeID, nextQueueID = fs.inodeQueue.Next(nextQueueID)
			if inodeID == 0 {
				if curPriorityOk {
					break
				}
				priority++
				if priority > MAX_FLUSH_PRIORITY {
					attempts--
					priority = 1
				}
				if curPriorityOk {
					break
				}
			} else {
				if atomic.CompareAndSwapUint64(&fs.hasNewWrites, 1, 0) {
					// restart from the beginning
					inodeID, nextQueueID = 0, 0
					priority = 1
					attempts = 1
					curPriorityOk = atomic.LoadInt64(&fs.flushPriorities[1]) > 0
					continue
				}
				fs.mu.RLock()
				inode := fs.inodes[fuseops.InodeID(inodeID)]
				fs.mu.RUnlock()
				started := false
				if inode != nil {
					started = inode.TryFlush(priority)
				}
				curPriorityOk = curPriorityOk || started
			}
		}
	}
}

func (fs *Goofys) EvictEntry(id fuseops.InodeID) bool {
	fs.mu.RLock()
	childTmp := fs.inodes[id]
	fs.mu.RUnlock()
	if childTmp == nil ||
		childTmp.Id == fuseops.RootInodeID ||
		atomic.LoadInt32(&childTmp.fileHandles) > 0 ||
		atomic.LoadInt32(&childTmp.CacheState) > ST_DEAD ||
		childTmp.isDir() && atomic.LoadInt64(&childTmp.dir.ModifiedChildren) > 0 {
		return false
	}
	if !childTmp.mu.TryLock() {
		return false
	}
	// We CAN evict inodes which are still referenced by the kernel,
	// but only if they're expired!
	if childTmp.ExpireTime.After(time.Now()) {
		childTmp.mu.Unlock()
		return false
	}
	tmpParent := childTmp.Parent
	// Respect locking order: parent before child, inode before fs
	childTmp.mu.Unlock()
	if !tmpParent.mu.TryLock() {
		return false
	}
	if !childTmp.mu.TryLock() {
		tmpParent.mu.Unlock()
		return false
	}
	if childTmp.Parent != tmpParent ||
		atomic.LoadInt32(&tmpParent.fileHandles) > 0 {
		childTmp.mu.Unlock()
		tmpParent.mu.Unlock()
		return false
	}
	found := tmpParent.findChildUnlocked(childTmp.Name)
	if found == childTmp {
		tmpParent.removeChildUnlocked(childTmp)
		// Mark directory listing as unfinished
		tmpParent.dir.DirTime = time.Time{}
		tmpParent.dir.forgetDuringList = true
	}
	childTmp.resetCache()
	childTmp.SetCacheState(ST_DEAD)
	// Drop inode
	fs.mu.Lock()
	childTmp.resetExpireTime()
	delete(fs.inodes, childTmp.Id)
	fs.forgotCnt += 1
	fs.mu.Unlock()
	childTmp.mu.Unlock()
	tmpParent.mu.Unlock()
	return true
}

func (fs *Goofys) MetaEvictor() {
	retry := false
	var seen map[fuseops.InodeID]bool
	for atomic.LoadInt32(&fs.shutdown) == 0 {
		if !retry {
			select {
			case <-time.After(1 * time.Second):
			case <-fs.shutdownCh:
				return
			}
			seen = make(map[fuseops.InodeID]bool)
		}
		// Try to keep the number of cached inodes under control %)
		fs.mu.RLock()
		totalInodes := len(fs.inodes)
		toEvict := (totalInodes - fs.flags.EntryLimit) * 2
		if toEvict < 0 {
			fs.mu.RUnlock()
			retry = false
			continue
		}
		if toEvict < fs.flags.EntryLimit/100 {
			toEvict = fs.flags.EntryLimit / 100
		}
		if toEvict < 10 {
			toEvict = 10
		}
		expireUnix := time.Now().Add(-fs.flags.StatCacheTTL).Unix()
		var scan []fuseops.InodeID
		for tm, inodes := range fs.inodesByTime {
			if tm < expireUnix {
				for inode, _ := range inodes {
					if !seen[inode] {
						scan = append(scan, inode)
					}
					if len(scan) >= toEvict {
						break
					}
				}
			}
			if len(scan) >= toEvict {
				break
			}
		}
		fs.mu.RUnlock()
		evicted := 0
		for _, id := range scan {
			if fs.EvictEntry(id) {
				evicted++
			} else {
				seen[id] = true
			}
		}
		retry = len(scan) >= toEvict && totalInodes > fs.flags.EntryLimit
		atomic.AddInt64(&fs.stats.evicts, int64(evicted))
		if len(scan) > 0 {
			log.Debugf("metadata cache: alive %v, scanned %v, evicted %v", totalInodes, len(scan), evicted)
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
			dirInode.SetAttrTime(TIME_MAX)
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
		mountInode.SetAttrTime(TIME_MAX)
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
		prev.SetAttrTime(TIME_MAX)

	}
	prev.addModified(1)
	fuseLog.Infof("mounted /%v", prev.FullName())
	b.mounted = true
}

func (fs *Goofys) MountAll(mounts []*Mount) {
	root := fs.getInodeOrDie(fuseops.RootInodeID)

	for _, m := range mounts {
		fs.mount(root, m)
	}
}

func (fs *Goofys) Mount(mount *Mount) {
	root := fs.getInodeOrDie(fuseops.RootInodeID)
	fs.mount(root, mount)
}

func (fs *Goofys) Unmount(mountPoint string) {
	mp := fs.getInodeOrDie(fuseops.RootInodeID)

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

func (fs *Goofys) RefreshInodeCache(inode *Inode) error {
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
	var notifications []interface{}
	if parent == nil {
		// For regular directories it's enough to send one invalidation
		// message, the kernel will send forgets for their children and
		// everything will be refreshed just fine.
		// But root directory is a special case: we should invalidate all
		// inodes in it ourselves. Basically this means that we have to do
		// a listing and notify the kernel about every file in the root
		// directory.
		dh := inode.OpenDir()
		dh.mu.Lock()
		for {
			en, err := dh.ReadDir()
			if err != nil {
				mappedErr = mapAwsError(err)
				break
			}
			if en == nil {
				break
			}
			if dh.lastInternalOffset >= 2 {
				// Delete notifications are sent by ReadDir() itself
				notifications = append(notifications, &fuseops.NotifyInvalEntry{
					Parent: inode.Id,
					Name:   en.Name,
				})
			}
			dh.Next(en.Name)
		}
		dh.CloseDir()
		dh.mu.Unlock()
		if fs.NotifyCallback != nil {
			fs.NotifyCallback(notifications)
		}
		return mappedErr
	}
	inode, err := parent.recheckInode(inode, name)
	mappedErr = mapAwsError(err)
	if mappedErr == syscall.ENOENT {
		notifications = append(notifications, &fuseops.NotifyDelete{
			Parent: parentId,
			Child:  inodeId,
			Name:   name,
		})
	} else {
		notifications = append(notifications, &fuseops.NotifyInvalEntry{
			Parent: parentId,
			Name:   name,
		})
	}
	if fs.NotifyCallback != nil {
		fs.NotifyCallback(notifications)
	}
	if mappedErr == syscall.ENOENT {
		// We don't mind if the file disappeared
		return nil
	}
	return mappedErr
}

// FIXME: Add similar write backoff (now it's handled by file/dir code)
func ReadBackoff(flags *cfg.FlagStorage, try func(attempt int) error) (err error) {
	interval := flags.ReadRetryInterval
	attempt := 1
	for {
		err = try(attempt)
		if err != nil {
			if shouldRetry(err) && (flags.ReadRetryAttempts < 1 || attempt < flags.ReadRetryAttempts) {
				attempt++
				time.Sleep(interval)
				interval = time.Duration(flags.ReadRetryMultiplier * float64(interval))
				if interval > flags.ReadRetryMax {
					interval = flags.ReadRetryMax
				}
			} else {
				break
			}
		} else {
			break
		}
	}
	return
}

func mapHttpError(status int) error {
	switch status {
	case 400:
		return syscall.EINVAL
	case 401:
		return syscall.EACCES
	case 403:
		return syscall.EACCES
	case 404:
		return syscall.ENOENT
	case 405:
		return syscall.ENOTSUP
	case http.StatusConflict:
		return syscall.EINTR
	case http.StatusRequestedRangeNotSatisfiable:
		return syscall.ERANGE
	case 429:
		return syscall.EAGAIN
	case 503:
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
			return syscall.EEXIST
		case "ConcurrentUpdatesPatchConflict", "ObjectVersionPatchConflict":
			return syscall.EBUSY
		}

		if reqErr, ok := err.(awserr.RequestFailure); ok {
			// A service error occurred
			err = mapHttpError(reqErr.StatusCode())
			if err != nil {
				return err
			} else {
				s3Log.Warnf("http=%v %v s3=%v request=%v\n",
					reqErr.StatusCode(), reqErr.Message(),
					awsErr.Code(), reqErr.RequestID())
				return reqErr
			}
		} else {
			// Generic AWS Error with Code, Message, and original error (if any)
			s3Log.Warnf("code=%v msg=%v, err=%v\n", awsErr.Code(), awsErr.Message(), awsErr.OrigErr())
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

// LOCKS_REQUIRED(parent.mu)
// LOCKS_EXCLUDED(fs.mu)
func (fs *Goofys) insertInode(parent *Inode, inode *Inode) {
	if inode.Id != 0 {
		panic(fmt.Sprintf("inode id is set: %v %v", inode.Name, inode.Id))
	}
	fs.mu.Lock()
	inode.Id = fs.allocateInodeId()
	parent.insertChildUnlocked(inode)
	fs.inodes[inode.Id] = inode
	fs.mu.Unlock()
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

func (fs *Goofys) SyncTree(parent *Inode) (err error) {
	if parent == nil || parent.Id == fuseops.RootInodeID {
		log.Infof("Flushing all changes")
		parent = nil
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

func (fs *Goofys) LookupParent(path string) (parent *Inode, child string, err error) {
	parts := strings.Split(path, "/")
	child = parts[len(parts)-1]
	fs.mu.RLock()
	parent = fs.inodes[fuseops.RootInodeID]
	fs.mu.RUnlock()
	for i := 0; i < len(parts)-1; i++ {
		if parts[i] != "" {
			parent, err = parent.LookUpCached(parts[i])
			if err != nil {
				return
			}
			if !parent.isDir() {
				return nil, "", syscall.ENOTDIR
			}
			if atomic.LoadInt32(&parent.CacheState) == ST_DEAD {
				// Stale inode
				return nil, "", syscall.ESTALE
			}
		}
	}
	return
}

func (fs *Goofys) LookupPath(path string) (inode *Inode, err error) {
	parts := strings.Split(path, "/")
	fs.mu.RLock()
	inode = fs.inodes[fuseops.RootInodeID]
	fs.mu.RUnlock()
	for i := 0; i < len(parts); i++ {
		if parts[i] != "" {
			if !inode.isDir() {
				return nil, syscall.ENOTDIR
			}
			inode, err = inode.LookUpCached(parts[i])
			if err != nil {
				return
			}
			if atomic.LoadInt32(&inode.CacheState) == ST_DEAD {
				// Stale inode
				return nil, syscall.ESTALE
			}
		}
	}
	return
}
