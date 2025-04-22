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
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"
)

type FileHandle struct {
	inode         *Inode
	lastReadEnd   uint64
	seqReadSize   uint64
	lastReadCount uint64
	lastReadTotal uint64
	lastReadSizes []uint64
	lastReadIdx   int
}

// On Linux and MacOS, IOV_MAX = 1024
const IOV_MAX = 1024
const READ_BUF_SIZE = 128 * 1024
const MAX_FLUSH_PRIORITY = 3

var errContentNotFound = errors.New("content not found")

// NewFileHandle returns a new file handle for the given `inode`
func NewFileHandle(inode *Inode) *FileHandle {
	fh := &FileHandle{inode: inode}
	if inode.fs.flags.SmallReadCount > 1 {
		fh.lastReadSizes = make([]uint64, inode.fs.flags.SmallReadCount-1)
	}
	return fh
}

func (fs *Goofys) partNum(offset uint64) uint64 {
	n := uint64(0)
	start := uint64(0)
	for _, s := range fs.flags.PartSizes {
		p := (offset - start) / s.PartSize
		if p < s.PartCount {
			return n + p
		}
		start += s.PartSize * s.PartCount
		n += s.PartCount
	}
	if offset == start {
		// Sometimes we use partNum() to calculate total part count from end offset - allow it
		return n
	}
	panic(fmt.Sprintf(
		"Offset too large: %v, max supported file size with current part size configuration is %v",
		offset, start,
	))
}

func (fs *Goofys) partRange(num uint64) (offset uint64, size uint64) {
	n := uint64(0)
	start := uint64(0)
	for _, s := range fs.flags.PartSizes {
		if num < n+s.PartCount {
			return start + (num-n)*s.PartSize, s.PartSize
		}
		start += s.PartSize * s.PartCount
		n += s.PartCount
	}
	panic(fmt.Sprintf("Part number too large: %v", num))
}

func (fs *Goofys) getMaxFileSize() (size uint64) {
	for _, s := range fs.flags.PartSizes {
		size += s.PartSize * s.PartCount
	}
	return size
}

func (inode *Inode) ResizeUnlocked(newSize uint64, finalizeFlushed bool) {
	var allocated int64
	// Truncate or extend
	inode.checkPauseWriters()
	if inode.Attributes.Size > newSize {
		// Truncate - remove extra buffers
		for inode.IsRangeLocked(newSize, inode.Attributes.Size-newSize, true) ||
			inode.buffers.AnyFlushed(newSize, inode.Attributes.Size-newSize) {
			// We can't remove already flushed parts from the server :-(
			// And S3 (at least Ceph and Yandex, even though not Amazon) requires
			// to use ALL uploaded parts when completing the upload
			// So... we have to first finish the upload to be able to truncate it
			inode.pauseWriters++
			inode.mu.Unlock()
			inode.SyncFile()
			inode.mu.Lock()
			inode.pauseWriters--
			if inode.readCond != nil {
				inode.readCond.Broadcast()
			}
		}
		allocated = inode.buffers.RemoveRange(newSize, inode.Attributes.Size-newSize, nil)
	} else if inode.Attributes.Size < newSize {
		// Zero fill extended region
		_, allocated = inode.buffers.ZeroRange(inode.Attributes.Size, newSize-inode.Attributes.Size)
	}
	inode.fs.bufferPool.Use(allocated, true)
	inode.Attributes.Size = newSize
}

func (inode *Inode) checkPauseWriters() {
	for inode.pauseWriters > 0 {
		if inode.readCond == nil {
			inode.readCond = sync.NewCond(&inode.mu)
		}
		inode.readCond.Wait()
	}
}

func (fh *FileHandle) WriteFile(offset int64, data []byte, copyData bool) (err error) {
	fh.inode.logFuse("WriteFile", offset, len(data))

	end := uint64(offset) + uint64(len(data))

	if end > fh.inode.fs.getMaxFileSize() {
		// File offset too large
		log.Warnf(
			"Maximum file size exceeded when writing %v bytes at offset %v to %v",
			len(data), offset, fh.inode.FullName(),
		)
		return syscall.EFBIG
	}

	// Try to reserve space without the inode lock
	if fh.inode.fs.flags.UseEnomem {
		err = fh.inode.fs.bufferPool.Use(int64(len(data)), false)
		if err != nil {
			return err
		}
	}

	fh.inode.mu.Lock()

	if fh.inode.CacheState == ST_DELETED || fh.inode.CacheState == ST_DEAD {
		// Oops, it's a deleted file. We don't support changing invisible files
		if fh.inode.fs.flags.UseEnomem {
			fh.inode.fs.bufferPool.Use(-int64(len(data)), false)
		}
		fh.inode.mu.Unlock()
		return syscall.ENOENT
	}

	fh.inode.checkPauseWriters()

	if fh.inode.Attributes.Size < end {
		// Extend and zero fill
		fh.inode.ResizeUnlocked(end, false)
	}

	allocated := fh.inode.buffers.Add(uint64(offset), data, BUF_DIRTY, copyData)
	atomic.StoreUint64(&fh.inode.fs.hasNewWrites, 1)

	fh.inode.lastWriteEnd = end
	if fh.inode.CacheState == ST_CACHED {
		fh.inode.SetCacheState(ST_MODIFIED)
	}
	// FIXME: Don't activate the flusher immediately for small writes
	fh.inode.fs.WakeupFlusher()
	fh.inode.Attributes.Mtime = time.Now()
	fh.inode.Attributes.Ctime = fh.inode.Attributes.Mtime
	if fh.inode.fs.flags.EnableMtime && fh.inode.userMetadata != nil &&
		fh.inode.userMetadata[fh.inode.fs.flags.MtimeAttr] != nil {
		delete(fh.inode.userMetadata, fh.inode.fs.flags.MtimeAttr)
		fh.inode.userMetadataDirty = 2
	}

	fh.inode.mu.Unlock()

	// Correct memory usage
	if !fh.inode.fs.flags.UseEnomem {
		fh.inode.fs.bufferPool.Use(allocated, true)
	} else if allocated != int64(len(data)) {
		err = fh.inode.fs.bufferPool.Use(allocated-int64(len(data)), true)
	}

	return
}

func (inode *Inode) OpenCacheFD() error {
	fs := inode.fs
	if inode.DiskCacheFD == nil {
		cacheFileName := fs.flags.CachePath + "/" + inode.FullName()
		os.MkdirAll(path.Dir(cacheFileName), fs.flags.CacheFileMode|((fs.flags.CacheFileMode&0777)>>2))
		var err error
		inode.DiskCacheFD, err = os.OpenFile(cacheFileName, os.O_RDWR|os.O_CREATE, fs.flags.CacheFileMode)
		if err != nil {
			log.Errorf("Couldn't open %v: %v", cacheFileName, err)
			return err
		} else {
			inode.OnDisk = true
			fs.diskFdQueue.InsertFD(inode)
		}
	} else {
		// LRU
		fs.diskFdQueue.UseFD(inode)
	}
	return nil
}

func (inode *Inode) loadFromServer(readRanges []Range, readAheadSize uint64, ignoreMemoryLimit bool) {
	// Add readahead & merge adjacent requests
	readRanges = mergeRA(readRanges, readAheadSize, inode.fs.flags.ReadMergeKB*1024)
	last := &readRanges[len(readRanges)-1]
	if last.End > inode.knownSize {
		last.End = inode.knownSize
	}

	// Split very large requests into smaller chunks to read in parallel
	readRanges = splitRA(readRanges, inode.fs.flags.ReadAheadParallelKB*1024)

	// Mark new ranges as being loaded from the server
	for _, rr := range readRanges {
		inode.buffers.AddLoading(rr.Start, rr.End-rr.Start)
	}

	// Send requests
	if inode.readCond == nil {
		inode.readCond = sync.NewCond(&inode.mu)
	}
	cloud, key := inode.cloud()
	if inode.oldParent != nil {
		_, key = inode.oldParent.cloud()
		key = appendChildName(key, inode.oldName)
	}

	for _, rr := range readRanges {
		go inode.retryRead(cloud, key, rr.Start, rr.End-rr.Start, ignoreMemoryLimit)
	}
}

func (inode *Inode) loadFromDisk(diskRanges []Range) (allocated int64, err error) {
	err = inode.OpenCacheFD()
	if err != nil {
		return
	}
	for _, rr := range diskRanges {
		readSize := rr.End - rr.Start
		data := make([]byte, readSize)
		_, err = inode.DiskCacheFD.ReadAt(data, int64(rr.Start))
		if err == nil {
			inode.buffers.ReviveFromDisk(rr.Start, data)
		}
	}
	return
}

func (inode *Inode) loadFromExternalCache(offset uint64, size uint64, hash string) (allocated int64, totalDone uint64, err error) {
	buf, err := inode.fs.flags.ExternalCacheClient.GetContent(string(hash), int64(offset), int64(size), struct{ RoutingKey string }{RoutingKey: hash})
	if err != nil || buf == nil {
		if err.Error() == errContentNotFound.Error() {
			inode.fs.CacheFileInExternalCache(inode)
		}

		return 0, 0, err
	}

	totalDone = uint64(len(buf))

	// Cache the result
	inode.mu.Lock()
	allocated += inode.buffers.Add(offset, buf, BUF_CLEAN, false)
	inode.mu.Unlock()

	// Notify waiting readers
	inode.readCond.Broadcast()
	return allocated, totalDone, nil
}

// Load some inode data into memory
// Must be called with inode.mu taken
// Loaded range should be guarded against eviction by adding it into inode.readRanges
func (inode *Inode) LoadRange(offset, size uint64, readAheadSize uint64, ignoreMemoryLimit bool) (miss bool, err error) {
	if offset >= inode.Attributes.Size {
		return
	}
	if offset+size >= inode.Attributes.Size {
		size = inode.Attributes.Size - offset
	}
	raSize := size
	if raSize < readAheadSize {
		raSize = readAheadSize
	}
	if offset+raSize > inode.Attributes.Size {
		raSize = inode.Attributes.Size - offset
	}

	// Collect requests to the server and disk
	readRanges, loading, flushCleared := inode.buffers.GetHoles(offset, raSize)
	if flushCleared {
		// One of the buffers is saved as a part and then removed
		// We must complete multipart upload to be able to read it back
		return true, syscall.ESPIPE
	}

	if len(readRanges) > 0 {
		miss = true
		inode.loadFromServer(readRanges, readAheadSize, ignoreMemoryLimit)
	}

	if inode.fs.flags.CachePath != "" {
		diskRanges := inode.buffers.AddLoadingFromDisk(offset, size)
		if len(diskRanges) > 0 {
			allocated, err := inode.loadFromDisk(diskRanges)
			// Correct memory usage without the inode lock
			inode.mu.Unlock()
			inode.fs.bufferPool.Use(allocated, true)
			inode.mu.Lock()
			// Return on error
			if err != nil {
				return miss, err
			}
		}
	}

	// Wait for the data to load
	if len(readRanges) > 0 || loading {
		for {
			_, _, err := inode.buffers.GetData(offset, size, false)
			if err == ErrBufferIsLoading {
				// still loading
				inode.readCond.Wait()
			} else if err == ErrBufferIsMissing {
				// loading buffer disappeared => read error
				err = inode.readError
				if err == nil {
					err = syscall.EIO
				}
				return true, err
			} else {
				return true, nil
			}
		}
	}

	return
}

func (inode *Inode) retryRead(cloud StorageBackend, key string, offset, size uint64, ignoreMemoryLimit bool) {
	// Maybe free some buffers first
	if inode.fs.flags.UseEnomem {
		err := inode.fs.bufferPool.Use(int64(size), ignoreMemoryLimit)
		if err != nil {
			log.Errorf("Error reading %v +%v of %v: %v", offset, size, key, err)
			inode.mu.Lock()
			inode.readError = err
			inode.buffers.RemoveLoading(offset, size)
			inode.mu.Unlock()
			inode.readCond.Broadcast()
			return
		}
	}
	inode.mu.Lock()
	inode.LockRange(offset, size, false)
	inode.mu.Unlock()

	// We want to retry all errors and sometimes even OK states because S3 may
	// sometimes return 200 or 206 and then drop the connection if some data
	// is temporarily unavailable (err would be io.EOF in that case)
	allocated := int64(0)
	curOffset, curSize := offset, size
	err := ReadBackoff(inode.fs.flags, func(attempt int) error {
		hash, hashFound := inode.userMetadata[inode.fs.flags.HashAttr]

		var alloc int64 = 0
		var done uint64 = 0
		var err error = nil

		if inode.fs.flags.ExternalCacheClient != nil && hashFound {
			alloc, done, err = inode.loadFromExternalCache(curOffset, curSize, string(hash))
			if err != nil {
				alloc, done, err = inode.sendRead(cloud, key, curOffset, curSize)
			}
		} else {
			alloc, done, err = inode.sendRead(cloud, key, curOffset, curSize)
		}

		if err != nil && shouldRetry(err) {
			s3Log.Warnf("Error reading %v +%v of %v (attempt %v): %v", curOffset, curSize, key, attempt, err)
		}

		curOffset += done
		curSize -= done
		allocated += alloc
		return err
	})

	if !inode.fs.flags.UseEnomem {
		inode.fs.bufferPool.Use(int64(allocated), true)
	} else if allocated != int64(size) {
		inode.fs.bufferPool.Use(int64(allocated)-int64(size), true)
	}

	inode.mu.Lock()
	inode.buffers.RemoveLoading(offset, size)
	inode.UnlockRange(offset, size, false)
	inode.readError = err
	inode.mu.Unlock()
	if err != nil {
		inode.readCond.Broadcast()
	}
}

func (inode *Inode) sendRead(cloud StorageBackend, key string, offset, size uint64) (allocated int64, totalDone uint64, err error) {
	resp, err := cloud.GetBlob(&GetBlobInput{
		Key:   key,
		Start: offset,
		Count: size,
	})
	if err != nil {
		return 0, 0, err
	}

	for size > 0 {
		// Read the result in smaller parts so parallelism can be utilized better
		bs := size
		if bs > READ_BUF_SIZE {
			bs = READ_BUF_SIZE
		}
		buf := make([]byte, bs)
		done := uint64(0)
		for done < bs {
			n, err := resp.Body.Read(buf[done:])
			done += uint64(n)
			if err != nil && (err != io.EOF || done < bs) {
				return allocated, totalDone, err
			}
		}

		// Cache part of the result
		inode.mu.Lock()
		if inode.userMetadata == nil {
			// Cache xattrs
			inode.fillXattrFromHead(&(*resp).HeadBlobOutput)
		}
		allocated += inode.buffers.Add(offset, buf, BUF_CLEAN, false)
		inode.mu.Unlock()
		size -= done
		offset += done
		totalDone += done
		// Notify waiting readers
		inode.readCond.Broadcast()
	}
	return allocated, totalDone, nil
}

// LockRange/UnlockRange could be moved into buffer_list.go, but they still have
// to be stored separately from buffers and can't be a refcount - otherwise
// an overwrite would reset the reference count and break locking
func (inode *Inode) LockRange(offset uint64, size uint64, flushing bool) {
	inode.readRanges = append(inode.readRanges, ReadRange{
		Offset:   offset,
		Size:     size,
		Flushing: flushing,
	})
}

func (inode *Inode) UnlockRange(offset uint64, size uint64, flushing bool) {
	for i, v := range inode.readRanges {
		if v.Offset == offset && v.Size == size && v.Flushing == flushing {
			// append(inode.readRanges[0 : i], inode.readRanges[i+1 : ]...) was leading to corruption?..
			// reproduced in TestReadWriteMinimumMemory
			copy(inode.readRanges[i:], inode.readRanges[i+1:])
			inode.readRanges = inode.readRanges[0 : len(inode.readRanges)-1]
			break
		}
	}
}

func (inode *Inode) IsRangeLocked(offset uint64, size uint64, onlyFlushing bool) bool {
	for _, r := range inode.readRanges {
		if r.Offset < offset+size &&
			r.Offset+r.Size > offset &&
			(!onlyFlushing || r.Flushing) {
			return true
		}
	}
	return false
}

func (inode *Inode) CheckLoadRange(offset, size, readAheadSize uint64, ignoreMemoryLimit bool) (bool, error) {
	miss, err := inode.LoadRange(offset, size, readAheadSize, ignoreMemoryLimit)
	if err == syscall.ESPIPE {
		// Finalize multipart upload to get some flushed data back
		// We have to flush all parts that extend the file up until the last flushed part
		// Everything else can be copied on the server. Example:
		//
		// ON SERVER    [111111111111           ]
		// UPDATED      [2222222    222222222222]
		// FLUSHED      [22222             2    ]
		// READ EVICTED [  2                    ]
		// FLUSH NOW    [           2222222     ]
		// COPY NOW     [     111111            ]
		// WILL BECOME  [2222211111122222222    ]
		// FLUSH LATER  [     22            2222]
		//
		// But... simpler way is, in fact, to just block writers and flush the whole file
		inode.pauseWriters++
		for err == syscall.ESPIPE {
			inode.mu.Unlock()
			err = inode.SyncFile()
			inode.mu.Lock()
			if err == nil {
				_, err = inode.LoadRange(offset, size, readAheadSize, ignoreMemoryLimit)
			}
		}
		inode.pauseWriters--
		if inode.readCond != nil {
			inode.readCond.Broadcast()
		}
	}
	return miss, err
}

func (fh *FileHandle) trackRead(offset, size uint64) {
	if size == 0 {
		// Just in case if the length is zero
	} else if offset == fh.lastReadEnd {
		fh.seqReadSize += size
	} else {
		// Track sizes of last N read requests
		if len(fh.lastReadSizes) > 0 {
			if fh.lastReadSizes[fh.lastReadIdx] != 0 {
				fh.lastReadTotal -= fh.lastReadSizes[fh.lastReadIdx]
				fh.lastReadCount--
			}
			fh.lastReadSizes[fh.lastReadIdx] = fh.seqReadSize
			fh.lastReadTotal += fh.lastReadSizes[fh.lastReadIdx]
			fh.lastReadCount++
			fh.lastReadIdx = (fh.lastReadIdx + 1) % len(fh.lastReadSizes)
		}
		fh.seqReadSize = size
	}
	fh.lastReadEnd = offset + size
}

func (fh *FileHandle) getReadAhead() uint64 {
	ra := fh.inode.fs.flags.ReadAheadKB * 1024
	if fh.seqReadSize >= fh.inode.fs.flags.LargeReadCutoffKB*1024 {
		// Use larger readahead with 'pipelining'
		ra = fh.inode.fs.flags.ReadAheadLargeKB * 1024
	} else if fh.lastReadCount > 0 {
		// Disable readahead if last N read requests are smaller than X on average
		avg := (fh.seqReadSize + fh.lastReadTotal) / (1 + fh.lastReadCount)
		if avg <= fh.inode.fs.flags.SmallReadCutoffKB*1024 {
			// Use smaller readahead
			ra = fh.inode.fs.flags.ReadAheadSmallKB * 1024
		}
	}
	return ra
}

func (fh *FileHandle) ReadFile(sOffset int64, sLen int64) (data [][]byte, bytesRead int, err error) {
	offset := uint64(sOffset)
	size := uint64(sLen)

	fh.inode.logFuse("ReadFile", offset, size)
	defer func() {
		fh.inode.logFuse("< ReadFile", bytesRead, err)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
		}
	}()

	if fh.inode.fs.flags.ExternalCacheClient != nil && fh.inode.userMetadata == nil && !fh.inode.isDir() {
		fh.inode.mu.Lock()
		cloud, path := fh.inode.cloud()
		head, err := cloud.HeadBlob(&HeadBlobInput{Key: path})
		if err != nil {
			log.Errorf("Error getting head blob: %v", err)
		}

		fh.inode.setMetadata(head.Metadata)
		fh.inode.mu.Unlock()
	}

	// Lock inode
	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()

	if offset >= fh.inode.Attributes.Size {
		// nothing to read
		err = io.EOF
		return
	}
	if offset+size > fh.inode.Attributes.Size {
		size = fh.inode.Attributes.Size - offset
	}

	// Guard buffers against eviction
	fh.inode.LockRange(offset, size, false)
	defer fh.inode.UnlockRange(offset, size, false)

	// Check if anything requires to be loaded from the server
	ra := fh.getReadAhead()
	fh.trackRead(offset, size)
	miss, requestErr := fh.inode.CheckLoadRange(offset, size, ra, false)
	if !miss {
		atomic.AddInt64(&fh.inode.fs.stats.readHits, 1)
	}
	mappedErr := mapAwsError(requestErr)
	if requestErr != nil {
		err = requestErr
		if mappedErr == syscall.ENOENT || mappedErr == syscall.ERANGE {
			// Object is deleted or resized remotely (416). Discard local version
			log.Warnf("File %v is deleted or resized remotely, discarding local changes", fh.inode.FullName())
			fh.inode.resetCache()
		}
		return
	}

	// return cached buffers directly without copying
	data, _, err = fh.inode.buffers.GetData(offset, size, false)
	if err != nil && requestErr != nil {
		return nil, 0, requestErr
	} else if err != nil {
		return nil, 0, syscall.EIO
	}

	// Don't exceed IOV_MAX-1 for writev.
	if len(data) > IOV_MAX-1 {
		var tail []byte
		for i := IOV_MAX - 2; i < len(data); i++ {
			tail = append(tail, data[i]...)
		}
		data = append(data[0:IOV_MAX-2], tail)
	}

	bytesRead = int(size)

	return
}

func (fh *FileHandle) Release() {
	// LookUpInode accesses fileHandles without mutex taken, so use atomics for now
	n := atomic.AddInt32(&fh.inode.fileHandles, -1)
	if n == -1 {
		panic(fmt.Sprintf("Released more file handles than acquired, n = %v", n))
	}
	if n == 0 {
		fh.inode.Parent.addModified(-1)
	}
	fh.inode.fs.WakeupFlusher()
}

func (inode *Inode) getMultiReader(offset, size uint64) (reader *MultiReader, ids map[uint64]bool, err error) {
	inode.buffers.SplitAt(offset)
	inode.buffers.SplitAt(offset + size)
	data, ids, err := inode.buffers.GetData(offset, size, true)
	if err != nil {
		return nil, nil, err
	}
	reader = NewMultiReader()
	for _, buf := range data {
		reader.AddBuffer(buf)
	}
	return reader, ids, err
}

func (inode *Inode) recordFlushError(err error) {
	inode.flushError = err
	inode.flushErrorTime = time.Now()
	// The original idea was to schedule retry only if err != nil
	// However, current version unblocks flushing in case of bugs, so... okay. Let it be
	inode.fs.ScheduleRetryFlush()
}

func (inode *Inode) TryFlush(priority int) bool {
	overDeleted := false
	parent := inode.Parent
	if parent != nil {
		parent.mu.Lock()
		if parent.dir.DeletedChildren != nil {
			_, overDeleted = parent.dir.DeletedChildren[inode.Name]
		}
		parent.mu.Unlock()
	}
	inode.mu.Lock()
	defer inode.mu.Unlock()
	if inode.Parent != parent {
		return false
	}
	if inode.flushError != nil && time.Now().Sub(inode.flushErrorTime) < inode.fs.flags.RetryInterval {
		inode.fs.ScheduleRetryFlush()
		return false
	}
	if inode.CacheState == ST_DELETED {
		if inode.IsFlushing == 0 && (!inode.isDir() || atomic.LoadInt64(&inode.dir.ModifiedChildren) == 0) {
			inode.SendDelete()
			return true
		}
	} else if (inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED) && inode.isDir() {
		if inode.IsFlushing == 0 && !overDeleted {
			inode.SendMkDir()
			return true
		}
	} else if inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED {
		if overDeleted {
			return false
		}
		return inode.sendUpload(priority)
	}
	return false
}

func (inode *Inode) sendUpload(priority int) bool {
	if inode.oldParent != nil && inode.IsFlushing == 0 && inode.mpu == nil {
		// Rename file
		inode.sendRename()
		return true
	}

	if inode.CacheState == ST_MODIFIED && inode.userMetadataDirty != 0 &&
		inode.oldParent == nil && inode.IsFlushing == 0 {
		hasDirty := inode.buffers.AnyUnclean()
		if !hasDirty {
			// Update metadata by COPYing into the same object
			// It results in the optimized implementation in S3
			inode.sendUpdateMeta()
			return true
		}
	}

	if inode.IsFlushing >= inode.fs.flags.MaxParallelParts {
		return false
	}

	smallFile := inode.Attributes.Size <= inode.fs.flags.SinglePartMB*1024*1024
	canPatch := inode.fs.flags.UsePatch &&
		// Can only patch modified inodes with completed MPUs.
		inode.CacheState == ST_MODIFIED && inode.mpu == nil &&
		// In current implemetation we should not patch big simple objects. Reupload them as multiparts first.
		// If current ETag is unknown, try patching anyway, so that we don't trigger an unecessary mpu.
		(inode.uploadedAsMultipart() || inode.knownETag == "" || smallFile) &&
		// Current PATCH works incorrectly when updating an empty file. Do not update the empty file using PATCH.
		inode.knownSize > 0 &&
		// Currently PATCH does not support truncates. If the file was truncated, reupload it.
		inode.knownSize <= inode.Attributes.Size

	if canPatch {
		return inode.patchObjectRanges()
	}

	if smallFile && inode.mpu == nil {
		// Don't flush small files with active file handles (if not under memory pressure)
		if inode.IsFlushing == 0 && (inode.fileHandles == 0 || inode.forceFlush || atomic.LoadInt32(&inode.fs.wantFree) > 0) {
			// Don't accidentally trigger a parallel multipart flush
			inode.IsFlushing += inode.fs.flags.MaxParallelParts
			atomic.AddInt64(&inode.fs.stats.flushes, 1)
			atomic.AddInt64(&inode.fs.activeFlushers, 1)
			go inode.flushSmallObject()
			return true
		}
		return false
	}

	// Initiate multipart upload, if not yet
	if inode.mpu == nil {
		// Wait for other updates to complete.
		if inode.IsFlushing > 0 {
			return false
		}
		if inode.fs.flags.UsePatch && inode.fs.flags.PreferPatchUploads {
			inode.uploadMinMultipart()
		} else {
			inode.sendStartMultipart()
		}
		return true
	}

	// Pick part(s) to flush
	initiated, canComplete := inode.sendUploadParts(priority)
	if initiated {
		return true
	}

	canComplete = canComplete && !inode.IsRangeLocked(0, inode.Attributes.Size, true)

	if canComplete && (inode.fileHandles == 0 || inode.forceFlush || atomic.LoadInt32(&inode.fs.wantFree) > 0) {
		// Complete the multipart upload
		inode.IsFlushing += inode.fs.flags.MaxParallelParts
		atomic.AddInt64(&inode.fs.stats.flushes, 1)
		atomic.AddInt64(&inode.fs.activeFlushers, 1)
		go func() {
			inode.mu.Lock()
			inode.completeMultipart()
			inode.IsFlushing -= inode.fs.flags.MaxParallelParts
			inode.mu.Unlock()
			atomic.AddInt64(&inode.fs.activeFlushers, -1)
			inode.fs.WakeupFlusher()
		}()
		return true
	}

	return false
}

func (inode *Inode) sendRename() {
	cloud, key := inode.cloud()
	if inode.isDir() {
		key += "/"
	}
	inode.IsFlushing += inode.fs.flags.MaxParallelParts
	atomic.AddInt64(&inode.fs.stats.flushes, 1)
	atomic.AddInt64(&inode.fs.activeFlushers, 1)
	_, from := inode.oldParent.cloud()
	from = appendChildName(from, inode.oldName)
	oldParent := inode.oldParent
	oldName := inode.oldName
	newParent := inode.Parent
	newName := inode.Name
	inode.renamingTo = true
	skipRename := false
	if inode.isDir() {
		from += "/"
		skipRename = true
	}
	go func() {
		var err error
		if !inode.isDir() || !inode.fs.flags.NoDirObject {
			// We don't use RenameBlob here even for hypothetical clouds that support it (not S3),
			// because if we used it we'd have to do it under the inode lock. Because otherwise
			// a parallel read could hit a non-existing name. So, with S3, we do it in 2 passes.
			// First we copy the object, change the inode name, and then we delete the old copy.
			inode.fs.addInflightChange(key)
			_, err = cloud.CopyBlob(&CopyBlobInput{
				Source:      from,
				Destination: key,
			})
			inode.fs.completeInflightChange(key)
			notFoundIgnore := false
			if err != nil {
				mappedErr := mapAwsError(err)
				// Rename the old directory object to copy xattrs from it if it has them
				// We're almost never sure if the directory is implicit or not so we
				// always try to rename the directory object, but ignore NotFound errors
				if mappedErr == syscall.ENOENT && skipRename {
					err = nil
					notFoundIgnore = true
				} else if mappedErr == syscall.ENOENT || mappedErr == syscall.ERANGE {
					s3Log.Warnf("Conflict detected (inode %v): failed to copy %v to %v: %v. File is removed remotely, dropping cache", inode.Id, from, key, err)
					inode.mu.Lock()
					newParent := inode.Parent
					oldParent := inode.oldParent
					oldName := inode.oldName
					inode.oldParent = nil
					inode.oldName = ""
					inode.renamingTo = false
					inode.resetCache()
					inode.mu.Unlock()
					newParent.removeChild(inode)
					if oldParent != nil {
						oldParent.mu.Lock()
						if _, ok := oldParent.dir.DeletedChildren[oldName]; ok {
							delete(oldParent.dir.DeletedChildren, oldName)
							oldParent.addModified(-1)
						}
						oldParent.mu.Unlock()
					}
				} else {
					log.Warnf("Failed to copy %v to %v (rename): %v", from, key, err)
					inode.mu.Lock()
					inode.recordFlushError(err)
					if inode.Parent == oldParent && inode.Name == oldName {
						// Someone renamed the inode back to the original name
						// ...while we failed to copy it :)
						inode.oldParent = nil
						inode.oldName = ""
						inode.renamingTo = false
						inode.Parent.addModified(-1)
						if (inode.CacheState == ST_MODIFIED || inode.CacheState == ST_CREATED) &&
							!inode.isStillDirty() {
							inode.SetCacheState(ST_CACHED)
							inode.SetAttrTime(time.Now())
						}
					}
					inode.mu.Unlock()
				}
			}
			if err == nil {
				log.Debugf("Copied %v to %v (rename)", from, key)
				delKey := from
				delParent := oldParent
				delName := oldName
				inode.mu.Lock()
				// Now we know that the object is accessible by the new name
				if inode.Parent == newParent && inode.Name == newName {
					// Just clear the old path
					inode.oldParent = nil
					inode.oldName = ""
				} else if inode.Parent == oldParent && inode.Name == oldName {
					// Someone renamed the inode back to the original name(!)
					inode.oldParent = nil
					inode.oldName = ""
					// Delete the new key instead of the old one (?)
					delKey = key
					delParent = newParent
					delName = newName
				} else {
					// Someone renamed the inode again(!)
					inode.oldParent = newParent
					inode.oldName = newName
				}
				if (inode.CacheState == ST_MODIFIED || inode.CacheState == ST_CREATED) &&
					!inode.isStillDirty() {
					inode.SetCacheState(ST_CACHED)
					inode.SetAttrTime(time.Now())
				}
				inode.renamingTo = false
				inode.mu.Unlock()
				// Now delete the old key
				if !notFoundIgnore {
					inode.fs.addInflightChange(delKey)
					_, err = cloud.DeleteBlob(&DeleteBlobInput{
						Key: delKey,
					})
					inode.fs.completeInflightChange(delKey)
				}
				if err != nil {
					log.Debugf("Failed to delete %v during rename, will retry later", delKey)
					// Emulate a deleted file
					delParent.mu.Lock()
					delParent.fs.mu.Lock()
					tomb := NewInode(delParent.fs, delParent, delName)
					tomb.Id = delParent.fs.allocateInodeId()
					tomb.fs.inodes[tomb.Id] = tomb
					tomb.userMetadata = make(map[string][]byte)
					tomb.SetCacheState(ST_DELETED)
					tomb.recordFlushError(err)
					delParent.dir.DeletedChildren[delName] = tomb
					delParent.fs.mu.Unlock()
					delParent.mu.Unlock()
				} else {
					log.Debugf("Deleted %v - rename completed", from)
					// Remove from DeletedChildren of the old parent
					delParent.mu.Lock()
					delete(delParent.dir.DeletedChildren, delName)
					delParent.mu.Unlock()
					// And track ModifiedChildren because rename is special - it takes two parents
					delParent.addModified(-1)
				}
			}
		}
		inode.mu.Lock()
		inode.IsFlushing -= inode.fs.flags.MaxParallelParts
		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.WakeupFlusher()
		inode.mu.Unlock()
	}()
}

func (inode *Inode) sendUpdateMeta() {
	// Update metadata by COPYing into the same object
	// It results in the optimized implementation in S3
	cloud, key := inode.cloud()
	if inode.isDir() {
		key += "/"
	}
	inode.userMetadataDirty = 0
	inode.IsFlushing += inode.fs.flags.MaxParallelParts
	atomic.AddInt64(&inode.fs.stats.flushes, 1)
	atomic.AddInt64(&inode.fs.activeFlushers, 1)
	copyIn := &CopyBlobInput{
		Source:      key,
		Destination: key,
		Size:        PUInt64(inode.knownSize),
		ETag:        PString(inode.knownETag),
		Metadata:    escapeMetadata(inode.userMetadata),
	}
	go func() {
		inode.fs.addInflightChange(key)
		_, err := cloud.CopyBlob(copyIn)
		inode.fs.completeInflightChange(key)
		inode.mu.Lock()
		inode.recordFlushError(err)
		if err != nil {
			mappedErr := mapAwsError(err)
			inode.userMetadataDirty = 2
			if mappedErr == syscall.ENOENT || mappedErr == syscall.ERANGE {
				// Object is deleted or resized remotely (416). Discard local version
				s3Log.Warnf("Conflict detected (inode %v): File %v is deleted or resized remotely, discarding local changes", inode.Id, inode.FullName())
				inode.resetCache()
			}
			log.Warnf("Error flushing metadata using COPY for %v: %v", key, err)
		} else if inode.CacheState == ST_MODIFIED && !inode.isStillDirty() {
			inode.SetCacheState(ST_CACHED)
			inode.SetAttrTime(time.Now())
		}
		inode.IsFlushing -= inode.fs.flags.MaxParallelParts
		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.WakeupFlusher()
		inode.mu.Unlock()
	}()
}

func (inode *Inode) sendStartMultipart() {
	cloud, key := inode.cloud()
	if inode.isDir() {
		key += "/"
	}
	inode.IsFlushing += inode.fs.flags.MaxParallelParts
	atomic.AddInt64(&inode.fs.stats.flushes, 1)
	atomic.AddInt64(&inode.fs.activeFlushers, 1)
	go func() {
		inode.beginMultipartUpload(cloud, key)
		inode.IsFlushing -= inode.fs.flags.MaxParallelParts
		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.WakeupFlusher()
		inode.mu.Unlock()
	}()
}

func (inode *Inode) beginMultipartUpload(cloud StorageBackend, key string) {
	params := &MultipartBlobBeginInput{
		Key:         key,
		ContentType: inode.fs.flags.GetMimeType(key),
	}
	if inode.userMetadataDirty != 0 {
		params.Metadata = escapeMetadata(inode.userMetadata)
		// userMetadataDirty == 1 indicates that metadata wasn't changed
		// since the multipart upload was initiated
		inode.userMetadataDirty = 1
	}
	resp, err := cloud.MultipartBlobBegin(params)
	inode.mu.Lock()
	inode.recordFlushError(err)
	if err != nil {
		log.Warnf("Failed to initiate multipart upload for %v: %v", key, err)
	} else {
		log.Debugf("Started multi-part upload of object %v", key)
		inode.mpu = resp
	}
}

func (inode *Inode) sendUploadParts(priority int) (bool, bool) {
	initiated := false
	shouldComplete := true
	flushInode := inode.fileHandles == 0 || inode.forceFlush
	wantFree := atomic.LoadInt32(&inode.fs.wantFree) > 0
	var partlyZero []uint64
	var fullyZero []uint64
	anyEvicted := false
	// Dirty parts should be flushed in the following order:
	// 1) completely filled non-zero non-RMW-evicted parts
	// 2) only when there are no more (priority 1) parts: partly filled non-RMW parts
	// 3) only when there are no (1) and (2) parts, but there exist RMW-evicted parts: zero parts
	inode.buffers.IterateDirtyParts(func(partNum uint64) bool {
		partOffset, partSize := inode.fs.partRange(partNum)
		if inode.IsRangeLocked(partOffset, partSize, true) {
			// Don't flush parts being currently flushed
			shouldComplete = false
			return true
		}
		if partNum == inode.fs.partNum(inode.lastWriteEnd) && !flushInode && !wantFree {
			// Don't write out the last part which is still written to (if not under memory pressure)
			shouldComplete = false
			return true
		}
		partEnd := partOffset + partSize
		var partDirty, partEvicted, partZero, partNonZero bool
		var lastBufferEnd uint64
		inode.buffers.Ascend(partOffset+1, func(end uint64, buf *FileBuffer) (cont bool, changed bool) {
			if buf.offset >= partEnd {
				return false, false
			}
			partDirty = partDirty || buf.state == BUF_DIRTY
			partEvicted = partEvicted || buf.state == BUF_FL_CLEARED
			partZero = partZero || buf.zero
			partNonZero = partNonZero || !buf.zero
			lastBufferEnd = end
			return true, false
		})
		partZero = partZero || lastBufferEnd < partEnd
		if !partDirty || partEvicted {
			// Don't flush parts which require RMW with evicted buffers
			anyEvicted = anyEvicted || partEvicted
		} else if partZero && !flushInode {
			// Don't flush parts with empty ranges UNLESS we're under memory pressure
			// and tried all parts without empty ranges, because flushing them may be
			// the only way to free memory
			if !partNonZero && priority >= 3 {
				fullyZero = append(fullyZero, partNum)
			}
			if wantFree && partNonZero && priority >= 2 {
				partlyZero = append(partlyZero, partNum)
			}
			shouldComplete = false
		} else {
			shouldComplete = false
			if inode.goFlushPart(partNum, partOffset, partSize, 1) {
				initiated = true
				if inode.flushLimitsExceeded() {
					return false
				}
			}
		}
		return true
	})
	if !initiated && len(partlyZero) > 0 {
		for _, partNum := range partlyZero {
			partOffset, partSize := inode.fs.partRange(partNum)
			initiated = initiated || inode.goFlushPart(partNum, partOffset, partSize, 2)
			if inode.flushLimitsExceeded() {
				break
			}
		}
	}
	if !initiated && anyEvicted && len(fullyZero) > 0 {
		for _, partNum := range partlyZero {
			partOffset, partSize := inode.fs.partRange(partNum)
			initiated = initiated || inode.goFlushPart(partNum, partOffset, partSize, 3)
			if inode.flushLimitsExceeded() {
				break
			}
		}
	}
	return initiated, shouldComplete
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) goFlushPart(partNum, partOffset, partSize uint64, priority uint64) bool {
	// Guard part against eviction
	inode.LockRange(partOffset, partSize, true)
	inode.IsFlushing++
	atomic.AddInt64(&inode.fs.stats.flushes, 1)
	atomic.AddInt64(&inode.fs.activeFlushers, 1)
	atomic.AddInt64(&inode.fs.flushPriorities[priority], 1)
	go func() {
		inode.mu.Lock()
		inode.flushPart(partNum)
		inode.UnlockRange(partOffset, partSize, true)
		inode.IsFlushing--
		inode.mu.Unlock()
		atomic.AddInt64(&inode.fs.flushPriorities[priority], -1)
		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.WakeupFlusher()
	}()
	return true
}

func (inode *Inode) uploadedAsMultipart() bool {
	return strings.Contains(inode.knownETag, "-")
}

func (inode *Inode) uploadMinMultipart() {
	inode.IsFlushing += inode.fs.flags.MaxParallelParts
	atomic.AddInt64(&inode.fs.activeFlushers, 1)

	cloud, key := inode.cloud()
	if inode.isDir() {
		key += "/"
	}

	go func() {
		defer func() {
			inode.IsFlushing -= inode.fs.flags.MaxParallelParts
			atomic.AddInt64(&inode.fs.activeFlushers, -1)
			inode.fs.WakeupFlusher()
			inode.mu.Unlock()
		}()

		atomic.AddInt64(&inode.fs.stats.flushes, 1)
		inode.beginMultipartUpload(cloud, key)
		if inode.mpu == nil {
			return
		}

		if ok := inode.syncFlushPartsUpTo(2); !ok {
			inode.abortMultipart()
			return
		}

		partOffset, partSize := inode.fs.partRange(1)
		if inode.Attributes.Size < partOffset+partSize {
			partSize = inode.Attributes.Size - partOffset
		}

		atomic.AddInt64(&inode.fs.stats.flushes, 1)
		inode.commitMultipartUpload(2, partOffset+partSize)
		if inode.mpu != nil {
			inode.abortMultipart()
		}
	}()
}

func (inode *Inode) syncFlushPartsUpTo(part uint64) bool {
	if inode.mpu == nil {
		return false
	}
	for i := uint64(0); i < part; i++ {
		atomic.AddInt64(&inode.fs.stats.flushes, 1)
		inode.flushPart(i)
		if inode.mpu == nil || inode.mpu.Parts[i] == nil {
			return false
		}
	}
	return true
}

func (inode *Inode) patchObjectRanges() (initiated bool) {
	smallFile := inode.Attributes.Size <= inode.fs.flags.SinglePartMB*1024*1024
	wantFlush := inode.fileHandles == 0 || inode.forceFlush || atomic.LoadInt32(&inode.fs.wantFree) > 0

	if smallFile {
		if inode.flushLimitsExceeded() || !wantFlush {
			return
		}
		flushBufs := inode.buffers.Select(0, inode.Attributes.Size, func(buf *FileBuffer) bool { return buf.state == BUF_DIRTY })
		inode.patchSimpleObj(flushBufs)
		return true
	}

	updatedPartID := inode.fs.partNum(inode.lastWriteEnd)

	inode.buffers.IterateDirtyParts(func(part uint64) bool {
		if inode.flushLimitsExceeded() {
			return false
		}

		partStart, partSize := inode.fs.partRange(part)
		// In its current implementation PATCH doesn't support ranges with start offset larger than object size.
		if partStart > inode.knownSize {
			return false
		}

		var prevPart uint64
		if part > 0 {
			prevPart = part - 1
		}
		_, prevSize := inode.fs.partRange(prevPart)

		partEnd, rangeBorder := partStart+partSize, partSize != prevSize
		appendPatch, newPart := partEnd > inode.knownSize, partStart == inode.knownSize

		// When entering a new part range, we can't immediately switch to the new part size,
		// because we need to init a new part first.
		if newPart && rangeBorder {
			partEnd, partSize = partStart+prevSize, prevSize
		}

		smallTail := appendPatch && inode.Attributes.Size-partStart < partSize
		if smallTail && !wantFlush {
			return false
		}

		partLocked := inode.IsRangeLocked(partStart, partEnd, true)
		if partLocked || !wantFlush && part == updatedPartID {
			return true
		}

		inode.buffers.SplitAt(partStart)
		inode.buffers.SplitAt(partEnd)

		flushBufs := inode.buffers.Select(partStart, partEnd, func(buf *FileBuffer) bool {
			return buf.state == BUF_DIRTY && (!buf.zero || wantFlush || appendPatch)
		})
		if len(flushBufs) != 0 {
			inode.patchPart(partStart, partSize, flushBufs)
			initiated = true
		}
		return true
	})
	return
}

func (inode *Inode) flushLimitsExceeded() bool {
	return atomic.LoadInt64(&inode.fs.activeFlushers) >= inode.fs.flags.MaxFlushers ||
		inode.IsFlushing >= inode.fs.flags.MaxParallelParts
}

func (inode *Inode) patchSimpleObj(bufs []*FileBuffer) {
	size := inode.Attributes.Size

	inode.LockRange(0, size, true)
	inode.IsFlushing += inode.fs.flags.MaxParallelParts
	atomic.AddInt64(&inode.fs.stats.flushes, 1)
	atomic.AddInt64(&inode.fs.activeFlushers, 1)

	go func() {
		inode.mu.Lock()
		inode.patchFromBuffers(bufs, inode.fs.flags.SinglePartMB*1024*1024)

		inode.UnlockRange(0, size, true)
		inode.IsFlushing -= inode.fs.flags.MaxParallelParts
		inode.mu.Unlock()

		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.WakeupFlusher()
	}()
}

func (inode *Inode) patchPart(partOffset, partSize uint64, bufs []*FileBuffer) {
	inode.LockRange(partOffset, partSize, true)
	inode.IsFlushing++
	atomic.AddInt64(&inode.fs.stats.flushes, 1)
	atomic.AddInt64(&inode.fs.activeFlushers, 1)

	go func() {
		inode.mu.Lock()
		inode.patchFromBuffers(bufs, partSize)

		inode.UnlockRange(partOffset, partSize, true)
		inode.IsFlushing--
		inode.mu.Unlock()

		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.WakeupFlusher()
	}()
}

func (inode *Inode) patchFromBuffers(bufs []*FileBuffer, partSize uint64) {
	if len(bufs) == 0 {
		return
	}

	first, last := bufs[0], bufs[len(bufs)-1]
	offset, size := first.offset, last.offset+last.length-first.offset

	var bufsSize uint64
	for _, b := range bufs {
		bufsSize += b.length
	}
	contiguous := bufsSize == size

	// If bufs is a contiguous range of buffers then we can send them as PATCH immediately,
	// otherwise we need to read missing ranges first.
	var (
		reader    io.ReadSeeker
		dirtyBufs map[uint64]bool
	)
	if contiguous {
		dirtyBufs = make(map[uint64]bool)
		r := NewMultiReader()
		for _, buf := range bufs {
			dirtyBufs[buf.dirtyID] = true
			if !buf.zero {
				r.AddBuffer(buf.data)
			} else {
				r.AddZero(buf.length)
			}
		}
		reader = r
	} else {
		key := inode.FullName()
		_, err := inode.LoadRange(offset, size, 0, true)
		if err != nil {
			switch mapAwsError(err) {
			case syscall.ENOENT, syscall.ERANGE:
				s3Log.Warnf("File %s (inode %d) is deleted or resized remotely, discarding all local changes", key, inode.Id)
				inode.resetCache()
			default:
				log.Errorf("Failed to load range %d-%d of file %s (inode %d) to patch it: %s", offset, offset+size, key, inode.Id, err)
			}
			return
		}
		// File size or inode state may have been changed again, abort patch. These are local changes,
		// so we don't need to drop any cached state here.
		if inode.Attributes.Size < offset || inode.CacheState != ST_MODIFIED {
			log.Warnf("Local state of file %s (inode %d) changed, aborting patch", key, inode.Id)
			return
		}
		reader, dirtyBufs, err = inode.getMultiReader(offset, size)
		if err != nil {
			log.Errorf("File %s data in %v+%v is missing during PATCH attempt: %v", key, offset, size, err)
			return
		}
	}

	if ok := inode.sendPatch(offset, size, reader, partSize); !ok {
		return
	}

	inode.buffers.SetState(offset, size, dirtyBufs, BUF_CLEAN)
	if !inode.isStillDirty() {
		inode.SetCacheState(ST_CACHED)
	}
}

func (inode *Inode) sendPatch(offset, size uint64, r io.ReadSeeker, partSize uint64) bool {
	cloud, key := inode.cloud()
	if inode.oldParent != nil {
		_, key = inode.oldParent.cloud()
		key = appendChildName(key, inode.oldName)
	}
	log.Debugf("Patching range %d-%d of file %s (inode %d)", offset, offset+size, key, inode.Id)

	inode.mu.Unlock()
	inode.fs.addInflightChange(key)
	resp, err := cloud.PatchBlob(&PatchBlobInput{
		Key:            key,
		Offset:         offset,
		Size:           size,
		AppendPartSize: int64(partSize),
		Body:           r,
	})
	inode.fs.completeInflightChange(key)
	inode.mu.Lock()

	// File was deleted while we were flushing it
	if inode.CacheState == ST_DELETED {
		return false
	}

	inode.recordFlushError(err)
	if err != nil {
		switch mapAwsError(err) {
		case syscall.ENOENT, syscall.ERANGE:
			s3Log.Warnf("File %s (inode %d) is deleted or resized remotely, discarding all local changes", key, inode.Id)
			inode.resetCache()
		case syscall.EBUSY:
			s3Log.Warnf("Failed to patch range %d-%d of file %s (inode %d) due to concurrent updates", offset, offset+size, key, inode.Id)
			if inode.fs.flags.DropPatchConflicts {
				inode.discardChanges(offset, size)
			}
		default:
			log.Errorf("Failed to patch range %d-%d of file %s (inode %d): %s", offset, offset+size, key, inode.Id, err)
		}
		return false
	}

	log.Debugf("Succesfully patched range %d-%d of file %s (inode %d), etag: %s", offset, offset+size, key, inode.Id, NilStr(resp.ETag))
	inode.updateFromFlush(MaxUInt64(inode.knownSize, offset+size), resp.ETag, resp.LastModified, nil)
	return true
}

func (inode *Inode) discardChanges(offset, size uint64) {
	allocated := inode.buffers.RemoveRange(offset, size, nil)
	inode.fs.bufferPool.Use(allocated, true)
}

func (inode *Inode) isStillDirty() bool {
	if inode.userMetadataDirty != 0 || inode.oldParent != nil || inode.Attributes.Size != inode.knownSize {
		return true
	}
	return inode.buffers.AnyUnclean()
}

func (inode *Inode) resetCache() {
	// Drop all buffers including dirty ones
	allocated := inode.buffers.RemoveRange(0, 0xffffffffffffffff, nil)
	inode.fs.bufferPool.Use(allocated, true)
	// Also remove the cache file from disk, if present
	if inode.OnDisk {
		if inode.DiskCacheFD != nil {
			inode.DiskCacheFD.Close()
			inode.DiskCacheFD = nil
			inode.fs.diskFdQueue.DeleteFD(inode)
		}
		cacheFileName := inode.fs.flags.CachePath + "/" + inode.FullName()
		err := os.Remove(cacheFileName)
		if err != nil {
			log.Errorf("Couldn't remove %v: %v", cacheFileName, err)
		} else {
			inode.OnDisk = false
		}
	}
	// And abort multipart upload, too
	if inode.mpu != nil {
		inode.abortMultipart()
	}
	inode.userMetadataDirty = 0
	inode.SetCacheState(ST_CACHED)
	// Invalidate metadata entry
	inode.SetAttrTime(time.Time{})
}

func (inode *Inode) abortMultipart() {
	cloud, key := inode.cloud()
	go func(mpu *MultipartBlobCommitInput) {
		_, abortErr := cloud.MultipartBlobAbort(mpu)
		if abortErr != nil {
			log.Warnf("Failed to abort multi-part upload of object %v: %v", key, abortErr)
		}
	}(inode.mpu)
	inode.mpu = nil
}

func (inode *Inode) flushSmallObject() {

	inode.mu.Lock()

	if inode.CacheState != ST_CREATED && inode.CacheState != ST_MODIFIED {
		inode.IsFlushing -= inode.fs.flags.MaxParallelParts
		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.WakeupFlusher()
		inode.mu.Unlock()
		return
	}

	sz := inode.Attributes.Size
	inode.LockRange(0, sz, true)

	if inode.CacheState == ST_MODIFIED {
		_, err := inode.LoadRange(0, sz, 0, true)
		mappedErr := mapAwsError(err)
		if mappedErr == syscall.ENOENT || mappedErr == syscall.ERANGE {
			// Object is deleted or resized remotely (416). Discard local version
			s3Log.Warnf("Conflict detected (inode %v): File %v is deleted or resized remotely, discarding local changes", inode.Id, inode.FullName())
			inode.resetCache()
			inode.IsFlushing -= inode.fs.flags.MaxParallelParts
			atomic.AddInt64(&inode.fs.activeFlushers, -1)
			inode.fs.WakeupFlusher()
			inode.mu.Unlock()
			return
		}
	}

	// Key may have been changed in between (if it was moved)
	cloud, key := inode.cloud()
	if inode.oldParent != nil {
		// In this case, modify it in the old place and move when we're done with modifications
		_, key = inode.oldParent.cloud()
		key = appendChildName(key, inode.oldName)
	}

	// File size may have been changed in between
	bufReader, bufIds, err := inode.getMultiReader(0, sz)
	if err != nil {
		inode.UnlockRange(0, sz, true)
		inode.IsFlushing -= inode.fs.flags.MaxParallelParts
		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.WakeupFlusher()
		inode.mu.Unlock()
		return
	}
	params := &PutBlobInput{
		Key:         key,
		Body:        bufReader,
		Size:        PUInt64(uint64(bufReader.Len())),
		ContentType: inode.fs.flags.GetMimeType(inode.FullName()),
	}
	if inode.userMetadataDirty != 0 {
		params.Metadata = escapeMetadata(inode.userMetadata)
		inode.userMetadataDirty = 0
	}

	if inode.mpu != nil {
		// Abort and forget abort multipart upload, because otherwise we may
		// not be able to proceed to rename - it waits until inode.mpu == nil
		inode.abortMultipart()
	}
	inode.mu.Unlock()
	inode.fs.addInflightChange(key)
	resp, err := cloud.PutBlob(params)
	inode.fs.completeInflightChange(key)
	inode.mu.Lock()

	inode.recordFlushError(err)
	if err != nil {
		log.Warnf("Failed to flush small file %v: %v", key, err)
		if params.Metadata != nil {
			inode.userMetadataDirty = 2
		}
	} else {
		log.Debugf("Flushed small file %v (inode %v): etag=%v, size=%v", key, inode.Id, NilStr(resp.ETag), sz)
		inode.buffers.SetState(0, sz, bufIds, BUF_CLEAN)
		inode.updateFromFlush(sz, resp.ETag, resp.LastModified, resp.StorageClass)
		if inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED {
			if !inode.isStillDirty() {
				inode.SetCacheState(ST_CACHED)
			} else {
				inode.SetCacheState(ST_MODIFIED)
			}
		}

		// Compute hash of file and store it in user metadata
		err = inode.finalizeAndHash()
		if err != nil {
			log.Warnf("Failed to finalize and hash object %v: %v", key, err)
		}

	}

	inode.UnlockRange(0, sz, true)
	inode.IsFlushing -= inode.fs.flags.MaxParallelParts
	atomic.AddInt64(&inode.fs.activeFlushers, -1)
	inode.fs.WakeupFlusher()
	inode.mu.Unlock()
}

func (inode *Inode) copyUnmodifiedParts(numParts uint64) (err error) {
	maxMerge := inode.fs.flags.MaxMergeCopyMB * 1024 * 1024

	// First collect ranges to be unaffected by sudden parallel changes
	var ranges []uint64
	var startPart, endPart uint64
	var startOffset, endOffset uint64
	for i := uint64(0); i < numParts; i++ {
		partOffset, partSize := inode.fs.partRange(i)
		partEnd := partOffset + partSize
		if partEnd > inode.Attributes.Size {
			partEnd = inode.Attributes.Size
		}
		if inode.mpu.Parts[i] == nil {
			if endPart == 0 {
				startPart, startOffset = i, partOffset
			}
			endPart, endOffset = i+1, partEnd
			if endOffset-startOffset >= maxMerge {
				ranges = append(ranges, startPart, startOffset, endOffset-startOffset)
				startPart, endPart = 0, 0
			}
		} else if endPart != 0 {
			ranges = append(ranges, startPart, startOffset, endOffset-startOffset)
			endPart = 0
		}
	}
	if endPart != 0 {
		ranges = append(ranges, startPart, startOffset, endOffset-startOffset)
	}
	if len(ranges) > 0 {
		cloud, key := inode.cloud()
		if inode.oldParent != nil {
			// Modify the object in the old place, move it when we're done with modifications
			_, key = inode.oldParent.cloud()
			key = appendChildName(key, inode.oldName)
		}
		mpu := inode.mpu
		guard := make(chan int, inode.fs.flags.MaxParallelCopy)
		var wg sync.WaitGroup
		inode.mu.Unlock()
		for i := 0; i < len(ranges); i += 3 {
			guard <- i
			if err != nil {
				break
			}
			wg.Add(1)
			go func(partNum, offset, size uint64) {
				inode.mu.Lock()
				if inode.mpu == nil {
					// Upload was canceled (file deleted)
					inode.mu.Unlock()
					err = syscall.ENOENT
				} else {
					inode.mu.Unlock()
					log.Debugf("Copying unmodified range %v-%v MB of object %v",
						offset/1024/1024, (offset+size+1024*1024-1)/1024/1024, key)
					resp, requestErr := cloud.MultipartBlobCopy(&MultipartBlobCopyInput{
						Commit:     mpu,
						PartNumber: uint32(partNum + 1),
						CopySource: key,
						Offset:     offset,
						Size:       size,
					})
					if requestErr != nil {
						log.Warnf("Failed to copy unmodified range %v-%v MB of object %v: %v",
							offset/1024/1024, (offset+size+1024*1024-1)/1024/1024, key, requestErr)
						err = requestErr
					} else {
						mpu.Parts[partNum] = resp.PartId
					}
				}
				wg.Done()
				<-guard
			}(uint64(ranges[i]), uint64(ranges[i+1]), uint64(ranges[i+2]))
		}
		wg.Wait()
		inode.mu.Lock()
	}
	return
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) flushPart(part uint64) {

	partOffset, partSize := inode.fs.partRange(part)
	partFullSize := partSize

	cloud, key := inode.cloud()
	if inode.oldParent != nil {
		// Always apply modifications before moving
		_, key = inode.oldParent.cloud()
		key = appendChildName(key, inode.oldName)
	}
	log.Debugf("Flushing part %v (%v-%v MB) of %v", part, partOffset/1024/1024, (partOffset+partSize)/1024/1024, key)

	// Last part may be shorter
	if inode.Attributes.Size < partOffset+partSize {
		partSize = inode.Attributes.Size - partOffset
	}

	// Load part from the server if we have to read-modify-write it
	if inode.CacheState == ST_MODIFIED {
		// Ignore memory limit to not produce a deadlock when we need to free some memory
		// by flushing objects, but we can't flush a part without allocating more memory
		// for read-modify-write...
		_, err := inode.LoadRange(partOffset, partSize, 0, true)
		if err == syscall.ESPIPE {
			// Part is partly evicted, we can't flush it
			log.Warnf("Could not flush part %v (%v-%v) of object %v because it's partly evicted", part, partOffset, partSize, key)
			return
		}
		mappedErr := mapAwsError(err)
		if mappedErr == syscall.ENOENT || mappedErr == syscall.ERANGE {
			// Object is deleted or resized remotely (416). Discard local version
			s3Log.Warnf("Conflict detected (inode %v): File %v is deleted or resized remotely, discarding local changes", inode.Id, inode.FullName())
			inode.resetCache()
			return
		}
		if err != nil {
			log.Warnf("Failed to load part %v of object %v to flush it: %v", part, key, err)
			return
		}
		// File size may have been changed again
		if inode.Attributes.Size <= partOffset || inode.CacheState != ST_MODIFIED {
			// Abort flush
			return
		}
		if inode.Attributes.Size < partOffset+partSize {
			partSize = inode.Attributes.Size - partOffset
		}
	}

	if inode.mpu == nil {
		// Multipart upload was canceled in the meantime => don't flush
		return
	}

	// Finally upload it
	bufReader, bufIds, err := inode.getMultiReader(partOffset, partSize)
	if err != nil {
		log.Errorf("BUG: Failed to get MultiReader for flushed part %v (%v-%v) of object %v: %v", part, partOffset, partSize, key, err)
		return
	}
	bufLen := bufReader.Len()
	partInput := MultipartBlobAddInput{
		Commit:     inode.mpu,
		PartNumber: uint32(part + 1),
		Body:       bufReader,
		Size:       bufLen,
		Offset:     partOffset,
	}
	inode.mu.Unlock()
	resp, err := cloud.MultipartBlobAdd(&partInput)
	inode.mu.Lock()

	if inode.CacheState == ST_DELETED {
		// File was deleted while we were flushing it
		return
	}
	inode.recordFlushError(err)
	if err != nil {
		log.Warnf("Failed to flush part %v of object %v: %v", part, key, err)
		mappedErr := mapAwsError(err)
		if mappedErr == syscall.ENOENT {
			// Multipart upload is deleted
			s3Log.Warnf("Conflict detected (inode %v): Multipart upload of file %v is aborted remotely, discarding local changes", inode.Id, inode.FullName())
			inode.resetCache()
		}
	} else {
		if inode.mpu != nil {
			// It could become nil if the file was deleted remotely in the meantime
			inode.mpu.Parts[part] = resp.PartId
		}
		doneState := BUF_FLUSHED_FULL
		if bufLen < partFullSize || part == 0 {
			// Do not evict modified header (first part)
			doneState = BUF_FLUSHED_CUT
		}
		log.Debugf("Flushed part %v of object %v", part, key)
		inode.buffers.SetState(partOffset, partSize, bufIds, doneState)
	}
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) completeMultipart() {
	// Server-side copy unmodified parts
	if inode.mpu == nil {
		// Multipart upload was canceled in the meantime (by a parallel conflict) => do not complete
		return
	}
	finalSize := inode.Attributes.Size
	numParts := inode.fs.partNum(finalSize)
	numPartOffset, _ := inode.fs.partRange(numParts)
	if numPartOffset < finalSize {
		numParts++
	}
	err := inode.copyUnmodifiedParts(numParts)
	if !(inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED) {
		// State changed, abort this flush (even if we get ENOENT)
		return
	}
	mappedErr := mapAwsError(err)
	if mappedErr == syscall.ENOENT || mappedErr == syscall.ERANGE {
		// Object is deleted or resized remotely (416). Discard local version
		s3Log.Warnf("Conflict detected (inode %v): File %v is deleted or resized remotely, discarding local changes", inode.Id, inode.FullName())
		inode.resetCache()
		return
	}
	inode.recordFlushError(err)
	if err != nil || inode.CacheState != ST_CREATED && inode.CacheState != ST_MODIFIED ||
		inode.mpu == nil {
		// Error, or already flushed, or conflict => do not complete
		return
	}
	inode.commitMultipartUpload(numParts, finalSize)
}

func (inode *Inode) commitMultipartUpload(numParts, finalSize uint64) {
	cloud, key := inode.cloud()
	if inode.oldParent != nil {
		// Always apply modifications before moving
		_, key = inode.oldParent.cloud()
		key = appendChildName(key, inode.oldName)
	}
	// Finalize the upload
	mpu := inode.mpu
	mpu.NumParts = uint32(numParts)
	inode.mu.Unlock()
	inode.fs.addInflightChange(key)
	resp, err := cloud.MultipartBlobCommit(mpu)
	inode.fs.completeInflightChange(key)
	inode.mu.Lock()
	if inode.mpu != mpu || inode.CacheState != ST_CREATED && inode.CacheState != ST_MODIFIED {
		// Already flushed or conflict => do not complete
		return
	}
	inode.recordFlushError(err)
	if err != nil {
		log.Warnf("Failed to finalize multi-part upload of object %v: %v", key, err)
		if inode.mpu.Metadata != nil {
			inode.userMetadataDirty = 2
		}
	} else {
		log.Debugf("Finalized multi-part upload of object %v: etag=%v, size=%v", key, NilStr(resp.ETag), finalSize)

		// Compute hash of file and store it in user metadata
		err := inode.finalizeAndHash()
		if err != nil {
			log.Warnf("Failed to finalize and hash object %v: %v", key, err)
		}

		if inode.userMetadataDirty == 1 {
			inode.userMetadataDirty = 0
		}
		inode.mpu = nil
		inode.buffers.SetFlushedClean()
		inode.updateFromFlush(finalSize, resp.ETag, resp.LastModified, resp.StorageClass)
		if inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED {
			if !inode.isStillDirty() {
				inode.SetCacheState(ST_CACHED)
			} else {
				inode.SetCacheState(ST_MODIFIED)
			}
		}
	}
}

func (inode *Inode) finalizeAndHash() error {
	if inode.isDir() {
		return nil
	}

	if inode.fs.flags.HashAttr == "" {
		return nil
	}

	// Assume the inode is locked and multipart upload is already finalized
	reader, _, err := inode.getMultiReader(0, inode.Attributes.Size)
	if err != nil {
		return err
	}

	hasher := sha256.New()
	if _, err := io.Copy(hasher, reader); err != nil {
		return err
	}
	hash := hex.EncodeToString(hasher.Sum(nil))

	log.Debugf("Computed and stored hash of file '%s': %s", inode.FullName(), hash)

	if inode.userMetadata == nil {
		inode.userMetadata = make(map[string][]byte)
	}

	inode.userMetadata[inode.fs.flags.HashAttr] = []byte(hash)
	inode.sendUpdateMeta()

	inode.fs.CacheFileInExternalCache(inode)

	return nil
}

func (inode *Inode) updateFromFlush(size uint64, etag *string, lastModified *time.Time, storageClass *string) {
	if etag != nil {
		inode.s3Metadata["etag"] = []byte(*etag)
	}
	if storageClass != nil {
		inode.s3Metadata["storage-class"] = []byte(*storageClass)
	}
	if lastModified != nil {
		inode.Attributes.Ctime = *lastModified
	}
	inode.knownSize = size
	inode.knownETag = *etag
	inode.SetAttrTime(time.Now())
}

func (inode *Inode) SyncFile() (err error) {
	inode.logFuse("SyncFile")
	for {
		inode.mu.Lock()
		inode.forceFlush = false
		if inode.CacheState <= ST_DEAD {
			inode.mu.Unlock()
			break
		}
		if inode.flushError != nil {
			// Return the error to user
			err = inode.flushError
			inode.mu.Unlock()
			break
		}
		inode.forceFlush = true
		inode.mu.Unlock()
		inode.TryFlush(MAX_FLUSH_PRIORITY)
		inode.fs.flusherMu.Lock()
		if inode.fs.flushPending == 0 {
			inode.fs.flusherCond.Wait()
		}
		inode.fs.flusherMu.Unlock()
	}
	inode.logFuse("Done SyncFile")
	return
}

func (inode *Inode) SetAttributes(size *uint64, mode *os.FileMode,
	mtime *time.Time, uid *uint32, gid *uint32) (err error) {

	if inode.Parent == nil {
		// chmod/chown on the root directory of mountpoint is not supported
		return syscall.ENOTSUP
	}

	fs := inode.fs

	if size != nil || mode != nil || mtime != nil || uid != nil || gid != nil {
		inode.mu.Lock()
		if inode.CacheState == ST_DELETED || inode.CacheState == ST_DEAD {
			// Oops, it's a deleted file. We don't support changing invisible files
			inode.mu.Unlock()
			return syscall.ENOENT
		}
	}

	modified := false

	if size != nil && inode.Attributes.Size != *size {
		if *size > fs.getMaxFileSize() {
			// File size too large
			log.Warnf(
				"Maximum file size exceeded when trying to truncate %v to %v bytes",
				inode.FullName(), *size,
			)
			inode.mu.Unlock()
			return syscall.EFBIG
		}
		inode.ResizeUnlocked(*size, true)
		modified = true
	}

	if mode != nil {
		m, err := inode.setFileMode(*mode)
		if err != nil {
			inode.mu.Unlock()
			return err
		}
		modified = modified || m
	}

	if mtime != nil && fs.flags.EnableMtime && inode.Attributes.Mtime != *mtime {
		inode.Attributes.Mtime = *mtime
		err = inode.setUserMeta(fs.flags.MtimeAttr, []byte(fmt.Sprintf("%d", inode.Attributes.Mtime.Unix())))
		if err != nil {
			inode.mu.Unlock()
			return err
		}
		modified = true
	}

	if uid != nil && fs.flags.EnablePerms && inode.Attributes.Uid != *uid {
		inode.Attributes.Uid = *uid
		if inode.Attributes.Uid != fs.flags.Uid {
			err = inode.setUserMeta(fs.flags.UidAttr, []byte(fmt.Sprintf("%d", inode.Attributes.Uid)))
		} else {
			err = inode.setUserMeta(fs.flags.UidAttr, nil)
		}
		if err != nil {
			inode.mu.Unlock()
			return err
		}
		modified = true
	}

	if gid != nil && fs.flags.EnablePerms && inode.Attributes.Gid != *gid {
		inode.Attributes.Gid = *gid
		if inode.Attributes.Gid != fs.flags.Gid {
			err = inode.setUserMeta(fs.flags.GidAttr, []byte(fmt.Sprintf("%d", inode.Attributes.Gid)))
		} else {
			err = inode.setUserMeta(fs.flags.GidAttr, nil)
		}
		if err != nil {
			inode.mu.Unlock()
			return err
		}
		modified = true
	}

	if modified && inode.CacheState == ST_CACHED {
		inode.SetCacheState(ST_MODIFIED)
		inode.fs.WakeupFlusher()
	}

	if size != nil || mode != nil || mtime != nil || uid != nil || gid != nil {
		inode.mu.Unlock()
	}

	return
}
