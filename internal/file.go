// Copyright 2015 - 2017 Ka-Hing Cheung
// Copyright 2021 Vitaliy Filippov
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
//	"errors"
//	"fmt"
	"io"
	"io/ioutil"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
)

type FileHandle struct {
	inode *Inode
}

const MAX_BUF = 5 * 1024 * 1024

// NewFileHandle returns a new file handle for the given `inode` triggered by fuse
// operation with the given `opMetadata`
func NewFileHandle(inode *Inode, opMetadata fuseops.OpMetadata) *FileHandle {
	fh := &FileHandle{inode: inode}
	return fh
}

func (fs *Goofys) partNum(offset uint64) uint64 {
	// 5 MB
	n := offset/(5*1024*1024)
	if n < 1000 {
		return n
	}
	// 25 MB
	n = (n-1000)/5
	if n < 1000 {
		return 1000 + n
	}
	// 125 MB
	n = (n-1000)/5
	return 2000 + n
}

func (fs *Goofys) partRange(num uint64) (offset uint64, size uint64) {
	if num < 1000 {
		// 5 MB
		size = 5*1024*1024
		offset = num*size
	} else if num < 2000 {
		// 25 MB
		size = 25*1024*1024
		offset = 1000*5*1024*1024 + (num-1000)*size
	} else {
		// 125 MB
		size = 125*1024*1024
		offset = 1000*5*1024*1024 + 1000*25*1024*1024 + (num-2000)*size
	}
	return
}

func locateBuffer(buffers []FileBuffer, offset uint64) int {
	return sort.Search(len(buffers), func(i int) bool {
		return buffers[i].offset + buffers[i].length > offset
	})
}

func (inode *Inode) appendBuffer(buf *FileBuffer, data []byte) int64 {
	allocated := int64(0)
	oldLen := len(buf.data)
	newLen := oldLen + len(data)
	if cap(buf.data) >= newLen {
		// It fits
		buf.data = buf.data[0 : newLen]
		buf.length = uint64(newLen)
		copy(buf.data[oldLen : ], data)
	} else {
		// Reallocate
		newCap := newLen
		if newCap < 2*oldLen {
			newCap = 2*oldLen
		}
		allocated += int64(newCap)
		newData := make([]byte, newCap)
		copy(newData[0 : oldLen], buf.data)
		copy(newData[oldLen : newLen], data)
		buf.data = newData[0 : newLen]
		buf.length = uint64(newLen)
		// Refcount
		buf.ptr.refs--
		if buf.ptr.refs == 0 {
			allocated -= int64(len(buf.ptr.mem))
		}
		buf.ptr = &BufferPointer{
			mem: newData,
			refs: 1,
		}
	}
	return allocated
}

func (inode *Inode) insertBuffer(pos int, offset uint64, data []byte, dirty bool, copyData bool, dataPtr *BufferPointer) int64 {
	allocated := int64(0)
	dirtyID := uint64(0)
	if dirty {
		dirtyID = atomic.AddUint64(&inode.fs.bufferPool.curDirtyID, 1)
	}
	partStart, _ := inode.fs.partRange(inode.fs.partNum(offset))
	if copyData && pos > 0 &&
		!inode.buffers[pos-1].zero &&
		(inode.buffers[pos-1].offset + inode.buffers[pos-1].length) == offset &&
		offset != partStart &&
		(inode.buffers[pos-1].dirtyID != 0) == (dirtyID != 0) &&
		(cap(inode.buffers[pos-1].data) < len(inode.buffers[pos-1].data)+len(data) ||
			inode.buffers[pos-1].ptr.refs == 1 && len(inode.buffers[pos-1].data) <= MAX_BUF/2) {
		// We can append to the previous buffer if it doesn't result
		// in overwriting data that may be referenced by other buffers
		// This is profitable because a lot of tools write in small chunks
		inode.buffers[pos-1].dirtyID = dirtyID
		inode.buffers[pos-1].flushed = false
		allocated += inode.appendBuffer(&inode.buffers[pos-1], data)
	} else {
		var newBuf []byte
		if copyData {
			allocated += int64(len(data))
			newBuf = make([]byte, len(data))
			copy(newBuf, data)
			dataPtr = &BufferPointer{
				mem: newBuf,
				refs: 0,
			}
		} else {
			newBuf = data
		}
		dataPtr.refs++
		// Ugly insert()...
		// Why can't Golang do append(buffers[0:s], a, buffers[s+1]...) ?
		inode.buffers = append(inode.buffers, FileBuffer{})
		copy(inode.buffers[pos+1 : ], inode.buffers[pos : ])
		inode.buffers[pos] = FileBuffer{
			offset: offset,
			dirtyID: dirtyID,
			flushed: false,
			zero: false,
			length: uint64(len(newBuf)),
			data: newBuf,
			ptr: dataPtr,
		}
	}
	return allocated
}

func (inode *Inode) addBuffer(offset uint64, data []byte, dirty bool, copyData bool) int64 {
	allocated := int64(0)

	start := locateBuffer(inode.buffers, offset)
	dataLen := uint64(len(data))
	endOffset := offset+dataLen

	// Remove intersecting parts as they're being overwritten
	// If we're inserting a clean buffer, don't remove dirty ones
	for pos := start; pos < len(inode.buffers); pos++ {
		b := &inode.buffers[pos]
		if b.offset >= endOffset {
			break
		}
		bufEnd := b.offset+b.length
		if (dirty || b.dirtyID == 0) && bufEnd > offset && endOffset > b.offset {
			if offset <= b.offset {
				if endOffset >= bufEnd {
					// whole buffer
					if !b.zero {
						b.ptr.refs--
						if b.ptr.refs == 0 {
							allocated -= int64(len(b.ptr.mem))
						}
					}
					inode.buffers = append(inode.buffers[0 : pos], inode.buffers[pos+1 : ]...)
					pos--
				} else {
					// beginning
					if !b.zero {
						b.data = b.data[endOffset - b.offset : ]
					}
					b.length = bufEnd-endOffset
					b.offset = endOffset
				}
			} else if endOffset >= bufEnd {
				// end
				if !b.zero {
					b.data = b.data[0 : offset - b.offset]
				}
				b.length = offset - b.offset
			} else {
				// middle
				b.ptr.refs++
				startBuf := FileBuffer{
					offset: b.offset,
					dirtyID: b.dirtyID,
					flushed: b.flushed,
					length: offset-b.offset,
					zero: b.zero,
					ptr: b.ptr,
				}
				endBuf := FileBuffer{
					offset: endOffset,
					dirtyID: b.dirtyID,
					flushed: b.flushed,
					length: b.length-(endOffset-b.offset),
					zero: b.zero,
					ptr: b.ptr,
				}
				if !b.zero {
					startBuf.data = b.data[0 : offset-b.offset]
					endBuf.data = b.data[endOffset-b.offset : ]
				}
				if b.dirtyID != 0 {
					endBuf.dirtyID = atomic.AddUint64(&inode.fs.bufferPool.curDirtyID, 1)
				}
				// Ugly insert() again
				inode.buffers = append(inode.buffers, FileBuffer{})
				copy(inode.buffers[pos+2 : ], inode.buffers[pos+1 : ])
				inode.buffers[pos] = startBuf
				inode.buffers[pos+1] = endBuf
			}
		}
	}

	// Insert non-overlapping parts of the buffer
	curOffset := offset
	dataPtr := &BufferPointer{
		mem: data,
		refs: 0,
	}
	for pos := start; pos < len(inode.buffers) && curOffset < endOffset; pos++ {
		b := &inode.buffers[pos]
		if b.offset > curOffset {
			// insert curOffset->min(b.offset,endOffset)
			nextEnd := b.offset
			if nextEnd > endOffset {
				nextEnd = endOffset
			}
			allocated += inode.insertBuffer(pos, curOffset, data[curOffset-offset : nextEnd-offset], dirty, copyData, dataPtr)
		}
		curOffset = b.offset + b.length
	}
	if curOffset < endOffset {
		// Insert curOffset->endOffset
		allocated += inode.insertBuffer(len(inode.buffers), curOffset, data[curOffset-offset : ], dirty, copyData, dataPtr)
	}

	return allocated
}

func (inode *Inode) ResizeUnlocked(newSize uint64) {
	// Truncate or extend
	if inode.Attributes.Size > newSize && len(inode.buffers) > 0 {
		// Truncate - remove extra buffers
		end := len(inode.buffers)
		for end > 0 && inode.buffers[end-1].offset >= newSize {
			b := &inode.buffers[end-1]
			if !b.zero {
				b.ptr.refs--
				if b.ptr.refs == 0 {
					inode.fs.bufferPool.Use(-int64(len(b.ptr.mem)))
				}
			}
			end--
		}
		inode.buffers = inode.buffers[0 : end]
		if end > 0 {
			buf := &inode.buffers[end-1]
			if buf.offset + buf.length > newSize {
				buf.length = newSize - buf.offset
				buf.data = buf.data[0 : buf.length]
			}
		}
	}
	if inode.Attributes.Size < newSize {
		// Zero fill extended region
		inode.buffers = append(inode.buffers, FileBuffer{
			offset: inode.Attributes.Size,
			dirtyID: atomic.AddUint64(&inode.fs.bufferPool.curDirtyID, 1),
			length: newSize - inode.Attributes.Size,
			zero: true,
		})
	}
	inode.Attributes.Size = newSize
	if inode.CacheState == ST_CACHED {
		inode.SetCacheState(ST_MODIFIED)
		inode.fs.WakeupFlusher()
	}
}

func (fh *FileHandle) WriteFile(offset int64, data []byte) (err error) {
	fh.inode.logFuse("WriteFile", offset, len(data))

	end := uint64(offset)+uint64(len(data))

	// Try to reserve space without the inode lock
	fh.inode.fs.bufferPool.Use(int64(len(data)))

	fh.inode.mu.Lock()

	if fh.inode.Attributes.Size < end {
		// Extend and zero fill
		fh.inode.ResizeUnlocked(end)
	}

	allocated := fh.inode.addBuffer(uint64(offset), data, true, true)

	fh.inode.lastWriteEnd = end
	if fh.inode.CacheState == ST_CACHED {
		fh.inode.SetCacheState(ST_MODIFIED)
		// FIXME: Don't activate the flusher immediately for small writes
		fh.inode.fs.WakeupFlusher()
	}
	fh.inode.Attributes.Mtime = time.Now()

	fh.inode.mu.Unlock()

	// Correct memory usage
	if allocated != int64(len(data)) {
		fh.inode.fs.bufferPool.Use(allocated-int64(len(data)))
	}

	return
}

func appendRequest(requests []uint64, offset uint64, size uint64, requestCost uint64) []uint64 {
	if len(requests) > 0 {
		lastOffset := requests[len(requests)-2]
		lastSize := requests[len(requests)-1]
		if offset-lastOffset-lastSize <= requestCost {
			requests[len(requests)-1] = offset+size-lastOffset
			return requests
		}
	}
	return append(requests, offset, size)
}

// Load some inode data into memory
// Must be called with inode.mu taken
// Loaded range should be guarded against eviction by adding it into inode.readRanges
func (inode *Inode) LoadRange(offset uint64, size uint64, skipReadahead bool) (requestErr error) {

	end := offset+size

	// Collect requests to the server
	requests := make([]uint64, 0)
	start := locateBuffer(inode.buffers, offset)
	pos := offset
	for i := start; i < len(inode.buffers); i++ {
		b := &inode.buffers[i]
		if b.offset >= end {
			break
		}
		if b.offset > pos {
			requests = appendRequest(requests, pos, b.offset-pos, inode.fs.flags.ReadMergeKB*1024)
		}
		pos = b.offset+b.length
	}
	if pos < end {
		requests = appendRequest(requests, pos, end-pos, inode.fs.flags.ReadMergeKB*1024)
	}

	if len(requests) == 0 {
		return
	}

	// add readahead
	if !skipReadahead {
		nr := len(requests)
		lastEnd := requests[nr-2]+requests[nr-1] + inode.fs.flags.ReadAheadKB*1024
		if lastEnd > inode.Attributes.Size {
			lastEnd = inode.Attributes.Size
		}
		requests[nr-1] = lastEnd-requests[nr-2]
	}

	// FIXME split requests into smaller chunks if we want to read in parallel

	inode.mu.Unlock()

	// FIXME Don't issue requests if another read is already loading the same range

	// send requests
	var wg sync.WaitGroup
	cloud, key := inode.cloud()
	if inode.oldParent != nil {
		_, key = inode.oldParent.cloud()
		key = appendChildName(key, *inode.oldName)
	}
	for i := 0; i < len(requests); i += 2 {
		offset := requests[i]
		size := requests[i+1]
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Maybe free some buffers first
			inode.fs.bufferPool.Use(int64(size))
			resp, err := cloud.GetBlob(&GetBlobInput{
				Key:   key,
				Start: offset,
				Count: size,
			})
			if err != nil {
				inode.fs.bufferPool.Use(-int64(size))
				requestErr = err
				return
			}
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				inode.fs.bufferPool.Use(-int64(size))
				requestErr = err
				return
			}
			// Cache result from the server
			inode.mu.Lock()
			if inode.userMetadata == nil {
				// Cache xattrs
				inode.fillXattrFromHead(&(*resp).HeadBlobOutput)
			}
			allocated := inode.addBuffer(offset, data, false, false)
			inode.mu.Unlock()
			// Correct memory usage
			if allocated < int64(size) {
				inode.fs.bufferPool.Use(allocated-int64(size))
			}
		}()
	}
	wg.Wait()

	inode.mu.Lock()
	return
}

func (inode *Inode) LockRange(offset uint64, size uint64, flushing bool) {
	inode.readRanges = append(inode.readRanges, ReadRange{
		Offset: offset,
		Size: size,
		Flushing: flushing,
	})
}

func (inode *Inode) UnlockRange(offset uint64, size uint64, flushing bool) {
	for i, v := range inode.readRanges {
		if v.Offset == offset && v.Size == size && v.Flushing == flushing {
			inode.readRanges = append(inode.readRanges[0 : i], inode.readRanges[i+1 : ]...)
			break
		}
	}
}

func (inode *Inode) IsRangeLocked(offset uint64, size uint64, onlyFlushing bool) bool {
	for _, r := range inode.readRanges {
		if r.Offset < offset+size &&
			r.Offset+r.Size >= offset &&
			(!onlyFlushing || r.Flushing) {
			return true
		}
	}
	return false
}

func (fh *FileHandle) ReadFile(sOffset int64, buf []byte) (bytesRead int, err error) {
	offset := uint64(sOffset)

	fh.inode.logFuse("ReadFile", offset, len(buf))
	defer func() {
		fh.inode.logFuse("< ReadFile", bytesRead, err)
		if err != nil {
			if err == io.EOF {
				err = nil
			}
		}
	}()

	if offset >= fh.inode.Attributes.Size {
		// nothing to read
		err = io.EOF
		return
	}
	end := offset+uint64(len(buf))
	if end >= fh.inode.Attributes.Size {
		end = fh.inode.Attributes.Size
	}

	// Guard buffers against eviction
	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()
	fh.inode.LockRange(offset, end-offset, false)
	defer fh.inode.UnlockRange(offset, end-offset, false)

	// Don't read anything from the server if the file is just created
	// FIXME: Track random reads and temporarily disable readahead, as in the original
	var requestErr error
	if fh.inode.CacheState != ST_CREATED {
		requestErr = fh.inode.LoadRange(offset, end-offset, false)
	}

	// copy cached buffers into the result
	start := locateBuffer(fh.inode.buffers, offset)
	pos := offset
	for i := start; i < len(fh.inode.buffers); i++ {
		b := &fh.inode.buffers[i]
		if b.offset >= end {
			break
		}
		if b.offset > pos {
			// How is this possible? We should've just received it from the server!
			if fh.inode.CacheState == ST_CREATED {
				// It's okay if the file is just created
				// Zero empty ranges in this case
				memzero(buf[pos-offset : b.offset-offset])
				pos = b.offset
			} else {
				err = requestErr
				if err == nil {
					err = fuse.EIO
				}
				return
			}
		}
		readEnd := b.offset+b.length
		if readEnd > end {
			readEnd = end
		}
		if b.zero {
			memzero(buf[pos-offset : readEnd-offset])
		} else {
			copy(buf[pos-offset : readEnd-offset], b.data[pos-b.offset : readEnd-b.offset])
		}
		pos = readEnd
	}
	if pos < end {
		// How is this possible? We should've just received it from the server!
		if fh.inode.CacheState == ST_CREATED {
			// It's okay if the file is just created
			// Zero empty ranges in this case
			memzero(buf[pos-offset : end-offset])
			pos = end
		} else {
			err = requestErr
			if err == nil {
				err = fuse.EIO
			}
			return
		}
	}

	bytesRead = int(end-offset)

	return
}

func (fh *FileHandle) Release() {
	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()

	// LookUpInode accesses fileHandles without mutex taken, so use atomics for now
	n := atomic.AddInt32(&fh.inode.fileHandles, -1)
	if n == -1 {
		panic(fh.inode.fileHandles)
	} else if n == 0 {
		// delete fh
		fh.inode.fileHandle = nil
	}

	fh.inode.fs.WakeupFlusher()
}

func (inode *Inode) CheckLoadRange(offset uint64, size uint64) {
	loadStart := uint64(0)
	loadEnd := uint64(0)
	last := offset
	end := offset+size
	for i := locateBuffer(inode.buffers, offset); i < len(inode.buffers); i++ {
		buf := &inode.buffers[i]
		if buf.offset > last {
			if loadEnd == 0 {
				loadStart = last
			}
			loadEnd = buf.offset
		}
		last = buf.offset + buf.length
		if last >= end {
			break
		}
	}
	if last < end {
		if loadEnd == 0 {
			loadStart = last
		}
		loadEnd = end
	}
	if loadEnd > 0 {
		inode.LoadRange(loadStart, loadEnd-loadStart, true)
	}
}

func (inode *Inode) splitBuffer(i int, size uint64) {
	b := &inode.buffers[i]
	endBuf := FileBuffer{
		offset: b.offset+size,
		dirtyID: b.dirtyID,
		flushed: b.flushed,
		length: b.length-size,
		zero: b.zero,
		ptr: b.ptr,
	}
	b.length = size
	if !b.zero {
		endBuf.data = b.data[size : ]
		b.data = b.data[0 : size]
		endBuf.ptr.refs++
	}
	if b.dirtyID != 0 {
		endBuf.dirtyID = atomic.AddUint64(&inode.fs.bufferPool.curDirtyID, 1)
	}
	// Ugly insert() again
	inode.buffers = append(inode.buffers, FileBuffer{})
	copy(inode.buffers[i+2 : ], inode.buffers[i+1 : ])
	inode.buffers[i+1] = endBuf
}

func (inode *Inode) GetMultiReader(offset uint64, size uint64) (reader *MultiReader, bufIds map[uint64]bool) {
	reader = NewMultiReader()
	bufIds = make(map[uint64]bool)
	last := offset
	end := offset+size
	for i := locateBuffer(inode.buffers, offset); i < len(inode.buffers); i++ {
		b := &inode.buffers[i]
		if last < b.offset {
			// It can happen if the file is sparse. Then we have to zero-fill empty ranges
			reader.AddZero(b.offset-last)
		} else if last > b.offset {
			// Split the buffer as we need to track dirty state
			inode.splitBuffer(i, last-b.offset)
			continue
		}
		last = b.offset+b.length
		if last > end {
			// Split the buffer
			inode.splitBuffer(i, end-b.offset)
			b = &inode.buffers[i]
			last = b.offset+b.length
		}
		if b.dirtyID != 0 {
			bufIds[b.dirtyID] = true
		}
		if last >= end {
			if b.zero {
				reader.AddZero(end-b.offset)
			} else {
				reader.AddBuffer(b.data[0 : end-b.offset])
			}
			break
		} else {
			if b.zero {
				reader.AddZero(b.length)
			} else {
				reader.AddBuffer(b.data)
			}
		}
	}
	if last < end {
		// Again, can happen for new sparse files
		reader.AddZero(end-last)
	}
	return
}

func (inode *Inode) recordFlushError(err error) {
	inode.flushError = err
	inode.flushErrorTime = time.Now()
	if atomic.CompareAndSwapInt32(&inode.fs.flushRetrySet, 0, 1) {
		fs := inode.fs
		time.AfterFunc(fs.flags.RetryInterval, func() {
			atomic.StoreInt32(&fs.flushRetrySet, 0)
			// Wakeup flusher after retry interval
			fs.WakeupFlusher()
		})
	}
}

func (inode *Inode) TryFlush() bool {
	overDeleted := false
	if inode.Parent != nil {
		inode.Parent.mu.Lock()
		if inode.Parent.dir.DeletedChildren != nil {
			_, overDeleted = inode.Parent.dir.DeletedChildren[*inode.Name]
		}
		inode.Parent.mu.Unlock()
	}
	inode.mu.Lock()
	defer inode.mu.Unlock()
	if inode.flushError != nil && inode.flushErrorTime.Sub(time.Now()) < inode.fs.flags.RetryInterval {
		return false
	}
	if inode.CacheState == ST_DELETED {
		if inode.IsFlushing == 0 && (!inode.isDir() || atomic.LoadInt64(&inode.dir.ModifiedChildren) == 0) {
			inode.SendDelete()
			return true
		}
	} else if inode.CacheState == ST_CREATED && inode.isDir() {
		if inode.IsFlushing == 0 && !overDeleted {
			inode.SendMkDir()
			return true
		}
	} else if inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED {
		if overDeleted {
			return false
		}
		return inode.SendUpload()
	}
	return false
}

func (inode *Inode) SendUpload() bool {

	cloud, key := inode.cloud()
	if inode.isDir() {
		key += "/"
	}

	if inode.oldParent != nil && inode.IsFlushing == 0 && inode.mpu == nil {
		inode.IsFlushing += inode.fs.flags.MaxParallelParts
		atomic.AddInt64(&inode.fs.activeFlushers, 1)
		_, from := inode.oldParent.cloud()
		from = appendChildName(from, *inode.oldName)
		oldParent := inode.oldParent
		oldName := inode.oldName
		// Set to nil so another rename will move it again
		inode.oldParent = nil
		inode.oldName = nil
		skipRename := false
		if inode.isDir() {
			from += "/"
			skipRename = true
		}
		go func() {
			var err error
			if !inode.fs.flags.NoDirObject {
				err = RenameObject(cloud, from, key, nil)
				if err == syscall.ENOENT && skipRename {
					// Rename the old directory object to copy xattrs from it if it has them
					// We're almost never sure if the directory is implicit so we always try
					// to rename the directory object
					err = nil
				}
			}

			if err == nil {
				// Remove from DeletedChildren of the old parent
				oldParent.mu.Lock()
				delete(oldParent.dir.DeletedChildren, *oldName)
				oldParent.mu.Unlock()
				// And track ModifiedChildren because rename is special - it takes two parents
				oldParent.addModified(-1)
			}

			inode.mu.Lock()
			inode.recordFlushError(err)
			var unmoveParent *Inode
			var unmoveName *string
			if err != nil {
				log.Errorf("Error renaming object from %v to %v: %v", from, key, err)
				unmoveParent = inode.oldParent
				unmoveName = inode.oldName
				inode.oldParent = oldParent
				inode.oldName = oldName
			} else if (inode.CacheState == ST_MODIFIED || inode.CacheState == ST_CREATED) &&
				!inode.isStillDirty() {
				inode.SetCacheState(ST_CACHED)
			}
			inode.IsFlushing -= inode.fs.flags.MaxParallelParts
			atomic.AddInt64(&inode.fs.activeFlushers, -1)
			inode.fs.WakeupFlusher()
			inode.mu.Unlock()

			// "Undo" the second move
			if unmoveParent != nil {
				unmoveParent.mu.Lock()
				delete(unmoveParent.dir.DeletedChildren, *unmoveName)
				unmoveParent.mu.Unlock()
				unmoveParent.addModified(-1)
			}
		}()
		return true
	}

	if inode.CacheState == ST_MODIFIED && inode.userMetadataDirty &&
		inode.oldParent == nil && inode.IsFlushing == 0 {
		hasDirty := false
		for i := 0; i < len(inode.buffers); i++ {
			buf := &inode.buffers[i]
			if buf.dirtyID != 0 {
				hasDirty = true
				break
			}
		}
		if !hasDirty {
			// Update metadata by COPYing into the same object
			// It results in the optimized implementation in S3
			inode.userMetadataDirty = false
			inode.IsFlushing += inode.fs.flags.MaxParallelParts
			atomic.AddInt64(&inode.fs.activeFlushers, 1)
			go func() {
				_, err := cloud.CopyBlob(&CopyBlobInput{
					Source:      key,
					Destination: key,
					Size:        &inode.Attributes.Size,
					ETag:        PString(string(inode.s3Metadata["etag"])),
					Metadata:    convertMetadata(inode.userMetadata),
				})
				inode.mu.Lock()
				inode.recordFlushError(err)
				if err != nil {
					inode.userMetadataDirty = true
					log.Errorf("Error flushing metadata using COPY for %v: %v", key, err)
				} else if inode.CacheState == ST_MODIFIED && !inode.isStillDirty() {
					inode.SetCacheState(ST_CACHED)
				}
				inode.IsFlushing -= inode.fs.flags.MaxParallelParts
				atomic.AddInt64(&inode.fs.activeFlushers, -1)
				inode.fs.WakeupFlusher()
				inode.mu.Unlock()
			}()
			return true
		}
	}

	if inode.Attributes.Size <= inode.fs.flags.SinglePartMB*1024*1024 {
		// Don't flush small files with active file handles (if not under memory pressure)
		if inode.IsFlushing == 0 && (inode.fileHandles == 0 || inode.forceFlush || inode.fs.bufferPool.wantFree > 0) {
			// Don't accidentally trigger a parallel multipart flush
			inode.IsFlushing += inode.fs.flags.MaxParallelParts
			atomic.AddInt64(&inode.fs.activeFlushers, 1)
			go inode.FlushSmallObject()
			return true
		}
		return false
	}

	if inode.IsFlushing >= inode.fs.flags.MaxParallelParts {
		return false
	}

	// Initiate multipart upload, if not yet
	if inode.mpu == nil {
		inode.IsFlushing += inode.fs.flags.MaxParallelParts
		atomic.AddInt64(&inode.fs.activeFlushers, 1)
		go func() {
			resp, err := cloud.MultipartBlobBegin(&MultipartBlobBeginInput{
				Key: key,
				ContentType: inode.fs.flags.GetMimeType(key),
			})
			inode.mu.Lock()
			inode.recordFlushError(err)
			if err != nil {
				log.Errorf("Failed to initiate multipart upload for %v: %v", key, err)
			} else {
				inode.mpu = resp
			}
			inode.IsFlushing -= inode.fs.flags.MaxParallelParts
			atomic.AddInt64(&inode.fs.activeFlushers, -1)
			inode.fs.WakeupFlusher()
			inode.mu.Unlock()
		}()
		return true
	}

	// Pick part(s) to flush
	for i := 0; i < len(inode.buffers); i++ {
		buf := &inode.buffers[i]
		if buf.dirtyID != 0 && !buf.flushed && !inode.IsRangeLocked(buf.offset, buf.length, true) {
			part := inode.fs.partNum(buf.offset)
			// Don't write out the last part that's still written to (if not under memory pressure)
			if inode.fileHandles == 0 || inode.forceFlush ||
				inode.fs.bufferPool.wantFree > 0 || part != inode.fs.partNum(inode.lastWriteEnd) {
				// Found
				partOffset, partSize := inode.fs.partRange(part)
				// Guard part against eviction
				inode.LockRange(partOffset, partSize, true)
				inode.IsFlushing++
				atomic.AddInt64(&inode.fs.activeFlushers, 1)
				go inode.FlushPart(part)
				if atomic.LoadInt64(&inode.fs.activeFlushers) >= inode.fs.flags.MaxFlushers ||
					inode.IsFlushing >= inode.fs.flags.MaxParallelParts {
					return true
				}
			}
		}
	}

	return false
}

func (inode *Inode) isStillDirty() bool {
	if inode.userMetadataDirty || inode.oldParent != nil {
		return true
	}
	for i := 0; i < len(inode.buffers); i++ {
		b := &inode.buffers[i]
		if b.dirtyID != 0 {
			return true
		}
	}
	return false
}

func (inode *Inode) FlushSmallObject() {

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
		inode.CheckLoadRange(0, sz)
	}

	// Key may have been changed in between (if it was moved)
	cloud, key := inode.cloud()
	if inode.oldParent != nil {
		// In this case, modify it in the old place and move when we're done with modifications
		_, key = inode.oldParent.cloud()
		key = appendChildName(key, *inode.oldName)
	}
	// File size may have been changed in between
	bufReader, bufIds := inode.GetMultiReader(0, inode.Attributes.Size)
	params := &PutBlobInput{
		Key:         key,
		Body:        bufReader,
		Size:        PUInt64(uint64(bufReader.Len())),
		ContentType: inode.fs.flags.GetMimeType(*inode.FullName()),
	}
	if inode.userMetadataDirty {
		params.Metadata = convertMetadata(inode.userMetadata)
		inode.userMetadataDirty = false
	}

	inode.mu.Unlock()
	resp, err := cloud.PutBlob(params)
	inode.mu.Lock()

	inode.recordFlushError(err)
	if err != nil {
		log.Errorf("Failed to flush small file %v: %v", key, err)
		if params.Metadata != nil {
			inode.userMetadataDirty = true
		}
	} else {
		log.Debugf("Flushed small file %v", key)
		stillDirty := inode.userMetadataDirty || inode.oldParent != nil
		for i := 0; i < len(inode.buffers); i++ {
			b := &inode.buffers[i]
			if b.dirtyID != 0 {
				if bufIds[b.dirtyID] {
					// OK, not dirty anymore
					b.dirtyID = 0
					b.flushed = false
				} else {
					stillDirty = true
				}
			}
		}
		if inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED {
			if !stillDirty {
				inode.SetCacheState(ST_CACHED)
			} else {
				inode.SetCacheState(ST_MODIFIED)
			}
			if inode.fs.bufferPool.wantFree > 0 {
				inode.fs.bufferPool.cond.Broadcast()
			}
		}
		inode.updateFromFlush(resp.ETag, resp.LastModified, resp.StorageClass)
	}

	inode.UnlockRange(0, sz, true)
	inode.IsFlushing -= inode.fs.flags.MaxParallelParts
	atomic.AddInt64(&inode.fs.activeFlushers, -1)
	inode.fs.WakeupFlusher()
	inode.mu.Unlock()
}

func (inode *Inode) copyUnmodifiedRange(partNum, offset, size uint64) error {
	cloud, key := inode.cloud()
	log.Debugf("Copying unmodified range %v-%v MB of object %v", offset/1024/1024, (offset+size+1024*1024-1)/1024/1024, key)
	resp, err := cloud.MultipartBlobCopy(&MultipartBlobCopyInput{
		Commit:     inode.mpu,
		PartNumber: uint32(partNum+1),
		CopySource: key,
		Offset:     offset,
		Size:       size,
	})
	if err != nil {
		log.Errorf("Failed to copy unmodified range %v-%v MB of object %v: %v", offset/1024/1024, (offset+size+1024*1024-1)/1024/1024, key, err)
	} else {
		inode.mu.Lock()
		inode.mpu.Parts[partNum] = resp.PartId
		inode.mu.Unlock()
	}
	return err
}

func (inode *Inode) copyUnmodifiedParts(numParts uint64) (err error) {
	maxMerge := inode.fs.flags.MaxMergeCopyMB * 1024*1024

	// First collect ranges to be unaffected by sudden parallel changes
	var ranges []uint64
	var startPart, endPart uint64
	var startOffset, endOffset uint64
	for i := uint64(0); i < numParts; i++ {
		partOffset, partSize := inode.fs.partRange(i)
		partEnd := partOffset+partSize
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
		guard := make(chan int, inode.fs.flags.MaxParallelCopy)
		var wg sync.WaitGroup
		inode.mu.Unlock()
		for i := 0; i < len(ranges); i += 3 {
			guard <- i
			wg.Add(1)
			go func(part, offset, size uint64) {
				requestErr := inode.copyUnmodifiedRange(part, offset, size)
				if requestErr != nil {
					err = requestErr
				}
				wg.Done()
				<- guard
			}(uint64(ranges[i]), uint64(ranges[i+1]), uint64(ranges[i+2]))
		}
		wg.Wait()
		inode.mu.Lock()
	}
	return
}

func (inode *Inode) FlushPart(part uint64) {

	inode.mu.Lock()

	partOffset, partSize := inode.fs.partRange(part)

	defer func(partOffset, partSize uint64) {
		inode.UnlockRange(partOffset, partSize, true)
		inode.IsFlushing--
		inode.mu.Unlock()
		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.WakeupFlusher()
	} (partOffset, partSize)

	cloud, key := inode.cloud()
	if inode.oldParent != nil {
		// Always apply modifications before moving
		_, key = inode.oldParent.cloud()
		key = appendChildName(key, *inode.oldName)
	}
	log.Debugf("Flushing part %v (%v-%v MB) of %v", part, partOffset/1024/1024, (partOffset+partSize)/1024/1024, key)

	// Last part may be shorter
	if inode.Attributes.Size < partOffset+partSize {
		partSize = inode.Attributes.Size-partOffset
	}

	// Load part from the server if we have to read-modify-write it
	if inode.CacheState == ST_MODIFIED {
		inode.CheckLoadRange(partOffset, partSize)
		// File size may have been changed again
		if inode.Attributes.Size <= partOffset || inode.CacheState != ST_MODIFIED {
			// Abort flush
			return
		}
		if inode.Attributes.Size < partOffset+partSize {
			partSize = inode.Attributes.Size-partOffset
		}
	}

	// Finally upload it
	bufReader, bufIds := inode.GetMultiReader(partOffset, partSize)
	partInput := MultipartBlobAddInput{
		Commit:     inode.mpu,
		PartNumber: uint32(part+1),
		Body:       bufReader,
		Size:       bufReader.Len(),
		Offset:     partOffset,
	}
	inode.mu.Unlock()
	resp, err := cloud.MultipartBlobAdd(&partInput)
	inode.mu.Lock()

	inode.recordFlushError(err)
	if err != nil {
		log.Errorf("Failed to flush part %v of object %v: %v", part, key, err)
	} else {
		inode.mpu.Parts[part] = resp.PartId
		log.Debugf("Flushed part %v of object %v", part, key)
		stillDirty := false
		for i := 0; i < len(inode.buffers); i++ {
			b := &inode.buffers[i]
			if b.dirtyID != 0 {
				if bufIds[b.dirtyID] {
					// Still dirty because the upload is not completed yet,
					// but flushed to the server
					b.flushed = true
				} else if !b.flushed {
					stillDirty = true
				}
			}
		}
		if !stillDirty && (inode.fileHandles == 0 || inode.forceFlush || inode.fs.bufferPool.wantFree > 0) && (
			inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED) {
			// Server-size copy unmodified parts
			numParts := inode.fs.partNum(inode.Attributes.Size)
			numPartOffset, _ := inode.fs.partRange(numParts)
			if numPartOffset < inode.Attributes.Size {
				numParts++
			}
			err := inode.copyUnmodifiedParts(numParts)
			inode.recordFlushError(err)
			if err == nil && (inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED) {
				// Finalize the upload
				inode.mpu.NumParts = uint32(numParts)
				if inode.userMetadataDirty {
					inode.mpu.Metadata = convertMetadata(inode.userMetadata)
					inode.userMetadataDirty = false
				}
				inode.mu.Unlock()
				resp, err := cloud.MultipartBlobCommit(inode.mpu)
				inode.mu.Lock()
				if inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED {
					inode.recordFlushError(err)
					if err != nil {
						log.Errorf("Failed to finalize multi-part upload of object %v: %v", key, err)
						if inode.mpu.Metadata != nil {
							inode.userMetadataDirty = true
						}
					} else {
						inode.mpu = nil
						inode.updateFromFlush(resp.ETag, resp.LastModified, resp.StorageClass)
						stillDirty := inode.userMetadataDirty || inode.oldParent != nil
						for i := 0; i < len(inode.buffers); i++ {
							if inode.buffers[i].flushed {
								inode.buffers[i].dirtyID = 0
								inode.buffers[i].flushed = false
							}
							if inode.buffers[i].dirtyID != 0 {
								stillDirty = true
							}
						}
						if inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED {
							if !stillDirty {
								inode.SetCacheState(ST_CACHED)
							} else {
								inode.SetCacheState(ST_MODIFIED)
							}
							if inode.fs.bufferPool.wantFree > 0 {
								inode.fs.bufferPool.cond.Broadcast()
							}
						}
					}
				}
			} else {
				// FIXME: Abort multipart upload, but not just here
				// For example, we also should abort it if a partially flushed file is deleted
			}
		}
	}
}

func (inode *Inode) updateFromFlush(etag *string, lastModified *time.Time, storageClass *string) {
	if etag != nil {
		inode.s3Metadata["etag"] = []byte(*etag)
	}
	if storageClass != nil {
		inode.s3Metadata["storage-class"] = []byte(*storageClass)
	}
	if lastModified != nil {
		inode.Attributes.Mtime = *lastModified
	}
	inode.knownETag = etag
}

func (inode *Inode) SyncFile() (err error) {
	inode.logFuse("SyncFile")
	for true {
		inode.mu.Lock()
		inode.forceFlush = false
		if inode.CacheState == ST_CACHED {
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
		inode.TryFlush()
		inode.fs.flusherMu.Lock()
		if inode.fs.flushPending == 0 {
			inode.fs.flusherCond.Wait()
		}
		inode.fs.flusherMu.Unlock()
	}
	inode.logFuse("Done SyncFile")
	return
}
