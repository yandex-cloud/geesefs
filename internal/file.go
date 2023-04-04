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
	"fmt"
	"io"
	"os"
	"path"
	"sort"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
)

type FileHandle struct {
	inode *Inode
	lastReadEnd uint64
	seqReadSize uint64
	lastReadCount uint64
	lastReadTotal uint64
	lastReadSizes []uint64
	lastReadIdx int
}

// On Linux and MacOS, IOV_MAX = 1024
const IOV_MAX = 1024
const MAX_BUF = 5 * 1024 * 1024
const READ_BUF_SIZE = 128 * 1024

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
	panic(fmt.Sprintf(
		"Offset too large: %v, max supported file size with current part size configuration is %v",
		offset, start,
	))
}

func (fs *Goofys) partRange(num uint64) (offset uint64, size uint64) {
	n := uint64(0)
	start := uint64(0)
	for _, s := range fs.flags.PartSizes {
		if num < n + s.PartCount {
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

func locateBuffer(buffers []*FileBuffer, offset uint64) int {
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

func (inode *Inode) insertOrAppendBuffer(pos int, offset uint64, data []byte, state int16, copyData bool, dataPtr *BufferPointer) int64 {
	allocated := int64(0)
	dirtyID := uint64(0)
	if state == BUF_DIRTY {
		dirtyID = atomic.AddUint64(&inode.fs.bufferPool.curDirtyID, 1)
	}
	if pos > 0 && (inode.buffers[pos-1].offset+inode.buffers[pos-1].length) > offset ||
		pos < len(inode.buffers)-1 && (offset+uint64(len(data))) > inode.buffers[pos+1].offset {
		s := fmt.Sprintf("Tried to insert out of order: %x+%x", offset, len(data))
		if pos > 0 {
			s += fmt.Sprintf(" after %x+%x (s%v)", inode.buffers[pos-1].offset, inode.buffers[pos-1].length, inode.buffers[pos-1].state)
		}
		if pos < len(inode.buffers)-1 {
			s += fmt.Sprintf(" before %x+%x (s%v)", inode.buffers[pos+1].offset, inode.buffers[pos+1].length, inode.buffers[pos+1].state)
		}
		panic(s)
	}
	partStart, _ := inode.fs.partRange(inode.fs.partNum(offset))
	if copyData && pos > 0 &&
		inode.buffers[pos-1].data != nil &&
		(inode.buffers[pos-1].offset + inode.buffers[pos-1].length) == offset &&
		offset != partStart &&
		state == BUF_DIRTY &&
		inode.buffers[pos-1].state == BUF_DIRTY &&
		inode.buffers[pos-1].ptr.refs == 1 &&
		len(inode.buffers[pos-1].data) <= MAX_BUF/2 {
		// We can append to the previous buffer if it doesn't result
		// in overwriting data that may be referenced by other buffers
		// This is profitable because a lot of tools write in small chunks
		inode.buffers[pos-1].dirtyID = dirtyID
		inode.buffers[pos-1].onDisk = false
		inode.buffers[pos-1].recency = atomic.AddUint64(&inode.fs.memRecency, uint64(len(data)))
		allocated += inode.appendBuffer(inode.buffers[pos-1], data)
	} else {
		var newBuf []byte
		allocated += int64(len(data))
		if copyData {
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
		inode.buffers = insertBuffer(inode.buffers, pos, &FileBuffer{
			offset: offset,
			dirtyID: dirtyID,
			state: state,
			onDisk: false,
			zero: false,
			recency: atomic.AddUint64(&inode.fs.memRecency, uint64(len(newBuf))),
			length: uint64(len(newBuf)),
			data: newBuf,
			ptr: dataPtr,
		})
	}
	return allocated
}

func insertBuffer(buffers []*FileBuffer, pos int, add ...*FileBuffer) []*FileBuffer {
	// Ugly insert()...
	// Why can't Golang do append(buffers[0:s], a, buffers[s+1]...) ?
	if pos >= len(buffers) {
		return append(buffers, add...)
	}
	buffers = append(buffers, add...)
	copy(buffers[pos+len(add) : ], buffers[pos : ])
	copy(buffers[pos : ], add)
	return buffers
}

func (inode *Inode) addBuffer(offset uint64, data []byte, state int16, copyData bool) int64 {
	dataLen := uint64(len(data))
	endOffset := offset+dataLen

	// Remove intersecting parts as they're being overwritten
	allocated := inode.removeRange(offset, dataLen, state)

	// Insert non-overlapping parts of the buffer
	curOffset := offset
	dataPtr := &BufferPointer{
		mem: data,
		refs: 0,
	}
	start := locateBuffer(inode.buffers, offset)
	pos := start
	for ; pos < len(inode.buffers) && curOffset < endOffset; pos++ {
		b := inode.buffers[pos]
		if b.offset + b.length <= offset {
			continue
		}
		if b.offset > curOffset {
			// insert curOffset->min(b.offset,endOffset)
			nextEnd := b.offset
			if nextEnd > endOffset {
				nextEnd = endOffset
			}
			allocated += inode.insertOrAppendBuffer(pos, curOffset, data[curOffset-offset : nextEnd-offset], state, copyData, dataPtr)
		}
		curOffset = b.offset + b.length
	}
	if curOffset < endOffset {
		// Insert curOffset->endOffset
		allocated += inode.insertOrAppendBuffer(pos, curOffset, data[curOffset-offset : ], state, copyData, dataPtr)
	}

	return allocated
}

// Remove buffers in range (offset..size)
func (inode *Inode) removeRange(offset, size uint64, state int16) (allocated int64) {
	start := locateBuffer(inode.buffers, offset)
	endOffset := offset+size
	for pos := start; pos < len(inode.buffers); pos++ {
		b := inode.buffers[pos]
		if b.offset >= endOffset {
			break
		}
		bufEnd := b.offset+b.length
		// If we're inserting a clean buffer, don't remove dirty ones
		if (state >= BUF_DIRTY || b.state < BUF_DIRTY) && bufEnd > offset && endOffset > b.offset {
			if offset <= b.offset {
				if endOffset >= bufEnd {
					// whole buffer
					if b.data != nil {
						b.ptr.refs--
						if b.ptr.refs == 0 {
							allocated -= int64(len(b.ptr.mem))
						}
						b.ptr = nil
						b.data = nil
					}
					inode.buffers = append(inode.buffers[0 : pos], inode.buffers[pos+1 : ]...)
					pos--
				} else {
					// beginning
					if b.data != nil {
						b.data = b.data[endOffset - b.offset : ]
					}
					b.length = bufEnd-endOffset
					b.offset = endOffset
				}
			} else if endOffset >= bufEnd {
				// end
				if b.data != nil {
					b.data = b.data[0 : offset - b.offset]
				}
				b.length = offset - b.offset
			} else {
				// middle
				startBuf := &FileBuffer{
					offset: b.offset,
					dirtyID: b.dirtyID,
					state: b.state,
					onDisk: b.onDisk,
					recency: b.recency,
					loading: b.loading,
					length: offset-b.offset,
					zero: b.zero,
					ptr: b.ptr,
				}
				endBuf := &FileBuffer{
					offset: endOffset,
					dirtyID: b.dirtyID,
					state: b.state,
					onDisk: b.onDisk,
					recency: b.recency,
					loading: b.loading,
					length: b.length-(endOffset-b.offset),
					zero: b.zero,
					ptr: b.ptr,
				}
				if b.data != nil {
					b.ptr.refs++
					startBuf.data = b.data[0 : offset-b.offset]
					endBuf.data = b.data[endOffset-b.offset : ]
				}
				if b.dirtyID != 0 {
					endBuf.dirtyID = atomic.AddUint64(&inode.fs.bufferPool.curDirtyID, 1)
				}
				inode.buffers[pos] = startBuf
				inode.buffers = insertBuffer(inode.buffers, pos+1, endBuf)
			}
		}
	}
	return
}

func (inode *Inode) zeroRange(offset, size uint64) (bool, int64) {
	// Check if it's already zeroed
	pos := locateBuffer(inode.buffers, offset)
	if pos < len(inode.buffers) && inode.buffers[pos].zero &&
		inode.buffers[pos].offset == offset && inode.buffers[pos].length == size {
		return false, 0
	}

	// Remove intersecting parts as they're being overwritten
	allocated := inode.removeRange(offset, size, BUF_DIRTY)

	// Insert a zero buffer
	pos = locateBuffer(inode.buffers, offset)
	inode.buffers = insertBuffer(inode.buffers, pos, &FileBuffer{
		offset: offset,
		dirtyID: atomic.AddUint64(&inode.fs.bufferPool.curDirtyID, 1),
		state: BUF_DIRTY,
		onDisk: false,
		zero: true,
		recency: 0,
		length: size,
		data: nil,
		ptr: nil,
	})

	return true, allocated
}

func (inode *Inode) ResizeUnlocked(newSize uint64, zeroFill bool, finalizeFlushed bool) {
	// Truncate or extend
	inode.checkPauseWriters()
	if inode.Attributes.Size > newSize && len(inode.buffers) > 0 {
		// Truncate - remove extra buffers
		end := 0
		pauseAndFlush := true
		for pauseAndFlush {
			pauseAndFlush = false
			end = len(inode.buffers)
			for end > 0 && inode.buffers[end-1].offset >= newSize {
				b := inode.buffers[end-1]
				if b.state == BUF_FLUSHED_FULL || b.state == BUF_FLUSHED_CUT || b.state == BUF_FL_CLEARED ||
					inode.IsRangeLocked(b.offset, b.length, true) {
					// This buffer is flushed or is currently being flushed
					// We can't remove already flushed parts from the server :-(
					// And S3 (at least Ceph and Yandex, even though not Amazon) requires
					// to use ALL uploaded parts when completing the upload
					// So... we have to first finish the upload to be able to truncate it
					if !finalizeFlushed {
						log.Errorf("BUG: Trimming a flushed buffer where it should not happen")
					} else {
						pauseAndFlush = true
						break
					}
				}
				if b.data != nil {
					b.ptr.refs--
					if b.ptr.refs == 0 {
						inode.fs.bufferPool.Use(-int64(len(b.ptr.mem)), false)
					}
					b.ptr = nil
					b.data = nil
				}
				end--
			}
			if pauseAndFlush {
				inode.buffers = inode.buffers[0 : end]
				inode.Attributes.Size = inode.buffers[end-1].offset + inode.buffers[end-1].length
				inode.pauseWriters++
				inode.mu.Unlock()
				inode.SyncFile()
				inode.mu.Lock()
				inode.pauseWriters--
				if inode.readCond != nil {
					inode.readCond.Broadcast()
				}
			}
		}
		inode.buffers = inode.buffers[0 : end]
		if end > 0 {
			buf := inode.buffers[end-1]
			if buf.offset + buf.length > newSize {
				buf.length = newSize - buf.offset
				if buf.data != nil {
					buf.data = buf.data[0 : buf.length]
				}
			}
		}
	}
	if zeroFill && inode.Attributes.Size < newSize {
		// Zero fill extended region
		inode.buffers = append(inode.buffers, &FileBuffer{
			offset: inode.Attributes.Size,
			dirtyID: atomic.AddUint64(&inode.fs.bufferPool.curDirtyID, 1),
			state: BUF_DIRTY,
			onDisk: false,
			recency: 0,
			length: newSize - inode.Attributes.Size,
			zero: true,
		})
	}
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

	end := uint64(offset)+uint64(len(data))

	if end > fh.inode.fs.getMaxFileSize() {
		// File offset too large
		log.Warnf(
			"Maximum file size exceeded when writing %v bytes at offset %v to %v",
			len(data), offset, fh.inode.FullName(),
		)
		return syscall.EFBIG
	}

	// Try to reserve space without the inode lock
	err = fh.inode.fs.bufferPool.Use(int64(len(data)), false)
	if err != nil {
		return err
	}

	fh.inode.fs.lfru.Hit(fh.inode.Id, 0)

	fh.inode.mu.Lock()

	if fh.inode.CacheState == ST_DELETED || fh.inode.CacheState == ST_DEAD {
		// Oops, it's a deleted file. We don't support changing invisible files
		fh.inode.fs.bufferPool.Use(-int64(len(data)), false)
		fh.inode.mu.Unlock()
		return fuse.ENOENT
	}

	fh.inode.checkPauseWriters()

	if fh.inode.Attributes.Size < end {
		// Extend and zero fill
		fh.inode.ResizeUnlocked(end, true, false)
	}

	allocated := fh.inode.addBuffer(uint64(offset), data, BUF_DIRTY, copyData)

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
	if allocated != int64(len(data)) {
		err = fh.inode.fs.bufferPool.Use(allocated-int64(len(data)), true)
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

// FIXME: All "foreach-buffers" operations require some serious refactoring.
//
// We need the following "buffer-list" operations:
// 1) Remove/cut all buffers in a given range, matching a condition
// 2) Insert non-overlapping parts of a buffer at a given offset,
//    with support for merging adjacent buffers in the same state
// 3) Find (and fill) all empty areas in a given range
// 4) Find all parts of buffers in a given range, matching a condition
// 5) Cut buffers at given range boundaries
// 6) Quickly select buffers in a given state

func (inode *Inode) addLoadingBuffers(offset uint64, size uint64) {
	end := offset+size
	pos := offset
	i := locateBuffer(inode.buffers, offset)
	for ; i < len(inode.buffers); i++ {
		b := inode.buffers[i]
		if b.offset >= end {
			break
		}
		if b.offset > pos {
			inode.buffers = insertBuffer(inode.buffers, i, &FileBuffer{
				offset: pos,
				dirtyID: 0,
				state: BUF_CLEAN,
				loading: true,
				onDisk: false,
				zero: false,
				length: b.offset-pos,
			})
			i++
			b = inode.buffers[i]
		}
		pos = b.offset+b.length
	}
	if pos < end {
		inode.buffers = insertBuffer(inode.buffers, i, &FileBuffer{
			offset: pos,
			dirtyID: 0,
			state: BUF_CLEAN,
			loading: true,
			onDisk: false,
			zero: false,
			length: end-pos,
		})
	}
}

func (inode *Inode) removeLoadingBuffers(offset uint64, size uint64) {
	end := offset+size
	i := locateBuffer(inode.buffers, offset)
	for ; i < len(inode.buffers); i++ {
		b := inode.buffers[i]
		if b.offset >= end {
			break
		}
		if b.loading {
			inode.buffers = append(inode.buffers[0 : i], inode.buffers[i+1 : ]...)
			i--
			continue
		}
	}
}

func (inode *Inode) OpenCacheFD() error {
	if inode.DiskCacheFD == nil {
		fs := inode.fs
		cacheFileName := fs.flags.CachePath+"/"+inode.FullName()
		os.MkdirAll(path.Dir(cacheFileName), fs.flags.CacheFileMode | ((fs.flags.CacheFileMode & 0777) >> 2))
		var err error
		inode.DiskCacheFD, err = os.OpenFile(cacheFileName, os.O_RDWR|os.O_CREATE, fs.flags.CacheFileMode)
		if err != nil {
			log.Errorf("Couldn't open %v: %v", cacheFileName, err)
			return err
		} else {
			inode.OnDisk = true
			fs.diskFdMu.Lock()
			fs.diskFdCount++
			if fs.flags.MaxDiskCacheFD > 0 && fs.diskFdCount >= fs.flags.MaxDiskCacheFD {
				// Wakeup the coroutine that closes old FDs
				// This way we don't guarantee that we never exceed MaxDiskCacheFD
				// But we assume it's not a problem if we leave some safety margin for the limit
				// The same thing happens with our memory usage anyway :D
				fs.diskFdCond.Broadcast()
			}
			fs.diskFdMu.Unlock()
		}
	}
	return nil
}

// Load some inode data into memory
// Must be called with inode.mu taken
// Loaded range should be guarded against eviction by adding it into inode.readRanges
func (inode *Inode) LoadRange(offset uint64, size uint64, readAheadSize uint64, ignoreMemoryLimit bool) (miss bool, requestErr error) {

	end := offset+readAheadSize
	if size > readAheadSize {
		end = offset+size
	}
	if end > inode.Attributes.Size {
		end = inode.Attributes.Size
	}

	// Collect requests to the server and disk
	requests := []uint64(nil)
	diskRequests := []uint64(nil)
	start := locateBuffer(inode.buffers, offset)
	pos := offset
	toLoad := uint64(0)
	useSplit := false
	splitThreshold := 2*inode.fs.flags.ReadAheadParallelKB*1024
	for i := start; i < len(inode.buffers); i++ {
		b := inode.buffers[i]
		if b.offset >= end {
			break
		}
		if b.offset > pos {
			requests = appendRequest(requests, pos, b.offset-pos, inode.fs.flags.ReadMergeKB*1024)
			toLoad += b.offset-pos
			useSplit = useSplit || b.offset-pos > splitThreshold
		}
		if b.loading || !b.zero && b.data == nil && b.onDisk {
			// We're already reading it from the server or from disk
			// OR it should be loaded from disk
			s := b.offset
			if s < pos {
				s = pos
			}
			e := end
			if end > b.offset+b.length {
				e = b.offset+b.length
			}
			toLoad += e-s
			if !b.loading {
				diskRequests = append(diskRequests, s, e-s)
			}
		} else if b.state == BUF_FL_CLEARED {
			// Buffer is saved as a part and then removed
			// We must complete multipart upload to be able to read it back
			return true, syscall.ESPIPE
		}
		pos = b.offset+b.length
	}
	if pos < end {
		requests = appendRequest(requests, pos, end-pos, inode.fs.flags.ReadMergeKB*1024)
		toLoad += end-pos
		useSplit = useSplit || end-pos > splitThreshold
	}

	if toLoad == 0 {
		// Everything is already loaded
		return
	}

	// add readahead to the server request
	if len(requests) > 0 {
		nr := len(requests)
		lastEnd := requests[nr-2]+readAheadSize
		if readAheadSize > inode.fs.flags.ReadAheadParallelKB*1024 {
			// Pipelining
			lastEnd = requests[nr-2]+inode.fs.flags.ReadAheadParallelKB*1024
		}
		if lastEnd > inode.Attributes.Size {
			lastEnd = inode.Attributes.Size
		}
		lastEnd = lastEnd-requests[nr-2]
		if requests[nr-1] < lastEnd {
			requests[nr-1] = lastEnd
			useSplit = useSplit || lastEnd > splitThreshold
		}
	}

	if useSplit {
		// split very large requests into smaller chunks to read in parallel
		minPart := inode.fs.flags.ReadAheadParallelKB*1024
		splitRequests := make([]uint64, 0)
		for i := 0; i < len(requests); i += 2 {
			offset := requests[i]
			size := requests[i+1]
			if size > minPart {
				parts := int(size/minPart)
				for j := 0; j < parts; j++ {
					partLen := minPart
					if j == parts-1 {
						partLen = size - uint64(j)*minPart
					}
					splitRequests = append(splitRequests, offset + minPart*uint64(j), partLen)
				}
			} else {
				splitRequests = append(splitRequests, offset, size)
			}
		}
		requests = splitRequests
	}

	// Mark new ranges as being loaded from the server
	for i := 0; i < len(requests); i += 2 {
		offset := requests[i]
		size := requests[i+1]
		inode.addLoadingBuffers(offset, size)
	}

	// Mark other ranges as being loaded from the disk (may require splitting)
	for i := 0; i < len(diskRequests); i += 2 {
		last := diskRequests[i]
		end := diskRequests[i]+diskRequests[i+1]
		for i := locateBuffer(inode.buffers, last); i < len(inode.buffers); i++ {
			b := inode.buffers[i]
			if last > b.offset {
				// Split the buffer
				inode.splitBuffer(i, last-b.offset)
				continue
			}
			last = b.offset+b.length
			if last > end {
				// Split the buffer
				inode.splitBuffer(i, end-b.offset)
				b = inode.buffers[i]
				last = b.offset+b.length
			}
			b.loading = true
			if last >= end {
				break
			}
		}
	}

	// send requests
	if len(requests) > 0 {
		if inode.readCond == nil {
			inode.readCond = sync.NewCond(&inode.mu)
		}
		cloud, key := inode.cloud()
		if inode.oldParent != nil {
			_, key = inode.oldParent.cloud()
			key = appendChildName(key, inode.oldName)
		}
		for i := 0; i < len(requests); i += 2 {
			requestOffset := requests[i]
			requestSize := requests[i+1]
			go inode.sendRead(cloud, key, requestOffset, requestSize, ignoreMemoryLimit)
		}
	}

	if len(diskRequests) > 0 {
		if err := inode.OpenCacheFD(); err != nil {
			return true, err
		}
		loadedFromDisk := uint64(0)
		for i := 0; i < len(diskRequests); i += 2 {
			requestOffset := diskRequests[i]
			requestSize := diskRequests[i+1]
			data := make([]byte, requestSize)
			_, err := inode.DiskCacheFD.ReadAt(data, int64(requestOffset))
			if err != nil {
				return true, err
			}
			pos := locateBuffer(inode.buffers, requestOffset)
			var ib *FileBuffer
			if pos < len(inode.buffers) {
				ib = inode.buffers[pos]
			}
			if ib == nil || ib.offset != requestOffset || ib.length != requestSize || !ib.loading {
				panic("BUG: Disk read buffer was modified by someone else in meantime")
			}
			ib.loading = false
			ib.data = data
			ib.ptr = &BufferPointer{
				mem: data,
				refs: 1,
			}
			if ib.state == BUF_FL_CLEARED {
				ib.state = BUF_FLUSHED_FULL
			}
			loadedFromDisk += requestSize
		}
		inode.mu.Unlock()
		// Correct memory usage without the inode lock
		inode.fs.bufferPool.Use(int64(loadedFromDisk), true)
		inode.mu.Lock()
		toLoad -= loadedFromDisk
	}

	if toLoad == 0 {
		return
	}

	miss = true
	end = offset+size
	for {
		// Check if all buffers are loaded or if there is a read error
		pos := offset
		start := locateBuffer(inode.buffers, offset)
		stillLoading := false
		i := start
		for ; i < len(inode.buffers); i++ {
			b := inode.buffers[i]
			if b.offset >= end {
				break
			}
			if b.offset > pos {
				// One of the buffers disappeared => read error
				requestErr = inode.readError
				if requestErr == nil {
					requestErr = fuse.EIO
				}
				return
			}
			if b.loading {
				stillLoading = true
				break
			}
			pos = b.offset+b.length
		}
		if !stillLoading {
			if pos < end {
				// One of the buffers disappeared => read error
				requestErr = inode.readError
				if requestErr == nil {
					requestErr = fuse.EIO
				}
				return
			}
			break
		}
		inode.readCond.Wait()
	}

	return
}

func (inode *Inode) sendRead(cloud StorageBackend, key string, offset, size uint64, ignoreMemoryLimit bool) {
	// Maybe free some buffers first
	origOffset := offset
	origSize := size
	err := inode.fs.bufferPool.Use(int64(size), ignoreMemoryLimit)
	if err != nil {
		log.Errorf("Error reading %v +%v of %v: %v", offset, size, key, err)
		inode.mu.Lock()
		inode.readError = err
		inode.removeLoadingBuffers(offset, size)
		inode.mu.Unlock()
		inode.readCond.Broadcast()
		return
	}
	inode.mu.Lock()
	inode.LockRange(offset, size, false)
	inode.mu.Unlock()
	resp, err := cloud.GetBlob(&GetBlobInput{
		Key:   key,
		Start: offset,
		Count: size,
	})
	if err != nil {
		log.Errorf("Error reading %v +%v of %v: %v", offset, size, key, err)
		inode.fs.bufferPool.Use(-int64(size), false)
		inode.mu.Lock()
		inode.UnlockRange(origOffset, origSize, false)
		inode.removeLoadingBuffers(offset, size)
		inode.readError = err
		inode.mu.Unlock()
		inode.readCond.Broadcast()
		return
	}
	allocated := uint64(0)
	left := size
	for left > 0 {
		// Read the result in smaller parts so parallelism can be utilized better
		bs := left
		if bs > READ_BUF_SIZE {
			bs = READ_BUF_SIZE
		}
		buf := make([]byte, bs)
		done := uint64(0)
		for done < bs {
			n, err := resp.Body.Read(buf[done :])
			done += uint64(n)
			if err != nil && (err != io.EOF || done < bs) {
				log.Errorf("Error reading %v +%v of %v: %v", offset, bs, key, err)
				inode.mu.Lock()
				inode.readError = err
				inode.removeLoadingBuffers(offset, left)
				inode.UnlockRange(origOffset, origSize, false)
				inode.mu.Unlock()
				if allocated != size {
					inode.fs.bufferPool.Use(int64(allocated)-int64(size), true)
				}
				inode.readCond.Broadcast()
				return
			}
		}
		// Cache part of the result
		inode.mu.Lock()
		if inode.userMetadata == nil {
			// Cache xattrs
			inode.fillXattrFromHead(&(*resp).HeadBlobOutput)
		}
		added := inode.addBuffer(offset, buf, BUF_CLEAN, false)
		inode.mu.Unlock()
		left -= done
		offset += done
		if added != 0 {
			allocated += bs
		}
		// Notify waiting readers
		inode.readCond.Broadcast()
	}
	// Correct memory usage
	if allocated != size {
		inode.fs.bufferPool.Use(int64(allocated)-int64(size), true)
	}
	inode.mu.Lock()
	inode.UnlockRange(origOffset, origSize, false)
	inode.mu.Unlock()
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

func appendZero(data [][]byte, zeroBuf []byte, zeroLen int) [][]byte {
	for zeroLen > len(zeroBuf) {
		data = append(data, zeroBuf)
		zeroLen -= len(zeroBuf)
	}
	if zeroLen > 0 {
		data = append(data, zeroBuf[0 : zeroLen])
	}
	return data
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

	// Lock inode
	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()

	if offset >= fh.inode.Attributes.Size {
		// nothing to read
		err = io.EOF
		return
	}

	end := offset+size
	if end >= fh.inode.Attributes.Size {
		end = fh.inode.Attributes.Size
	}
	maxFileSize := fh.inode.fs.getMaxFileSize()
	if end > maxFileSize {
		// File offset too large
		log.Warnf(
			"Maximum file size exceeded when reading %v bytes at offset %v from %v",
			size, offset, fh.inode.FullName(),
		)
		err = syscall.EFBIG
		return
	}
	if size == 0 {
		// Just in case if the length is zero
	} else if offset == fh.lastReadEnd {
		fh.seqReadSize += size
		if fh.lastReadCount == 0 && fh.lastReadEnd == 0 {
			fh.inode.fs.lfru.Hit(fh.inode.Id, 1)
		}
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
			fh.lastReadIdx = (fh.lastReadIdx+1) % len(fh.lastReadSizes)
		}
		fh.seqReadSize = size
		fh.inode.fs.lfru.Hit(fh.inode.Id, 1)
	}
	fh.lastReadEnd = end

	// Guard buffers against eviction
	fh.inode.LockRange(offset, end-offset, false)
	defer fh.inode.UnlockRange(offset, end-offset, false)

	// Check if anything requires to be loaded from the server
	ra := fh.inode.fs.flags.ReadAheadKB*1024
	if fh.seqReadSize >= fh.inode.fs.flags.LargeReadCutoffKB*1024 {
		// Use larger readahead with 'pipelining'
		ra = fh.inode.fs.flags.ReadAheadLargeKB*1024
	} else if fh.lastReadCount > 0 {
		// Disable readahead if last N read requests are smaller than X on average
		avg := (fh.seqReadSize + fh.lastReadTotal) / (1 + fh.lastReadCount)
		if avg <= fh.inode.fs.flags.SmallReadCutoffKB*1024 {
			// Use smaller readahead
			ra = fh.inode.fs.flags.ReadAheadSmallKB*1024
		}
	}
	if ra+end > maxFileSize {
		ra = 0
	}
	miss, requestErr := fh.inode.CheckLoadRange(offset, end-offset, ra, false)
	if !miss {
		atomic.AddInt64(&fh.inode.fs.stats.readHits, 1)
	}
	mappedErr := mapAwsError(requestErr)
	if requestErr != nil {
		err = requestErr
		if mappedErr == fuse.ENOENT || mappedErr == syscall.ERANGE {
			// Object is deleted or resized remotely (416). Discard local version
			log.Warnf("File %v is deleted or resized remotely, discarding local changes", fh.inode.FullName())
			fh.inode.resetCache()
		}
		return
	}

	// return cached buffers directly without copying
	start := locateBuffer(fh.inode.buffers, offset)
	pos := offset
	for i := start; i < len(fh.inode.buffers); i++ {
		b := fh.inode.buffers[i]
		if b.offset >= end {
			break
		}
		if b.offset > pos {
			// How is this possible? We should've just received it from the server!
			if fh.inode.CacheState == ST_CREATED {
				// It's okay if the file is just created
				// Zero empty ranges in this case
				data = appendZero(data, fh.inode.fs.zeroBuf, int(b.offset-pos))
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
		if b.loading {
			panic("Tried to read a loading buffer")
		} else if b.zero {
			data = appendZero(data, fh.inode.fs.zeroBuf, int(readEnd-pos))
		} else {
			data = append(data, b.data[pos-b.offset : readEnd-b.offset])
		}
		pos = readEnd
	}
	if pos < end {
		// How is this possible? We should've just received it from the server!
		if fh.inode.CacheState == ST_CREATED {
			// It's okay if the file is just created
			// Zero empty ranges in this case
			data = appendZero(data, fh.inode.fs.zeroBuf, int(end-pos))
			pos = end
		} else {
			err = requestErr
			if err == nil {
				err = fuse.EIO
			}
			return
		}
	}

	// Don't exceed IOV_MAX-1 for writev.
	if len(data) > IOV_MAX-1 {
		var tail []byte
		for i := IOV_MAX-2; i < len(data); i++ {
			tail = append(tail, data[i]...)
		}
		data = append(data[0:IOV_MAX-2], tail)
	}

	bytesRead = int(end-offset)

	return
}

func (fh *FileHandle) Release() {
	// LookUpInode accesses fileHandles without mutex taken, so use atomics for now
	n := atomic.AddInt32(&fh.inode.fileHandles, -1)
	if n == -1 {
		panic(fmt.Sprintf("Released more file handles than acquired, n = %v", n))
	}
	if n == 0 && atomic.LoadInt32(&fh.inode.CacheState) <= ST_DEAD {
		fh.inode.Parent.addModified(-1)
	}
	fh.inode.fs.WakeupFlusher()
}

func (inode *Inode) splitBuffer(i int, size uint64) {
	b := inode.buffers[i]
	endBuf := &FileBuffer{
		offset: b.offset+size,
		dirtyID: b.dirtyID,
		state: b.state,
		onDisk: b.onDisk,
		recency: b.recency,
		loading: b.loading,
		length: b.length-size,
		zero: b.zero,
		ptr: b.ptr,
	}
	b.length = size
	if b.data != nil {
		endBuf.data = b.data[size : ]
		b.data = b.data[0 : size]
		endBuf.ptr.refs++
	}
	if b.dirtyID != 0 {
		endBuf.dirtyID = atomic.AddUint64(&inode.fs.bufferPool.curDirtyID, 1)
	}
	inode.buffers = insertBuffer(inode.buffers, i+1, endBuf)
}

func (inode *Inode) GetMultiReader(offset uint64, size uint64) (reader *MultiReader, bufIds map[uint64]bool) {
	reader = NewMultiReader()
	bufIds = make(map[uint64]bool)
	last := offset
	end := offset+size
	for i := locateBuffer(inode.buffers, offset); i < len(inode.buffers); i++ {
		b := inode.buffers[i]
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
			b = inode.buffers[i]
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
	inode.fs.ScheduleRetryFlush()
}

func (inode *Inode) TryFlush() bool {
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
		// Send rename
		inode.IsFlushing += inode.fs.flags.MaxParallelParts
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
					if mappedErr == fuse.ENOENT && skipRename {
						err = nil
						notFoundIgnore = true
					} else if mappedErr == fuse.ENOENT || mappedErr == syscall.ERANGE {
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
						newParent.mu.Lock()
						newParent.removeChildUnlocked(inode)
						newParent.mu.Unlock()
						if oldParent != nil {
							oldParent.mu.Lock()
							delete(oldParent.dir.DeletedChildren, oldName)
							oldParent.addModified(-1)
							oldParent.mu.Unlock()
						}
					} else {
						log.Debugf("Failed to copy %v to %v (rename): %v", from, key, err)
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
								inode.AttrTime = time.Now()
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
						inode.AttrTime = time.Now()
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
						tomb.CacheState = ST_DELETED
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
		return true
	}

	if inode.CacheState == ST_MODIFIED && inode.userMetadataDirty != 0 &&
		inode.oldParent == nil && inode.IsFlushing == 0 {
		hasDirty := false
		for i := 0; i < len(inode.buffers); i++ {
			buf := inode.buffers[i]
			if buf.dirtyID != 0 {
				hasDirty = true
				break
			}
		}
		if !hasDirty {
			// Update metadata by COPYing into the same object
			// It results in the optimized implementation in S3
			inode.userMetadataDirty = 0
			inode.IsFlushing += inode.fs.flags.MaxParallelParts
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
					if mappedErr == fuse.ENOENT || mappedErr == syscall.ERANGE {
						// Object is deleted or resized remotely (416). Discard local version
						s3Log.Warnf("Conflict detected (inode %v): File %v is deleted or resized remotely, discarding local changes", inode.Id, inode.FullName())
						inode.resetCache()
					}
					log.Errorf("Error flushing metadata using COPY for %v: %v", key, err)
				} else if inode.CacheState == ST_MODIFIED && !inode.isStillDirty() {
					inode.SetCacheState(ST_CACHED)
					inode.AttrTime = time.Now()
				}
				inode.IsFlushing -= inode.fs.flags.MaxParallelParts
				atomic.AddInt64(&inode.fs.activeFlushers, -1)
				inode.fs.WakeupFlusher()
				inode.mu.Unlock()
			}()
			return true
		}
	}

	if inode.Attributes.Size <= inode.fs.flags.SinglePartMB*1024*1024 && inode.mpu == nil {
		// Don't flush small files with active file handles (if not under memory pressure)
		if inode.IsFlushing == 0 && (inode.fileHandles == 0 || inode.forceFlush || atomic.LoadInt32(&inode.fs.wantFree) > 0) {
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
			params := &MultipartBlobBeginInput{
				Key: key,
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
				log.Errorf("Failed to initiate multipart upload for %v: %v", key, err)
			} else {
				log.Debugf("Started multi-part upload of object %v", key)
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
	initiated := false
	lastPart := uint64(0)
	flushInode := inode.fileHandles == 0 || inode.forceFlush || atomic.LoadInt32(&inode.fs.wantFree) > 0
	partDirty := false
	partLocked := false
	partEvicted := false
	partZero := false
	hasEvictedParts := false
	canComplete := true
	processPart := func() bool {
		// Don't flush parts that are being currently flushed
		if partLocked {
			canComplete = false
		// Don't flush empty ranges when we're not under pressure
		} else if partZero && !flushInode {
			canComplete = false
		// Don't flush parts that require RMW with evicted buffers
		} else if partDirty && !partEvicted {
			canComplete = false
			// Don't write out the last part that's still written to (if not under memory pressure)
			if flushInode || lastPart != inode.fs.partNum(inode.lastWriteEnd) {
				partOffset, partSize := inode.fs.partRange(lastPart)
				// Guard part against eviction
				inode.LockRange(partOffset, partSize, true)
				inode.IsFlushing++
				atomic.AddInt64(&inode.fs.activeFlushers, 1)
				go func(lastPart, partOffset, partSize uint64) {
					inode.mu.Lock()
					inode.FlushPart(lastPart)
					inode.UnlockRange(partOffset, partSize, true)
					inode.IsFlushing--
					inode.mu.Unlock()
					atomic.AddInt64(&inode.fs.activeFlushers, -1)
					inode.fs.WakeupFlusher()
				}(lastPart, partOffset, partSize)
				initiated = true
				if atomic.LoadInt64(&inode.fs.activeFlushers) >= inode.fs.flags.MaxFlushers ||
					inode.IsFlushing >= inode.fs.flags.MaxParallelParts {
					return true
				}
			}
		} else if partDirty && partEvicted {
			hasEvictedParts = true
		}
		return false
	}
	for i := 0; i < len(inode.buffers); i++ {
		buf := inode.buffers[i]
		startPart := inode.fs.partNum(buf.offset)
		endPart := inode.fs.partNum(buf.offset + buf.length - 1)
		if i == 0 || startPart != lastPart {
			if i > 0 {
				if processPart() {
					return true
				}
			}
			partDirty = false
			partLocked = false
			partEvicted = false
			partZero = false
			lastPart = startPart
		}
		partDirty = partDirty || buf.state == BUF_DIRTY
		partLocked = partLocked || inode.IsRangeLocked(buf.offset, buf.length, true)
		partEvicted = partEvicted || buf.state == BUF_FL_CLEARED
		partZero = partZero || buf.zero
		for lastPart < endPart {
			if processPart() {
				return true
			}
			partDirty = buf.state == BUF_DIRTY
			partLocked = inode.IsRangeLocked(buf.offset, buf.length, true)
			partEvicted = buf.state == BUF_FL_CLEARED
			partZero = buf.zero
			lastPart++
		}
	}
	if len(inode.buffers) > 0 {
		if processPart() {
			return true
		}
	}
	if canComplete && (inode.fileHandles == 0 || inode.forceFlush ||
		atomic.LoadInt32(&inode.fs.wantFree) > 0 && hasEvictedParts) {
		// Complete the multipart upload
		inode.IsFlushing += inode.fs.flags.MaxParallelParts
		atomic.AddInt64(&inode.fs.activeFlushers, 1)
		go func() {
			inode.mu.Lock()
			inode.completeMultipart()
			inode.IsFlushing -= inode.fs.flags.MaxParallelParts
			inode.mu.Unlock()
			atomic.AddInt64(&inode.fs.activeFlushers, -1)
			inode.fs.WakeupFlusher()
		}()
	}

	return initiated
}

func (inode *Inode) isStillDirty() bool {
	if inode.userMetadataDirty != 0 || inode.oldParent != nil {
		return true
	}
	for i := 0; i < len(inode.buffers); i++ {
		b := inode.buffers[i]
		if b.dirtyID != 0 {
			return true
		}
	}
	return false
}

func (inode *Inode) resetCache() {
	// Drop all buffers including dirty ones
	for _, b := range inode.buffers {
		if b.data != nil {
			b.ptr.refs--
			if b.ptr.refs == 0 {
				inode.fs.bufferPool.Use(-int64(len(b.ptr.mem)), false)
			}
			b.ptr = nil
			b.data = nil
		}
	}
	inode.buffers = nil
	// Also remove the cache file from disk, if present
	if inode.OnDisk {
		if inode.DiskCacheFD != nil {
			inode.DiskCacheFD.Close()
			inode.DiskCacheFD = nil
			atomic.AddInt64(&inode.fs.diskFdCount, -1)
		}
		cacheFileName := inode.fs.flags.CachePath+"/"+inode.FullName()
		err := os.Remove(cacheFileName)
		if err != nil {
			log.Errorf("Couldn't remove %v: %v", cacheFileName, err)
		} else {
			inode.OnDisk = false
		}
	}
	// And abort multipart upload, too
	if inode.mpu != nil {
		cloud, key := inode.cloud()
		go func(mpu *MultipartBlobCommitInput) {
			_, abortErr := cloud.MultipartBlobAbort(mpu)
			if abortErr != nil {
				log.Errorf("Failed to abort multi-part upload of object %v: %v", key, abortErr)
			}
		}(inode.mpu)
		inode.mpu = nil
	}
	inode.userMetadataDirty = 0
	inode.SetCacheState(ST_CACHED)
	// Invalidate metadata entry
	inode.AttrTime = time.Time{}
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
		_, err := inode.LoadRange(0, sz, 0, true)
		mappedErr := mapAwsError(err)
		if mappedErr == fuse.ENOENT || mappedErr == syscall.ERANGE {
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
	bufReader, bufIds := inode.GetMultiReader(0, inode.Attributes.Size)
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
		go func(mpu *MultipartBlobCommitInput) {
			_, abortErr := cloud.MultipartBlobAbort(mpu)
			if abortErr != nil {
				log.Errorf("Failed to abort multi-part upload of object %v: %v", key, abortErr)
			}
		}(inode.mpu)
		inode.mpu = nil
	}
	inode.mu.Unlock()
	inode.fs.addInflightChange(key)
	resp, err := cloud.PutBlob(params)
	inode.fs.completeInflightChange(key)
	inode.mu.Lock()

	inode.recordFlushError(err)
	if err != nil {
		log.Errorf("Failed to flush small file %v: %v", key, err)
		if params.Metadata != nil {
			inode.userMetadataDirty = 2
		}
	} else {
		log.Debugf("Flushed small file %v (inode %v): etag=%v, size=%v", key, inode.Id, NilStr(resp.ETag), sz)
		stillDirty := inode.userMetadataDirty != 0 || inode.oldParent != nil
		for i := 0; i < len(inode.buffers); i++ {
			b := inode.buffers[i]
			if b.dirtyID != 0 {
				if bufIds[b.dirtyID] {
					// OK, not dirty anymore
					b.dirtyID = 0
					b.state = BUF_CLEAN
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
		}
		inode.updateFromFlush(sz, resp.ETag, resp.LastModified, resp.StorageClass)
	}

	inode.UnlockRange(0, sz, true)
	inode.IsFlushing -= inode.fs.flags.MaxParallelParts
	atomic.AddInt64(&inode.fs.activeFlushers, -1)
	inode.fs.WakeupFlusher()
	inode.mu.Unlock()
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
					err = fuse.ENOENT
				} else {
					inode.mu.Unlock()
					log.Debugf("Copying unmodified range %v-%v MB of object %v",
						offset/1024/1024, (offset+size+1024*1024-1)/1024/1024, key)
					resp, requestErr := cloud.MultipartBlobCopy(&MultipartBlobCopyInput{
						Commit:     mpu,
						PartNumber: uint32(partNum+1),
						CopySource: key,
						Offset:     offset,
						Size:       size,
					})
					if requestErr != nil {
						log.Errorf("Failed to copy unmodified range %v-%v MB of object %v: %v",
							offset/1024/1024, (offset+size+1024*1024-1)/1024/1024, key, requestErr)
						err = requestErr
					} else {
						mpu.Parts[partNum] = resp.PartId
					}
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
		partSize = inode.Attributes.Size-partOffset
	}

	// Load part from the server if we have to read-modify-write it
	if inode.CacheState == ST_MODIFIED {
		// Ignore memory limit to not produce a deadlock when we need to free some memory
		// by flushing objects, but we can't flush a part without allocating more memory
		// for read-modify-write...
		_, err := inode.LoadRange(partOffset, partSize, 0, true)
		if err == syscall.ESPIPE {
			// Part is partly evicted, we can't flush it
			return
		}
		mappedErr := mapAwsError(err)
		if mappedErr == fuse.ENOENT || mappedErr == syscall.ERANGE {
			// Object is deleted or resized remotely (416). Discard local version
			s3Log.Warnf("Conflict detected (inode %v): File %v is deleted or resized remotely, discarding local changes", inode.Id, inode.FullName())
			inode.resetCache()
			return
		}
		if err != nil {
			log.Errorf("Failed to load part %v of object %v to flush it: %v", part, key, err)
			return
		}
		// File size may have been changed again
		if inode.Attributes.Size <= partOffset || inode.CacheState != ST_MODIFIED {
			// Abort flush
			return
		}
		if inode.Attributes.Size < partOffset+partSize {
			partSize = inode.Attributes.Size-partOffset
		}
	}

	if inode.mpu == nil {
		// Multipart upload was canceled in the meantime => don't flush
		return
	}

	// Finally upload it
	bufReader, bufIds := inode.GetMultiReader(partOffset, partSize)
	bufLen := bufReader.Len()
	partInput := MultipartBlobAddInput{
		Commit:     inode.mpu,
		PartNumber: uint32(part+1),
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
		log.Errorf("Failed to flush part %v of object %v: %v", part, key, err)
	} else {
		if inode.mpu != nil {
			// It could become nil if the file was deleted remotely in the meantime
			inode.mpu.Parts[part] = resp.PartId
		}
		doneState := BUF_FLUSHED_FULL
		if bufLen < partFullSize {
			doneState = BUF_FLUSHED_CUT
		}
		log.Debugf("Flushed part %v of object %v", part, key)
		for i := 0; i < len(inode.buffers); i++ {
			b := inode.buffers[i]
			if b.dirtyID != 0 {
				if bufIds[b.dirtyID] {
					// Still dirty because the upload is not completed yet,
					// but flushed to the server
					b.state = doneState
				}
			}
		}
	}
}

// LOCKS_REQUIRED(inode.mu)
func (inode *Inode) completeMultipart() {
	// Server-side copy unmodified parts
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
	if mappedErr == fuse.ENOENT || mappedErr == syscall.ERANGE {
		// Object is deleted or resized remotely (416). Discard local version
		s3Log.Warnf("Conflict detected (inode %v): File %v is deleted or resized remotely, discarding local changes", inode.Id, inode.FullName())
		inode.resetCache()
		return
	}
	inode.recordFlushError(err)
	if err == nil && (inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED) {
		cloud, key := inode.cloud()
		if inode.oldParent != nil {
			// Always apply modifications before moving
			_, key = inode.oldParent.cloud()
			key = appendChildName(key, inode.oldName)
		}
		// Finalize the upload
		inode.mpu.NumParts = uint32(numParts)
		inode.mu.Unlock()
		inode.fs.addInflightChange(key)
		resp, err := cloud.MultipartBlobCommit(inode.mpu)
		inode.fs.completeInflightChange(key)
		inode.mu.Lock()
		if inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED {
			inode.recordFlushError(err)
			if err != nil {
				log.Errorf("Failed to finalize multi-part upload of object %v: %v", key, err)
				if inode.mpu.Metadata != nil {
					inode.userMetadataDirty = 2
				}
			} else {
				log.Debugf("Finalized multi-part upload of object %v: etag=%v, size=%v", key, NilStr(resp.ETag), finalSize)
				if inode.mpu.Metadata != nil && inode.userMetadataDirty == 1 {
					inode.userMetadataDirty = 0
				}
				inode.mpu = nil
				inode.updateFromFlush(finalSize, resp.ETag, resp.LastModified, resp.StorageClass)
				stillDirty := inode.userMetadataDirty != 0 || inode.oldParent != nil || inode.Attributes.Size != inode.knownSize
				for i := 0; i < len(inode.buffers); {
					if inode.buffers[i].state == BUF_FL_CLEARED {
						inode.buffers = append(inode.buffers[0 : i], inode.buffers[i+1 : ]...)
					} else {
						if inode.buffers[i].state == BUF_FLUSHED_FULL ||
							inode.buffers[i].state == BUF_FLUSHED_CUT {
							inode.buffers[i].dirtyID = 0
							inode.buffers[i].state = BUF_CLEAN
						}
						if inode.buffers[i].dirtyID != 0 {
							stillDirty = true
						}
						i++
					}
				}
				if inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED {
					if !stillDirty {
						inode.SetCacheState(ST_CACHED)
					} else {
						inode.SetCacheState(ST_MODIFIED)
					}
				}
			}
		}
	} else {
		// FIXME: Abort multipart upload, but not just here
		// For example, we also should abort it if a partially flushed file is deleted
	}
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
	inode.AttrTime = time.Now()
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
