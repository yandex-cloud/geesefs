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
	"sync"
	"sync/atomic"
//	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
)

type FileHandle struct {
	inode *Inode
	cloud StorageBackend
	key   string

	// User space PID. All threads created by a process will have the same TGID,
	// but different PIDs[1].
	// This value can be nil if we fail to get TGID from PID[2].
	// [1] : https://godoc.org/github.com/shirou/gopsutil/process#Process.Tgid
	// [2] : https://github.com/shirou/gopsutil#process-class
	Tgid *int32

	keepPageCache bool // the same value we returned to OpenFile
}

// FIXME -> to configuration
const MAX_BUF = 5 * 1024 * 1024
const SINGLE_PART_SIZE = uint64(5 * 1024 * 1024)

// NewFileHandle returns a new file handle for the given `inode` triggered by fuse
// operation with the given `opMetadata`
func NewFileHandle(inode *Inode, opMetadata fuseops.OpMetadata) *FileHandle {
	tgid, err := GetTgid(opMetadata.Pid)
	if err != nil {
		log.Debugf(
			"Failed to retrieve tgid for the given pid. pid: %v err: %v inode id: %v err: %v",
			opMetadata.Pid, err, inode.Id, err)
	}
	fh := &FileHandle{inode: inode, Tgid: tgid}
	fh.cloud, fh.key = inode.cloud()
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
	start := 0
	for start < len(buffers) {
		// FIXME binary search?
		b := &buffers[start]
		if b.offset + uint64(len(b.buf)) > offset {
			break
		}
		start++
	}
	return start
}

func appendBuffer(forInode fuseops.InodeID, pool *BufferPool, buf *FileBuffer, data []byte) {
	oldLen := len(buf.buf)
	newLen := oldLen + len(data)
	if cap(buf.buf) >= newLen {
		// It fits
		buf.buf = buf.buf[0 : newLen]
		copy(buf.buf[oldLen : ], data)
	} else {
		// Reallocate
		newCap := newLen
		if newCap < 2*oldLen {
			newCap = 2*oldLen
		}
		newData := make([]byte, newCap)
		copy(newData[0 : oldLen], buf.buf)
		copy(newData[oldLen : newLen], data)
		buf.buf = newData[0 : newLen]
		// Refcount
		oldPtr := buf.ptr
		oldPtr.refs--
		if oldPtr.refs == 0 {
			freed := uint64(len(oldPtr.buf))
			pool.Free(freed, buf.dirtyID != 0)
		}
		buf.ptr = &BufferPointer{
			buf: newData,
			refs: 1,
		}
	}
}

func insertBuffer(forInode fuseops.InodeID, pool *BufferPool, buffers []FileBuffer,
	pos int, offset uint64, data []byte, dirty bool, copyData bool, dataPtr *BufferPointer) []FileBuffer {
	dirtyID := uint64(0)
	if dirty {
		dirtyID = atomic.AddUint64(&pool.curDirtyID, 1)
	}
	if copyData && pos > 0 &&
		(buffers[pos-1].offset + uint64(len(buffers[pos-1].buf)) == offset) &&
		(buffers[pos-1].dirtyID != 0) == (dirtyID != 0) &&
		(cap(buffers[pos-1].buf) < len(buffers[pos-1].buf)+len(data) ||
			buffers[pos-1].ptr.refs == 1 && len(buffers[pos-1].buf) <= MAX_BUF/2) {
		// We can append to the previous buffer if it doesn't result
		// in overwriting data that may be referenced by other buffers
		// This is profitable because a lot of tools write in small chunks
		buffers[pos-1].dirtyID = dirtyID
		buffers[pos-1].flushed = false
		appendBuffer(forInode, pool, &buffers[pos-1], data)
	} else {
		var newBuf []byte
		if copyData {
			newBuf = make([]byte, len(data))
			pool.Use(forInode, uint64(len(data)), dirty)
			copy(newBuf, data)
			dataPtr = &BufferPointer{
				buf: newBuf,
				refs: 0,
			}
		} else {
			newBuf = data
		}
		dataPtr.refs++
		// Ugly insert()...
		// Why can't Golang do append(buffers[0:s], a, buffers[s+1]...) ?
		buffers = append(buffers, FileBuffer{})
		copy(buffers[pos+1 : ], buffers[pos : ])
		buffers[pos] = FileBuffer{
			offset: offset,
			dirtyID: dirtyID,
			flushed: false,
			buf: newBuf,
			ptr: dataPtr,
		}
	}
	return buffers
}

func addFileBuffer(forInode fuseops.InodeID, pool *BufferPool, buffers []FileBuffer, offset uint64, data []byte, dirty bool, copyData bool) []FileBuffer {
	start := locateBuffer(buffers, offset)
	dataLen := uint64(len(data))
	endOffset := offset+dataLen

	// Remove intersecting parts as they're being overwritten
	// If we're inserting a clean buffer, don't remove dirty ones
	for pos := start; pos < len(buffers); pos++ {
		b := &buffers[pos]
		if b.offset >= endOffset {
			break
		}
		bufEnd := b.offset+uint64(len(b.buf))
		if (dirty || b.dirtyID == 0) && bufEnd > offset && endOffset > b.offset {
			if offset <= b.offset {
				if endOffset >= bufEnd {
					// whole buffer
					pool.FreeBuffer(&buffers, pos)
					pos--
				} else {
					// beginning
					b.buf = b.buf[endOffset - b.offset : ]
					b.offset = endOffset
				}
			} else if endOffset >= bufEnd {
				// end
				b.buf = b.buf[0:offset - b.offset]
			} else {
				// middle
				b.ptr.refs++
				startBuf := FileBuffer{
					offset: b.offset,
					dirtyID: b.dirtyID,
					flushed: b.flushed,
					buf: b.buf[0:offset-b.offset],
					ptr: b.ptr,
				}
				endBuf := FileBuffer{
					offset: endOffset,
					dirtyID: b.dirtyID,
					flushed: b.flushed,
					buf: b.buf[endOffset-b.offset : ],
					ptr: b.ptr,
				}
				if b.dirtyID != 0 {
					endBuf.dirtyID = atomic.AddUint64(&pool.curDirtyID, 1)
				}
				// Ugly insert() again
				buffers = append(buffers, FileBuffer{})
				copy(buffers[pos+2 : ], buffers[pos+1 : ])
				buffers[pos] = startBuf
				buffers[pos+1] = endBuf
			}
		}
	}

	// Insert non-overlapping parts of the buffer
	curOffset := offset
	dataPtr := &BufferPointer{
		buf: data,
		refs: 0,
	}
	for pos := start; pos < len(buffers) && curOffset < endOffset; pos++ {
		b := &buffers[pos]
		if b.offset > curOffset {
			// insert curOffset->min(b.offset,endOffset)
			nextEnd := b.offset
			if nextEnd > endOffset {
				nextEnd = endOffset
			}
			buffers = insertBuffer(forInode, pool, buffers, pos, curOffset, data[curOffset-offset : nextEnd-offset], dirty, copyData, dataPtr)
		}
		curOffset = b.offset+uint64(len(b.buf))
	}
	if curOffset < endOffset {
		// Insert curOffset->endOffset
		buffers = insertBuffer(forInode, pool, buffers, len(buffers), curOffset, data[curOffset-offset : ], dirty, copyData, dataPtr)
	}

	return buffers
}

func (fh *FileHandle) WriteFile(offset int64, data []byte) (err error) {
	fh.inode.logFuse("WriteFile", offset, len(data))

	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()

	// FIXME With this cache in action, usual kernel page cache is probably redundant
	fh.inode.buffers = addFileBuffer(fh.inode.Id, fh.inode.fs.bufferPool, fh.inode.buffers, uint64(offset), data, true, true)
	end := uint64(offset)+uint64(len(data))
	fh.inode.lastWriteEnd = end

	if fh.inode.CacheState != ST_CREATED {
		fh.inode.CacheState = ST_MODIFIED
		fh.inode.fs.flusherCond.Broadcast()
	}
	if fh.inode.Attributes.Size < end {
		fh.inode.Attributes.Size = end
	}
	fh.inode.Attributes.Mtime = time.Now()

	/*
	// we are updating this file, set knownETag to nil so
	// on next lookup we won't think it's changed, to
	// always prefer to read back our own write. We set
	// this back to the ETag at flush time
	//
	// XXX this doesn't actually work, see the notes in
	// Goofys.OpenFile about KeepPageCache
	fh.inode.knownETag = nil
	fh.inode.invalidateCache = false
	*/

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
		pos = b.offset+uint64(len(b.buf))
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
	for i := 0; i < len(requests); i += 2 {
		offset := requests[i]
		size := requests[i+1]
		wg.Add(1)
		go func() {
			defer wg.Done()
			// Maybe free some buffers first
			inode.fs.bufferPool.Use(inode.Id, size, false)
			resp, err := cloud.GetBlob(&GetBlobInput{
				Key:   key,
				Start: offset,
				Count: size,
			})
			if err != nil {
				inode.fs.bufferPool.Free(size, false)
				requestErr = err
				return
			}
			data, err := ioutil.ReadAll(resp.Body)
			if err != nil {
				inode.fs.bufferPool.Free(size, false)
				requestErr = err
				return
			}
			if uint64(len(data)) < size {
				inode.fs.bufferPool.Free(size-uint64(len(data)), false)
			}
			// Cache result from the server
			inode.mu.Lock()
			inode.buffers = addFileBuffer(inode.Id, inode.fs.bufferPool, inode.buffers, offset, data, false, false)
			inode.mu.Unlock()
		}()
	}
	wg.Wait()

	inode.mu.Lock()
	return
}

func (inode *Inode) LockRange(offset uint64, size uint64) {
	inode.readRanges = append(inode.readRanges, ReadRange{
		Offset: offset,
		Size: size,
	})
}

func (inode *Inode) UnlockRange(offset uint64, size uint64) {
	for i, v := range inode.readRanges {
		if v.Offset == offset && v.Size == size {
			inode.readRanges = append(inode.readRanges[0 : i], inode.readRanges[i+1 : ]...)
			break
		}
	}
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
		if fh.inode.Invalid {
			err = fuse.ENOENT
		} else if fh.inode.KnownSize == nil {
			err = io.EOF
		} else {
			err = io.EOF
		}
		return
	}
	end := offset+uint64(len(buf))
	if end >= fh.inode.Attributes.Size {
		end = fh.inode.Attributes.Size
	}

	// Guard buffers against eviction
	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()
	fh.inode.LockRange(offset, end-offset)
	defer fh.inode.UnlockRange(offset, end-offset)

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
				for j := pos-offset; j < b.offset-offset; j++ {
					buf[j] = 0
				}
				pos = b.offset
			} else {
				err = requestErr
				if err == nil {
					err = fuse.EIO
				}
				return
			}
		}
		readEnd := b.offset+uint64(len(b.buf))
		if readEnd > end {
			readEnd = end
		}
		copy(buf[pos-offset : readEnd-offset], b.buf[pos-b.offset : readEnd-b.offset])
		pos = readEnd
	}
	if pos < end {
		// How is this possible? We should've just received it from the server!
		if fh.inode.CacheState == ST_CREATED {
			// It's okay if the file is just created
			// Zero empty ranges in this case
			for j := pos-offset; j < end-offset; j++ {
				buf[j] = 0
			}
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

	// FIXME: atomic probably isn't needed (always guarded by mutex)
	n := atomic.AddInt32(&fh.inode.fileHandles, -1)
	if n == -1 {
		panic(fh.inode.fileHandles)
	} else if n == 0 {
		// delete fh
		fh.inode.fileHandle = nil
	}

	fh.inode.fs.flusherCond.Broadcast()
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
		last = buf.offset + uint64(len(buf.buf))
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
	// FIXME: Don't fail to load range when the file is extended by sparse write
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
		buf: b.buf[size : ],
		ptr: b.ptr,
	}
	endBuf.ptr.refs++
	if b.dirtyID != 0 {
		endBuf.dirtyID = atomic.AddUint64(&inode.fs.bufferPool.curDirtyID, 1)
	}
	b.buf = b.buf[0 : size]
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
		last = b.offset+uint64(len(b.buf))
		if last > end {
			// Split the buffer
			inode.splitBuffer(i, end-b.offset)
			b = &inode.buffers[i]
			last = b.offset+uint64(len(b.buf))
		}
		if b.dirtyID != 0 {
			bufIds[b.dirtyID] = true
		}
		if last >= end {
			reader.AddBuffer(b.buf[0 : end-b.offset])
			break
		} else {
			reader.AddBuffer(b.buf)
		}
	}
	if last < end {
		// Again, can happen for new sparse files
		reader.AddZero(end-last)
	}
	return
}

func (inode *Inode) FlushSmallObject() {

	inode.mu.Lock()

	sz := inode.Attributes.Size
	if sz > SINGLE_PART_SIZE || inode.CacheState != ST_CREATED && inode.CacheState != ST_MODIFIED {
		inode.IsFlushing--
		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.flusherCond.Broadcast()
		inode.mu.Unlock()
		return
	}

	inode.LockRange(0, sz)

	if inode.CacheState == ST_MODIFIED {
		inode.CheckLoadRange(0, sz)
		if inode.Attributes.Size < sz {
			// File size may have been changed in between
			sz = inode.Attributes.Size
		}
	}

	cloud, key := inode.cloud()
	bufReader, bufIds := inode.GetMultiReader(0, sz)
	params := &PutBlobInput{
		Key:         key,
		Body:        bufReader,
		Size:        PUInt64(uint64(bufReader.Len())),
		ContentType: inode.fs.flags.GetMimeType(*inode.FullName()),
	}

	inode.mu.Unlock()
	resp, err := cloud.PutBlob(params)
	inode.mu.Lock()

	if err != nil {
		// FIXME Handle failures
		log.Errorf("Failed to flush small file %v: %v", key, err)
	} else {
		log.Debugf("Flushed small file %v", key)
		stillDirty := false
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
		if !stillDirty {
			inode.CacheState = ST_CACHED
		}
		inode.updateFromFlush(resp.ETag, resp.LastModified, resp.StorageClass)
	}

	inode.IsFlushing--
	atomic.AddInt64(&inode.fs.activeFlushers, -1)
	inode.fs.flusherCond.Broadcast()

	inode.UnlockRange(0, sz)
	inode.mu.Unlock()
}

func (inode *Inode) copyUnmodifiedRange(startPart uint64, endPart uint64) error {
	cloud, key := inode.cloud()
	startOffset, _ := inode.fs.partRange(startPart)
	endOffset, endSize := inode.fs.partRange(endPart-1)
	log.Debugf("Copying unmodified range %v-%v MB of object %v", startOffset/1024/1024, (endOffset+endSize)/1024/1024, key)
	_, err := cloud.MultipartBlobCopy(&MultipartBlobCopyInput{
		Commit:     inode.mpu,
		PartNumber: uint32(startPart+1),
		CopySource: key,
		Offset:     startOffset,
		Size:       endOffset+endSize-startOffset,
	})
	if err != nil {
		log.Errorf("Failed to copy unmodified range %v-%v MB of object %v: %v", startOffset/1024/1024, (endOffset+endSize)/1024/1024, key, err)
	}
	return err
}

func (inode *Inode) copyUnmodifiedParts(numParts uint64) (err error) {
	// FIXME: move to configuration and maybe merge with MaxFlushers
	noMerge := true
	maxParallelCopy := 16
	// First collect ranges to be unaffected by sudden parallel changes
	var ranges []uint64
	var lastStart, lastEnd uint64
	for i := uint64(0); i < numParts; i++ {
		if inode.mpu.Parts[i] == nil {
			if noMerge {
				ranges = append(ranges, i, i+1)
			} else {
				if lastEnd == 0 {
					lastStart = i
				}
				lastEnd = i+1
			}
		} else if lastEnd != 0 {
			ranges = append(ranges, lastStart, lastEnd)
			lastEnd = 0
		}
	}
	if lastEnd != 0 {
		ranges = append(ranges, lastStart, lastEnd)
	}
	guard := make(chan int, maxParallelCopy)
	var wg sync.WaitGroup
	inode.mu.Unlock()
	for i := 0; i < len(ranges); i += 2 {
		guard <- i
		wg.Add(1)
		go func(start, end uint64) {
			requestErr := inode.copyUnmodifiedRange(start, end)
			if requestErr != nil {
				err = requestErr
			}
			wg.Done()
			<- guard
		}(uint64(ranges[i]), uint64(ranges[i+1]))
	}
	wg.Wait()
	inode.mu.Lock()
	return
}

func (inode *Inode) FlushMultipart() {

	inode.mu.Lock()

	// FIXME: Upload parts of the same object in parallel
	if inode.Attributes.Size <= SINGLE_PART_SIZE || inode.CacheState != ST_CREATED && inode.CacheState != ST_MODIFIED {
		inode.IsFlushing--
		inode.mu.Unlock()
		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.flusherCond.Broadcast()
		return
	}

	// Pick a part ID to flush
	var part, partOffset, partSize uint64
	var found bool
	for i := 0; i < len(inode.buffers); i++ {
		buf := &inode.buffers[i]
		if buf.dirtyID != 0 && !buf.flushed {
			part = inode.fs.partNum(buf.offset)
			// Don't write out the last part that's still written to
			if inode.fileHandles == 0 || part != inode.fs.partNum(inode.lastWriteEnd) {
				partOffset, partSize = inode.fs.partRange(part)
				found = true
				break
			}
		}
	}
	if !found {
		inode.IsFlushing--
		inode.mu.Unlock()
		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.flusherCond.Broadcast()
		return
	}

	cloud, key := inode.cloud()
	log.Debugf("Flushing part %v (%v-%v MB) of %v", part, partOffset/1024/1024, (partOffset+partSize)/1024/1024, key)

	// Initiate multipart upload, if not yet
	if inode.mpu == nil {
		inode.mu.Unlock()
		resp, err := cloud.MultipartBlobBegin(&MultipartBlobBeginInput{
			Key: key,
			ContentType: inode.fs.flags.GetMimeType(key),
		})
		inode.mu.Lock()
		if err != nil {
			//fh.lastWriteError = mapAwsError(err) // FIXME return to user?
			log.Errorf("Failed to initiate multipart upload for %v: %v", key, err)
		} else {
			inode.mpu = resp
		}
		// File size may have been changed
		if inode.Attributes.Size <= partOffset || inode.CacheState != ST_CREATED && inode.CacheState != ST_MODIFIED {
			// Don't flush the part at all
			inode.IsFlushing--
			inode.mu.Unlock()
			atomic.AddInt64(&inode.fs.activeFlushers, -1)
			inode.fs.flusherCond.Broadcast()
			return
		}
	}

	// Last part may be shorter
	if inode.Attributes.Size < partOffset+partSize {
		partSize = inode.Attributes.Size-partOffset
	}

	// Guard part against eviction
	inode.LockRange(partOffset, partSize)

	// Load part from the server if we have to read-modify-write it
	if inode.CacheState == ST_MODIFIED {
		inode.CheckLoadRange(partOffset, partSize)
		// File size may have been changed again
		if inode.Attributes.Size <= partOffset || inode.CacheState != ST_CREATED && inode.CacheState != ST_MODIFIED {
			// Don't flush the part at all
			inode.UnlockRange(partOffset, partSize)
			inode.IsFlushing--
			inode.mu.Unlock()
			atomic.AddInt64(&inode.fs.activeFlushers, -1)
			inode.fs.flusherCond.Broadcast()
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
	_, err := cloud.MultipartBlobAdd(&partInput)
	inode.mu.Lock()

	if err != nil {
		// FIXME Handle failures
		log.Errorf("Failed to flush part %v of object %v: %v", part, key, err)
	} else {
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
		if !stillDirty && inode.fileHandles == 0 && (
			inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED) {
			// Server-size copy unmodified parts
			numParts := inode.fs.partNum(inode.Attributes.Size)
			numPartOffset, _ := inode.fs.partRange(numParts)
			if numPartOffset < inode.Attributes.Size {
				numParts++
			}
			err := inode.copyUnmodifiedParts(numParts)
			if err == nil {
				// Finalize the upload
				inode.mpu.NumParts = uint32(numParts)
				inode.mu.Unlock()
				resp, err := cloud.MultipartBlobCommit(inode.mpu)
				inode.mu.Lock()
				if err != nil {
					// FIXME handle failures
					log.Errorf("Failed to finalize multi-part upload of object %v: %v", key, err)
				} else {
					inode.mpu = nil
					inode.updateFromFlush(resp.ETag, resp.LastModified, resp.StorageClass)
					stillDirty := false
					for i := 0; i < len(inode.buffers); i++ {
						if inode.buffers[i].flushed {
							inode.buffers[i].dirtyID = 0
							inode.buffers[i].flushed = false
						}
						if inode.buffers[i].dirtyID != 0 {
							stillDirty = true
						}
					}
					if !stillDirty && (inode.CacheState == ST_CREATED || inode.CacheState == ST_MODIFIED) {
						inode.CacheState = ST_CACHED
					}
				}
			}
		}
	}

	inode.UnlockRange(partOffset, partSize)
	inode.IsFlushing--
	inode.mu.Unlock()
	atomic.AddInt64(&inode.fs.activeFlushers, -1)
	inode.fs.flusherCond.Broadcast()
}

func (inode *Inode) updateFromFlush(etag *string, lastModified *time.Time, storageClass *string) {
	if etag != nil {
		inode.s3Metadata["etag"] = []byte(*etag)
	}
	if storageClass != nil {
		inode.s3Metadata["storage-class"] = []byte(*storageClass)
	}
	if false { // FIXME was if inode.keepPageCache
		// if this write didn't update page cache, don't try
		// to update these values so on next lookup, we would
		// invalidate the cache. We want to do that because
		// our cache could have been populated by subsequent
		// reads
		if lastModified != nil {
			inode.Attributes.Mtime = *lastModified
		}
		inode.knownETag = etag
	}
}

/*func (fh *FileHandle) resetToKnownSize() {
	if fh.inode.KnownSize != nil {
		fh.inode.Attributes.Size = *fh.inode.KnownSize
	} else {
		fh.inode.Attributes.Size = 0
		fh.inode.Invalid = true
	}
}*/

func (fh *FileHandle) FlushFile() (err error) {
//	fh.mu.Lock()
//	defer fh.mu.Unlock()

	fh.inode.logFuse("FlushFile")

/*	if !fh.dirty || fh.lastWriteError != nil {
		if fh.lastWriteError != nil {
			err = fh.lastWriteError
			fh.resetToKnownSize()
		}
		return
	}

	if fh.inode.Parent == nil {
		// the file is deleted
		if fh.mpuId != nil {
			go func() {
				_, _ = fh.cloud.MultipartBlobAbort(fh.mpuId)
				fh.mpuId = nil
			}()
		}
		return
	}

	fs := fh.inode.fs

	// abort mpu on error
	defer func() {
		if err != nil {
			if fh.mpuId != nil {
				go func() {
					_, _ = fh.cloud.MultipartBlobAbort(fh.mpuId)
					fh.mpuId = nil
				}()
			}

			fh.resetToKnownSize()
		} else {
			if fh.dirty {
				// don't unset this if we never actually flushed
				size := fh.inode.Attributes.Size
				fh.inode.KnownSize = &size
				fh.inode.Invalid = false
			}
			fh.dirty = false
		}

		fh.writeInit = sync.Once{}
		fh.nextWriteOffset = 0
		fh.lastPartId = 0
	}()

	if fh.lastPartId == 0 {
		return fh.flushSmallFile()
	}

	fh.mpuWG.Wait()

	if fh.lastWriteError != nil {
		return fh.lastWriteError
	}

	if fh.mpuId == nil {
		return
	}

	nParts := fh.lastPartId
	if fh.buf != nil {
		// upload last part
		nParts++
		err = fh.mpuPartNoSpawn(fh.buf, nParts, fh.nextWriteOffset, true)
		if err != nil {
			return
		}
		fh.buf = nil
	}

	resp, err := fh.cloud.MultipartBlobCommit(fh.mpuId)
	if err != nil {
		return
	}

	fh.updateFromFlush(resp.ETag, resp.LastModified, resp.StorageClass)

	fh.mpuId = nil

	// we want to get key from inode because the file could have been renamed
	_, key := fh.inode.cloud()
	if *fh.mpuName != key {
		// the file was renamed
		err = fh.inode.renameObject(fs, PUInt64(uint64(fh.nextWriteOffset)), *fh.mpuName, *fh.inode.FullName())
	}*/

	return
}
