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
	"fmt"
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

/*	mpuName   *string
	writeInit sync.Once
	mpuWG     sync.WaitGroup

	mu              sync.Mutex
	mpuId           *MultipartBlobCommitInput
	lastPartId      uint32
	lastWriteError error

	// read
	reader        io.ReadCloser
	readBufOffset int64

	existingReadahead int
	seqReadAmount     uint64
	numOOORead        uint64 // number of out of order read*/
	// User space PID. All threads created by a process will have the same TGID,
	// but different PIDs[1].
	// This value can be nil if we fail to get TGID from PID[2].
	// [1] : https://godoc.org/github.com/shirou/gopsutil/process#Process.Tgid
	// [2] : https://github.com/shirou/gopsutil#process-class
	Tgid *int32

	keepPageCache bool // the same value we returned to OpenFile
}

// FIXME -> to configuration
const MAX_BUF_INC = uint32(1024 * 1024)
const MAX_READAHEAD = uint32(400 * 1024 * 1024)
const READAHEAD_CHUNK = uint32(20 * 1024 * 1024)
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

/*
func (fh *FileHandle) initWrite() {
	fh.writeInit.Do(func() {
		fh.mpuWG.Add(1)
		go fh.initMPU()
	})
}

func (fh *FileHandle) initMPU() {
	defer func() {
		fh.mpuWG.Done()
	}()

	fs := fh.inode.fs
	fh.mpuName = &fh.key

	resp, err := fh.cloud.MultipartBlobBegin(&MultipartBlobBeginInput{
		Key:         *fh.mpuName,
		ContentType: fs.flags.GetMimeType(*fh.mpuName),
	})

	fh.mu.Lock()
	defer fh.mu.Unlock()

	if err != nil {
		fh.lastWriteError = mapAwsError(err)
	} else {
		fh.mpuId = resp
	}

	return
}

func (fh *FileHandle) mpuPartNoSpawn(buf *MBuf, part uint32, total int64, last bool) (err error) {
	fs := fh.inode.fs

	fs.replicators.Take(1, true)
	defer fs.replicators.Return(1)

	if part == 0 || part > 10000 {
		return errors.New(fmt.Sprintf("invalid part number: %v", part))
	}

	mpu := MultipartBlobAddInput{
		Commit:     fh.mpuId,
		PartNumber: part,
		Body:       buf,
		Size:       uint64(buf.Len()),
		Last:       last,
		Offset:     uint64(total - int64(buf.Len())),
	}

	defer func() {
		if mpu.Body != nil {
			bufferLog.Debugf("Free %T", buf)
			buf.Free()
		}
	}()

	_, err = fh.cloud.MultipartBlobAdd(&mpu)

	return
}

func (fh *FileHandle) mpuPart(buf *MBuf, part uint32, total int64) {
	defer func() {
		fh.mpuWG.Done()
	}()

	// maybe wait for CreateMultipartUpload
	if fh.mpuId == nil {
		fh.mpuWG.Wait()
		// initMPU might have errored
		if fh.mpuId == nil {
			return
		}
	}

	err := fh.mpuPartNoSpawn(buf, part, total, false)
	if err != nil {
		if fh.lastWriteError == nil {
			fh.lastWriteError = err
		}
	}
}

func (fh *FileHandle) waitForCreateMPU() (err error) {
	if fh.mpuId == nil {
		fh.mu.Unlock()
		fh.initWrite()
		fh.mpuWG.Wait() // wait for initMPU
		fh.mu.Lock()

		if fh.lastWriteError != nil {
			return fh.lastWriteError
		}
	}

	return
}

func (fh *FileHandle) partSize() uint64 {
	var size uint64

	if fh.lastPartId <= 1000 {
		size = 5 * 1024 * 1024
	} else if fh.lastPartId <= 2000 {
		size = 25 * 1024 * 1024
	} else {
		size = 125 * 1024 * 1024
	}

	maxPartSize := fh.cloud.Capabilities().MaxMultipartSize
	if maxPartSize != 0 {
		size = MinUInt64(maxPartSize, size)
	}

	return size
}

func (fh *FileHandle) partNum(offset uint64) uint64 {
	maxPartSize := fh.cloud.Capabilities().MaxMultipartSize
	// 5 MB
	bs = 5*1024*1024
	if bs > maxPartSize {
		return offset/maxPartSize
	}
	n := offset/bs
	if n < 1000 {
		return n
	}
	// 25 MB
	bs := 25*1024*1024
	if bs > maxPartSize {
		return 1000 + (offset-1000*5*1024*1024)/maxPartSize
	}
	n = (offset-1000*5*1024*1024)/bs
	if n < 1000 {
		return 1000 + n
	}
	// 125 MB
	bs = 125*1024*1024
	if bs > maxPartSize {
		bs = maxPartSize
	}
	return 2000 + (offset-1000*5*1024*1024-1000*25*1024*1024)/bs
}*/

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

/*func (fh *FileHandle) uploadCurrentBuf(parallel bool) (err error) {
	err = fh.waitForCreateMPU()
	if err != nil {
		return
	}

	fh.lastPartId++
	part := fh.lastPartId
	buf := fh.buf
	fh.buf = nil

	if parallel {
		fh.mpuWG.Add(1)
		go fh.mpuPart(buf, part, fh.nextWriteOffset)
	} else {
		err = fh.mpuPartNoSpawn(buf, part, fh.nextWriteOffset, false)
		if fh.lastWriteError == nil {
			fh.lastWriteError = err
		}
	}

	return
}*/

/*func extendBuf(buf []byte, request int64) []byte {
	oldLen := len(buf)
	if cap(buf) < request {
		inc := oldLen
		if inc > MAX_BUF_INC {
			inc = MAX_BUF_INC
		}
		if inc < request-oldLen {
			inc = request-oldLen
		}
		newBuf := make([]byte, oldLen+inc)
		copy(newBuf, buf)
		return newBuf[0:request]
	}
	return buf[0:request]
}*/

func locateBuffer(buffers []FileBuffer, offset uint64) int {
	start := 0
	for start < len(buffers) {
		b := &buffers[start]
		if b.offset + uint64(len(b.buf)) > offset {
			break
		}
		start++
	}
	return start
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
				last := buffers[pos+1 : ]
				buffers = append(buffers[0 : pos], startBuf, endBuf)
				buffers = append(buffers, last...)
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
			var newBuf []byte
			nextEnd := b.offset
			if nextEnd > endOffset {
				nextEnd = endOffset
			}
			if copyData {
				newBuf = make([]byte, nextEnd-curOffset)
				pool.Use(forInode, nextEnd-curOffset, dirty)
				copy(newBuf, data[curOffset-offset : nextEnd-offset])
				dataPtr = &BufferPointer{
					buf: newBuf,
					refs: 0,
				}
			} else {
				newBuf = data[curOffset-offset : nextEnd-offset]
			}
			dataPtr.refs++
			// FIXME maybe try to append to the previous buffer?
			last := buffers[pos+1 : ]
			dirtyID := uint64(0)
			if dirty {
				dirtyID = atomic.AddUint64(&pool.curDirtyID, 1)
			}
			buffers = append(buffers[0 : pos], FileBuffer{
				offset: curOffset,
				dirtyID: dirtyID,
				flushed: false,
				buf: newBuf,
				ptr: dataPtr,
			})
			buffers = append(buffers, last...)
		}
		curOffset = b.offset+uint64(len(b.buf))
	}
	if curOffset < endOffset {
		// Insert curOffset->endOffset
		var newBuf []byte
		if copyData {
			newBuf = make([]byte, endOffset-curOffset)
			pool.Use(forInode, endOffset-curOffset, dirty)
			copy(newBuf, data[curOffset-offset : ])
			dataPtr = &BufferPointer{
				buf: newBuf,
				refs: 0,
			}
		} else {
			newBuf = data[curOffset-offset : ]
		}
		dataPtr.refs++
		dirtyID := uint64(0)
		if dirty {
			dirtyID = atomic.AddUint64(&pool.curDirtyID, 1)
		}
		buffers = append(buffers, FileBuffer{
			offset: curOffset,
			dirtyID: dirtyID,
			flushed: false,
			buf: newBuf,
			ptr: dataPtr,
		})
	}

	return buffers
}

func (fh *FileHandle) WriteFile(offset int64, data []byte) (err error) {
	fh.inode.logFuse("WriteFile", offset, len(data))

	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()

	// FIXME With this cache in action, usual kernel page cache is probably redundant
	fh.inode.buffers = addFileBuffer(fh.inode.Id, fh.inode.fs.bufferPool, fh.inode.buffers, uint64(offset), data, true, true)

	if fh.inode.CacheState != ST_CREATED {
		fh.inode.CacheState = ST_MODIFIED
		fh.inode.fs.flusherCond.Broadcast()
	}
	if fh.inode.Attributes.Size < uint64(offset)+uint64(len(data)) {
		fh.inode.Attributes.Size = uint64(offset)+uint64(len(data))
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

	err = fh.uploadCurrentBuf(!fh.cloud.Capabilities().NoParallelMultipart)
	if err != nil {
		return
	}
	*/

	return
}

/*type S3ReadBuffer struct {
	s3          StorageBackend
	startOffset uint64
	nRetries    uint8
	mbuf        *MBuf

	offset uint64
	size   uint32
	buf    *Buffer
}

func (b S3ReadBuffer) Init(fh *FileHandle, offset uint64, size uint32) *S3ReadBuffer {
	b.s3 = fh.cloud
	b.offset = offset
	b.startOffset = offset
	b.size = size
	b.nRetries = 3

	b.mbuf = MBuf{}.Init(fh.inode.fs.bufferPool, uint64(size), false)
	if b.mbuf == nil {
		return nil
	}

	b.initBuffer(fh, offset, size)
	return &b
}

func (b *S3ReadBuffer) initBuffer(fh *FileHandle, offset uint64, size uint32) {
	getFunc := func() (io.ReadCloser, error) {
		resp, err := b.s3.GetBlob(&GetBlobInput{
			Key:   fh.key,
			Start: offset,
			Count: uint64(size),
		})
		if err != nil {
			return nil, err
		}

		return resp.Body, nil
	}

	if b.buf == nil {
		b.buf = Buffer{}.Init(b.mbuf, getFunc)
	} else {
		b.buf.ReInit(getFunc)
	}
}

func (b *S3ReadBuffer) Read(offset uint64, p []byte) (n int, err error) {
	if b.offset == offset {
		n, err = io.ReadFull(b.buf, p)
		if n != 0 && err == io.ErrUnexpectedEOF {
			err = nil
		}
		if n > 0 {
			if uint32(n) > b.size {
				panic(fmt.Sprintf("read more than available %v %v", n, b.size))
			}

			b.offset += uint64(n)
			b.size -= uint32(n)
		}
		if b.size == 0 && err != nil {
			// we've read everything, sometimes we may
			// request for more bytes then there's left in
			// this chunk so we could get an error back,
			// ex: http2: response body closed this
			// doesn't tend to happen because our chunks
			// are aligned to 4K and also 128K (except for
			// the last chunk, but seems kernel requests
			// for a smaller buffer for the last chunk)
			err = nil
		}

		return
	} else {
		panic(fmt.Sprintf("not the right buffer, expecting %v got %v, %v left", b.offset, offset, b.size))
		err = errors.New(fmt.Sprintf("not the right buffer, expecting %v got %v", b.offset, offset))
		return
	}
}

func (fh *FileHandle) readFromReadAhead(offset uint64, buf []byte) (bytesRead int, err error) {
	var nread int
	for len(fh.buffers) != 0 {
		readAheadBuf := fh.buffers[0]

		nread, err = readAheadBuf.Read(offset+uint64(bytesRead), buf)
		bytesRead += nread
		if err != nil {
			if err == io.EOF && readAheadBuf.size != 0 {
				// in case we hit
				// https://github.com/kahing/goofys/issues/464
				// again, this will convert that into
				// an error
				fuseLog.Errorf("got EOF when data remains: %v", *fh.inode.FullName())
				err = io.ErrUnexpectedEOF
			} else if err != io.EOF && readAheadBuf.size > 0 {
				// we hit some other errors when
				// reading from this part. If we can
				// retry, do that
				if readAheadBuf.nRetries > 0 {
					readAheadBuf.nRetries -= 1
					readAheadBuf.initBuffer(fh, readAheadBuf.offset, readAheadBuf.size)
					// we unset error and return,
					// so upper layer will retry
					// this read
					err = nil
				}
			}
			return
		}

		if readAheadBuf.size == 0 {
			// we've exhausted the first buffer
			readAheadBuf.buf.Close()
			fh.buffers = fh.buffers[1:]
		}

		buf = buf[nread:]

		if len(buf) == 0 {
			// we've filled the user buffer
			return
		}
	}

	return
}

func (fh *FileHandle) readAhead(offset uint64, needAtLeast int) (err error) {
	existingReadahead := uint32(0)
	for _, b := range fh.buffers {
		existingReadahead += b.size
	}

	readAheadAmount := MAX_READAHEAD

	for readAheadAmount-existingReadahead >= READAHEAD_CHUNK {
		off := offset + uint64(existingReadahead)
		remaining := fh.inode.Attributes.Size - off

		// only read up to readahead chunk each time
		size := MinUInt32(readAheadAmount-existingReadahead, READAHEAD_CHUNK)
		// but don't read past the file
		size = uint32(MinUInt64(uint64(size), remaining))

		if size != 0 {
			fh.inode.logFuse("readahead", off, size, existingReadahead)

			readAheadBuf := S3ReadBuffer{}.Init(fh, off, size)
			if readAheadBuf != nil {
				fh.buffers = append(fh.buffers, readAheadBuf)
				existingReadahead += size
			} else {
				if existingReadahead != 0 {
					// don't do more readahead now, but don't fail, cross our
					// fingers that we will be able to allocate the buffers
					// later
					return nil
				} else {
					return syscall.ENOMEM
				}
			}
		}

		if size != READAHEAD_CHUNK {
			// that was the last remaining chunk to readahead
			break
		}
	}

	return nil
}*/

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

/*func (fh *FileHandle) readFile(offset int64, buf []byte) (bytesRead int, err error) {
	defer func() {
		if bytesRead > 0 {
			fh.readBufOffset += int64(bytesRead)
			fh.seqReadAmount += uint64(bytesRead)
		}

		fh.inode.logFuse("< readFile", bytesRead, err)
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

	fs := fh.inode.fs

	if fh.readBufOffset != offset {
		// XXX out of order read, maybe disable prefetching
		fh.inode.logFuse("out of order read", offset, fh.readBufOffset)

		fh.readBufOffset = offset
		fh.seqReadAmount = 0
		if fh.reader != nil {
			fh.reader.Close()
			fh.reader = nil
		}

		if fh.buffers != nil {
			// we misdetected
			fh.numOOORead++
		}

		for _, b := range fh.buffers {
			b.buf.Close()
		}
		fh.buffers = nil
	}

	if !fs.flags.Cheap && fh.seqReadAmount >= uint64(READAHEAD_CHUNK) && fh.numOOORead < 3 {
		if fh.reader != nil {
			fh.inode.logFuse("cutover to the parallel algorithm")
			fh.reader.Close()
			fh.reader = nil
		}

		err = fh.readAhead(uint64(offset), len(buf))
		if err == nil {
			bytesRead, err = fh.readFromReadAhead(uint64(offset), buf)
			return
		} else {
			// fall back to read serially
			fh.inode.logFuse("not enough memory, fallback to serial read")
			fh.seqReadAmount = 0
			for _, b := range fh.buffers {
				b.buf.Close()
			}
			fh.buffers = nil
		}
	}

	bytesRead, err = fh.readFromStream(offset, buf)

	return
}*/

func (fh *FileHandle) Release() {
/*	// read buffers
	for _, b := range fh.buffers {
		b.buf.Close()
	}
	fh.buffers = nil

	if fh.reader != nil {
		fh.reader.Close()
	}

	// write buffers
	if fh.inode.fs.bufferPool != nil {
		if fh.buf != nil && fh.buf.buffers != nil {
			if fh.lastWriteError == nil {
				panic("buf not freed but error is nil")
			}

			fh.buf.Free()
			// the other in-flight multipart PUT buffers will be
			// freed when they finish/error out
		}
	}*/

	fh.inode.mu.Lock()
	defer fh.inode.mu.Unlock()

	n := atomic.AddInt32(&fh.inode.fileHandles, -1)
	if n == -1 {
		panic(fh.inode.fileHandles)
	} else if n == 0 {
		// delete fh
		fh.inode.fileHandle = nil
	}

	fh.inode.fs.flusherCond.Broadcast()
}

/*func (fh *FileHandle) readFromStream(offset int64, buf []byte) (bytesRead int, err error) {
	defer func() {
		if fh.inode.fs.flags.DebugFuse {
			fh.inode.logFuse("< readFromStream", bytesRead)
		}
	}()

	if uint64(offset) >= fh.inode.Attributes.Size {
		// nothing to read
		return
	}

	if fh.reader == nil {
		resp, err := fh.cloud.GetBlob(&GetBlobInput{
			Key:   fh.key,
			Start: uint64(offset),
		})
		if err != nil {
			return bytesRead, err
		}

		fh.reader = resp.Body
	}

	bytesRead, err = fh.reader.Read(buf)
	if err != nil {
		if err != io.EOF {
			fh.inode.logFuse("< readFromStream error", bytesRead, err)
		}
		// always retry error on read
		fh.reader.Close()
		fh.reader = nil
		err = nil
	}

	return
}*/

func (inode *Inode) FlushSmallObject() {

	inode.mu.Lock()
	defer inode.mu.Unlock()

	sz := inode.Attributes.Size
	if sz > SINGLE_PART_SIZE || inode.CacheState != ST_CREATED && inode.CacheState != ST_MODIFIED {
		inode.IsFlushing = false
		atomic.AddInt64(&inode.fs.activeFlushers, -1)
		inode.fs.flusherCond.Broadcast()
		return
	}

	inode.LockRange(0, sz)
	defer inode.UnlockRange(0, sz)

	if inode.CacheState == ST_MODIFIED {
		loadStart := uint64(0)
		loadEnd := uint64(0)
		last := uint64(0)
		for i := 0; i < len(inode.buffers); i++ {
			buf := &inode.buffers[i]
			if buf.offset > last {
				if loadEnd == 0 {
					loadStart = last
				}
				loadEnd = buf.offset
			}
			last = buf.offset + uint64(len(buf.buf))
			if last >= sz {
				break
			}
		}
		if last < sz {
			if loadEnd == 0 {
				loadStart = last
			}
			loadEnd = sz
		}
		if loadEnd > 0 {
			inode.LoadRange(loadStart, loadEnd-loadStart, true)
		}
	}

	var bufs [][]byte
	var bufIds map[uint64]bool
	bufs = make([][]byte, len(inode.buffers))
	bufIds = make(map[uint64]bool)
	last := uint64(0)
	for i := 0; i < len(inode.buffers); i++ {
		b := &inode.buffers[i]
		if b.offset > last {
			panic("It cannot be!")
		}
		last = b.offset+uint64(len(b.buf))
		bufIds[inode.buffers[i].dirtyID] = true
		if last > sz {
			bufs[i] = b.buf[0:sz-b.offset]
			break
		} else {
			bufs[i] = b.buf
		}
	}

	cloud, key := inode.cloud()
	bufReader := NewMultiReader(bufs)
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
				} else {
					b.flushed = false
				}
			}
		}
		if !stillDirty {
			inode.CacheState = ST_CACHED
		}
		inode.updateFromFlush(resp.ETag, resp.LastModified, resp.StorageClass)
	}

	inode.IsFlushing = false
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
