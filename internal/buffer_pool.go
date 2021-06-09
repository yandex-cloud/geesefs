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
	. "github.com/kahing/goofys/api/common"
	"github.com/jacobsa/fuse/fuseops"
	"io"
	"runtime"
	"runtime/debug"
	"sync"
	"github.com/shirou/gopsutil/mem"
)

var bufferLog = GetLogger("buffer")

// BufferPool tracks memory used by cache buffers
type BufferPool struct {
	mu   sync.Mutex
	cond *sync.Cond

	curDirtyID uint64

	cur uint64
	curDirty uint64
	max uint64

	requests uint64

	limit uint64

	FreeSomeCleanBuffers func(inode fuseops.InodeID, size uint64) uint64
}

// Several FileBuffers may be slices of the same array,
// but we want to track memory usage, so we have to refcount them...
type BufferPointer struct {
	buf []byte
	refs int
}

type FileBuffer struct {
	offset uint64
	// Unmodified chunks (equal to the current server-side object state) have dirtyID = 0.
	// Every write or split assigns a new unique chunk ID.
	// Flusher tracks IDs that are currently being flushed to the server,
	// which allows to do flush and write in parallel.
	dirtyID uint64
	// Is this chunk already saved to the server as a part of multipart upload?
	flushed bool
	// Data
	buf []byte
	ptr *BufferPointer
}

type MultiBuffer struct {
	data []byte
	zero bool
	size uint64
}

type MultiReader struct {
	buffers []MultiBuffer
	idx int
	pos uint64
	bufPos uint64
	size uint64
}

func NewMultiReader() *MultiReader {
	return &MultiReader{
	}
}

func (r *MultiReader) AddBuffer(buf []byte) {
	r.buffers = append(r.buffers, MultiBuffer{
		data: buf,
		size: uint64(len(buf)),
	})
	r.size += uint64(len(buf))
}

func (r *MultiReader) AddZero(size uint64) {
	r.buffers = append(r.buffers, MultiBuffer{
		zero: true,
		size: size,
	})
	r.size += size
}

func (r *MultiReader) Read(buf []byte) (n int, err error) {
	n = 0
	if r.idx >= len(r.buffers) {
		err = io.EOF
		return
	}
	remaining := uint64(len(buf))
	outPos := uint64(0)
	for r.idx < len(r.buffers) && remaining > 0 {
		l := r.buffers[r.idx].size - r.bufPos
		if l > remaining {
			l = remaining
		}
		if r.buffers[r.idx].zero {
			for i := outPos; i < outPos+l; i++ {
				buf[i] = 0
			}
		} else {
			copy(buf[outPos : outPos+l], r.buffers[r.idx].data[r.bufPos : r.bufPos+l])
		}
		outPos += l
		remaining -= l
		r.pos += l
		r.bufPos += l
		if r.bufPos >= r.buffers[r.idx].size {
			r.idx++
			r.bufPos = 0
		}
	}
	n = int(outPos)
	return
}

func (r *MultiReader) Seek(offset int64, whence int) (newOffset int64, err error) {
	if whence == io.SeekEnd {
		offset += int64(r.size)
	} else if whence == io.SeekCurrent {
		offset += int64(r.pos)
	}
	if offset > int64(r.size) {
		offset = int64(r.size)
	}
	if offset < 0 {
		offset = 0
	}
	uOffset := uint64(offset)
	r.idx = 0
	r.pos = 0
	r.bufPos = 0
	for r.pos < uOffset {
		end := r.pos + r.buffers[r.idx].size
		if end <= uOffset {
			r.pos = end
			r.idx++
		} else {
			r.bufPos = uOffset-r.pos
			r.pos = uOffset
		}
	}
	return int64(r.pos), nil
}

func (r *MultiReader) Len() uint64 {
	return r.size
}

func maxMemToUse(usedMem uint64) uint64 {
	m, err := mem.VirtualMemory()
	if err != nil {
		panic(err)
	}

	availableMem, err := getCgroupAvailableMem()
	if err != nil {
		log.Debugf("amount of available memory from cgroup is: %v", availableMem/1024/1024)
	}

	if err != nil || availableMem < 0 || availableMem > m.Available {
		availableMem = m.Available
	}

	log.Debugf("amount of available memory: %v", availableMem/1024/1024)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	log.Debugf("amount of allocated memory: %v/%v MB", ms.Sys/1024/1024, ms.Alloc/1024/1024)

	max := availableMem+usedMem
	log.Debugf("using up to %vMB for in-memory buffers (%v MB used)", max/1024/1024, usedMem/1024/1024)

	return max
}

func (pool BufferPool) Init() *BufferPool {
	pool.cond = sync.NewCond(&pool.mu)
	return &pool
}

func NewBufferPool(limit uint64) *BufferPool {
	pool := BufferPool{limit: limit}.Init()
	return pool
}

func (pool *BufferPool) recomputeBufferLimit() {
	pool.max = maxMemToUse(pool.cur)
	if pool.limit > 0 && pool.max > pool.limit {
		pool.max = pool.limit
	}
}

func (pool *BufferPool) Use(inode fuseops.InodeID, size uint64, dirty bool) {
	if dirty {
		// FIXME
		return
	}

	bufferLog.Debugf("requesting %v", size)

	pool.mu.Lock()
	defer pool.mu.Unlock()
	pool.requests++
	if pool.requests >= 16 {
		debug.FreeOSMemory()
		pool.recomputeBufferLimit()
		pool.requests = 0
	}

	for pool.cur+size > pool.max {
		// Try to free clean buffers, then flush dirty buffers
		freed := pool.FreeSomeCleanBuffers(inode, pool.cur+size - pool.max)
		bufferLog.Debugf("Freed %v, now: %v %v %v", freed, pool.cur, size, pool.max)
		if pool.cur+size > pool.max {
			if pool.cur == 0 {
				debug.FreeOSMemory()
				pool.recomputeBufferLimit()
				if pool.cur+size > pool.max {
					// we don't have any in use buffers, and we've made attempts to
					// free memory AND correct our limits, yet we still can't allocate.
					// it's likely that we are simply asking for too much
					log.Errorf("Unable to allocate %d bytes, used %d bytes, limit is %d bytes", size, pool.cur, pool.max)
					panic("OOM")
				} else {
					break
				}
			} else {
				pool.cond.Wait()
			}
		}
	}

	pool.cur += size
	if dirty {
		pool.curDirty += size
	}
}

func (pool *BufferPool) Free(size uint64, dirty bool) {
	if dirty {
		// FIXME
		return
	}
	pool.mu.Lock()
	defer pool.mu.Unlock()
	pool.FreeUnlocked(size, dirty)
}

func (pool *BufferPool) FreeUnlocked(size uint64, dirty bool) {
	if dirty {
		// FIXME
		return
	}
	notify := pool.cur+size > pool.max
	pool.cur -= size
	if dirty {
		pool.curDirty -= size
	}
	if notify {
		pool.cond.Broadcast()
	}
}

func (pool *BufferPool) AddDirty(size int64) {
	pool.mu.Lock()
	defer pool.mu.Unlock()
	pool.curDirty = uint64(int64(pool.curDirty)+size)
}

func (pool *BufferPool) FreeBuffer(buffers *[]FileBuffer, i int) uint64 {
	buf := &((*buffers)[i])
	freed := uint64(0)
	buf.ptr.refs--
	if buf.ptr.refs == 0 {
		freed = uint64(len(buf.ptr.buf))
		pool.Free(freed, buf.dirtyID != 0)
	}
	*buffers = append((*buffers)[0 : i], (*buffers)[i+1 : ]...)
	return freed
}

func (pool *BufferPool) FreeBufferUnlocked(buffers *[]FileBuffer, i int) uint64 {
	buf := &((*buffers)[i])
	freed := uint64(0)
	buf.ptr.refs--
	if buf.ptr.refs == 0 {
		freed = uint64(len(buf.ptr.buf))
		pool.FreeUnlocked(freed, buf.dirtyID != 0)
	}
	*buffers = append((*buffers)[0 : i], (*buffers)[i+1 : ]...)
	return freed
}
