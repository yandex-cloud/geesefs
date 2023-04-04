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
	"io"
	"runtime"
	"runtime/debug"
	"sync"
	"sync/atomic"
	"syscall"
	"github.com/shirou/gopsutil/mem"
)

var bufferLog = GetLogger("buffer")

// BufferPool tracks memory used by cache buffers
type BufferPool struct {
	mu   sync.Mutex

	curDirtyID uint64

	cur int64
	max int64
	limit int64
	cgroupLimit uint64

	requested uint64
	gcPrev uint64
	gcInterval uint64

	FreeSomeCleanBuffers func(size int64) (int64, bool)
}

// Several FileBuffers may be slices of the same array,
// but we want to track memory usage, so we have to refcount them... O_o
type BufferPointer struct {
	mem []byte
	refs int
}

type BufferOrZero struct {
	data []byte
	zero bool
	size uint64
}

type MultiReader struct {
	buffers []BufferOrZero
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
	r.buffers = append(r.buffers, BufferOrZero{
		data: buf,
		size: uint64(len(buf)),
	})
	r.size += uint64(len(buf))
}

func (r *MultiReader) AddZero(size uint64) {
	r.buffers = append(r.buffers, BufferOrZero{
		zero: true,
		size: size,
	})
	r.size += size
}

func memzero(buf []byte) {
	for j := 0; j < len(buf); j++ {
		buf[j] = 0
	}
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
			memzero(buf[outPos : outPos+l])
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

func NewBufferPool(limit int64, gcInterval uint64) *BufferPool {

	max, _ := getCgroupAvailableMem()
	m, err := mem.VirtualMemory()
	if err != nil {
		panic(err)
	}
	if max > 0 {
		// divide cgroup limit by 2 by default
		max = max / 2
	}
	if max <= 0 || max > m.Available {
		max = m.Available
	}
	if limit > int64(max) {
		limit = int64(max)
	}

	pool := BufferPool{
		limit: limit,
		max: limit,
		gcInterval: gcInterval,
	}

	return &pool
}

func (pool *BufferPool) recomputeBufferLimit() {
	usedMem := atomic.LoadInt64(&pool.cur)

	var ms runtime.MemStats
	runtime.ReadMemStats(&ms)

	log.Debugf("limit: %v MB, buffers: %v MB, metadata: %v MB, system: %v MB",
		pool.limit >> 20, usedMem >> 20, (ms.Alloc-uint64(usedMem)) >> 20, ms.Sys >> 20)
}

func (pool *BufferPool) Use(size int64, ignoreMemoryLimit bool) (err error) {
	if size <= 0 {
		atomic.AddInt64(&pool.cur, size)
	} else {
		pool.mu.Lock()
		err = pool.UseUnlocked(size, ignoreMemoryLimit)
		pool.mu.Unlock()
	}
	return
}

func (pool *BufferPool) UseUnlocked(size int64, ignoreMemoryLimit bool) error {
	if size > 0 {
		req := atomic.AddUint64(&pool.requested, uint64(size))
		prev := atomic.LoadUint64(&pool.gcPrev)
		if pool.gcInterval > 0 && (req-prev) >= pool.gcInterval {
			debug.FreeOSMemory()
			pool.recomputeBufferLimit()
			atomic.StoreUint64(&pool.gcPrev, req)
		}
	}

	newSize := atomic.AddInt64(&pool.cur, size)

	if size > 0 && newSize > pool.max {
		// Try to free clean buffers, then flush dirty buffers
		freed, canFreeMoreAsync := pool.FreeSomeCleanBuffers(newSize - pool.max)
		bufferLog.Debugf("Freed %v, now: %v/%v", freed, newSize, pool.max)
		for atomic.LoadInt64(&pool.cur) > pool.max && canFreeMoreAsync && !ignoreMemoryLimit {
			freed, canFreeMoreAsync = pool.FreeSomeCleanBuffers(atomic.LoadInt64(&pool.cur) - pool.max)
			bufferLog.Debugf("Freed %v, now: %v/%v", freed, atomic.LoadInt64(&pool.cur), pool.max)
		}
		if atomic.LoadInt64(&pool.cur) > pool.max && !ignoreMemoryLimit {
			debug.FreeOSMemory()
			pool.recomputeBufferLimit()
			if atomic.LoadInt64(&pool.cur) > pool.max {
				// we can't free anything else asynchronously, and we've made attempts to
				// free memory AND correct our limits, yet we still can't allocate.
				// it's likely that we are simply asking for too much
				atomic.AddInt64(&pool.cur, -size)
				log.Errorf("Unable to allocate %d bytes, used %d bytes, limit is %d bytes", size, atomic.LoadInt64(&pool.cur)-size, pool.max)
				return syscall.ENOMEM
			}
		}
	}

	return nil
}
