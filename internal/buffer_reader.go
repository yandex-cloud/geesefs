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
	"io"
)

type BufferOrZero struct {
	data []byte
	zero bool
	size uint64
}

type MultiReader struct {
	buffers []BufferOrZero
	idx     int
	pos     uint64
	bufPos  uint64
	size    uint64
}

func NewMultiReader() *MultiReader {
	return &MultiReader{}
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
			copy(buf[outPos:outPos+l], r.buffers[r.idx].data[r.bufPos:r.bufPos+l])
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
			r.bufPos = uOffset - r.pos
			r.pos = uOffset
		}
	}
	return int64(r.pos), nil
}

func (r *MultiReader) Len() uint64 {
	return r.size
}
