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
	"errors"
	"fmt"

	"github.com/tidwall/btree"
)

var ErrBufferIsMissing = errors.New("tried to read from a missing buffer")
var ErrBufferIsLoading = errors.New("tried to read from a loading buffer")

type BufferState int16

var zeroBuf = make([]byte, 1048576)

const MAX_BUF = 2 * 1024 * 1024

// Yes I know this is against Go style.
// But it's easier to grep and distinguish visually so fuck off. :-)
const (
	// Buffer is clean
	BUF_CLEAN BufferState = 1
	// Buffer is modified locally
	BUF_DIRTY BufferState = 2
	// Buffer is flushed to the server as a full part, but multipart upload is not finalized yet
	BUF_FLUSHED_FULL BufferState = 3
	// Buffer is flushed to the server as an undersized part
	// (and multipart upload is not finalized yet)
	BUF_FLUSHED_CUT BufferState = 4
	// Buffer is flushed to the server and then removed from memory
	// (which is only possible for BUF_FLUSHED_FULL buffers)
	// (and multipart upload is not finalized yet)
	BUF_FL_CLEARED BufferState = 5
)

type BufferListHelpers interface {
	PartNum(uint64) uint64
	QueueCleanBuffer(*FileBuffer)
	UnqueueCleanBuffer(*FileBuffer)
}

type dirtyPart struct {
	partNum uint64
	queueId uint64
	refcnt  int
}

type BufferList struct {
	// current buffers in list
	// end offset => buffer
	at btree.Map[uint64, *FileBuffer]
	// interface to external methods
	helpers BufferListHelpers
	// dirty part map and queue
	dirtyQueue btree.Map[uint64, *dirtyPart]
	dirtyParts map[uint64]*dirtyPart
	dirtyQid   uint64
	// unclean (anything except BUF_CLEAN) buffer count
	uncleanCount int64
	// next dirty index for new buffers
	curDirtyID uint64
}

type FileBuffer struct {
	queueId uint64
	offset  uint64
	length  uint64
	// Chunk state: 1 = clean. 2 = dirty. 3 = part flushed, but not finalized
	// 4 = flushed, not finalized, but removed from memory
	state BufferState
	// Loading from server or from disk
	loading bool
	// Latest chunk data is written to the disk cache
	onDisk bool
	// Chunk only contains zeroes, data and ptr are nil
	zero bool
	// Unmodified chunks (equal to the current server-side object state) have dirtyID = 0.
	// Every write or split assigns a new unique chunk ID.
	// Flusher tracks IDs that are currently being flushed to the server,
	// which allows to do flush and write in parallel.
	dirtyID uint64
	// Data
	data []byte
	ptr  *BufferPointer
}

type Range struct {
	Start, End uint64
}

// Several FileBuffers may be slices of the same array,
// but we want to track memory usage, so we have to refcount them... O_o
type BufferPointer struct {
	mem  []byte
	refs int
}

func (buf *FileBuffer) Append(data []byte) int64 {
	allocated := int64(0)
	oldLen := len(buf.data)
	newLen := oldLen + len(data)
	if cap(buf.data) >= newLen {
		// It fits
		buf.data = buf.data[0:newLen]
		buf.length = uint64(newLen)
		copy(buf.data[oldLen:], data)
	} else {
		// Reallocate
		newCap := newLen
		if newCap < 2*oldLen {
			newCap = 2 * oldLen
		}
		allocated += int64(newCap)
		newData := make([]byte, newCap)
		copy(newData[0:oldLen], buf.data)
		copy(newData[oldLen:newLen], data)
		buf.data = newData[0:newLen]
		buf.length = uint64(newLen)
		// Refcount
		buf.ptr.refs--
		if buf.ptr.refs == 0 {
			allocated -= int64(len(buf.ptr.mem))
		}
		buf.ptr = &BufferPointer{
			mem:  newData,
			refs: 1,
		}
	}
	return allocated
}

// Iterate on B-Tree & revalidate iterators on change
func ascendChange(m *btree.Map[uint64, *FileBuffer], start uint64, iter func(key uint64, value *FileBuffer) (cont bool, changed bool)) {
	var last uint64
	skipLast := false
	var wrappedIter func(key uint64, value *FileBuffer) bool
	wrappedIter = func(key uint64, value *FileBuffer) bool {
		if skipLast && key == last {
			return true
		}
		cont, chg := iter(key, value)
		if chg && cont {
			last = key
			skipLast = true
			m.Ascend(key, wrappedIter)
			return false
		}
		return cont
	}
	m.Ascend(start, wrappedIter)
}

func (l *BufferList) Ascend(offset uint64, iter func(end uint64, b *FileBuffer) (cont bool, changed bool)) {
	ascendChange(&l.at, offset, iter)
}

func (l *BufferList) Count() int {
	return l.at.Len()
}

func (l *BufferList) EvictFromMemory(buf *FileBuffer) (allocated int64, deleted bool) {
	// Release memory
	buf.ptr.refs--
	if buf.ptr.refs == 0 {
		allocated -= int64(len(buf.ptr.mem))
	}
	buf.ptr = nil
	buf.data = nil
	if buf.onDisk {
		// Try to merge it with the previous buffer
		var prev *FileBuffer
		l.at.Descend(buf.offset, func(end uint64, b *FileBuffer) bool {
			prev = b
			return false
		})
		if prev != nil && prev.offset+prev.length == buf.offset &&
			prev.state == buf.state &&
			prev.ptr == nil &&
			prev.onDisk == buf.onDisk {
			l.unqueue(buf)
			l.unqueue(prev)
			l.at.Delete(prev.offset + prev.length)
			buf.length += prev.length
			buf.offset = prev.offset
			l.queue(buf)
			deleted = true
		}
	} else if buf.state == BUF_CLEAN {
		l.unqueue(buf)
		l.at.Delete(buf.offset + buf.length)
		deleted = true
	} else if buf.state == BUF_FLUSHED_FULL {
		// A flushed buffer can be removed at a cost of finalizing multipart upload
		// to read it back later. However it's likely not a problem if we're uploading
		// a large file because we may never need to read it back.
		// One exception is that we don't do it with the header because various
		// software commonly modifies header after writing the whole large file
		l.unqueue(buf)
		buf.state = BUF_FL_CLEARED
		// Try to merge it with the previous buffer
		var prev *FileBuffer
		l.at.Descend(buf.offset, func(end uint64, b *FileBuffer) bool {
			prev = b
			return false
		})
		if prev != nil && prev.offset+prev.length == buf.offset && prev.state == buf.state {
			l.unqueue(prev)
			l.at.Delete(prev.offset + prev.length)
			buf.length += prev.length
			buf.offset = prev.offset
			deleted = true
		}
		l.queue(buf)
	}
	return
}

func (l *BufferList) Get(end uint64) *FileBuffer {
	buf, _ := l.at.Get(end)
	return buf
}

func (l *BufferList) Select(start, end uint64, cb func(buf *FileBuffer) (good bool)) (bufs []*FileBuffer) {
	l.Ascend(start+1, func(bufEnd uint64, buf *FileBuffer) (cont bool, chg bool) {
		if buf.offset >= end {
			return false, false
		}
		if cb(buf) {
			bufs = append(bufs, buf)
		}
		return true, false
	})
	return bufs
}

func (l *BufferList) nextID() uint64 {
	l.curDirtyID++
	return l.curDirtyID
}

func (l *BufferList) unqueue(b *FileBuffer) {
	if b.state != BUF_CLEAN {
		l.uncleanCount--
	}
	if b.state == BUF_DIRTY {
		sp := l.helpers.PartNum(b.offset)
		ep := l.helpers.PartNum(b.offset + b.length - 1)
		for i := sp; i < ep+1; i++ {
			p := l.dirtyParts[i]
			if p == nil || p.refcnt == 0 {
				panic("BUG: dirty buffer count of part < 0")
			}
			p.refcnt--
			if p.refcnt == 0 {
				l.dirtyQueue.Delete(p.queueId)
				delete(l.dirtyParts, i)
			}
		}
	} else if b.state == BUF_CLEAN || b.state == BUF_FLUSHED_FULL {
		l.helpers.UnqueueCleanBuffer(b)
	}
}

func (l *BufferList) referenceDirtyPart(partNum uint64) {
	l.dirtyQid++
	p := l.dirtyParts[partNum]
	if p == nil {
		p = &dirtyPart{partNum: partNum, queueId: l.dirtyQid, refcnt: 1}
		l.dirtyParts[partNum] = p
		l.dirtyQueue.Set(p.queueId, p)
	} else {
		l.dirtyQueue.Delete(p.queueId)
		p.refcnt++
		p.queueId = l.dirtyQid
		l.dirtyQueue.Set(p.queueId, p)
	}
}

func (l *BufferList) queue(b *FileBuffer) {
	if b.length == 0 {
		panic("BUG: buffer length should never be 0")
	}
	if b.state != BUF_CLEAN {
		l.uncleanCount++
	}
	if b.state == BUF_DIRTY {
		if l.dirtyParts == nil {
			l.dirtyParts = make(map[uint64]*dirtyPart)
		}
		sp := l.helpers.PartNum(b.offset)
		ep := l.helpers.PartNum(b.offset + b.length - 1)
		for i := sp; i <= ep; i++ {
			l.referenceDirtyPart(i)
		}
	} else if b.state == BUF_CLEAN || b.state == BUF_FLUSHED_FULL {
		l.helpers.QueueCleanBuffer(b)
	}
}

func (l *BufferList) requeueSplit(left *FileBuffer) {
	if left.length == 0 {
		panic("BUG: buffer length should never be 0")
	}
	if left.state != BUF_CLEAN {
		l.uncleanCount++
	}
	if left.state == BUF_DIRTY {
		if l.dirtyParts == nil {
			l.dirtyParts = make(map[uint64]*dirtyPart)
		}
		// most refcounts don't change - except if splitting not at part boundary
		lbound := l.helpers.PartNum(left.offset + left.length - 1)
		rbound := l.helpers.PartNum(left.offset + left.length)
		if lbound == rbound {
			l.referenceDirtyPart(lbound)
		}
	} else if left.state == BUF_CLEAN || left.state == BUF_FLUSHED_FULL {
		// we only have to add the left buffer, right remains as is
		l.helpers.QueueCleanBuffer(left)
	}
}

func (l *BufferList) IterateDirtyParts(cb func(partNum uint64) bool) {
	l.dirtyQueue.Ascend(0, func(qid uint64, p *dirtyPart) bool {
		return cb(p.partNum)
	})
}

func (l *BufferList) SetState(offset, size uint64, ids map[uint64]bool, state BufferState) {
	l.at.Ascend(offset+1, func(end uint64, b *FileBuffer) bool {
		if b.dirtyID != 0 && ids[b.dirtyID] {
			l.unqueue(b)
			b.dirtyID = 0
			b.state = state
			l.queue(b)
		}
		return b.offset < offset+size-1
	})
}

func (l *BufferList) SetFlushedClean() {
	ascendChange(&l.at, 0, func(end uint64, b *FileBuffer) (cont bool, chg bool) {
		if b.state == BUF_FL_CLEARED {
			l.delete(b)
			return true, true
		} else if b.state == BUF_FLUSHED_FULL || b.state == BUF_FLUSHED_CUT {
			l.unqueue(b)
			b.dirtyID = 0
			b.state = BUF_CLEAN
			l.queue(b)
		}
		return true, false
	})
}

func (l *BufferList) delete(b *FileBuffer) (allocated int64) {
	if b.data != nil {
		b.ptr.refs--
		if b.ptr.refs == 0 {
			allocated -= int64(len(b.ptr.mem))
		}
		b.ptr = nil
		b.data = nil
	}
	l.at.Delete(b.offset + b.length)
	l.unqueue(b)
	return
}

// Remove buffers in range (offset..size)
func (l *BufferList) RemoveRange(removeOffset, removeSize uint64, filter func(b *FileBuffer) bool) (allocated int64) {
	endOffset := removeOffset + removeSize
	ascendChange(&l.at, removeOffset+1, func(end uint64, b *FileBuffer) (cont bool, changed bool) {
		if b.offset >= endOffset {
			return false, false
		}
		bufEnd := b.offset + b.length
		if (filter == nil || filter(b)) && bufEnd > removeOffset && endOffset > b.offset {
			if removeOffset <= b.offset && endOffset >= bufEnd {
				// whole buffer
				allocated += l.delete(b)
				changed = true
			} else {
				rm := b
				bufStart := b.offset
				// split-delete is simpler than cut regarding the dirty part reference count
				if endOffset < bufEnd {
					// remove beginning
					rm, _ = l.split(rm, endOffset)
				}
				if removeOffset > bufStart {
					// remove end
					_, rm = l.split(rm, removeOffset)
				}
				if removeOffset > bufStart || endOffset < bufEnd {
					allocated += l.delete(rm)
					changed = true
				}
			}
		}
		return true, changed
	})
	return
}

func (l *BufferList) insertOrAppend(offset uint64, data []byte, state BufferState, copyData bool, dataPtr *BufferPointer) (allocated int64) {
	if len(data) == 0 {
		return 0
	}
	dirtyID := uint64(0)
	if state == BUF_DIRTY {
		dirtyID = l.nextID()
	}
	end := offset + uint64(len(data))
	var prev *FileBuffer
	l.at.Descend(end, func(end uint64, b *FileBuffer) bool {
		prev = b
		return false
	})
	if prev != nil && prev.offset+prev.length == end {
		panic(fmt.Sprintf(
			"BUG: Tried to insert %x+%x (s%v) but already have %x+%x (s%v)",
			offset, len(data), state, prev.offset, prev.length, prev.state,
		))
	}
	// Check if we can merge it with the previous buffer
	if copyData && prev != nil &&
		prev.offset+prev.length == offset &&
		l.helpers.PartNum(prev.offset) == l.helpers.PartNum(offset) &&
		prev.state == state &&
		prev.ptr != nil && prev.ptr.refs == 1 &&
		(len(prev.data)+len(data) <= cap(prev.data) || cap(prev.data) <= MAX_BUF/2) {
		// We can append to the previous buffer if it doesn't result
		// in overwriting data that may be referenced by other buffers
		// This is profitable because a lot of tools write in small chunks
		l.unqueue(prev)
		l.at.Delete(prev.offset + prev.length)
		allocated += prev.Append(data)
		prev.onDisk = false
		prev.dirtyID = dirtyID
		l.at.Set(prev.offset+prev.length, prev)
		l.queue(prev)
		return
	}
	var newData []byte
	allocated += int64(len(data))
	if copyData {
		newData = make([]byte, len(data))
		copy(newData, data)
		dataPtr = &BufferPointer{
			mem:  newData,
			refs: 0,
		}
	} else {
		newData = data
	}
	dataPtr.refs++
	newBuf := &FileBuffer{
		offset:  offset,
		dirtyID: dirtyID,
		state:   state,
		onDisk:  false,
		zero:    false,
		length:  uint64(len(newData)),
		data:    newData,
		ptr:     dataPtr,
	}
	l.at.Set(end, newBuf)
	l.queue(newBuf)
	return allocated
}

func (l *BufferList) ZeroRange(offset, size uint64) (zeroed bool, allocated int64) {
	if size == 0 {
		return false, 0
	}

	// Check if it's already zeroed
	var existing *FileBuffer
	l.at.Ascend(offset+1, func(end uint64, b *FileBuffer) bool {
		existing = b
		return false
	})
	if existing != nil && existing.zero && existing.offset <= offset && existing.offset+existing.length >= offset+size {
		return false, 0
	}

	// Remove intersecting parts as they're being overwritten
	zeroed = true
	allocated = l.RemoveRange(offset, size, nil)

	// Insert a zero buffer
	buf := &FileBuffer{
		offset:  offset,
		dirtyID: l.nextID(),
		state:   BUF_DIRTY,
		onDisk:  false,
		zero:    true,
		length:  size,
		data:    nil,
		ptr:     nil,
	}
	l.at.Set(offset+size, buf)
	l.queue(buf)

	return
}

func (l *BufferList) Add(offset uint64, data []byte, state BufferState, copyData bool) (allocated int64) {
	dataLen := uint64(len(data))

	// Remove intersecting parts as they're being overwritten
	// If we're inserting a clean buffer, don't remove dirty ones
	allocated = l.RemoveRange(offset, dataLen, func(b *FileBuffer) bool { return b.state == BUF_CLEAN || state != BUF_CLEAN })

	// Insert non-overlapping parts of the buffer
	dataPtr := &BufferPointer{
		mem:  data,
		refs: 0,
	}
	l.fill(offset, dataLen, func(curOffset, curEnd uint64) {
		allocated += l.insertOrAppend(curOffset, data[curOffset-offset:curEnd-offset], state, copyData, dataPtr)
	})

	return
}

func (l *BufferList) fill(offset, size uint64, cb func(start, end uint64)) {
	curOffset := offset
	endOffset := offset + size
	for curOffset < endOffset {
		var next *FileBuffer
		l.at.Ascend(curOffset+1, func(end uint64, b *FileBuffer) bool {
			next = b
			if next.offset <= curOffset {
				curOffset = next.offset + next.length
				next = nil
				return curOffset < endOffset
			}
			return false
		})
		if curOffset < endOffset {
			newEnd := endOffset
			if next != nil && next.offset < newEnd {
				newEnd = next.offset
			}
			cb(curOffset, newEnd)
			curOffset = newEnd
		}
	}
}

func (l *BufferList) AddLoading(offset, size uint64) {
	l.fill(offset, size, func(curOffset, curEnd uint64) {
		b := &FileBuffer{
			offset:  curOffset,
			length:  curEnd - curOffset,
			dirtyID: 0,
			state:   BUF_CLEAN,
			loading: true,
			onDisk:  false,
			zero:    false,
		}
		l.at.Set(curEnd, b)
		l.queue(b)
	})
}

func (l *BufferList) AddLoadingFromDisk(offset, size uint64) (readRanges []Range) {
	endOffset := offset + size
	ascendChange(&l.at, offset+1, func(end uint64, b *FileBuffer) (cont bool, changed bool) {
		if b.offset >= endOffset {
			return
		}
		if b.data == nil && b.onDisk && !b.loading {
			if b.offset < offset {
				_, b = l.split(b, offset)
				changed = true
			}
			if b.offset+b.length > endOffset {
				b, _ = l.split(b, endOffset)
				changed = true
			}
			b.loading = true
			readRanges = append(readRanges, Range{b.offset, b.offset + b.length})
		}
		cont = true
		return
	})
	return
}

func (l *BufferList) ReviveFromDisk(offset uint64, data []byte) {
	l.at.Ascend(offset+1, func(end uint64, b *FileBuffer) bool {
		if b.offset == offset && b.length == uint64(len(data)) && b.loading && b.onDisk {
			b.data = data
			b.ptr = &BufferPointer{
				mem:  data,
				refs: 1,
			}
			b.loading = false
			if b.state == BUF_FL_CLEARED {
				l.unqueue(b)
				b.state = BUF_FLUSHED_FULL
				l.queue(b)
			}
		}
		return false
	})
}

func (l *BufferList) RemoveLoading(offset, size uint64) {
	l.RemoveRange(offset, size, func(b *FileBuffer) bool { return !b.onDisk && b.loading })
}

func (l *BufferList) split(b *FileBuffer, offset uint64) (left, right *FileBuffer) {
	startBuf := *b
	startBuf.length = offset - b.offset
	if startBuf.data != nil {
		startBuf.data = startBuf.data[0:startBuf.length]
		b.data = b.data[offset-b.offset:]
		startBuf.ptr.refs++
	}
	if b.dirtyID != 0 {
		b.dirtyID = l.nextID()
	}
	b.length = b.offset + b.length - offset
	b.offset = offset
	l.at.Set(offset, &startBuf)
	l.requeueSplit(&startBuf)
	return &startBuf, b
}

// Left here for the ease of debugging
func (l *BufferList) Dump(offset, size uint64) string {
	r := ""
	l.at.Ascend(offset+1, func(end uint64, b *FileBuffer) bool {
		if b.offset >= offset+size {
			return false
		}
		l := 0
		if b.loading {
			l = 1
		}
		z := 0
		if b.zero {
			z = 1
		}
		r += fmt.Sprintf("%x-%x s%v l%v z%v d%v\n", b.offset, b.offset+b.length, b.state, l, z, b.dirtyID)
		return true
	})
	return r
}

func (l *BufferList) DebugCheckHoles(s string) {
	var eof uint64
	l.at.Descend(0xFFFFFFFFFFFFFFFF, func(end uint64, b *FileBuffer) bool {
		eof = end
		return false
	})
	h, _, _ := l.GetHoles(0, eof)
	if len(h) > 0 {
		fmt.Printf("Debug: holes detected%s: %#v\n%v", s, h, l.Dump(0, 0xFFFFFFFFFFFFFFFF))
		panic("holes detected")
	}
}

func (l *BufferList) SplitAt(offset uint64) {
	l.at.Ascend(offset+1, func(end uint64, b *FileBuffer) bool {
		if b.offset < offset {
			l.split(b, offset)
		}
		return false
	})
}

func (l *BufferList) AnyUnclean() bool {
	return l.uncleanCount > 0
}

func (l *BufferList) AnyFlushed(offset, size uint64) (flushed bool) {
	l.at.Ascend(offset+1, func(end uint64, b *FileBuffer) bool {
		if b.offset >= offset+size {
			return false
		}
		if b.state == BUF_FLUSHED_FULL || b.state == BUF_FLUSHED_CUT || b.state == BUF_FL_CLEARED {
			flushed = true
			return false
		}
		return true
	})
	return
}

func appendZero(data [][]byte, zeroLen uint64) [][]byte {
	for zeroLen > uint64(len(zeroBuf)) {
		data = append(data, zeroBuf)
		zeroLen -= uint64(len(zeroBuf))
	}
	if zeroLen > 0 {
		data = append(data, zeroBuf[0:zeroLen])
	}
	return data
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func (l *BufferList) GetHoles(offset, size uint64) (holes []Range, loading bool, flushCleared bool) {
	curOffset := offset
	endOffset := offset + size
	l.at.Ascend(offset+1, func(end uint64, b *FileBuffer) bool {
		if b.offset >= endOffset {
			return false
		}
		if b.offset > curOffset {
			curEnd := min(endOffset, b.offset)
			holes = append(holes, Range{curOffset, curEnd})
			curOffset = curEnd
		}
		curOffset = b.offset + b.length
		loading = loading || b.loading
		flushCleared = flushCleared || b.state == BUF_FL_CLEARED
		return true
	})
	if curOffset < endOffset {
		holes = append(holes, Range{curOffset, endOffset})
	}
	return
}

func (l *BufferList) GetData(offset, size uint64, returnIds bool) (data [][]byte, ids map[uint64]bool, err error) {
	if returnIds {
		ids = make(map[uint64]bool)
	}
	curOffset := offset
	endOffset := offset + size
	l.at.Ascend(curOffset+1, func(end uint64, b *FileBuffer) bool {
		if b.offset > curOffset {
			// hole
			data = nil
			err = ErrBufferIsMissing
			return false
		}
		if b.offset >= endOffset {
			return false
		}
		curEnd := min(endOffset, b.offset+b.length)
		if returnIds && b.dirtyID != 0 {
			ids[b.dirtyID] = true
		}
		if b.loading {
			// tried to read a loading buffer
			data = nil
			err = ErrBufferIsLoading
			return false
		} else if b.zero {
			data = appendZero(data, curEnd-curOffset)
		} else {
			data = append(data, b.data[curOffset-b.offset:curEnd-b.offset])
		}
		curOffset = curEnd
		return curOffset < endOffset
	})
	if err == nil && curOffset < endOffset {
		data = nil
		err = ErrBufferIsMissing
		return
	}
	return
}

func mergeRA(rr []Range, readAhead uint64, readMerge uint64) []Range {
	if readMerge >= readAhead {
		readMerge -= readAhead
	} else {
		readMerge = 0
	}
	prev := -1
	for i := 0; i < len(rr); i++ {
		if prev >= 0 && rr[prev].End+readMerge >= rr[i].Start {
			rr[prev].End = rr[i].End
		} else {
			prev++
			sz := rr[i].End - rr[i].Start
			if sz < readAhead {
				sz = readAhead
			}
			rr[prev] = Range{Start: rr[i].Start, End: rr[i].Start + sz}
		}
	}
	return rr[0 : prev+1]
}

func splitRA(rr []Range, maxPart uint64) []Range {
	res := rr
	split := false
	for i := 0; i < len(rr); i++ {
		if rr[i].End-rr[i].Start > maxPart {
			if !split {
				res = append([]Range(nil), rr[0:i]...)
				split = true
			}
			for off := rr[i].Start; off < rr[i].End; off += maxPart {
				res = append(res, Range{Start: off, End: off + maxPart})
			}
			res[len(res)-1].End = rr[i].End
		} else if split {
			res = append(res, rr[i])
		}
	}
	return res
}
