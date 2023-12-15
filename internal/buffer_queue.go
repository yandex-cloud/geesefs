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
	"sync"

	"github.com/tidwall/btree"
)

type QueuedBuffer struct {
	inode *Inode
	end uint64
}

type BufferQueue struct {
	mu sync.Mutex
	// dirty buffer queue - buffers eligible for flushing
	// queue index (arbitrary increasing value) => buffer
	dirtyQueue btree.Map[uint64, QueuedBuffer]
	// clean/flushed buffer queue - buffers eligible for eviction from memory
	// queue index (arbitrary increasing value) => buffer
	cleanQueue btree.Map[uint64, QueuedBuffer]
	// next queue index for new buffers
	curQueueID uint64
}

func (l *BufferQueue) Unqueue(b *FileBuffer) {
	l.mu.Lock()
	if b.state == BUF_DIRTY {
		// We don't flush zero buffers until file is closed
		// We don't flush the currently modified part (last part written to)
		// However this filtering is done outside BufferQueue
		l.dirtyQueue.Delete(b.queueId)
	} else if b.state == BUF_CLEAN || b.state == BUF_FLUSHED_FULL {
		// We don't evict used buffers
		// We don't evict the first part
		// However, this filtering is also done outside BufferQueue :)
		l.cleanQueue.Delete(b.queueId)
	}
	l.mu.Unlock()
}

func (l *BufferQueue) Queue(inode *Inode, b *FileBuffer) {
	l.mu.Lock()
	l.curQueueID++
	b.queueId = l.curQueueID
	if b.state == BUF_DIRTY {
		l.dirtyQueue.Set(b.queueId, QueuedBuffer{inode, b.offset+b.length})
	} else if b.state == BUF_CLEAN || b.state == BUF_FLUSHED_FULL {
		l.cleanQueue.Set(b.queueId, QueuedBuffer{inode, b.offset+b.length})
	}
	l.mu.Unlock()
}

func (l *BufferQueue) DirtyCount() int {
	return l.dirtyQueue.Len()
}

func (l *BufferQueue) NextDirty(minQueueId uint64) (inode *Inode, end, nextQueueId uint64) {
	l.mu.Lock()
	l.dirtyQueue.Ascend(minQueueId, func(queueId uint64, b QueuedBuffer) bool {
		inode = b.inode
		end = b.end
		nextQueueId = queueId+1
		return false
	})
	l.mu.Unlock()
	return
}

func (l *BufferQueue) NextClean(minQueueId uint64) (inode *Inode, end, nextQueueId uint64) {
	l.mu.Lock()
	l.cleanQueue.Ascend(minQueueId, func(queueId uint64, b QueuedBuffer) bool {
		inode = b.inode
		end = b.end
		nextQueueId = queueId+1
		return false
	})
	l.mu.Unlock()
	return
}
