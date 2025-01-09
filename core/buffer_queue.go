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
	"sync"

	"github.com/tidwall/btree"
)

type QueuedBuffer struct {
	inode *Inode
	end   uint64
}

type BufferQueue struct {
	mu sync.Mutex
	// clean/flushed buffer queue - buffers eligible for eviction from memory
	// queue index (arbitrary increasing value) => buffer
	cleanQueue btree.Map[uint64, QueuedBuffer]
	// next queue index for new buffers
	curQueueID uint64
}

func (l *BufferQueue) Add(inode *Inode, b *FileBuffer) {
	l.mu.Lock()
	l.curQueueID++
	b.queueId = l.curQueueID
	l.cleanQueue.Set(b.queueId, QueuedBuffer{inode, b.offset + b.length})
	l.mu.Unlock()
}

func (l *BufferQueue) Delete(b *FileBuffer) {
	// We don't evict used buffers
	// We don't evict the first part
	// However, this filtering is also done outside BufferQueue :)
	l.mu.Lock()
	l.cleanQueue.Delete(b.queueId)
	l.mu.Unlock()
}

func (l *BufferQueue) NextClean(minQueueId uint64) (inode *Inode, end, nextQueueId uint64) {
	l.mu.Lock()
	l.cleanQueue.Ascend(minQueueId, func(queueId uint64, b QueuedBuffer) bool {
		inode = b.inode
		end = b.end
		nextQueueId = queueId + 1
		return false
	})
	l.mu.Unlock()
	return
}

type InodeQueue struct {
	mu sync.Mutex
	// dirty inode queue
	dirtyQueue btree.Map[uint64, uint64]
	// next queue index for new buffers
	curQueueID uint64
}

func (l *InodeQueue) Add(inodeID uint64) (queueID uint64) {
	l.mu.Lock()
	l.curQueueID++
	queueID = l.curQueueID
	l.dirtyQueue.Set(queueID, inodeID)
	l.mu.Unlock()
	return
}

func (l *InodeQueue) Delete(queueID uint64) {
	l.mu.Lock()
	l.dirtyQueue.Delete(queueID)
	l.mu.Unlock()
}

func (l *InodeQueue) Size() int {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.dirtyQueue.Len()
}

func (l *InodeQueue) Next(minQueueID uint64) (inodeID, nextQueueID uint64) {
	l.mu.Lock()
	l.dirtyQueue.Ascend(minQueueID, func(queueID uint64, ino uint64) bool {
		inodeID = ino
		nextQueueID = queueID + 1
		return false
	})
	l.mu.Unlock()
	return
}
