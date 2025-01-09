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

type FDQueue struct {
	mu         sync.Mutex
	cond       *sync.Cond
	q          btree.Map[uint64, *Inode]
	maxCount   int
	curQueueID uint64
}

func NewFDQueue(maxCount int) *FDQueue {
	q := &FDQueue{maxCount: maxCount}
	q.cond = sync.NewCond(&q.mu)
	return q
}

func (l *FDQueue) InsertFD(inode *Inode) {
	l.mu.Lock()
	l.curQueueID++
	inode.DiskFDQueueID = l.curQueueID
	l.q.Set(inode.DiskFDQueueID, inode)
	if l.maxCount > 0 && l.q.Len() > l.maxCount {
		// Wakeup the coroutine that closes old FDs
		// This way we don't guarantee that we never exceed MaxDiskCacheFD
		// But we assume it's not a problem if we leave some safety margin for the limit
		// The same thing happens with our memory usage anyway :D
		l.cond.Broadcast()
	}
	l.mu.Unlock()
}

func (l *FDQueue) UseFD(inode *Inode) {
	l.mu.Lock()
	if inode.DiskFDQueueID > 0 {
		l.q.Delete(inode.DiskFDQueueID)
	}
	l.curQueueID++
	inode.DiskFDQueueID = l.curQueueID
	l.q.Set(inode.DiskFDQueueID, inode)
	l.mu.Unlock()
}

func (l *FDQueue) DeleteFD(inode *Inode) {
	l.mu.Lock()
	l.q.Delete(inode.DiskFDQueueID)
	inode.DiskFDQueueID = 0
	l.mu.Unlock()
}

func (l *FDQueue) NextFD(minQueueID uint64) (inode *Inode, nextQueueId uint64) {
	return
}

// Close unneeded cache FDs
func (l *FDQueue) CloseExtra() {
	l.mu.Lock()
	l.cond.Wait()
	var nextQueueId uint64
	var inode *Inode
	for l.maxCount > 0 && l.q.Len() > l.maxCount {
		l.q.Ascend(nextQueueId, func(queueId uint64, i *Inode) bool {
			inode = i
			nextQueueId = queueId + 1
			return false
		})
		if inode == nil {
			break
		}
		l.mu.Unlock()
		inode.mu.Lock()
		if inode.DiskCacheFD != nil {
			inode.DiskCacheFD.Close()
			inode.DiskCacheFD = nil
			l.DeleteFD(inode)
			inode.mu.Unlock()
			l.mu.Lock()
		} else {
			inode.mu.Unlock()
			l.mu.Lock()
		}
	}
	l.mu.Unlock()
}
