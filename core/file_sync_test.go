// Copyright 2026 Yandex LLC
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
	"runtime"
	"sync"
	"testing"
	"time"
)

func TestSyncFileDoesNotMissFlushCompletion(t *testing.T) {
	oldMaxProcs := runtime.GOMAXPROCS(1)
	defer runtime.GOMAXPROCS(oldMaxProcs)

	fs := &Goofys{}
	fs.flusherCond = sync.NewCond(&fs.flusherMu)
	// A deleted inode with an active flush makes TryFlush a controlled no-op.
	inode := &Inode{
		fs:         fs,
		CacheState: ST_DELETED,
		IsFlushing: 1,
	}

	fs.flusherMu.Lock()
	syncDone := make(chan error, 1)
	go func() {
		syncDone <- inode.SyncFile()
	}()
	runtime.Gosched()

	// Keep SyncFile from registering its wait until flush completion changes the inode.
	stateChanged := make(chan struct{})
	allowWakeup := make(chan struct{})
	go func() {
		inode.mu.Lock()
		inode.CacheState = ST_CACHED
		inode.mu.Unlock()
		close(stateChanged)
		<-allowWakeup
		fs.WakeupFlusher()
	}()
	runtime.Gosched()

	stateChangedBeforeWait := false
	select {
	case <-stateChanged:
		stateChangedBeforeWait = true
	default:
	}
	// Model the flusher consuming the completion wakeup before SyncFile can observe it.
	fs.flusherCond.Broadcast()
	fs.flusherMu.Unlock()
	if !stateChangedBeforeWait {
		close(allowWakeup)
	}

	select {
	case err := <-syncDone:
		if stateChangedBeforeWait {
			close(allowWakeup)
		}
		if err != nil {
			t.Fatalf("SyncFile returned an error: %v", err)
		}
	case <-time.After(time.Second):
		if stateChangedBeforeWait {
			close(allowWakeup)
		}
		select {
		case <-syncDone:
		case <-time.After(time.Second):
		}
		t.Fatal("SyncFile missed the completed flush and kept waiting")
	}
}
