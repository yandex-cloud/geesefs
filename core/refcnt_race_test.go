package core

import (
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/jacobsa/fuse/fuseops"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// deleteBackend is a TestBackend that answers DeleteBlob with err.
type deleteBackend struct {
	*TestBackend
	err error
}

func (b *deleteBackend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	if b.err != nil {
		return nil, b.err
	}
	return &DeleteBlobOutput{}, nil
}

// newDeletedInode returns a deleted file inode with refcnt references, in a
// parent directory whose backend answers DeleteBlob with deleteErr.
func newDeletedInode(deleteErr error, refcnt int64) *Inode {
	flags := cfg.DefaultFlags()
	// Keep ScheduleRetryFlush from waking the flusher during the test
	flags.RetryInterval = time.Hour
	fs := &Goofys{flags: flags}
	fs.flusherCond = sync.NewCond(&fs.flusherMu)
	fs.bufferPool = &BufferPool{}
	fs.inodes = make(map[fuseops.InodeID]*Inode)
	fs.inodesByTime = make(map[int64]map[fuseops.InodeID]bool)
	fs.inflightChanges = make(map[string]int)
	parent := &Inode{fs: fs, Name: "dir", dir: &DirInodeData{
		cloud:           &deleteBackend{TestBackend: &TestBackend{}, err: deleteErr},
		DeletedChildren: map[string]*Inode{},
	}}
	inode := &Inode{fs: fs, Id: 2, Name: "file", Parent: parent, refcnt: refcnt}
	inode.buffers.helpers = inode
	inode.SetCacheState(ST_DELETED)
	parent.dir.DeletedChildren[inode.Name] = inode
	fs.inodes[inode.Id] = inode
	return inode
}

// waitWakeup waits until fs.WakeupFlusher is called. SendDelete calls it
// as the last step of its goroutine.
func waitWakeup(t testing.TB, fs *Goofys) {
	deadline := time.Now().Add(10 * time.Second)
	for {
		fs.flusherMu.Lock()
		pending := fs.flushPending
		fs.flusherMu.Unlock()
		if pending != 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("SendDelete did not finish")
		}
		runtime.Gosched()
	}
}

// TestRefcntConcurrentRead runs code that reads inode.refcnt (with inode.mu
// held) while another goroutine adds references with Ref(), the way
// LookUpInode does it: atomically and without inode.mu. Before the fix the
// plain reads were a data race (run with -race).
func TestRefcntConcurrentRead(t *testing.T) {
	tests := []struct {
		description string
		deleteErr   error
		refcnt      int64 // references before the delete
		lookups     int   // Ref() calls during the delete
		dump        bool  // call DumpThis instead of SendDelete
		// expected state after the delete goroutine is done
		expectedState     int32
		expectedRefcnt    int64
		expectedForgotten bool
	}{
		{description: "positive: deleted file with references is kept",
			refcnt: 1, lookups: 100,
			expectedState: ST_DEAD, expectedRefcnt: 101, expectedForgotten: false},
		// Forgetting the inode resets it (resetCache), so it ends as ST_CACHED
		{description: "positive: deleted file with no references is forgotten",
			refcnt: 0, lookups: 0,
			expectedState: ST_CACHED, expectedRefcnt: 0, expectedForgotten: true},
		{description: "negative: delete fails, the file is not marked dead",
			deleteErr: syscall.EACCES, refcnt: 1, lookups: 100,
			expectedState: ST_DELETED, expectedRefcnt: 101, expectedForgotten: false},
		{description: "boundary: one reference, no lookups",
			refcnt: 1, lookups: 0,
			expectedState: ST_DEAD, expectedRefcnt: 1, expectedForgotten: false},
		{description: "boundary: one reference, one lookup",
			refcnt: 1, lookups: 1,
			expectedState: ST_DEAD, expectedRefcnt: 2, expectedForgotten: false},
		{description: "corner: object already gone, a 404, counts as deleted",
			deleteErr: syscall.ENOENT, refcnt: 1, lookups: 100,
			expectedState: ST_DEAD, expectedRefcnt: 101, expectedForgotten: false},
		{description: "corner: debug dump while lookups add references",
			dump: true, refcnt: 1, lookups: 100,
			expectedState: ST_DELETED, expectedRefcnt: 101, expectedForgotten: false},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			inode := newDeletedInode(tc.deleteErr, tc.refcnt)
			fs := inode.fs

			var lookups sync.WaitGroup
			lookups.Add(1)
			inode.mu.Lock()
			if !tc.dump {
				inode.SendDelete()
			}
			go func() {
				defer lookups.Done()
				for range tc.lookups {
					inode.Ref()
				}
			}()
			inode.mu.Unlock()
			if tc.dump {
				for range 100 {
					inode.DumpThis(false)
				}
			} else {
				waitWakeup(t, fs)
			}
			lookups.Wait()

			inode.mu.Lock()
			state := inode.CacheState
			inode.mu.Unlock()
			fs.mu.RLock()
			_, found := fs.inodes[inode.Id]
			fs.mu.RUnlock()
			if state != tc.expectedState {
				t.Errorf("CacheState = %v, expected %v", state, tc.expectedState)
			}
			if got := atomic.LoadInt64(&inode.refcnt); got != tc.expectedRefcnt {
				t.Errorf("refcnt = %v, expected %v", got, tc.expectedRefcnt)
			}
			if !found != tc.expectedForgotten {
				t.Errorf("forgotten = %v, expected %v", !found, tc.expectedForgotten)
			}
		})
	}
}

// BenchmarkSendDelete deletes a file that still has a reference, so
// SendDelete reads refcnt and keeps the inode.
func BenchmarkSendDelete(b *testing.B) {
	inode := newDeletedInode(nil, 1)
	fs := inode.fs
	b.ReportAllocs()
	for b.Loop() {
		fs.flusherMu.Lock()
		fs.flushPending = 0
		fs.flusherMu.Unlock()
		inode.mu.Lock()
		inode.SetCacheState(ST_DELETED)
		inode.SendDelete()
		inode.mu.Unlock()
		waitWakeup(b, fs)
	}
}
