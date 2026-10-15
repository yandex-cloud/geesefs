package core

import (
	"sync"
	"sync/atomic"
	"testing"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// newHandlesInode returns a modified file inode of the given size, in a
// parent directory, that TryFlush can look at without a backend.
func newHandlesInode(size uint64, usePatch bool) *Inode {
	flags := cfg.DefaultFlags()
	flags.UsePatch = usePatch
	fs := &Goofys{flags: flags}
	fs.flusherCond = sync.NewCond(&fs.flusherMu)
	parent := &Inode{fs: fs, Name: "dir", dir: &DirInodeData{}}
	inode := &Inode{fs: fs, Name: "file", Parent: parent, CacheState: ST_MODIFIED}
	inode.buffers.helpers = inode
	inode.Attributes.Size = size
	return inode
}

// TestFileHandlesConcurrentRead calls code that reads inode.fileHandles
// (with inode.mu held) while another goroutine opens and releases file
// handles. FileHandle.Release changes fileHandles with atomic.AddInt32
// and without inode.mu, so before the fix the plain reads were a data race
// (run with -race).
//
// A base number of handles stays open the whole time, so the count never
// reaches 0 and TryFlush never starts a real flush.
func TestFileHandlesConcurrentRead(t *testing.T) {
	const bigFile = 100 * 1024 * 1024
	tests := []struct {
		description string
		size        uint64
		usePatch    bool
		multipart   bool // an upload is already in progress (inode.mpu is set)
		isFlushing  bool // the inode already has the most parallel flushes
		dump        bool // call DumpThis instead of TryFlush
		baseHandles int  // handles open the whole time
		churn       int  // handles opened and released while reading
		// expectedFlush is what TryFlush returns every time
		expectedFlush   bool
		expectedHandles int32
	}{
		{description: "positive: small file with an open handle is not flushed",
			size: 10, baseHandles: 1, churn: 200, expectedFlush: false, expectedHandles: 1},
		{description: "positive: multipart upload with an open handle is not completed",
			size: bigFile, multipart: true, baseHandles: 1, churn: 200, expectedFlush: false, expectedHandles: 1},
		{description: "positive: patch upload with an open handle is not started",
			size: 10, usePatch: true, baseHandles: 1, churn: 200, expectedFlush: false, expectedHandles: 1},
		{description: "negative: no handles change while reading",
			size: 10, baseHandles: 1, churn: 0, expectedFlush: false, expectedHandles: 1},
		{description: "negative: file that is already flushing is skipped",
			size: 10, isFlushing: true, baseHandles: 1, churn: 200, expectedFlush: false, expectedHandles: 1},
		{description: "boundary: one handle opened and released once",
			size: 10, baseHandles: 1, churn: 1, expectedFlush: false, expectedHandles: 1},
		{description: "boundary: many handles open",
			size: 10, baseHandles: 64, churn: 200, expectedFlush: false, expectedHandles: 64},
		{description: "corner: debug dump while handles change",
			size: 10, dump: true, baseHandles: 1, churn: 200, expectedFlush: false, expectedHandles: 1},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			inode := newHandlesInode(tc.size, tc.usePatch)
			if tc.multipart {
				inode.mpu = &MultipartBlobCommitInput{Parts: make([]*string, 100)}
			}
			if tc.isFlushing {
				inode.IsFlushing = inode.fs.flags.MaxParallelParts
			}
			for range tc.baseHandles {
				if _, err := inode.OpenFile(); err != nil {
					t.Fatal(err)
				}
			}

			start := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				<-start
				for range tc.churn {
					fh, err := inode.OpenFile()
					if err != nil {
						t.Error(err)
						return
					}
					fh.Release()
				}
			}()
			close(start)
			for range 200 {
				if tc.dump {
					inode.DumpThis(false)
				} else if got := inode.TryFlush(0); got != tc.expectedFlush {
					t.Errorf("TryFlush = %v, expected %v", got, tc.expectedFlush)
					break
				}
			}
			wg.Wait()

			if got := atomic.LoadInt32(&inode.fileHandles); got != tc.expectedHandles {
				t.Errorf("fileHandles = %v, expected %v", got, tc.expectedHandles)
			}
		})
	}
}

// BenchmarkTryFlushOpenHandle runs TryFlush on files with an open handle,
// which reads fileHandles and returns without flushing.
func BenchmarkTryFlushOpenHandle(b *testing.B) {
	for _, bc := range []struct {
		name      string
		size      uint64
		usePatch  bool
		multipart bool
	}{
		{"small", 10, false, false},
		{"multipart", 100 * 1024 * 1024, false, true},
		{"patch", 10, true, false},
	} {
		b.Run(bc.name, func(b *testing.B) {
			inode := newHandlesInode(bc.size, bc.usePatch)
			if bc.multipart {
				inode.mpu = &MultipartBlobCommitInput{Parts: make([]*string, 100)}
			}
			if _, err := inode.OpenFile(); err != nil {
				b.Fatal(err)
			}
			b.ReportAllocs()
			for b.Loop() {
				inode.TryFlush(0)
			}
		})
	}
}
