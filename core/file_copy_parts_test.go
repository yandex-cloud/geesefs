package core

import (
	"fmt"
	"sync/atomic"
	"syscall"
	"testing"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// newCopyPartsInode returns a file inode with an open multipart upload of
// numParts parts of 5 MiB. Parts listed in modified already have a part ID,
// so copyUnmodifiedParts must not copy them.
func newCopyPartsInode(copyFunc func(*Inode, *MultipartBlobCopyInput) (*MultipartBlobCopyOutput, error),
	numParts uint64, modified []uint64, maxParallel int) *Inode {
	flags := cfg.DefaultFlags()
	flags.MaxParallelCopy = maxParallel
	fs := &Goofys{flags: flags}
	inode := &Inode{fs: fs, Name: "file"}
	backend := &TestBackend{
		MultipartBlobCopyFunc: func(param *MultipartBlobCopyInput) (*MultipartBlobCopyOutput, error) {
			return copyFunc(inode, param)
		},
	}
	inode.Parent = &Inode{fs: fs, dir: &DirInodeData{cloud: backend}}
	inode.Attributes.Size = numParts * 5 * 1024 * 1024
	inode.mpu = &MultipartBlobCommitInput{
		Key:      PString("file"),
		UploadId: PString("upload"),
		Parts:    make([]*string, numParts),
	}
	for _, p := range modified {
		inode.mpu.Parts[p] = PString(fmt.Sprintf("modified-%d", p))
	}
	return inode
}

// TestCopyUnmodifiedPartsConcurrentErrors copies the unmodified parts of a
// multipart upload. Each range is copied in its own goroutine. Run it with
// -race: before the fix, the goroutines wrote the named result err without
// a lock, while the loop that starts them read it.
func TestCopyUnmodifiedPartsConcurrentErrors(t *testing.T) {
	tests := []struct {
		description string
		numParts    uint64
		modified    []uint64
		fail        map[uint64]bool // part numbers (0-based) whose copy fails
		cancelAt    int             // cancel the upload during copy number cancelAt (1-based); 0 = never
		maxParallel int
		expected    error
		// expectedCopies is the number of copy requests; -1 means any number
		expectedCopies int
	}{
		{description: "positive: all unmodified parts are copied",
			numParts: 8, maxParallel: 16, expected: nil, expectedCopies: 8},
		{description: "positive: modified parts are not copied",
			numParts: 8, modified: []uint64{1, 3, 6}, maxParallel: 16, expected: nil, expectedCopies: 5},
		{description: "negative: one copy fails",
			numParts: 8, fail: map[uint64]bool{5: true}, maxParallel: 16, expected: syscall.EACCES, expectedCopies: -1},
		{description: "negative: every copy fails",
			numParts: 8, fail: map[uint64]bool{0: true, 1: true, 2: true, 3: true, 4: true, 5: true, 6: true, 7: true},
			maxParallel: 16, expected: syscall.EACCES, expectedCopies: -1},
		{description: "boundary: no unmodified parts, nothing to copy",
			numParts: 3, modified: []uint64{0, 1, 2}, maxParallel: 16, expected: nil, expectedCopies: 0},
		{description: "boundary: one part",
			numParts: 1, maxParallel: 16, expected: nil, expectedCopies: 1},
		{description: "boundary: one copy at a time stops after the first failure",
			numParts: 8, fail: map[uint64]bool{0: true}, maxParallel: 1, expected: syscall.EACCES, expectedCopies: 1},
		{description: "corner: two copies fail at the same time",
			numParts: 2, fail: map[uint64]bool{0: true, 1: true}, maxParallel: 16, expected: syscall.EACCES, expectedCopies: -1},
		{description: "corner: upload canceled during the first copy",
			numParts: 8, cancelAt: 1, maxParallel: 1, expected: syscall.ENOENT, expectedCopies: 1},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			var copies int32
			copyFunc := func(inode *Inode, param *MultipartBlobCopyInput) (*MultipartBlobCopyOutput, error) {
				n := atomic.AddInt32(&copies, 1)
				if int(n) == tc.cancelAt {
					inode.mu.Lock()
					inode.mpu = nil
					inode.mu.Unlock()
				}
				if tc.fail[uint64(param.PartNumber-1)] {
					return nil, syscall.EACCES
				}
				return &MultipartBlobCopyOutput{PartId: PString(fmt.Sprintf("copied-%d", param.PartNumber))}, nil
			}
			inode := newCopyPartsInode(copyFunc, tc.numParts, tc.modified, tc.maxParallel)
			mpu := inode.mpu

			inode.mu.Lock()
			err := inode.copyUnmodifiedParts(tc.numParts)
			inode.mu.Unlock()

			if err != tc.expected {
				t.Fatalf("copyUnmodifiedParts() error = %v, want %v", err, tc.expected)
			}
			if got := atomic.LoadInt32(&copies); tc.expectedCopies >= 0 && int(got) != tc.expectedCopies {
				t.Fatalf("copy requests = %d, want %d", got, tc.expectedCopies)
			}
			if tc.expected != nil {
				return
			}
			for i, p := range mpu.Parts {
				if p == nil {
					t.Fatalf("part %d has no part ID after a successful copy", i)
				}
			}
		})
	}
}

func BenchmarkCopyUnmodifiedParts(b *testing.B) {
	copyFunc := func(inode *Inode, param *MultipartBlobCopyInput) (*MultipartBlobCopyOutput, error) {
		return &MultipartBlobCopyOutput{PartId: PString("copied")}, nil
	}
	for _, numParts := range []uint64{1, 16, 128} {
		b.Run(fmt.Sprintf("%dparts", numParts), func(b *testing.B) {
			inode := newCopyPartsInode(copyFunc, numParts, nil, 16)
			b.ReportAllocs()
			for b.Loop() {
				// Reset the part IDs so every part is copied again. This is
				// cheaper than StopTimer/StartTimer, which stop the world
				// to read memory stats.
				clear(inode.mpu.Parts)
				inode.mu.Lock()
				inode.copyUnmodifiedParts(numParts)
				inode.mu.Unlock()
			}
		})
	}
}
