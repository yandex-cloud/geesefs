package core

import (
	"fmt"
	"runtime"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// beginBackend is a TestBackend that answers MultipartBlobBegin with begin.
type beginBackend struct {
	*TestBackend
	begin func(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error)
}

func (b *beginBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	return b.begin(param)
}

// newBeginInode returns a file inode whose parent directory uses a
// beginBackend that calls begin.
func newBeginInode(begin func(*Inode, *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error)) *Inode {
	fs := &Goofys{flags: cfg.DefaultFlags()}
	fs.flusherCond = sync.NewCond(&fs.flusherMu)
	inode := &Inode{fs: fs, Name: "file", CacheState: ST_MODIFIED}
	backend := &beginBackend{
		TestBackend: &TestBackend{},
		begin: func(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
			return begin(inode, param)
		},
	}
	inode.Parent = &Inode{fs: fs, dir: &DirInodeData{cloud: backend}}
	return inode
}

// waitFlushers waits until no flush goroutine of fs is running. The flush
// goroutines lower activeFlushers just before they unlock inode.mu, so the
// caller should lock inode.mu after this returns.
func waitFlushers(t testing.TB, fs *Goofys) {
	deadline := time.Now().Add(10 * time.Second)
	for atomic.LoadInt64(&fs.activeFlushers) != 0 {
		if time.Now().After(deadline) {
			t.Fatal("flush goroutine did not finish")
		}
		runtime.Gosched()
	}
}

// TestBeginMultipartUploadMetadata starts a multipart upload the way
// TryFlush does: with inode.mu held, and unlocked right after. Before the
// fix, the flush goroutine read userMetadata and wrote userMetadataDirty
// after that unlock, without the lock, so a SetXattr at the same time was
// a data race (run with -race), and its change could be lost.
func TestBeginMultipartUploadMetadata(t *testing.T) {
	const (
		changeNone        = iota
		changeAfterUnlock // SetXattr from another goroutine right after TryFlush unlocks
		changeInFlight    // SetXattr while MultipartBlobBegin is running
	)
	beginErr := syscall.EACCES
	tests := []struct {
		description  string
		metadata     map[string][]byte
		dirty        int
		change       int
		minMultipart bool // start through uploadMinMultipart instead of sendStartMultipart
		beginFails   bool
		// expectedSent is the metadata sent with MultipartBlobBegin; nil means none
		expectedSent  map[string]string
		expectedDirty int
		expectedMpu   bool
		expectedErr   error
	}{
		{description: "positive: changed metadata is sent and marked as sent",
			metadata: map[string][]byte{"k": []byte("old")}, dirty: 2,
			expectedSent: map[string]string{"k": "old"}, expectedDirty: 1, expectedMpu: true},
		{description: "positive: unchanged metadata is not sent",
			metadata: map[string][]byte{"k": []byte("old")}, dirty: 0,
			expectedSent: nil, expectedDirty: 0, expectedMpu: true},
		{description: "negative: begin fails, the error is recorded",
			metadata: map[string][]byte{"k": []byte("old")}, dirty: 2, beginFails: true,
			expectedSent: map[string]string{"k": "old"}, expectedDirty: 1, expectedMpu: false, expectedErr: beginErr},
		{description: "boundary: empty changed metadata is sent as an empty map",
			metadata: map[string][]byte{}, dirty: 2,
			expectedSent: map[string]string{}, expectedDirty: 1, expectedMpu: true},
		{description: "boundary: nil metadata sends nothing",
			metadata: nil, dirty: 2,
			expectedSent: nil, expectedDirty: 1, expectedMpu: true},
		{description: "corner: SetXattr right after the flush starts is kept as a pending change",
			metadata: map[string][]byte{"k": []byte("old")}, dirty: 2, change: changeAfterUnlock,
			expectedSent: map[string]string{"k": "old"}, expectedDirty: 2, expectedMpu: true},
		{description: "corner: SetXattr while begin is running is kept as a pending change",
			metadata: map[string][]byte{"k": []byte("old")}, dirty: 2, change: changeInFlight,
			expectedSent: map[string]string{"k": "old"}, expectedDirty: 2, expectedMpu: true},
		{description: "corner: uploadMinMultipart, SetXattr right after the flush starts",
			metadata: map[string][]byte{"k": []byte("old")}, dirty: 2, change: changeAfterUnlock,
			minMultipart: true, beginFails: true,
			expectedSent: map[string]string{"k": "old"}, expectedDirty: 2, expectedMpu: false, expectedErr: beginErr},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			var sent *MultipartBlobBeginInput
			inode := newBeginInode(func(inode *Inode, param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
				sent = param
				if tc.change == changeInFlight {
					if err := inode.SetXattr("user.k", []byte("new"), 0); err != nil {
						t.Errorf("SetXattr: %v", err)
					}
				}
				if tc.beginFails {
					return nil, beginErr
				}
				return &MultipartBlobCommitInput{Key: &param.Key, UploadId: PString("upload")}, nil
			})
			inode.userMetadata = tc.metadata
			inode.userMetadataDirty = tc.dirty

			var changed sync.WaitGroup
			inode.mu.Lock()
			if tc.minMultipart {
				inode.uploadMinMultipart()
			} else {
				inode.sendStartMultipart()
			}
			if tc.change == changeAfterUnlock {
				changed.Add(1)
				go func() {
					defer changed.Done()
					if err := inode.SetXattr("user.k", []byte("new"), 0); err != nil {
						t.Errorf("SetXattr: %v", err)
					}
				}()
			}
			inode.mu.Unlock()
			changed.Wait()
			waitFlushers(t, inode.fs)

			inode.mu.Lock()
			defer inode.mu.Unlock()
			if sent == nil {
				t.Fatal("MultipartBlobBegin was not called")
			}
			var gotSent map[string]string
			if sent.Metadata != nil {
				gotSent = make(map[string]string)
				for k, v := range sent.Metadata {
					gotSent[k] = *v
				}
			}
			if fmt.Sprint(gotSent) != fmt.Sprint(tc.expectedSent) || (gotSent == nil) != (tc.expectedSent == nil) {
				t.Errorf("sent metadata = %v, expected %v", gotSent, tc.expectedSent)
			}
			if inode.userMetadataDirty != tc.expectedDirty {
				t.Errorf("userMetadataDirty = %v, expected %v", inode.userMetadataDirty, tc.expectedDirty)
			}
			if (inode.mpu != nil) != tc.expectedMpu {
				t.Errorf("mpu started = %v, expected %v", inode.mpu != nil, tc.expectedMpu)
			}
			if inode.flushError != tc.expectedErr {
				t.Errorf("flushError = %v, expected %v", inode.flushError, tc.expectedErr)
			}
			if inode.IsFlushing != 0 {
				t.Errorf("IsFlushing = %v, expected 0", inode.IsFlushing)
			}
		})
	}
}

// BenchmarkSendStartMultipart starts a multipart upload and waits for the
// flush goroutine to finish, with 0 and 8 changed metadata entries.
func BenchmarkSendStartMultipart(b *testing.B) {
	for _, n := range []int{0, 8} {
		b.Run(fmt.Sprintf("%dmeta", n), func(b *testing.B) {
			inode := newBeginInode(func(inode *Inode, param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
				return &MultipartBlobCommitInput{Key: &param.Key, UploadId: PString("upload")}, nil
			})
			inode.userMetadata = make(map[string][]byte)
			for i := range n {
				inode.userMetadata[fmt.Sprintf("key%d", i)] = []byte("value")
			}
			b.ReportAllocs()
			for b.Loop() {
				inode.mu.Lock()
				inode.mpu = nil
				inode.userMetadataDirty = 2
				inode.sendStartMultipart()
				inode.mu.Unlock()
				waitFlushers(b, inode.fs)
			}
		})
	}
}
