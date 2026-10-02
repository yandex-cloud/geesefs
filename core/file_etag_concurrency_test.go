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
	"bytes"
	"io"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
)

type concurrentReadBackend struct {
	*TestBackend
	mu             sync.Mutex
	data           []byte
	etag           string
	patchCommitted chan struct{}
	finishPatch    chan struct{}
	getStarted     chan struct{}
	getOnce        sync.Once
	finishRead     chan struct{}
}

func (b *concurrentReadBackend) PatchBlob(p *PatchBlobInput) (*PatchBlobOutput, error) {
	data, err := io.ReadAll(p.Body)
	if err != nil {
		return nil, err
	}
	b.mu.Lock()
	copy(b.data[p.Offset:p.Offset+p.Size], data)
	b.etag = `"new"`
	b.mu.Unlock()
	close(b.patchCommitted)
	<-b.finishPatch
	return &PatchBlobOutput{ETag: PString(`"new"`)}, nil
}

func (b *concurrentReadBackend) GetBlob(p *GetBlobInput) (*GetBlobOutput, error) {
	b.mu.Lock()
	etag := b.etag
	data := append([]byte(nil), b.data[p.Start:p.Start+p.Count]...)
	b.mu.Unlock()
	b.getOnce.Do(func() { close(b.getStarted) })
	if p.IfMatch != nil && *p.IfMatch != etag {
		return nil, syscall.ESTALE
	}
	return &GetBlobOutput{
		HeadBlobOutput: HeadBlobOutput{BlobItemOutput: BlobItemOutput{ETag: PString(etag)}},
		Body:           &blockedReadBody{Reader: bytes.NewReader(data), ready: b.finishRead},
	}, nil
}

type blockedReadBody struct {
	io.Reader
	ready <-chan struct{}
}

func (b *blockedReadBody) Read(p []byte) (int, error) { <-b.ready; return b.Reader.Read(p) }
func (*blockedReadBody) Close() error                 { return nil }

func TestReadFileConcurrentPatchPreservesWrites(t *testing.T) {
	for _, patchFirst := range []bool{true, false} {
		name := "read_before_patch"
		if patchFirst {
			name = "patch_before_read"
		}
		t.Run(name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				backend := &concurrentReadBackend{
					TestBackend: &TestBackend{err: syscall.ENOSYS},
					data:        []byte("headDATAxxxx"), etag: `"old"`,
					patchCommitted: make(chan struct{}), finishPatch: make(chan struct{}),
					getStarted: make(chan struct{}), finishRead: make(chan struct{}),
				}
				fs, inode := newStaleReadTestFile(t, backend, 12, `"old"`, true)
				inode.mu.Lock()
				inode.SetCacheState(ST_MODIFIED)
				allocated := inode.buffers.Add(8, []byte("KEEP"), BUF_DIRTY, false)
				inode.mu.Unlock()
				if err := fs.bufferPool.Use(allocated, true); err != nil {
					t.Fatal(err)
				}
				patchDone := make(chan bool, 1)
				startPatch := func() {
					started := make(chan struct{})
					go func() {
						close(started)
						inode.mu.Lock()
						ok := inode.sendPatch(0, 4, bytes.NewReader([]byte("sent")), 4)
						inode.mu.Unlock()
						patchDone <- ok
					}()
					<-started
				}
				readDone := make(chan error, 1)
				startRead := func() {
					started := make(chan struct{})
					go func() {
						close(started)
						data, n, err := NewFileHandle(inode).ReadFile(4, 4)
						if err == nil && (n != 4 || !bytes.Equal(bytes.Join(data, nil), []byte("DATA"))) {
							err = syscall.EIO
						}
						readDone <- err
					}()
					<-started
				}
				if patchFirst {
					close(backend.finishRead)
					startPatch()
					<-backend.patchCommitted
					startRead()
					synctest.Wait()
					close(backend.finishPatch)
				} else {
					close(backend.finishPatch)
					startRead()
					<-backend.getStarted
					startPatch()
					synctest.Wait()
					close(backend.finishRead)
				}
				if !<-patchDone {
					t.Error("own PATCH failed")
				}
				if err := <-readDone; err != nil {
					t.Errorf("read concurrent with own PATCH: %v", err)
				}
				data, n, err := NewFileHandle(inode).ReadFile(8, 4)
				if err != nil || n != 4 || !bytes.Equal(bytes.Join(data, nil), []byte("KEEP")) {
					t.Errorf("pending write lost: data=%q n=%d err=%v", data, n, err)
				}
			})
		})
	}
}

func TestReadFileInvalidationPreservesNewWrites(t *testing.T) {
	for _, priorConflict := range []bool{false, true} {
		name := "first_read"
		if priorConflict {
			name = "after_prior_conflict"
		}
		t.Run(name, func(t *testing.T) {
			backend := &concurrentReadBackend{
				TestBackend: &TestBackend{err: syscall.ENOSYS},
				data:        []byte("headDATAxxxx"), etag: `"old"`,
				getStarted: make(chan struct{}), finishRead: make(chan struct{}),
			}
			_, inode := newStaleReadTestFile(t, backend, 12, `"old"`, true)
			if priorConflict {
				backend.mu.Lock()
				backend.etag = `"previous"`
				backend.mu.Unlock()
				if _, _, err := NewFileHandle(inode).ReadFile(4, 4); err != syscall.ESTALE {
					t.Fatalf("initial conflict error = %v, want ESTALE", err)
				}
				backend.mu.Lock()
				backend.etag = `"old"`
				backend.mu.Unlock()
				backend.getOnce = sync.Once{}
				backend.getStarted = make(chan struct{})
			}
			readDone := make(chan error, 1)
			go func() { _, _, err := NewFileHandle(inode).ReadFile(4, 4); readDone <- err }()
			<-backend.getStarted
			inode.SetFromBlobItem(&BlobItemOutput{Size: 12, ETag: PString(`"new"`)})
			if err := NewFileHandle(inode).WriteFile(8, []byte("KEEP"), true); err != nil {
				t.Fatal(err)
			}
			close(backend.finishRead)
			if err := <-readDone; err == nil {
				t.Error("invalidated read returned old data")
			}
			data, n, err := NewFileHandle(inode).ReadFile(8, 4)
			if err != nil || n != 4 || !bytes.Equal(bytes.Join(data, nil), []byte("KEEP")) {
				t.Fatalf("old read discarded a newer write: data=%q n=%d err=%v", data, n, err)
			}
		})
	}
}

type reorderedPatchBackend struct {
	*TestBackend
	mu             sync.Mutex
	etag           string
	firstCommitted chan struct{}
	finishFirst    chan struct{}
}

func (b *reorderedPatchBackend) PatchBlob(p *PatchBlobInput) (*PatchBlobOutput, error) {
	etag := `"first"`
	b.mu.Lock()
	if p.Offset > 0 {
		etag = `"second"`
	}
	b.etag = etag
	b.mu.Unlock()
	if p.Offset == 0 {
		close(b.firstCommitted)
		<-b.finishFirst
	}
	return &PatchBlobOutput{ETag: PString(etag)}, nil
}

func (b *reorderedPatchBackend) GetBlob(p *GetBlobInput) (*GetBlobOutput, error) {
	b.mu.Lock()
	etag := b.etag
	b.mu.Unlock()
	if p.IfMatch != nil && *p.IfMatch != etag {
		return nil, syscall.ESTALE
	}
	return &GetBlobOutput{
		HeadBlobOutput: HeadBlobOutput{BlobItemOutput: BlobItemOutput{ETag: PString(etag)}},
		Body:           io.NopCloser(bytes.NewReader([]byte("DATA"))),
	}, nil
}

func TestReadFileAfterReorderedPatchResponses(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := &reorderedPatchBackend{
			TestBackend: &TestBackend{err: syscall.ENOSYS}, etag: `"old"`,
			firstCommitted: make(chan struct{}), finishFirst: make(chan struct{}),
		}
		_, inode := newStaleReadTestFile(t, backend, 12, `"old"`, true)
		inode.mu.Lock()
		inode.SetCacheState(ST_MODIFIED)
		inode.mu.Unlock()
		done := make(chan bool, 2)
		patch := func(offset uint64) {
			inode.mu.Lock()
			ok := inode.sendPatch(offset, 4, bytes.NewReader([]byte("sent")), 4)
			inode.mu.Unlock()
			done <- ok
		}
		go patch(0)
		<-backend.firstCommitted
		secondStarted := make(chan struct{})
		go func() {
			close(secondStarted)
			patch(4)
		}()
		<-secondStarted
		synctest.Wait()
		close(backend.finishFirst)
		for i := 0; i < 2; i++ {
			if !<-done {
				t.Fatal("own PATCH failed")
			}
		}
		data, n, err := NewFileHandle(inode).ReadFile(8, 4)
		if err != nil || n != 4 || !bytes.Equal(bytes.Join(data, nil), []byte("DATA")) {
			t.Fatalf("read after own PATCHes: data=%q n=%d err=%v", data, n, err)
		}
	})
}
