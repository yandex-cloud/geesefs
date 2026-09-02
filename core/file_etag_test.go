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
	"context"
	"io"
	"syscall"
	"testing"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/yandex-cloud/geesefs/core/cfg"
)

func newStaleReadTestFile(t *testing.T, backend *TestBackend, size uint64, etag string, enableETagCheck bool) (*Goofys, *Inode) {
	t.Helper()

	flags := cfg.DefaultFlags()
	flags.ReadAheadKB = 0
	flags.ReadAheadSmallKB = 0
	flags.ReadAheadLargeKB = 0
	flags.ReadRetryInterval = 0
	flags.ReadRetryAttempts = 3
	flags.EnableReadETagCheck = enableETagCheck

	fs, err := newGoofys(context.Background(), "test", flags, func(string, *cfg.FlagStorage) (StorageBackend, error) {
		return backend, nil
	})
	if err != nil {
		t.Fatalf("newGoofys: %v", err)
	}
	t.Cleanup(fs.Shutdown)

	inode := NewInode(fs, fs.inodes[fuseops.RootInodeID], "file")
	inode.Attributes.Size = size
	inode.knownSize = size
	inode.knownETag = etag
	inode.userMetadata = make(map[string][]byte)
	return fs, inode
}

func TestReadFileDoesNotCheckETagByDefault(t *testing.T) {
	const oldETag = `"old"`
	const newETag = `"new"`
	newData := []byte("new-data")
	requests := make(chan GetBlobInput, 1)

	backend := &TestBackend{
		err: syscall.ENOSYS,
		GetBlobFunc: func(param *GetBlobInput) (*GetBlobOutput, error) {
			requests <- *param
			return &GetBlobOutput{
				HeadBlobOutput: HeadBlobOutput{
					BlobItemOutput: BlobItemOutput{ETag: PString(newETag)},
				},
				Body: io.NopCloser(bytes.NewReader(newData)),
			}, nil
		},
	}
	_, inode := newStaleReadTestFile(t, backend, uint64(len(newData)), oldETag, false)

	fh := NewFileHandle(inode)
	data, bytesRead, err := fh.ReadFile(0, int64(len(newData)))
	if err != nil {
		t.Fatalf("ReadFile: %v", err)
	}
	if got := bytes.Join(data, nil); !bytes.Equal(got, newData) {
		t.Fatalf("ReadFile data = %q, want %q", got, newData)
	}
	if bytesRead != len(newData) {
		t.Fatalf("ReadFile bytesRead = %d, want %d", bytesRead, len(newData))
	}

	request := <-requests
	if request.IfMatch != nil {
		t.Fatalf("GetBlob IfMatch = %q, want nil", *request.IfMatch)
	}
}

func TestReadFileRejectsChangedObject(t *testing.T) {
	const oldETag = `"old"`
	const newETag = `"new"`
	newData := []byte("new-data")
	requests := make(chan GetBlobInput, 3)

	backend := &TestBackend{
		err: syscall.ENOSYS,
		GetBlobFunc: func(param *GetBlobInput) (*GetBlobOutput, error) {
			requests <- *param
			end := param.Start + param.Count
			if end > uint64(len(newData)) {
				return nil, syscall.EINVAL
			}
			body := newData[param.Start:end]
			return &GetBlobOutput{
				HeadBlobOutput: HeadBlobOutput{
					BlobItemOutput: BlobItemOutput{ETag: PString(newETag)},
				},
				Body: io.NopCloser(bytes.NewReader(body)),
			}, nil
		},
	}
	fs, inode := newStaleReadTestFile(t, backend, uint64(len(newData)), oldETag, true)
	allocated := inode.buffers.Add(0, []byte("old-"), BUF_CLEAN, false)
	if err := fs.bufferPool.Use(allocated, true); err != nil {
		t.Fatalf("reserve cached buffer: %v", err)
	}

	fh := NewFileHandle(inode)
	data, bytesRead, err := fh.ReadFile(0, int64(len(newData)))
	if err != syscall.ESTALE {
		t.Fatalf("ReadFile error = %v, want %v", err, syscall.ESTALE)
	}
	if data != nil {
		t.Fatalf("ReadFile data = %q, want nil", data)
	}
	if bytesRead != 0 {
		t.Fatalf("ReadFile bytesRead = %d, want 0", bytesRead)
	}

	request := <-requests
	if request.IfMatch == nil || *request.IfMatch != oldETag {
		t.Fatalf("GetBlob IfMatch = %v, want %q", request.IfMatch, oldETag)
	}
	if len(requests) != 0 {
		t.Fatalf("GetBlob calls = %d, want 1", len(requests)+1)
	}
	if inode.buffers.Count() != 0 {
		t.Fatalf("cached buffers = %d, want 0", inode.buffers.Count())
	}
	if !inode.AttrTime.IsZero() {
		t.Fatalf("AttrTime = %v, want invalidated metadata", inode.AttrTime)
	}
}

func TestReadFileRejectsMissingETag(t *testing.T) {
	const oldETag = `"old"`
	data := []byte("data")

	backend := &TestBackend{
		err: syscall.ENOSYS,
		GetBlobFunc: func(param *GetBlobInput) (*GetBlobOutput, error) {
			return &GetBlobOutput{
				Body: io.NopCloser(bytes.NewReader(data)),
			}, nil
		},
	}
	_, inode := newStaleReadTestFile(t, backend, uint64(len(data)), oldETag, true)

	fh := NewFileHandle(inode)
	readData, bytesRead, err := fh.ReadFile(0, int64(len(data)))
	if err != syscall.ESTALE {
		t.Fatalf("ReadFile error = %v, want %v", err, syscall.ESTALE)
	}
	if readData != nil {
		t.Fatalf("ReadFile data = %q, want nil", readData)
	}
	if bytesRead != 0 {
		t.Fatalf("ReadFile bytesRead = %d, want 0", bytesRead)
	}
	if inode.buffers.Count() != 0 {
		t.Fatalf("cached buffers = %d, want 0", inode.buffers.Count())
	}
}
