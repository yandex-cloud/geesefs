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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"cloud.google.com/go/storage"
	"github.com/yandex-cloud/geesefs/core/cfg"
	"google.golang.org/api/option"
)

type changedReadBackend struct {
	*TestBackend
	readError error
}

func (b *changedReadBackend) GetBlob(*GetBlobInput) (*GetBlobOutput, error) {
	if b.readError != nil {
		return nil, b.readError
	}
	return &GetBlobOutput{
		HeadBlobOutput: HeadBlobOutput{BlobItemOutput: BlobItemOutput{ETag: PString(`"new"`)}},
		Body:           io.NopCloser(bytes.NewReader([]byte("data"))),
	}, nil
}

func runConflictingFlush(t *testing.T, readError error) *Inode {
	t.Helper()
	backend := &changedReadBackend{
		TestBackend: &TestBackend{err: syscall.ENOSYS, capabilities: &Capabilities{Name: "unconditional"}},
		readError:   readError,
	}
	fs, inode := newStaleReadTestFile(t, backend, 8, `"old"`, true)
	inode.mu.Lock()
	inode.SetCacheState(ST_MODIFIED)
	inode.IsFlushing = fs.flags.MaxParallelParts
	allocated := inode.buffers.Add(0, []byte("KEEP"), BUF_DIRTY, false)
	inode.mu.Unlock()
	if err := fs.bufferPool.Use(allocated, true); err != nil {
		t.Fatal(err)
	}
	atomic.AddInt64(&fs.activeFlushers, 1)
	inode.flushSmallObject()
	return inode
}

func TestTruncateAfterStaleFlush(t *testing.T) {
	for _, readError := range []error{nil, syscall.ENOENT, syscall.ERANGE} {
		name := "ESTALE"
		if readError != nil {
			name = readError.Error()
		}
		t.Run(name, func(t *testing.T) {
			inode := runConflictingFlush(t, readError)
			done := make(chan error, 1)
			go func() { done <- inode.SetAttributes(PUInt64(4), nil, nil, nil, nil) }()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				inode.mu.Lock()
				inode.readRanges = nil
				inode.mu.Unlock()
				<-done
				t.Fatal("truncate hangs after a finished conflicting flush until its leaked range lock is removed")
			}
		})
	}
}

func TestSyncFileReportsStaleFlush(t *testing.T) {
	for _, readError := range []error{nil, syscall.ENOENT, syscall.ERANGE} {
		want := readError
		if want == nil {
			want = syscall.ESTALE
		}
		t.Run(want.Error(), func(t *testing.T) {
			inode := runConflictingFlush(t, readError)
			if err := inode.SyncFile(); err != want {
				t.Fatalf("fsync error = %v, want %v after dropping unsaved data", err, want)
			}
		})
	}
}

type gcsReadTestBackend struct{ *GCS3 }

func (*gcsReadTestBackend) Init(string) error { return nil }

func (*gcsReadTestBackend) MultipartExpire(*MultipartExpireInput) (*MultipartExpireOutput, error) {
	return &MultipartExpireOutput{}, nil
}

func TestGCSReadAfterListing(t *testing.T) {
	const jsonETag = "CKicn4fknbUCEAE="
	const xmlETag = `"8d777f385d3dfec8815d20f7496026dc"`
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if strings.TrimSuffix(r.URL.Path, "/") == "/testbucket" {
			w.Header().Set("Content-Type", "application/xml")
			_, _ = io.WriteString(w, `<ListBucketResult><Name>testbucket</Name><IsTruncated>false</IsTruncated><Contents><Key>file</Key><Size>4</Size><ETag>`+xmlETag+`</ETag><LastModified>2024-01-01T00:00:00Z</LastModified></Contents></ListBucketResult>`)
			return
		}
		if strings.HasPrefix(r.URL.Path, "/storage/v1/") {
			w.Header().Set("Content-Type", "application/json")
			_, _ = io.WriteString(w, `{"items":[{"name":"file","size":"4","etag":"`+jsonETag+`","updated":"2024-01-01T00:00:00Z"}]}`)
			return
		}
		w.Header().Set("ETag", xmlETag)
		_, _ = io.WriteString(w, "data")
	}))
	defer srv.Close()
	client, err := storage.NewClient(context.Background(), option.WithEndpoint(srv.URL+"/storage/v1/"), option.WithoutAuthentication())
	if err != nil {
		t.Fatal(err)
	}
	defer client.Close()
	s3Backend, err := NewS3("testbucket", &cfg.FlagStorage{Endpoint: srv.URL, EnableReadETagCheck: true}, (&cfg.S3Config{
		Region: "us-east-1", AccessKey: "test", SecretKey: "test",
	}).Init())
	if err != nil {
		t.Fatal(err)
	}
	s3Backend.Capabilities().Name = "gcs"
	backend := &GCS3{S3Backend: s3Backend, gcs: client}
	listing, err := backend.ListBlobs(&ListBlobsInput{})
	if err != nil || len(listing.Items) != 1 {
		t.Fatalf("ListBlobs: result=%v err=%v", listing, err)
	}
	wrapper := &gcsReadTestBackend{backend}
	_, inode := newStaleReadTestFile(t, wrapper, 4, "", true)
	inode.SetFromBlobItem(&listing.Items[0])
	data, n, err := NewFileHandle(inode).ReadFile(0, 4)
	if err != nil || n != 4 || !bytes.Equal(bytes.Join(data, nil), []byte("data")) {
		t.Fatalf("unchanged GCS object unreadable: data=%q n=%d err=%v", data, n, err)
	}
}

func TestSyncFileReportsMetadataConflict(t *testing.T) {
	_, inode := newStaleReadTestFile(t, &TestBackend{err: syscall.ENOSYS}, 8, `"old"`, true)
	if err := NewFileHandle(inode).WriteFile(0, []byte("KEEP"), true); err != nil {
		t.Fatal(err)
	}
	inode.SetFromBlobItem(&BlobItemOutput{Size: 8, ETag: PString(`"new"`)})
	if err := inode.SyncFile(); err != syscall.ESTALE {
		t.Fatalf("fsync error = %v, want ESTALE after metadata refresh discarded unsaved data", err)
	}
}
