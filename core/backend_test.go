// Copyright 2020 Ka-Hing Cheung
// Copyright 2020 Databricks
// Copyright 2021 Yandex LLC
// Copyright 2026 Dectris Ltd.
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
	"fmt"
	"io"
	"sync"
	"syscall"
	"time"

	. "gopkg.in/check.v1"
)

// ============================================================================
// TestBackend - Generic mock with function hooks (used by other tests)
// ============================================================================

type TestBackend struct {
	StorageBackend
	ListBlobsFunc           func(param *ListBlobsInput) (*ListBlobsOutput, error)
	HeadBlobFunc            func(param *HeadBlobInput) (*HeadBlobOutput, error)
	MultipartBlobAddFunc    func(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error)
	MultipartBlobCopyFunc   func(param *MultipartBlobCopyInput) (*MultipartBlobCopyOutput, error)
	MultipartBlobCommitFunc func(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error)
	capabilities            *Capabilities
	err                     error
}

func (s *TestBackend) Init(bucket string) error {
	if s.StorageBackend == nil {
		return nil
	}
	return s.StorageBackend.Init(bucket)
}

func (s *TestBackend) Capabilities() *Capabilities {
	if s.StorageBackend == nil {
		if s.capabilities == nil {
			s.capabilities = &Capabilities{
				Name:             "s3",
				MaxMultipartSize: 5 * 1024 * 1024 * 1024,
			}
		}
		return s.capabilities
	}
	return s.StorageBackend.Capabilities()
}

func (s *TestBackend) Delegate() interface{} {
	return s
}

func (s *TestBackend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	if s.HeadBlobFunc != nil {
		return s.HeadBlobFunc(param)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.HeadBlob(param)
}

func (s *TestBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	if s.ListBlobsFunc != nil {
		return s.ListBlobsFunc(param)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.ListBlobs(param)
}

func (s *TestBackend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.DeleteBlob(param)
}

func (s *TestBackend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.DeleteBlobs(param)
}

func (s *TestBackend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.RenameBlob(param)
}

func (s *TestBackend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.CopyBlob(param)
}

func (s *TestBackend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.GetBlob(param)
}

func (s *TestBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.PutBlob(param)
}

func (s *TestBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.MultipartBlobBegin(param)
}

func (s *TestBackend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	if s.MultipartBlobAddFunc != nil {
		return s.MultipartBlobAddFunc(param)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.MultipartBlobAdd(param)
}

func (s *TestBackend) MultipartBlobCopy(param *MultipartBlobCopyInput) (*MultipartBlobCopyOutput, error) {
	if s.MultipartBlobCopyFunc != nil {
		return s.MultipartBlobCopyFunc(param)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.MultipartBlobCopy(param)
}

func (s *TestBackend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.MultipartBlobAbort(param)
}

func (s *TestBackend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	if s.MultipartBlobCommitFunc != nil {
		return s.MultipartBlobCommitFunc(param)
	}
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.MultipartBlobCommit(param)
}

func (s *TestBackend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	if s.err != nil {
		return nil, s.err
	}
	return s.StorageBackend.MultipartExpire(param)
}

// ============================================================================
// BackendTest - Test suite for StorageBackend interface tests
// ============================================================================

type BackendTest struct{}

var _ = Suite(&BackendTest{})

// ============================================================================
// mockConditionalBackend - Full mock with in-memory storage and conditional writes
// ============================================================================

// mockConditionalBackend implements StorageBackend interface for testing conditional writes.
// It simulates S3/Azure conditional write behavior with If-Match and If-None-Match headers.
type mockConditionalBackend struct {
	mu      sync.Mutex
	objects map[string]*mockStoredObject

	// Hooks for testing - allow inspection of parameters
	onGetBlob func(param *GetBlobInput)
	onPutBlob func(param *PutBlobInput)
}

type mockStoredObject struct {
	data []byte
	etag string
}

func newMockConditionalBackend() *mockConditionalBackend {
	return &mockConditionalBackend{
		objects: make(map[string]*mockStoredObject),
	}
}

func (m *mockConditionalBackend) generateETag() string {
	return fmt.Sprintf("\"%d\"", time.Now().UnixNano())
}

func (m *mockConditionalBackend) Init(key string) error { return nil }
func (m *mockConditionalBackend) Capabilities() *Capabilities {
	return &Capabilities{Name: "mock-conditional"}
}
func (m *mockConditionalBackend) Bucket() string { return "mock-bucket" }

func (m *mockConditionalBackend) HeadBlob(param *HeadBlobInput) (*HeadBlobOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	obj, exists := m.objects[param.Key]
	if !exists {
		return nil, syscall.ENOENT
	}
	return &HeadBlobOutput{
		BlobItemOutput: BlobItemOutput{
			Key:  &param.Key,
			ETag: &obj.etag,
			Size: uint64(len(obj.data)),
		},
	}, nil
}

func (m *mockConditionalBackend) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	return &ListBlobsOutput{}, nil
}

func (m *mockConditionalBackend) DeleteBlob(param *DeleteBlobInput) (*DeleteBlobOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, param.Key)
	return &DeleteBlobOutput{}, nil
}

func (m *mockConditionalBackend) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	return &DeleteBlobsOutput{}, nil
}

func (m *mockConditionalBackend) RenameBlob(param *RenameBlobInput) (*RenameBlobOutput, error) {
	return &RenameBlobOutput{}, nil
}

func (m *mockConditionalBackend) CopyBlob(param *CopyBlobInput) (*CopyBlobOutput, error) {
	return &CopyBlobOutput{}, nil
}

func (m *mockConditionalBackend) GetBlob(param *GetBlobInput) (*GetBlobOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Call hook if set
	if m.onGetBlob != nil {
		m.onGetBlob(param)
	}

	obj, exists := m.objects[param.Key]
	if !exists {
		return nil, syscall.ENOENT
	}

	// Check IfMatch condition - only return if ETag matches
	if param.IfMatch != nil && *param.IfMatch != obj.etag {
		return nil, fmt.Errorf("PreconditionFailed: ETag mismatch for GetBlob")
	}

	return &GetBlobOutput{
		HeadBlobOutput: HeadBlobOutput{
			BlobItemOutput: BlobItemOutput{
				Key:  &param.Key,
				ETag: &obj.etag,
				Size: uint64(len(obj.data)),
			},
		},
		Body: io.NopCloser(bytes.NewReader(obj.data)),
	}, nil
}

func (m *mockConditionalBackend) PutBlob(param *PutBlobInput) (*PutBlobOutput, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Call hook if set
	if m.onPutBlob != nil {
		m.onPutBlob(param)
	}

	obj, exists := m.objects[param.Key]

	// Check If-None-Match: "*" condition (create-if-not-exists)
	// Returns 412 Precondition Failed if object already exists
	if param.IfNoneMatch != nil && *param.IfNoneMatch == "*" {
		if exists {
			return nil, fmt.Errorf("PreconditionFailed: object already exists")
		}
	}

	// Check If-Match condition (optimistic locking)
	// Returns 412 Precondition Failed if ETag doesn't match
	if param.IfMatch != nil {
		if !exists {
			return nil, fmt.Errorf("PreconditionFailed: object does not exist for If-Match")
		}
		if *param.IfMatch != obj.etag {
			return nil, fmt.Errorf("PreconditionFailed: ETag mismatch, expected %s, got %s", *param.IfMatch, obj.etag)
		}
	}

	// Read the body
	data, err := io.ReadAll(param.Body)
	if err != nil {
		return nil, err
	}

	etag := m.generateETag()
	m.objects[param.Key] = &mockStoredObject{
		data: data,
		etag: etag,
	}

	return &PutBlobOutput{
		ETag: &etag,
	}, nil
}

func (m *mockConditionalBackend) PatchBlob(param *PatchBlobInput) (*PatchBlobOutput, error) {
	return &PatchBlobOutput{}, nil
}

func (m *mockConditionalBackend) MultipartBlobBegin(param *MultipartBlobBeginInput) (*MultipartBlobCommitInput, error) {
	return &MultipartBlobCommitInput{}, nil
}

func (m *mockConditionalBackend) MultipartBlobAdd(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
	return &MultipartBlobAddOutput{}, nil
}

func (m *mockConditionalBackend) MultipartBlobCopy(param *MultipartBlobCopyInput) (*MultipartBlobCopyOutput, error) {
	return &MultipartBlobCopyOutput{}, nil
}

func (m *mockConditionalBackend) MultipartBlobAbort(param *MultipartBlobCommitInput) (*MultipartBlobAbortOutput, error) {
	return &MultipartBlobAbortOutput{}, nil
}

func (m *mockConditionalBackend) MultipartBlobCommit(param *MultipartBlobCommitInput) (*MultipartBlobCommitOutput, error) {
	return &MultipartBlobCommitOutput{}, nil
}

func (m *mockConditionalBackend) MultipartExpire(param *MultipartExpireInput) (*MultipartExpireOutput, error) {
	return &MultipartExpireOutput{}, nil
}

func (m *mockConditionalBackend) RemoveBucket(param *RemoveBucketInput) (*RemoveBucketOutput, error) {
	return &RemoveBucketOutput{}, nil
}

func (m *mockConditionalBackend) MakeBucket(param *MakeBucketInput) (*MakeBucketOutput, error) {
	return &MakeBucketOutput{}, nil
}

func (m *mockConditionalBackend) Delegate() interface{} {
	return nil
}

// ============================================================================
// Tests for PutBlob with IfNoneMatch (create-if-not-exists)
// ============================================================================

func (s *BackendTest) TestPutBlobIfNoneMatchCreatesNewObject(t *C) {
	mock := newMockConditionalBackend()

	ifNoneMatch := "*"
	resp, err := mock.PutBlob(&PutBlobInput{
		Key:         "test-key",
		Body:        bytes.NewReader([]byte("test data")),
		IfNoneMatch: &ifNoneMatch,
	})

	t.Assert(err, IsNil)
	t.Assert(resp.ETag, NotNil)
	t.Assert(*resp.ETag != "", Equals, true)

	// Verify object was created
	_, exists := mock.objects["test-key"]
	t.Assert(exists, Equals, true)
}

func (s *BackendTest) TestPutBlobIfNoneMatchFailsIfExists(t *C) {
	mock := newMockConditionalBackend()

	// Pre-create an object
	mock.objects["test-key"] = &mockStoredObject{
		data: []byte("existing data"),
		etag: "\"existing-etag\"",
	}

	ifNoneMatch := "*"
	_, err := mock.PutBlob(&PutBlobInput{
		Key:         "test-key",
		Body:        bytes.NewReader([]byte("new data")),
		IfNoneMatch: &ifNoneMatch,
	})

	t.Assert(err, NotNil)
	t.Assert(err.Error(), Matches, ".*PreconditionFailed.*")

	// Verify original data is unchanged
	t.Assert(string(mock.objects["test-key"].data), Equals, "existing data")
}

func (s *BackendTest) TestPutBlobWithoutConditionsOverwrites(t *C) {
	mock := newMockConditionalBackend()

	// Pre-create an object
	mock.objects["test-key"] = &mockStoredObject{
		data: []byte("existing data"),
		etag: "\"existing-etag\"",
	}

	// Put without conditions - should overwrite
	resp, err := mock.PutBlob(&PutBlobInput{
		Key:  "test-key",
		Body: bytes.NewReader([]byte("new data")),
	})

	t.Assert(err, IsNil)
	t.Assert(resp.ETag, NotNil)
	t.Assert(string(mock.objects["test-key"].data), Equals, "new data")
}

// ============================================================================
// Tests for PutBlob with IfMatch (optimistic locking)
// ============================================================================

func (s *BackendTest) TestPutBlobIfMatchSucceedsWithCorrectETag(t *C) {
	mock := newMockConditionalBackend()

	// Pre-create an object
	existingETag := "\"existing-etag\""
	mock.objects["test-key"] = &mockStoredObject{
		data: []byte("existing data"),
		etag: existingETag,
	}

	// Update with correct ETag
	resp, err := mock.PutBlob(&PutBlobInput{
		Key:     "test-key",
		Body:    bytes.NewReader([]byte("updated data")),
		IfMatch: &existingETag,
	})

	t.Assert(err, IsNil)
	t.Assert(resp.ETag, NotNil)
	t.Assert(*resp.ETag != existingETag, Equals, true) // New ETag should be different
	t.Assert(string(mock.objects["test-key"].data), Equals, "updated data")
}

func (s *BackendTest) TestPutBlobIfMatchFailsWithWrongETag(t *C) {
	mock := newMockConditionalBackend()

	// Pre-create an object
	mock.objects["test-key"] = &mockStoredObject{
		data: []byte("existing data"),
		etag: "\"actual-etag\"",
	}

	// Try to update with wrong ETag
	wrongETag := "\"wrong-etag\""
	_, err := mock.PutBlob(&PutBlobInput{
		Key:     "test-key",
		Body:    bytes.NewReader([]byte("updated data")),
		IfMatch: &wrongETag,
	})

	t.Assert(err, NotNil)
	t.Assert(err.Error(), Matches, ".*PreconditionFailed.*ETag mismatch.*")

	// Verify original data is unchanged
	t.Assert(string(mock.objects["test-key"].data), Equals, "existing data")
}

func (s *BackendTest) TestPutBlobIfMatchFailsIfObjectNotExists(t *C) {
	mock := newMockConditionalBackend()

	// Try to update non-existent object with If-Match
	etag := "\"some-etag\""
	_, err := mock.PutBlob(&PutBlobInput{
		Key:     "non-existent-key",
		Body:    bytes.NewReader([]byte("data")),
		IfMatch: &etag,
	})

	t.Assert(err, NotNil)
	t.Assert(err.Error(), Matches, ".*PreconditionFailed.*does not exist.*")
}

// ============================================================================
// Tests for GetBlob with IfMatch
// ============================================================================

func (s *BackendTest) TestGetBlobIfMatchSucceedsWithCorrectETag(t *C) {
	mock := newMockConditionalBackend()

	existingETag := "\"test-etag\""
	mock.objects["test-key"] = &mockStoredObject{
		data: []byte("test data"),
		etag: existingETag,
	}

	resp, err := mock.GetBlob(&GetBlobInput{
		Key:     "test-key",
		IfMatch: &existingETag,
	})

	t.Assert(err, IsNil)
	t.Assert(resp.ETag, NotNil)
	t.Assert(*resp.ETag, Equals, existingETag)

	data, _ := io.ReadAll(resp.Body)
	t.Assert(string(data), Equals, "test data")
}

func (s *BackendTest) TestGetBlobIfMatchFailsWithWrongETag(t *C) {
	mock := newMockConditionalBackend()

	mock.objects["test-key"] = &mockStoredObject{
		data: []byte("test data"),
		etag: "\"actual-etag\"",
	}

	wrongETag := "\"wrong-etag\""
	_, err := mock.GetBlob(&GetBlobInput{
		Key:     "test-key",
		IfMatch: &wrongETag,
	})

	t.Assert(err, NotNil)
	t.Assert(err.Error(), Matches, ".*PreconditionFailed.*")
}

func (s *BackendTest) TestGetBlobWithoutConditions(t *C) {
	mock := newMockConditionalBackend()

	existingETag := "\"test-etag\""
	mock.objects["test-key"] = &mockStoredObject{
		data: []byte("test data"),
		etag: existingETag,
	}

	// Get without IfMatch - should always succeed
	resp, err := mock.GetBlob(&GetBlobInput{
		Key: "test-key",
	})

	t.Assert(err, IsNil)
	t.Assert(resp.ETag, NotNil)
}

// ============================================================================
// Tests for optimistic locking pattern (read-modify-write)
// ============================================================================

func (s *BackendTest) TestOptimisticLockingPattern(t *C) {
	mock := newMockConditionalBackend()

	// Initial create
	ifNoneMatch := "*"
	resp1, err := mock.PutBlob(&PutBlobInput{
		Key:         "shared-resource",
		Body:        bytes.NewReader([]byte("initial value")),
		IfNoneMatch: &ifNoneMatch,
	})
	t.Assert(err, IsNil)
	etag1 := *resp1.ETag

	// Client A reads and gets ETag
	getResp, err := mock.GetBlob(&GetBlobInput{Key: "shared-resource"})
	t.Assert(err, IsNil)
	clientAETag := *getResp.ETag
	getResp.Body.Close()

	// Client B reads and gets same ETag
	getResp2, err := mock.GetBlob(&GetBlobInput{Key: "shared-resource"})
	t.Assert(err, IsNil)
	clientBETag := *getResp2.ETag
	getResp2.Body.Close()

	t.Assert(clientAETag, Equals, clientBETag)
	t.Assert(clientAETag, Equals, etag1)

	// Client A updates successfully
	resp2, err := mock.PutBlob(&PutBlobInput{
		Key:     "shared-resource",
		Body:    bytes.NewReader([]byte("client A update")),
		IfMatch: &clientAETag,
	})
	t.Assert(err, IsNil)
	newETag := *resp2.ETag
	t.Assert(newETag != etag1, Equals, true)

	// Client B tries to update with stale ETag - should fail
	_, err = mock.PutBlob(&PutBlobInput{
		Key:     "shared-resource",
		Body:    bytes.NewReader([]byte("client B update")),
		IfMatch: &clientBETag, // This is now stale
	})
	t.Assert(err, NotNil)
	t.Assert(err.Error(), Matches, ".*PreconditionFailed.*")

	// Verify Client A's update persisted
	t.Assert(string(mock.objects["shared-resource"].data), Equals, "client A update")
}

func (s *BackendTest) TestOptimisticLockingRetryPattern(t *C) {
	mock := newMockConditionalBackend()

	// Initial create
	ifNoneMatch := "*"
	resp, err := mock.PutBlob(&PutBlobInput{
		Key:         "counter",
		Body:        bytes.NewReader([]byte("0")),
		IfNoneMatch: &ifNoneMatch,
	})
	t.Assert(err, IsNil)
	currentETag := *resp.ETag

	// Simulate a retry loop for incrementing a counter
	maxRetries := 3
	success := false

	for i := 0; i < maxRetries; i++ {
		// Read current value
		getResp, err := mock.GetBlob(&GetBlobInput{Key: "counter"})
		t.Assert(err, IsNil)
		data, _ := io.ReadAll(getResp.Body)
		getResp.Body.Close()
		readETag := *getResp.ETag

		// Modify value
		newValue := string(data) + "+1"

		// Try to write with If-Match
		putResp, err := mock.PutBlob(&PutBlobInput{
			Key:     "counter",
			Body:    bytes.NewReader([]byte(newValue)),
			IfMatch: &readETag,
		})

		if err == nil {
			currentETag = *putResp.ETag
			success = true
			break
		}
		// If failed due to conflict, retry
	}

	t.Assert(success, Equals, true)
	t.Assert(string(mock.objects["counter"].data), Equals, "0+1")
	_ = currentETag // Used in real scenarios for subsequent operations
}

// ============================================================================
// Tests for hook inspection
// ============================================================================

func (s *BackendTest) TestPutBlobHookInspection(t *C) {
	mock := newMockConditionalBackend()

	var capturedIfMatch *string
	var capturedIfNoneMatch *string

	mock.onPutBlob = func(param *PutBlobInput) {
		capturedIfMatch = param.IfMatch
		capturedIfNoneMatch = param.IfNoneMatch
	}

	// Test IfNoneMatch is captured
	ifNoneMatch := "*"
	_, err := mock.PutBlob(&PutBlobInput{
		Key:         "test1",
		Body:        bytes.NewReader([]byte("data")),
		IfNoneMatch: &ifNoneMatch,
	})
	t.Assert(err, IsNil)
	t.Assert(capturedIfNoneMatch, NotNil)
	t.Assert(*capturedIfNoneMatch, Equals, "*")
	t.Assert(capturedIfMatch, IsNil)

	// Reset
	capturedIfMatch = nil
	capturedIfNoneMatch = nil

	// Test IfMatch is captured
	existingETag := mock.objects["test1"].etag
	_, err = mock.PutBlob(&PutBlobInput{
		Key:     "test1",
		Body:    bytes.NewReader([]byte("updated")),
		IfMatch: &existingETag,
	})
	t.Assert(err, IsNil)
	t.Assert(capturedIfMatch, NotNil)
	t.Assert(*capturedIfMatch, Equals, existingETag)
	t.Assert(capturedIfNoneMatch, IsNil)
}

func (s *BackendTest) TestGetBlobHookInspection(t *C) {
	mock := newMockConditionalBackend()

	existingETag := "\"test-etag\""
	mock.objects["test-key"] = &mockStoredObject{
		data: []byte("test data"),
		etag: existingETag,
	}

	var capturedIfMatch *string
	mock.onGetBlob = func(param *GetBlobInput) {
		capturedIfMatch = param.IfMatch
	}

	// Test without IfMatch
	resp, err := mock.GetBlob(&GetBlobInput{Key: "test-key"})
	t.Assert(err, IsNil)
	resp.Body.Close()
	t.Assert(capturedIfMatch, IsNil)

	// Test with IfMatch
	resp, err = mock.GetBlob(&GetBlobInput{
		Key:     "test-key",
		IfMatch: &existingETag,
	})
	t.Assert(err, IsNil)
	resp.Body.Close()
	t.Assert(capturedIfMatch, NotNil)
	t.Assert(*capturedIfMatch, Equals, existingETag)
}
