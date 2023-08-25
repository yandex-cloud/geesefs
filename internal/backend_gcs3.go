// Copyright 2019 Ka-Hing Cheung
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

package internal

import (
	"github.com/yandex-cloud/geesefs/internal/cfg"

	"context"
	"os"
	"sync"
	"syscall"

	"cloud.google.com/go/storage"
	"google.golang.org/api/iterator"
)

// GCS variant of S3
type GCS3 struct {
	*S3Backend
	gcs *storage.Client
	jsonCredFile string
}

func NewGCS3(bucket string, flags *cfg.FlagStorage, config *cfg.S3Config) (*GCS3, error) {
	s3Backend, err := NewS3(bucket, flags, config)
	if err != nil {
		return nil, err
	}
	s3Backend.Capabilities().Name = "gcs"
	s := &GCS3{S3Backend: s3Backend}
	s.S3Backend.gcs = true
	if os.Getenv("GOOGLE_APPLICATION_CREDENTIALS") != "" {
		s.gcs, err = storage.NewClient(context.Background())
		if err != nil {
			return nil, err
		}
	}
	return s, nil
}

func (s *GCS3) Delegate() interface{} {
	return s
}

func (s *GCS3) ListBlobs(param *ListBlobsInput) (*ListBlobsOutput, error) {
	if s.gcs == nil {
		// Listings with metadata are only supported in REST API
		// And REST API requires separate authentication credentials
		// And it's also hard to drop S3 API because REST API doesn't
		// have proper multipart upload support
		r, e := s.S3Backend.ListBlobs(param)
		return r, e
	}
	q := &storage.Query{}
	if param.Delimiter != nil {
		q.Delimiter = *param.Delimiter
	}
	if param.Prefix != nil {
		q.Prefix = *param.Prefix
	}
	if param.StartAfter != nil {
		q.StartOffset = *param.StartAfter
	}
	it := s.gcs.Bucket(s.bucket).Objects(context.Background(), q)
	prefixes := make([]BlobPrefixOutput, 0)
	items := make([]BlobItemOutput, 0)
	n := uint32(0)
	done := false
	var last string
	for {
		attrs, err := it.Next()
		if err == iterator.Done {
			done = true
			break
		}
		if err != nil {
			return nil, err
		}
		last = attrs.Name
		if attrs.Prefix != "" {
			last = attrs.Prefix
		}
		if param.StartAfter != nil && last == *param.StartAfter {
			continue
		}
		if attrs.Prefix != "" {
			prefixes = append(prefixes, BlobPrefixOutput{
				Prefix: &attrs.Prefix,
			})
		} else {
			items = append(items, BlobItemOutput{
				Key: &attrs.Name,
				ETag: &attrs.Etag,
				LastModified: &attrs.Updated,
				Size: uint64(attrs.Size),
				StorageClass: &attrs.StorageClass,
				Metadata: PMetadata(attrs.Metadata),
			})
		}
		n++
		if param.MaxKeys != nil && n >= *param.MaxKeys {
			break
		}
	}
	return &ListBlobsOutput{
		Prefixes:              prefixes,
		Items:                 items,
		NextContinuationToken: &last,
		IsTruncated:           !done,
	}, nil
}

func (s *GCS3) DeleteBlobs(param *DeleteBlobsInput) (*DeleteBlobsOutput, error) {
	// GCS does not have multi-delete
	var wg sync.WaitGroup
	var overallErr error

	for _, key := range param.Items {
		wg.Add(1)
		go func(key string) {
			_, err := s.DeleteBlob(&DeleteBlobInput{
				Key: key,
			})
			if err != nil && err != syscall.ENOENT {
				overallErr = err
			}
			wg.Done()
		}(key)
	}
	wg.Wait()
	if overallErr != nil {
		return nil, mapAwsError(overallErr)
	}

	return &DeleteBlobsOutput{}, nil
}

// FIXME GCS doesn't have UploadPartCopy, so optimized modification flushing doesn't work
// You can either reupload the whole object or use some other way of making multipart objects
// For example, Composite Objects are even better than multipart uploads but intermediate
// objects should be filtered out from List responses so they don't appear as separate files then
func (s *GCS3) MultipartBlobCopy(param *MultipartBlobCopyInput) (*MultipartBlobCopyOutput, error) {
	return nil, syscall.ENOSYS
}

func (s *GCS3) PatchBlob(param *PatchBlobInput) (*PatchBlobOutput, error) {
	return nil, syscall.ENOSYS
}
