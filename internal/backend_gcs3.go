// Copyright 2019 Ka-Hing Cheung
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
	. "github.com/kahing/goofys/api/common"

	"sync"
	"syscall"

	"github.com/jacobsa/fuse"
)

// GCS variant of S3
type GCS3 struct {
	*S3Backend
}

func NewGCS3(bucket string, flags *FlagStorage, config *S3Config) (*GCS3, error) {
	s3Backend, err := NewS3(bucket, flags, config)
	if err != nil {
		return nil, err
	}
	s3Backend.Capabilities().Name = "gcs"
	s := &GCS3{S3Backend: s3Backend}
	s.S3Backend.gcs = true
	return s, nil
}

func (s *GCS3) Delegate() interface{} {
	return s
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
			if err != nil && err != fuse.ENOENT {
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
