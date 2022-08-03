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
	"context"
	"encoding/json"
	"io/ioutil"
	"net/url"

	"github.com/minio/madmin-go"
	. "github.com/yandex-cloud/geesefs/api/common"
)

type BucketsUsage struct {
	BucketsSizes map[string]uint64
}

type MinioBackend struct {
	*S3Backend
}

func NewMinio(bucket string, flags *FlagStorage, config *S3Config) (*MinioBackend, error) {
	s3Backend, err := NewS3(bucket, flags, config)
	if err != nil {
		return nil, err
	}
	s3Backend.Capabilities().Name = "minio"
	s := &MinioBackend{S3Backend: s3Backend}
	return s, nil
}

func (s *MinioBackend) GetBucketUsage(param *GetBucketUsageInput) (*GetBucketUsageOutput, error) {
	value, err := s.Config.Credentials.Get()
	if err != nil {
		return nil, err
	}

	endpointURL, _ := url.Parse(s.Endpoint)
	madminClient, err := madmin.New(endpointURL.Host, value.AccessKeyID, value.SecretAccessKey, endpointURL.Scheme == "https")
	if err != nil {
		return nil, err
	}
	resp, err := madminClient.ExecuteMethod(context.Background(), "GET", madmin.RequestData{RelPath: "/v3/datausageinfo"})
	if err != nil {
		return nil, err
	}
	response, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var result BucketsUsage
	json.Unmarshal(response, &result)
	return &GetBucketUsageOutput{Size: result.BucketsSizes[s.Bucket()]}, nil
}
