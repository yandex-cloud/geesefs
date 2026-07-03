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

package ycs3ext

import (
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/service/s3"
)

// ToListObjectsV2Output converts an ext-v1 list result to the standard SDK type.
// UserMetadata is dropped because vanilla s3.Object has no such field.
func ToListObjectsV2Output(ext *ListObjectsV1ExtOutput) *s3.ListObjectsV2Output {
	if ext == nil {
		return nil
	}
	out := &s3.ListObjectsV2Output{
		CommonPrefixes:        ext.CommonPrefixes,
		ContinuationToken:     ext.ContinuationToken,
		Delimiter:             ext.Delimiter,
		EncodingType:          ext.EncodingType,
		IsTruncated:           ext.IsTruncated,
		KeyCount:              ext.KeyCount,
		MaxKeys:               ext.MaxKeys,
		Name:                  ext.Name,
		NextContinuationToken: ext.NextContinuationToken,
		Prefix:                ext.Prefix,
		StartAfter:            ext.StartAfter,
	}
	if len(ext.Contents) > 0 {
		out.Contents = make([]*s3.Object, len(ext.Contents))
		for i, o := range ext.Contents {
			out.Contents[i] = &s3.Object{
				ETag:         o.ETag,
				Key:          o.Key,
				LastModified: o.LastModified,
				Owner:        o.Owner,
				Size:         o.Size,
				StorageClass: o.StorageClass,
			}
		}
	}
	return out
}

// ListBlobItem holds per-object fields from an ext-v1 list, including user metadata.
type ListBlobItem struct {
	Key          *string
	ETag         *string
	LastModified *time.Time
	Size         uint64
	StorageClass *string
	Metadata     map[string]*string
}

// ListBlobItemsFromV1Ext extracts list entries with metadata from an ext-v1 response.
func ListBlobItemsFromV1Ext(resp *ListObjectsV1ExtOutput) []ListBlobItem {
	if resp == nil || len(resp.Contents) == 0 {
		return nil
	}
	items := make([]ListBlobItem, 0, len(resp.Contents))
	for _, o := range resp.Contents {
		meta := o.UserMetadata
		if meta == nil {
			meta = make(map[string]*string)
		}
		items = append(items, ListBlobItem{
			Key:          o.Key,
			ETag:         o.ETag,
			LastModified: o.LastModified,
			Size:         uint64(aws.Int64Value(o.Size)),
			StorageClass: o.StorageClass,
			Metadata:     meta,
		})
	}
	return items
}
