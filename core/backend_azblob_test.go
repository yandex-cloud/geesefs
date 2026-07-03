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
	"testing"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

func TestAzPutAccessConditions(t *testing.T) {
	star := "*"
	etag := `"abc123"`

	tests := []struct {
		name      string
		in        *PutBlobInput
		wantMatch azblob.ETag
		wantNone  azblob.ETag
	}{
		{"none", &PutBlobInput{}, azblob.ETagNone, azblob.ETagNone},
		{"IfNoneMatch wildcard", &PutBlobInput{IfNoneMatch: &star}, azblob.ETagNone, azblob.ETagAny},
		{"IfNoneMatch etag", &PutBlobInput{IfNoneMatch: &etag}, azblob.ETagNone, azblob.ETag(etag)},
		{"IfMatch", &PutBlobInput{IfMatch: &etag}, azblob.ETag(etag), azblob.ETagNone},
		{"IfNoneMatch wins over IfMatch", &PutBlobInput{IfNoneMatch: &star, IfMatch: &etag}, azblob.ETagNone, azblob.ETagAny},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mod := azPutAccessConditions(tc.in).ModifiedAccessConditions
			if mod.IfMatch != tc.wantMatch {
				t.Fatalf("IfMatch = %q, want %q", mod.IfMatch, tc.wantMatch)
			}
			if mod.IfNoneMatch != tc.wantNone {
				t.Fatalf("IfNoneMatch = %q, want %q", mod.IfNoneMatch, tc.wantNone)
			}
		})
	}
}

func TestAzPutBlobTags(t *testing.T) {
	got := azPutBlobTags(&PutBlobInput{Tags: map[string]string{"geesefs-lock": "true"}})
	if len(got) != 1 || got["geesefs-lock"] != "true" {
		t.Fatalf("got %v", got)
	}
	hdr := azblob.SerializeBlobTagsHeader(got)
	if hdr == nil || *hdr != "geesefs-lock=true" {
		t.Fatalf("SerializeBlobTagsHeader = %v", hdr)
	}
	if len(azPutBlobTags(&PutBlobInput{})) != 0 {
		t.Fatal("expected empty tags")
	}
}
