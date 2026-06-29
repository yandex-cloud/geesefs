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
