package ycs3ext

import (
	"io"
	"time"

	"github.com/aws/aws-sdk-go/service/s3"
)

// ListObjectsV1ExtInput is layout-compatible with ListObjectsV2Input.
type ListObjectsV1ExtInput s3.ListObjectsV2Input

func (s *ListObjectsV1ExtInput) getBucket() string {
	if s.Bucket == nil {
		return ""
	}
	return *s.Bucket
}

// ListObjectExt is a list entry returned by list-type=ext-v1, including per-object metadata.
// In the original s3ext fork this field lived on the global s3.Object type; geesefs only
// unmarshals it from ext-v1 responses (see ListObjectsV1ExtOutput).
type ListObjectExt struct {
	ETag *string `type:"string"`

	Key *string `min:"1" type:"string"`

	LastModified *time.Time `type:"timestamp"`

	Owner *s3.Owner `type:"structure"`

	Size *int64 `type:"integer"`

	StorageClass *string `type:"string" enum:"ObjectStorageClass"`

	UserMetadata map[string]*string `locationName:"Metadata" locationNameKey:"Name" locationNameValue:"Value" type:"map" flattened:"true"`
}

// ListObjectsV1ExtOutput carries list-type=ext-v1 results including per-object metadata.
type ListObjectsV1ExtOutput struct {
	_ struct{} `type:"structure"`

	CommonPrefixes []*s3.CommonPrefix `type:"list" flattened:"true"`

	Contents []*ListObjectExt `type:"list" flattened:"true"`

	ContinuationToken *string `type:"string"`

	Delimiter *string `type:"string"`

	EncodingType *string `type:"string" enum:"EncodingType"`

	IsTruncated *bool `type:"boolean"`

	KeyCount *int64 `type:"integer"`

	MaxKeys *int64 `type:"integer"`

	Name *string `type:"string"`

	NextContinuationToken *string `type:"string"`

	Prefix *string `type:"string"`

	StartAfter *string `type:"string"`
}

// PatchObjectInput is the input for the Yandex PatchObject (HTTP PATCH) operation.
type PatchObjectInput struct {
	_ struct{} `locationName:"PatchObjectRequest" type:"structure" payload:"Body"`

	Body io.ReadSeeker `type:"blob"`

	Bucket *string `location:"uri" locationName:"Bucket" type:"string" required:"true"`

	ContentLength *int64 `location:"header" locationName:"Content-Length" type:"long"`

	ContentRange *string `location:"header" locationName:"Content-Range" type:"string" required:"true"`

	IfMatch *string `location:"header" locationName:"If-Match" type:"string"`

	IfUnmodifiedSince *time.Time `location:"header" locationName:"If-Unmodified-Since" type:"timestamp"`

	Key *string `location:"uri" locationName:"Key" min:"1" type:"string" required:"true"`

	PatchAppendPartSize *int64 `location:"header" locationName:"X-Yc-S3-Patch-Append-Part-Size" type:"integer"`
}

func (s *PatchObjectInput) getBucket() string {
	if s.Bucket == nil {
		return ""
	}
	return *s.Bucket
}

// PatchObjectOutput is the output for the PatchObject operation.
type PatchObjectOutput struct {
	_ struct{} `type:"structure"`

	Object *PatchedObjectInfo `type:"structure"`
}

// PatchedObjectInfo describes the object after a successful patch.
type PatchedObjectInfo struct {
	_ struct{} `type:"structure"`

	ETag *string `type:"string"`

	LastModified *time.Time `type:"timestamp"`
}
