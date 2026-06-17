// Package ycs3ext adds Yandex Cloud S3 API extensions on top of the vanilla AWS SDK.
package ycs3ext

import (
	"io"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/service/s3"
)

// S3Client wraps the vanilla S3 client with Yandex-specific operations.
type S3Client struct {
	*s3.S3
}

// NewS3Client wraps an existing S3 client.
func NewS3Client(svc *s3.S3) *S3Client {
	return &S3Client{S3: svc}
}

// AddUnsignedPayloadHandler sets X-Amz-Content-Sha256 to UNSIGNED-PAYLOAD when
// S3DisableContentMD5Validation is enabled (--no-checksum). Call after any
// handler setup that may clear the Sign chain (IAM, v2 signer).
func AddUnsignedPayloadHandler(client *S3Client) {
	client.Handlers.Sign.PushFrontNamed(request.NamedHandler{
		Name: "ycs3ext.UnsignedPayload",
		Fn: func(req *request.Request) {
			if aws.BoolValue(req.Config.S3DisableContentMD5Validation) {
				req.HTTPRequest.Header.Set("X-Amz-Content-Sha256", "UNSIGNED-PAYLOAD")
			}
		},
	})
}

// ListObjectsV1ExtInput is layout-compatible with ListObjectsV2Input.
type ListObjectsV1ExtInput s3.ListObjectsV2Input

func (s *ListObjectsV1ExtInput) getBucket() string {
	if s.Bucket == nil {
		return ""
	}
	return *s.Bucket
}

// Object extends the S3 list entry with user metadata returned by list-type=ext-v1.
type Object struct {
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

	Contents []*Object `type:"list" flattened:"true"`

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

// ToListObjectsV2Output converts an ext-v1 list result to the standard SDK type.
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

// ListObjectsV1ExtRequest lists objects using Yandex list-type=ext-v1.
func (c *S3Client) ListObjectsV1ExtRequest(input *ListObjectsV1ExtInput) (*request.Request, *ListObjectsV1ExtOutput) {
	op := &request.Operation{
		Name:       "ListObjectsV1Ext",
		HTTPMethod: "GET",
		HTTPPath:   "/{Bucket}?list-type=ext-v1",
	}
	if input == nil {
		input = &ListObjectsV1ExtInput{}
	}
	output := &ListObjectsV1ExtOutput{}
	req := c.S3.NewRequest(op, input, output)
	return req, output
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

// Validate inspects the fields of the type to determine if they are valid.
func (s *PatchObjectInput) Validate() error {
	invalidParams := request.ErrInvalidParams{Context: "PatchObjectInput"}
	if s.Bucket == nil {
		invalidParams.Add(request.NewErrParamRequired("Bucket"))
	} else if len(*s.Bucket) < 1 {
		invalidParams.Add(request.NewErrParamMinLen("Bucket", 1))
	}
	if s.ContentRange == nil {
		invalidParams.Add(request.NewErrParamRequired("ContentRange"))
	}
	if s.Key == nil {
		invalidParams.Add(request.NewErrParamRequired("Key"))
	} else if len(*s.Key) < 1 {
		invalidParams.Add(request.NewErrParamMinLen("Key", 1))
	}
	if invalidParams.Len() > 0 {
		return invalidParams
	}
	return nil
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

// PatchObjectRequest patches an object using Yandex Cloud's PatchObject API.
func (c *S3Client) PatchObjectRequest(input *PatchObjectInput) (*request.Request, *PatchObjectOutput) {
	op := &request.Operation{
		Name:       "PatchObject",
		HTTPMethod: "PATCH",
		HTTPPath:   "/{Bucket}/{Key+}",
	}
	if input == nil {
		input = &PatchObjectInput{}
	}
	output := &PatchObjectOutput{}
	req := c.S3.NewRequest(op, input, output)
	return req, output
}
