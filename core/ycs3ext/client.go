// Package ycs3ext adds Yandex Cloud S3 API extensions on top of the vanilla AWS SDK.
package ycs3ext

import (
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

// ListObjectsV1Ext sends a list-type=ext-v1 request.
func (c *S3Client) ListObjectsV1Ext(input *ListObjectsV1ExtInput) (*ListObjectsV1ExtOutput, *request.Request, error) {
	req, output := c.ListObjectsV1ExtRequest(input)
	err := req.Send()
	return output, req, err
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
