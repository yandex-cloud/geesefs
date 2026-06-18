package ycs3ext

import (
	"github.com/aws/aws-sdk-go/aws/awserr"
)

// Standard AWS API error codes returned when list-type=ext-v1 is not supported.
// The s3 package only defines bucket/object-specific codes (see service/s3/errors.go).
const (
	ErrCodeInvalidArgument = "InvalidArgument"
	ErrCodeNotImplemented  = "NotImplemented"
)

// IsUnsupportedListV1Ext reports whether err means the backend does not implement ext-v1 listing.
func IsUnsupportedListV1Ext(err error) bool {
	awsErr, ok := err.(awserr.Error)
	if !ok {
		return false
	}
	switch awsErr.Code() {
	case ErrCodeInvalidArgument, ErrCodeNotImplemented:
		return true
	default:
		return false
	}
}
