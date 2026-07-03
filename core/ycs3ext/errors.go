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
	"github.com/aws/aws-sdk-go/aws/awserr"
)

// Standard AWS API error codes returned when list-type=ext-v1 is not supported.
// The s3 package only defines bucket/object-specific codes (see service/s3/errors.go).
const (
	ErrCodeInvalidArgument = "InvalidArgument"
	ErrCodeNotImplemented  = "NotImplemented"

	HeaderAmzContentSHA256 = "X-Amz-Content-Sha256"
	UnsignedPayload        = "UNSIGNED-PAYLOAD"
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
	}
	return false
}
