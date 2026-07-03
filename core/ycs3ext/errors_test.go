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
	"errors"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
)

func TestIsUnsupportedListV1Ext(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{nil, false},
		{errors.New("other"), false},
		{awserr.New("AccessDenied", "msg", nil), false},
		{awserr.New(ErrCodeInvalidArgument, "msg", nil), true},
		{awserr.New(ErrCodeNotImplemented, "msg", nil), true},
	}
	for _, tc := range cases {
		if got := IsUnsupportedListV1Ext(tc.err); got != tc.want {
			t.Fatalf("IsUnsupportedListV1Ext(%v) = %v, want %v", tc.err, got, tc.want)
		}
	}
}

func TestListBlobItemsFromV1Ext(t *testing.T) {
	items := ListBlobItemsFromV1Ext(&ListObjectsV1ExtOutput{
		Contents: []*ListObjectExt{
			{
				Key:  nil,
				Size: nil,
			},
			{
				Key:  aws.String("k"),
				Size: aws.Int64(10),
				UserMetadata: map[string]*string{
					"x": aws.String("y"),
				},
			},
		},
	})
	want := []ListBlobItem{
		{Metadata: map[string]*string{}},
		{Key: aws.String("k"), Size: 10, Metadata: map[string]*string{"x": aws.String("y")}},
	}
	if !reflect.DeepEqual(items, want) {
		t.Fatalf("items mismatch:\ngot:  %#v\nwant: %#v", items, want)
	}
}
