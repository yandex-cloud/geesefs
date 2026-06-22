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
