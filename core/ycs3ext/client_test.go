package ycs3ext

import (
	"net/http"
	"reflect"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/client/metadata"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/aws/aws-sdk-go/aws/session"
	"github.com/aws/aws-sdk-go/service/s3"
)

func TestPatchObjectInputValidate(t *testing.T) {
	err := (&PatchObjectInput{}).Validate()
	if err == nil {
		t.Fatal("expected validation error for empty input")
	}

	err = (&PatchObjectInput{
		Bucket:       aws.String("b"),
		Key:          aws.String("k"),
		ContentRange: aws.String("bytes 0-0/*"),
	}).Validate()
	if err != nil {
		t.Fatalf("unexpected validation error: %v", err)
	}
}

func TestToListObjectsV2OutputCopiesContents(t *testing.T) {
	ext := &ListObjectsV1ExtOutput{
		IsTruncated: aws.Bool(false),
		Contents: []*ListObjectExt{
			{
				Key:  aws.String("a"),
				Size: aws.Int64(42),
				UserMetadata: map[string]*string{
					"foo": aws.String("bar"),
				},
			},
		},
	}
	out := ToListObjectsV2Output(ext)
	want := &s3.ListObjectsV2Output{
		IsTruncated: aws.Bool(false),
		Contents: []*s3.Object{
			{Key: aws.String("a"), Size: aws.Int64(42)},
		},
	}
	if !reflect.DeepEqual(out, want) {
		t.Fatalf("output mismatch:\ngot:  %#v\nwant: %#v", out, want)
	}
}

func TestAddUnsignedPayloadHandler(t *testing.T) {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
	client := NewS3Client(s3.New(sess))
	AddUnsignedPayloadHandler(client)

	req := &request.Request{
		HTTPRequest: mustNewRequest(t, "PUT", "http://example.com/bucket/key"),
		Config:      aws.Config{S3DisableContentMD5Validation: aws.Bool(true)},
		ClientInfo:  metadata.ClientInfo{SigningName: "s3"},
	}
	runUnsignedPayloadHandler(t, client, req)
	if got := req.HTTPRequest.Header.Get(HeaderAmzContentSHA256); got != UnsignedPayload {
		t.Fatalf("header = %q, want %q", got, UnsignedPayload)
	}

	req.Config = aws.Config{S3DisableContentMD5Validation: aws.Bool(false)}
	req.HTTPRequest.Header.Del(HeaderAmzContentSHA256)
	runUnsignedPayloadHandler(t, client, req)
	if got := req.HTTPRequest.Header.Get(HeaderAmzContentSHA256); got != "" {
		t.Fatalf("header = %q, want empty when checksum enabled", got)
	}
}

func TestPatchObjectRequestBuildsOperation(t *testing.T) {
	sess := session.Must(session.NewSession(&aws.Config{Region: aws.String("us-east-1")}))
	client := NewS3Client(s3.New(sess))

	req, out := client.PatchObjectRequest(&PatchObjectInput{
		Bucket:       aws.String("b"),
		Key:          aws.String("k"),
		ContentRange: aws.String("bytes 0-0/*"),
	})

	wantOp := struct {
		Name       string
		HTTPMethod string
	}{"PatchObject", "PATCH"}
	gotOp := struct {
		Name       string
		HTTPMethod string
	}{req.Operation.Name, req.Operation.HTTPMethod}

	if !reflect.DeepEqual(gotOp, wantOp) {
		t.Fatalf("operation mismatch:\ngot:  %#v\nwant: %#v", gotOp, wantOp)
	}
	if out == nil {
		t.Fatal("expected output object")
	}
}

func mustNewRequest(t *testing.T, method, url string) *http.Request {
	t.Helper()
	r, err := http.NewRequest(method, url, nil)
	if err != nil {
		t.Fatal(err)
	}
	return r
}

func runUnsignedPayloadHandler(t *testing.T, client *S3Client, req *request.Request) {
	t.Helper()
	sign := client.Handlers.Sign
	sign.AfterEachFn = func(item request.HandlerListRunItem) bool {
		return item.Handler.Name != "ycs3ext.UnsignedPayload"
	}
	sign.Run(req)
	sign.AfterEachFn = nil
}
