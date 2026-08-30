package core

import (
	"bytes"
	"io"
	"net/http"
	"net/http/httptest"
	"syscall"
	"testing"

	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/yandex-cloud/geesefs/core/cfg"
)

func TestApplyS3PutConditions(t *testing.T) {
	star, etag := "*", `"abc123"`

	tests := []struct {
		name          string
		in            *PutBlobInput
		wantMatch     string
		wantNoneMatch string
	}{
		{"none", &PutBlobInput{}, "", ""},
		{"IfNoneMatch", &PutBlobInput{IfNoneMatch: &star}, "", star},
		{"IfMatch", &PutBlobInput{IfMatch: &etag}, etag, ""},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			req := &request.Request{HTTPRequest: httptest.NewRequest(http.MethodPut, "http://example/", nil)}
			applyS3PutConditions(req, tc.in)
			if got := req.HTTPRequest.Header.Get("If-Match"); got != tc.wantMatch {
				t.Fatalf("If-Match = %q, want %q", got, tc.wantMatch)
			}
			if got := req.HTTPRequest.Header.Get("If-None-Match"); got != tc.wantNoneMatch {
				t.Fatalf("If-None-Match = %q, want %q", got, tc.wantNoneMatch)
			}
		})
	}
}

// TestS3PutBlobConditionalHeaders checks that PutBlob wires applyS3PutConditions
func TestS3PutBlobConditionalHeaders(t *testing.T) {
	var ifNoneMatch string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ifNoneMatch = r.Header.Get("If-None-Match")
		w.Header().Set("ETag", `"test-etag"`)
		w.Header().Set("Date", "Mon, 01 Jan 2024 00:00:00 GMT")
	}))
	defer srv.Close()

	s, err := NewS3("testbucket", &cfg.FlagStorage{Endpoint: srv.URL}, (&cfg.S3Config{
		Region:    "us-east-1",
		AccessKey: "test",
		SecretKey: "test",
	}).Init())
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}

	star := "*"
	size := uint64(4)
	if _, err := s.PutBlob(&PutBlobInput{
		Key:         "obj",
		Body:        bytes.NewReader([]byte("data")),
		Size:        &size,
		IfNoneMatch: &star,
	}); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	if ifNoneMatch != star {
		t.Fatalf("If-None-Match = %q, want %q", ifNoneMatch, star)
	}
}

func TestS3GetBlobConditionalHeader(t *testing.T) {
	const etag = `"test-etag"`
	var ifMatch string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		ifMatch = r.Header.Get("If-Match")
		w.Header().Set("ETag", etag)
		w.Header().Set("Date", "Mon, 01 Jan 2024 00:00:00 GMT")
		_, _ = io.WriteString(w, "data")
	}))
	defer srv.Close()

	s, err := NewS3("testbucket", &cfg.FlagStorage{Endpoint: srv.URL}, (&cfg.S3Config{
		Region:    "us-east-1",
		AccessKey: "test",
		SecretKey: "test",
	}).Init())
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}

	resp, err := s.GetBlob(&GetBlobInput{
		Key:     "obj",
		Count:   4,
		IfMatch: PString(etag),
	})
	if err != nil {
		t.Fatalf("GetBlob: %v", err)
	}
	defer resp.Body.Close()
	if ifMatch != etag {
		t.Fatalf("If-Match = %q, want %q", ifMatch, etag)
	}
}

func TestS3GetBlobPreconditionFailed(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusPreconditionFailed)
	}))
	defer srv.Close()

	s, err := NewS3("testbucket", &cfg.FlagStorage{Endpoint: srv.URL}, (&cfg.S3Config{
		Region:    "us-east-1",
		AccessKey: "test",
		SecretKey: "test",
	}).Init())
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}

	_, err = s.GetBlob(&GetBlobInput{
		Key:     "obj",
		Count:   4,
		IfMatch: PString(`"old"`),
	})
	if err != syscall.ESTALE {
		t.Fatalf("GetBlob error = %v, want %v", err, syscall.ESTALE)
	}
}

func TestShouldNotRetryStaleRead(t *testing.T) {
	if shouldRetry(syscall.ESTALE) {
		t.Fatal("ESTALE must not be retried")
	}
}

func TestS3PutBlobTagging(t *testing.T) {
	var tagging string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		tagging = r.Header.Get("x-amz-tagging")
		w.Header().Set("ETag", `"test-etag"`)
		w.Header().Set("Date", "Mon, 01 Jan 2024 00:00:00 GMT")
	}))
	defer srv.Close()

	s, err := NewS3("testbucket", &cfg.FlagStorage{Endpoint: srv.URL}, (&cfg.S3Config{
		Region:    "us-east-1",
		AccessKey: "test",
		SecretKey: "test",
	}).Init())
	if err != nil {
		t.Fatalf("NewS3: %v", err)
	}

	want := "geesefs-lock=true"
	size := uint64(4)
	if _, err := s.PutBlob(&PutBlobInput{
		Key:  "obj",
		Body: bytes.NewReader([]byte("data")),
		Size: &size,
		Tags: map[string]string{"geesefs-lock": "true"},
	}); err != nil {
		t.Fatalf("PutBlob: %v", err)
	}
	if tagging != want {
		t.Fatalf("x-amz-tagging = %q, want %q", tagging, want)
	}
}

func TestEncodePutBlobTags(t *testing.T) {
	s := encodePutBlobTags(map[string]string{"geesefs-lock": "true"})
	if s == nil || *s != "geesefs-lock=true" {
		t.Fatalf("got %v", s)
	}
	if encodePutBlobTags(nil) != nil {
		t.Fatal("expected nil for empty tags")
	}
}
