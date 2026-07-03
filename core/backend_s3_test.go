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

package core

import (
	"bytes"
	"net/http"
	"net/http/httptest"
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
