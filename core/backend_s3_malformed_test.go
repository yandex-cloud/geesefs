package core

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// Throttling sometimes comes back as a 200 with an <Error> body instead of a
// real listing. The request looks successful to the SDK, so it returns no error
// and IsTruncated is nil; the old code dereferenced it and took the mount down
// with a SIGSEGV. Return an error here so read-retry can deal with it.
func TestListBlobsMalformed200(t *testing.T) {
	const errBody = `<?xml version="1.0" encoding="UTF-8"?>
<Error><Code>ServiceUnavailable</Code><Message>Reduce your request rate.</Message></Error>`

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/xml")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(errBody))
	}))
	defer srv.Close()

	s3, err := NewS3("test-bucket", &cfg.FlagStorage{Endpoint: srv.URL}, &cfg.S3Config{
		Region:    "us-east-1",
		AccessKey: "ak",
		SecretKey: "sk",
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = s3.ListBlobs(&ListBlobsInput{})
	if err == nil {
		t.Fatal("expected an error for a 200 response with a missing IsTruncated, got nil")
	}
	if !strings.Contains(err.Error(), "IsTruncated") {
		t.Fatalf("unexpected error: %v", err)
	}
}
