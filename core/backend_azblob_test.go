package core

import (
	"encoding/base64"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/yandex-cloud/geesefs/core/cfg"
)

func TestAzPutAccessConditions(t *testing.T) {
	star := "*"
	etag := `"abc123"`

	tests := []struct {
		name      string
		in        *PutBlobInput
		wantMatch azblob.ETag
		wantNone  azblob.ETag
	}{
		{"none", &PutBlobInput{}, azblob.ETagNone, azblob.ETagNone},
		{"IfNoneMatch wildcard", &PutBlobInput{IfNoneMatch: &star}, azblob.ETagNone, azblob.ETagAny},
		{"IfNoneMatch etag", &PutBlobInput{IfNoneMatch: &etag}, azblob.ETagNone, azblob.ETag(etag)},
		{"IfMatch", &PutBlobInput{IfMatch: &etag}, azblob.ETag(etag), azblob.ETagNone},
		{"IfNoneMatch wins over IfMatch", &PutBlobInput{IfNoneMatch: &star, IfMatch: &etag}, azblob.ETagNone, azblob.ETagAny},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			mod := azPutAccessConditions(tc.in).ModifiedAccessConditions
			if mod.IfMatch != tc.wantMatch {
				t.Fatalf("IfMatch = %q, want %q", mod.IfMatch, tc.wantMatch)
			}
			if mod.IfNoneMatch != tc.wantNone {
				t.Fatalf("IfNoneMatch = %q, want %q", mod.IfNoneMatch, tc.wantNone)
			}
		})
	}
}

func TestAzPutBlobTags(t *testing.T) {
	got := azPutBlobTags(&PutBlobInput{Tags: map[string]string{"geesefs-lock": "true"}})
	if len(got) != 1 || got["geesefs-lock"] != "true" {
		t.Fatalf("got %v", got)
	}
	hdr := azblob.SerializeBlobTagsHeader(got)
	if hdr == nil || *hdr != "geesefs-lock=true" {
		t.Fatalf("SerializeBlobTagsHeader = %v", hdr)
	}
	if len(azPutBlobTags(&PutBlobInput{})) != 0 {
		t.Fatal("expected empty tags")
	}
}

// newTestAZBlob returns an AZBlob backend that talks to an httptest server.
// DELETE requests for keys starting with "fail" get 403 AuthorizationFailure,
// keys starting with "gone" get 404 BlobNotFound, and every other key is
// deleted successfully.
func newTestAZBlob(t testing.TB) *AZBlob {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/testbucket/fail"):
			w.Header().Set("x-ms-error-code", "AuthorizationFailure")
			w.WriteHeader(http.StatusForbidden)
		case r.Method == http.MethodDelete && strings.Contains(r.URL.Path, "/testbucket/gone"):
			w.Header().Set("x-ms-error-code", "BlobNotFound")
			w.WriteHeader(http.StatusNotFound)
		default:
			w.WriteHeader(http.StatusAccepted)
		}
	}))
	t.Cleanup(srv.Close)

	b, err := NewAZBlob("testbucket", &cfg.AZBlobConfig{
		Endpoint:    srv.URL + "/devstoreaccount1/",
		AccountName: "devstoreaccount1",
		AccountKey:  base64.StdEncoding.EncodeToString([]byte("test")),
	})
	if err != nil {
		t.Fatalf("NewAZBlob: %v", err)
	}
	return b
}

func azTestKeys(ok, fail, gone int, suffix string) []string {
	keys := make([]string, 0, ok+fail+gone)
	for i := 0; i < ok; i++ {
		keys = append(keys, fmt.Sprintf("ok%d%s", i, suffix))
	}
	for i := 0; i < fail; i++ {
		keys = append(keys, fmt.Sprintf("fail%d%s", i, suffix))
	}
	for i := 0; i < gone; i++ {
		keys = append(keys, fmt.Sprintf("gone%d%s", i, suffix))
	}
	return keys
}

// TestAZBlobDeleteBlobsConcurrentErrors runs DeleteBlobs, which deletes every
// key in its own goroutine. Run it with -race: before the fix, the goroutines
// wrote the shared error without a lock while the loop that starts them read
// it, and the race detector failed the test whenever a delete failed while
// other keys were still being started.
func TestAZBlobDeleteBlobsConcurrentErrors(t *testing.T) {
	tests := []struct {
		description    string
		ok, fail, gone int
		suffix         string
		expected       error
	}{
		{"positive: all deletes succeed", 16, 0, 0, "", nil},
		{"positive: missing blobs are not an error", 8, 0, 8, "", nil},
		{"negative: one delete fails among many", 15, 1, 0, "", syscall.EACCES},
		{"negative: every delete fails", 0, 16, 0, "", syscall.EACCES},
		{"negative: half of the deletes fail", 8, 8, 0, "", syscall.EACCES},
		{"boundary: no keys", 0, 0, 0, "", nil},
		{"boundary: one key that succeeds", 1, 0, 0, "", nil},
		{"boundary: one key that fails", 0, 1, 0, "", syscall.EACCES},
		{"boundary: two keys that both fail", 0, 2, 0, "", syscall.EACCES},
		{"corner: failures mixed with missing blobs", 0, 8, 8, "", syscall.EACCES},
		{"corner: directory keys with a trailing slash fail", 0, 16, 0, "/", syscall.EACCES},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			b := newTestAZBlob(t)
			_, err := b.DeleteBlobs(&DeleteBlobsInput{Items: azTestKeys(tc.ok, tc.fail, tc.gone, tc.suffix)})
			if err != tc.expected {
				t.Fatalf("DeleteBlobs(%d ok, %d fail, %d gone) error = %v, want %v",
					tc.ok, tc.fail, tc.gone, err, tc.expected)
			}
		})
	}
}

func BenchmarkAZBlobDeleteBlobs(b *testing.B) {
	for _, bc := range []struct {
		name           string
		ok, fail, gone int
	}{
		{"16ok", 16, 0, 0},
		{"16gone", 0, 0, 16},
	} {
		b.Run(bc.name, func(b *testing.B) {
			az := newTestAZBlob(b)
			keys := azTestKeys(bc.ok, bc.fail, bc.gone, "")
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				az.DeleteBlobs(&DeleteBlobsInput{Items: keys})
			}
		})
	}
}
