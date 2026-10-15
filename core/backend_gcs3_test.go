package core

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"syscall"
	"testing"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// newTestGCS3 returns a GCS3 backend that talks to an httptest server.
// DELETE requests for keys starting with "fail" get the status code in
// failStatus; every other key is deleted successfully.
func newTestGCS3(t testing.TB, failStatus int) *GCS3 {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Date", "Mon, 01 Jan 2024 00:00:00 GMT")
		if r.Method == http.MethodDelete && strings.HasPrefix(r.URL.Path, "/testbucket/fail") {
			w.WriteHeader(failStatus)
			return
		}
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(srv.Close)
	t.Setenv("GOOGLE_APPLICATION_CREDENTIALS", "")

	s, err := NewGCS3("testbucket", &cfg.FlagStorage{Endpoint: srv.URL}, (&cfg.S3Config{
		Region:    "us-east-1",
		AccessKey: "test",
		SecretKey: "test",
	}).Init())
	if err != nil {
		t.Fatalf("NewGCS3: %v", err)
	}
	return s
}

func testKeys(ok, fail int) []string {
	keys := make([]string, 0, ok+fail)
	for i := 0; i < ok; i++ {
		keys = append(keys, fmt.Sprintf("ok%d", i))
	}
	for i := 0; i < fail; i++ {
		keys = append(keys, fmt.Sprintf("fail%d", i))
	}
	return keys
}

// TestGCS3DeleteBlobsConcurrentErrors runs DeleteBlobs, which deletes every
// key in its own goroutine. Run it with -race: before the fix, the goroutines
// wrote the shared error without a lock and the race detector failed the test
// whenever two or more deletes failed.
func TestGCS3DeleteBlobsConcurrentErrors(t *testing.T) {
	tests := []struct {
		description string
		ok, fail    int
		expected    error
	}{
		{"positive: all deletes succeed", 16, 0, nil},
		{"positive: one delete fails among many", 15, 1, syscall.EACCES},
		{"negative: every delete fails", 0, 16, syscall.EACCES},
		{"negative: half of the deletes fail", 8, 8, syscall.EACCES},
		{"boundary: no keys", 0, 0, nil},
		{"boundary: one key that succeeds", 1, 0, nil},
		{"boundary: one key that fails", 0, 1, syscall.EACCES},
		{"boundary: two keys that both fail", 0, 2, syscall.EACCES},
		{"corner: many keys that all fail", 0, 64, syscall.EACCES},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			s := newTestGCS3(t, http.StatusForbidden)
			_, err := s.DeleteBlobs(&DeleteBlobsInput{Items: testKeys(tc.ok, tc.fail)})
			if err != tc.expected {
				t.Fatalf("DeleteBlobs(%d ok, %d fail) error = %v, want %v", tc.ok, tc.fail, err, tc.expected)
			}
		})
	}
}

func BenchmarkGCS3DeleteBlobs(b *testing.B) {
	for _, bc := range []struct {
		name     string
		ok, fail int
	}{
		{"16ok", 16, 0},
		{"16fail", 0, 16},
	} {
		b.Run(bc.name, func(b *testing.B) {
			s := newTestGCS3(b, http.StatusForbidden)
			keys := testKeys(bc.ok, bc.fail)
			b.ReportAllocs()
			b.ResetTimer()
			for i := 0; i < b.N; i++ {
				s.DeleteBlobs(&DeleteBlobsInput{Items: keys})
			}
		})
	}
}
