package core

import (
	"encoding/xml"
	"fmt"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sort"
	"sync"
	"testing"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

func TestS3CopyBlobMetadataSelfCopy(t *testing.T) {
	const size = uint64(100 * 1024 * 1024)
	for _, tc := range []struct {
		name        string
		copyError   string
		failHead    bool
		failPart    bool
		failCommit  bool
		withoutHint bool
		wantErr     bool
		wantMPU     bool
	}{
		{name: "supported self-copy remains a single request"},
		{name: "oversized self-copy uses multipart", copyError: "EntityTooLarge", wantMPU: true},
		{name: "missing hints preserve source metadata", copyError: "EntityTooLarge", withoutHint: true, wantMPU: true},
		{name: "access denial is not retried as multipart", copyError: "AccessDenied", wantErr: true},
		{name: "missing source fails without upload", copyError: "EntityTooLarge", failHead: true, wantErr: true},
		{name: "part failure aborts owned upload", copyError: "EntityTooLarge", failPart: true, wantMPU: true, wantErr: true},
		{name: "completion failure aborts owned upload", copyError: "EntityTooLarge", failCommit: true, wantMPU: true, wantErr: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var mu sync.Mutex
			var copies, heads, starts, completes, aborts int
			var ranges []string
			var pending map[string]string
			stored := map[string]string{"mtime": "100", "custom": "retained"}
			wanted := map[string]string{"mtime": "200", "custom": "retained"}
			if tc.withoutHint {
				wanted = map[string]string{"mtime": "100", "custom": "retained"}
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				mu.Lock()
				defer mu.Unlock()
				failure := func(status int, code string) {
					w.WriteHeader(status)
					fmt.Fprintf(w, "<Error><Code>%s</Code><Message>fixture</Message></Error>", code)
				}
				if r.URL.Path != "/testbucket/object.iso" {
					t.Errorf("unexpected object path: %s", r.URL.Path)
					failure(http.StatusBadRequest, "InvalidRequest")
					return
				}
				query := r.URL.Query()
				switch {
				case r.Method == http.MethodHead:
					heads++
					if tc.failHead {
						failure(http.StatusNotFound, "NoSuchKey")
						return
					}
					w.Header().Set("Content-Length", fmt.Sprint(size))
					w.Header().Set("ETag", `"original"`)
					w.Header().Set("Last-Modified", "Mon, 01 Jan 2024 00:00:00 GMT")
					w.Header().Set("X-Amz-Storage-Class", "STANDARD_IA")
					w.Header().Set("X-Amz-Meta-Mtime", "100")
					w.Header().Set("X-Amz-Meta-Custom", "retained")
				case r.Method == http.MethodPost && query.Has("uploads"):
					starts++
					pending = map[string]string{"mtime": r.Header.Get("X-Amz-Meta-Mtime"), "custom": r.Header.Get("X-Amz-Meta-Custom")}
					if r.Header.Get("X-Amz-Storage-Class") != "STANDARD_IA" {
						t.Error("multipart copy lost source storage class")
					}
					fmt.Fprint(w, "<InitiateMultipartUploadResult><UploadId>upload</UploadId></InitiateMultipartUploadResult>")
				case r.Method == http.MethodPut && query.Get("uploadId") == "upload":
					ranges = append(ranges, r.Header.Get("X-Amz-Copy-Source-Range"))
					if r.Header.Get("X-Amz-Copy-Source") != "testbucket/object.iso" || r.Header.Get("X-Amz-Copy-Source-If-Match") != `"original"` {
						t.Error("multipart copy did not pin the original source/ETag")
					}
					if tc.failPart {
						failure(http.StatusPreconditionFailed, "PreconditionFailed")
						return
					}
					fmt.Fprint(w, `<CopyPartResult><ETag>"part"</ETag></CopyPartResult>`)
				case r.Method == http.MethodPost && query.Get("uploadId") == "upload":
					completes++
					var body struct {
						Parts []struct {
							Number int    `xml:"PartNumber"`
							ETag   string `xml:"ETag"`
						} `xml:"Part"`
					}
					if err := xml.NewDecoder(r.Body).Decode(&body); err != nil || len(body.Parts) != 2 {
						t.Errorf("invalid completed parts: %+v, %v", body, err)
					} else if body.Parts[0].Number != 1 || body.Parts[1].Number != 2 || body.Parts[0].ETag != `"part"` || body.Parts[1].ETag != `"part"` {
						t.Errorf("unexpected completed parts: %+v", body)
					}
					if tc.failCommit {
						failure(http.StatusForbidden, "AccessDenied")
						return
					}
					stored = pending
					fmt.Fprint(w, `<CompleteMultipartUploadResult><ETag>"copied"</ETag></CompleteMultipartUploadResult>`)
				case r.Method == http.MethodDelete && query.Get("uploadId") == "upload":
					aborts++
					w.WriteHeader(http.StatusNoContent)
				case r.Method == http.MethodPut && len(query) == 0:
					copies++
					if tc.copyError != "" {
						status := http.StatusBadRequest
						if tc.copyError == "AccessDenied" {
							status = http.StatusForbidden
						}
						failure(status, tc.copyError)
						return
					}
					stored = map[string]string{"mtime": r.Header.Get("X-Amz-Meta-Mtime"), "custom": r.Header.Get("X-Amz-Meta-Custom")}
					fmt.Fprint(w, `<CopyObjectResult><ETag>"copied"</ETag></CopyObjectResult>`)
				default:
					t.Errorf("unexpected request: %s %s", r.Method, r.URL)
					failure(http.StatusBadRequest, "InvalidRequest")
				}
			}))
			defer server.Close()
			s, err := NewS3("testbucket", &cfg.FlagStorage{Endpoint: server.URL, MaxParallelParts: 2}, (&cfg.S3Config{
				Region: "us-east-1", AccessKey: "test", SecretKey: "test",
				// The service's single-copy limit can be below the configured threshold.
				MultipartCopyThreshold: 256 * 1024 * 1024,
			}).Init())
			if err != nil {
				t.Fatal(err)
			}
			in := &CopyBlobInput{Source: "object.iso", Destination: "object.iso"}
			if !tc.withoutHint {
				in.Size = PUInt64(size)
				in.ETag = PString(`"original"`)
				in.Metadata = map[string]*string{"mtime": PString("200"), "custom": PString("retained")}
			}
			_, err = s.CopyBlob(in)
			if (err != nil) != tc.wantErr {
				t.Fatalf("CopyBlob error = %v, want error %v", err, tc.wantErr)
			}
			mu.Lock()
			defer mu.Unlock()
			if copies != 1 {
				t.Errorf("single-copy attempts = %d, want 1", copies)
			}
			if tc.wantMPU {
				sort.Strings(ranges)
				if heads != 1 || starts != 1 || !reflect.DeepEqual(ranges, []string{"bytes=0-52428799", "bytes=52428800-104857599"}) {
					t.Errorf("invalid multipart flow: heads=%d starts=%d ranges=%v", heads, starts, ranges)
				}
				wantAborts, wantCompletes := 0, 1
				if tc.wantErr {
					wantAborts = 1
				}
				if tc.failPart {
					wantCompletes = 0
				}
				if aborts != wantAborts || completes != wantCompletes {
					t.Errorf("aborts/completes = %d/%d, want %d/%d", aborts, completes, wantAborts, wantCompletes)
				}
			} else {
				wantHeads := 0
				if tc.failHead {
					wantHeads = 1
				}
				if starts != 0 || completes != 0 || aborts != 0 || heads != wantHeads {
					t.Errorf("unexpected multipart activity: heads=%d (want %d) starts=%d completes=%d aborts=%d", heads, wantHeads, starts, completes, aborts)
				}
			}
			if tc.wantErr {
				wanted = map[string]string{"mtime": "100", "custom": "retained"}
			}
			if !reflect.DeepEqual(stored, wanted) {
				t.Errorf("stored metadata = %v, want %v", stored, wanted)
			}
		})
	}
}
