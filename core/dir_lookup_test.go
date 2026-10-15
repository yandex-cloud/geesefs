package core

import (
	"syscall"
	"testing"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// lookupBackend is a fake backend for LookUpInodeMaybeDir. Objects maps
// a key to the HEAD result; a missing key returns ENOENT. Children lists
// the keys that a LIST of a prefix returns.
type lookupBackend struct {
	objects  map[string]error
	children map[string][]string
	listErr  error
}

func (lb *lookupBackend) backend(dirBlob bool) *TestBackend {
	return &TestBackend{
		capabilities: &Capabilities{Name: "s3", DirBlob: dirBlob},
		HeadBlobFunc: func(param *HeadBlobInput) (*HeadBlobOutput, error) {
			err, ok := lb.objects[param.Key]
			if !ok {
				return nil, syscall.ENOENT
			}
			if err != nil {
				return nil, err
			}
			return &HeadBlobOutput{BlobItemOutput: BlobItemOutput{Key: PString(param.Key)}}, nil
		},
		ListBlobsFunc: func(param *ListBlobsInput) (*ListBlobsOutput, error) {
			if lb.listErr != nil {
				return nil, lb.listErr
			}
			out := &ListBlobsOutput{}
			for _, k := range lb.children[NilStr(param.Prefix)] {
				out.Items = append(out.Items, BlobItemOutput{Key: PString(k)})
			}
			return out, nil
		},
	}
}

func newLookupParent(cloud StorageBackend, flags *cfg.FlagStorage) *Inode {
	fs := &Goofys{flags: flags}
	return &Inode{fs: fs, dir: &DirInodeData{cloud: cloud}}
}

// TestLookUpInodeMaybeDirConcurrent looks up a name with HEAD name, HEAD
// name/ and LIST name/, which run in parallel goroutines unless --cheap is
// set. Run it with -race: before the fix, the goroutines wrote their
// results into shared variables while the waiting loop read all of them
// after receiving only one.
func TestLookUpInodeMaybeDirConcurrent(t *testing.T) {
	type mode struct{ cheap, noDirObject, explicitDir, dirBlob bool }
	tests := []struct {
		description string
		mode        mode
		backend     lookupBackend
		expectedKey string
		expectedErr error
	}{
		{description: "positive: file object is found",
			backend:     lookupBackend{objects: map[string]error{"x": nil}},
			expectedKey: "x"},
		{description: "positive: directory object is found",
			backend:     lookupBackend{objects: map[string]error{"x/": nil}},
			expectedKey: "x/"},
		{description: "positive: implicit directory is found by listing",
			backend:     lookupBackend{children: map[string][]string{"x/": {"x/child"}}},
			expectedKey: "x/child"},
		{description: "negative: nothing exists",
			backend:     lookupBackend{},
			expectedErr: syscall.ENOENT},
		{description: "negative: HEAD of the file is denied",
			backend:     lookupBackend{objects: map[string]error{"x": syscall.EACCES}},
			expectedErr: syscall.EACCES},
		{description: "negative: listing is denied",
			backend:     lookupBackend{listErr: syscall.EACCES},
			expectedErr: syscall.EACCES},
		{description: "boundary: cheap mode runs the requests one by one",
			mode:        mode{cheap: true},
			backend:     lookupBackend{children: map[string][]string{"x/": {"x/child"}}},
			expectedKey: "x/child"},
		{description: "boundary: only the file HEAD with no-dir-object and explicit-dir",
			mode:        mode{noDirObject: true, explicitDir: true},
			backend:     lookupBackend{objects: map[string]error{"x": nil}},
			expectedKey: "x"},
		{description: "boundary: backend with directory blobs uses one HEAD",
			mode:        mode{dirBlob: true},
			backend:     lookupBackend{objects: map[string]error{"x": nil}},
			expectedKey: "x"},
		{description: "corner: listing finds children while the file HEAD is denied",
			backend:     lookupBackend{objects: map[string]error{"x": syscall.EACCES}, children: map[string][]string{"x/": {"x/child"}}},
			expectedKey: "x/child"},
		{description: "corner: listing returns the directory marker itself",
			backend:     lookupBackend{children: map[string][]string{"x/": {"x/"}}},
			expectedKey: "x/"},
		{description: "corner: directory object found while the file HEAD is denied",
			backend:     lookupBackend{objects: map[string]error{"x": syscall.EACCES, "x/": nil}},
			expectedKey: "x/"},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			flags := cfg.DefaultFlags()
			flags.Cheap = tc.mode.cheap
			flags.NoDirObject = tc.mode.noDirObject
			flags.ExplicitDir = tc.mode.explicitDir
			flags.ReadRetryAttempts = 1
			parent := newLookupParent(tc.backend.backend(tc.mode.dirBlob), flags)

			blob, err := parent.LookUpInodeMaybeDir("x")
			if err != tc.expectedErr {
				t.Fatalf("LookUpInodeMaybeDir(x) error = %v, want %v", err, tc.expectedErr)
			}
			got := ""
			if blob != nil {
				got = NilStr(blob.Key)
			}
			if got != tc.expectedKey {
				t.Fatalf("LookUpInodeMaybeDir(x) key = %q, want %q", got, tc.expectedKey)
			}
		})
	}
}

func BenchmarkLookUpInodeMaybeDir(b *testing.B) {
	for _, bc := range []struct {
		name    string
		backend lookupBackend
	}{
		{"file", lookupBackend{objects: map[string]error{"x": nil}}},
		{"missing", lookupBackend{}},
	} {
		b.Run(bc.name, func(b *testing.B) {
			flags := cfg.DefaultFlags()
			parent := newLookupParent(bc.backend.backend(false), flags)
			b.ReportAllocs()
			b.ResetTimer()
			b.RunParallel(func(pb *testing.PB) {
				for pb.Next() {
					parent.LookUpInodeMaybeDir("x")
				}
			})
		})
	}
}
