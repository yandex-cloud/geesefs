package core

import (
	"testing"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/yandex-cloud/geesefs/core/cfg"
)

func TestUpdateFromFlushETag(t *testing.T) {
	str := func(s string) *string { return &s }

	cases := []struct {
		description      string
		prevETag         string
		etag             *string
		storageClass     *string
		size             uint64
		expectedETag     string
		expectedMetaETag *string // nil means the "etag" metadata key is absent
		expectedSize     uint64
	}{
		// Positive: the backend returned an ETag.
		{
			description:      "positive: returned ETag is stored",
			etag:             str(`"abc"`),
			size:             10,
			expectedETag:     `"abc"`,
			expectedMetaETag: str(`"abc"`),
			expectedSize:     10,
		},
		{
			description:      "positive: multipart ETag is stored",
			etag:             str(`"abc-3"`),
			size:             15 << 20,
			expectedETag:     `"abc-3"`,
			expectedMetaETag: str(`"abc-3"`),
			expectedSize:     15 << 20,
		},
		// Negative: the backend returned no ETag header.
		{
			description:      "negative: missing ETag does not panic and leaves it unknown",
			etag:             nil,
			size:             10,
			expectedETag:     "",
			expectedMetaETag: nil,
			expectedSize:     10,
		},
		{
			description:      "negative: missing ETag still records the storage class and size",
			etag:             nil,
			storageClass:     str("STANDARD"),
			size:             20,
			expectedETag:     "",
			expectedMetaETag: nil,
			expectedSize:     20,
		},
		// Boundary: empty ETag and empty object.
		{
			description:      "boundary: empty ETag is stored as empty",
			etag:             str(""),
			size:             10,
			expectedETag:     "",
			expectedMetaETag: str(""),
			expectedSize:     10,
		},
		{
			description:      "boundary: zero size with an ETag",
			etag:             str(`"d41d8cd98f00b204e9800998ecf8427e"`),
			size:             0,
			expectedETag:     `"d41d8cd98f00b204e9800998ecf8427e"`,
			expectedMetaETag: str(`"d41d8cd98f00b204e9800998ecf8427e"`),
			expectedSize:     0,
		},
		// Corner: a previous ETag is replaced or cleared, never kept stale.
		{
			description:      "corner: new ETag replaces the previous one",
			prevETag:         `"old"`,
			etag:             str(`"new"`),
			size:             10,
			expectedETag:     `"new"`,
			expectedMetaETag: str(`"new"`),
			expectedSize:     10,
		},
		{
			description:      "corner: missing ETag clears the previous one",
			prevETag:         `"old"`,
			etag:             nil,
			size:             10,
			expectedETag:     "",
			expectedMetaETag: nil,
			expectedSize:     10,
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			inode := &Inode{
				fs: &Goofys{
					flags:        &cfg.FlagStorage{},
					inodesByTime: make(map[int64]map[fuseops.InodeID]bool),
				},
				s3Metadata: make(map[string][]byte),
				knownETag:  tc.prevETag,
			}
			if tc.prevETag != "" {
				inode.s3Metadata["etag"] = []byte(tc.prevETag)
			}

			var panicked interface{}
			func() {
				defer func() { panicked = recover() }()
				inode.updateFromFlush(tc.size, tc.etag, nil, tc.storageClass)
			}()
			if panicked != nil {
				t.Fatalf("updateFromFlush panicked: %v", panicked)
			}

			if inode.knownETag != tc.expectedETag {
				t.Errorf("knownETag = %q, expected %q", inode.knownETag, tc.expectedETag)
			}
			meta, ok := inode.s3Metadata["etag"]
			if tc.expectedMetaETag == nil && ok {
				t.Errorf("etag metadata = %q, expected it to be absent", meta)
			} else if tc.expectedMetaETag != nil && (!ok || string(meta) != *tc.expectedMetaETag) {
				t.Errorf("etag metadata = %q (present: %v), expected %q", meta, ok, *tc.expectedMetaETag)
			}
			if inode.knownSize != tc.expectedSize {
				t.Errorf("knownSize = %d, expected %d", inode.knownSize, tc.expectedSize)
			}
			if tc.storageClass != nil && string(inode.s3Metadata["storage-class"]) != *tc.storageClass {
				t.Errorf("storage-class = %q, expected %q", inode.s3Metadata["storage-class"], *tc.storageClass)
			}
		})
	}
}
