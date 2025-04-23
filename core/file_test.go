package core

import (
	"testing"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

func TestShouldRetrieveHash(t *testing.T) {
	tests := []struct {
		name     string
		fileSize int64
		isDir    bool
		metadata map[string]string
		expected bool
	}{
		{
			name:     "No metadata, large file",
			fileSize: 10 * 1024 * 1024, // 10 MB
			isDir:    false,
			metadata: nil,
			expected: true,
		},
		{
			name:     "No metadata, small file",
			fileSize: 512 * 1024, // 512 KB
			isDir:    false,
			metadata: nil,
			expected: false,
		},
		{
			name:     "Has metadata",
			fileSize: 10 * 1024 * 1024, // 10 MB
			isDir:    false,
			metadata: map[string]string{"content-sha256": "abc123"},
			expected: false,
		},
		{
			name:     "Is directory",
			fileSize: 0,
			isDir:    true,
			metadata: nil,
			expected: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var metadata map[string][]byte = nil

			if tt.metadata != nil {
				metadata = make(map[string][]byte)
				for k, v := range tt.metadata {
					metadata[k] = []byte(v)
				}
			}

			var dir *DirInodeData = nil
			if tt.isDir {
				dir = &DirInodeData{}
			}

			fh := &FileHandle{
				inode: &Inode{
					Attributes:   InodeAttributes{Size: uint64(tt.fileSize)},
					userMetadata: metadata,
					fs: &Goofys{
						flags: cfg.DefaultFlags(),
					},
					dir: dir,
				},
			}

			if got := fh.shouldRetrieveHash(); got != tt.expected {
				t.Errorf("shouldRetrieveHash() = %v, want %v", got, tt.expected)
			}
		})
	}
}
