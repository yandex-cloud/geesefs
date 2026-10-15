package core

import (
	"os"
	"path/filepath"
	"testing"
)

func TestMemoryCgroupDir(t *testing.T) {
	root := t.TempDir()
	for _, dir := range []string{"docker/abc123", "user.slice"} {
		if err := os.MkdirAll(filepath.Join(root, dir), 0o755); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.WriteFile(filepath.Join(root, "not-a-dir"), nil, 0o644); err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		description string
		cgroupPath  string
		expected    string
	}{
		// Positive: the cgroup's own directory exists and is used.
		{
			description: "positive: nested docker cgroup directory exists",
			cgroupPath:  "/docker/abc123",
			expected:    filepath.Join(root, "docker/abc123"),
		},
		{
			description: "positive: single-level cgroup directory exists",
			cgroupPath:  "/user.slice",
			expected:    filepath.Join(root, "user.slice"),
		},
		// Negative: the cgroup's directory is missing, so fall back to root.
		{
			description: "negative: missing cgroup directory falls back to root",
			cgroupPath:  "/docker/missing",
			expected:    root,
		},
		{
			description: "negative: missing parent directory falls back to root",
			cgroupPath:  "/kubepods/pod1/abc",
			expected:    root,
		},
		// Boundary: the root cgroup itself.
		{
			description: "boundary: root cgroup path is root",
			cgroupPath:  "/",
			expected:    root,
		},
		{
			description: "boundary: empty cgroup path is root",
			cgroupPath:  "",
			expected:    root,
		},
		// Corner: path spelling and non-directory entries.
		{
			description: "corner: trailing slash on an existing directory",
			cgroupPath:  "/docker/abc123/",
			expected:    filepath.Join(root, "docker/abc123"),
		},
		{
			description: "corner: path without a leading slash",
			cgroupPath:  "docker/abc123",
			expected:    filepath.Join(root, "docker/abc123"),
		},
		{
			description: "corner: path naming a regular file falls back to root",
			cgroupPath:  "/not-a-dir",
			expected:    root,
		},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			if got := memoryCgroupDir(root, tc.cgroupPath); got != tc.expected {
				t.Fatalf("memoryCgroupDir(%q) = %q, expected %q", tc.cgroupPath, got, tc.expected)
			}
		})
	}
}
