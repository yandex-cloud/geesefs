//go:build !windows

package core

import (
	"errors"
	"fmt"
	"os"
	"syscall"
	"testing"

	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

func TestToErrno(t *testing.T) {
	cases := []struct {
		description string
		err         error
		expected    syscall.Errno
	}{
		// Positive: errnos pass through unchanged.
		{
			description: "positive: ENOENT is returned as is",
			err:         syscall.ENOENT,
			expected:    syscall.ENOENT,
		},
		{
			description: "positive: EIO is returned as is",
			err:         syscall.EIO,
			expected:    syscall.EIO,
		},
		// Negative: errors that carry no errno become EIO instead of panicking.
		{
			description: "negative: plain error becomes EIO",
			err:         errors.New("something failed"),
			expected:    syscall.EIO,
		},
		{
			description: "negative: gRPC status error from another node becomes EIO",
			err:         status.Error(codes.Unavailable, "connection refused"),
			expected:    syscall.EIO,
		},
		// Boundary: no error, and the zero errno.
		{
			description: "boundary: nil error is 0",
			err:         nil,
			expected:    0,
		},
		{
			description: "boundary: zero errno is 0",
			err:         syscall.Errno(0),
			expected:    0,
		},
		// Corner: errnos wrapped by other errors are found.
		{
			description: "corner: errno wrapped with %w is unwrapped",
			err:         fmt.Errorf("writing part: %w", syscall.ENOSPC),
			expected:    syscall.ENOSPC,
		},
		{
			description: "corner: errno wrapped twice is unwrapped",
			err:         fmt.Errorf("outer: %w", fmt.Errorf("inner: %w", syscall.EACCES)),
			expected:    syscall.EACCES,
		},
		{
			description: "corner: errno inside an os.PathError is unwrapped",
			err:         &os.PathError{Op: "open", Path: "/x", Err: syscall.EPERM},
			expected:    syscall.EPERM,
		},
		{
			description: "corner: errno joined with another error is found",
			err:         errors.Join(errors.New("other"), syscall.ENOTEMPTY),
			expected:    syscall.ENOTEMPTY,
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			var got syscall.Errno
			var panicked interface{}
			func() {
				defer func() { panicked = recover() }()
				got = toErrno(tc.err)
			}()
			if panicked != nil {
				t.Fatalf("toErrno(%v) panicked: %v", tc.err, panicked)
			}
			if got != tc.expected {
				t.Errorf("toErrno(%v) = %d (%v), expected %d (%v)", tc.err, got, got, tc.expected, tc.expected)
			}
		})
	}
}
