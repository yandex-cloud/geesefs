package cfg

import (
	"math"
	"testing"
)

func TestS3ConfigInitMultipartCopyThreshold(t *testing.T) {
	const MiB = 1024 * 1024

	cases := []struct {
		description string
		threshold   uint64
		inits       int
		expected    uint64
	}{
		// Positive: an unset threshold gets the documented 128 MB default,
		// in bytes like the value the command line sets.
		{
			description: "positive: unset threshold defaults to 128 MiB in bytes",
			threshold:   0,
			inits:       1,
			expected:    128 * MiB,
		},
		// Negative: a value that is already set is not replaced.
		{
			description: "negative: explicit 64 MiB is kept",
			threshold:   64 * MiB,
			inits:       1,
			expected:    64 * MiB,
		},
		{
			description: "negative: explicit 5 GiB maximum is kept",
			threshold:   5 * 1024 * MiB,
			inits:       1,
			expected:    5 * 1024 * MiB,
		},
		// Boundary: the smallest and largest values that count as set.
		{
			description: "boundary: explicit 1 byte is kept",
			threshold:   1,
			inits:       1,
			expected:    1,
		},
		{
			description: "boundary: explicit max uint64 is kept",
			threshold:   math.MaxUint64,
			inits:       1,
			expected:    math.MaxUint64,
		},
		// Corner: values that look like the old default, and repeated Init.
		{
			description: "corner: explicit 128 bytes is kept, not scaled",
			threshold:   128,
			inits:       1,
			expected:    128,
		},
		{
			description: "corner: Init twice on an unset threshold still gives 128 MiB",
			threshold:   0,
			inits:       2,
			expected:    128 * MiB,
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			c := &S3Config{MultipartCopyThreshold: tc.threshold}
			for i := 0; i < tc.inits; i++ {
				c.Init()
			}
			if c.MultipartCopyThreshold != tc.expected {
				t.Errorf("MultipartCopyThreshold = %d, expected %d", c.MultipartCopyThreshold, tc.expected)
			}
		})
	}
}
