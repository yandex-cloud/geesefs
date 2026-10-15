package core

import (
	"testing"
)

func TestAnyFlushInFlight(t *testing.T) {
	// counts builds a priorities slice the size Goofys uses, index 0 unused.
	counts := func(byPriority map[int]int64) []int64 {
		p := make([]int64, MAX_FLUSH_PRIORITY+1)
		for i, n := range byPriority {
			p[i] = n
		}
		return p
	}

	cases := []struct {
		description string
		priorities  []int64
		upTo        int
		expected    bool
	}{
		// Positive: a flush at any priority up to upTo counts.
		{
			description: "positive: flush at the current priority",
			priorities:  counts(map[int]int64{2: 1}),
			upTo:        2,
			expected:    true,
		},
		{
			description: "positive: flush at a lower priority than the current one",
			priorities:  counts(map[int]int64{1: 1}),
			upTo:        2,
			expected:    true,
		},
		{
			description: "positive: flush at priority 1 while at the maximum priority",
			priorities:  counts(map[int]int64{1: 1}),
			upTo:        MAX_FLUSH_PRIORITY,
			expected:    true,
		},
		// Negative: nothing in flight at priorities up to upTo.
		{
			description: "negative: no flushes at all",
			priorities:  counts(nil),
			upTo:        MAX_FLUSH_PRIORITY,
			expected:    false,
		},
		{
			description: "negative: flush only at a higher priority than upTo",
			priorities:  counts(map[int]int64{3: 1}),
			upTo:        2,
			expected:    false,
		},
		// Boundary: the lowest and highest priorities.
		{
			description: "boundary: upTo 1 with a flush at priority 1",
			priorities:  counts(map[int]int64{1: 1}),
			upTo:        1,
			expected:    true,
		},
		{
			description: "boundary: upTo 1 ignores a flush at priority 2",
			priorities:  counts(map[int]int64{2: 1}),
			upTo:        1,
			expected:    false,
		},
		{
			description: "boundary: flush at the maximum priority",
			priorities:  counts(map[int]int64{MAX_FLUSH_PRIORITY: 1}),
			upTo:        MAX_FLUSH_PRIORITY,
			expected:    true,
		},
		// Corner: the unused index 0, upTo 0, and several flushes at once.
		{
			description: "corner: index 0 is not a priority",
			priorities:  counts(map[int]int64{0: 1}),
			upTo:        MAX_FLUSH_PRIORITY,
			expected:    false,
		},
		{
			description: "corner: upTo 0 checks nothing",
			priorities:  counts(map[int]int64{1: 1}),
			upTo:        0,
			expected:    false,
		},
		{
			description: "corner: several flushes at lower priorities, none at upTo",
			priorities:  counts(map[int]int64{1: 4, 2: 2}),
			upTo:        MAX_FLUSH_PRIORITY,
			expected:    true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			if got := anyFlushInFlight(tc.priorities, tc.upTo); got != tc.expected {
				t.Errorf("anyFlushInFlight(%v, %d) = %v, expected %v", tc.priorities, tc.upTo, got, tc.expected)
			}
		})
	}
}
