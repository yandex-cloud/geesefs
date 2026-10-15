package core

import (
	"reflect"
	"testing"
)

func TestZeroPartsToFlush(t *testing.T) {
	cases := []struct {
		description      string
		anyEvicted       bool
		partlyZero       []uint64
		fullyZero        []uint64
		expectedParts    []uint64
		expectedPriority uint64
	}{
		// Positive: step 2 takes partly zero parts, step 3 takes fully zero
		// parts.
		{
			description:      "positive: partly zero parts are flushed at priority 2",
			partlyZero:       []uint64{1, 4},
			expectedParts:    []uint64{1, 4},
			expectedPriority: 2,
		},
		{
			description:      "positive: fully zero parts are flushed at priority 3 when parts are evicted",
			anyEvicted:       true,
			fullyZero:        []uint64{2, 5},
			expectedParts:    []uint64{2, 5},
			expectedPriority: 3,
		},
		// Negative: nothing qualifies.
		{
			description:      "negative: no zero parts flushes nothing",
			anyEvicted:       true,
			expectedParts:    nil,
			expectedPriority: 0,
		},
		{
			description:      "negative: fully zero parts are not flushed without evicted parts",
			anyEvicted:       false,
			fullyZero:        []uint64{2},
			expectedParts:    nil,
			expectedPriority: 0,
		},
		// Boundary: a single part in either list.
		{
			description:      "boundary: one partly zero part",
			partlyZero:       []uint64{0},
			expectedParts:    []uint64{0},
			expectedPriority: 2,
		},
		{
			description:      "boundary: one fully zero part with evicted parts",
			anyEvicted:       true,
			fullyZero:        []uint64{0},
			expectedParts:    []uint64{0},
			expectedPriority: 3,
		},
		// Corner: both lists set, and empty but non-nil lists.
		{
			description:      "corner: partly zero parts win over fully zero parts",
			anyEvicted:       true,
			partlyZero:       []uint64{1},
			fullyZero:        []uint64{2},
			expectedParts:    []uint64{1},
			expectedPriority: 2,
		},
		{
			description:      "corner: empty partly zero list falls through to fully zero parts",
			anyEvicted:       true,
			partlyZero:       []uint64{},
			fullyZero:        []uint64{3},
			expectedParts:    []uint64{3},
			expectedPriority: 3,
		},
		{
			description:      "corner: empty fully zero list with evicted parts flushes nothing",
			anyEvicted:       true,
			partlyZero:       []uint64{},
			fullyZero:        []uint64{},
			expectedParts:    nil,
			expectedPriority: 0,
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			parts, priority := zeroPartsToFlush(tc.anyEvicted, tc.partlyZero, tc.fullyZero)
			if len(parts) == 0 {
				parts = nil
			}
			if !reflect.DeepEqual(parts, tc.expectedParts) || priority != tc.expectedPriority {
				t.Errorf("zeroPartsToFlush = %v at priority %d, expected %v at priority %d",
					parts, priority, tc.expectedParts, tc.expectedPriority)
			}
		})
	}
}
