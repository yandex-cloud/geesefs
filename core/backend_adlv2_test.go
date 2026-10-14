package core

import (
	"sync/atomic"
	"testing"
	"time"
)

func TestRenewLeaseStops(t *testing.T) {
	const never = -1

	cases := []struct {
		description string
		interval    time.Duration
		renewDelay  time.Duration // time each renewal takes
		stopAfter   int           // renewals before the stop signal, or never
		closeStop   bool          // close the channel instead of sending on it
		expected    bool          // whether renewLease returns
		renews      int64         // renewals seen: exact if it returns, minimum if not
	}{
		// Positive: the stop signal ends the loop after renewals happened.
		{
			description: "positive: stop after several renewals returns",
			interval:    time.Millisecond,
			stopAfter:   3,
			expected:    true,
			renews:      3,
		},
		{
			description: "positive: closing the channel after several renewals returns",
			interval:    time.Millisecond,
			stopAfter:   2,
			closeStop:   true,
			expected:    true,
			renews:      2,
		},
		// Negative: without a stop signal the loop must keep running.
		{
			description: "negative: no stop signal keeps renewing",
			interval:    time.Millisecond,
			stopAfter:   never,
			expected:    false,
			renews:      3,
		},
		{
			description: "negative: no stop signal and no elapsed interval does nothing",
			interval:    time.Hour,
			stopAfter:   never,
			expected:    false,
			renews:      0,
		},
		// Boundary: stopping at zero and exactly one renewal.
		{
			description: "boundary: stop before the first renewal returns without renewing",
			interval:    time.Hour,
			stopAfter:   0,
			expected:    true,
			renews:      0,
		},
		{
			description: "boundary: stop after exactly one renewal",
			interval:    time.Millisecond,
			stopAfter:   1,
			expected:    true,
			renews:      1,
		},
		// Corner: closed before starting, and renewals slower than the interval.
		{
			description: "corner: channel closed before the first renewal",
			interval:    time.Hour,
			stopAfter:   0,
			closeStop:   true,
			expected:    true,
			renews:      0,
		},
		{
			description: "corner: renewal slower than the interval still stops",
			interval:    time.Millisecond,
			renewDelay:  5 * time.Millisecond,
			stopAfter:   2,
			expected:    true,
			renews:      2,
		},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			// Buffered like ADLv2MultipartBlobCommitInput.RenewLeaseStop, so
			// the stop signal can be sent from inside renew.
			stop := make(chan bool, 1)
			signal := func() {
				if tc.closeStop {
					close(stop)
				} else {
					stop <- true
				}
			}
			var renews atomic.Int64
			renew := func() {
				time.Sleep(tc.renewDelay)
				if renews.Add(1) == int64(tc.stopAfter) {
					signal()
				}
			}
			if tc.stopAfter == 0 {
				signal()
			}

			done := make(chan struct{})
			go func() {
				renewLease(stop, tc.interval, renew)
				close(done)
			}()

			if tc.expected {
				select {
				case <-done:
				case <-time.After(5 * time.Second):
					t.Fatal("renewLease did not return after the stop signal")
				}
				if got := renews.Load(); got != tc.renews {
					t.Fatalf("renewals = %d, expected %d", got, tc.renews)
				}
				return
			}

			deadline := time.Now().Add(5 * time.Second)
			for renews.Load() < tc.renews && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			select {
			case <-done:
				t.Fatal("renewLease returned without a stop signal")
			case <-time.After(20 * time.Millisecond):
			}
			if got := renews.Load(); got < tc.renews {
				t.Fatalf("renewals = %d, expected at least %d", got, tc.renews)
			}
			// Stop it so the goroutine does not outlive the test.
			signal()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("renewLease did not return after the stop signal")
			}
		})
	}
}
