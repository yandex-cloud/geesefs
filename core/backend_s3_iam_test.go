// Copyright 2026 Yandex LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/request"
	"github.com/yandex-cloud/geesefs/core/cfg"
)

// iamTokenServer is a hermetic stand-in for the GCP-flavored IAM metadata
// endpoint. It counts requests and hands out a distinct token each time.
// When fail is set it returns a broken response so TryIAM reports an error.
//
// Tests must keep expiresIn large enough that the background AfterFunc timer
// (scheduled by TryIAM) cannot fire within the test run, otherwise it would
// race the assertions and re-hit the server after Close.
type iamTokenServer struct {
	*httptest.Server
	hits      atomic.Int64
	fail      atomic.Bool
	expiresIn int
}

func newIAMTokenServer(expiresIn int) *iamTokenServer {
	srv := &iamTokenServer{expiresIn: expiresIn}
	srv.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n := srv.hits.Add(1)
		if srv.fail.Load() {
			http.Error(w, "metadata unavailable", http.StatusInternalServerError)
			return
		}
		fmt.Fprintf(w, `{"access_token":"token-%d","token_type":"Bearer","expires_in":%d}`, n, srv.expiresIn)
	}))
	return srv
}

func newIAMBackend(srv *iamTokenServer) *S3Backend {
	return &S3Backend{
		config: (&cfg.S3Config{
			UseIAM:    true,
			IAMFlavor: "gcp",
			IAMUrl:    srv.URL,
		}).Init(),
	}
}

// stopRefreshTimer disables the background monotonic-clock timer so it cannot
// add nondeterministic hits during the test.
func stopRefreshTimer(s *S3Backend) {
	if s.iamRefreshTimer != nil {
		s.iamRefreshTimer.Stop()
	}
}

func currentIAMToken(t *testing.T, s *S3Backend) string {
	t.Helper()
	v := s.iamToken.Load()
	if v == nil {
		t.Fatal("IAM token is not set")
	}
	return v.(string)
}

func expireIAMTokenInThePast(s *S3Backend) {
	s.iamTokenExpUnix.Store(time.Now().Add(-time.Hour).UnixNano())
}

// TestIAMLazyRefreshOnFrozenTimer reproduces the suspended-VM scenario: the
// background AfterFunc (monotonic clock) never fired, the token expired by
// wall-clock time, and the lazy refresh must pick it up.
func TestIAMLazyRefreshOnFrozenTimer(t *testing.T) {
	srv := newIAMTokenServer(3600)
	defer srv.Close()
	s := newIAMBackend(srv)

	if err := s.TryIAM(); err != nil {
		t.Fatalf("TryIAM: %v", err)
	}
	stopRefreshTimer(s)
	if got := srv.hits.Load(); got != 1 {
		t.Fatalf("hits after TryIAM = %d, want 1", got)
	}
	tokenBefore := currentIAMToken(t, s)

	// Emulate: wall-clock advanced past expiry while the monotonic timer
	// stayed frozen, so the deadline is now in the past.
	expireIAMTokenInThePast(s)

	s.ensureFreshIAMToken()
	stopRefreshTimer(s)

	if got := srv.hits.Load(); got != 2 {
		t.Fatalf("hits after lazy refresh = %d, want 2", got)
	}
	if tokenAfter := currentIAMToken(t, s); tokenAfter == tokenBefore {
		t.Fatalf("token was not refreshed, still %q", tokenAfter)
	}
}

// TestIAMLazyRefreshConcurrent verifies the double-checked locking: many
// concurrent callers on an expired token trigger exactly one refresh.
func TestIAMLazyRefreshConcurrent(t *testing.T) {
	srv := newIAMTokenServer(3600)
	defer srv.Close()
	s := newIAMBackend(srv)

	if err := s.TryIAM(); err != nil {
		t.Fatalf("TryIAM: %v", err)
	}
	stopRefreshTimer(s)
	expireIAMTokenInThePast(s)

	const goroutines = 20
	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func() {
			defer wg.Done()
			s.ensureFreshIAMToken()
		}()
	}
	wg.Wait()
	stopRefreshTimer(s)

	if got := srv.hits.Load(); got != 2 {
		t.Fatalf("hits = %d, want 2 (1 initial + exactly 1 lazy refresh)", got)
	}
}

// TestIAMLazyRefreshSkippedWhenFresh verifies the fast path does not hit the
// metadata server while the token is still valid.
func TestIAMLazyRefreshSkippedWhenFresh(t *testing.T) {
	srv := newIAMTokenServer(3600)
	defer srv.Close()
	s := newIAMBackend(srv)

	if err := s.TryIAM(); err != nil {
		t.Fatalf("TryIAM: %v", err)
	}
	stopRefreshTimer(s)

	s.ensureFreshIAMToken()
	stopRefreshTimer(s)

	if got := srv.hits.Load(); got != 1 {
		t.Fatalf("hits = %d, want 1 (no refresh for a fresh token)", got)
	}
}

// TestIAMLazyRefreshMarginBoundary checks that the refresh fires proactively
// once the deadline is within iamLazyRefreshMargin, but not earlier. A sign
// error in exp-int64(iamLazyRefreshMargin) would defeat the whole fix yet
// still pass the other tests, so this boundary is asserted explicitly.
func TestIAMLazyRefreshMarginBoundary(t *testing.T) {
	cases := []struct {
		name        string
		untilExpiry time.Duration
		wantRefresh bool
	}{
		{"inside margin", iamLazyRefreshMargin / 2, true},
		{"outside margin", 10 * time.Minute, false},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := newIAMTokenServer(3600)
			defer srv.Close()
			s := newIAMBackend(srv)
			if err := s.TryIAM(); err != nil {
				t.Fatalf("TryIAM: %v", err)
			}
			stopRefreshTimer(s)

			s.iamTokenExpUnix.Store(time.Now().Add(tc.untilExpiry).UnixNano())
			s.ensureFreshIAMToken()
			stopRefreshTimer(s)

			want := int64(1)
			if tc.wantRefresh {
				want = 2
			}
			if got := srv.hits.Load(); got != want {
				t.Fatalf("hits = %d, want %d", got, want)
			}
		})
	}
}

// TestEnsureFreshIAMTokenNoopWhenUninitialized verifies the exp == 0 fast
// path: with no successful TryIAM, the lazy refresh must be a pure no-op and
// must not panic (no token has ever been stored).
func TestEnsureFreshIAMTokenNoopWhenUninitialized(t *testing.T) {
	srv := newIAMTokenServer(3600)
	defer srv.Close()
	s := newIAMBackend(srv) // TryIAM never called -> iamTokenExpUnix == 0

	s.ensureFreshIAMToken()
	stopRefreshTimer(s)

	if got := srv.hits.Load(); got != 0 {
		t.Fatalf("uninitialized IAM must not trigger a fetch, hits = %d", got)
	}
}

// TestIAMLazyRefreshKeepsStaleTokenOnError pins the fallback contract: when
// the metadata endpoint is broken, a lazy refresh must keep the last good
// token, must not advance the deadline (so the next request retries instead
// of going quiet), and must recover once the endpoint is healthy again.
func TestIAMLazyRefreshKeepsStaleTokenOnError(t *testing.T) {
	srv := newIAMTokenServer(3600)
	defer srv.Close()
	s := newIAMBackend(srv)

	if err := s.TryIAM(); err != nil {
		t.Fatalf("TryIAM: %v", err)
	}
	stopRefreshTimer(s)
	tokenBefore := currentIAMToken(t, s)

	srv.fail.Store(true)
	expireIAMTokenInThePast(s)
	expBefore := s.iamTokenExpUnix.Load()

	s.ensureFreshIAMToken() // must not panic
	stopRefreshTimer(s)

	if got := currentIAMToken(t, s); got != tokenBefore {
		t.Fatalf("stale token must be kept on refresh failure, got %q want %q", got, tokenBefore)
	}
	if got := s.iamTokenExpUnix.Load(); got != expBefore {
		t.Fatalf("deadline must not advance on failure, else the next request stops retrying")
	}

	srv.fail.Store(false)
	s.ensureFreshIAMToken()
	stopRefreshTimer(s)

	if got := currentIAMToken(t, s); got == tokenBefore {
		t.Fatalf("token must refresh once the metadata endpoint recovers")
	}
}

// TestSetIAMSignerRefreshesAndSetsHeader exercises the real integration point:
// signing a non-anonymous request must trigger a lazy refresh when the token
// is stale and put the refreshed token into the IAM header, while an
// anonymous request must neither refresh nor set the header.
func TestSetIAMSignerRefreshesAndSetsHeader(t *testing.T) {
	srv := newIAMTokenServer(3600)
	defer srv.Close()
	s := newIAMBackend(srv)

	if err := s.TryIAM(); err != nil {
		t.Fatalf("TryIAM: %v", err)
	}
	stopRefreshTimer(s)
	expireIAMTokenInThePast(s)

	handlers := &request.Handlers{}
	s.setIAMSigner(handlers)

	signReq := func(creds *credentials.Credentials) *http.Request {
		httpReq, _ := http.NewRequest("GET", "http://example.invalid/", nil)
		req := &request.Request{
			Config:      aws.Config{Credentials: creds},
			HTTPRequest: httpReq,
		}
		handlers.Sign.Run(req)
		stopRefreshTimer(s)
		return httpReq
	}

	anonReq := signReq(credentials.AnonymousCredentials)
	if got := srv.hits.Load(); got != 1 {
		t.Fatalf("anonymous request must not trigger a refresh, hits = %d", got)
	}
	if h := anonReq.Header.Get(s.config.IAMHeader); h != "" {
		t.Fatalf("anonymous request must not set the IAM header, got %q", h)
	}

	signedReq := signReq(credentials.NewStaticCredentials("ak", "sk", ""))
	if got := srv.hits.Load(); got != 2 {
		t.Fatalf("signing an expired-token request must trigger lazy refresh, hits = %d", got)
	}
	if h := signedReq.Header.Get(s.config.IAMHeader); h != "token-2" {
		t.Fatalf("refreshed token must land in the IAM header, got %q", h)
	}
}
