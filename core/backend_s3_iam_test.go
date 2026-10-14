package core

import (
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
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

// tryIAMOutcome is what TestTryIAMErrors expects TryIAM to return.
type tryIAMOutcome int

const (
	iamOK         tryIAMOutcome = iota // nil error, token stored
	iamURLError                        // error unwraps to *url.Error
	iamOtherError                      // non-nil error that is not a *url.Error
)

// TestTryIAMErrors checks that a failed metadata request surfaces the
// underlying *url.Error. The gcp flavor (also the default) used to lose it to
// a shadowed err and return a generic "no response" instead.
func TestTryIAMErrors(t *testing.T) {
	serve := func(h http.HandlerFunc) func(t *testing.T) string {
		return func(t *testing.T) string {
			srv := httptest.NewServer(h)
			t.Cleanup(srv.Close)
			return srv.URL
		}
	}
	fixed := func(u string) func(t *testing.T) string {
		return func(t *testing.T) string { return u }
	}
	// A server closed straight away gives a URL that refuses connections.
	refused := func(t *testing.T) string {
		srv := httptest.NewServer(nil)
		srv.Close()
		return srv.URL
	}
	// Accepts the connection, then drops it without writing a response.
	hangUp := serve(func(w http.ResponseWriter, r *http.Request) {
		conn, _, err := w.(http.Hijacker).Hijack()
		if err == nil {
			conn.Close()
		}
	})
	gcpToken := serve(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprint(w, `{"access_token":"gcp-token","token_type":"Bearer","expires_in":3600}`)
	})
	imdsToken := serve(func(w http.ResponseWriter, r *http.Request) {
		fmt.Fprintf(w, `{"Code":"Success","Token":"imds-token","Expiration":%q}`,
			time.Now().Add(time.Hour).Format(time.RFC3339))
	})

	cases := []struct {
		description string
		flavor      string
		server      func(t *testing.T) string // returns the IAM URL
		expected    tryIAMOutcome
	}{
		// Positive: a well-formed token response succeeds.
		{
			description: "positive: gcp flavor, valid token",
			flavor:      "gcp",
			server:      gcpToken,
			expected:    iamOK,
		},
		{
			description: "positive: imdsv1 flavor, valid token",
			flavor:      "imdsv1",
			server:      imdsToken,
			expected:    iamOK,
		},
		// Negative: the request itself fails, and that error is returned.
		{
			description: "negative: gcp flavor, connection refused",
			flavor:      "gcp",
			server:      refused,
			expected:    iamURLError,
		},
		{
			description: "negative: default (empty) flavor, connection refused",
			flavor:      "",
			server:      refused,
			expected:    iamURLError,
		},
		{
			description: "negative: imdsv1 flavor, connection refused",
			flavor:      "imdsv1",
			server:      refused,
			expected:    iamURLError,
		},
		// Boundary: the request gets as far as the server, or just past it.
		{
			description: "boundary: gcp flavor, server hangs up before responding",
			flavor:      "gcp",
			server:      hangUp,
			expected:    iamURLError,
		},
		{
			description: "boundary: gcp flavor, HTTP 500 with a non-JSON body",
			flavor:      "gcp",
			server: serve(func(w http.ResponseWriter, r *http.Request) {
				http.Error(w, "metadata unavailable", http.StatusInternalServerError)
			}),
			expected: iamOtherError,
		},
		{
			description: "boundary: gcp flavor, valid JSON with an empty token",
			flavor:      "gcp",
			server: serve(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"access_token":"","expires_in":3600}`)
			}),
			expected: iamOtherError,
		},
		// Corner: failures before the request is sent, and non-success codes.
		{
			description: "corner: gcp flavor, malformed URL",
			flavor:      "gcp",
			server:      fixed("http://[::1"),
			expected:    iamURLError,
		},
		{
			description: "corner: imdsv1 flavor, non-success code",
			flavor:      "imdsv1",
			server: serve(func(w http.ResponseWriter, r *http.Request) {
				fmt.Fprint(w, `{"Code":"AccessDenied"}`)
			}),
			expected: iamOtherError,
		},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			s := &S3Backend{
				config: (&cfg.S3Config{
					UseIAM:    true,
					IAMFlavor: tc.flavor,
					IAMUrl:    tc.server(t),
				}).Init(),
			}
			err := s.TryIAM()
			stopRefreshTimer(s)

			var uerr *url.Error
			switch tc.expected {
			case iamOK:
				if err != nil {
					t.Fatalf("TryIAM: %v, expected success", err)
				}
				if currentIAMToken(t, s) == "" {
					t.Fatal("TryIAM succeeded but stored an empty token")
				}
			case iamURLError:
				if !errors.As(err, &uerr) {
					t.Fatalf("TryIAM error %v, expected a *url.Error", err)
				}
			case iamOtherError:
				if err == nil || errors.As(err, &uerr) {
					t.Fatalf("TryIAM error %v, expected a non-URL error", err)
				}
			}
		})
	}
}
