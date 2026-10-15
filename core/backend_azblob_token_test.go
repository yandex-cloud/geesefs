package core

import (
	"errors"
	"net/url"
	"sync"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/Azure/azure-storage-blob-go/azblob"
)

const azTestBareURL = "http://127.0.0.1:1/devstoreaccount1/"

// sasToken returns a SAS token query string that expires at the given time.
// A zero time gives a token without the "se" (expiry) parameter.
func sasToken(expire time.Time) string {
	if expire.IsZero() {
		return "sig=test"
	}
	return "se=" + url.QueryEscape(expire.UTC().Format("2006-01-02T15:04:05Z")) + "&sig=test"
}

// newTokenTestAZBlob builds an AZBlob that uses SAS tokens, without network
// access: refreshToken and updateToken only parse URLs.
func newTokenTestAZBlob(provider func() (string, error)) *AZBlob {
	b := &AZBlob{
		pipeline:       azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{}),
		bucket:         "testbucket",
		bareURL:        azTestBareURL,
		tokenRenewGate: make(chan int, 1),
	}
	if provider != nil {
		b.sasTokenProvider = provider
	}
	return b
}

// TestAZBlobRefreshTokenConcurrent calls refreshToken from many goroutines at
// the same time. Run it with -race: before the fix, refreshToken read b.c
// after unlocking b.mu, while another goroutine's updateToken wrote b.c.
func TestAZBlobRefreshTokenConcurrent(t *testing.T) {
	past := time.Now().Add(-time.Hour)
	future := time.Now().Add(time.Hour)
	sharedKey := azblob.NewServiceURL(url.URL{Scheme: "http", Host: "127.0.0.1:1", Path: "/devstoreaccount1/"},
		azblob.NewPipeline(azblob.NewAnonymousCredential(), azblob.PipelineOptions{})).NewContainerURL("testbucket")

	tests := []struct {
		description string
		goroutines  int
		// noProvider uses a shared key: b.c is set once and there is no token
		noProvider bool
		// token is the expiry of the tokens the provider returns
		token time.Time
		// providerErr makes the provider fail
		providerErr bool
		// preset sets b.c from a token that expires at presetExpire
		preset       bool
		presetExpire time.Time
		expected     error
		// expectedQuery is the token the returned container URL must carry
		expectedQuery string
	}{
		{description: "positive: first use from many goroutines gets a token",
			goroutines: 32, token: future, expected: nil, expectedQuery: sasToken(future)},
		{description: "positive: valid token is reused, not renewed",
			goroutines: 32, token: future, preset: true, presetExpire: future, expected: nil, expectedQuery: sasToken(future)},
		{description: "positive: expired token is renewed",
			goroutines: 32, token: future, preset: true, presetExpire: past, expected: nil, expectedQuery: sasToken(future)},
		{description: "negative: provider fails on first use",
			goroutines: 32, providerErr: true, expected: syscall.EACCES},
		{description: "negative: provider fails when the token is expired",
			goroutines: 32, providerErr: true, preset: true, presetExpire: past, expected: syscall.EACCES},
		{description: "boundary: one goroutine on first use",
			goroutines: 1, token: future, expected: nil, expectedQuery: sasToken(future)},
		{description: "boundary: two goroutines on first use",
			goroutines: 2, token: future, expected: nil, expectedQuery: sasToken(future)},
		{description: "corner: provider always returns an expired token",
			goroutines: 32, token: past, expected: nil, expectedQuery: sasToken(past)},
		{description: "corner: token without an expiry never expires",
			goroutines: 32, token: time.Time{}, expected: nil, expectedQuery: sasToken(time.Time{})},
		{description: "corner: shared key without a token provider",
			goroutines: 32, noProvider: true, expected: nil, expectedQuery: ""},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			var calls int32
			provider := func() (string, error) {
				atomic.AddInt32(&calls, 1)
				if tc.providerErr {
					return "", errors.New("no token")
				}
				return sasToken(tc.token), nil
			}
			b := newTokenTestAZBlob(provider)
			if tc.noProvider {
				b = newTokenTestAZBlob(nil)
				b.c = &sharedKey
			}
			if tc.preset {
				pre := newTokenTestAZBlob(func() (string, error) { return sasToken(tc.presetExpire), nil })
				if _, err := pre.updateToken(); err != nil {
					t.Fatal(err)
				}
				b.u, b.c, b.tokenExpire = pre.u, pre.c, pre.tokenExpire
			}

			start := make(chan struct{})
			var wg sync.WaitGroup
			cs := make([]*azblob.ContainerURL, tc.goroutines)
			errs := make([]error, tc.goroutines)
			for i := 0; i < tc.goroutines; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					<-start
					cs[i], errs[i] = b.refreshToken()
				}(i)
			}
			close(start)
			wg.Wait()

			for i := range cs {
				if errs[i] != tc.expected {
					t.Fatalf("goroutine %d: refreshToken() error = %v, want %v", i, errs[i], tc.expected)
				}
				if tc.expected != nil {
					if cs[i] != nil {
						t.Fatalf("goroutine %d: refreshToken() returned a container with error %v", i, errs[i])
					}
					continue
				}
				if cs[i] == nil {
					t.Fatalf("goroutine %d: refreshToken() returned no container", i)
				}
				u := cs[i].URL()
				if u.RawQuery != tc.expectedQuery {
					t.Fatalf("goroutine %d: container query = %q, want %q", i, u.RawQuery, tc.expectedQuery)
				}
			}
			if tc.noProvider && atomic.LoadInt32(&calls) != 0 {
				t.Fatalf("token provider called %d times without SAS", calls)
			}
			if tc.preset && !tc.presetExpire.Before(time.Now()) && atomic.LoadInt32(&calls) != 0 {
				t.Fatalf("valid token renewed %d times", calls)
			}
		})
	}
}

func BenchmarkAZBlobRefreshToken(b *testing.B) {
	future := time.Now().Add(time.Hour)
	az := newTokenTestAZBlob(func() (string, error) { return sasToken(future), nil })
	// Set a valid token directly: updateToken logs at INFO level, which
	// would mix with the benchmark output.
	u, err := url.Parse(azTestBareURL + "?" + sasToken(future))
	if err != nil {
		b.Fatal(err)
	}
	c := azblob.NewServiceURL(*u, az.pipeline).NewContainerURL(az.bucket)
	az.c, az.tokenExpire = &c, future
	b.ReportAllocs()
	b.ResetTimer()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			az.refreshToken()
		}
	})
}
