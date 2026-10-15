package core

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go/aws/credentials"
)

func TestV2SignerCanonicalResource(t *testing.T) {
	cases := []struct {
		description string
		url         string
		pathStyle   bool
		expected    string
	}{
		// Positive: the resource is the escaped path, as sent on the wire,
		// in both addressing styles.
		{
			description: "positive: path-style key with a space is escaped",
			url:         "https://s3.example.com/mybucket/a%20b",
			pathStyle:   true,
			expected:    "/mybucket/a%20b",
		},
		{
			description: "positive: virtual-hosted key with a space is escaped",
			url:         "https://mybucket.s3.example.com/a%20b",
			pathStyle:   false,
			expected:    "/mybucket/a%20b",
		},
		// Negative: keys with nothing to escape are unchanged.
		{
			description: "negative: virtual-hosted plain key is unchanged",
			url:         "https://mybucket.s3.example.com/dir/file.txt",
			pathStyle:   false,
			expected:    "/mybucket/dir/file.txt",
		},
		{
			description: "negative: virtual-hosted unreserved characters are not escaped",
			url:         "https://mybucket.s3.example.com/a-b_c.d~e",
			pathStyle:   false,
			expected:    "/mybucket/a-b_c.d~e",
		},
		// Boundary: the bucket root.
		{
			description: "boundary: virtual-hosted bucket root",
			url:         "https://mybucket.s3.example.com/",
			pathStyle:   false,
			expected:    "/mybucket/",
		},
		{
			description: "boundary: path-style bucket root",
			url:         "https://s3.example.com/mybucket/",
			pathStyle:   true,
			expected:    "/mybucket/",
		},
		// Corner: characters that are easy to get wrong, and subresources.
		{
			description: "corner: virtual-hosted plus sign is escaped",
			url:         "https://mybucket.s3.example.com/a+b",
			pathStyle:   false,
			expected:    "/mybucket/a%2Bb",
		},
		{
			description: "corner: virtual-hosted non-ASCII key is escaped as UTF-8",
			url:         "https://mybucket.s3.example.com/caf%C3%A9",
			pathStyle:   false,
			expected:    "/mybucket/caf%C3%A9",
		},
		{
			description: "corner: virtual-hosted literal percent is escaped",
			url:         "https://mybucket.s3.example.com/100%25",
			pathStyle:   false,
			expected:    "/mybucket/100%25",
		},
		{
			description: "corner: virtual-hosted escaped key keeps its subresource",
			url:         "https://mybucket.s3.example.com/a%20b?uploadId=x&foo=bar",
			pathStyle:   false,
			expected:    "/mybucket/a%20b?uploadId=x",
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			req, err := http.NewRequest("GET", tc.url, nil)
			if err != nil {
				t.Fatal(err)
			}
			v2 := signer{
				Request:     req,
				Time:        time.Unix(0, 0),
				Credentials: credentials.NewStaticCredentials("AKID", "SECRET", ""),
				pathStyle:   tc.pathStyle,
			}
			if err := v2.Sign(); err != nil {
				t.Fatal(err)
			}
			lines := strings.Split(v2.stringToSign, "\n")
			if got := lines[len(lines)-1]; got != tc.expected {
				t.Errorf("canonical resource = %q, expected %q", got, tc.expected)
			}
		})
	}
}
