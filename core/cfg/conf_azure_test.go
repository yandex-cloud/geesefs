package cfg

import (
	"testing"

	azblob "github.com/Azure/azure-sdk-for-go/services/storage/mgmt/2019-04-01/storage"
)

func TestPickAccountKey(t *testing.T) {
	key := func(value string, perm azblob.KeyPermission) azblob.AccountKey {
		return azblob.AccountKey{Value: &value, Permissions: perm}
	}
	cases := []struct {
		description string
		keys        []azblob.AccountKey
		expected    string
	}{
		// Positive: a full-permission key exists and is chosen.
		{
			description: "positive: full key after a read key is preferred",
			keys: []azblob.AccountKey{
				key("read1", azblob.Read),
				key("full1", azblob.Full),
			},
			expected: "full1",
		},
		{
			description: "positive: full key between read keys is preferred",
			keys: []azblob.AccountKey{
				key("read1", azblob.Read),
				key("full1", azblob.Full),
				key("read2", azblob.Read),
			},
			expected: "full1",
		},
		// Negative: no full-permission key, so fall back to the first key.
		{
			description: "negative: only read keys falls back to the first",
			keys: []azblob.AccountKey{
				key("read1", azblob.Read),
				key("read2", azblob.Read),
			},
			expected: "read1",
		},
		{
			description: "negative: unrecognized permission is not treated as full",
			keys: []azblob.AccountKey{
				key("other1", azblob.KeyPermission("Other")),
				key("read1", azblob.Read),
			},
			expected: "other1",
		},
		// Boundary: smallest inputs and the ends of the list.
		{
			description: "boundary: single full key",
			keys:        []azblob.AccountKey{key("full1", azblob.Full)},
			expected:    "full1",
		},
		{
			description: "boundary: single read key",
			keys:        []azblob.AccountKey{key("read1", azblob.Read)},
			expected:    "read1",
		},
		{
			description: "boundary: full key first",
			keys: []azblob.AccountKey{
				key("full1", azblob.Full),
				key("read1", azblob.Read),
			},
			expected: "full1",
		},
		{
			description: "boundary: full key last of several",
			keys: []azblob.AccountKey{
				key("read1", azblob.Read),
				key("read2", azblob.Read),
				key("read3", azblob.Read),
				key("full1", azblob.Full),
			},
			expected: "full1",
		},
		// Corner: ties, case sensitivity and empty values.
		{
			description: "corner: first of several full keys wins",
			keys: []azblob.AccountKey{
				key("read1", azblob.Read),
				key("full1", azblob.Full),
				key("full2", azblob.Full),
			},
			expected: "full1",
		},
		{
			description: "corner: permission match is case sensitive",
			keys: []azblob.AccountKey{
				key("lower1", azblob.KeyPermission("full")),
				key("full1", azblob.Full),
			},
			expected: "full1",
		},
		{
			description: "corner: full key with an empty value is still chosen",
			keys: []azblob.AccountKey{
				key("read1", azblob.Read),
				key("", azblob.Full),
			},
			expected: "",
		},
	}
	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			if got := pickAccountKey(tc.keys); got != tc.expected {
				t.Fatalf("pickAccountKey() = %q, expected %q", got, tc.expected)
			}
		})
	}
}
