package core

import (
	"strings"
	"testing"
)

func TestPMetadata(t *testing.T) {
	cases := []struct {
		description string
		input       map[string]string
		expected    map[string]string
	}{
		// Positive: values survive and keys are lowercased.
		{
			description: "positive: lowercase key keeps its value",
			input:       map[string]string{"owner": "alice"},
			expected:    map[string]string{"owner": "alice"},
		},
		{
			description: "positive: mixed-case key is lowercased and keeps its value",
			input:       map[string]string{"Content-Owner": "alice"},
			expected:    map[string]string{"content-owner": "alice"},
		},
		{
			description: "positive: uppercase key is lowercased and keeps its value",
			input:       map[string]string{"OWNER": "alice"},
			expected:    map[string]string{"owner": "alice"},
		},
		// Negative: nothing to convert.
		{
			description: "negative: nil map gives an empty map",
			input:       nil,
			expected:    map[string]string{},
		},
		{
			description: "negative: empty map gives an empty map",
			input:       map[string]string{},
			expected:    map[string]string{},
		},
		// Boundary: empty keys and values.
		{
			description: "boundary: empty value on a mixed-case key stays empty",
			input:       map[string]string{"Owner": ""},
			expected:    map[string]string{"owner": ""},
		},
		{
			description: "boundary: empty key keeps its value",
			input:       map[string]string{"": "alice"},
			expected:    map[string]string{"": "alice"},
		},
		// Corner: several keys, non-ASCII, and keys that only differ from
		// another entry's lowercased form.
		{
			description: "corner: several mixed-case keys each keep their own value",
			input:       map[string]string{"A": "1", "bB": "2", "c": "3"},
			expected:    map[string]string{"a": "1", "bb": "2", "c": "3"},
		},
		{
			description: "corner: non-ASCII uppercase key is lowercased and keeps its value",
			input:       map[string]string{"ÄRGER": "x"},
			expected:    map[string]string{"ärger": "x"},
		},
		{
			description: "corner: value is not taken from a different key's lowercased form",
			input:       map[string]string{"Key": "upper", "other": "lower"},
			expected:    map[string]string{"key": "upper", "other": "lower"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			got := PMetadata(tc.input)
			if got == nil {
				t.Fatalf("PMetadata returned nil, expected %v", tc.expected)
			}
			if len(got) != len(tc.expected) {
				t.Fatalf("PMetadata returned %d keys, expected %d (%v)", len(got), len(tc.expected), tc.expected)
			}
			for k, want := range tc.expected {
				v, ok := got[k]
				if !ok || v == nil {
					t.Errorf("key %q missing, expected value %q", k, want)
					continue
				}
				if *v != want {
					t.Errorf("key %q = %q, expected %q", k, *v, want)
				}
			}
		})
	}
}

// FuzzPMetadata checks properties of PMetadata that hold for any input. Go
// fuzzing cannot generate maps, so each input is built from up to three
// key/value pairs; pairs whose keys collide simply overwrite each other, as
// they would in a map literal.
func FuzzPMetadata(f *testing.F) {
	seeds := []struct {
		description            string
		k1, v1, k2, v2, k3, v3 string
	}{
		{description: "lowercase key", k1: "owner", v1: "alice"},
		{description: "mixed-case key", k1: "Content-Owner", v1: "alice"},
		{description: "uppercase key", k1: "OWNER", v1: "alice"},
		{description: "empty key and value", k1: "", v1: ""},
		{description: "several mixed-case keys", k1: "A", v1: "1", k2: "bB", v2: "2", k3: "c", v3: "3"},
		{description: "non-ASCII uppercase key", k1: "ÄRGER", v1: "x"},
		{description: "keys that differ only in case", k1: "Key", v1: "upper", k2: "key", v2: "lower"},
		{description: "invalid UTF-8 key", k1: "\xffOwner", v1: "alice"},
	}
	for _, s := range seeds {
		f.Add(s.k1, s.v1, s.k2, s.v2, s.k3, s.v3)
	}

	f.Fuzz(func(t *testing.T, k1, v1, k2, v2, k3, v3 string) {
		input := map[string]string{k1: v1, k2: v2, k3: v3}

		// Expected: for each lowercased key, the values of the input keys
		// that lowercase to it.
		candidates := make(map[string][]string)
		for k, v := range input {
			lk := strings.ToLower(k)
			candidates[lk] = append(candidates[lk], v)
		}

		got := PMetadata(input)
		if got == nil {
			t.Fatalf("PMetadata(%q) returned nil", input)
		}
		if len(got) != len(candidates) {
			t.Fatalf("PMetadata(%q) returned %d keys, expected %d", input, len(got), len(candidates))
		}

		seen := make(map[*string]string)
		for k, v := range got {
			if strings.ToLower(k) != k {
				t.Errorf("key %q is not lowercase", k)
			}
			want, ok := candidates[k]
			if !ok {
				t.Errorf("unexpected key %q", k)
				continue
			}
			if v == nil {
				t.Errorf("key %q has a nil value, expected one of %q", k, want)
				continue
			}
			if !containsString(want, *v) {
				t.Errorf("key %q = %q, expected one of %q", k, *v, want)
			}
			if other, dup := seen[v]; dup {
				t.Errorf("keys %q and %q share a value pointer", other, k)
			}
			seen[v] = k
		}
	})
}

func containsString(list []string, s string) bool {
	for _, x := range list {
		if x == s {
			return true
		}
	}
	return false
}
