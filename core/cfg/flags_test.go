// Copyright 2026 Yandex LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package cfg

import (
	"testing"

	"github.com/urfave/cli"
)

func TestReadETagCheckFlag(t *testing.T) {
	tests := []struct {
		name string
		args []string
		want bool
	}{
		{name: "disabled by default", args: []string{"bucket", "mountpoint"}},
		{name: "enabled", args: []string{"--enable-read-etag-check", "bucket", "mountpoint"}, want: true},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			app := NewApp()
			var got bool
			app.Action = func(c *cli.Context) error {
				flags := PopulateFlags(c)
				if flags == nil {
					t.Fatal("PopulateFlags returned nil")
				}
				got = flags.EnableReadETagCheck
				return nil
			}

			args := append([]string{"geesefs"}, tc.args...)
			if err := app.Run(args); err != nil {
				t.Fatalf("Run: %v", err)
			}
			if got != tc.want {
				t.Fatalf("EnableReadETagCheck = %v, want %v", got, tc.want)
			}
		})
	}
}
