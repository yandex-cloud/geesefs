package main

import (
	"bytes"
	"testing"

	"github.com/urfave/cli"
	"github.com/yandex-cloud/geesefs/core/cfg"
)

func TestCheckArgs(t *testing.T) {
	cases := []struct {
		description      string
		args             []string
		expectedShowHelp bool
		expectErr        bool
	}{
		// Positive: --help and -h show the help and are not an error.
		{
			description:      "positive: --help alone shows help",
			args:             []string{"--help"},
			expectedShowHelp: true,
		},
		{
			description:      "positive: -h alone shows help",
			args:             []string{"-h"},
			expectedShowHelp: true,
		},
		{
			description:      "positive: --help before bucket and mount point shows help instead of mounting",
			args:             []string{"--help", "bucket", "/mnt"},
			expectedShowHelp: true,
		},
		// Negative: the wrong number of arguments is an error.
		{
			description: "negative: no arguments is an error",
			args:        []string{},
			expectErr:   true,
		},
		{
			description: "negative: bucket without mount point is an error",
			args:        []string{"bucket"},
			expectErr:   true,
		},
		// Boundary: exactly two arguments, and one too many.
		{
			description: "boundary: bucket and mount point proceed",
			args:        []string{"bucket", "/mnt"},
		},
		{
			description: "boundary: three arguments is an error",
			args:        []string{"bucket", "/mnt", "extra"},
			expectErr:   true,
		},
		// Corner: --help mixed with other flags, or explicitly turned off.
		{
			description:      "corner: --help after other flags shows help",
			args:             []string{"--debug", "--help"},
			expectedShowHelp: true,
		},
		{
			description: "corner: --help=false with bucket and mount point proceeds",
			args:        []string{"--help=false", "bucket", "/mnt"},
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			var showHelp bool
			var err error
			app := cfg.NewApp()
			app.Writer = &bytes.Buffer{}
			app.ErrWriter = &bytes.Buffer{}
			app.Action = func(c *cli.Context) error {
				showHelp, err = checkArgs(c)
				return nil
			}
			if runErr := app.Run(append([]string{"geesefs"}, tc.args...)); runErr != nil {
				t.Fatalf("app.Run: %v", runErr)
			}
			if showHelp != tc.expectedShowHelp {
				t.Errorf("showHelp = %v, expected %v", showHelp, tc.expectedShowHelp)
			}
			if (err != nil) != tc.expectErr {
				t.Errorf("error = %v, expected error: %v", err, tc.expectErr)
			}
		})
	}
}
