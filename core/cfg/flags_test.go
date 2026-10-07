package cfg_test

import (
	"io"
	"testing"

	"github.com/urfave/cli"
	"github.com/yandex-cloud/geesefs/core/cfg"
)

func TestNegativeLookupCacheSize(t *testing.T) {
	if got := cfg.DefaultFlags().NegativeLookupCacheSize; got != 0 {
		t.Fatalf("default cache size = %d, want 0", got)
	}
	for _, tc := range []struct {
		name string
		args []string
		want int
	}{
		{"default", nil, 0},
		{"disabled", []string{"--negative-lookup-cache-size=0"}, 0},
		{"one", []string{"--negative-lookup-cache-size=1"}, 1},
		{"three", []string{"--negative-lookup-cache-size=3"}, 3},
	} {
		t.Run(tc.name, func(t *testing.T) {
			app := cfg.NewApp()
			app.Writer, app.ErrWriter = io.Discard, io.Discard
			var flags *cfg.FlagStorage
			app.Action = func(c *cli.Context) error {
				flags = cfg.PopulateFlags(c)
				return nil
			}
			args := append([]string{"geesefs"}, tc.args...)
			if err := app.Run(append(args, "test", "mountpoint")); err != nil {
				t.Fatal(err)
			}
			if flags == nil || flags.NegativeLookupCacheSize != tc.want {
				t.Fatalf("parsed flags = %+v, want cache size %d", flags, tc.want)
			}
		})
	}
}

func TestNegativeLookupCacheSizeRejectsNegative(t *testing.T) {
	app := cfg.NewApp()
	app.Writer, app.ErrWriter = io.Discard, io.Discard
	app.Action = func(c *cli.Context) error {
		cfg.PopulateFlags(c)
		return nil
	}
	defer func() {
		if got := recover(); got != "--negative-lookup-cache-size must not be negative" {
			t.Fatalf("negative size produced %v, want validation panic", got)
		}
	}()
	if err := app.Run([]string{"geesefs", "--negative-lookup-cache-size=-1", "test", "mountpoint"}); err != nil {
		t.Fatal(err)
	}
}
