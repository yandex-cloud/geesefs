//go:build linux

package cfg

import (
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRedirectStandardStreams(t *testing.T) {
	for _, stream := range []string{"stdout", "stderr"} {
		t.Run(stream, func(t *testing.T) {
			output := filepath.Join(t.TempDir(), stream)
			cmd := exec.Command(os.Args[0], "-test.run=TestRedirectStandardStreamsHelper")
			cmd.Env = append(os.Environ(),
				"GO_WANT_REDIRECT_HELPER=1",
				"GO_REDIRECT_STREAM="+stream,
				"GO_REDIRECT_OUTPUT="+output,
			)

			if combined, err := cmd.CombinedOutput(); err != nil {
				t.Fatalf("helper failed: %v\n%s", err, combined)
			}

			got, err := os.ReadFile(output)
			if err != nil {
				t.Fatal(err)
			}
			if string(got) != stream {
				t.Fatalf("redirected output = %q, want %q", got, stream)
			}
		})
	}
}

func TestRedirectStandardStreamsHelper(t *testing.T) {
	if os.Getenv("GO_WANT_REDIRECT_HELPER") != "1" {
		return
	}

	stream := os.Getenv("GO_REDIRECT_STREAM")
	file, err := os.Create(os.Getenv("GO_REDIRECT_OUTPUT"))
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()

	switch stream {
	case "stdout":
		err = redirectStdout(file)
	case "stderr":
		err = redirectStderr(file)
	default:
		t.Fatalf("unknown stream %q", stream)
	}
	if err != nil {
		t.Fatal(err)
	}

	if stream == "stdout" {
		fmt.Fprint(os.Stdout, stream)
	} else {
		fmt.Fprint(os.Stderr, stream)
	}
	if err := file.Sync(); err != nil {
		t.Fatal(err)
	}
	os.Exit(0)
}
