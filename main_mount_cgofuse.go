//go:build fuset

package main

import (
	"context"
	"fmt"

	"github.com/yandex-cloud/geesefs/core"
	"github.com/yandex-cloud/geesefs/core/cfg"
)

// Build with `-tags fuset` on macOS to mount through cgofuse and FUSE-T
// (libfuse-t, NFS-based, no kernel extension) instead of jacobsa/fuse,
// which only speaks to the macFUSE/OSXFUSE kernel driver.
func mount(
	ctx context.Context,
	bucketName string,
	flags *cfg.FlagStorage) (fs *core.Goofys, mfs core.MountedFS, err error) {
	if flags.ClusterMode {
		return nil, nil, fmt.Errorf("cluster mode is not supported with the cgofuse (fuset) backend")
	}
	return core.MountWin(ctx, bucketName, flags)
}
