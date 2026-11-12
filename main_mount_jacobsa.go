//go:build !windows && !fuset

package main

import (
	"context"

	"github.com/yandex-cloud/geesefs/core"
	"github.com/yandex-cloud/geesefs/core/cfg"
)

// Mount the file system based on the supplied arguments, returning a
// MountedFS that can be joined to wait for unmounting.
func mount(
	ctx context.Context,
	bucketName string,
	flags *cfg.FlagStorage) (fs *core.Goofys, mfs core.MountedFS, err error) {
	if flags.ClusterMode {
		return core.MountCluster(ctx, bucketName, flags)
	} else {
		return core.MountFuse(ctx, bucketName, flags)
	}
}
