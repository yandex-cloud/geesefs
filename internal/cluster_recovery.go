// +build !windows

package internal

import (
	"context"

	"github.com/yandex-cloud/geesefs/internal/cfg"
	"github.com/yandex-cloud/geesefs/internal/pb"
)

var recLog = cfg.GetLogger("rec")

type Recovery struct {
	pb.UnimplementedRecoveryServer
	Flags *cfg.FlagStorage
}

func (rec *Recovery) Unmount(ctx context.Context, req *pb.UnmountRequest) (*pb.UnmountResponse, error) {
	go TryUnmount(rec.Flags.MountPoint)
	return &pb.UnmountResponse{}, nil
}
