package internal

import (
	"context"

	. "github.com/yandex-cloud/geesefs/api/common"
	"github.com/yandex-cloud/geesefs/internal/pb"
)

var recLog = GetLogger("rec")

type Recovery struct {
	pb.UnimplementedRecoveryServer
	Flags *FlagStorage
}

func (rec *Recovery) Unmount(ctx context.Context, req *pb.UnmountRequest) (*pb.UnmountResponse, error) {
	go TryUnmount(rec.Flags.MountPoint)
	return &pb.UnmountResponse{}, nil
}
