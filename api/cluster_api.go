package goofys

import (
	"context"
	"fmt"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/sirupsen/logrus"
	. "github.com/yandex-cloud/geesefs/api/common"
	"github.com/yandex-cloud/geesefs/internal"
	"github.com/yandex-cloud/geesefs/internal/pb"
)

func MountCluster(
	ctx context.Context,
	bucketName string,
	flags *FlagStorage,
) (*Goofys, *fuse.MountedFileSystem, *internal.ConnPool, error) {

	if flags.DebugS3 {
		SetCloudLogLevel(logrus.DebugLevel)
	}

	mountConfig := &fuse.MountConfig{
		FSName:                  bucketName,
		Subtype:                 "geesefs",
		Options:                 flags.MountOptions,
		ErrorLogger:             GetStdLogger(NewLogger("fuse"), logrus.ErrorLevel),
		DisableWritebackCaching: true,
		UseVectoredRead:         true,
	}

	if flags.DebugFuse {
		fuseLog := GetLogger("fuse")
		fuseLog.Level = logrus.DebugLevel
		mountConfig.DebugLogger = GetStdLogger(fuseLog, logrus.DebugLevel)
	}

	if flags.DebugFuse || flags.DebugMain {
		log.Level = logrus.DebugLevel
	}

	if flags.DebugGrpc {
		grpcLog := GetLogger("grpc")
		grpcLog.Level = logrus.DebugLevel
	}

	srv := internal.NewGrpcServer(flags)
	conns := internal.NewConnPool(flags)
	rec := &internal.Recovery{
		Flags: flags,
	}
	goofys := internal.NewClusterGoofys(context.Background(), bucketName, flags, conns)
	fs := &internal.Fs{
		Flags:  flags,
		Conns:  conns,
		Goofys: goofys,
	}
	go fs.StatPrinter()

	pb.RegisterRecoveryServer(srv, rec)
	pb.RegisterFsGrpcServer(srv, &internal.FsGrpc{Fs: fs})

	go func() {
		err := srv.Start()
		if err != nil {
			panic(err)
		}
	}()

	mfs, err := fuse.Mount(
		flags.MountPoint,
		fuseutil.NewFileSystemServer(&internal.FsFuse{Fs: fs}),
		mountConfig,
	)
	if err != nil {
		err = fmt.Errorf("Mount: %v", err)
		return nil, nil, nil, err
	}

	return goofys, mfs, conns, nil
}
