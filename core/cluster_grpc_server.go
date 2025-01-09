//go:build !windows

package core

import (
	"context"
	"net"

	"github.com/yandex-cloud/geesefs/core/cfg"
	"github.com/yandex-cloud/geesefs/core/pb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/reflection"
)

var grpcLog = cfg.GetLogger("grpc")

type GrpcServer struct {
	*grpc.Server
	flags *cfg.FlagStorage
}

func NewGrpcServer(flags *cfg.FlagStorage) *GrpcServer {
	return &GrpcServer{
		Server: grpc.NewServer(grpc.ChainUnaryInterceptor(
			LogServerInterceptor,
		)),
		flags: flags,
	}
}

func (srv *GrpcServer) Start() error {
	grpcLog.Info("start server")
	lis, err := net.Listen("tcp", srv.flags.ClusterMe.Address)
	if err != nil {
		return err
	}
	if srv.flags.ClusterGrpcReflection {
		grpcLog.Info("enable grpc reflection")
		reflection.Register(srv)
	}
	if err = srv.Serve(lis); err != nil {
		return err
	}
	return nil
}

const (
	SRC_NODE_ID_METADATA_KEY = "src-node-id"
	DST_NODE_ID_METADATA_KEY = "dst-node-id"
)

var traceLog = cfg.GetLogger("trace")

func LogServerInterceptor(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (resp interface{}, err error) {
	// theese requests generate a lot of bytes of logs, so disable it for now
	_, ok1 := req.(*pb.WriteFileRequest)
	_, ok2 := req.(*pb.ReadFileRequest)
	_, ok3 := req.(*pb.ReadDirRequest)
	ok := ok1 || ok2 || ok3

	src := "<unknown>"
	dst := "<unknown>"
	md, okMd := metadata.FromIncomingContext(ctx)
	if okMd {
		if keys, okKeys := md[SRC_NODE_ID_METADATA_KEY]; okKeys {
			if len(keys) >= 1 {
				src = keys[0]
			}
		}
		if keys, okKeys := md[DST_NODE_ID_METADATA_KEY]; okKeys {
			if len(keys) >= 1 {
				dst = keys[0]
			}
		}
	}

	if !ok {
		traceLog.Debug(src, " --> ", dst, " : ", info.FullMethod, " : ", req)
	}
	resp, err = handler(ctx, req)
	if !ok {
		traceLog.Debug(src, " <-- ", dst, " : ", info.FullMethod, " : ", resp)
	}
	return
}

func LogClientInterceptor(ctx context.Context, method string, req, resp interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
	// theese requests generate a lot of bytes of logs, so disable it for now
	_, ok1 := req.(*pb.WriteFileRequest)
	_, ok2 := req.(*pb.ReadFileRequest)
	_, ok3 := req.(*pb.ReadDirRequest)
	ok := ok1 || ok2 || ok3

	src := "<unknown>"
	dst := "<unknown>"
	md, okMd := metadata.FromOutgoingContext(ctx)
	if okMd {
		if keys, okKeys := md[SRC_NODE_ID_METADATA_KEY]; okKeys {
			if len(keys) >= 1 {
				src = keys[0]
			}
		}
		if keys, okKeys := md[DST_NODE_ID_METADATA_KEY]; okKeys {
			if len(keys) >= 1 {
				dst = keys[0]
			}
		}
	}

	if !ok {
		traceLog.Debug(src, " --> ", dst, " : ", method, " : ", req)
	}
	err := invoker(ctx, method, req, resp, cc, opts...)
	if !ok {
		traceLog.Debug(src, " <-- ", dst, " : ", method, " : ", resp)
	}
	return err
}
