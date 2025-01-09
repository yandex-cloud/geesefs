//go:build !windows

package core

import (
	"context"
	iofs "io/fs"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/yandex-cloud/geesefs/core/pb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ClusterFsGrpc struct {
	pb.UnimplementedFsGrpcServer
	*ClusterFs
}

func (fs *ClusterFsGrpc) TryStealInodeOwnership(ctx context.Context, req *pb.TryStealInodeOwnershipRequest) (*pb.TryStealInodeOwnershipResponse, error) {
	inode := fs.inodeById(fuseops.InodeID(req.InodeId))

	inode.ChangeOwnerLock()

	if inode.owner != fs.Conns.id {
		inode.ChangeOwnerUnlock()
		return &pb.TryStealInodeOwnershipResponse{AnotherOwner: inode.pbOwner()}, nil
	}

	resp := &pb.TryStealInodeOwnershipResponse{
		StolenInode: fs.tryYield(inode, NodeId(req.NodeId)),
	}

	inode.ChangeOwnerUnlock()

	return resp, nil
}

// file

func (fs *ClusterFsGrpc) CreateFile(ctx context.Context, req *pb.CreateFileRequest) (*pb.CreateFileResponse, error) {
	parent := fs.inodeById(fuseops.InodeID(req.Parent))

	parent.KeepOwnerLock()

	if parent.owner != fs.Conns.id {
		parent.KeepOwnerUnlock()
		return &pb.CreateFileResponse{AnotherOwner: parent.pbOwner()}, nil
	}

	pbInode, child, attr, handleId := fs.createFile(parent, req.Name, iofs.FileMode(req.Mode))

	parent.KeepOwnerUnlock()

	return &pb.CreateFileResponse{
		Inode:    pbInode,
		Child:    child,
		Attr:     attr,
		HandleId: uint64(handleId),
	}, nil
}

func (fs *ClusterFsGrpc) OpenFile(ctx context.Context, req *pb.OpenFileRequest) (*pb.OpenFileResponse, error) {
	parent := fs.inodeById(fuseops.InodeID(req.InodeId))

	parent.KeepOwnerLock()

	if parent.owner != fs.Conns.id {
		parent.KeepOwnerUnlock()
		return &pb.OpenFileResponse{AnotherOwner: parent.pbOwner()}, nil
	}

	handleId := fs.openFile(parent)

	parent.KeepOwnerUnlock()

	return &pb.OpenFileResponse{
		HandleId: uint64(handleId),
	}, nil
}

func (fs *ClusterFsGrpc) ReadFile(ctx context.Context, req *pb.ReadFileRequest) (*pb.ReadFileResponse, error) {
	parent := fs.inodeByFileHandleId(fuseops.HandleID(req.HandleId))

	parent.KeepOwnerLock()

	if parent.owner != fs.Conns.id {
		parent.KeepOwnerUnlock()
		return &pb.ReadFileResponse{AnotherOwner: parent.pbOwner()}, nil
	}

	data, bytesRead, err := fs.readFile(fuseops.HandleID(req.HandleId), req.Offset, req.Size)

	parent.KeepOwnerUnlock()

	if err != nil {
		return &pb.ReadFileResponse{Errno: uint64(toErrno(err))}, nil
	}

	var dataCollapsed []byte
	for i := range data {
		dataCollapsed = append(dataCollapsed, data[i]...)
	}

	return &pb.ReadFileResponse{
		BytesRead: int32(bytesRead),
		Data:      dataCollapsed,
	}, nil
}

func (fs *ClusterFsGrpc) WriteFile(ctx context.Context, req *pb.WriteFileRequest) (*pb.WriteFileResponse, error) {
	parent := fs.inodeByFileHandleId(fuseops.HandleID(req.HandleId))

	parent.KeepOwnerLock()

	if parent.owner != fs.Conns.id {
		parent.KeepOwnerUnlock()
		return &pb.WriteFileResponse{AnotherOwner: parent.pbOwner()}, nil
	}

	_, err := fs.writeFile(fuseops.HandleID(req.HandleId), req.Offset, req.Data)

	parent.KeepOwnerUnlock()

	if err != nil {
		return &pb.WriteFileResponse{Errno: uint64(toErrno(err))}, nil
	}

	return &pb.WriteFileResponse{}, nil
}

func (fs *ClusterFsGrpc) ReleaseFileHandle(ctx context.Context, req *pb.ReleaseFileHandleRequest) (*pb.ReleaseFileHandleResponse, error) {
	inode := fs.inodeByFileHandleId(fuseops.HandleID(req.HandleId))

	inode.KeepOwnerLock()

	if inode.owner != fs.Conns.id {
		inode.KeepOwnerUnlock()
		return &pb.ReleaseFileHandleResponse{AnotherOwner: inode.pbOwner()}, nil
	}

	inode.KeepOwnerUnlock()

	fs.releaseFileHandle(fuseops.HandleID(req.HandleId))

	return &pb.ReleaseFileHandleResponse{}, nil
}

func (fs *ClusterFsGrpc) Unlink(ctx context.Context, req *pb.UnlinkRequest) (*pb.UnlinkResponse, error) {
	parent := fs.inodeById(fuseops.InodeID(req.Parent))

	parent.KeepOwnerLock()

	if parent.owner != fs.Conns.id {
		parent.KeepOwnerUnlock()
		return &pb.UnlinkResponse{AnotherOwner: parent.pbOwner()}, nil
	}

	err := fs.unlink(parent, req.Name)

	parent.KeepOwnerUnlock()

	return &pb.UnlinkResponse{Errno: uint64(toErrno(err))}, nil
}

func (fs *ClusterFsGrpc) CreateSymlink(ctx context.Context, req *pb.CreateSymlinkRequest) (*pb.CreateSymlinkResponse, error) {
	parent := fs.inodeById(fuseops.InodeID(req.Parent))

	parent.KeepOwnerLock()

	if parent.owner != fs.Conns.id {
		parent.KeepOwnerUnlock()
		return &pb.CreateSymlinkResponse{AnotherOwner: parent.pbOwner()}, nil
	}

	pbInode, child, attr := fs.createSymlink(parent, req.Name, req.Target)

	parent.KeepOwnerUnlock()

	return &pb.CreateSymlinkResponse{
		Inode: pbInode,
		Child: child,
		Attr:  attr,
	}, nil
}

func (fs *ClusterFsGrpc) ReadSymlink(ctx context.Context, req *pb.ReadSymlinkRequest) (*pb.ReadSymlinkResponse, error) {
	inode := fs.inodeById(fuseops.InodeID(req.Inode))

	inode.KeepOwnerLock()

	if inode.owner != fs.Conns.id {
		inode.KeepOwnerUnlock()
		return &pb.ReadSymlinkResponse{AnotherOwner: inode.pbOwner()}, nil
	}

	target, err := fs.readSymlink(inode)

	inode.KeepOwnerUnlock()

	return &pb.ReadSymlinkResponse{
		Target: target,
		Errno:  uint64(toErrno(err)),
	}, nil
}

// dir

func (fs *ClusterFsGrpc) MkDir(ctx context.Context, req *pb.MkDirRequest) (*pb.MkDirResponse, error) {
	parent := fs.inodeById(fuseops.InodeID(req.Parent))

	parent.KeepOwnerLock()

	if parent.owner != fs.Conns.id {
		parent.KeepOwnerUnlock()
		return &pb.MkDirResponse{AnotherOwner: parent.pbOwner()}, nil
	}

	pbInode, child, attr := fs.mkDir(parent, req.Name, iofs.FileMode(req.Mode))

	parent.KeepOwnerUnlock()

	return &pb.MkDirResponse{
		Inode: pbInode,
		Child: child,
		Attr:  attr,
	}, nil
}

func (fs *ClusterFsGrpc) OpenDir(ctx context.Context, req *pb.OpenDirRequest) (*pb.OpenDirResponse, error) {
	inode := fs.inodeById(fuseops.InodeID(req.InodeId))

	inode.KeepOwnerLock()

	if inode.owner != fs.Conns.id {
		inode.KeepOwnerUnlock()
		return &pb.OpenDirResponse{AnotherOwner: inode.pbOwner()}, nil
	}

	handleId := fs.openDir(inode)

	inode.KeepOwnerUnlock()

	return &pb.OpenDirResponse{
		HandleId: uint64(handleId),
	}, nil
}

func (fs *ClusterFsGrpc) ReadDir(ctx context.Context, req *pb.ReadDirRequest) (*pb.ReadDirResponse, error) {
	inode := fs.inodeByDirHandleId(fuseops.HandleID(req.HandleId))

	inode.KeepOwnerLock()

	if inode.owner != fs.Conns.id {
		inode.KeepOwnerUnlock()
		return &pb.ReadDirResponse{AnotherOwner: inode.pbOwner()}, nil
	}

	dst := make([]byte, len(req.Dst))
	copy(dst, req.Dst)
	bytesRead := int(req.BytesRead)
	err := fs.readDir(
		fuseops.HandleID(req.HandleId),
		fuseops.DirOffset(req.Offset),
		dst,
		&bytesRead,
	)

	inode.KeepOwnerUnlock()

	if err != nil {
		return &pb.ReadDirResponse{Errno: uint64(toErrno(err))}, nil
	}

	return &pb.ReadDirResponse{
		Dst:       dst,
		BytesRead: int32(bytesRead),
	}, nil
}

func (fs *ClusterFsGrpc) ReleaseDirHandle(ctx context.Context, req *pb.ReleaseDirHandleRequest) (*pb.ReleaseDirHandleResponse, error) {
	inode := fs.inodeByDirHandleId(fuseops.HandleID(req.HandleId))

	inode.ownerMu.RLock()

	if inode.owner != fs.Conns.id {
		inode.ownerMu.RUnlock()
		return &pb.ReleaseDirHandleResponse{AnotherOwner: inode.pbOwner()}, nil
	}

	inode.ownerMu.RUnlock()

	fs.releaseDirHandle(fuseops.HandleID(req.HandleId))

	return &pb.ReleaseDirHandleResponse{}, nil
}

func (fs *ClusterFsGrpc) LookUpInode(ctx context.Context, req *pb.LookUpInodeRequest) (*pb.LookUpInodeResponse, error) {
	parent := fs.inodeById(fuseops.InodeID(req.Parent))

	parent.KeepOwnerLock()

	if parent.owner != fs.Conns.id {
		parent.KeepOwnerUnlock()
		return &pb.LookUpInodeResponse{AnotherOwner: parent.pbOwner()}, nil
	}

	pbInode, child, pbAttr, err := fs.lookUpInode1(parent, req.Name)

	parent.KeepOwnerUnlock()

	return &pb.LookUpInodeResponse{
		Inode: pbInode,
		Child: child,
		Attr:  pbAttr,
		Errno: uint64(toErrno(err)),
	}, nil
}

func (fs *ClusterFsGrpc) LookUpInode2(ctx context.Context, req *pb.LookUpInode2Request) (*pb.LookUpInode2Response, error) {
	inode := fs.inodeById(fuseops.InodeID(req.InodeId))

	inode.KeepOwnerLock()

	if inode.owner != fs.Conns.id {
		inode.KeepOwnerUnlock()
		return &pb.LookUpInode2Response{AnotherOwner: inode.pbOwner()}, nil
	}

	inode.UpgradeToStateLock()

	pbAttr := inode.pbAttr()
	inode.Ref()

	inode.StateUnlock()

	return &pb.LookUpInode2Response{
		Attr: pbAttr,
	}, nil
}

func (fs *ClusterFsGrpc) RmDir(ctx context.Context, req *pb.RmDirRequest) (*pb.RmDirResponse, error) {
	parent := fs.inodeById(fuseops.InodeID(req.Parent))

	parent.KeepOwnerLock()

	if parent.owner != fs.Conns.id {
		parent.KeepOwnerUnlock()
		return &pb.RmDirResponse{AnotherOwner: parent.pbOwner()}, nil
	}

	err := fs.rmDir(parent, req.Name)

	parent.KeepOwnerUnlock()

	return &pb.RmDirResponse{Errno: uint64(toErrno(err))}, nil
}

// both

func (fs *ClusterFsGrpc) GetInodeAttributes(ctx context.Context, req *pb.GetInodeAttributesRequest) (*pb.GetInodeAttributesResponse, error) {
	inode := fs.inodeById(fuseops.InodeID(req.InodeId))

	inode.KeepOwnerLock()

	if inode.owner != fs.Conns.id {
		inode.KeepOwnerUnlock()
		return &pb.GetInodeAttributesResponse{AnotherOwner: inode.pbOwner()}, nil
	}

	var size uint64
	var mtime time.Time
	var ctime time.Time
	var mode iofs.FileMode
	fs.getInodeAttributes(inode, &size, &mtime, &ctime, &mode)

	inode.KeepOwnerUnlock()

	return &pb.GetInodeAttributesResponse{
		Attributes: &pb.Attributes{
			Size:  size,
			Mtime: timestamppb.New(mtime),
			Ctime: timestamppb.New(ctime),
			Mode:  uint32(mode),
		},
	}, nil
}

func (fs *ClusterFsGrpc) SetInodeAttributes(ctx context.Context, req *pb.SetInodeAttributesRequest) (*pb.SetInodeAttributesResponse, error) {
	inode := fs.inodeById(fuseops.InodeID(req.InodeId))

	inode.KeepOwnerLock()

	if inode.owner != fs.Conns.id {
		inode.ownerMu.RUnlock()
		return &pb.SetInodeAttributesResponse{AnotherOwner: inode.pbOwner()}, nil
	}

	var size uint64
	if req.Size != nil {
		size = *req.Size
	}
	var mtime time.Time
	if req.Mtime != nil {
		mtime = req.Mtime.AsTime()
	}
	var ctime time.Time
	if req.Ctime != nil {
		ctime = req.Ctime.AsTime()
	}
	var mode iofs.FileMode
	if req.Mode != nil {
		mode = iofs.FileMode(*req.Mode)
	}
	fs.setInodeAttributes(inode, &size, &mtime, &ctime, &mode)

	inode.KeepOwnerUnlock()

	return &pb.SetInodeAttributesResponse{
		Attributes: &pb.Attributes{
			Size:  size,
			Mtime: timestamppb.New(mtime),
			Ctime: timestamppb.New(ctime),
			Mode:  uint32(mode),
		},
	}, nil
}

func (fs *ClusterFsGrpc) ForgetInode(ctx context.Context, req *pb.ForgetInodeRequest) (*pb.ForgetInodeResponse, error) {
	inode := fs.inodeById(fuseops.InodeID(req.InodeId))

	inode.KeepOwnerLock()

	if inode.owner != fs.Conns.id {
		inode.KeepOwnerUnlock()
		return &pb.ForgetInodeResponse{AnotherOwner: inode.pbOwner()}, nil
	}

	inode.UpgradeToStateLock()

	forget := inode.DeRef(int64(req.N))
	if forget {
		atomic.AddUint64(&fs.stat.forgetInodeCnt, 1)
		fs.broadcastForget2(inode.Id)
	}

	inode.StateUnlock()

	return &pb.ForgetInodeResponse{}, nil
}

func (fs *ClusterFsGrpc) ForgetInode2(ctx context.Context, req *pb.ForgetInode2Request) (*pb.ForgetInode2Response, error) {
	fs.Goofys.mu.RLock()
	inode := fs.Goofys.inodes[fuseops.InodeID(req.InodeId)]
	fs.Goofys.mu.RUnlock()

	if inode == nil {
		return &pb.ForgetInode2Response{}, nil
	}

	atomic.AddUint64(&fs.stat.forgetInodeCnt, 1)

	inode.resetCache()
	inode.fs.mu.Lock()
	// FIXME: These lines may have a bug (check). Only expired inodes should be forgotten
	inode.resetExpireTime()
	delete(inode.fs.inodes, inode.Id)
	inode.fs.mu.Unlock()

	return &pb.ForgetInode2Response{}, nil
}

// utils

func toErrno(err error) syscall.Errno {
	if err != nil {
		return err.(syscall.Errno)
	} else {
		return 0
	}
}
