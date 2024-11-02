// +build !windows

package internal

import (
	"context"
	iofs "io/fs"
	"fmt"
	"syscall"
	"sync/atomic"

	"github.com/yandex-cloud/geesefs/internal/cfg"
	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/yandex-cloud/geesefs/internal/pb"
	"github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type ClusterFsFuse struct {
	fuseutil.NotImplementedFileSystem
	*ClusterFs
}

// fs

func (fs *ClusterFsFuse) StatFS(ctx context.Context, op *fuseops.StatFSOp) error {
	atomic.AddInt64(&fs.Goofys.stats.metadataReads, 1)

	const BLOCK_SIZE = 4096
	const TOTAL_SPACE = 1 * 1024 * 1024 * 1024 * 1024 * 1024 // 1PB
	const TOTAL_BLOCKS = TOTAL_SPACE / BLOCK_SIZE
	const INODES = 1 * 1000 * 1000 * 1000 // 1 billion
	op.BlockSize = BLOCK_SIZE
	op.Blocks = TOTAL_BLOCKS
	op.BlocksFree = TOTAL_BLOCKS
	op.BlocksAvailable = TOTAL_BLOCKS
	op.IoSize = 1 * 1024 * 1024 // 1MB
	op.Inodes = INODES
	op.InodesFree = INODES

	return nil
}

// file

func (fs *ClusterFsFuse) CreateFile(ctx context.Context, op *fuseops.CreateFileOp) (err error) {
	fs.routeByInodeId(
		op.Parent,
		false,
		func(parent *Inode) {
			pbInode, child, attr, handleId := fs.createFile(parent, op.Name, op.Mode)
			if pbInode == nil {
				err = syscall.EEXIST
				return
			}
			op.Entry.Child = fuseops.InodeID(child)
			op.Entry.Attributes.Size = attr.Size
			if parent.isDir() {
				op.Entry.Attributes.Nlink = 2
			} else {
				op.Entry.Attributes.Nlink = 1
			}
			op.Entry.Attributes.Mtime = attr.Mtime.AsTime()
			op.Entry.Attributes.Ctime = attr.Ctime.AsTime()
			op.Entry.Attributes.Mode = iofs.FileMode(attr.Mode)
			op.Entry.Attributes.Uid = 1000
			op.Entry.Attributes.Gid = 1001
			op.Handle = fuseops.HandleID(handleId)
		},
		func(parent *Inode, parentOwner NodeId) *pb.Owner {
			// 1st phase
			var resp *pb.CreateFileResponse
			err = fs.Conns.Unary(parentOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.CreateFileRequest{
					Parent: uint64(op.Parent),
					Name:   op.Name,
					Mode:   uint32(op.Mode),
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).CreateFile(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			if resp.Inode == nil {
				err = syscall.EEXIST
				return nil
			}

			// 2nd phase
			parent.StateLock()
			child := fs.ensure(parent, resp.Inode)
			fh := NewFileHandle(child)
			fs.Goofys.mu.Lock()
			fs.Goofys.fileHandles[fuseops.HandleID(resp.HandleId)] = fh
			fs.Goofys.mu.Unlock()
			child.StateUnlock()
			parent.StateUnlock()

			op.Entry.Child = fuseops.InodeID(resp.Child)
			op.Entry.Attributes.Size = resp.Attr.Size
			if parent.isDir() {
				op.Entry.Attributes.Nlink = 2
			} else {
				op.Entry.Attributes.Nlink = 1
			}
			op.Entry.Attributes.Mtime = resp.Attr.Mtime.AsTime()
			op.Entry.Attributes.Ctime = resp.Attr.Ctime.AsTime()
			op.Entry.Attributes.Mode = iofs.FileMode(resp.Attr.Mode)
			op.Entry.Attributes.Uid = 1000
			op.Entry.Attributes.Gid = 1001
			op.Handle = fuseops.HandleID(resp.HandleId)

			return nil
		},
	)
	return
}

func (fs *ClusterFsFuse) OpenFile(ctx context.Context, op *fuseops.OpenFileOp) (err error) {
	fs.routeByInodeId(
		op.Inode,
		true,
		func(inode *Inode) {
			op.Handle = fs.openFile(inode)
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			// 1st phase
			var resp *pb.OpenFileResponse
			err = fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.OpenFileRequest{
					InodeId: uint64(op.Inode),
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).OpenFile(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			// 2nd phase
			fs.Goofys.mu.Lock()
			fh := &FileHandle{inode: inode}
			fs.Goofys.fileHandles[fuseops.HandleID(resp.HandleId)] = fh
			fs.Goofys.mu.Unlock()

			op.Handle = fuseops.HandleID(resp.HandleId)

			return nil
		},
	)
	return
}

func (fs *ClusterFsFuse) ReadFile(ctx context.Context, op *fuseops.ReadFileOp) (err error) {
	fs.routeByFileHandle(
		op.Handle,
		func(inode *Inode) {
			op.Data, op.BytesRead, err = fs.readFile(op.Handle, op.Offset, op.Size)
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			var resp *pb.ReadFileResponse
			err = fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.ReadFileRequest{
					HandleId: uint64(op.Handle),
					Offset:   op.Offset,
					Size:     op.Size,
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).ReadFile(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			if resp.Errno != 0 {
				err = syscall.Errno(resp.Errno)
				return nil
			}

			op.Data = [][]byte{resp.Data}
			op.BytesRead = int(resp.BytesRead)

			return nil
		},
	)
	return
}

func (fs *ClusterFsFuse) WriteFile(ctx context.Context, op *fuseops.WriteFileOp) (err error) {
	fs.routeByFileHandle(
		op.Handle,
		func(inode *Inode) {
			op.SuppressReuse, err = fs.writeFile(op.Handle, op.Offset, op.Data)
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			var resp *pb.WriteFileResponse
			err = fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.WriteFileRequest{
					HandleId: uint64(op.Handle),
					Offset:   op.Offset,
					Data:     op.Data,
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).WriteFile(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			if resp.Errno != 0 {
				err = syscall.Errno(resp.Errno)
			}

			return nil
		},
	)
	return
}

func (fs *ClusterFsFuse) ReleaseFileHandle(ctx context.Context, op *fuseops.ReleaseFileHandleOp) (err error) {
	fs.routeByFileHandle(
		op.Handle,
		func(inode *Inode) {
			fs.releaseFileHandle(op.Handle)
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			// 1st phase
			var resp *pb.ReleaseFileHandleResponse
			err = fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.ReleaseFileHandleRequest{
					HandleId: uint64(op.Handle),
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).ReleaseFileHandle(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			// 2nd phase
			fs.Goofys.mu.Lock()
			delete(fs.Goofys.fileHandles, op.Handle)
			fs.Goofys.mu.Unlock()

			return nil
		},
	)
	return nil
}

func (fs *ClusterFsFuse) Unlink(ctx context.Context, op *fuseops.UnlinkOp) (err error) {
	fs.routeByInodeId(
		op.Parent,
		false,
		func(parent *Inode) {
			err = fs.unlink(parent, op.Name)
		},
		func(parent *Inode, parentOwner NodeId) *pb.Owner {
			var resp *pb.UnlinkResponse
			err = fs.Conns.Unary(parentOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.UnlinkRequest{
					Parent: uint64(op.Parent),
					Name:   op.Name,
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).Unlink(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			if resp.Errno != 0 {
				err = syscall.Errno(resp.Errno)
			}

			return nil
		},
	)
	return
}

// symlinks

func (fs *ClusterFsFuse) CreateSymlink(ctx context.Context, op *fuseops.CreateSymlinkOp) (err error) {
	fs.routeByInodeId(
		op.Parent,
		false,
		func(parent *Inode) {
			pbInode, childId, attr := fs.createSymlink(parent, op.Name, op.Target)
			if pbInode == nil {
				err = syscall.EEXIST
				return
			}
			op.Entry.Child = fuseops.InodeID(childId)
			op.Entry.Attributes.Mtime = attr.Mtime.AsTime()
			op.Entry.Attributes.Ctime = attr.Ctime.AsTime()
			op.Entry.Attributes.Mode = iofs.FileMode(attr.Mode)
			op.Entry.Attributes.Uid = 1000
			op.Entry.Attributes.Gid = 1001
		},
		func(parent *Inode, parentOwner NodeId) *pb.Owner {
			var resp *pb.CreateSymlinkResponse
			err = fs.Conns.Unary(parentOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.CreateSymlinkRequest{
					Parent: uint64(op.Parent),
					Name:   op.Name,
					Target: op.Target,
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).CreateSymlink(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			if resp.Inode == nil {
				err = syscall.EEXIST
				return nil
			}

			// 2nd phase
			parent.StateLock()
			child := fs.ensure(parent, resp.Inode)
			child.StateUnlock()
			parent.StateUnlock()

			op.Entry.Child = fuseops.InodeID(resp.Child)
			op.Entry.Attributes.Size = resp.Attr.Size
			if parent.isDir() {
				op.Entry.Attributes.Nlink = 2
			} else {
				op.Entry.Attributes.Nlink = 1
			}
			op.Entry.Attributes.Mtime = resp.Attr.Mtime.AsTime()
			op.Entry.Attributes.Ctime = resp.Attr.Ctime.AsTime()
			op.Entry.Attributes.Mode = iofs.FileMode(resp.Attr.Mode)
			op.Entry.Attributes.Uid = 1000
			op.Entry.Attributes.Gid = 1001

			return nil
		},
	)
	return err
}

func (fs *ClusterFsFuse) ReadSymlink(ctx context.Context, op *fuseops.ReadSymlinkOp) (err error) {
	fs.routeByInodeId(
		op.Inode,
		false,
		func(inode *Inode) {
			op.Target, err = fs.readSymlink(inode)
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			var resp *pb.ReadSymlinkResponse
			err = fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.ReadSymlinkRequest{
					Inode: uint64(op.Inode),
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).ReadSymlink(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			if resp.Errno != 0 {
				err = syscall.Errno(resp.Errno)
			}

			op.Target = resp.Target

			return nil
		},
	)
	return err
}

// dir

func (fs *ClusterFsFuse) MkDir(ctx context.Context, op *fuseops.MkDirOp) (err error) {
	fs.routeByInodeId(
		op.Parent,
		false,
		func(parent *Inode) {
			pbInode, child, attr := fs.mkDir(parent, op.Name, op.Mode)
			if pbInode == nil {
				err = syscall.EEXIST
				return
			}
			op.Entry.Child = fuseops.InodeID(child)
			op.Entry.Attributes.Size = attr.Size
			if parent.isDir() {
				op.Entry.Attributes.Nlink = 2
			} else {
				op.Entry.Attributes.Nlink = 1
			}
			op.Entry.Attributes.Mtime = attr.Mtime.AsTime()
			op.Entry.Attributes.Ctime = attr.Ctime.AsTime()
			op.Entry.Attributes.Mode = iofs.FileMode(attr.Mode)
			op.Entry.Attributes.Uid = 1000
			op.Entry.Attributes.Gid = 1001
		},
		func(parent *Inode, parentOwner NodeId) *pb.Owner {
			// 1st phase
			var resp *pb.MkDirResponse
			err = fs.Conns.Unary(parentOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.MkDirRequest{
					Parent: uint64(op.Parent),
					Name:   op.Name,
					Mode:   uint32(op.Mode),
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).MkDir(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			if resp.Inode == nil {
				err = syscall.EEXIST
				return nil
			}

			// 2nd phase
			parent.StateLock()
			child := fs.ensure(parent, resp.Inode) // parent.applyChildSnapshot(resp.ChildSnapshot)
			child.StateUnlock()
			parent.StateUnlock()

			op.Entry.Child = fuseops.InodeID(resp.Child)
			op.Entry.Attributes.Size = resp.Attr.Size
			if parent.isDir() {
				op.Entry.Attributes.Nlink = 2
			} else {
				op.Entry.Attributes.Nlink = 1
			}
			op.Entry.Attributes.Mtime = resp.Attr.Mtime.AsTime()
			op.Entry.Attributes.Ctime = resp.Attr.Ctime.AsTime()
			op.Entry.Attributes.Mode = iofs.FileMode(resp.Attr.Mode)
			op.Entry.Attributes.Uid = 1000
			op.Entry.Attributes.Gid = 1001

			return nil
		},
	)
	return
}

func (fs *ClusterFsFuse) OpenDir(ctx context.Context, op *fuseops.OpenDirOp) (err error) {
	fs.routeByInodeId(
		op.Inode,
		true,
		func(inode *Inode) {
			op.Handle = fs.openDir(inode)
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			// 1st phase
			var resp *pb.OpenDirResponse
			err = fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.OpenDirRequest{
					InodeId: uint64(op.Inode),
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).OpenDir(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			// 2nd phase
			fs.Goofys.mu.Lock()
			dh := &DirHandle{inode: inode}
			fs.Goofys.dirHandles[fuseops.HandleID(resp.HandleId)] = dh
			fs.Goofys.mu.Unlock()

			op.Handle = fuseops.HandleID(resp.HandleId)

			return nil
		},
	)
	return
}

func (fs *ClusterFsFuse) ReadDir(ctx context.Context, op *fuseops.ReadDirOp) (err error) {
	fs.routeByDirHandle(
		op.Handle,
		func(inode *Inode) {
			err = fs.readDir(op.Handle, op.Offset, op.Dst, &op.BytesRead)
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			var resp *pb.ReadDirResponse
			err = fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.ReadDirRequest{
					HandleId:  uint64(op.Handle),
					Offset:    uint64(op.Offset),
					Dst:       op.Dst,
					BytesRead: int32(op.BytesRead),
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).ReadDir(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			copy(op.Dst, resp.Dst)
			op.BytesRead = int(resp.BytesRead)

			return nil
		},
	)
	return
}

func (fs *ClusterFsFuse) ReleaseDirHandle(ctx context.Context, op *fuseops.ReleaseDirHandleOp) (err error) {
	fs.routeByDirHandle(
		op.Handle,
		func(inode *Inode) {
			fs.releaseDirHandle(op.Handle)
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			// 1st phase
			var resp *pb.ReleaseDirHandleResponse
			err = fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.ReleaseDirHandleRequest{
					HandleId: uint64(op.Handle),
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).ReleaseDirHandle(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			// 2nd phase
			fs.Goofys.mu.Lock()
			delete(fs.Goofys.dirHandles, op.Handle)
			fs.Goofys.mu.Unlock()

			return nil
		},
	)
	return nil
}

func (fs *ClusterFsFuse) LookUpInode(ctx context.Context, op *fuseops.LookUpInodeOp) (err error) {
	fs.routeByInodeId(
		op.Parent,
		false,
		func(parent *Inode) {
			var (
				child  uint64
				pbAttr *pb.Attributes
			)
			_, child, pbAttr, err = fs.lookUpInode1(parent, op.Name)
			if err != nil {
				return
			}
			op.Entry.Child = fuseops.InodeID(child)
			op.Entry.Attributes.Size = pbAttr.Size
			if pbAttr.Mode&uint32(iofs.ModeDir) != 0 {
				op.Entry.Attributes.Nlink = 2
			} else {
				op.Entry.Attributes.Nlink = 1
			}
			op.Entry.Attributes.Mtime = pbAttr.Mtime.AsTime()
			op.Entry.Attributes.Ctime = pbAttr.Ctime.AsTime()
			op.Entry.Attributes.Mode = iofs.FileMode(pbAttr.Mode)
			op.Entry.Attributes.Uid = 1000
			op.Entry.Attributes.Gid = 1001
		},
		func(parent *Inode, parentOwner NodeId) *pb.Owner {
			// 1st phase
			var resp *pb.LookUpInodeResponse
			err = fs.Conns.Unary(parentOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.LookUpInodeRequest{
					Parent: uint64(op.Parent),
					Name:   op.Name,
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).LookUpInode(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			if resp.Errno != 0 {
				err = syscall.Errno(resp.Errno)
				return nil
			}

			// 2nd phase
			parent.StateLock()
			child := fs.ensure(parent, resp.Inode)
			child.StateUnlock()
			parent.StateUnlock()

			op.Entry.Child = fuseops.InodeID(resp.Child)
			op.Entry.Attributes.Size = resp.Attr.Size
			if resp.Attr.Mode&uint32(iofs.ModeDir) != 0 {
				op.Entry.Attributes.Nlink = 2
			} else {
				op.Entry.Attributes.Nlink = 1
			}
			op.Entry.Attributes.Mtime = resp.Attr.Mtime.AsTime()
			op.Entry.Attributes.Ctime = resp.Attr.Ctime.AsTime()
			op.Entry.Attributes.Mode = iofs.FileMode(resp.Attr.Mode)
			op.Entry.Attributes.Uid = 1000
			op.Entry.Attributes.Gid = 1001

			return nil
		},
	)
	return
}

func (fs *ClusterFsFuse) RmDir(ctx context.Context, op *fuseops.RmDirOp) (err error) {
	fs.routeByInodeId(
		op.Parent,
		false,
		func(parent *Inode) {
			err = fs.rmDir(parent, op.Name)
		},
		func(parent *Inode, parentOwner NodeId) *pb.Owner {
			// 1st phase
			var resp *pb.RmDirResponse
			err = fs.Conns.Unary(parentOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.RmDirRequest{
					Parent: uint64(op.Parent),
					Name:   op.Name,
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).RmDir(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			if resp.Errno != 0 {
				err = syscall.Errno(resp.Errno)
			}

			return nil
		},
	)
	return
}

// both

func (fs *ClusterFsFuse) GetInodeAttributes(ctx context.Context, op *fuseops.GetInodeAttributesOp) (err error) {
	fs.routeByInodeId(
		op.Inode,
		false,
		func(inode *Inode) {
			fs.getInodeAttributes(inode, &op.Attributes.Size, &op.Attributes.Mtime,
				&op.Attributes.Ctime, &op.Attributes.Mode)
			if inode.isDir() {
				op.Attributes.Nlink = 2
			} else {
				op.Attributes.Nlink = 1
			}
			op.Attributes.Uid = 1000
			op.Attributes.Gid = 1001
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			var resp *pb.GetInodeAttributesResponse
			err = fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.GetInodeAttributesRequest{
					InodeId: uint64(op.Inode),
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).GetInodeAttributes(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			op.Attributes.Size = resp.Attributes.Size
			if inode.isDir() {
				op.Attributes.Nlink = 2
			} else {
				op.Attributes.Nlink = 1
			}
			op.Attributes.Mtime = resp.Attributes.Mtime.AsTime()
			op.Attributes.Ctime = resp.Attributes.Ctime.AsTime()
			op.Attributes.Mode = iofs.FileMode(resp.Attributes.Mode)
			op.Attributes.Uid = 1000
			op.Attributes.Gid = 1001

			return nil
		},
	)
	return
}

func (fs *ClusterFsFuse) SetInodeAttributes(ctx context.Context, op *fuseops.SetInodeAttributesOp) (err error) {
	fs.routeByInodeId(
		op.Inode,
		false,
		func(inode *Inode) {
			fs.setInodeAttributes(inode, &op.Attributes.Size, &op.Attributes.Mtime,
				&op.Attributes.Ctime, &op.Attributes.Mode)
			if inode.isDir() {
				op.Attributes.Nlink = 2
			} else {
				op.Attributes.Nlink = 1
			}
			op.Attributes.Uid = 1000
			op.Attributes.Gid = 1001
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			var mtime *timestamppb.Timestamp
			if op.Mtime != nil {
				mtime = timestamppb.New(*op.Mtime)
			}
			var resp *pb.SetInodeAttributesResponse
			err = fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.SetInodeAttributesRequest{
					InodeId: uint64(op.Inode),
					Size:    op.Size,
					Mode:    (*uint32)(op.Mode),
					Mtime:   mtime,
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).SetInodeAttributes(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			op.Attributes.Size = resp.Attributes.Size
			if inode.isDir() {
				op.Attributes.Nlink = 2
			} else {
				op.Attributes.Nlink = 1
			}
			op.Attributes.Mtime = resp.Attributes.Mtime.AsTime()
			op.Attributes.Ctime = resp.Attributes.Ctime.AsTime()
			op.Attributes.Mode = iofs.FileMode(resp.Attributes.Mode)
			op.Attributes.Uid = 1000
			op.Attributes.Gid = 1001

			return nil
		},
	)
	return
}

func (fs *ClusterFsFuse) ForgetInode(ctx context.Context, op *fuseops.ForgetInodeOp) (err error) {
	fs.routeByInodeId(
		op.Inode,
		false,
		func(inode *Inode) {
			inode.UpgradeToStateLock()
			forget := inode.DeRef(int64(op.N))
			if forget {
				fs.Conns.Broad(func(ctx context.Context, conn *grpc.ClientConn) error {
					req := &pb.ForgetInode2Request{
						InodeId: uint64(inode.Id),
					}
					_, err = pb.NewFsGrpcClient(conn).ForgetInode2(ctx, req)
					return err
				})
			}
			inode.DowngradeToKeepOwnerLock()
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			var resp *pb.ForgetInodeResponse
			err = fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.ForgetInodeRequest{
					InodeId: uint64(op.Inode),
					N:       op.N,
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).ForgetInode(ctx, req)
				return err
			})
			if err != nil {
				err = syscall.EIO
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			return nil
		},
	)
	return nil
}

func MountCluster(
	ctx context.Context,
	bucketName string,
	flags *cfg.FlagStorage,
) (*Goofys, MountedFS, error) {

	if flags.DebugS3 {
		cfg.SetCloudLogLevel(logrus.DebugLevel)
	}

	mountConfig := &fuse.MountConfig{
		FSName:                  bucketName,
		Subtype:                 "geesefs",
		Options:                 convertFuseOptions(flags),
		ErrorLogger:             cfg.GetStdLogger(cfg.NewLogger("fuse"), logrus.ErrorLevel),
		DisableWritebackCaching: true,
		UseVectoredRead:         true,
		FuseImpl:                fuse.FUSEImplMacFUSE,
	}

	if flags.DebugFuse {
		fuseLog := cfg.GetLogger("fuse")
		fuseLog.Level = logrus.DebugLevel
		mountConfig.DebugLogger = cfg.GetStdLogger(fuseLog, logrus.DebugLevel)
	}

	if flags.DebugFuse || flags.DebugMain {
		log.Level = logrus.DebugLevel
	}

	if flags.DebugGrpc {
		grpcLog := cfg.GetLogger("grpc")
		grpcLog.Level = logrus.DebugLevel
	}

	srv := NewGrpcServer(flags)
	conns := NewConnPool(flags)
	rec := &Recovery{
		Flags: flags,
	}
	goofys, err := NewClusterGoofys(context.Background(), bucketName, flags, conns)
	if err != nil {
		return nil, nil, err
	}
	fs := &ClusterFs{
		Flags:  flags,
		Conns:  conns,
		Goofys: goofys,
	}
	go fs.StatPrinter()

	pb.RegisterRecoveryServer(srv, rec)
	pb.RegisterFsGrpcServer(srv, &ClusterFsGrpc{ClusterFs: fs})

	go func() {
		err := srv.Start()
		if err != nil {
			panic(err)
		}
	}()

	mfs, err := fuse.Mount(
		flags.MountPoint,
		fuseutil.NewFileSystemServer(&ClusterFsFuse{ClusterFs: fs}),
		mountConfig,
	)
	if err != nil {
		err = fmt.Errorf("Mount: %v", err)
		return nil, nil, err
	}
	fs.mfs = mfs

	return goofys, fs, nil
}

func (fs *ClusterFs) Join(ctx context.Context) error {
	err := fs.mfs.Join(ctx)
	if err != nil {
		return err
	}
	if fs.Conns != nil {
		_ = fs.Conns.BroadConfigurable(
			func(ctx context.Context, conn *grpc.ClientConn) error {
				_, err := pb.NewRecoveryClient(conn).Unmount(ctx, &pb.UnmountRequest{})
				return err
			},
			false,
		)
	}
	return nil
}

func (fs *ClusterFs) Unmount() error {
	return TryUnmount(fs.Flags.MountPoint)
}
