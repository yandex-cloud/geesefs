// Copyright 2015 - 2017 Ka-Hing Cheung
// Copyright 2021 Yandex LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// +build !windows

package internal

import (
	. "github.com/yandex-cloud/geesefs/api/common"

	"context"
	"fmt"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/sirupsen/logrus"
)

// jacobsa/fuse interface to the file system

type GoofysFuse struct {
	fuseutil.NotImplementedFileSystem
	*Goofys
	connection *fuse.Connection
}

func NewGoofysFuse(fs *Goofys) *GoofysFuse {
	fsint := &GoofysFuse{
		Goofys: fs,
	}
	fs.NotifyCallback = func(notifications []interface{}) {
		if fsint.connection != nil {
			// Notify kernel in a separate thread/goroutine
			go func() {
				for _, n := range notifications {
					fsint.connection.Notify(n)
				}
			}()
		}
	}
	return fsint
}

func (fs *GoofysFuse) StatFS(
	ctx context.Context,
	op *fuseops.StatFSOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataReads, 1)

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
	return
}

func (fs *GoofysFuse) GetInodeAttributes(
	ctx context.Context,
	op *fuseops.GetInodeAttributesOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	attr := inode.GetAttributes()
	op.Attributes = *attr
	op.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	return
}

func (fs *GoofysFuse) GetXattr(ctx context.Context,
	op *fuseops.GetXattrOp) (err error) {
	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	value, err := inode.GetXattr(op.Name)
	err = mapAwsError(err)
	if err != nil {
		return err
	}

	op.BytesRead = len(value)

	if len(op.Dst) != 0 {
		if len(op.Dst) < op.BytesRead {
			return syscall.ERANGE
		}

		copy(op.Dst, value)
	}
	return
}

func (fs *GoofysFuse) ListXattr(ctx context.Context,
	op *fuseops.ListXattrOp) (err error) {
	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	xattrs, err := inode.ListXattr()
	err = mapAwsError(err)

	ncopied := 0

	for _, name := range xattrs {
		buf := op.Dst[ncopied:]
		nlen := len(name) + 1

		if nlen <= len(buf) {
			copy(buf, name)
			ncopied += nlen
			buf[nlen-1] = '\x00'
		}

		op.BytesRead += nlen
	}

	if len(op.Dst) != 0 && ncopied < op.BytesRead {
		err = syscall.ERANGE
	}

	return
}

func (fs *GoofysFuse) RemoveXattr(ctx context.Context,
	op *fuseops.RemoveXattrOp) (err error) {
	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	err = inode.RemoveXattr(op.Name)
	err = mapAwsError(err)
	if err == syscall.EPERM {
		// Silently ignore forbidden xattr operations
		err = nil
	}

	return
}

func (fs *GoofysFuse) SetXattr(ctx context.Context,
	op *fuseops.SetXattrOp) (err error) {
	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	if op.Name == fs.flags.RefreshAttr {
		// Setting xattr with special name (.invalidate) refreshes the inode's cache
		return fs.RefreshInodeCache(inode)
	}

	err = inode.SetXattr(op.Name, op.Value, op.Flags)
	err = mapAwsError(err)
	if err == syscall.EPERM {
		// Silently ignore forbidden xattr operations
		err = nil
	}
	return
}

func (fs *GoofysFuse) CreateSymlink(ctx context.Context,
	op *fuseops.CreateSymlinkOp) (err error) {
	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	inode := parent.CreateSymlink(op.Name, op.Target)
	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	return
}

func (fs *GoofysFuse) ReadSymlink(ctx context.Context,
	op *fuseops.ReadSymlinkOp) (err error) {
	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	op.Target, err = inode.ReadSymlink()
	err = mapAwsError(err)
	return
}

func (fs *GoofysFuse) LookUpInode(
	ctx context.Context,
	op *fuseops.LookUpInodeOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	defer func() { fuseLog.Debugf("<-- LookUpInode %v %v %v", op.Parent, op.Name, err) }()

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	inode, err := parent.LookUpCached(op.Name)
	if err != nil {
		return err
	}

	inode.Ref()
	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	return
}

func (fs *GoofysFuse) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	inode.mu.Lock()
	inode.DeRef(int64(op.N))
	inode.mu.Unlock()

	return
}

func (fs *GoofysFuse) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {

	atomic.AddInt64(&fs.stats.noops, 1)

	fs.mu.Lock()
	in := fs.getInodeOrDie(op.Inode)
	if atomic.LoadInt32(&in.refreshed) == -1 {
		// Stale inode
		fs.mu.Unlock()
		return syscall.ESTALE
	}
	handleID := fs.nextHandleID
	fs.nextHandleID++
	fs.mu.Unlock()

	if atomic.LoadInt32(&in.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	dh := in.OpenDir()

	fs.mu.Lock()
	defer fs.mu.Unlock()

	fs.dirHandles[handleID] = dh
	op.Handle = handleID

	return
}

func makeDirEntry(en *DirHandleEntry) fuseutil.Dirent {
	dt := fuseutil.DT_File
	if en.IsDir {
		dt = fuseutil.DT_Directory
	}
	return fuseutil.Dirent{
		Name:   en.Name,
		Type:   dt,
		Inode:  en.Inode,
		Offset: en.Offset,
	}
}

func (fs *GoofysFuse) ReadDir(
	ctx context.Context,
	op *fuseops.ReadDirOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	// Find the handle.
	fs.mu.RLock()
	dh := fs.dirHandles[op.Handle]
	fs.mu.RUnlock()

	if dh == nil {
		panic(fmt.Sprintf("can't find dh=%v", op.Handle))
	}

	inode := dh.inode
	inode.logFuse("ReadDir", op.Offset)

	dh.mu.Lock()

	dh.Seek(op.Offset)

	for {
		e, err := dh.ReadDir(dh.lastInternalOffset, dh.lastExternalOffset)
		if err != nil {
			dh.mu.Unlock()
			err = mapAwsError(err)
			return err
		}
		if e == nil {
			break
		}

		if e.Inode == 0 {
			panic(fmt.Sprintf("unset inode %v", e.Name))
		}

		n := fuseutil.WriteDirent(op.Dst[op.BytesRead:], makeDirEntry(e))
		if n == 0 {
			break
		}

		dh.inode.logFuse("<-- ReadDir", e.Name, e.Offset)

		op.BytesRead += n
		// We have to modify it here because WriteDirent MAY not send the entry
		if dh.lastInternalOffset >= 0 {
			dh.lastInternalOffset++
		}
		dh.lastExternalOffset++
		dh.lastName = e.Name
	}

	dh.mu.Unlock()
	return
}

func (fs *GoofysFuse) ReleaseDirHandle(
	ctx context.Context,
	op *fuseops.ReleaseDirHandleOp) (err error) {

	atomic.AddInt64(&fs.stats.noops, 1)

	fs.mu.RLock()
	dh := fs.dirHandles[op.Handle]
	fs.mu.RUnlock()

	dh.CloseDir()

	fuseLog.Debugln("ReleaseDirHandle", dh.inode.FullName())

	fs.mu.Lock()
	delete(fs.dirHandles, op.Handle)
	fs.mu.Unlock()

	return
}

func (fs *GoofysFuse) OpenFile(
	ctx context.Context,
	op *fuseops.OpenFileOp) (err error) {
	fs.mu.RLock()
	in := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	atomic.AddInt64(&fs.stats.noops, 1)

	if atomic.LoadInt32(&in.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	fh, err := in.OpenFile()
	if err != nil {
		err = mapAwsError(err)
		return
	}

	fs.mu.Lock()

	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = fh
	fs.mu.Unlock()

	op.Handle = handleID

	in.mu.Lock()
	defer in.mu.Unlock()

	// this flag appears to tell the kernel if this open should
	// use the page cache or not. "use" here means:
	//
	// read will read from cache
	// write will populate cache

	// We have our own in-memory cache, kernel page cache is redundant
	op.KeepPageCache = false

	return
}

func (fs *GoofysFuse) ReadFile(
	ctx context.Context,
	op *fuseops.ReadFileOp) (err error) {

	atomic.AddInt64(&fs.stats.reads, 1)

	fs.mu.RLock()
	fh := fs.fileHandles[op.Handle]
	fs.mu.RUnlock()

	op.Data, op.BytesRead, err = fh.ReadFile(op.Offset, op.Size)
	err = mapAwsError(err)

	return
}

func (fs *GoofysFuse) SyncFile(
	ctx context.Context,
	op *fuseops.SyncFileOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if !fs.flags.IgnoreFsync {
		fs.mu.RLock()
		in := fs.getInodeOrDie(op.Inode)
		fs.mu.RUnlock()

		if in.Id == fuseops.RootInodeID {
			err = fs.SyncFS(nil)
		} else if in.isDir() {
			err = fs.SyncFS(in)
		} else {
			err = in.SyncFile()
		}
		err = mapAwsError(err)
	}

	return
}

func (fs *GoofysFuse) FlushFile(
	ctx context.Context,
	op *fuseops.FlushFileOp) (err error) {

	// FlushFile is a no-op because we flush changes to the server asynchronously
	// If the user really wants to persist a file to the server he should call fsync()

	atomic.AddInt64(&fs.stats.noops, 1)

	return
}

func (fs *GoofysFuse) ReleaseFileHandle(
	ctx context.Context,
	op *fuseops.ReleaseFileHandleOp) (err error) {
	fs.mu.Lock()
	defer fs.mu.Unlock()
	fh := fs.fileHandles[op.Handle]
	fh.Release()

	atomic.AddInt64(&fs.stats.noops, 1)

	fuseLog.Debugln("ReleaseFileHandle", fh.inode.FullName(), op.Handle, fh.inode.Id)

	delete(fs.fileHandles, op.Handle)

	// try to compact heap
	//fs.bufferPool.MaybeGC()
	return
}

func (fs *GoofysFuse) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	inode, fh := parent.Create(op.Name)

	// Always take inode locks after fs lock if you need both...
	fs.mu.Lock()
	defer fs.mu.Unlock()

	inode.setFileMode(op.Mode)

	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	// Allocate a handle.
	handleID := fs.nextHandleID
	fs.nextHandleID++

	fs.fileHandles[handleID] = fh

	op.Handle = handleID

	inode.logFuse("<-- CreateFile")

	return
}

// MkNode is required for NFS even with regular files
// because kernel nfsd uses vfs_create() -> fuse_create() -> fuse_mknod()
// and then separate fuse_open() for file creation instead of fuse_create_open()
func (fs *GoofysFuse) MkNode(
	ctx context.Context,
	op *fuseops.MkNodeOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if (op.Mode & os.ModeType) != os.ModeDir &&
		(op.Mode & os.ModeType) != 0 &&
		!fs.flags.EnableSpecials {
		return syscall.ENOTSUP
	}

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	var inode *Inode
	if (op.Mode & os.ModeDir) != 0 {
		inode, err = parent.MkDir(op.Name)
		if err != nil {
			err = mapAwsError(err)
			return err
		}
	} else {
		var fh *FileHandle
		inode, fh = parent.Create(op.Name)
		fh.Release()
	}
	inode.Attributes.Rdev = op.Rdev
	inode.setFileMode(op.Mode)

	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	return
}

func (fs *GoofysFuse) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	// ignore op.Mode for now
	inode, err := parent.MkDir(op.Name)
	if err != nil {
		err = mapAwsError(err)
		return err
	}
	if fs.flags.EnablePerms {
		inode.Attributes.Mode = os.ModeDir | (op.Mode & os.ModePerm)
	} else {
		inode.Attributes.Mode = os.ModeDir | fs.flags.DirMode
	}

	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	return
}

func (fs *GoofysFuse) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	err = parent.RmDir(op.Name)
	err = mapAwsError(err)
	parent.logFuse("<-- RmDir", op.Name, err)
	return
}

func (fs *GoofysFuse) SetInodeAttributes(
	ctx context.Context,
	op *fuseops.SetInodeAttributesOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	err = inode.SetAttributes(op.Size, op.Mode, op.Mtime, op.Uid, op.Gid)
	if err != nil {
		return
	}

	attr := inode.GetAttributes()
	op.Attributes = *attr
	op.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)

	return
}

func (fs *GoofysFuse) WriteFile(
	ctx context.Context,
	op *fuseops.WriteFileOp) (err error) {

	atomic.AddInt64(&fs.stats.writes, 1)

	fs.mu.RLock()

	fh, ok := fs.fileHandles[op.Handle]
	if !ok {
		panic(fmt.Sprintf("WriteFile: can't find handle %v", op.Handle))
	}
	fs.mu.RUnlock()

	// fuse binding leaves extra room for header, so we
	// account for it when we decide whether to do "zero-copy" write
	copyData := len(op.Data) < cap(op.Data)-4096
	err = fh.WriteFile(op.Offset, op.Data, copyData)
	err = mapAwsError(err)
	op.SuppressReuse = !copyData

	return
}

func (fs *GoofysFuse) Unlink(
	ctx context.Context,
	op *fuseops.UnlinkOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.Parent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	err = parent.Unlink(op.Name)
	err = mapAwsError(err)
	return
}

// rename("from", "to") causes the kernel to send lookup of "from" and
// "to" prior to sending rename to us
func (fs *GoofysFuse) Rename(
	ctx context.Context,
	op *fuseops.RenameOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	parent := fs.getInodeOrDie(op.OldParent)
	newParent := fs.getInodeOrDie(op.NewParent)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&parent.refreshed) == -1 ||
		atomic.LoadInt32(&newParent.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	if op.OldParent == op.NewParent {
		parent.mu.Lock()
		defer parent.mu.Unlock()
	} else {
		// lock ordering to prevent deadlock
		if op.OldParent < op.NewParent {
			parent.mu.Lock()
			newParent.mu.Lock()
		} else {
			newParent.mu.Lock()
			parent.mu.Lock()
		}
		defer parent.mu.Unlock()
		defer newParent.mu.Unlock()
	}

	err = parent.Rename(op.OldName, newParent, op.NewName)
	err = mapAwsError(err)

	return
}

const (
	FALLOC_FL_KEEP_SIZE      = uint32(0x01)
	FALLOC_FL_PUNCH_HOLE     = uint32(0x02)
	FALLOC_FL_COLLAPSE_RANGE = uint32(0x08)
	FALLOC_FL_ZERO_RANGE     = uint32(0x10)
	FALLOC_FL_INSERT_RANGE   = uint32(0x20)
)

func (fs *GoofysFuse) Fallocate(
	ctx context.Context,
	op *fuseops.FallocateOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	fs.mu.RLock()
	inode := fs.getInodeOrDie(op.Inode)
	fs.mu.RUnlock()

	if atomic.LoadInt32(&inode.refreshed) == -1 {
		// Stale inode
		return syscall.ESTALE
	}

	if op.Length == 0 {
		return nil
	}

	inode.mu.Lock()

	modified := false

	if (op.Mode & (FALLOC_FL_COLLAPSE_RANGE | FALLOC_FL_INSERT_RANGE)) != 0 {
		// Insert range/remove range operations are not supported
		// It's possible to support them, but it will require buffer remapping support.
		// I.e. if you open a file, insert/collapse a range and then read past the
		// affected offset you should get data from the old offset! And it's probably
		// wise to use UploadPartCopy with the corresponding ranges to optimize copying
		// on the server side in this case. Some day we might even be able to preserve
		// multipart part IDs if cutting a non-finalized upload across part boundaries,
		// but now we can't - part offsets are always fixed.
		inode.mu.Unlock()
		return syscall.ENOTSUP
	}

	if op.Offset+op.Length > inode.Attributes.Size {
		if (op.Mode & FALLOC_FL_KEEP_SIZE) == 0 {
			// Resize
			if op.Offset+op.Length > fs.getMaxFileSize() {
				// File size too large
				log.Warnf(
					"Maximum file size exceeded when trying to extend %v to %v bytes using fallocate",
					inode.FullName(), op.Offset+op.Length,
				)
				inode.mu.Unlock()
				return syscall.EFBIG
			}
			inode.ResizeUnlocked(op.Offset+op.Length, true, true)
			modified = true
		} else {
			if op.Offset > inode.Attributes.Size {
				op.Offset = inode.Attributes.Size
			}
			op.Length = inode.Attributes.Size-op.Offset
		}
	}

	if (op.Mode & (FALLOC_FL_PUNCH_HOLE | FALLOC_FL_ZERO_RANGE)) != 0 {
		// Zero fill
		mod, _ := inode.zeroRange(op.Offset, op.Length)
		modified = modified || mod
	}

	if modified && inode.CacheState == ST_CACHED {
		inode.SetCacheState(ST_MODIFIED)
		inode.fs.WakeupFlusher()
	}

	inode.mu.Unlock()

	return
}

func (fs *GoofysFuse) SetConnection(conn *fuse.Connection) {
	fs.connection = conn
}

type FuseMfsWrapper struct {
	*fuse.MountedFileSystem
	mountPoint string
}

func (m *FuseMfsWrapper) Unmount() error {
	return TryUnmount(m.mountPoint)
}

// Mount the file system based on the supplied arguments, returning a
// fuse.MountedFileSystem that can be joined to wait for unmounting.
func MountFuse(
	ctx context.Context,
	bucketName string,
	flags *FlagStorage) (fs *Goofys, mfs MountedFS, err error) {

	// Mount the file system.
	mountCfg := &fuse.MountConfig{
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
		mountCfg.DebugLogger = GetStdLogger(fuseLog, logrus.DebugLevel)
	}

	if flags.DebugFuse || flags.DebugMain {
		log.Level = logrus.DebugLevel
	}

	fs, err = NewGoofys(ctx, bucketName, flags)
	if fs == nil {
		if err == nil {
			err = fmt.Errorf("Mount: initialization failed")
		}
		return
	}
	fsint := NewGoofysFuse(fs)
	server := fuseutil.NewFileSystemServer(fsint)

	fuseMfs, err := fuse.Mount(flags.MountPoint, server, mountCfg)
	if err != nil {
		err = fmt.Errorf("Mount: %v", err)
		return
	}

	mfs = &FuseMfsWrapper{
		MountedFileSystem: fuseMfs,
		mountPoint: flags.MountPoint,
	}

	return
}

func TryUnmount(mountPoint string) (err error) {
	for i := 0; i < 20; i++ {
		err = fuse.Unmount(mountPoint)
		if err != nil {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	return
}
