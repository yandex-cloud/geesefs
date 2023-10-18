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
	"github.com/yandex-cloud/geesefs/internal/cfg"

	"context"
	"fmt"
	"os"
	"strings"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"

	"github.com/sirupsen/logrus"
)

// jacobsa/fuse interface to the file system

func init() {
	cfg.FuseOptions = `FUSE OPTIONS:
   -o allow_other  allow all users (including root) to access files
   -o allow_root   allow root and filesystem owner to access files
   -o rootmode=M   set file mode of the filesystem's root (octal)
`;
}

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

	inode := fs.getInodeOrDie(op.Inode)

	if atomic.LoadInt32(&inode.CacheState) == ST_DEAD {
		// Stale inode
		return syscall.ESTALE
	}

	attr := inode.GetAttributes()
	op.Attributes = *attr
	op.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	inode.SetExpireLocked(op.AttributesExpiration)

	return
}

func (fs *GoofysFuse) GetXattr(ctx context.Context,
	op *fuseops.GetXattrOp) (err error) {
	if fs.flags.DisableXattr {
		return syscall.ENOSYS
	}

	inode := fs.getInodeOrDie(op.Inode)

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	if atomic.LoadInt32(&inode.CacheState) == ST_DEAD {
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
	if fs.flags.DisableXattr {
		return syscall.ENOSYS
	}

	inode := fs.getInodeOrDie(op.Inode)

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	if atomic.LoadInt32(&inode.CacheState) == ST_DEAD {
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
	if fs.flags.DisableXattr {
		return syscall.ENOSYS
	}

	inode := fs.getInodeOrDie(op.Inode)

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if atomic.LoadInt32(&inode.CacheState) == ST_DEAD {
		// Stale inode
		return syscall.ESTALE
	}

	err = inode.RemoveXattr(op.Name)
	return mapAwsError(err)
}

func (fs *GoofysFuse) SetXattr(ctx context.Context,
	op *fuseops.SetXattrOp) (err error) {
	if fs.flags.DisableXattr {
		return syscall.ENOSYS
	}

	inode := fs.getInodeOrDie(op.Inode)

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if atomic.LoadInt32(&inode.CacheState) == ST_DEAD {
		// Stale inode
		return syscall.ESTALE
	}

	if op.Name == fs.flags.RefreshAttr {
		// Setting xattr with special name (.invalidate) refreshes the inode's cache
		return fs.RefreshInodeCache(inode)
	}

	err = inode.SetXattr(op.Name, op.Value, op.Flags)
	return mapAwsError(err)
}

func (fs *GoofysFuse) CreateSymlink(ctx context.Context,
	op *fuseops.CreateSymlinkOp) (err error) {
	parent := fs.getInodeOrDie(op.Parent)

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if atomic.LoadInt32(&parent.CacheState) == ST_DEAD {
		// Stale inode
		return syscall.ESTALE
	}

	inode, err := parent.CreateSymlink(op.Name, op.Target)
	if err != nil {
		return err
	}
	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration
	inode.SetExpireLocked(op.Entry.AttributesExpiration)
	return
}

func (fs *GoofysFuse) ReadSymlink(ctx context.Context,
	op *fuseops.ReadSymlinkOp) (err error) {
	inode := fs.getInodeOrDie(op.Inode)

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	if atomic.LoadInt32(&inode.CacheState) == ST_DEAD {
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

	parent := fs.getInodeOrDie(op.Parent)

	inode, err := parent.LookUpCached(op.Name)
	if err != nil {
		return err
	}

	inode.Ref()
	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration
	inode.SetExpireLocked(op.Entry.AttributesExpiration)

	return
}

func (fs *GoofysFuse) ForgetInode(
	ctx context.Context,
	op *fuseops.ForgetInodeOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	fs.mu.RLock()
	inode := fs.inodes[op.Inode]
	fs.mu.RUnlock()

	// Forget requests are allowed for expired inodes which were dropped from memory
	if inode != nil {
		inode.mu.Lock()
		inode.DeRef(int64(op.N))
		inode.mu.Unlock()
	}

	return
}

func (fs *GoofysFuse) OpenDir(
	ctx context.Context,
	op *fuseops.OpenDirOp) (err error) {

	atomic.AddInt64(&fs.stats.noops, 1)

	in := fs.getInodeOrDie(op.Inode)
	if atomic.LoadInt32(&in.CacheState) == ST_DEAD {
		// Stale inode
		return syscall.ESTALE
	}

	dh := in.OpenDir()
	op.Handle = fs.AddDirHandle(dh)

	return
}

func makeDirEntry(inode *Inode, offset fuseops.DirOffset) fuseutil.Dirent {
	dt := fuseutil.DT_File
	if inode.isDir() {
		dt = fuseutil.DT_Directory
	}
	name := inode.Name
	if offset == 0 {
		name = "."
	} else if offset == 1 {
		name = ".."
	}
	return fuseutil.Dirent{
		Name:   name,
		Type:   dt,
		Inode:  inode.Id,
		Offset: offset+1,
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
		e, err := dh.ReadDir()
		if err != nil {
			dh.mu.Unlock()
			err = mapAwsError(err)
			return err
		}
		if e == nil {
			break
		}

		var dirent fuseutil.Dirent
		n := 0
		if op.Plus {
			var inodeEntry fuseops.ChildInodeEntry
			e.mu.Lock()
			inodeEntry.Child = e.Id
			inodeEntry.Attributes = e.InflateAttributes()
			inodeEntry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
			inodeEntry.EntryExpiration = inodeEntry.AttributesExpiration
			e.SetExpireTime(inodeEntry.AttributesExpiration)
			dirent = makeDirEntry(e, dh.lastExternalOffset)
			e.mu.Unlock()
			n = fuseutil.WriteDirentPlus(op.Dst[op.BytesRead:], &inodeEntry, dirent)
			if n == 0 {
				break
			}
			e.Ref()
		} else {
			e.mu.Lock()
			dirent = makeDirEntry(e, dh.lastExternalOffset)
			e.mu.Unlock()
			n = fuseutil.WriteDirent(op.Dst[op.BytesRead:], dirent)
			if n == 0 {
				break
			}
		}

		dh.inode.logFuse("<-- ReadDir", e.Name, dh.lastExternalOffset)

		op.BytesRead += n
		// We have to modify it here because WriteDirent MAY not send the entry
		dh.Next(dirent.Name)
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
	in := fs.getInodeOrDie(op.Inode)

	atomic.AddInt64(&fs.stats.noops, 1)

	if atomic.LoadInt32(&in.CacheState) == ST_DEAD {
		// Stale inode
		return syscall.ESTALE
	}

	fh, err := in.OpenFile()
	if err != nil {
		err = mapAwsError(err)
		return
	}

	op.Handle = fs.AddFileHandle(fh)

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
		in := fs.getInodeOrDie(op.Inode)

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
	fh := fs.fileHandles[op.Handle]
	fh.Release()
	atomic.AddInt64(&fs.stats.noops, 1)
	fuseLog.Debugln("ReleaseFileHandle", fh.inode.FullName(), op.Handle, fh.inode.Id)
	delete(fs.fileHandles, op.Handle)
	fs.mu.Unlock()

	if fh.inode.fs.flags.FsyncOnClose {
		return fh.inode.SyncFile()
	}

	return
}

func (fs *GoofysFuse) CreateFile(
	ctx context.Context,
	op *fuseops.CreateFileOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	parent := fs.getInodeOrDie(op.Parent)

	if atomic.LoadInt32(&parent.CacheState) == ST_DEAD {
		// Stale inode
		return syscall.ESTALE
	}

	inode, fh, err := parent.Create(op.Name)
	if err != nil {
		return err
	}

	inode.setFileMode(op.Mode)

	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration
	inode.SetExpireLocked(op.Entry.AttributesExpiration)

	op.Handle = fs.AddFileHandle(fh)

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

	parent := fs.getInodeOrDie(op.Parent)

	if atomic.LoadInt32(&parent.CacheState) == ST_DEAD {
		// Stale inode
		return syscall.ESTALE
	}

	var inode *Inode
	if (op.Mode & os.ModeDir) != 0 {
		inode, err = parent.MkDir(op.Name)
		if err != nil {
			return mapAwsError(err)
		}
	} else {
		var fh *FileHandle
		inode, fh, err = parent.Create(op.Name)
		if err != nil {
			return mapAwsError(err)
		}
		fh.Release()
	}
	inode.Attributes.Rdev = op.Rdev
	inode.setFileMode(op.Mode)

	op.Entry.Child = inode.Id
	op.Entry.Attributes = inode.InflateAttributes()
	op.Entry.AttributesExpiration = time.Now().Add(fs.flags.StatCacheTTL)
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration
	inode.SetExpireLocked(op.Entry.AttributesExpiration)

	if fs.flags.FsyncOnClose {
		err = inode.SyncFile()
		if err != nil {
			return mapAwsError(err)
		}
	}

	return
}

func (fs *GoofysFuse) MkDir(
	ctx context.Context,
	op *fuseops.MkDirOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	parent := fs.getInodeOrDie(op.Parent)

	if atomic.LoadInt32(&parent.CacheState) == ST_DEAD {
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
	op.Entry.EntryExpiration = op.Entry.AttributesExpiration
	inode.SetExpireLocked(op.Entry.AttributesExpiration)

	return
}

func (fs *GoofysFuse) RmDir(
	ctx context.Context,
	op *fuseops.RmDirOp) (err error) {

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	parent := fs.getInodeOrDie(op.Parent)

	if atomic.LoadInt32(&parent.CacheState) == ST_DEAD {
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

	inode := fs.getInodeOrDie(op.Inode)

	if atomic.LoadInt32(&inode.CacheState) == ST_DEAD {
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
	inode.SetExpireLocked(op.AttributesExpiration)

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

	parent := fs.getInodeOrDie(op.Parent)

	if atomic.LoadInt32(&parent.CacheState) == ST_DEAD {
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

	parent := fs.getInodeOrDie(op.OldParent)
	newParent := fs.getInodeOrDie(op.NewParent)

	if atomic.LoadInt32(&parent.CacheState) == ST_DEAD ||
		atomic.LoadInt32(&newParent.CacheState) == ST_DEAD {
		// Stale inode
		return syscall.ESTALE
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

	inode := fs.getInodeOrDie(op.Inode)

	if atomic.LoadInt32(&inode.CacheState) == ST_DEAD {
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
			inode.ResizeUnlocked(op.Offset+op.Length, true)
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
		mod, _ := inode.buffers.ZeroRange(op.Offset, op.Length)
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
	fs *Goofys
	mountPoint string
}

func (m *FuseMfsWrapper) Unmount() error {
	err := TryUnmount(m.mountPoint)
	m.fs.Shutdown()
	return err
}

func convertFuseOptions(flags *cfg.FlagStorage) map[string]string {
	optMap := make(map[string]string)
	for _, o := range flags.MountOptions {
		// NOTE(jacobsa): The man pages don't define how escaping works, and as far
		// as I can tell there is no way to properly escape or quote a comma in the
		// options list for an fstab entry. So put our fingers in our ears and hope
		// that nobody needs a comma.
		for _, p := range strings.Split(o, ",") {
			// Split on the first equals sign.
			if equalsIndex := strings.IndexByte(p, '='); equalsIndex != -1 {
				optMap[p[:equalsIndex]] = p[equalsIndex+1:]
			} else {
				optMap[p] = ""
			}
		}
	}
	if flags.Setuid > 0 {
		if _, ok := optMap["user_id"]; !ok {
			optMap["user_id"] = fmt.Sprintf("%v", flags.Setuid)
		}
	}
	return optMap
}

// Mount the file system based on the supplied arguments, returning a
// fuse.MountedFileSystem that can be joined to wait for unmounting.
func MountFuse(
	ctx context.Context,
	bucketName string,
	flags *cfg.FlagStorage) (fs *Goofys, mfs MountedFS, err error) {
	fs, err = NewGoofys(ctx, bucketName, flags)
	if fs == nil {
		if err == nil {
			err = fmt.Errorf("Mount: initialization failed")
		}
		return
	}
	mfs, err = mountFuseFS(fs)
	return
}

func mountFuseFS(fs *Goofys) (mfs MountedFS, err error) {
	// Mount the file system.
	mountCfg := &fuse.MountConfig{
		FSName:                  fs.bucket,
		Subtype:                 "geesefs",
		Options:                 convertFuseOptions(fs.flags),
		ErrorLogger:             cfg.GetStdLogger(cfg.NewLogger("fuse"), logrus.ErrorLevel),
		DisableWritebackCaching: true,
		UseVectoredRead:         true,
		UseReadDirPlus:          true,
	}

	if fs.flags.DebugFuse {
		fuseLog := cfg.GetLogger("fuse")
		mountCfg.DebugLogger = cfg.GetStdLogger(fuseLog, logrus.DebugLevel)
	}

	fsint := NewGoofysFuse(fs)
	server := fuseutil.NewFileSystemServer(fsint)

	fuseMfs, err := fuse.Mount(fs.flags.MountPoint, server, mountCfg)
	if err != nil {
		err = fmt.Errorf("Mount: %v", err)
		return
	}

	mfs = &FuseMfsWrapper{
		MountedFileSystem: fuseMfs,
		fs: fs,
		mountPoint: fs.flags.MountPoint,
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
