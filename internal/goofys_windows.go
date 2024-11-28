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

package internal

import (
	"context"
	"fmt"
	"os"
	"runtime"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/winfsp/cgofuse/fuse"
	"github.com/jacobsa/fuse/fuseops"
	"github.com/sirupsen/logrus"

	"github.com/yandex-cloud/geesefs/internal/cfg"
)

// winfsp/cgofuse interface to the file system

func init() {
	cfg.FuseOptions = `WinFSP options:
   -o umask=MASK              set file permissions (octal)
   -o FileSecurity=SDDL       set file DACL (SDDL format)
   -o create_umask=MASK       set newly created file permissions (octal)
      -o create_file_umask=MASK      for files only
      -o create_dir_umask=MASK       for directories only
   -o uid=N                   set file owner (default is mounting user id)
   -o gid=N                   set file group (default is mounting user group)
   -o rellinks                interpret absolute symlinks as volume relative
   -o dothidden               dot files have the Windows hidden file attrib
   -o volname=NAME            set volume label
   -o VolumePrefix=UNC        set UNC prefix (/Server/Share)
   -o FileSystemName=NAME     set file system name (use NTFS to run executables)
   -o debug                   enable debug output
   -o DebugLog=FILE           debug log file (requires -o debug)
WinFSP advanced options:
   -o FileInfoTimeout=N       metadata timeout (millis, -1 for data caching)
   -o DirInfoTimeout=N        directory info timeout (millis)
   -o EaTimeout=N             extended attribute timeout (millis)
   -o VolumeInfoTimeout=N     volume info timeout (millis)
   -o KeepFileCache           do not discard cache when files are closed
   -o LegacyUnlinkRename      do not support new POSIX unlink/rename
   -o ThreadCount             number of file system dispatcher threads
   -o uidmap=UID:SID[;...]    explicit UID <-> SID map (max 8 entries)
`;
}

type GoofysWin struct {
	fuse.FileSystemBase
	*Goofys
	host        *fuse.FileSystemHost
	initialized bool
	initCh      chan int
}

func NewGoofysWin(fs *Goofys) *GoofysWin {
	fsint := &GoofysWin{
		Goofys: fs,
	}
	fsint.initCh = make(chan int, 3)
	fs.NotifyCallback = func(notifications []interface{}) {
		go fsint.Notify(notifications)
	}
	if fs.flags.WinRefreshDirs {
		go fsint.WinDirRefresher()
	}
	return fsint
}

// Init is called when the file system is created.
func (fs *GoofysWin) Init() {
	fs.initialized = true
	fs.initCh <- 1
}

// Destroy is called when the file system is destroyed.
func (fs *GoofysWin) Destroy() {
	fs.initialized = false
	fs.initCh <- 2
}

// Statfs gets file system statistics.
func (fs *GoofysWin) Statfs(path string, stat *fuse.Statfs_t) int {
	atomic.AddInt64(&fs.stats.metadataReads, 1)
	fuseLog.Debugf("<--> Statfs %v", path)
	const BLOCK_SIZE = 4096
	const TOTAL_SPACE = 1 * 1024 * 1024 * 1024 * 1024 * 1024 // 1PB
	const TOTAL_BLOCKS = TOTAL_SPACE / BLOCK_SIZE
	const INODES = 1 * 1000 * 1000 * 1000 // 1 billion
	stat.Bsize = BLOCK_SIZE
	stat.Frsize = BLOCK_SIZE
	stat.Blocks = TOTAL_BLOCKS
	stat.Bfree = TOTAL_BLOCKS
	stat.Bavail = TOTAL_BLOCKS
	stat.Files = INODES
	stat.Ffree = INODES
	stat.Favail = INODES
	stat.Namemax = 255
	return 0
}

func mapWinError(err error) int {
	if err == nil {
		return 0
	}
	if fuseLog.Level == logrus.DebugLevel {
		pc, _, _, _ := runtime.Caller(1)
		details := runtime.FuncForPC(pc)
		fuseLog.Debugf("%v: error %v", details, err)
	}
	err = mapAwsError(err)
	switch err {
	case syscall.EACCES:
		return -fuse.EACCES
	case syscall.EAGAIN:
		return -fuse.EAGAIN
	case syscall.EBUSY:
		return -fuse.EBUSY
	case syscall.ECONNRESET:
		return -fuse.ECONNRESET
	case syscall.EEXIST:
		return -fuse.EEXIST
	case syscall.EFBIG:
		return -fuse.EFBIG
	case syscall.EINTR:
		return -fuse.EINTR
	case syscall.EINVAL:
		return -fuse.EINVAL
	case syscall.EIO:
		return -fuse.EIO
	case syscall.EISDIR:
		return -fuse.EISDIR
	case syscall.ENODATA:
		return -fuse.ENODATA
	case syscall.ENODEV:
		return -fuse.ENODEV
	case syscall.ENOENT:
		return -fuse.ENOENT
	case syscall.ENOMEM:
		return -fuse.ENOMEM
	case syscall.ENOSYS:
		return -fuse.ENOSYS
	case syscall.ENOTDIR:
		return -fuse.ENOTDIR
	case syscall.ENOTEMPTY:
		return -fuse.ENOTEMPTY
	case syscall.ENOTSUP:
		return -fuse.ENOTSUP
	case syscall.ENXIO:
		return -fuse.ENXIO
	case syscall.EOPNOTSUPP:
		return -fuse.EOPNOTSUPP
	case syscall.EPERM:
		return -fuse.EPERM
	case syscall.ERANGE:
		return -fuse.ERANGE
	case syscall.ESPIPE:
		return -fuse.ESPIPE
	case syscall.ESTALE:
		return -fuse.EINVAL
	default:
		return -fuse.EIO
	}
}

// Mknod creates a file node.
func (fs *GoofysWin) Mknod(path string, mode uint32, dev uint64) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Mknod %v %v %v", path, mode, dev)
		defer func() {
			fuseLog.Debugf("<- Mknod %v %v %v = %v", path, mode, dev, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if (mode & fuse.S_IFMT) != fuse.S_IFDIR &&
		(mode & fuse.S_IFMT) != 0 &&
		!fs.flags.EnableSpecials {
		return -fuse.ENOTSUP
	}

	parent, child, err := fs.LookupParent(path)
	if err != nil {
		return mapWinError(err)
	}

	var inode *Inode
	if (mode & fuse.S_IFDIR) != 0 {
		inode, err = parent.MkDir(child)
		if err != nil {
			return mapWinError(err)
		}
	} else {
		var fh *FileHandle
		inode, fh, err = parent.Create(child)
		if err != nil {
			return mapWinError(err)
		}
		fh.Release()
	}
	inode.Attributes.Rdev = uint32(dev)
	inode.setFileMode(fuseops.ConvertFileMode(mode))

	if fs.flags.FsyncOnClose {
		err = inode.SyncFile()
		if err != nil {
			return mapWinError(err)
		}
	}

	return 0
}

// Mkdir creates a directory.
func (fs *GoofysWin) Mkdir(path string, mode uint32) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Mkdir %v %v", path, mode)
		defer func() {
			fuseLog.Debugf("<- Mkdir %v %v = %v", path, mode, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	parent, child, err := fs.LookupParent(path)
	if err != nil {
		return mapWinError(err)
	}

	inode, err := parent.MkDir(child)
	if err != nil {
		return mapWinError(err)
	}
	if fs.flags.EnablePerms {
		inode.Attributes.Mode = os.ModeDir | fuseops.ConvertFileMode(mode) & os.ModePerm
	} else {
		inode.Attributes.Mode = os.ModeDir | fs.flags.DirMode
	}

	return 0
}

// Unlink removes a file.
func (fs *GoofysWin) Unlink(path string) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Unlink %v", path)
		defer func() {
			fuseLog.Debugf("<- Unlink %v = %v", path, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	parent, child, err := fs.LookupParent(path)
	if err != nil {
		return mapWinError(err)
	}

	err = parent.Unlink(child)
	return mapWinError(err)
}

// Rmdir removes a directory.
func (fs *GoofysWin) Rmdir(path string) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Rmdir %v", path)
		defer func() {
			fuseLog.Debugf("<- Rmdir %v = %v", path, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	parent, child, err := fs.LookupParent(path)
	if err != nil {
		return mapWinError(err)
	}

	err = parent.RmDir(child)
	return mapWinError(err)
}

// Symlink creates a symbolic link.
func (fs *GoofysWin) Symlink(target string, newpath string) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Symlink %v %v", target, newpath)
		defer func() {
			fuseLog.Debugf("<- Symlink %v %v = %v", target, newpath, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	parent, child, err := fs.LookupParent(newpath)
	if err != nil {
		return mapWinError(err)
	}

	parent.CreateSymlink(child, target)
	return 0
}

// Readlink reads the target of a symbolic link.
func (fs *GoofysWin) Readlink(path string) (ret int, target string) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Readlink %v", path)
		defer func() {
			fuseLog.Debugf("<- Readlink %v = %v %v", path, ret, target)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		return mapWinError(err), ""
	}

	target, err = inode.ReadSymlink()
	if err != nil {
		return mapWinError(err), ""
	}

	return 0, target
}

// Rename renames a file.
func (fs *GoofysWin) Rename(oldpath string, newpath string) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Rename %v %v", oldpath, newpath)
		defer func() {
			fuseLog.Debugf("<- Rename %v %v = %v", oldpath, newpath, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	parent, oldName, err := fs.LookupParent(oldpath)
	if err != nil {
		return mapWinError(err)
	}
	newParent, newName, err := fs.LookupParent(newpath)
	if err != nil {
		return mapWinError(err)
	}

	err = parent.Rename(oldName, newParent, newName)

	return mapWinError(err)
}

// Chmod changes the permission bits of a file.
func (fs *GoofysWin) Chmod(path string, mode uint32) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Chmod %v %v", path, mode)
		defer func() {
			fuseLog.Debugf("<- Chmod %v %v = %v", path, mode, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		return mapWinError(err)
	}

	goMode := fuseops.ConvertFileMode(mode)

	return mapWinError(mapAwsError(inode.SetAttributes(nil, &goMode, nil, nil, nil)))
}

// Chown changes the owner and group of a file.
func (fs *GoofysWin) Chown(path string, uid uint32, gid uint32) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Chown %v %v %v", path, uid, gid)
		defer func() {
			fuseLog.Debugf("<- Chown %v %v %v = %v", path, uid, gid, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		return mapWinError(err)
	}

	return mapWinError(mapAwsError(inode.SetAttributes(nil, nil, nil, &uid, &gid)))
}

// Utimens changes the access and modification times of a file.
func (fs *GoofysWin) Utimens(path string, tmsp []fuse.Timespec) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Utimens %v %v", path, tmsp)
		defer func() {
			fuseLog.Debugf("<- Utimens %v %v = %v", path, tmsp, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		return mapWinError(err)
	}

	// only mtime, atime is ignored
	tm := time.Unix(tmsp[1].Sec, tmsp[1].Nsec)

	return mapWinError(mapAwsError(inode.SetAttributes(nil, nil, &tm, nil, nil)))
}

// Access is only used by winfsp with FSP_FUSE_DELETE_OK. Ignore it
func (fs *GoofysWin) Access(path string, mask uint32) int {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("<--> Access %v %v = 0", path, mask)
	}
	atomic.AddInt64(&fs.stats.noops, 1)
	return 0
}

// Create creates and opens a file.
// The flags are a combination of the fuse.O_* constants.
func (fs *GoofysWin) Create(path string, flags int, mode uint32) (ret int, fhId uint64) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Create %v %v %v", path, flags, mode)
		defer func() {
			fuseLog.Debugf("<- Create %v %v %v = %v %v", path, flags, mode, ret, fhId)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	parent, child, err := fs.LookupParent(path)
	if err != nil {
		return mapWinError(err), 0
	}

	if fs.flags.FlushFilename != "" && child == fs.flags.FlushFilename {
		err = fs.SyncTree(parent)
		if err == nil {
			err = syscall.ENOENT
		}
		return mapWinError(err), 0
	}

	if fs.flags.RefreshFilename != "" && child == fs.flags.RefreshFilename {
		err = fs.RefreshInodeCache(parent)
		if err == nil {
			err = syscall.ENOENT
		}
		return mapWinError(err), 0
	}

	inode, fh, err := parent.Create(child)
	if err != nil {
		return mapWinError(err), 0
	}

	inode.setFileMode(fuseops.ConvertFileMode(mode))

	handleID := fs.AddFileHandle(fh)

	return 0, uint64(handleID)
}

func endsWith(path, part string) bool {
	ld := len(path)-len(part)
	return len(part) > 0 && ld >= 0 && (ld == 0 || path[ld-1] == '/') && path[ld:] == part
}

// Open opens a file.
// The flags are a combination of the fuse.O_* constants.
func (fs *GoofysWin) Open(path string, flags int) (ret int, fhId uint64) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Open %v %v", path, flags)
		defer func() {
			fuseLog.Debugf("<- Open %v %v = %v %v", path, flags, ret, fhId)
		}()
	}

	atomic.AddInt64(&fs.stats.noops, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		if endsWith(path, fs.flags.FlushFilename) {
			parent, _, err := fs.LookupParent(path)
			if err != nil {
				return mapWinError(err), 0
			}
			if err == nil {
				err = fs.SyncTree(parent)
			}
			if err == nil {
				err = syscall.ENOENT
			}
		}
		if endsWith(path, fs.flags.RefreshFilename) {
			parent, _, err := fs.LookupParent(path)
			if err != nil {
				return mapWinError(err), 0
			}
			if err == nil {
				err = fs.RefreshInodeCache(parent)
			}
			if err == nil {
				err = syscall.ENOENT
			}
		}
		return mapWinError(err), 0
	}

	fh, err := inode.OpenFile()
	if err != nil {
		return mapWinError(err), 0
	}

	handleID := fs.AddFileHandle(fh)

	return 0, uint64(handleID)
}

// Getattr gets file attributes.
func (fs *GoofysWin) Getattr(path string, stat *fuse.Stat_t, fh uint64) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Getattr %v %v", path, fh)
		defer func() {
			fuseLog.Debugf("<- Getattr %v %v = %v %v", path, fh, ret, *stat)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		return mapWinError(err)
	}

	makeFuseAttributes(inode.GetAttributes(), stat)

	return 0
}

func makeFuseAttributes(attr *fuseops.InodeAttributes, stat *fuse.Stat_t) {
	stat.Mode = fuseops.ConvertGoMode(attr.Mode)
	stat.Nlink = 1
	stat.Uid = attr.Uid
	stat.Gid = attr.Gid
	stat.Rdev = uint64(attr.Rdev)
	stat.Size = int64(attr.Size)
	stat.Atim.Sec = attr.Atime.Unix()
	stat.Atim.Nsec = int64(attr.Atime.Nanosecond())
	stat.Mtim.Sec = attr.Mtime.Unix()
	stat.Mtim.Nsec = int64(attr.Mtime.Nanosecond())
	stat.Ctim.Sec = attr.Ctime.Unix()
	stat.Ctim.Nsec = int64(attr.Ctime.Nanosecond())
	stat.Blksize = 4096
	stat.Blocks = int64(attr.Size) / stat.Blksize
	stat.Birthtim.Sec = attr.Mtime.Unix()
	stat.Birthtim.Nsec = int64(attr.Mtime.Nanosecond())
}

// Truncate changes the size of a file.
func (fs *GoofysWin) Truncate(path string, size int64, fh uint64) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Truncate %v %v %v", path, size, fh)
		defer func() {
			fuseLog.Debugf("<- Truncate %v %v %v = %v", path, size, fh, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		return mapWinError(err)
	}

	usize := uint64(size)

	return mapWinError(mapAwsError(inode.SetAttributes(&usize, nil, nil, nil, nil)))
}

// Read reads data from a file.
func (fs *GoofysWin) Read(path string, buff []byte, ofst int64, fhId uint64) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Read %v %v %v %v", path, len(buff), ofst, fhId)
		defer func() {
			fuseLog.Debugf("<- Read %v %v %v %v = %v", path, len(buff), ofst, fhId, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.reads, 1)

	fs.mu.RLock()
	fh := fs.fileHandles[fuseops.HandleID(fhId)]
	fs.mu.RUnlock()
	if fh == nil {
		return -fuse.EINVAL
	}

	data, bytesRead, err := fh.ReadFile(ofst, int64(len(buff)))
	if err != nil {
		return mapWinError(err)
	}
	done := 0
	for i := 0; i < len(data); i++ {
		copy(buff[done:], data[i])
		done += len(data[i])
	}

	return bytesRead
}

// Write writes data to a file.
func (fs *GoofysWin) Write(path string, buff []byte, ofst int64, fhId uint64) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Write %v %v %v %v", path, len(buff), ofst, fhId)
		defer func() {
			fuseLog.Debugf("<- Write %v %v %v %v = %v", path, len(buff), ofst, fhId, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.writes, 1)

	fs.mu.RLock()
	fh := fs.fileHandles[fuseops.HandleID(fhId)]
	fs.mu.RUnlock()
	if fh == nil {
		return -fuse.EINVAL
	}

	err := fh.WriteFile(ofst, buff, true)
	if err != nil {
		return mapWinError(err)
	}

	return len(buff)
}

// Flush flushes cached file data. Ignore it.
func (fs *GoofysWin) Flush(path string, fhId uint64) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("<--> Flush %v %v = 0", path, fhId)
	}
	atomic.AddInt64(&fs.stats.noops, 1)
	return 0
}

// Release closes an open file.
func (fs *GoofysWin) Release(path string, fhId uint64) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Release %v %v", path, fhId)
		defer func() {
			fuseLog.Debugf("<- Release %v %v = %v", path, fhId, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.noops, 1)

	fs.mu.RLock()
	fh := fs.fileHandles[fuseops.HandleID(fhId)]
	fs.mu.RUnlock()
	if fh == nil {
		return -fuse.EINVAL
	}
	fh.Release()

	fs.mu.Lock()
	delete(fs.fileHandles, fuseops.HandleID(fhId))
	fs.mu.Unlock()

	if fs.flags.FsyncOnClose {
		err := fh.inode.SyncFile()
		if err != nil {
			return mapWinError(err)
		}
	}

	return 0
}

// Fsync synchronizes file contents.
func (fs *GoofysWin) Fsync(path string, datasync bool, fhId uint64) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Fsync %v %v %v", path, datasync, fhId)
		defer func() {
			fuseLog.Debugf("<- Fsync %v %v %v = %v", path, datasync, fhId, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	if !fs.flags.IgnoreFsync {
		var inode *Inode
		var err error
		if fhId != 0 {
			fs.mu.RLock()
			fh := fs.fileHandles[fuseops.HandleID(fhId)]
			fs.mu.RUnlock()
			if fh == nil {
				return -fuse.EINVAL
			}
			inode = fh.inode
		} else {
			inode, err = fs.LookupPath(path)
			if err != nil {
				return mapWinError(err)
			}
		}
		if inode.Id == fuseops.RootInodeID {
			err = fs.SyncTree(nil)
		} else if inode.isDir() {
			err = fs.SyncTree(inode)
		} else {
			err = inode.SyncFile()
		}
		return mapWinError(err)
	}

	return 0
}

// Opendir opens a directory.
func (fs *GoofysWin) Opendir(path string) (ret int, dhId uint64) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Opendir %v", path)
		defer func() {
			fuseLog.Debugf("<- Opendir %v = %v %v", path, ret, dhId)
		}()
	}

	atomic.AddInt64(&fs.stats.noops, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		return mapWinError(err), 0
	}

	dh := inode.OpenDir()
	handleID := fs.AddDirHandle(dh)

	return 0, uint64(handleID)
}

// Readdir reads a directory.
func (fs *GoofysWin) Readdir(path string,
	fill func(name string, stat *fuse.Stat_t, ofst int64) bool,
	ofst int64, dhId uint64) (ret int) {

	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Readdir %v %v %v", path, ofst, dhId)
		defer func() {
			fuseLog.Debugf("<- Readdir %v %v %v = %v", path, ofst, dhId, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	// Find the handle.
	fs.mu.RLock()
	dh := fs.dirHandles[fuseops.HandleID(dhId)]
	fs.mu.RUnlock()

	if dh == nil {
		return -fuse.EINVAL
	}

	dh.inode.logFuse("ReadDir", ofst)

	dh.mu.Lock()
	defer dh.mu.Unlock()

	dh.Seek(fuseops.DirOffset(ofst))

	for {
		inode, err := dh.ReadDir()
		if err != nil {
			return mapWinError(err)
		}
		if inode == nil {
			break
		}
		st := &fuse.Stat_t{}
		inode.mu.Lock()
		name := inode.Name
		attr := inode.InflateAttributes()
		makeFuseAttributes(&attr, st)
		inode.mu.Unlock()
		if dh.lastExternalOffset == 0 {
			name = "."
		} else if dh.lastExternalOffset == 1 {
			name = ".."
		}
		if !fill(name, st, int64(dh.lastExternalOffset)) {
			break
		}
		fuseLog.Debugf("<- Readdir %v %v %v = %v %v", path, ofst, dhId, name, dh.lastExternalOffset)
		// We have to modify it here because fill() MAY not send the entry
		dh.Next(name)
	}

	return 0
}

// Releasedir closes an open directory.
func (fs *GoofysWin) Releasedir(path string, dhId uint64) (ret int) {
	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Releasedir %v %v", path, dhId)
		defer func() {
			fuseLog.Debugf("<- Releasedir %v %v = %v", path, dhId, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.noops, 1)

	fs.mu.RLock()
	dh := fs.dirHandles[fuseops.HandleID(dhId)]
	fs.mu.RUnlock()

	dh.CloseDir()

	fs.mu.Lock()
	delete(fs.dirHandles, fuseops.HandleID(dhId))
	fs.mu.Unlock()

	return 0
}

// Fsyncdir synchronizes directory contents.
func (fs *GoofysWin) Fsyncdir(path string, datasync bool, fhId uint64) (ret int) {
	return fs.Fsync(path, datasync, fhId)
}

// Setxattr sets extended attributes.
func (fs *GoofysWin) Setxattr(path string, name string, value []byte, flags int) (ret int) {
	if fs.flags.DisableXattr {
		return -fuse.EOPNOTSUPP
	}

	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Setxattr %v %v %v %v", path, name, value, flags)
		defer func() {
			fuseLog.Debugf("<- Setxattr %v %v %v %v = %v", path, name, value, flags, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		return mapWinError(err)
	}

	if name == fs.flags.RefreshAttr {
		// Setting xattr with special name (.invalidate) refreshes the inode's cache
		return mapWinError(fs.RefreshInodeCache(inode))
	}

	err = inode.SetXattr(name, value, uint32(flags))
	return mapWinError(err)
}

// Getxattr gets extended attributes.
func (fs *GoofysWin) Getxattr(path string, name string) (ret int, data []byte) {
	if fs.flags.DisableXattr {
		return -fuse.EOPNOTSUPP, nil
	}

	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Getxattr %v %v", path, name)
		defer func() {
			fuseLog.Debugf("<- Getxattr %v %v = %v %v", path, name, ret, data)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		return mapWinError(err), nil
	}

	value, err := inode.GetXattr(name)
	if err != nil {
		return mapWinError(err), nil
	}

	return 0, value
}

// Removexattr removes extended attributes.
func (fs *GoofysWin) Removexattr(path string, name string) (ret int) {
	if fs.flags.DisableXattr {
		return -fuse.EOPNOTSUPP
	}

	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Removexattr %v %v", path, name)
		defer func() {
			fuseLog.Debugf("<- Removexattr %v %v = %v", path, name, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataWrites, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		return mapWinError(err)
	}

	err = inode.RemoveXattr(name)
	return mapWinError(err)
}

// Listxattr lists extended attributes.
func (fs *GoofysWin) Listxattr(path string, fill func(name string) bool) (ret int) {
	if fs.flags.DisableXattr {
		return -fuse.EOPNOTSUPP
	}

	if fuseLog.Level == logrus.DebugLevel {
		fuseLog.Debugf("-> Listxattr %v", path)
		defer func() {
			fuseLog.Debugf("<- Listxattr %v = %v", path, ret)
		}()
	}

	atomic.AddInt64(&fs.stats.metadataReads, 1)

	inode, err := fs.LookupPath(path)
	if err != nil {
		return mapWinError(err)
	}

	xattrs, err := inode.ListXattr()
	if err != nil {
		return mapWinError(err)
	}

	for _, name := range xattrs {
		if !fill(name) {
			return -fuse.ERANGE
		}
		fuseLog.Debugf("<- Listxattr %v = %v", path, name)
	}

	return 0
}

// Notify sends file invalidation/deletion notifications to the kernel
func (fs *GoofysWin) Notify(notifications []interface{}) {
	if fs.host == nil {
		return
	}
	var parent fuseops.InodeID
	var child string
	var op uint32
	for _, n := range notifications {
		switch v := n.(type) {
		case *fuseops.NotifyDelete:
			parent = v.Parent
			child = v.Name
			op = fuse.NOTIFY_UNLINK
		case *fuseops.NotifyInvalEntry:
			parent = v.Parent
			child = v.Name
			op = fuse.NOTIFY_CHMOD | fuse.NOTIFY_CHOWN | fuse.NOTIFY_UTIME | fuse.NOTIFY_CHFLAGS | fuse.NOTIFY_TRUNCATE
		default:
			panic("Unexpected notification")
		}
		fs.mu.RLock()
		in := fs.inodes[parent]
		fs.mu.RUnlock()
		if in != nil {
			in.mu.Lock()
			p := in.FullName()
			in.mu.Unlock()
			fs.host.Notify("/"+p+"/"+child, op)
		}
	}
}

func (fs *GoofysWin) WinDirRefresher() {
	for atomic.LoadInt32(&fs.shutdown) == 0 {
		select {
		case <-time.After(1 * time.Second):
		case <-fs.shutdownCh:
			return
		}
		fs.mu.Lock()
		var dirs []*Inode
		for _, dh := range fs.dirHandles {
			dirs = append(dirs, dh.inode)
		}
		fs.mu.Unlock()
		expireUnix := time.Now().Add(-fs.flags.StatCacheTTL)
		notifications := make(map[string]struct{})
		for _, dir := range dirs {
			dir.mu.Lock()
			if dir.Parent != nil && dir.dir.DirTime.Before(expireUnix) {
				notifications["/"+dir.FullName()] = struct{}{}
			}
			dir.mu.Unlock()
		}
		for dir := range notifications {
			fs.host.Notify(dir, fuse.NOTIFY_CHMOD | fuse.NOTIFY_CHOWN | fuse.NOTIFY_UTIME | fuse.NOTIFY_CHFLAGS | fuse.NOTIFY_TRUNCATE)
		}
	}
}

// Mount the file system based on the supplied arguments, returning a
// MountedFS that can be joined to wait for unmounting.
func MountWin(
	ctx context.Context,
	bucketName string,
	flags *cfg.FlagStorage) (fs *Goofys, mfs MountedFS, err error) {
	fs, err = NewGoofys(ctx, bucketName, flags)
	if fs == nil {
		if err == nil {
			err = fmt.Errorf("GeeseFS initialization failed")
		}
		return
	}
	mfs, err = mountFuseFS(fs)
	return
}

func mountFuseFS(fs *Goofys) (mfs MountedFS, err error) {
	var mountOpt []string
	if fs.flags.DebugFuse {
		mountOpt = append(mountOpt, "-o", "debug")
	}
	mountOpt = append(mountOpt, "-o", fmt.Sprintf("uid=%v", int32(fs.flags.Uid)))
	mountOpt = append(mountOpt, "-o", fmt.Sprintf("gid=%v", int32(fs.flags.Gid)))
	for _, s := range fs.flags.MountOptions {
		mountOpt = append(mountOpt, "-o", s)
	}
	fuseLog.Debugf("Starting WinFSP with options: %v", mountOpt)

	fsint := NewGoofysWin(fs)
	fuse.RecoverFromPanic = false
	host := fuse.NewFileSystemHost(fsint)
	fsint.host = host
	host.SetCapReaddirPlus(true)
	go func() {
		ok := host.Mount(fs.flags.MountPoint, mountOpt)
		if !ok {
			fsint.initCh <- 0
		}
	}()
	v := <-fsint.initCh
	if v == 0 {
		return nil, fmt.Errorf("WinFSP initialization failed")
	}

	return fsint, nil
}

// Join is a part of MountedFS interface
func (fs *GoofysWin) Join(ctx context.Context) error {
	<-fs.initCh
	return nil
}

// Unmount is also a part of MountedFS interface
func (fs *GoofysWin) Unmount() error {
	if !fs.initialized {
		return fmt.Errorf("not mounted")
	}
	r := fs.host.Unmount()
	if !r {
		return fmt.Errorf("unmounting failed")
	}
	fs.Shutdown()
	return nil
}
