// +build !windows

package internal

import (
	"context"
	"fmt"
	iofs "io/fs"
	"os"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/jacobsa/fuse/fuseops"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/yandex-cloud/geesefs/internal/cfg"
	"github.com/yandex-cloud/geesefs/internal/pb"
	"google.golang.org/grpc"
)

var (
	ownerLog = cfg.GetLogger("owner")
	statLog  = cfg.GetLogger("stat")
)

const (
	N_INODES  = fuseops.InodeID(1 << 32)
	N_HANDLES = fuseops.HandleID(1 << 32)

	STEAL_INODE_BACKOFF = 200 * time.Millisecond

	UNKNOWN_OWNER = 0
)

type ClusterFs struct {
	Flags  *cfg.FlagStorage
	Conns  *ConnPool
	Goofys *Goofys
	mfs    Joinable

	stat Stat
}

func NewClusterGoofys(ctx context.Context, bucket string, flags *cfg.FlagStorage, conns *ConnPool) (*Goofys, error) {
	fs, err := NewGoofys(ctx, bucket, flags)
	if err != nil {
		return nil, err
	}

	// choose node with min id as root owner
	var rootOwner NodeId
	for i := range flags.ClusterPeers {
		nodeId := NodeId(flags.ClusterPeers[i].Id)
		if i == 0 {
			rootOwner = nodeId
		} else {
			if nodeId < rootOwner {
				rootOwner = nodeId
			}
		}
	}

	fs.inodes[fuseops.RootInodeID].ownerTerm = 0
	fs.inodes[fuseops.RootInodeID].owner = rootOwner
	fs.inodes[fuseops.RootInodeID].readyOwner = true
	fs.nextHandleID = N_HANDLES * fuseops.HandleID(conns.id)
	fs.nextInodeID = N_INODES * fuseops.InodeID(conns.id)

	return fs, nil
}

// REQUIRED_LOCK(parent.KeepOwnerLock)
func (fs *ClusterFs) createFile(parent *Inode, name string, mode os.FileMode) (
	*pb.Inode,
	uint64, // childId
	*pb.Attributes,
	uint64, // handleId
) {
	parent.UpgradeToStateLock()

	if parent.findChildUnlocked(name) != nil {
		parent.DowngradeToKeepOwnerLock()
		return nil, 0, nil, 0
	}

	child := fs.createChild(parent, name, mode)

	child.Ref()

	// allocate file handle
	fh := NewFileHandle(child)
	child.fileHandles = 1
	handleId := fs.Goofys.AddFileHandle(fh)

	pbInode := child.pbInode()
	childId := uint64(child.Id)
	attr := child.pbAttr()

	child.StateUnlock()
	parent.DowngradeToKeepOwnerLock()

	return pbInode, childId, attr, uint64(handleId)
}

// REQUIRED_LOCK(parent.KeepOwnerLock)
func (fs *ClusterFs) unlink(parent *Inode, name string) error {
	parent.UpgradeToStateLock()

	child := parent.findChildUnlocked(name)
	if child == nil {
		parent.DowngradeToKeepOwnerLock()
		return nil
	}

	if child.isDir() {
		parent.DowngradeToKeepOwnerLock()
		return syscall.EISDIR
	}

	child.KeepOwnerLock()
	if child.owner != fs.Conns.id {
		err := fs.steal(child)
		if err != nil {
			child.KeepOwnerUnlock()
			parent.DowngradeToKeepOwnerLock()
			return err
		}
	}

	child.UpgradeToStateLock()
	atomic.AddUint64(&fs.stat.removeInodeCnt, 1)
	child.doUnlink()
	fs.Goofys.WakeupFlusher()
	child.StateUnlock()

	parent.DowngradeToKeepOwnerLock()

	return nil
}

// REQUIRED_LOCK(inode.KeepOwnerLock)
func (fs *ClusterFs) createSymlink(parent *Inode, name string, target string) (
	*pb.Inode,
	uint64, // childId
	*pb.Attributes,
) {
	parent.UpgradeToStateLock()

	if parent.findChildUnlocked(name) != nil {
		parent.DowngradeToKeepOwnerLock()
		return nil, 0, nil
	}

	child := fs.createChild(parent, name, fs.Flags.FileMode|iofs.ModeSymlink)

	child.userMetadata[fs.Flags.SymlinkAttr] = []byte(target)
	child.userMetadataDirty = 2

	child.Ref()

	pbInode := child.pbInode()
	childId := uint64(child.Id)
	attr := child.pbAttr()

	child.StateUnlock()
	parent.DowngradeToKeepOwnerLock()

	return pbInode, childId, attr
}

// REQUIRED_LOCK(inode.KeepOwnerLock)
func (fs *ClusterFs) readSymlink(inode *Inode) (target string, err error) {
	inode.UpgradeToStateLock()
	defer inode.DowngradeToKeepOwnerLock()

	if inode.userMetadata[inode.fs.flags.SymlinkAttr] == nil {
		return "", syscall.EIO
	}

	return string(inode.userMetadata[inode.fs.flags.SymlinkAttr]), nil
}

// REQUIRED_LOCK(inode.KeepOwnerLock)
func (fs *ClusterFs) openFile(inode *Inode) fuseops.HandleID {
	fh := NewFileHandle(inode)

	n := atomic.AddInt32(&inode.fileHandles, 1)
	if n == 1 && inode.CacheState == ST_CACHED {
		inode.Parent.addModified(1)
	}

	handleId := fs.Goofys.AddFileHandle(fh)

	return handleId
}

func (fs *ClusterFs) releaseFileHandle(handleId fuseops.HandleID) {
	fuseLog.Debugf("-> releaseFileHandle(%v)", handleId)
	defer fuseLog.Debugf("<- releaseFileHandle(%v)", handleId)

	fs.Goofys.mu.Lock()
	defer fs.Goofys.mu.Unlock()

	fh := fs.Goofys.fileHandles[handleId]
	fh.Release()
	delete(fs.Goofys.fileHandles, handleId)
}

// REQUIRED_LOCK(inode.mu)
func (fs *ClusterFs) readFile(handleId fuseops.HandleID, offset int64, size int64) (data [][]byte, bytesRead int, err error) {
	fs.Goofys.mu.RLock()
	fh := fs.Goofys.fileHandles[handleId]
	fs.Goofys.mu.RUnlock()

	return fh.ReadFile(offset, size)
}

// REQUIRED_LOCK(inode.mu)
func (fs *ClusterFs) writeFile(handleId fuseops.HandleID, offset int64, data []byte) (suppressReuse bool, err error) {
	fs.Goofys.mu.RLock()
	fh := fs.Goofys.fileHandles[handleId]
	fs.Goofys.mu.RUnlock()

	// fuse binding leaves extra room for header, so we
	// account for it when we decide whether to do "zero-copy" write
	copyData := len(data) < cap(data)-4096
	err = fh.WriteFile(offset, data, copyData)
	err = mapAwsError(err)
	suppressReuse = !copyData

	return
}

// LOCK_REQUIRED(parent.KeepOwnerLock)
func (fs *ClusterFs) mkDir(parent *Inode, name string, mode os.FileMode) (
	*pb.Inode,
	uint64,
	*pb.Attributes,
) {
	parent.UpgradeToStateLock()

	if parent.findChildUnlocked(name) != nil {
		parent.DowngradeToKeepOwnerLock()
		return nil, 0, nil
	}

	if parent.dir.DeletedChildren != nil {
		if child, ok := parent.dir.DeletedChildren[name]; ok {
			if child.isDir() {
				atomic.AddUint64(&fs.stat.createInodeCnt, 1)
				atomic.AddUint64(&fs.stat.resurectionCnt, 1)

				child.ChangeOwnerLock()
				fs.Goofys.mu.Lock()

				// create forget clone of child
				childClone := NewInode(parent.fs, parent, name)
				childClone.ToDir()
				childClone.Id = child.Id
				childClone.refcnt = child.refcnt
				childClone.ownerTerm = child.ownerTerm
				childClone.owner = child.owner
				childClone.readyOwner = true

				// resurrect old child

				// update id
				child.Id = parent.fs.allocateInodeId()

				// reset metadata
				child.userMetadataDirty = 0
				child.userMetadata = make(map[string][]byte)

				// time
				child.touch()
				child.Attributes.Ctime = time.Now()
				if parent.Attributes.Ctime.Before(child.Attributes.Ctime) {
					parent.Attributes.Ctime = child.Attributes.Ctime
				}
				child.Attributes.Mtime = time.Now()
				if parent.Attributes.Mtime.Before(child.Attributes.Mtime) {
					parent.Attributes.Mtime = child.Attributes.Mtime
				}

				// state
				child.SetCacheState(ST_MODIFIED)

				// refs
				child.refcnt = 0
				child.Ref()

				// stop deleting it
				delete(parent.dir.DeletedChildren, name)

				// swap them in fs.inodes
				parent.fs.inodes[childClone.Id] = childClone
				parent.fs.inodes[child.Id] = child

				// insert resurrected child in tree
				parent.insertChildUnlocked(child)

				pbInode := child.pbInode()
				childId := uint64(child.Id)
				childAttr := child.pbAttr()

				fs.Goofys.mu.Unlock()
				child.ChangeOwnerUnlock()
				parent.DowngradeToKeepOwnerLock()

				return pbInode, childId, childAttr
			}
		}
	}

	child := fs.createChild(parent, name, mode)

	child.Ref()

	pbInode := child.pbInode()
	childId := uint64(child.Id)
	childAttr := child.pbAttr()

	child.StateUnlock()
	parent.DowngradeToKeepOwnerLock()

	return pbInode, childId, childAttr
}

// REQUIRED_LOCK(parent.KeepOwnerLock)
func (fs *ClusterFs) rmDir(parent *Inode, name string) error {
	parent.UpgradeToStateLock()

	child := parent.findChildUnlocked(name)
	if child == nil {
		parent.DowngradeToKeepOwnerLock()
		return nil
	}

	if !child.isDir() {
		parent.DowngradeToKeepOwnerLock()
		return syscall.ENOTDIR
	}

	child.KeepOwnerLock()
	if child.owner != fs.Conns.id {
		err := fs.steal(child)
		if err != nil {
			child.KeepOwnerUnlock()
			parent.DowngradeToKeepOwnerLock()
			return err
		}
	}

	child.UpgradeToStateLock()

	if len(child.dir.Children) > 0 {
		child.StateUnlock()
		parent.DowngradeToKeepOwnerLock()
		return syscall.ENOTEMPTY
	}

	atomic.AddUint64(&fs.stat.removeInodeCnt, 1)
	child.doUnlink()
	fs.Goofys.WakeupFlusher()

	child.StateUnlock()

	parent.DowngradeToKeepOwnerLock()

	return nil
}

// REQUIRED_LOCK(inode.KeepOwnerLock)
func (fs *ClusterFs) openDir(inode *Inode) fuseops.HandleID {
	dh := NewDirHandle(inode)
	inode.mu.Lock()
	inode.dir.handles = append(inode.dir.handles, dh)
	atomic.AddInt32(&inode.fileHandles, 1)
	inode.mu.Unlock()
	handleId := fs.Goofys.AddDirHandle(dh)
	return handleId
}

func (fs *ClusterFs) releaseDirHandle(handleId fuseops.HandleID) {
	fs.Goofys.mu.RLock()
	dh := fs.Goofys.dirHandles[handleId]
	fs.Goofys.mu.RUnlock()

	dh.CloseDir()

	fs.Goofys.mu.Lock()
	delete(fs.Goofys.dirHandles, handleId)
	fs.Goofys.mu.Unlock()
}

// REQUIRED_LOCK(dh.inode.KeepOwnerLock)
func (fs *ClusterFs) readDir(handleId fuseops.HandleID, offset fuseops.DirOffset, dst []byte, bytesRead *int) (err error) {
	fs.Goofys.mu.RLock()
	dh := fs.Goofys.dirHandles[handleId]
	fs.Goofys.mu.RUnlock()

	dh.mu.Lock()
	defer dh.mu.Unlock()

	err = dh.loadChildren()
	if err != nil {
		err = mapAwsError(err)
		return err
	}

	if offset == 0 {
		dh.lastExternalOffset = 0
		dh.lastInternalOffset = 0
		dh.lastName = ""
	}

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

		n := fuseutil.WriteDirent(dst[*bytesRead:], makeDirEntry(e, dh.lastExternalOffset))
		if n == 0 {
			break
		}

		*bytesRead += n
		// We have to modify it here because WriteDirent MAY not send the entry
		dh.Next(e.Name)
	}

	return nil
}

// REQUIRED_LOCK(parent.KeepOwnerLock)
func (fs *ClusterFs) lookUpInode1(parent *Inode, name string) (
	*pb.Inode,
	uint64,
	*pb.Attributes,
	error,
) {
	if parent.findChildUnlocked(name) == nil {
		parent.loadChild(name)
	}

	parent.UpgradeToStateLock()

	child := parent.findChildUnlocked(name)
	if child == nil {
		parent.DowngradeToKeepOwnerLock()
		return nil, 0, nil, syscall.ENOENT
	}

	child.ChangeOwnerLock()
	if child.owner == UNKNOWN_OWNER {
		fs.unshadow(child)
	}
	child.ChangeOwnerUnlock()

	pbAttr, err := fs.lookUpInode2(child)
	if err != nil {
		parent.DowngradeToKeepOwnerLock()
		return nil, 0, nil, err
	}

	pbInode := child.pbInode()
	inodeId := uint64(child.Id)

	parent.DowngradeToKeepOwnerLock()

	return pbInode, inodeId, pbAttr, nil
}

// REQUIRED_LOCK(parent.StateLock)
func (fs *ClusterFs) lookUpInode2(inode *Inode) (pbAttr *pb.Attributes, err error) {
	fs.route(
		func() *Inode { return inode },
		false,
		func(inode *Inode) {
			inode.UpgradeToStateLock()
			pbAttr = inode.pbAttr()
			inode.Ref()
			inode.DowngradeToKeepOwnerLock()
		},
		func(inode *Inode, inodeOwner NodeId) *pb.Owner {
			var resp *pb.LookUpInode2Response
			fs.Conns.Unary(inodeOwner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.LookUpInode2Request{
					InodeId: uint64(inode.Id),
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).LookUpInode2(ctx, req)
				return err
			})
			if err != nil {
				return nil
			}

			if resp.AnotherOwner != nil {
				return resp.AnotherOwner
			}

			pbAttr = resp.Attr

			return nil
		},
	)
	return
}

// REQUIRED_LOCK(inode.KeepOwnerLock)
func (fs *ClusterFs) getInodeAttributes(inode *Inode, size *uint64, mtime *time.Time, ctime *time.Time, mode *os.FileMode) {
	inode.mu.Lock()
	attr := inode.GetAttributes()
	inode.mu.Unlock()

	*size = attr.Size
	*mtime = attr.Mtime
	*ctime = attr.Ctime
	*mode = attr.Mode
}

// REQUIRED_LOCK(inode.KeepOwnerLock)
func (fs *ClusterFs) setInodeAttributes(inode *Inode, size *uint64, mtime *time.Time, ctime *time.Time, mode *os.FileMode) error {
	modified := false

	if size != nil && inode.Attributes.Size != *size {
		if *size > fs.Goofys.getMaxFileSize() {
			return syscall.EFBIG
		}
		inode.ResizeUnlocked(*size, true)
		modified = true
	}

	if mode != nil {
		m, err := inode.setFileMode(*mode)
		if err != nil {
			return err
		}
		modified = modified || m
	}

	if mtime != nil && fs.Flags.EnableMtime && inode.Attributes.Mtime != *mtime {
		inode.Attributes.Mtime = *mtime
		inode.setUserMeta(fs.Flags.MtimeAttr, []byte(fmt.Sprintf("%d", inode.Attributes.Mtime.Unix())))
		modified = true
	}

	if modified && inode.CacheState == ST_CACHED {
		inode.SetCacheState(ST_MODIFIED)
		inode.fs.WakeupFlusher()
	}

	attr := inode.GetAttributes()

	*size = attr.Size
	*mtime = attr.Mtime
	*ctime = attr.Ctime
	*mode = attr.Mode

	return nil
}

// getting of inode

func (fs *ClusterFs) inodeById(inodeId fuseops.InodeID) *Inode {
	return fs.Goofys.getInodeOrDie(inodeId)
}

func (fs *ClusterFs) inodeByFileHandleId(handleId fuseops.HandleID) *Inode {
	fs.Goofys.mu.RLock()
	defer fs.Goofys.mu.RUnlock()
	return fs.Goofys.fileHandles[handleId].inode
}

func (fs *ClusterFs) inodeByDirHandleId(handleId fuseops.HandleID) *Inode {
	fs.Goofys.mu.RLock()
	defer fs.Goofys.mu.RUnlock()
	return fs.Goofys.dirHandles[handleId].inode
}

// stealing

// REQUIRED_LOCK(inode.KeepOwnerLock)
func (fs *ClusterFs) trySteal(inode *Inode) (success bool, err error) {
	defer func() {
		atomic.AddUint64(&fs.stat.tryStealCnt, 1)
		if success {
			atomic.AddUint64(&fs.stat.successTryStealCnt, 1)
		}
	}()
	for {
		switch {
		case inode.owner == fs.Conns.id:
			return true, nil
		case inode.owner != fs.Conns.id:
			owner := inode.owner

			inode.KeepOwnerUnlock()
			var resp *pb.TryStealInodeOwnershipResponse
			err := fs.Conns.Unary(owner, func(ctx context.Context, conn *grpc.ClientConn) error {
				req := &pb.TryStealInodeOwnershipRequest{
					InodeId: uint64(inode.Id),
					NodeId:  uint64(fs.Conns.id),
				}
				var err error
				resp, err = pb.NewFsGrpcClient(conn).TryStealInodeOwnership(ctx, req)
				return err
			})
			if err != nil {
				return false, err
			}

			if resp.AnotherOwner != nil {
				inode.ChangeOwnerLock()
				inode.applyOwner(resp.AnotherOwner)
				inode.ChangeOwnerUnlock()
				inode.KeepOwnerLock()
				continue
			}

			if resp.StolenInode != nil {
				inode.ChangeOwnerLock()
				fs.applyStolenInode(inode, resp.StolenInode)
				ownerLog.Infof("%v \"%v\" %v %v", inode.Id, inode.Name, owner, fs.Conns.id)
				inode.ChangeOwnerUnlock()
				inode.KeepOwnerLock()
				if inode.owner == fs.Conns.id {
					return true, nil
				} else {
					continue
				}
			} else {
				inode.KeepOwnerLock()
				return false, nil
			}
		}
	}
}

// REQUIRED_LOCK(inode.ChangeOwner)
func (fs *ClusterFs) applyStolenInode(inode *Inode, stolenInode *pb.StolenInode) {
	if inode.isDir() {
		for _, pbInode := range stolenInode.Children {
			child := fs.ensure(inode, pbInode)
			inode.dir.Children = append(inode.dir.Children, child)
			child.StateUnlock()
		}
	}
	inode.Attributes.Size = stolenInode.Attr.Size
	inode.Attributes.Mtime = stolenInode.Attr.Mtime.AsTime()
	inode.Attributes.Ctime = stolenInode.Attr.Ctime.AsTime()
	inode.Attributes.Mode = iofs.FileMode(stolenInode.Attr.Mode)

	inode.refcnt = stolenInode.Refcnt

	inode.ownerTerm = stolenInode.OwnerTerm
	inode.owner = fs.Conns.id
	inode.readyOwner = true
}

// REQUIRED_LOCK(inode.KeepOwnerLock)
func (fs *ClusterFs) steal(inode *Inode) error {
	for {
		stolen, err := fs.trySteal(inode)
		if err != nil {
			return err
		}
		if stolen {
			return nil
		}
		time.Sleep(STEAL_INODE_BACKOFF)
	}
}

// REQUIRED_LOCK(inode.ChangeOwnerLock)
func (fs *ClusterFs) tryYield(inode *Inode, newOwner NodeId) *pb.StolenInode {
	if inode.CacheState == ST_CACHED && inode.fileHandles == 0 {
		if inode.isDir() {
			var children []*pb.Inode
			for _, child := range inode.dir.Children {
				child.KeepOwnerLock()
				if child.owner == UNKNOWN_OWNER {
					child.KeepOwnerUnlock()
					child.ChangeOwnerLock()
					if child.owner == UNKNOWN_OWNER {
						fs.unshadow(child)
					}
					child.ChangeOwnerUnlock()
					child.KeepOwnerLock()
				}
			}
			for _, child := range inode.dir.Children {
				children = append(children, child.pbInode())
			}
			for _, child := range inode.dir.Children {
				child.KeepOwnerUnlock()
			}
			if len(inode.dir.DeletedChildren) == 0 {
				inode.dir.Children = nil
				inode.dir.DeletedChildren = nil

				inode.ownerTerm++
				inode.owner = newOwner
				inode.readyOwner = false

				userMetadata := inode.userMetadata
				inode.userMetadata = nil

				refcnt := inode.refcnt
				inode.refcnt = 0

				return &pb.StolenInode{
					OwnerTerm:    inode.ownerTerm,
					Attr:         inode.pbAttr(),
					UserMetadata: userMetadata,
					Refcnt:       refcnt,
					Children:     children,
				}
			} else {
				fuseLog.Infof("could not yield inode %v: len(inode.dir.DeletedChildren) == %v",
					inode.Id, len(inode.dir.DeletedChildren))
				return nil
			}
		} else {
			inode.resetCache()

			inode.ownerTerm++
			inode.owner = newOwner
			inode.readyOwner = false

			userMetadata := inode.userMetadata
			inode.userMetadata = nil

			refcnt := inode.refcnt
			inode.refcnt = 0

			return &pb.StolenInode{
				OwnerTerm:    inode.ownerTerm,
				Attr:         inode.pbAttr(),
				UserMetadata: userMetadata,
				Refcnt:       refcnt,
			}
		}
	} else {
		fuseLog.Infof("could not yield inode %v: inode.CacheState == %v inode.fileHandles == %v",
			inode.Id, inode.CacheState, inode.fileHandles)
		return nil
	}
}

// utils

const STAT_PRINT_INTERVAL = 1 * time.Second

type Stat struct {
	tryStealCnt        uint64
	successTryStealCnt uint64

	createInodeCnt uint64
	removeInodeCnt uint64
	forgetInodeCnt uint64

	resurectionCnt uint64
}

func (fs *ClusterFs) StatPrinter() {
	for {
		time.Sleep(STAT_PRINT_INTERVAL)
		statLog.Infof(
			"tryStealCnt=%v successTryStealCnt=%v createInodeCnt=%v removeInodeCnt=%v forgetInodeCnt=%v resurectionCnt=%v",
			atomic.LoadUint64(&fs.stat.tryStealCnt),
			atomic.LoadUint64(&fs.stat.successTryStealCnt),
			atomic.LoadUint64(&fs.stat.createInodeCnt),
			atomic.LoadUint64(&fs.stat.removeInodeCnt),
			atomic.LoadUint64(&fs.stat.forgetInodeCnt),
			atomic.LoadUint64(&fs.stat.resurectionCnt),
		)
	}
}

func (dh *DirHandle) loadChildren() error {
	inode := dh.inode
	for inode.dir.lastFromCloud == nil && !inode.dir.listDone {
		_, err := dh.listObjectsFlat()
		if err != nil {
			return err
		}
	}
	return nil
}

func (parent *Inode) loadChild(name string) (child *Inode, err error) {
	child, err = parent.LookUp(name, false)
	if err != nil {
		if child != nil {
			parent.removeChild(child)
		}
	}
	return
}

func (fs *ClusterFs) unshadow(inode *Inode) {
	atomic.AddUint64(&fs.stat.createInodeCnt, 1)

	inode.ownerTerm = 0
	inode.owner = fs.Conns.id
	inode.readyOwner = true

	ownerLog.Infof("%v \"%v\" _ %v", inode.Id, inode.Name, fs.Conns.id)
}

// Returns inode with StateLock!
func (fs *ClusterFs) ensure(parent *Inode, pbInode *pb.Inode) *Inode {
	fs.Goofys.mu.RLock()
	child, ok := fs.Goofys.inodes[fuseops.InodeID(pbInode.Id)]
	fs.Goofys.mu.RUnlock()
	if ok {
		child.StateLock()
		return child
	} else {
		atomic.AddUint64(&fs.stat.createInodeCnt, 1)

		child = NewInode(fs.Goofys, parent, pbInode.Name)
		child.Id = fuseops.InodeID(pbInode.Id)
		if pbInode.Dir {
			child.ToDir()
		}
		if pbInode.Symlink {
			child.Attributes.Mode = child.Attributes.Mode | iofs.ModeSymlink
		}
		child.ownerTerm = pbInode.Owner.Term
		child.owner = NodeId(pbInode.Owner.NodeId)
		child.readyOwner = false

		child.StateLock()

		fs.Goofys.mu.Lock()
		fs.Goofys.inodes[fuseops.InodeID(pbInode.Id)] = child
		fs.Goofys.mu.Unlock()

		return child
	}
}

// REQUIRED_LOCK(parent.StateLock)
// Returns inode with StateLock!
func (fs *ClusterFs) createChild(parent *Inode, name string, mode iofs.FileMode) *Inode {
	atomic.AddUint64(&fs.stat.createInodeCnt, 1)

	child := NewInode(fs.Goofys, parent, name)
	if mode&iofs.ModeDir != 0 {
		child.ToDir()
	}

	// metadata
	child.userMetadata = make(map[string][]byte)

	// time
	child.touch()
	if child.isDir() {
		if parent.Attributes.Ctime.Before(child.Attributes.Ctime) {
			parent.Attributes.Ctime = child.Attributes.Ctime
		}
		if parent.Attributes.Mtime.Before(child.Attributes.Mtime) {
			parent.Attributes.Mtime = child.Attributes.Mtime
		}
	}

	// mode
	if child.isDir() {
		if fs.Flags.EnablePerms {
			child.Attributes.Mode = os.ModeDir | (mode & os.ModePerm)
		} else {
			child.Attributes.Mode = os.ModeDir | fs.Flags.DirMode
		}
	} else {
		child.setFileMode(mode)
	}

	// owner
	child.ownerTerm = 0
	child.owner = fs.Conns.id
	child.readyOwner = true

	child.StateLock()

	// insert child
	parent.fs.insertInode(parent, child)
	child.SetCacheState(ST_CREATED)
	parent.fs.WakeupFlusher()

	ownerLog.Infof("%v \"%v\" _ %v", child.Id, child.Name, fs.Conns.id)

	return child
}

func (fs *ClusterFs) broadcastForget2(inodeId fuseops.InodeID) {
	fs.Conns.Broad(func(ctx context.Context, conn *grpc.ClientConn) error {
		req := &pb.ForgetInode2Request{
			InodeId: uint64(inodeId),
		}
		_, err := pb.NewFsGrpcClient(conn).ForgetInode2(ctx, req)
		return err
	})
}

// utils

func (fs *ClusterFs) routeByInodeId(
	inodeId fuseops.InodeID,
	trySteal bool,
	execLocally func(inode *Inode),
	tryExecRemotely func(inode *Inode, inodeOwner NodeId) *pb.Owner,
) {
	fs.route(
		func() *Inode {
			return fs.inodeById(inodeId)
		},
		trySteal,
		execLocally,
		tryExecRemotely,
	)
}

func (fs *ClusterFs) routeByFileHandle(
	handleId fuseops.HandleID,
	execLocally func(inode *Inode),
	tryExecRemotely func(inode *Inode, inodeOwner NodeId) *pb.Owner,
) {
	fs.route(
		func() *Inode {
			return fs.inodeByFileHandleId(handleId)
		},
		false,
		execLocally,
		tryExecRemotely,
	)
}

func (fs *ClusterFs) routeByDirHandle(
	handleId fuseops.HandleID,
	execLocally func(inode *Inode),
	tryExecRemotely func(inode *Inode, inodeOwner NodeId) *pb.Owner,
) {
	fs.route(
		func() *Inode {
			return fs.inodeByDirHandleId(handleId)
		},
		false,
		execLocally,
		tryExecRemotely,
	)
}

const READY_OWNER_BACKOFF = 100 * time.Millisecond

func (fs *ClusterFs) route(
	getInode func() *Inode,
	trySteal bool,
	execLocally func(inode *Inode),
	tryExecRemotely func(inode *Inode, inodeOwner NodeId) *pb.Owner,
) {
	for {
		inode := getInode()
		inode.KeepOwnerLock()
		if trySteal {
			fs.trySteal(inode)
		}
		switch {
		case inode.owner == fs.Conns.id:
			if inode.readyOwner {
				execLocally(inode)
				inode.KeepOwnerUnlock()
				return
			} else {
				inode.KeepOwnerUnlock()
				fuseLog.Debugf("this fs is owner of inode %v, but it is not ready", inode.info())
				time.Sleep(READY_OWNER_BACKOFF)
				continue
			}
		case inode.owner != fs.Conns.id:
			inodeOwner := inode.owner
			inode.KeepOwnerUnlock()
			anotherOwner := tryExecRemotely(inode, inodeOwner)
			if anotherOwner != nil {
				fuseLog.Debugf("fs %v is not owner of inode %v, apply new owner %+v and retry", inodeOwner, inode.info(), anotherOwner)
				inode.ChangeOwnerLock()
				inode.applyOwner(anotherOwner)
				inode.ChangeOwnerUnlock()
				continue
			} else {
				return
			}
		}
	}
}
