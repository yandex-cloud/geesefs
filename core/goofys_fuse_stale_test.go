//go:build !windows

package core

import (
	"context"
	"fmt"
	"syscall"
	"time"

	"github.com/jacobsa/fuse/fuseops"
	. "gopkg.in/check.v1"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// The kernel can send a request for an inode number until it sends FORGET for
// that number. GeeseFS can drop the inode from fs.inodes before that:
// EvictEntry removes expired inodes that the kernel still references. A
// request for such an inode must fail with ESTALE, like a request for a dead
// inode. It must not stop the daemon.
type StaleInodeTest struct {
	fs  *GoofysFuse
	dir fuseops.InodeID
}

var _ = Suite(&StaleInodeTest{})

func (s *StaleInodeTest) SetUpTest(t *C) {
	flags := cfg.DefaultFlags()
	flags.DisableXattr = false
	flags.IgnoreFsync = false
	flags.EmulateHardlinks = true
	backend := &TestBackend{err: syscall.ENOSYS}
	goofys, err := newGoofys(context.Background(), "test", flags,
		func(string, *cfg.FlagStorage) (StorageBackend, error) {
			return backend, nil
		})
	t.Assert(err, IsNil)
	s.fs = NewGoofysFuse(goofys)

	root := goofys.getInodeOrDie(fuseops.RootInodeID)
	dir := NewInode(goofys, root, "dir")
	dir.ToDir()
	root.mu.Lock()
	goofys.insertInode(root, dir)
	root.mu.Unlock()

	// A LOOKUP reply gives the kernel one reference to the directory.
	dir.Ref()
	// The entry expires after --stat-cache-ttl, and the evictor drops it.
	dir.mu.Lock()
	dir.SetExpireTime(time.Now().Add(-time.Second))
	dir.mu.Unlock()
	t.Assert(goofys.EvictEntry(dir.Id), Equals, true)

	s.dir = dir.Id
}

func (s *StaleInodeTest) TestRequestsForEvictedInodeReturnESTALE(t *C) {
	ctx := context.Background()
	id := s.dir
	root := fuseops.InodeID(fuseops.RootInodeID)
	requests := map[string]func() error{
		"GetInodeAttributes": func() error {
			return s.fs.GetInodeAttributes(ctx, &fuseops.GetInodeAttributesOp{Inode: id})
		},
		"GetXattr": func() error {
			return s.fs.GetXattr(ctx, &fuseops.GetXattrOp{Inode: id, Name: "user.a"})
		},
		"ListXattr": func() error {
			return s.fs.ListXattr(ctx, &fuseops.ListXattrOp{Inode: id})
		},
		"RemoveXattr": func() error {
			return s.fs.RemoveXattr(ctx, &fuseops.RemoveXattrOp{Inode: id, Name: "user.a"})
		},
		"SetXattr": func() error {
			return s.fs.SetXattr(ctx, &fuseops.SetXattrOp{Inode: id, Name: "user.a", Value: []byte("v")})
		},
		"CreateSymlink": func() error {
			return s.fs.CreateSymlink(ctx, &fuseops.CreateSymlinkOp{Parent: id, Name: "l", Target: "t"})
		},
		"ReadSymlink": func() error {
			return s.fs.ReadSymlink(ctx, &fuseops.ReadSymlinkOp{Inode: id})
		},
		"CreateLink target": func() error {
			return s.fs.CreateLink(ctx, &fuseops.CreateLinkOp{Parent: root, Name: "h", Target: id})
		},
		"CreateLink parent": func() error {
			return s.fs.CreateLink(ctx, &fuseops.CreateLinkOp{Parent: id, Name: "h", Target: root})
		},
		"LookUpInode": func() error {
			return s.fs.LookUpInode(ctx, &fuseops.LookUpInodeOp{Parent: id, Name: "f"})
		},
		"OpenDir": func() error {
			return s.fs.OpenDir(ctx, &fuseops.OpenDirOp{Inode: id})
		},
		"OpenFile": func() error {
			return s.fs.OpenFile(ctx, &fuseops.OpenFileOp{Inode: id})
		},
		"SyncFile": func() error {
			return s.fs.SyncFile(ctx, &fuseops.SyncFileOp{Inode: id})
		},
		"CreateFile": func() error {
			return s.fs.CreateFile(ctx, &fuseops.CreateFileOp{Parent: id, Name: "f", Mode: 0644})
		},
		"MkNode": func() error {
			return s.fs.MkNode(ctx, &fuseops.MkNodeOp{Parent: id, Name: "f", Mode: 0644})
		},
		"MkDir": func() error {
			return s.fs.MkDir(ctx, &fuseops.MkDirOp{Parent: id, Name: "d", Mode: 0755})
		},
		"RmDir": func() error {
			return s.fs.RmDir(ctx, &fuseops.RmDirOp{Parent: id, Name: "d"})
		},
		"SetInodeAttributes": func() error {
			return s.fs.SetInodeAttributes(ctx, &fuseops.SetInodeAttributesOp{Inode: id})
		},
		"Unlink": func() error {
			return s.fs.Unlink(ctx, &fuseops.UnlinkOp{Parent: id, Name: "f"})
		},
		"Rename old parent": func() error {
			return s.fs.Rename(ctx, &fuseops.RenameOp{OldParent: id, OldName: "a", NewParent: root, NewName: "b"})
		},
		"Rename new parent": func() error {
			return s.fs.Rename(ctx, &fuseops.RenameOp{OldParent: root, OldName: "a", NewParent: id, NewName: "b"})
		},
		"Fallocate": func() error {
			return s.fs.Fallocate(ctx, &fuseops.FallocateOp{Inode: id, Length: 1})
		},
	}
	for name, request := range requests {
		var err error
		func() {
			defer func() {
				if r := recover(); r != nil {
					err = fmt.Errorf("panic: %v", r)
				}
			}()
			err = request()
		}()
		t.Check(err, Equals, error(syscall.ESTALE), Commentf("%v", name))
	}

	// FORGET for the evicted inode stays harmless.
	t.Check(s.fs.ForgetInode(ctx, &fuseops.ForgetInodeOp{Inode: id, N: 1}), IsNil)
}
