//go:build !windows

package core

import (
	"context"
	"os"
	"sync"
	"syscall"
	"testing"
	"time"

	"github.com/jacobsa/fuse/fuseops"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// newLookupFS returns a GoofysFuse with a root directory and, if childName
// is set, one cached child in it. Nothing in it needs a backend as long as
// the cached attributes are used.
func newLookupFS(childName string, dir bool, symlink bool) (*GoofysFuse, *Inode, *Inode) {
	flags := cfg.DefaultFlags()
	fs := &Goofys{flags: flags}
	fs.flusherCond = sync.NewCond(&fs.flusherMu)
	fs.inodes = make(map[fuseops.InodeID]*Inode)
	fs.inodesByTime = make(map[int64]map[fuseops.InodeID]bool)
	root := NewInode(fs, nil, "")
	root.Id = fuseops.RootInodeID
	root.ToDir()
	root.dir.DirTime = time.Now()
	root.AttrTime = time.Now()
	fs.inodes[root.Id] = root
	var child *Inode
	if childName != "" {
		child = NewInode(fs, root, childName)
		child.Id = fuseops.RootInodeID + 1
		if dir {
			child.ToDir()
			child.dir.DirTime = time.Now()
		}
		if symlink {
			child.userMetadata = map[string][]byte{flags.SymlinkAttr: []byte("target")}
		}
		child.AttrTime = time.Now()
		root.insertChildUnlocked(child)
		fs.inodes[child.Id] = child
	}
	return &GoofysFuse{Goofys: fs}, root, child
}

// TestLookUpInodeConcurrentAttributes calls LookUpInode while another
// goroutine changes the inode the way the write and flush paths do: with
// inode.mu held. Before the fix, LookUpInode read the attributes without
// inode.mu, and LookUpCached read CacheState without an atomic load, so
// both were data races (run with -race).
func TestLookUpInodeConcurrentAttributes(t *testing.T) {
	const (
		changeNone  = iota
		changeSize  // grow Attributes.Size, as a write does
		changeMtime // set Attributes.Mtime, as a write does
		changeState // SetCacheState(ST_MODIFIED), as the flusher does
	)
	tests := []struct {
		description string
		child       string // "" means the directory is empty
		lookup      string
		dir         bool
		symlink     bool
		expiredAttr bool  // the child's attributes are older than StatCacheTTL
		state       int32 // child CacheState
		size        uint64
		change      int
		// expected result; the size is in [expectedMinSize, expectedMaxSize]
		expectedErr     error
		expectedMode    os.FileMode
		expectedNlink   uint32
		expectedMinSize uint64
		expectedMaxSize uint64
	}{
		{description: "positive: cached file returns its attributes",
			child: "file", lookup: "file", size: 10, change: changeNone,
			expectedMode: 0644, expectedNlink: 1, expectedMinSize: 10, expectedMaxSize: 10},
		{description: "positive: file that grows while it is looked up",
			child: "file", lookup: "file", size: 10, change: changeSize,
			expectedMode: 0644, expectedNlink: 1, expectedMinSize: 10, expectedMaxSize: 10 + 200},
		{description: "positive: modified file with expired attributes uses the local state",
			child: "file", lookup: "file", expiredAttr: true, state: ST_MODIFIED, size: 10, change: changeState,
			expectedMode: 0644, expectedNlink: 1, expectedMinSize: 10, expectedMaxSize: 10},
		{description: "negative: missing name in a fresh directory is ENOENT",
			child: "file", lookup: "other", change: changeSize, expectedErr: syscall.ENOENT},
		{description: "negative: empty directory is ENOENT",
			child: "", lookup: "file", change: changeNone, expectedErr: syscall.ENOENT},
		{description: "boundary: empty file",
			child: "file", lookup: "file", size: 0, change: changeMtime,
			expectedMode: 0644, expectedNlink: 1, expectedMinSize: 0, expectedMaxSize: 0},
		{description: "corner: directory has 2 links and the directory mode",
			child: "sub", lookup: "sub", dir: true, change: changeMtime,
			expectedMode: os.ModeDir | 0755, expectedNlink: 2, expectedMinSize: 0, expectedMaxSize: 0},
		{description: "corner: symlink has the symlink mode",
			child: "link", lookup: "link", symlink: true, change: changeMtime,
			expectedMode: os.ModeSymlink | 0644, expectedNlink: 1, expectedMinSize: 0, expectedMaxSize: 0},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			fs, root, child := newLookupFS(tc.child, tc.dir, tc.symlink)
			if child != nil {
				child.Attributes.Size = tc.size
				if !tc.dir {
					child.Attributes.Mode = 0644
				} else {
					child.Attributes.Mode = os.ModeDir | 0755
				}
				child.CacheState = tc.state
				if tc.expiredAttr {
					child.AttrTime = time.Now().Add(-2 * fs.flags.StatCacheTTL)
				}
			}

			start := make(chan struct{})
			var wg sync.WaitGroup
			if child != nil && tc.change != changeNone {
				wg.Add(1)
				go func() {
					defer wg.Done()
					<-start
					for range 200 {
						child.mu.Lock()
						switch tc.change {
						case changeSize:
							child.Attributes.Size++
						case changeMtime:
							child.Attributes.Mtime = time.Now()
						case changeState:
							child.SetCacheState(ST_MODIFIED)
						}
						child.mu.Unlock()
					}
				}()
			}
			close(start)
			for range 200 {
				op := &fuseops.LookUpInodeOp{Parent: root.Id, Name: tc.lookup}
				err := fs.LookUpInode(context.Background(), op)
				if err != tc.expectedErr {
					t.Errorf("LookUpInode error = %v, expected %v", err, tc.expectedErr)
					break
				}
				if err != nil {
					continue
				}
				attr := op.Entry.Attributes
				if attr.Mode != tc.expectedMode || attr.Nlink != tc.expectedNlink ||
					attr.Size < tc.expectedMinSize || attr.Size > tc.expectedMaxSize {
					t.Errorf("attributes = mode %v nlink %v size %v, expected mode %v nlink %v size %v..%v",
						attr.Mode, attr.Nlink, attr.Size,
						tc.expectedMode, tc.expectedNlink, tc.expectedMinSize, tc.expectedMaxSize)
					break
				}
			}
			wg.Wait()
		})
	}
}

// BenchmarkLookUpInode looks up a cached file, in parallel.
func BenchmarkLookUpInode(b *testing.B) {
	fs, root, child := newLookupFS("file", false, false)
	child.Attributes.Size = 10
	b.ReportAllocs()
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			op := &fuseops.LookUpInodeOp{Parent: root.Id, Name: "file"}
			if err := fs.LookUpInode(context.Background(), op); err != nil {
				b.Error(err)
				return
			}
		}
	})
}
