//go:build !windows

package core

import (
	"io"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/sirupsen/logrus"

	"github.com/yandex-cloud/geesefs/core/pb"
)

// TestClusterRouteConcurrentOwnerChange runs ClusterFs.route while another
// goroutine applies newer owners to the inode, the way the gRPC handlers do
// it: with ChangeOwnerLock held. Before the fix, route called inode.info()
// for its debug logs after KeepOwnerUnlock, so info() read owner and
// ownerTerm without the owner lock. That was a data race (run with -race),
// at every log level, because the Debugf arguments are always evaluated.
func TestClusterRouteConcurrentOwnerChange(t *testing.T) {
	const self, other, third NodeId = 1, 2, 3
	tests := []struct {
		description string
		owner       NodeId
		readyOwner  bool
		redirects   int    // how many times the remote node answers "ask another owner"
		redirTerm   uint64 // term of the owner in that answer (0 = newer each time)
		debug       bool   // run with the fuse logger at debug level
		// expected number of local and remote executions
		expectedLocal  int32
		expectedRemote int32
	}{
		{description: "positive: remote owner runs the request",
			owner: other, redirects: 0, expectedLocal: 0, expectedRemote: 1},
		{description: "positive: remote node redirects to a newer owner, then it runs",
			owner: other, redirects: 5, expectedLocal: 0, expectedRemote: 6},
		{description: "positive: local ready owner runs the request locally",
			owner: self, readyOwner: true, expectedLocal: 1, expectedRemote: 0},
		{description: "negative: redirect to an older owner is ignored, then it runs",
			owner: other, redirects: 5, redirTerm: 1, expectedLocal: 0, expectedRemote: 6},
		{description: "boundary: one redirect",
			owner: other, redirects: 1, expectedLocal: 0, expectedRemote: 2},
		{description: "corner: local owner that is not ready yet waits, then runs locally",
			owner: self, readyOwner: false, expectedLocal: 1, expectedRemote: 0},
		{description: "corner: debug logging on, remote node redirects 5 times",
			owner: other, redirects: 5, debug: true, expectedLocal: 0, expectedRemote: 6},
		{description: "corner: debug logging on, local owner that is not ready yet",
			owner: self, readyOwner: false, debug: true, expectedLocal: 1, expectedRemote: 0},
	}

	for _, tc := range tests {
		t.Run(tc.description, func(t *testing.T) {
			if tc.debug {
				level, out := fuseLog.GetLevel(), fuseLog.Out
				fuseLog.SetOutput(io.Discard)
				fuseLog.SetLevel(logrus.DebugLevel)
				defer func() {
					fuseLog.SetLevel(level)
					fuseLog.SetOutput(out)
				}()
			}
			fs := &ClusterFs{Conns: &ConnPool{id: self}}
			inode := &Inode{Id: 2, Name: "file", owner: tc.owner, ownerTerm: 10, readyOwner: tc.readyOwner}

			// Another goroutine applies newer owners. Owners stay remote
			// (other/third) unless the inode is already local; for a
			// local inode only the term grows, and it becomes ready.
			stop := make(chan struct{})
			var wg sync.WaitGroup
			wg.Add(1)
			go func() {
				defer wg.Done()
				term := uint64(100)
				for {
					select {
					case <-stop:
						return
					default:
					}
					inode.ChangeOwnerLock()
					if inode.owner == self {
						inode.ownerTerm++
						inode.readyOwner = true
					} else {
						term++
						inode.applyOwner(&pb.Owner{Term: term, NodeId: uint64(third)})
					}
					inode.ChangeOwnerUnlock()
				}
			}()

			var local, remote int32
			fs.route(
				func() *Inode { return inode },
				false,
				func(inode *Inode) { atomic.AddInt32(&local, 1) },
				func(inode *Inode, inodeOwner NodeId) *pb.Owner {
					n := atomic.AddInt32(&remote, 1)
					if int(n) > tc.redirects {
						return nil
					}
					term := tc.redirTerm
					if term == 0 {
						term = 1000 + uint64(n)
					}
					return &pb.Owner{Term: term, NodeId: uint64(other)}
				},
			)
			close(stop)
			wg.Wait()

			if local != tc.expectedLocal || remote != tc.expectedRemote {
				t.Errorf("local = %v, remote = %v, expected local = %v, remote = %v",
					local, remote, tc.expectedLocal, tc.expectedRemote)
			}
		})
	}
}

// BenchmarkClusterRouteRedirect runs route with one redirect, which is the
// path that logs the inode info.
func BenchmarkClusterRouteRedirect(b *testing.B) {
	const self, other NodeId = 1, 2
	fs := &ClusterFs{Conns: &ConnPool{id: self}}
	inode := &Inode{Id: 2, Name: "file", owner: other}
	b.ReportAllocs()
	for b.Loop() {
		calls := 0
		fs.route(
			func() *Inode { return inode },
			false,
			func(inode *Inode) {},
			func(inode *Inode, inodeOwner NodeId) *pb.Owner {
				calls++
				if calls > 1 {
					return nil
				}
				return &pb.Owner{Term: inode.ownerTerm + 1, NodeId: uint64(other)}
			},
		)
	}
}
