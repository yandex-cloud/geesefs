package core

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"syscall"
	"testing"
	"testing/synctest"
	"time"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// Model backend metadata only; local namespace operations use the real inode API.
type lookupStore struct {
	mu          sync.Mutex
	objects     map[string]BlobItemOutput
	calls       int
	err         error
	headErrors  map[string]error
	listErr     error
	listStarted chan struct{}
	listRelease <-chan struct{}
}

func lookupFS(t *testing.T, ttl time.Duration, options ...func(*cfg.FlagStorage)) (*Goofys, *lookupStore) {
	t.Helper()
	store := &lookupStore{objects: make(map[string]BlobItemOutput)}
	backend := &TestBackend{err: syscall.ENOSYS, capabilities: &Capabilities{Name: "s3"}}
	backend.HeadBlobFunc = func(p *HeadBlobInput) (*HeadBlobOutput, error) {
		store.mu.Lock()
		defer store.mu.Unlock()
		store.calls++
		if store.err != nil {
			return nil, store.err
		}
		if err := store.headErrors[p.Key]; err != nil {
			return nil, err
		}
		if item, ok := store.objects[p.Key]; ok {
			return &HeadBlobOutput{BlobItemOutput: item}, nil
		}
		return nil, syscall.ENOENT
	}
	backend.ListBlobsFunc = func(p *ListBlobsInput) (*ListBlobsOutput, error) {
		store.mu.Lock()
		store.calls++
		err := store.err
		if err == nil {
			err = store.listErr
		}
		resp := &ListBlobsOutput{}
		for name, item := range store.objects {
			if (p.Prefix == nil || strings.HasPrefix(name, *p.Prefix)) &&
				(p.StartAfter == nil || name > *p.StartAfter) {
				resp.Items = append(resp.Items, item)
			}
		}
		sort.Slice(resp.Items, func(i, j int) bool { return *resp.Items[i].Key < *resp.Items[j].Key })
		started, release := store.listStarted, store.listRelease
		store.listStarted, store.listRelease = nil, nil
		store.mu.Unlock()
		if started != nil {
			close(started)
			<-release
		}
		return resp, err
	}
	flags := cfg.DefaultFlags()
	flags.StatCacheTTL = ttl
	flags.NoPreloadDir = true
	flags.Cheap = true
	// These tests exercise the local namespace, not background persistence.
	flags.MaxFlushers = 0
	for _, option := range options {
		option(flags)
	}
	fs, err := newGoofys(context.Background(), "test", flags, func(string, *cfg.FlagStorage) (StorageBackend, error) {
		return backend, nil
	})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(fs.Shutdown)
	return fs, store
}

func (s *lookupStore) blockNextList(t *testing.T) (<-chan struct{}, func()) {
	t.Helper()
	started, release := make(chan struct{}), make(chan struct{})
	s.mu.Lock()
	s.listStarted, s.listRelease = started, release
	s.mu.Unlock()
	unblock := sync.OnceFunc(func() { close(release) })
	t.Cleanup(unblock)
	return started, unblock
}

func (s *lookupStore) count() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.calls
}

func (s *lookupStore) put(name string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.objects[name] = BlobItemOutput{Key: PString(name), Size: 3, LastModified: PTime(time.Now())}
}

func requireMissing(t *testing.T, fs *Goofys, name string) {
	t.Helper()
	if inode, err := fs.LookupPath(name); err != syscall.ENOENT || inode != nil {
		t.Fatalf("LookupPath(%q) = %v, %v; want ENOENT", name, inode, err)
	}
}

func TestNegativeLookupRepeatedMiss(t *testing.T) {
	fs, store := lookupFS(t, time.Minute)
	requireMissing(t, fs, ".~tmp~")
	calls := store.count()
	if calls == 0 {
		t.Fatal("initial lookup did not check the backend")
	}
	for i := 0; i < 20; i++ {
		requireMissing(t, fs, ".~tmp~")
	}
	if got := store.count(); got != calls {
		t.Fatalf("repeated ENOENT performed %d additional backend requests", got-calls)
	}
}

func TestNegativeLookupExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, store := lookupFS(t, time.Minute)
		requireMissing(t, fs, "external")
		store.put("external")
		time.Sleep(59 * time.Second)
		requireMissing(t, fs, "external")
		// A cache hit must not extend the original expiry.
		time.Sleep(time.Second)
		inode, err := fs.LookupPath("external")
		if err != nil || inode == nil || inode.Attributes.Size != 3 {
			t.Fatalf("external object not visible at TTL: %v, %v", inode, err)
		}
	})
}

func TestNegativeLookupDisabled(t *testing.T) {
	fs, store := lookupFS(t, 0)
	requireMissing(t, fs, "external")
	store.put("external")
	if inode, err := fs.LookupPath("external"); err != nil || inode == nil {
		t.Fatalf("TTL=0 hid external creation: %v, %v", inode, err)
	}
}

func TestNegativeLookupDoesNotCacheErrors(t *testing.T) {
	for _, failure := range []error{syscall.EACCES, syscall.EIO} {
		t.Run(failure.Error(), func(t *testing.T) {
			fs, store := lookupFS(t, time.Minute)
			store.err = failure
			if _, err := fs.LookupPath("external"); err != failure {
				t.Fatalf("got %v, want %v", err, failure)
			}
			store.err = nil
			store.put("external")
			if inode, err := fs.LookupPath("external"); err != nil || inode == nil {
				t.Fatalf("backend recovery hidden: %v, %v", inode, err)
			}
		})
	}
}

func TestNegativeLookupLocalCreation(t *testing.T) {
	for _, kind := range []string{"file", "directory", "symlink", "rename"} {
		t.Run(kind, func(t *testing.T) {
			fs, _ := lookupFS(t, time.Minute)
			root, err := fs.LookupPath("")
			if err != nil {
				t.Fatal(err)
			}
			requireMissing(t, fs, "new")
			var created *Inode
			switch kind {
			case "file":
				var handle *FileHandle
				created, handle, err = root.Create("new")
				if handle != nil {
					defer handle.Release()
				}
			case "directory":
				created, err = root.MkDir("new")
			case "symlink":
				created, err = root.CreateSymlink("new", "target")
			case "rename":
				created, err = root.CreateSymlink("old", "target")
				if err == nil {
					err = root.Rename("old", root, "new")
				}
			}
			if err != nil {
				t.Fatal(err)
			}
			if found, err := fs.LookupPath("new"); err != nil || found != created {
				t.Fatalf("local %s hidden by prior miss: %v, %v", kind, found, err)
			}
		})
	}
}

func TestNegativeLookupPreload(t *testing.T) {
	fs, store := lookupFS(t, time.Minute, func(flags *cfg.FlagStorage) { flags.NoPreloadDir = false })
	store.put("present")
	requireMissing(t, fs, ".~tmp~")
	// The first slurp may populate positive children and invalidate the
	// in-flight miss. Once that batch is loaded, repeated misses must be cheap.
	requireMissing(t, fs, ".~tmp~")
	calls := store.count()
	for i := 0; i < 20; i++ {
		requireMissing(t, fs, ".~tmp~")
	}
	if store.count() != calls {
		t.Fatal("preloading still repeats missing-name requests")
	}
}

func TestNegativeLookupPartialErrors(t *testing.T) {
	for _, request := range []string{"directory marker", "prefix listing"} {
		t.Run(request, func(t *testing.T) {
			fs, store := lookupFS(t, time.Minute)
			store.mu.Lock()
			if request == "directory marker" {
				store.headErrors = map[string]error{"external/": syscall.EACCES}
			} else {
				store.listErr = syscall.EACCES
			}
			store.mu.Unlock()
			if _, err := fs.LookupPath("external"); err != syscall.EACCES {
				t.Fatalf("partial failure became %v", err)
			}
			store.mu.Lock()
			store.headErrors, store.listErr = nil, nil
			store.mu.Unlock()
			store.put("external")
			if _, err := fs.LookupPath("external"); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestNegativeLookupRefresh(t *testing.T) {
	fs, store := lookupFS(t, time.Minute)
	store.put("dir/")
	dir, err := fs.LookupPath("dir")
	if err != nil {
		t.Fatal(err)
	}
	requireMissing(t, fs, "dir/external")
	store.put("dir/external")
	if err := fs.RefreshInodeCache(dir); err != nil {
		t.Fatal(err)
	}
	if _, err := fs.LookupPath("dir/external"); err != nil {
		t.Fatalf("refresh retained negative entry: %v", err)
	}
}

func TestNegativeLookupPositiveListing(t *testing.T) {
	fs, store := lookupFS(t, time.Minute)
	requireMissing(t, fs, "external")
	store.put("external")
	root, _ := fs.LookupPath("")
	listed, err := root.LookUp("external", true)
	if err != nil || listed == nil {
		t.Fatalf("positive listing failed: %v", err)
	}
	if found, err := fs.LookupPath("external"); err != nil || found != listed {
		t.Fatalf("positive listing hidden: %v", err)
	}
}

func TestNegativeLookupInflightRefresh(t *testing.T) {
	fs, store := lookupFS(t, time.Minute)
	store.put("dir/")
	dir, err := fs.LookupPath("dir")
	if err != nil {
		t.Fatal(err)
	}
	started, unblock := store.blockNextList(t)
	done := make(chan error, 1)
	go func() { _, err := fs.LookupPath("dir/external"); done <- err }()
	<-started
	if err := fs.RefreshInodeCache(dir); err != nil {
		t.Fatal(err)
	}
	store.put("dir/external")
	unblock()
	if err := <-done; err != syscall.ENOENT {
		t.Fatalf("old snapshot returned %v", err)
	}
	if _, err := fs.LookupPath("dir/external"); err != nil {
		t.Fatalf("late ENOENT undid refresh: %v", err)
	}
}

func TestNegativeLookupInflightCreate(t *testing.T) {
	fs, store := lookupFS(t, time.Minute)
	root, _ := fs.LookupPath("")
	started, unblock := store.blockNextList(t)
	done := make(chan error, 1)
	go func() { _, err := fs.LookupPath("new"); done <- err }()
	<-started
	created, handle, err := root.Create("new")
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Release()
	unblock()
	if err := <-done; err != nil {
		t.Fatalf("late ENOENT hid local creation: %v", err)
	}
	if found, err := fs.LookupPath("new"); err != nil || found != created {
		t.Fatalf("created inode lost: %v", err)
	}
}

func TestNegativeLookupSlowRequest(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, store := lookupFS(t, time.Minute)
		started, unblock := store.blockNextList(t)
		done := make(chan error, 1)
		go func() { _, err := fs.LookupPath("external"); done <- err }()
		<-started
		time.Sleep(time.Minute)
		store.put("external")
		unblock()
		if err := <-done; err != syscall.ENOENT {
			t.Fatalf("old snapshot returned %v", err)
		}
		if _, err := fs.LookupPath("external"); err != nil {
			t.Fatalf("request latency extended TTL: %v", err)
		}
	})
}

func TestNegativeLookupBounded(t *testing.T) {
	fs, store := lookupFS(t, time.Minute)
	requireMissing(t, fs, "first")
	for i := 0; i < 1024; i++ {
		requireMissing(t, fs, fmt.Sprintf("missing-%d", i))
	}
	store.put("first")
	if _, err := fs.LookupPath("first"); err != nil {
		t.Fatalf("old miss retained after cache pressure: %v", err)
	}
}

func TestNegativeLookupCreateUnlink(t *testing.T) {
	fs, store := lookupFS(t, time.Minute)
	root, _ := fs.LookupPath("")
	requireMissing(t, fs, "external")
	_, handle, err := root.Create("external")
	if err != nil {
		t.Fatal(err)
	}
	handle.Release()
	if err := root.Unlink("external"); err != nil {
		t.Fatal(err)
	}
	store.put("external")
	if _, err := fs.LookupPath("external"); err != nil {
		t.Fatalf("old negative entry survived local mutations: %v", err)
	}
}

func TestNegativeLookupConcurrentExpiry(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		fs, store := lookupFS(t, time.Minute)
		started, unblock := store.blockNextList(t)
		done := make(chan error, 1)
		go func() { _, err := fs.LookupPath("external"); done <- err }()
		<-started
		time.Sleep(30 * time.Second)
		requireMissing(t, fs, "external")
		time.Sleep(15 * time.Second)
		unblock()
		if err := <-done; err != syscall.ENOENT {
			t.Fatal(err)
		}
		store.put("external")
		time.Sleep(14 * time.Second)
		requireMissing(t, fs, "external")
		time.Sleep(time.Second)
		if _, err := fs.LookupPath("external"); err != nil {
			t.Fatalf("concurrent miss extended expiry: %v", err)
		}
	})
}
