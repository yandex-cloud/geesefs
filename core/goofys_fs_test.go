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

// Tests for a mounted UNIX or Windows FS - suitable for both

package core

import (
	"bytes"
	"context"
	"fmt"
	"io/fs"
	"io/ioutil"
	"math/rand"
	"os"
	"path/filepath"
	"runtime"
	"syscall"
	"time"

	. "gopkg.in/check.v1"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

func (s *GoofysTest) mount(t *C, mountPoint string) {
	s.mountCommon(t, mountPoint, false)
}

func (s *GoofysTest) mountInside(t *C, mountPoint string) {
	s.mountCommon(t, mountPoint, true)
}

func (s *GoofysTest) TestIssue69Fuse(t *C) {
	s.fs.flags.StatCacheTTL = 0

	mountPoint := s.tmp + "/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	oldCwd, err := os.Getwd()
	t.Assert(err, IsNil)

	defer func() {
		err := os.Chdir(oldCwd)
		t.Assert(err, IsNil)
	}()

	err = os.Chdir(mountPoint)
	t.Assert(err, IsNil)

	_, err = os.Stat("dir1")
	t.Assert(err, IsNil)

	err = os.Remove("dir1/file3")
	t.Assert(err, IsNil)

	// don't really care about error code, but it should be a PathError
	os.Stat("dir1")
	os.Stat("dir1")
}

func (s *GoofysTest) TestWriteAnonymousFuse(t *C) {
	s.anonymous(t)
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	mountPoint := s.tmp + "/mnt" + s.fs.bucket

	accessKey := os.Getenv("AWS_ACCESS_KEY_ID")
	os.Setenv("AWS_ACCESS_KEY_ID", "")
	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)
	os.Setenv("AWS_ACCESS_KEY_ID", accessKey)

	file, err := os.OpenFile(mountPoint+"/test", os.O_WRONLY|os.O_CREATE, 0600)
	// Writes always succeed because flushes are asynchronous
	t.Assert(err, IsNil)

	// Flushes return an error
	err = file.Sync()
	t.Assert(err, NotNil)
	pathErr, ok := err.(*os.PathError)
	t.Assert(ok, Equals, true)
	t.Assert(IsAccessDenied(pathErr.Err), Equals, true)

	err = file.Close()
	t.Assert(err, IsNil)

	_, err = os.Stat(mountPoint + "/test")
	t.Assert(err, IsNil)

	_, err = ioutil.ReadFile(mountPoint + "/test")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestWriteSyncWriteFuse(t *C) {
	mountPoint := s.tmp + "/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	var f *os.File
	var n int
	var err error

	defer func() {
		if err != nil {
			f.Close()
		}
	}()

	f, err = os.Create(mountPoint + "/TestWriteSyncWrite")
	t.Assert(err, IsNil)

	n, err = f.Write([]byte("hello\n"))
	t.Assert(err, IsNil)
	t.Assert(n, Equals, 6)

	err = f.Sync()
	t.Assert(err, IsNil)

	n, err = f.Write([]byte("world\n"))
	t.Assert(err, IsNil)
	t.Assert(n, Equals, 6)

	err = f.Close()
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestReadDirSlurpContinuation(t *C) {
	if _, ok := s.cloud.Delegate().(*S3Backend); !ok {
		t.Skip("only for S3")
	}

	// Mount
	mountPoint := s.tmp + "/mnt" + s.fs.bucket
	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)
	// First create some directories to trigger slurp when listing
	for i := 1; i <= 4; i++ {
		err := os.MkdirAll(mountPoint+"/slurpc/"+fmt.Sprintf("%v", i), 0700)
		if err == syscall.EEXIST {
			err = nil
		}
		t.Assert(err, IsNil)
		fh, err := os.OpenFile(mountPoint+"/slurpc/"+fmt.Sprintf("%v/%v", i, i), os.O_WRONLY|os.O_CREATE, 0600)
		t.Assert(err, IsNil)
		err = fh.Close()
		t.Assert(err, IsNil)
	}
	// Then create a large number of files (> 1000) in the 4th subdirectory
	for i := 0; i < 2000; i++ {
		fh, err := os.OpenFile(mountPoint+"/slurpc/"+fmt.Sprintf("4/%v", i), os.O_WRONLY|os.O_CREATE, 0600)
		t.Assert(err, IsNil)
		err = fh.Close()
		t.Assert(err, IsNil)
	}
	// Sync the whole filesystem
	err := FsyncDir(mountPoint)
	t.Assert(err, IsNil)
	// Unmount
	s.umount(t, mountPoint)

	// Mount again
	s.mount(t, mountPoint)
	// Check that all files are present to check that slurp works correctly with the continuation
	count := 0
	filepath.Walk(mountPoint+"/slurpc", func(path string, info fs.FileInfo, err error) error {
		t.Assert(err, IsNil)
		if !info.IsDir() {
			count++
		}
		return nil
	})
	t.Assert(count, Equals, 2003)
}

func (s *GoofysTest) writeSeekWriteFuse(t *C, file string, fh *os.File, first string, second string, third string) {
	fi, err := os.Stat(file)
	t.Assert(err, IsNil)

	defer func() {
		// close the file if the test failed so we can unmount
		if fh != nil {
			fh.Close()
		}
	}()

	_, err = fh.WriteString(first)
	t.Assert(err, IsNil)

	off, err := fh.Seek(int64(len(second)), 1)
	t.Assert(err, IsNil)
	t.Assert(off, Equals, int64(len(first)+len(second)))

	_, err = fh.WriteString(third)
	t.Assert(err, IsNil)

	off, err = fh.Seek(int64(len(first)), 0)
	t.Assert(err, IsNil)
	t.Assert(off, Equals, int64(len(first)))

	_, err = fh.WriteString(second)
	t.Assert(err, IsNil)

	err = fh.Close()
	t.Assert(err, IsNil)
	fh = nil

	content, err := ioutil.ReadFile(file)
	t.Assert(err, IsNil)
	t.Assert(string(content), Equals, first+second+third)

	fi2, err := os.Stat(file)
	t.Assert(err, IsNil)
	t.Assert(fi.Mode(), Equals, fi2.Mode())
}

func (s *GoofysTest) TestWriteSeekWriteFuse(t *C) {
	mountPoint := s.tmp + "/mnt" + s.fs.bucket
	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	file := mountPoint + "/newfile"

	fh, err := os.Create(file)
	t.Assert(err, IsNil)

	s.writeSeekWriteFuse(t, file, fh, "hello", " ", "world")

	fh, err = os.OpenFile(file, os.O_WRONLY, 0600)
	t.Assert(err, IsNil)

	s.writeSeekWriteFuse(t, file, fh, "", "never", "minding")
}

func (s *GoofysTest) TestRenameOverwrite(t *C) {
	mountPoint := s.tmp + "/mnt" + s.fs.bucket
	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	file := mountPoint + "/newfile"
	rename := mountPoint + "/file1"

	fh, err := os.Create(file)
	t.Assert(err, IsNil)

	err = fh.Close()
	t.Assert(err, IsNil)

	err = os.Rename(file, rename)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestRmdirWithDiropen(t *C) {
	s.clearPrefix(t, s.cloud, "dir2")

	s.setupBlobs(s.cloud, t, map[string]*string{
		"dir2/":           nil,
		"dir2/dir3/":      nil,
		"dir2/dir3/file4": nil,
	})

	mountPoint := s.tmp + "/mnt" + s.fs.bucket
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	err := os.MkdirAll(mountPoint+"/dir2/dir4", 0700)
	t.Assert(err, IsNil)
	err = os.MkdirAll(mountPoint+"/dir2/dir5", 0700)
	t.Assert(err, IsNil)

	// 1, open dir5
	dir := mountPoint + "/dir2/dir5"
	fh, err := os.Open(dir)
	t.Assert(err, IsNil)
	defer fh.Close()

	fh2, err := os.Open(mountPoint + "/dir2")
	t.Assert(err, IsNil)
	defer fh2.Close()
	names, err := fh2.Readdirnames(0)
	t.Assert(err, IsNil)
	t.Assert(names, DeepEquals, []string{"dir3", "dir4", "dir5"})
	fh2.Close()

	// 2, remove dir5
	if runtime.GOOS == "windows" {
		// Can't remove opened files under Windows
		err = fh.Close()
		t.Assert(err, IsNil)
	}
	err = os.RemoveAll(dir)
	t.Assert(err, IsNil)

	// 3, readdir dir2
	fh1, err := os.Open(mountPoint + "/dir2")
	t.Assert(err, IsNil)
	defer fh1.Close()
	names, err = fh1.Readdirnames(0)
	t.Assert(err, IsNil)
	t.Assert(names, DeepEquals, []string{"dir3", "dir4"})

	fh2, err = os.Open(mountPoint + "/dir2")
	t.Assert(err, IsNil)
	names, err = fh2.Readdirnames(0)
	t.Assert(err, IsNil)
	t.Assert(names, DeepEquals, []string{"dir3", "dir4"})
	err = fh2.Close()
	t.Assert(err, IsNil)

	err = fh1.Close()
	t.Assert(err, IsNil)

	// 4, reset env
	if runtime.GOOS != "windows" {
		err = fh.Close()
		t.Assert(err, IsNil)
	}

	err = os.RemoveAll(mountPoint + "/dir2/dir4")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestRmImplicitDir(t *C) {
	s.setupDefaultEnv(t, "test_rm_implicit_dir/")

	mountPoint := s.tmp + "/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	oldCwd, err := os.Getwd()
	t.Assert(err, IsNil)
	defer os.Chdir(oldCwd)

	if runtime.GOOS != "windows" {
		dir, err := os.Open(mountPoint + "/test_rm_implicit_dir/dir2")
		t.Assert(err, IsNil)
		defer dir.Close()

		err = os.Chdir(mountPoint + "/test_rm_implicit_dir/dir2")
		t.Assert(err, IsNil)
	}

	err = os.RemoveAll(mountPoint + "/test_rm_implicit_dir/dir2")
	t.Assert(err, IsNil)

	if s.emulator {
		// s3proxy seems to return stale listing after delete here...
		time.Sleep(time.Second)
	}

	root, err := os.Open(mountPoint + "/test_rm_implicit_dir")
	t.Assert(err, IsNil)
	defer root.Close()

	files, err := root.Readdirnames(0)
	t.Assert(err, IsNil)
	t.Assert(files, DeepEquals, []string{
		"dir1", "dir4", "empty_dir", "empty_dir2", "file1", "file2", "zero",
	})
}

func (s *GoofysTest) TestMount(t *C) {
	mountPoint := s.tmp + "/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	log.Printf("Mounted at %v", mountPoint)

	time.Sleep(5 * time.Second)
}

func (s *GoofysTest) TestReadExternalChangesFuse(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Second

	mountPoint := s.tmp + "/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	file := "file1"
	filePath := mountPoint + "/file1"

	buf, err := ioutil.ReadFile(filePath)
	t.Assert(err, IsNil)
	t.Assert(string(buf), Equals, file)

	update := "file2"
	_, err = s.cloud.PutBlob(&PutBlobInput{
		Key:  file,
		Body: bytes.NewReader([]byte(update)),
		Size: PUInt64(uint64(len(update))),
	})
	t.Assert(err, IsNil)

	time.Sleep(s.fs.flags.StatCacheTTL + 1*time.Second)

	buf, err = ioutil.ReadFile(filePath)
	t.Assert(err, IsNil)
	t.Assert(string(buf), Equals, update)

	// the next read shouldn't talk to cloud
	// doesn't work because we're mounting in a different process
	//root := s.getRoot(t)
	//root.dir.cloud = &StorageBackendInitError{
	//	syscall.EINVAL, *root.dir.cloud.Capabilities(),
	//}

	buf, err = ioutil.ReadFile(filePath)
	t.Assert(err, IsNil)
	t.Assert(string(buf), Equals, update)
}

func (s *GoofysTest) TestReadMyOwnWriteFuse(t *C) {
	s.testReadMyOwnWriteFuse(t, false)
}

func (s *GoofysTest) TestReadMyOwnWriteExternalChangesFuse(t *C) {
	s.testReadMyOwnWriteFuse(t, true)
}

func (s *GoofysTest) testReadMyOwnWriteFuse(t *C, externalUpdate bool) {
	file := "read_my_own_write"
	s.fs.flags.StatCacheTTL = 1 * time.Second

	update := "file1"
	_, err := s.cloud.PutBlob(&PutBlobInput{
		Key:  file,
		Body: bytes.NewReader([]byte(update)),
		Size: PUInt64(uint64(len(update))),
	})
	t.Assert(err, IsNil)

	mountPoint := s.tmp + "/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	filePath := mountPoint + "/" + file

	buf, err := ioutil.ReadFile(filePath)
	t.Assert(err, IsNil)
	t.Assert(string(buf), Equals, update)

	if externalUpdate {
		update := "file2"
		_, err = s.cloud.PutBlob(&PutBlobInput{
			Key:  file,
			Body: bytes.NewReader([]byte(update)),
			Size: PUInt64(uint64(len(update))),
		})
		t.Assert(err, IsNil)

		time.Sleep(s.fs.flags.StatCacheTTL)
	}

	fh, err := os.Create(filePath)
	t.Assert(err, IsNil)

	_, err = fh.WriteString("file3")
	t.Assert(err, IsNil)
	// we can't flush yet because if we did, we would be reading
	// the new copy from cloud and that's not the point of this
	// test
	defer func() {
		// want fh to be late-binding because we re-use the variable
		fh.Close()
	}()

	buf, err = ioutil.ReadFile(filePath)
	t.Assert(err, IsNil)
	t.Assert(string(buf), Equals, "file3")

	err = fh.Close()
	t.Assert(err, IsNil)

	time.Sleep(s.fs.flags.StatCacheTTL)

	root := s.getRoot(t)
	cloud := &TestBackend{StorageBackend: root.dir.cloud}
	root.dir.cloud = cloud

	fh, err = os.Open(filePath)
	t.Assert(err, IsNil)

	if !externalUpdate {
		// we flushed and ttl expired, next lookup should
		// realize nothing is changed and NOT invalidate the
		// cache. Except ADLv1 because PUT there doesn't
		// return the mtime, so the open above will think the
		// file is updated and not re-use cache
		if _, adlv1 := s.cloud.(*ADLv1); !adlv1 {
			cloud.err = syscall.EINVAL
		}
	} else {
		// if there was externalUpdate, we wrote our own
		// update with KeepPageCache=false, so we should read
		// from the cloud her
	}

	buf, err = ioutil.ReadAll(fh)
	t.Assert(err, IsNil)
	t.Assert(string(buf), Equals, "file3")
}

func (s *GoofysTest) TestReadMyOwnNewFileFuse(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Second

	mountPoint := s.tmp + "/mnt" + s.fs.bucket

	s.mount(t, mountPoint)
	defer s.umount(t, mountPoint)

	filePath := mountPoint + "/filex"

	// jacobsa/fuse doesn't support setting OpenKeepCache on
	// CreateFile but even after manually setting in in
	// fuse/conversions.go, we still receive read ops instead of
	// being handled by kernel

	fh, err := os.Create(filePath)
	t.Assert(err, IsNil)

	_, err = fh.WriteString("filex")
	t.Assert(err, IsNil)
	// we can't flush yet because if we did, we would be reading
	// the new copy from cloud and that's not the point of this
	// test
	defer fh.Close()

	// disabled: we can't actually read back our own update
	//buf, err := ioutil.ReadFile(filePath)
	//t.Assert(err, IsNil)
	//t.Assert(string(buf), Equals, "filex")
}

func (s *GoofysTest) TestSlurpLookupNoCloud(t *C) {
	var err error

	flags := cfg.DefaultFlags()

	// Use mocked backend
	backend := &TestBackend{
		err: syscall.ENOSYS,
		ListBlobsFunc: func(param *ListBlobsInput) (*ListBlobsOutput, error) {
			p, d, a := NilStr(param.Prefix), NilStr(param.Delimiter), NilStr(param.StartAfter)
			fmt.Printf("ListBlobs: Prefix=%v, Delimiter=%v, StartAfter=%v\n", p, d, a)
			if p == "" && d == "" && a == "testdir" {
				return &ListBlobsOutput{
					IsTruncated: true,
					// No testdir/ or testdir in result - suppose testdir/ comes in next pages
					Items: []BlobItemOutput{
						{Key: PString("testdir-1")},
						{Key: PString("testdir-2")},
					},
				}, nil
			} else if p == "testdir/" && d == "/" && a == "" {
				return &ListBlobsOutput{
					IsTruncated: false,
					Items: []BlobItemOutput{
						{Key: PString("testdir/")},
						{Key: PString("testdir/abc")},
					},
				}, nil
			}
			return nil, syscall.ENOSYS
		},
	}
	s.cloud = backend
	s.fs, err = newGoofys(context.Background(), "test", flags, func(string, *cfg.FlagStorage) (StorageBackend, error) {
		return backend, nil
	})
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("testdir")
	t.Assert(err, IsNil)
}

// Case # 1 - check that a directory longer than eviction limit is listed correctly
// Case # 2 - check that it is listed correctly in 2 parallel threads
func (s *GoofysTest) TestListParallelExpireNoCloud(t *C) {
	var err error

	flags := cfg.DefaultFlags()

	// Set low eviction limits
	flags.StatCacheTTL = 1 * time.Second
	flags.EntryLimit = 100

	// Use mocked backend
	testCount := 9800
	backend := &TestBackend{
		err: syscall.ENOSYS,
		ListBlobsFunc: func(param *ListBlobsInput) (*ListBlobsOutput, error) {
			p, d, a := NilStr(param.Prefix), NilStr(param.Delimiter), NilStr(param.StartAfter)
			fmt.Printf("ListBlobs: Prefix=%v, Delimiter=%v, StartAfter=%v\n", p, d, a)
			if p == "" && d == "" && a == "testdir" {
				return &ListBlobsOutput{
					IsTruncated: true,
					Items: []BlobItemOutput{
						{Key: PString("testdir/")},
					},
				}, nil
			} else if p == "" && d == "" && a == "testdir/" {
				var o []BlobItemOutput
				for i := 0; i < 100; i++ {
					o = append(o, BlobItemOutput{Key: PString("testdir/f" + fmt.Sprintf("%04d", i))})
				}
				return &ListBlobsOutput{
					IsTruncated: true,
					Items:       o,
				}, nil
			} else if p == "testdir/" && d == "/" {
				pos := 0
				if a != "" {
					n, err := fmt.Sscanf(a, "testdir/f%d\n", &pos)
					pos++
					t.Assert(err, IsNil)
					t.Assert(n, Equals, 1)
					t.Assert((pos%100) == 0 && pos <= testCount-100, Equals, true)
				}
				var o []BlobItemOutput
				for i := 0; i < 100; i++ {
					o = append(o, BlobItemOutput{Key: PString("testdir/f" + fmt.Sprintf("%04d", i+pos))})
				}
				if (rand.Int() % 30) == 0 {
					// Add some pauses to trigger eviction
					time.Sleep(1 * time.Second)
				}
				return &ListBlobsOutput{
					IsTruncated: pos < testCount-100,
					Items:       o,
				}, nil
			}
			return nil, syscall.ENOSYS
		},
	}
	s.cloud = backend
	s.fs, err = newGoofys(context.Background(), "test", flags, func(string, *cfg.FlagStorage) (StorageBackend, error) {
		return backend, nil
	})
	t.Assert(err, IsNil)

	var names []string
	for i := 0; i < testCount; i++ {
		names = append(names, "f"+fmt.Sprintf("%04d", i))
	}
	checkDir := func(ch chan int) {
		defer func() {
			// So that it gets called on panic
			ch <- 1
		}()
		in, err := s.fs.LookupPath("testdir")
		t.Assert(err, IsNil)
		dh := in.OpenDir()
		t.Assert(namesOf(s.readDirFully(t, dh)), DeepEquals, names)
		dh.CloseDir()
	}
	ch := make(chan int)
	go checkDir(ch)
	go checkDir(ch)
	<-ch
	<-ch
}

// Case # 3 - start listing, trigger slurp for other directories, verify that eviction didn't affect correctness of listings
func (s *GoofysTest) TestListSlurpExpireNoCloud(t *C) {
	var err error

	flags := cfg.DefaultFlags()

	// Set low eviction limits
	flags.StatCacheTTL = 1 * time.Second
	flags.EntryLimit = 100

	// Use mocked backend
	backend := &TestBackend{
		err: syscall.ENOSYS,
		ListBlobsFunc: func(param *ListBlobsInput) (*ListBlobsOutput, error) {
			p, d, a := NilStr(param.Prefix), NilStr(param.Delimiter), NilStr(param.StartAfter)
			time.Sleep(1 * time.Second)
			fmt.Printf("ListBlobs: Prefix=%v, Delimiter=%v, StartAfter=%v\n", p, d, a)
			if p == "" && d == "" && (a == "testdir" || a == "testdir/g" || a == "testdir/h" || a == "testdir/i") {
				return &ListBlobsOutput{
					IsTruncated: true,
					Items: []BlobItemOutput{
						{Key: PString(a + "/")},
					},
				}, nil
			} else if p == "" && d == "" && (a == "testdir/" || a == "testdir/g/" || a == "testdir/h/" || a == "testdir/i/") {
				var o []BlobItemOutput
				if a <= "testdir/" {
					for i := 0; i < 100; i++ {
						o = append(o, BlobItemOutput{Key: PString("testdir/f" + fmt.Sprintf("%04d", i))})
					}
					o = append(o, BlobItemOutput{Key: PString("testdir/g/")})
				}
				if a <= "testdir/g/" {
					for i := 0; i < 100; i++ {
						o = append(o, BlobItemOutput{Key: PString("testdir/g/gf" + fmt.Sprintf("%04d", i))})
					}
					o = append(o, BlobItemOutput{Key: PString("testdir/h/")})
				}
				if a <= "testdir/h/" {
					for i := 0; i < 100; i++ {
						o = append(o, BlobItemOutput{Key: PString("testdir/h/hf" + fmt.Sprintf("%04d", i))})
					}
					o = append(o, BlobItemOutput{Key: PString("testdir/i/")})
				}
				for i := 0; i < 100; i++ {
					o = append(o, BlobItemOutput{Key: PString("testdir/i/if" + fmt.Sprintf("%04d", i))})
				}
				return &ListBlobsOutput{
					IsTruncated: a < "testdir/i/",
					Items:       o,
				}, nil
			} else if p == "testdir/" && d == "/" && a == "" {
				var o []BlobItemOutput
				for i := 0; i < 100; i++ {
					o = append(o, BlobItemOutput{Key: PString("testdir/f" + fmt.Sprintf("%04d", i))})
				}
				return &ListBlobsOutput{
					IsTruncated: true,
					Items:       o,
					Prefixes: []BlobPrefixOutput{
						{Prefix: PString("testdir/g/")},
						{Prefix: PString("testdir/h/")},
						{Prefix: PString("testdir/i/")},
					},
				}, nil
			} else if p == "testdir/" && d == "/" && a == "testdir/i/" {
				var o []BlobItemOutput
				for i := 0; i < 100; i++ {
					o = append(o, BlobItemOutput{Key: PString("testdir/j" + fmt.Sprintf("%04d", i))})
				}
				return &ListBlobsOutput{
					IsTruncated: false,
					Items:       o,
				}, nil
			}
			return nil, syscall.ENOSYS
		},
	}
	s.cloud = backend
	s.fs, err = newGoofys(context.Background(), "test", flags, func(string, *cfg.FlagStorage) (StorageBackend, error) {
		return backend, nil
	})
	t.Assert(err, IsNil)

	var names []string
	for i := 0; i < 100; i++ {
		names = append(names, "f"+fmt.Sprintf("%04d", i))
	}
	names = append(names, "g", "h", "i")
	for i := 0; i < 100; i++ {
		names = append(names, "j"+fmt.Sprintf("%04d", i))
	}
	in, err := s.fs.LookupPath("testdir")
	t.Assert(err, IsNil)
	dh := in.OpenDir()
	t.Assert(namesOf(s.readDirFully(t, dh)), DeepEquals, names)
	dh.CloseDir()

	// Sleep a bit to trigger eviction
	time.Sleep(1 * time.Second)

	names = nil
	for i := 0; i < 100; i++ {
		names = append(names, "gf"+fmt.Sprintf("%04d", i))
	}
	in, err = s.fs.LookupPath("testdir/g")
	t.Assert(err, IsNil)
	dh = in.OpenDir()
	t.Assert(namesOf(s.readDirFully(t, dh)), DeepEquals, names)
	dh.CloseDir()

	names = nil
	for i := 0; i < 100; i++ {
		names = append(names, "hf"+fmt.Sprintf("%04d", i))
	}
	in, err = s.fs.LookupPath("testdir/h")
	t.Assert(err, IsNil)
	dh = in.OpenDir()
	t.Assert(namesOf(s.readDirFully(t, dh)), DeepEquals, names)
	dh.CloseDir()

	names = nil
	for i := 0; i < 100; i++ {
		names = append(names, "if"+fmt.Sprintf("%04d", i))
	}
	in, err = s.fs.LookupPath("testdir/i")
	t.Assert(err, IsNil)
	dh = in.OpenDir()
	t.Assert(namesOf(s.readDirFully(t, dh)), DeepEquals, names)
	dh.CloseDir()
}
