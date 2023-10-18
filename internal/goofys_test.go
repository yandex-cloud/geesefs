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

// Integration tests for internal GeeseFS interfaces (not dependent on any FUSE binding)

/*

USAGE:

[ DEBUG=1 ] \
[ BUCKET=.. ] \
[ AWS_ACCESS_KEY_ID=.. ] \
[ AWS_SECRET_ACCESS_KEY=.. ] \
[ ENDPOINT=.. ] \
[ EMULATOR=1 ] \
[ EVENTUAL_CONSISTENCY=1 ] \
CLOUD=s3|gcs|azblob|adlv1|adlv2 \
    go test -v github.com/yandex-cloud/geesefs/internal \
    [ -check.f TestName ]

NOTES:

- If BUCKET is empty, an empty bucket will be created to run tests
- EMULATOR is for s3proxy/azurite

*/

package internal

import (
	"github.com/yandex-cloud/geesefs/internal/cfg"

	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"math/rand"
	"os"
	"reflect"
	"sort"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/corehandlers"
	"github.com/aws/aws-sdk-go/aws/credentials"
	aws_s3 "github.com/aws/aws-sdk-go/service/s3"

	"github.com/jacobsa/fuse/fuseops"

	. "gopkg.in/check.v1"
)

func (s *GoofysTest) getRoot(t *C) (inode *Inode) {
	inode = s.fs.inodes[fuseops.RootInodeID]
	t.Assert(inode, NotNil)
	return
}

func (s *GoofysTest) TestGetRootInode(t *C) {
	root := s.getRoot(t)
	t.Assert(root.Id, Equals, fuseops.InodeID(fuseops.RootInodeID))
}

func (s *GoofysTest) TestSetup(t *C) {
}

func (s *GoofysTest) TestLookUpInode(t *C) {
	_, err := s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("fileNotFound")
	t.Assert(err, Equals, syscall.ENOENT)

	_, err = s.fs.LookupPath("dir1/file3")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("dir2/dir3")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("dir2/dir3/file4")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("empty_dir")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestGetInodeAttributes(t *C) {
	inode, err := s.getRoot(t).LookUp("file1", false)
	t.Assert(err, IsNil)

	attr := inode.GetAttributes()
	t.Assert(attr.Size, Equals, uint64(len("file1")))
}

func (s *GoofysTest) readDirFully(t *C, dh *DirHandle) (entries []*Inode) {
	dh.mu.Lock()

	en, err := dh.ReadDir()
	t.Assert(err, IsNil)
	t.Assert(en, NotNil)
	t.Assert(en.Id, Equals, dh.inode.Id)
	dh.Next(".")

	en, err = dh.ReadDir()
	t.Assert(err, IsNil)
	t.Assert(en, NotNil)
	if dh.inode.Parent == nil {
		t.Assert(en.Id, Equals, dh.inode.Id)
	} else {
		t.Assert(en.Id, Equals, dh.inode.Parent.Id)
	}
	dh.Next("..")

	for i := 2; ; i++ {
		en, err = dh.ReadDir()
		t.Assert(err, IsNil)

		if en == nil {
			dh.mu.Unlock()
			return
		}

		entries = append(entries, en)
		dh.Next(en.Name)
	}

	dh.mu.Unlock()
	return
}

func nameMap(entries []*Inode) (names map[string]bool) {
	names = make(map[string]bool)
	for _, en := range entries {
		names[en.Name] = true
	}
	return
}

func namesOf(entries []*Inode) (names []string) {
	for _, en := range entries {
		names = append(names, en.Name)
	}
	return
}

func (s *GoofysTest) assertEntries(t *C, in *Inode, names []string) {
	dh := in.OpenDir()
	defer dh.CloseDir()

	t.Assert(namesOf(s.readDirFully(t, dh)), DeepEquals, names)
}

func (s *GoofysTest) assertHasEntries(t *C, in *Inode, names []string) {
	dh := in.OpenDir()
	defer dh.CloseDir()

	m := nameMap(s.readDirFully(t, dh))
	var found []string
	for _, n := range names {
		if m[n] {
			found = append(found, n)
		}
	}
	t.Assert(found, DeepEquals, names)
}

func (s *GoofysTest) readDirIntoCache(t *C, inode fuseops.InodeID) {
	in := s.fs.getInodeOrDie(inode)
	dh := in.OpenDir()
	dh.mu.Lock()
	for i := 0; ; i++ {
		en, err := dh.ReadDir()
		t.Assert(err, IsNil)
		if en == nil {
			break
		}
		dh.Next(en.Name)
	}
	dh.CloseDir()
	dh.mu.Unlock()
}

func (s *GoofysTest) TestReadDirCacheLookup(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	s.readDirIntoCache(t, fuseops.RootInodeID)
	s.disableS3()

	// should be cached so lookup should not need to talk to s3
	entries := []string{"dir1", "dir2", "dir4", "empty_dir", "empty_dir2", "file1", "file2", "zero"}
	for _, en := range entries {
		_, err := s.fs.LookupPath(en)
		t.Assert(err, IsNil)
	}
}

func (s *GoofysTest) TestReadDirWithExternalChanges(t *C) {
	s.fs.flags.StatCacheTTL = time.Second

	dir1, err := s.fs.LookupPath("dir1")
	t.Assert(err, IsNil)

	defaultEntries := []string{
		"dir1", "dir2", "dir4", "empty_dir",
		"empty_dir2", "file1", "file2", "zero"}
	s.assertHasEntries(t, s.getRoot(t), defaultEntries)
	// dir1 has file3.
	dir1, err = s.fs.LookupPath("dir1")
	t.Assert(err, IsNil)
	s.assertHasEntries(t, dir1, []string{"file3"})

	// Do the following 'external' changes in s3 without involving goofys.
	// - Remove file1, add file3.
	// - Remove dir1/file3. Given that dir1 has just this one file,
	//   we are effectively removing dir1 as well.
	s.removeBlob(s.cloud, t, "file1")
	s.setupBlobs(s.cloud, t, map[string]*string{"file3": nil})
	s.removeBlob(s.cloud, t, "dir1/file3")

	time.Sleep(s.fs.flags.StatCacheTTL)
	// newEntries = `defaultEntries` - dir1 - file1 + file3.
	newEntries := []string{
		"dir2", "dir4", "empty_dir", "empty_dir2",
		"file2", "file3", "zero"}
	if s.cloud.Capabilities().DirBlob {
		// dir1 is not automatically deleted
		newEntries = append([]string{"dir1"}, newEntries...)
	}
	s.assertHasEntries(t, s.getRoot(t), newEntries)

	_, err = s.fs.LookupPath("file1")
	t.Assert(err, Equals, syscall.ENOENT)

	s.setupBlobs(s.cloud, t, map[string]*string{"file1": nil, "dir1/file3": nil})
	s.removeBlob(s.cloud, t, "file3")
}

func (s *GoofysTest) TestReadDir(t *C) {
	// test listing /
	dh := s.getRoot(t).OpenDir()
	defer dh.CloseDir()

	s.assertHasEntries(t, s.getRoot(t), []string{"dir1", "dir2", "dir4", "empty_dir", "empty_dir2", "file1", "file2", "zero"})

	// test listing dir1/
	in, err := s.fs.LookupPath("dir1")
	t.Assert(err, IsNil)
	s.assertHasEntries(t, in, []string{"file3"})

	// test listing dir2/
	in, err = s.fs.LookupPath("dir2")
	t.Assert(err, IsNil)
	s.assertHasEntries(t, in, []string{"dir3"})

	// test listing dir2/dir3/
	in, err = s.fs.LookupPath("dir2/dir3")
	t.Assert(err, IsNil)
	s.assertHasEntries(t, in, []string{"file4"})
}

func (s *GoofysTest) TestReadFiles(t *C) {
	parent := s.getRoot(t)
	dh := parent.OpenDir()
	defer dh.CloseDir()

	entries := s.readDirFully(t, dh)

	for _, en := range entries {
		if !en.isDir() && (en.Name == "file1" || en.Name == "file2" || en.Name == "zero") {
			fh, err := en.OpenFile()
			t.Assert(err, IsNil)

			bufs, nread, err := fh.ReadFile(0, 4096)
			if en.Name == "zero" {
				t.Assert(nread, Equals, 0)
			} else {
				t.Assert(nread, Equals, len(en.Name))
				t.Assert(len(bufs), Equals, 1)
				buf := bufs[0][0:nread]
				t.Assert(string(buf), Equals, en.Name)
			}
		} else {

		}
	}
}

func (s *GoofysTest) TestReadOffset(t *C) {
	root := s.getRoot(t)
	f := "file1"

	in, err := root.LookUp(f, false)
	t.Assert(err, IsNil)

	fh, err := in.OpenFile()
	t.Assert(err, IsNil)

	bufs, nread, err := fh.ReadFile(1, 4096)
	t.Assert(err, IsNil)
	t.Assert(nread, Equals, len(f)-1)

	t.Assert(len(bufs), Equals, 1)
	t.Assert(string(bufs[0][0:nread]), DeepEquals, f[1:])

	r := rand.New(rand.NewSource(time.Now().UnixNano()))

	for i := 0; i < 3; i++ {
		off := r.Int31n(int32(len(f)))
		bufs, nread, err = fh.ReadFile(int64(off), 4096)
		t.Assert(err, IsNil)
		t.Assert(nread, Equals, len(f)-int(off))
		t.Assert(len(bufs), Equals, 1)
		t.Assert(string(bufs[0][0:nread]), DeepEquals, f[off:])
	}
}

func (s *GoofysTest) TestCreateFiles(t *C) {
	fileName := "testCreateFile"

	_, fh, err := s.getRoot(t).Create(fileName)
	t.Assert(err, IsNil)

	err = fh.inode.SyncFile()
	t.Assert(err, IsNil)

	resp, err := s.cloud.GetBlob(&GetBlobInput{Key: fileName})
	t.Assert(err, IsNil)
	t.Assert(resp.HeadBlobOutput.Size, DeepEquals, uint64(0))
	defer resp.Body.Close()

	_, err = s.getRoot(t).LookUp(fileName, false)
	t.Assert(err, IsNil)

	fileName = "testCreateFile2"
	s.testWriteFile(t, fileName, 1, 1)

	inode, err := s.getRoot(t).LookUp(fileName, false)
	t.Assert(err, IsNil)

	fh, err = inode.OpenFile()
	t.Assert(err, IsNil)

	err = fh.inode.SyncFile()
	t.Assert(err, IsNil)

	resp, err = s.cloud.GetBlob(&GetBlobInput{Key: fileName})
	t.Assert(err, IsNil)
	// ADLv1 doesn't return size when we do a GET
	if _, adlv1 := s.cloud.(*ADLv1); !adlv1 {
		t.Assert(resp.HeadBlobOutput.Size, Equals, uint64(1))
	}
	defer resp.Body.Close()
}

func (s *GoofysTest) TestUnlink(t *C) {
	fileName := "file1"

	in, err := s.fs.LookupPath(fileName)
	t.Assert(err, IsNil)
	err = s.getRoot(t).Unlink(fileName)
	t.Assert(err, IsNil)

	// sync deletion
	err = in.SyncFile()
	t.Assert(err, IsNil)

	// make sure that it's gone from s3
	_, err = s.cloud.GetBlob(&GetBlobInput{Key: fileName})
	t.Assert(mapAwsError(err), Equals, syscall.ENOENT)
}

type FileHandleReader struct {
	fs     *Goofys
	fh     *FileHandle
	offset int64
}

func (r *FileHandleReader) Read(p []byte) (nread int, err error) {
	var bufs [][]byte
	bufs, nread, err = r.fh.ReadFile(r.offset, int64(len(p)))
	r.offset += int64(nread)
	off := 0
	for _, buf := range bufs {
		copy(p[off : ], buf)
		off += len(buf)
	}
	return
}

func (r *FileHandleReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		r.offset = offset
	case 1:
		r.offset += offset
	default:
		panic(fmt.Sprintf("unsupported whence: %v", whence))
	}

	return r.offset, nil
}

func (s *GoofysTest) testWriteFile(t *C, fileName string, size int64, write_size int64) {
	s.testWriteFileAt(t, fileName, int64(0), size, write_size, true)
}

func (s *GoofysTest) testCreateAndWrite(t *C, fileName string, size int64, write_size int64, truncate bool) *FileHandle {
	var fh *FileHandle
	root := s.getRoot(t)

	in, err := s.fs.LookupPath(fileName)
	if err != nil {
		if err == syscall.ENOENT {
			_, fh, err = root.Create(fileName)
			t.Assert(err, IsNil)
		} else {
			t.Assert(err, IsNil)
		}
	} else {
		if truncate {
			err = in.SetAttributes(PUInt64(0), nil, nil, nil, nil)
			t.Assert(err, IsNil)
		}
		fh, err = in.OpenFile()
		t.Assert(err, IsNil)
	}

	buf := make([]byte, write_size)
	nwritten := int64(0)

	src := io.LimitReader(&SeqReader{}, size)

	for {
		nread, err := src.Read(buf)
		if err == io.EOF {
			t.Assert(nwritten, Equals, size)
			break
		}
		t.Assert(err, IsNil)

		err = fh.WriteFile(nwritten, buf[:nread], true)
		t.Assert(err, IsNil)
		nwritten += int64(nread)
	}

	return fh
}

// Write <size> bytes at <offset> in <write_size> byte chunks
func (s *GoofysTest) testWriteFileAt(t *C, fileName string, offset int64, size int64, write_size int64, truncate bool) {
	fh := s.testCreateAndWrite(t, fileName, size, write_size, truncate)

	err := fh.inode.SyncFile()
	t.Assert(err, IsNil)

	resp, err := s.cloud.HeadBlob(&HeadBlobInput{Key: fileName})
	t.Assert(err, IsNil)
	if truncate {
		t.Assert(resp.Size, Equals, uint64(size+offset))
	}

	fr := &FileHandleReader{s.fs, fh, offset}
	diff, err := CompareReader(io.LimitReader(fr, size), io.LimitReader(&SeqReader{offset}, size), 0)
	t.Assert(err, IsNil)
	t.Assert(diff, Equals, -1)
	t.Assert(fr.offset, Equals, size+offset)

	err = fh.inode.SyncFile()
	t.Assert(err, IsNil)

	// read again with exact 4KB to catch aligned read case
	fr = &FileHandleReader{s.fs, fh, offset}
	diff, err = CompareReader(io.LimitReader(fr, size), io.LimitReader(&SeqReader{offset}, size), 4096)
	t.Assert(err, IsNil)
	t.Assert(diff, Equals, -1)
	t.Assert(fr.offset, Equals, size+offset)

	fh.Release()
}

func (s *GoofysTest) TestWriteLargeFile(t *C) {
	s.testWriteFile(t, "testLargeFile", 5*1024*1024, 128*1024)
	s.testWriteFile(t, "testLargeFile2", 4*1024*1024, 128*1024)
	s.testWriteFile(t, "testLargeFile3", 4*1024*1024+1, 128*1024)
}

func (s *GoofysTest) TestWriteLargeMem20M(t *C) {
	s.testWriteFile(t, "testLargeFile", 5*1024*1024, 128*1024)
}

func (s *GoofysTest) TestWriteLargeTruncateMem20M(t *C) {
	fileName := "testLargeTruncate"

	root := s.getRoot(t)
	s3 := root.dir.cloud
	cloud := &TestBackend{StorageBackend: s3}
	cloud.MultipartBlobAddFunc = func(param *MultipartBlobAddInput) (*MultipartBlobAddOutput, error) {
		if param.PartNumber > 20 {
			return nil, syscall.ENOSYS
		}
		return s3.MultipartBlobAdd(param)
	}
	cloud.MultipartBlobCopyFunc = func(param *MultipartBlobCopyInput) (*MultipartBlobCopyOutput, error) {
		// MultipartBlobCopyFunc returning error makes sure it doesn't get called
		return nil, syscall.ENOSYS
	}
	root.dir.cloud = cloud

	err := root.Unlink(fileName)
	t.Assert(err == nil || err == syscall.ENOENT, Equals, true)

	in, fh, err := root.Create(fileName)
	t.Assert(err, IsNil)

	// Allocate 50 GB
	err = in.SetAttributes(PUInt64(50*1024*1024*1024), nil, nil, nil, nil)
	t.Assert(err, IsNil)

	// But only write 100 MB
	buf := make([]byte, 1048576)
	for i := 0; i < len(buf); i++ {
		buf[i] = byte(i)
	}
	for i := 0; i < 100; i++ {
		err := fh.WriteFile(int64(i*len(buf)), buf, true)
		t.Assert(err, IsNil)
	}

	// Truncate again
	// Zeroed areas shouldn't get flushed - it would require calling MultipartBlobCopy
	err = in.SetAttributes(PUInt64(100*1024*1024), nil, nil, nil, nil)
	t.Assert(err, IsNil)

	// Modify the beginning of the file - will work after adding "header hack"
	buf[4095] = 1
	err = fh.WriteFile(0, buf[0:4096], true)
	t.Assert(err, IsNil)

	// Close
	fh.Release()

	// And sync
	err = in.SyncFile()
	t.Assert(err, IsNil)

	// Check size
	resp, err := s.cloud.HeadBlob(&HeadBlobInput{Key: fileName})
	t.Assert(err, IsNil)
	t.Assert(resp.Size, Equals, uint64(100*1024*1024))
}

func (s *GoofysTest) TestMultipartOverwrite(t *C) {
	s.testWriteFile(t, "test%d0%b0", 20*1024*1024, 128*1024)
	// Test overwrite
	s.testWriteFileAt(t, "test%d0%b0", 0, 1, 1, false)
	// Test copying object into itself on metadata change
	_, err := s.cloud.CopyBlob(&CopyBlobInput{
		Source:      "test%d0%b0",
		Destination: "test%d0%b0",
		Metadata:    map[string]*string{
			"foo": aws.String("bar"),
		},
	})
	t.Assert(err, IsNil)
	// It must remain readable and ovewritable
	s.testWriteFileAt(t, "test%d0%b0", 6*1024*1024, 1, 1, false)
}

func (s *GoofysTest) TestWriteReallyLargeFile(t *C) {
	if _, ok := s.cloud.(*S3Backend); ok && s.emulator {
		t.Skip("seems to be OOM'ing S3proxy 1.8.0")
	}
	s.testWriteFile(t, "testLargeFile", 64*1024*1024+1, 128*1024)
}

func (s *GoofysTest) TestWriteReplicatorThrottle(t *C) {
	s.fs.flags.MaxFlushers = 1
	s.testWriteFile(t, "testLargeFile", 21*1024*1024, 128*1024)
}

func (s *GoofysTest) TestMultipartWriteAndTruncate(t *C) {
	fh := s.testCreateAndWrite(t, "testMP", 10*1024*1024, 128*1024, true)
	// Now don't close the FD, but wait until both parts are flushed
	for {
		fh.inode.mu.Lock()
		dirty := fh.inode.buffers.AnyDirty()
		fh.inode.mu.Unlock()
		if !dirty {
			break
		}
		s.fs.flusherMu.Lock()
		if s.fs.flushPending == 0 {
			s.fs.flusherCond.Wait()
		}
		s.fs.flusherMu.Unlock()
	}
	// Truncate the file so now it only consists of 1 part
	err := fh.inode.SetAttributes(PUInt64(1*1024*1024), nil, nil, nil, nil)
	t.Assert(err, IsNil)
	// And now try to flush the file. It would fail if GeeseFS wasn't flushing it before truncation
	err = fh.inode.SyncFile()
	t.Assert(err, IsNil)
	fh.Release()
}

func (s *GoofysTest) TestReadWriteMinimumMemory(t *C) {
	// First part is fixed for "header hack", last part is "still written to"
	s.fs.bufferPool.max = 20*1024*1024
	s.fs.flags.ReadAheadLargeKB = s.fs.flags.ReadAheadKB
	s.testWriteFile(t, "testLargeFile", 21*1024*1024, 128*1024)
}

func (s *GoofysTest) TestWriteManyFilesFile(t *C) {
	var files sync.WaitGroup

	for i := 0; i < 21; i++ {
		files.Add(1)
		fileName := "testSmallFile" + strconv.Itoa(i)
		go func() {
			defer files.Done()
			s.testWriteFile(t, fileName, 1, 128*1024)
		}()
	}

	files.Wait()
}

func (s *GoofysTest) testWriteFileNonAlign(t *C) {
	s.testWriteFile(t, "testWriteFileNonAlign", 6*1024*1024, 128*1024+1)
}

func (s *GoofysTest) TestReadRandom(t *C) {
	size := int64(21 * 1024 * 1024)

	s.testWriteFile(t, "testLargeFile", size, 128*1024)
	in, err := s.fs.LookupPath("testLargeFile")
	t.Assert(err, IsNil)

	fh, err := in.OpenFile()
	t.Assert(err, IsNil)
	fr := &FileHandleReader{s.fs, fh, 0}

	src := rand.NewSource(time.Now().UnixNano())
	truth := &SeqReader{}

	for i := 0; i < 10; i++ {
		offset := src.Int63() % (size / 2)

		fr.Seek(offset, 0)
		truth.Seek(offset, 0)

		// read 5MB+1 from that offset
		nread := int64(5*1024*1024 + 1)
		CompareReader(io.LimitReader(fr, nread), io.LimitReader(truth, nread), 0)
	}
}

func (s *GoofysTest) TestMkDir(t *C) {
	dirName := "test_mkdir"
	fileName := "file"

	inode, err := s.fs.LookupPath(dirName)
	if err == nil {
		_, err := s.fs.LookupPath(dirName+"/"+fileName)
		if err == nil {
			err := inode.Unlink(fileName)
			t.Assert(err, IsNil)
		} else {
			t.Assert(err, Equals, syscall.ENOENT)
		}
		err = s.getRoot(t).RmDir(dirName)
		t.Assert(err, IsNil)
	} else {
		t.Assert(err, Equals, syscall.ENOENT)
	}

	inode, err = s.getRoot(t).MkDir(dirName)
	t.Assert(err, IsNil)
	t.Assert(inode.FullName(), Equals, dirName)

	_, err = s.fs.LookupPath(dirName)
	t.Assert(err, IsNil)

	_, fh, err := inode.Create(fileName)
	t.Assert(err, IsNil)

	err = fh.inode.SyncFile()
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath(dirName+"/"+fileName)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestRmDir(t *C) {
	s.setupBlobs(s.cloud, t, map[string]*string{
		"test_rmdir/dir1/file3": nil,
		"test_rmdir/dir2/":      nil,
		"test_rmdir/dir2/dir3/": nil,
		"test_rmdir/empty_dir/": nil,
	})

	root, err := s.fs.LookupPath("test_rmdir")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("test_rmdir/dir1")
	t.Assert(err, IsNil)
	err = root.RmDir("dir1")
	t.Assert(err, Equals, syscall.ENOTEMPTY)

	_, err = s.fs.LookupPath("test_rmdir/dir2")
	t.Assert(err, IsNil)
	err = root.RmDir("dir2")
	t.Assert(err, Equals, syscall.ENOTEMPTY)

	_, err = s.fs.LookupPath("test_rmdir/empty_dir")
	t.Assert(err, IsNil)
	err = root.RmDir("empty_dir")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestRenamePreserveMetadata(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}
	root := s.getRoot(t)

	from, to := "file1", "new_file"

	metadata := make(map[string]*string)
	metadata["foo"] = aws.String("bar")

	_, err := s.cloud.CopyBlob(&CopyBlobInput{
		Source:      from,
		Destination: from,
		Metadata:    metadata,
	})
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, root.Id)

	_, err = s.fs.LookupPath(from)
	t.Assert(err, IsNil)

	toInode, err := s.fs.LookupPath(to)
	if err != nil {
		t.Assert(err, Equals, syscall.ENOENT)
	} else {
		err = root.Unlink(to)
		t.Assert(err, IsNil)
		err = toInode.SyncFile()
		t.Assert(err, IsNil)
	}

	s.fs.flags.MaxFlushers = 0

	err = root.Rename(from, root, to)
	t.Assert(err, IsNil)

	toInode, err = s.fs.LookupPath(to)
	t.Assert(err, IsNil)

	// Check that xattrs are filled correctly from the moved object

	xattrVal, err := toInode.GetXattr("user.foo")
	t.Assert(xattrVal, DeepEquals, []byte("bar"))

	s.fs.flags.MaxFlushers = 16

	err = toInode.SyncFile()
	t.Assert(err, IsNil)

	// Check that xattrs are present in the cloud after move

	resp, err := s.cloud.HeadBlob(&HeadBlobInput{Key: to})
	t.Assert(err, IsNil)

	t.Assert(resp.Metadata["foo"], NotNil)
	t.Assert(*resp.Metadata["foo"], Equals, "bar")
}

func (s *GoofysTest) TestRenameLarge(t *C) {
	fileSize := int64(21 * 1024 * 1024)

	s.testWriteFile(t, "large_file", fileSize, 128*1024)

	root := s.getRoot(t)

	from, to := "large_file", "large_file2"
	err := root.Rename(from, root, to)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestRenameToExisting(t *C) {
	root := s.getRoot(t)

	// cache these 2 files first
	_, err := s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("file2")
	t.Assert(err, IsNil)

	err = root.Rename("file1", root, "file2")
	t.Assert(err, IsNil)

	file1 := root.findChild("file1")
	t.Assert(file1, IsNil)

	file2 := root.findChild("file2")
	t.Assert(file2, NotNil)
	t.Assert(file2.Name, Equals, "file2")
}

// Check that renames of open files with flushed modifications work
// That didn't work in 0.30.5 and older versions
func (s *GoofysTest) TestRenameOpenedUnmodified(t *C) {
	root := s.getRoot(t)

	var fh *FileHandle

	in, err := s.fs.LookupPath("file20")
	t.Assert(err, Equals, syscall.ENOENT)

	in, err = s.fs.LookupPath("file10")
	t.Assert(err == nil || err == syscall.ENOENT, Equals, true)
	if err == syscall.ENOENT {
		_, fh, err = root.Create("file10")
		t.Assert(err, IsNil)
	} else {
		fh, err = in.OpenFile()
		t.Assert(err, IsNil)
	}

	err = fh.inode.SyncFile()
	t.Assert(err, IsNil)

	err = root.Rename("file10", root, "file20")
	t.Assert(err, IsNil)

	fh.Release()

	err = s.fs.SyncFS(nil)
	t.Assert(err, IsNil)

	// Check that the file is actually renamed
	_, err = s.cloud.HeadBlob(&HeadBlobInput{Key: "file10"})
	t.Assert(err, NotNil)
	t.Assert(mapAwsError(err), Equals, syscall.ENOENT)
	_, err = s.cloud.HeadBlob(&HeadBlobInput{Key: "file20"})
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestBackendListPagination(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't have pagination")
	}
	if s.azurite {
		// https://github.com/Azure/Azurite/issues/262
		t.Skip("Azurite doesn't support pagination")
	}

	var itemsPerPage int
	switch s.cloud.Delegate().(type) {
	case *S3Backend, *GCS3:
		itemsPerPage = 1000
	case *AZBlob, *ADLv2:
		itemsPerPage = 5000
	default:
		t.Fatalf("unknown backend: %T", s.cloud)
	}

	root := s.getRoot(t)
	root.dir.mountPrefix = "test_list_pagination/"

	blobs := make(map[string]*string)
	expect := make([]string, 0)
	for i := 0; i < itemsPerPage+1; i++ {
		b := fmt.Sprintf("%08v", i)
		blobs["test_list_pagination/"+b] = nil
		expect = append(expect, b)
	}

	switch s.cloud.(type) {
	case *ADLv1, *ADLv2:
		// these backends don't support parallel delete so I
		// am doing this here
		defer func() {
			var wg sync.WaitGroup

			for b, _ := range blobs {
				SmallActionsGate <- 1
				wg.Add(1)

				go func(key string) {
					// ignore the error here,
					// anything we didn't cleanup
					// will be handled by teardown
					_, _ = s.cloud.DeleteBlob(&DeleteBlobInput{key})
					<- SmallActionsGate
					wg.Done()
				}(b)
			}

			wg.Wait()
		}()
	}

	s.setupBlobs(s.cloud, t, blobs)

	dh := root.OpenDir()
	defer dh.CloseDir()

	children := namesOf(s.readDirFully(t, dh))
	t.Assert(children, DeepEquals, expect)
}

func (s *GoofysTest) TestBackendListPrefix(t *C) {
	s.setupDefaultEnv(t, "test_list_prefix/")

	res, err := s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("random"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 0)

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("test_list_prefix/empty_dir"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Not(Equals), 0)
	t.Assert(*res.Prefixes[0].Prefix, Equals, "test_list_prefix/empty_dir/")
	t.Assert(len(res.Items), Equals, 0)

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("test_list_prefix/empty_dir/"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 1)
	t.Assert(*res.Items[0].Key, Equals, "test_list_prefix/empty_dir/")

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("test_list_prefix/file1"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 1)
	t.Assert(*res.Items[0].Key, Equals, "test_list_prefix/file1")

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("test_list_prefix/file1/"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 0)

	// ListBlobs:
	// - Case1: If the prefix foo/ is not added explicitly, then ListBlobs foo/ might or might not return foo/.
	//   In the test setup dir2 is not expliticly created.
	// - Case2: Else, ListBlobs foo/ must return foo/
	//   In the test setup dir2/dir3 is expliticly created.

	// ListBlobs:Case1
	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("test_list_prefix/dir2/"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 1)
	t.Assert(*res.Prefixes[0].Prefix, Equals, "test_list_prefix/dir2/dir3/")
	if len(res.Items) == 1 {
		// azblob(with hierarchial ns on), adlv1, adlv2.
		t.Assert(*res.Items[0].Key, Equals, "test_list_prefix/dir2/")
	} else {
		// s3, azblob(with hierarchial ns off)
		t.Assert(len(res.Items), Equals, 0)
	}

	// ListBlobs:Case2
	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("test_list_prefix/dir2/dir3/"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 2)
	t.Assert(*res.Items[0].Key, Equals, "test_list_prefix/dir2/dir3/")
	t.Assert(*res.Items[1].Key, Equals, "test_list_prefix/dir2/dir3/file4")

	// ListBlobs:Case1
	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix: PString("test_list_prefix/dir2/"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	if len(res.Items) == 3 {
		// azblob(with hierarchial ns on), adlv1, adlv2.
		t.Assert(*res.Items[0].Key, Equals, "test_list_prefix/dir2/")
		t.Assert(*res.Items[1].Key, Equals, "test_list_prefix/dir2/dir3/")
		t.Assert(*res.Items[2].Key, Equals, "test_list_prefix/dir2/dir3/file4")
	} else {
		// s3, azblob(with hierarchial ns off)
		t.Assert(len(res.Items), Equals, 2)
		t.Assert(*res.Items[0].Key, Equals, "test_list_prefix/dir2/dir3/")
		t.Assert(*res.Items[1].Key, Equals, "test_list_prefix/dir2/dir3/file4")
	}

	res, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix: PString("test_list_prefix/dir2/dir3/file4"),
	})
	t.Assert(err, IsNil)
	t.Assert(len(res.Prefixes), Equals, 0)
	t.Assert(len(res.Items), Equals, 1)
	t.Assert(*res.Items[0].Key, Equals, "test_list_prefix/dir2/dir3/file4")
}

func (s *GoofysTest) TestRenameDir(t *C) {
	s.fs.flags.StatCacheTTL = 0

	root := s.getRoot(t)

	_, err := s.fs.LookupPath("empty_dir")
	t.Assert(err, IsNil)
	_, err = s.fs.LookupPath("dir1")
	t.Assert(err, IsNil)

	err = root.Rename("empty_dir", root, "dir1")
	t.Assert(err, Equals, syscall.ENOTEMPTY)

	err = root.Rename("empty_dir", root, "new_dir")
	t.Assert(err, IsNil)

	dir2, err := s.fs.LookupPath("dir2")
	t.Assert(err, IsNil)
	t.Assert(dir2, NotNil)
	oldId := dir2.Id

	_, err = s.fs.LookupPath("new_dir2")
	t.Assert(err, Equals, syscall.ENOENT)

	err = root.Rename("dir2", root, "new_dir2")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("dir2/dir3")
	t.Assert(err, Equals, syscall.ENOENT)

	_, err = s.fs.LookupPath("dir2/dir3/file4")
	t.Assert(err, Equals, syscall.ENOENT)

	_, err = s.fs.LookupPath("dir2")
	t.Assert(err, Equals, syscall.ENOENT)

	new_dir2, err := s.fs.LookupPath("new_dir2")
	t.Assert(err, IsNil)
	t.Assert(new_dir2, NotNil)
	t.Assert(oldId, Equals, new_dir2.Id)

	old, err := s.fs.LookupPath("new_dir2/dir3/file4")
	t.Assert(err, IsNil)
	t.Assert(old, NotNil)

	_, err = s.fs.LookupPath("new_dir3")
	t.Assert(err, Equals, syscall.ENOENT)

	err = root.Rename("new_dir2", root, "new_dir3")
	t.Assert(err, IsNil)

	new, err := s.fs.LookupPath("new_dir3/dir3/file4")
	t.Assert(err, IsNil)
	t.Assert(new, NotNil)
	t.Assert(old.Id, Equals, new.Id)

	_, err = s.fs.LookupPath("new_dir2/dir3")
	t.Assert(err, Equals, syscall.ENOENT)

	_, err = s.fs.LookupPath("new_dir2/dir3/file4")
	t.Assert(err, Equals, syscall.ENOENT)
}

func (s *GoofysTest) TestRename(t *C) {
	root := s.getRoot(t)

	from, to := "empty_dir", "file1"
	_, err := s.fs.LookupPath(from)
	t.Assert(err, IsNil)
	_, err = s.fs.LookupPath(to)
	t.Assert(err, IsNil)
	err = root.Rename(from, root, to)
	t.Assert(err, Equals, syscall.ENOTDIR)

	from, to = "file1", "empty_dir"
	_, err = s.fs.LookupPath(from)
	t.Assert(err, IsNil)
	_, err = s.fs.LookupPath(to)
	t.Assert(err, IsNil)
	err = root.Rename(from, root, to)
	t.Assert(err, Equals, syscall.EISDIR)

	from, to = "file1", "new_file"
	_, err = s.fs.LookupPath(from)
	t.Assert(err, IsNil)
	_, err = s.fs.LookupPath(to)
	if err != nil {
		t.Assert(err, Equals, syscall.ENOENT)
	}
	err = root.Rename(from, root, to)
	t.Assert(err, IsNil)
	toInode, err := s.fs.LookupPath(to)
	t.Assert(err, IsNil)
	err = toInode.SyncFile()
	t.Assert(err, IsNil)

	_, err = s.cloud.HeadBlob(&HeadBlobInput{Key: to})
	t.Assert(err, IsNil)

	_, err = s.cloud.HeadBlob(&HeadBlobInput{Key: from})
	t.Assert(mapAwsError(err), Equals, syscall.ENOENT)

	from, to = "file3", "new_file2"
	dir, _ := s.fs.LookupPath("dir1")
	_, err = s.fs.LookupPath("dir1/"+from)
	t.Assert(err, IsNil)
	_, err = s.fs.LookupPath(to)
	if err != nil {
		t.Assert(err, Equals, syscall.ENOENT)
	}
	err = dir.Rename(from, root, to)
	t.Assert(err, IsNil)
	toInode, err = s.fs.LookupPath(to)
	t.Assert(err, IsNil)
	err = toInode.SyncFile()
	t.Assert(err, IsNil)

	_, err = s.cloud.HeadBlob(&HeadBlobInput{Key: to})
	t.Assert(err, IsNil)

	_, err = s.cloud.HeadBlob(&HeadBlobInput{Key: from})
	t.Assert(mapAwsError(err), Equals, syscall.ENOENT)

	from, to = "no_such_file", "new_file"
	err = root.Rename(from, root, to)
	t.Assert(err, Equals, syscall.ENOENT)

	if s3, ok := s.cloud.Delegate().(*S3Backend); ok {
		if !hasEnv("GCS") {
			// not really rename but can be used by rename
			from, to = s.fs.bucket+"/file2", "new_file"
			_, err = s3.copyObjectMultipart(int64(len("file2")), from, to, "", nil, nil, nil)
			t.Assert(err, IsNil)
		}
	}
}

func hasEnv(env string) bool {
	v := os.Getenv(env)

	return !(v == "" || v == "0" || v == "false")
}

func isTravis() bool {
	return hasEnv("TRAVIS")
}

func (s *GoofysTest) TestCheap(t *C) {
	s.fs.flags.Cheap = true
	s.TestLookUpInode(t)
	s.TestWriteLargeFile(t)
}

func (s *GoofysTest) TestExplicitDir(t *C) {
	s.fs.flags.ExplicitDir = true
	s.testExplicitDir(t)
}

func (s *GoofysTest) TestExplicitDirAndCheap(t *C) {
	s.fs.flags.ExplicitDir = true
	s.fs.flags.Cheap = true
	s.testExplicitDir(t)
}

func (s *GoofysTest) testExplicitDir(t *C) {
	if s.cloud.Capabilities().DirBlob {
		t.Skip("only for backends without dir blob")
	}

	_, err := s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("fileNotFound")
	t.Assert(err, Equals, syscall.ENOENT)

	_, err = s.fs.LookupPath("dir4/file5")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("empty_dir")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestChmod(t *C) {
	in, err := s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	targetMode := os.FileMode(0777)

	err = in.SetAttributes(nil, &targetMode, nil, nil, nil)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestGetMimeType(t *C) {
	// option to use mime type not turned on
	mime := s.fs.flags.GetMimeType("foo.css")
	t.Assert(mime, IsNil)

	s.fs.flags.UseContentType = true

	mime = s.fs.flags.GetMimeType("foo.css")
	t.Assert(mime, NotNil)
	t.Assert(*mime, Equals, "text/css")

	mime = s.fs.flags.GetMimeType("foo")
	t.Assert(mime, IsNil)

	mime = s.fs.flags.GetMimeType("foo.")
	t.Assert(mime, IsNil)

	mime = s.fs.flags.GetMimeType("foo.unknownExtension")
	t.Assert(mime, IsNil)
}

func (s *GoofysTest) TestPutMimeType(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		// ADLv1 doesn't support content-type
		t.Skip("ADLv1 doesn't support content-type")
	}

	s.fs.flags.UseContentType = true

	root := s.getRoot(t)
	jpg := "test.jpg"
	jpg2 := "test2.jpg"
	file := "test"

	s.testWriteFile(t, jpg, 10, 128)

	resp, err := s.cloud.HeadBlob(&HeadBlobInput{Key: jpg})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentType, Equals, "image/jpeg")

	_, err = s.fs.LookupPath(jpg)
	t.Assert(err, IsNil)
	_, err = s.fs.LookupPath(file)
	if err != nil {
		t.Assert(err, Equals, syscall.ENOENT)
	}
	err = root.Rename(jpg, root, file)
	t.Assert(err, IsNil)
	toInode, err := s.fs.LookupPath(file)
	err = toInode.SyncFile()
	t.Assert(err, IsNil)

	resp, err = s.cloud.HeadBlob(&HeadBlobInput{Key: file})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentType, Equals, "image/jpeg")

	_, err = s.fs.LookupPath(jpg2)
	if err != nil {
		t.Assert(err, Equals, syscall.ENOENT)
	}
	err = root.Rename(file, root, jpg2)
	t.Assert(err, IsNil)
	toInode, err = s.fs.LookupPath(jpg2)
	err = toInode.SyncFile()
	t.Assert(err, IsNil)

	resp, err = s.cloud.HeadBlob(&HeadBlobInput{Key: jpg2})
	t.Assert(err, IsNil)
	t.Assert(*resp.ContentType, Equals, "image/jpeg")
}

func (s *GoofysTest) TestBucketPrefixSlash(t *C) {
	s.fs.Shutdown()

	s.fs, _ = NewGoofys(context.Background(), s.fs.bucket+":dir2", s.fs.flags)
	defer s.fs.Shutdown()
	t.Assert(s.getRoot(t).dir.mountPrefix, Equals, "dir2/")

	s.fs, _ = NewGoofys(context.Background(), s.fs.bucket+":dir2///", s.fs.flags)
	t.Assert(s.getRoot(t).dir.mountPrefix, Equals, "dir2/")
}

func (s *GoofysTest) TestRenameCache(t *C) {
	root := s.getRoot(t)
	s.fs.flags.StatCacheTTL = 60 * 1000 * 1000 * 1000

	_, err := s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("newfile")
	t.Assert(err, Equals, syscall.ENOENT)

	err = root.Rename("file1", root, "newfile")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("file1")
	t.Assert(err, Equals, syscall.ENOENT)

	_, err = s.fs.LookupPath("newfile")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) anonymous(t *C) {
	// On azure this fails because we re-create the bucket with
	// the same name right away. And well anonymous access is not
	// implemented yet in our azure backend anyway
	var s3 *S3Backend
	var ok bool
	if s3, ok = s.cloud.Delegate().(*S3Backend); !ok {
		t.Skip("only for S3")
	}

	// use a different bucket name to prevent 409 Conflict from
	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud := s.newBackend(t, bucket, false)
	s3, ok = cloud.Delegate().(*S3Backend)
	t.Assert(ok, Equals, true)
	s3.config.ACL = "public-read"
	_, err := cloud.MakeBucket(&MakeBucketInput{})
	t.Assert(err, IsNil)
	s.removeBucket = append(s.removeBucket, cloud)

	acl, err := s3.S3.GetBucketAcl(&aws_s3.GetBucketAclInput{Bucket: PString(bucket)})
	t.Assert(err, IsNil)
	if len(acl.Grants) == 0 {
		t.Skip("cloud does not support canned ACL")
	}

	s.fs.Shutdown()
	s.fs, _ = NewGoofys(context.Background(), bucket, s.fs.flags)
	t.Assert(s.fs, NotNil)

	// should have auto-detected by S3 backend
	cloud = s.getRoot(t).dir.cloud
	t.Assert(cloud, NotNil)
	s3, ok = cloud.Delegate().(*S3Backend)
	t.Assert(ok, Equals, true)

	s3.awsConfig.Credentials = credentials.AnonymousCredentials
	s3.newS3()
}

func (s *GoofysTest) disableS3() {
	time.Sleep(1 * time.Second) // wait for any background goroutines to finish
	dir := s.fs.inodes[fuseops.RootInodeID].dir
	dir.cloud = StorageBackendInitError{
		fmt.Errorf("cloud disabled"),
		*dir.cloud.Capabilities(),
	}
}

func (s *GoofysTest) TestWriteAnonymous(t *C) {
	s.anonymous(t)
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	fileName := "test"

	in, fh, err := s.getRoot(t).Create(fileName)
	t.Assert(err, IsNil)

	err = in.SyncFile()
	t.Assert(mapAwsError(err), Equals, syscall.EACCES)

	fh.Release()
}

func (s *GoofysTest) TestIssue156(t *C) {
	_, err := s.fs.LookupPath("\xae\x8a-")
	// S3Proxy and aws s3 return different errors
	// https://github.com/andrewgaul/s3proxy/issues/201
	t.Assert(err, NotNil)
}

func (s *GoofysTest) TestIssue162(t *C) {
	if s.azurite {
		t.Skip("https://github.com/Azure/Azurite/issues/221")
	}

	params := &PutBlobInput{
		Key:  "dir1/l├â┬╢r 006.jpg",
		Body: bytes.NewReader([]byte("foo")),
		Size: PUInt64(3),
	}
	_, err := s.cloud.PutBlob(params)
	t.Assert(err, IsNil)

	dir, err := s.fs.LookupPath("dir1")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("dir1/l├â┬╢r 006.jpg")
	t.Assert(err, IsNil)
	toInode, err := s.fs.LookupPath("dir1/myfile.jpg")
	if err != nil {
		t.Assert(err, Equals, syscall.ENOENT)
	} else {
		err = s.getRoot(t).Unlink("dir1/myfile.jpg")
		t.Assert(err, IsNil)
		err = toInode.SyncFile()
		t.Assert(err, IsNil)
	}
	err = dir.Rename("l├â┬╢r 006.jpg", dir, "myfile.jpg")
	t.Assert(err, IsNil)
	toInode, err = s.fs.LookupPath("dir1/myfile.jpg")
	t.Assert(err, IsNil)
	err = toInode.SyncFile()
	t.Assert(err, IsNil)

	resp, err := s.cloud.HeadBlob(&HeadBlobInput{Key: "dir1/myfile.jpg"})
	t.Assert(resp.Size, Equals, uint64(3))
}

func (s *GoofysTest) TestXAttrGet(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}

	_, checkETag := s.cloud.Delegate().(*S3Backend)
	xattrPrefix := s.cloud.Capabilities().Name + "."

	file1, err := s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	names, err := file1.ListXattr()
	t.Assert(err, IsNil)
	expectedXattrs := []string{
		xattrPrefix + "etag",
		xattrPrefix + "storage-class",
		"user.name",
	}
	if len(names) == 2 {
		// STANDARD storage-class may be present or not
		expectedXattrs = []string{ xattrPrefix + "etag", "user.name" }
	}
	t.Assert(names, DeepEquals, expectedXattrs)

	_, err = file1.GetXattr("user.foobar")
	t.Assert(err, Equals, ENOATTR)

	if checkETag {
		value, err := file1.GetXattr("s3.etag")
		t.Assert(err, IsNil)
		// md5sum of "file1"
		t.Assert(string(value), Equals, "\"826e8142e6baabe8af779f5f490cf5f5\"")
	}

	value, err := file1.GetXattr("user.name")
	t.Assert(err, IsNil)
	t.Assert(string(value), Equals, "file1+/#\x00")

	dir1, err := s.fs.LookupPath("dir1")
	t.Assert(err, IsNil)

	if !s.cloud.Capabilities().DirBlob {
		// implicit dir blobs don't have s3.etag at all
		names, err = dir1.ListXattr()
		t.Assert(err, IsNil)
		t.Assert(len(names), Equals, 0, Commentf("names: %v", names))

		value, err = dir1.GetXattr(xattrPrefix + "etag")
		t.Assert(err, Equals, ENOATTR)
	}

	// list dir1 to populate file3 in cache, then get file3's xattr
	dir1, err = s.fs.LookupPath("dir1")
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, dir1.Id)

	file3 := dir1.findChild("file3")
	t.Assert(file3, NotNil)

	if checkETag {
		value, err = file3.GetXattr("s3.etag")
		t.Assert(err, IsNil)
		// md5sum of "dir1/file3"
		t.Assert(string(value), Equals, "\"5cd67e0e59fb85be91a515afe0f4bb24\"")
	}

	// ensure that we get the dir blob instead of list
	s.fs.flags.Cheap = true

	emptyDir2, err := s.fs.LookupPath("empty_dir2")
	t.Assert(err, IsNil)

	names, err = emptyDir2.ListXattr()
	t.Assert(err, IsNil)
	sort.Strings(names)
	expectedXattrs = []string{
		xattrPrefix + "etag",
		xattrPrefix + "storage-class",
		"user.name",
	}
	if len(names) == 2 {
		// STANDARD storage-class may be present or not
		expectedXattrs = []string{ xattrPrefix + "etag", "user.name" }
	}
	t.Assert(names, DeepEquals, expectedXattrs)

	emptyDir, err := s.fs.LookupPath("empty_dir")
	t.Assert(err, IsNil)

	if checkETag {
		value, err = emptyDir.GetXattr("s3.etag")
		t.Assert(err, IsNil)
		// dir blobs are empty
		t.Assert(string(value), Equals, "\"d41d8cd98f00b204e9800998ecf8427e\"")
	}

	// s3proxy doesn't support storage class yet
	if hasEnv("AWS") {
		cloud := s.getRoot(t).dir.cloud
		s3, ok := cloud.Delegate().(*S3Backend)
		t.Assert(ok, Equals, true)
		s3.config.StorageClass = "STANDARD_IA"

		s.testWriteFile(t, "ia", 1, 128*1024)

		ia, err := s.fs.LookupPath("ia")
		t.Assert(err, IsNil)

		names, err = ia.ListXattr()
		t.Assert(names, DeepEquals, []string{"s3.etag", "s3.storage-class"})

		value, err = ia.GetXattr("s3.storage-class")
		t.Assert(err, IsNil)
		// smaller than 128KB falls back to standard
		t.Assert(string(value), Equals, "STANDARD")

		s.testWriteFile(t, "ia", 128*1024, 128*1024)
		time.Sleep(100 * time.Millisecond)

		names, err = ia.ListXattr()
		t.Assert(names, DeepEquals, []string{"s3.etag", "s3.storage-class"})

		value, err = ia.GetXattr("s3.storage-class")
		t.Assert(err, IsNil)
		t.Assert(string(value), Equals, "STANDARD_IA")
	}
}

func (s *GoofysTest) TestXAttrGetCached(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}

	xattrPrefix := s.cloud.Capabilities().Name + "."

	s.fs.flags.StatCacheTTL = 1 * time.Minute
	s.readDirIntoCache(t, fuseops.RootInodeID)
	s.disableS3()

	in, err := s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	_, err = in.GetXattr(xattrPrefix + "etag")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestXAttrCopied(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}

	root := s.getRoot(t)

	_, err := s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	_, err = s.fs.LookupPath("file0")
	if err != nil {
		t.Assert(err, Equals, syscall.ENOENT)
	}

	err = root.Rename("file1", root, "file0")
	t.Assert(err, IsNil)

	in, err := s.fs.LookupPath("file0")
	t.Assert(err, IsNil)

	_, err = in.GetXattr("user.name")
	t.Assert(err, IsNil)
}

func (s *GoofysTest) TestXAttrRemove(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}

	in, err := s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	_, err = in.GetXattr("user.name")
	t.Assert(err, IsNil)

	err = in.RemoveXattr("user.name")
	t.Assert(err, IsNil)

	_, err = in.GetXattr("user.name")
	t.Assert(err, Equals, ENOATTR)
}

func (s *GoofysTest) TestXAttrSet(t *C) {
	if _, ok := s.cloud.(*ADLv1); ok {
		t.Skip("ADLv1 doesn't support metadata")
	}

	in, err := s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	err = in.SetXattr("user.bar", []byte("hello"), XATTR_REPLACE)
	t.Assert(err, Equals, ENOATTR)

	err = in.SetXattr("user.bar", []byte("hello"), XATTR_CREATE)
	t.Assert(err, IsNil)

	err = in.SetXattr("user.bar", []byte("hello"), XATTR_CREATE)
	t.Assert(err, Equals, syscall.EEXIST)

	in, err = s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	value, err := in.GetXattr("user.bar")
	t.Assert(err, IsNil)
	t.Assert(string(value), Equals, "hello")

	value = []byte("file1+%/#\x00")

	err = in.SetXattr("user.bar", value, XATTR_REPLACE)
	t.Assert(err, IsNil)

	in, err = s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	value2, err := in.GetXattr("user.bar")
	t.Assert(err, IsNil)
	t.Assert(value2, DeepEquals, value)

	// setting with flag = 0 always works
	err = in.SetXattr("user.bar", []byte("world"), 0)
	t.Assert(err, IsNil)

	err = in.SetXattr("user.baz", []byte("world"), 0)
	t.Assert(err, IsNil)

	value, err = in.GetXattr("user.bar")
	t.Assert(err, IsNil)

	value2, err = in.GetXattr("user.baz")
	t.Assert(err, IsNil)

	t.Assert(value2, DeepEquals, value)
	t.Assert(string(value2), DeepEquals, "world")

	err = in.SetXattr("s3.bar", []byte("hello"), XATTR_CREATE)
	t.Assert(err, IsNil)
	// But check that the change is silently ignored
	value, err = in.GetXattr("s3.bar")
	t.Assert(err, Equals, ENOATTR)
}

func (s *GoofysTest) TestInodeInsert(t *C) {
	root := s.getRoot(t)

	in := NewInode(s.fs, root, "2")
	in.Attributes = InodeAttributes{}
	root.insertChild(in)
	t.Assert(root.dir.Children[0].Name, Equals, "2")

	in = NewInode(s.fs, root, "1")
	in.Attributes = InodeAttributes{}
	root.insertChild(in)
	t.Assert(root.dir.Children[0].Name, Equals, "1")
	t.Assert(root.dir.Children[1].Name, Equals, "2")

	in = NewInode(s.fs, root, "4")
	in.Attributes = InodeAttributes{}
	root.insertChild(in)
	t.Assert(root.dir.Children[0].Name, Equals, "1")
	t.Assert(root.dir.Children[1].Name, Equals, "2")
	t.Assert(root.dir.Children[2].Name, Equals, "4")

	inode := root.findChild("1")
	t.Assert(inode, NotNil)
	t.Assert(inode.Name, Equals, "1")

	inode = root.findChild("2")
	t.Assert(inode, NotNil)
	t.Assert(inode.Name, Equals, "2")

	inode = root.findChild("4")
	t.Assert(inode, NotNil)
	t.Assert(inode.Name, Equals, "4")

	inode = root.findChild("0")
	t.Assert(inode, IsNil)

	inode = root.findChild("3")
	t.Assert(inode, IsNil)

	root.removeChild(root.dir.Children[1])
	root.removeChild(root.dir.Children[0])
	root.removeChild(root.dir.Children[0])
	t.Assert(len(root.dir.Children), Equals, 0)
}

func (s *GoofysTest) TestReadDirSlurpHeuristic(t *C) {
	if _, ok := s.cloud.Delegate().(*S3Backend); !ok {
		t.Skip("only for S3")
	}
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	s.setupBlobs(s.cloud, t, map[string]*string{"dir2isafile": nil})

	root := s.getRoot(t).dir
	t.Assert(root.seqOpenDirScore, Equals, uint8(0))
	s.assertHasEntries(t, s.getRoot(t), []string{
		"dir1", "dir2", "dir2isafile", "dir4", "empty_dir",
		"empty_dir2", "file1", "file2", "zero"})

	dir1, err := s.fs.LookupPath("dir1")
	t.Assert(err, IsNil)
	dh1 := dir1.OpenDir()
	defer dh1.CloseDir()
	score := root.seqOpenDirScore

	dir2, err := s.fs.LookupPath("dir2")
	t.Assert(err, IsNil)
	dh2 := dir2.OpenDir()
	defer dh2.CloseDir()
	t.Assert(root.seqOpenDirScore, Equals, score+1)

	dir3, err := s.fs.LookupPath("dir4")
	t.Assert(err, IsNil)
	dh3 := dir3.OpenDir()
	defer dh3.CloseDir()
	t.Assert(root.seqOpenDirScore, Equals, score+2)
}

func (s *GoofysTest) TestReadDirSlurpSubtree(t *C) {
	if _, ok := s.cloud.Delegate().(*S3Backend); !ok {
		t.Skip("only for S3")
	}
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	s.getRoot(t).dir.seqOpenDirScore = 2
	in, err := s.fs.LookupPath("dir2")
	t.Assert(err, IsNil)
	t.Assert(s.getRoot(t).dir.seqOpenDirScore, Equals, uint8(2))

	s.readDirIntoCache(t, in.Id)
	// should have incremented the score
	t.Assert(s.getRoot(t).dir.seqOpenDirScore, Equals, uint8(3))

	// reading dir2 should cause dir2/dir3 to have cached readdir
	s.disableS3()

	in, err = s.fs.LookupPath("dir2/dir3")
	t.Assert(err, IsNil)

	s.assertEntries(t, in, []string{"file4"})
}

func (s *GoofysTest) TestReadDirCached(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	s.getRoot(t).dir.seqOpenDirScore = 2
	dh := s.getRoot(t).OpenDir()
	entries := s.readDirFully(t, dh)
	dh.CloseDir()
	s.disableS3()

	dh = s.getRoot(t).OpenDir()
	cachedEntries := s.readDirFully(t, dh)
	dh.CloseDir()

	t.Assert(len(entries) > 0, Equals, true)
	t.Assert(cachedEntries, DeepEquals, entries)
}

func (s *GoofysTest) TestReadDirLookUp(t *C) {
	s.getRoot(t).dir.seqOpenDirScore = 2

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(2)
		go func() {
			defer wg.Done()
			s.readDirIntoCache(t, fuseops.RootInodeID)
		}()
		go func() {
			defer wg.Done()
			_, err := s.fs.LookupPath("file1")
			t.Assert(err, IsNil)
		}()
	}
	wg.Wait()
}

func (s *GoofysTest) TestDirMtimeCreate(t *C) {
	root := s.getRoot(t)

	attr := root.GetAttributes()
	m1 := attr.Mtime
	time.Sleep(time.Second)

	_, _, err := root.Create("foo")
	t.Assert(err, IsNil)
	attr2 := root.GetAttributes()
	m2 := attr2.Mtime

	t.Assert(m1.Before(m2), Equals, true)
}

func (s *GoofysTest) TestDirMtimeLs(t *C) {
	root := s.getRoot(t)

	attr := root.GetAttributes()
	m1 := attr.Mtime
	time.Sleep(3 * time.Second)

	params := &PutBlobInput{
		Key:  "newfile",
		Body: bytes.NewReader([]byte("foo")),
		Size: PUInt64(3),
	}
	_, err := s.cloud.PutBlob(params)
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, fuseops.RootInodeID)

	attr2 := root.GetAttributes()
	m2 := attr2.Mtime

	t.Assert(m1.Before(m2), Equals, true)
}

func (s *GoofysTest) TestRead403(t *C) {
	// anonymous only works in S3 for now
	cloud := s.getRoot(t).dir.cloud
	s3, ok := cloud.Delegate().(*S3Backend)
	if !ok {
		t.Skip("only for S3")
	}

	s.fs.flags.StatCacheTTL = 1 * time.Minute

	// cache the inode first so we don't get 403 when we lookup
	in, err := s.fs.LookupPath("file1")
	t.Assert(err, IsNil)

	fh, err := in.OpenFile()
	t.Assert(err, IsNil)

	s3.awsConfig.Credentials = credentials.AnonymousCredentials
	s3.newS3()

	_, _, err = fh.ReadFile(0, 5)
	t.Assert(mapAwsError(err), Equals, syscall.EACCES)

	// now that the S3 GET has failed, try again, see
	// https://github.com/kahing/goofys/pull/243
	_, _, err = fh.ReadFile(0, 5)
	t.Assert(mapAwsError(err), Equals, syscall.EACCES)
}

func (s *GoofysTest) TestDirMTimeNoTTL(t *C) {
	if s.cloud.Capabilities().DirBlob {
		t.Skip("Tests for behavior without dir blob")
	}

	time.Sleep(2 * time.Second)
	s.setupBlobs(s.cloud, t, map[string]*string{
		"dir2/dir10/": nil,
	})

	// enable cheap to ensure GET dir/ will come back before LIST dir/
	s.fs.flags.Cheap = true

	dir2, err := s.fs.LookupPath("dir2")
	t.Assert(err, IsNil)

	attr2 := dir2.GetAttributes()
	m2 := attr2.Mtime

	// dir2/dir3/ exists and has mtime
	s.readDirIntoCache(t, dir2.Id)
	dir3, err := s.fs.LookupPath("dir2/dir10")
	t.Assert(err, IsNil)

	attr3 := dir3.GetAttributes()
	// dir2/dir10 is preloaded when looking up dir2 so its mtime is the same
	t.Assert(attr3.Mtime, Equals, m2)
}

func (s *GoofysTest) TestIssue326(t *C) {
	root := s.getRoot(t)
	_, err := root.MkDir("folder@name.something")
	t.Assert(err, IsNil)
	_, err = root.MkDir("folder#1#")
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, root.Id)
	s.assertHasEntries(t, root, []string{"folder#1#", "folder@name.something"})
}

func (s *GoofysTest) TestSlurpFileAndDir(t *C) {
	if _, ok := s.cloud.Delegate().(*S3Backend); !ok {
		t.Skip("only for S3")
	}
	prefix := "TestSlurpFileAndDir/"
	// fileAndDir is both a file and a directory, and we are
	// slurping them together as part of our listing optimization
	blobs := []string{
		prefix + "fileAndDir",
		prefix + "fileAndDir/a",
	}

	for _, b := range blobs {
		params := &PutBlobInput{
			Key:  b,
			Body: bytes.NewReader([]byte("foo")),
			Size: PUInt64(3),
		}
		_, err := s.cloud.PutBlob(params)
		t.Assert(err, IsNil)
	}

	s.fs.flags.StatCacheTTL = 1 * time.Minute

	in, err := s.fs.LookupPath(prefix[0:len(prefix)-1])
	t.Assert(err, IsNil)
	t.Assert(in.dir, NotNil)

	s.getRoot(t).dir.seqOpenDirScore = 2
	s.readDirIntoCache(t, in.Id)

	// should have slurped these
	in = in.findChild("fileAndDir")
	t.Assert(in, NotNil)
	t.Assert(in.dir, NotNil)

	in = in.findChild("a")
	t.Assert(in, NotNil)

	// because of slurping we've decided that this is a directory,
	// lookup must _not_ talk to S3 again because otherwise we may
	// decide it's a file again because of S3 race
	s.disableS3()
	in, err = s.fs.LookupPath(prefix+"fileAndDir")
	t.Assert(err, IsNil)

	s.assertEntries(t, in, []string{"a"})
}

func (s *GoofysTest) TestAzureDirBlob(t *C) {
	if _, ok := s.cloud.(*AZBlob); !ok {
		t.Skip("only for Azure blob")
	}

	fakedir := []string{"dir2", "dir3"}

	for _, d := range fakedir {
		params := &PutBlobInput{
			Key:  "azuredir/" + d,
			Body: bytes.NewReader([]byte("")),
			Metadata: map[string]*string{
				AzureDirBlobMetadataKey: PString("true"),
			},
			Size: PUInt64(0),
		}
		_, err := s.cloud.PutBlob(params)
		t.Assert(err, IsNil)
	}

	defer func() {
		// because our listing changes dir3 to dir3/, test
		// cleanup could not delete the blob so we wneed to
		// clean up
		for _, d := range fakedir {
			_, err := s.cloud.DeleteBlob(&DeleteBlobInput{Key: "azuredir/" + d})
			t.Assert(err, IsNil)
		}
	}()

	s.setupBlobs(s.cloud, t, map[string]*string{
		// "azuredir/dir" would have gone here
		"azuredir/dir3,/":           nil,
		"azuredir/dir3/file1":       nil,
		"azuredir/dir345_is_a_file": nil,
	})

	head, err := s.cloud.HeadBlob(&HeadBlobInput{Key: "azuredir/dir3"})
	t.Assert(err, IsNil)
	t.Assert(head.IsDirBlob, Equals, true)

	head, err = s.cloud.HeadBlob(&HeadBlobInput{Key: "azuredir/dir345_is_a_file"})
	t.Assert(err, IsNil)
	t.Assert(head.IsDirBlob, Equals, false)

	list, err := s.cloud.ListBlobs(&ListBlobsInput{Prefix: PString("azuredir/")})
	t.Assert(err, IsNil)

	// for flat listing, we rename `dir3` to `dir3/` and add it to Items,
	// `dir3` normally sorts before `dir3./`, but after the rename `dir3/` should
	// sort after `dir3./`
	t.Assert(len(list.Items), Equals, 5)
	t.Assert(*list.Items[0].Key, Equals, "azuredir/dir2/")
	t.Assert(*list.Items[1].Key, Equals, "azuredir/dir3,/")
	t.Assert(*list.Items[2].Key, Equals, "azuredir/dir3/")
	t.Assert(*list.Items[3].Key, Equals, "azuredir/dir3/file1")
	t.Assert(*list.Items[4].Key, Equals, "azuredir/dir345_is_a_file")
	t.Assert(sort.IsSorted(sortBlobItemOutput(list.Items)), Equals, true)

	list, err = s.cloud.ListBlobs(&ListBlobsInput{
		Prefix:    PString("azuredir/"),
		Delimiter: PString("/"),
	})
	t.Assert(err, IsNil)

	// for delimited listing, we remove `dir3` from items and add `dir3/` to prefixes,
	// which should already be there
	t.Assert(len(list.Items), Equals, 1)
	t.Assert(*list.Items[0].Key, Equals, "azuredir/dir345_is_a_file")

	t.Assert(len(list.Prefixes), Equals, 3)
	t.Assert(*list.Prefixes[0].Prefix, Equals, "azuredir/dir2/")
	t.Assert(*list.Prefixes[1].Prefix, Equals, "azuredir/dir3,/")
	t.Assert(*list.Prefixes[2].Prefix, Equals, "azuredir/dir3/")

	// finally check that we are reading them in correctly
	in, err := s.fs.LookupPath("azuredir")
	t.Assert(err, IsNil)

	s.assertEntries(t, in, []string{"dir2", "dir3", "dir3,", "dir345_is_a_file"})
}

func (s *GoofysTest) TestReadDirLarge(t *C) {
	root := s.getRoot(t)
	root.dir.mountPrefix = "empty_dir"

	blobs := make(map[string]*string)
	expect := make([]string, 0)
	for i := 0; i < 998; i++ {
		blobs[fmt.Sprintf("empty_dir/%04vd/%v", i, i)] = nil
		expect = append(expect, fmt.Sprintf("%04vd", i))
	}
	blobs["empty_dir/0998f"] = nil
	blobs["empty_dir/0999f"] = nil
	blobs["empty_dir/1000f"] = nil
	expect = append(expect, "0998f")
	expect = append(expect, "0999f")
	expect = append(expect, "1000f")

	for i := 1001; i < 1003; i++ {
		blobs[fmt.Sprintf("empty_dir/%04vd/%v", i, i)] = nil
		expect = append(expect, fmt.Sprintf("%04vd", i))
	}

	s.setupBlobs(s.cloud, t, blobs)

	dh := root.OpenDir()
	defer dh.CloseDir()

	children := namesOf(s.readDirFully(t, dh))
	sort.Strings(children)

	t.Assert(children, DeepEquals, expect)
}

func (s *GoofysTest) newBackend(t *C, bucket string, createBucket bool) (cloud StorageBackend) {
	var err error
	switch s.cloud.Delegate().(type) {
	case *S3Backend:
		config, _ := s.fs.flags.Backend.(*cfg.S3Config)
		s3, err := NewS3(bucket, s.fs.flags, config)
		t.Assert(err, IsNil)

		s3.config.ListV1Ext = hasEnv("YANDEX")

		if s.emulator {
			s3.Handlers.Sign.Clear()
			s3.Handlers.Sign.PushBack(SignV2)
			s3.Handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)
		}

		if hasEnv("EVENTUAL_CONSISTENCY") {
			cloud = NewS3BucketEventualConsistency(s3)
		} else {
			cloud = s3
		}
	case *GCS3:
		config, _ := s.fs.flags.Backend.(*cfg.S3Config)
		cloud, err = NewGCS3(bucket, s.fs.flags, config)
		t.Assert(err, IsNil)
	case *AZBlob:
		config, _ := s.fs.flags.Backend.(*cfg.AZBlobConfig)
		cloud, err = NewAZBlob(bucket, config)
		t.Assert(err, IsNil)
	case *ADLv1:
		config, _ := s.fs.flags.Backend.(*cfg.ADLv1Config)
		cloud, err = NewADLv1(bucket, s.fs.flags, config)
		t.Assert(err, IsNil)
	case *ADLv2:
		config, _ := s.fs.flags.Backend.(*cfg.ADLv2Config)
		cloud, err = NewADLv2(bucket, s.fs.flags, config)
		t.Assert(err, IsNil)
	default:
		t.Fatal("unknown backend")
	}

	if createBucket {
		_, err = cloud.MakeBucket(&MakeBucketInput{})
		t.Assert(err, IsNil)
		s.removeBucket = append(s.removeBucket, cloud)
	}
	return
}

func (s *GoofysTest) TestVFS(t *C) {
	t.Skip("Test for the strange 'child mount' feature, unusable from cmdline")

	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud2 := s.newBackend(t, bucket, true)

	// "mount" this 2nd cloud
	in, err := s.fs.LookupPath("dir4")
	t.Assert(in, NotNil)
	t.Assert(err, IsNil)

	in.dir.cloud = cloud2
	in.dir.mountPrefix = "cloud2Prefix/"

	rootCloud, rootPath := in.cloud()
	t.Assert(rootCloud, NotNil)
	t.Assert(rootCloud == cloud2, Equals, true)
	t.Assert(rootPath, Equals, "cloud2Prefix")

	// the mount would shadow dir4/file5
	_, err = in.LookUp("file5", false)
	t.Assert(err, Equals, syscall.ENOENT)

	_, fh, err := in.Create("testfile")
	t.Assert(err, IsNil)
	err = fh.inode.SyncFile()
	t.Assert(err, IsNil)

	resp, err := cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/testfile"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()

	err = s.getRoot(t).Rename("file1", in, "file2")
	t.Assert(err, Equals, syscall.EINVAL)

	subdir, err := in.MkDir("subdir")
	t.Assert(err, IsNil)
	err = subdir.SyncFile()
	t.Assert(err, IsNil)

	subdirKey := "cloud2Prefix/subdir"
	if !cloud2.Capabilities().DirBlob {
		subdirKey += "/"
	}

	_, err = cloud2.HeadBlob(&HeadBlobInput{Key: subdirKey})
	t.Assert(err, IsNil)

	subdir, err = s.fs.LookupPath("dir4/subdir")
	t.Assert(err, IsNil)
	t.Assert(subdir, NotNil)
	t.Assert(subdir.dir, NotNil)
	t.Assert(subdir.dir.cloud, IsNil)

	subdirCloud, subdirPath := subdir.cloud()
	t.Assert(subdirCloud, NotNil)
	t.Assert(subdirCloud == cloud2, Equals, true)
	t.Assert(subdirPath, Equals, "cloud2Prefix/subdir")

	// create another file inside subdir to make sure that our
	// mount check is correct for dir inside the root
	_, fh, err = subdir.Create("testfile2")
	t.Assert(err, IsNil)
	err = fh.inode.SyncFile()
	t.Assert(err, IsNil)

	resp, err = cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/subdir/testfile2"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()

	err = subdir.Rename("testfile2", in, "testfile2")
	t.Assert(err, IsNil)

	_, err = cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/subdir/testfile2"})
	t.Assert(err, Equals, syscall.ENOENT)

	resp, err = cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/testfile2"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()

	err = in.Rename("testfile2", subdir, "testfile2")
	t.Assert(err, IsNil)

	_, err = cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/testfile2"})
	t.Assert(err, Equals, syscall.ENOENT)

	resp, err = cloud2.GetBlob(&GetBlobInput{Key: "cloud2Prefix/subdir/testfile2"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()
}

func (s *GoofysTest) TestMountsList(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud := s.newBackend(t, bucket, true)

	root := s.getRoot(t)
	rootCloud := root.dir.cloud

	s.fs.MountAll([]*Mount{
		&Mount{"dir4/cloud1", cloud, "", false},
	})

	in, err := s.fs.LookupPath("dir4")
	t.Assert(in, NotNil)
	t.Assert(err, IsNil)
	t.Assert(int(in.Id), Equals, 2)

	s.readDirIntoCache(t, in.Id)
	// ensure that listing is listing mounts and root bucket in one go
	root.dir.cloud = nil

	s.assertEntries(t, in, []string{"cloud1", "file5"})

	c1, err := s.fs.LookupPath("dir4/cloud1")
	t.Assert(err, IsNil)
	t.Assert(c1.Name, Equals, "cloud1")
	t.Assert(c1.dir.cloud == cloud, Equals, true)
	t.Assert(int(c1.Id), Equals, 3)

	// pretend we've passed the normal cache ttl
	s.fs.flags.StatCacheTTL = 0

	// listing root again should not overwrite the mounts
	root.dir.cloud = rootCloud

	s.readDirIntoCache(t, in.Parent.Id)
	s.assertEntries(t, in, []string{"cloud1", "file5"})

	c1, err = s.fs.LookupPath("dir4/cloud1")
	t.Assert(err, IsNil)
	t.Assert(c1.Name, Equals, "cloud1")
	t.Assert(c1.dir.cloud == cloud, Equals, true)
	t.Assert(int(c1.Id), Equals, 3)

	s.fs.Unmount("dir4/cloud1")
}

func (s *GoofysTest) TestMountsNewDir(t *C) {
	s.clearPrefix(t, s.cloud, "dir5")

	_, err := s.fs.LookupPath("dir5")
	t.Assert(err, Equals, syscall.ENOENT)

	s.fs.MountAll([]*Mount{
		&Mount{"dir5/cloud1", s.cloud, "", false},
	})

	in, err := s.fs.LookupPath("dir5")
	t.Assert(err, IsNil)
	t.Assert(in.isDir(), Equals, true)

	c1, err := s.fs.LookupPath("dir5/cloud1")
	t.Assert(err, IsNil)
	t.Assert(c1.isDir(), Equals, true)
	t.Assert(c1.dir.cloud, Equals, s.cloud)
}

func (s *GoofysTest) TestMountsNewMounts(t *C) {
	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud := s.newBackend(t, bucket, true)

	// "mount" this 2nd cloud
	in, err := s.fs.LookupPath("dir4")
	t.Assert(in, NotNil)
	t.Assert(err, IsNil)

	s.fs.MountAll([]*Mount{
		&Mount{"dir4/cloud1", cloud, "", false},
	})

	s.readDirIntoCache(t, in.Id)

	c1, err := s.fs.LookupPath("dir4/cloud1")
	t.Assert(err, IsNil)
	t.Assert(c1.Name, Equals, "cloud1")
	t.Assert(c1.dir.cloud == cloud, Equals, true)

	_, err = s.fs.LookupPath("dir4/cloud2")
	t.Assert(err, Equals, syscall.ENOENT)

	s.fs.MountAll([]*Mount{
		&Mount{"dir4/cloud1", cloud, "", false},
		&Mount{"dir4/cloud2", cloud, "cloudprefix", false},
	})

	c2, err := s.fs.LookupPath("dir4/cloud2")
	t.Assert(err, IsNil)
	t.Assert(c2.Name, Equals, "cloud2")
	t.Assert(c2.dir.cloud == cloud, Equals, true)
	t.Assert(c2.dir.mountPrefix, Equals, "cloudprefix")
}

func (s *GoofysTest) TestMountsError(t *C) {
	if s.emulator {
		t.Skip("Fails under s3proxy, presumably because s3proxy replies 403 to non-existing buckets")
	}
	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	var cloud StorageBackend
	if s3, ok := s.cloud.Delegate().(*S3Backend); ok {
		// S3Backend can't detect bucket doesn't exist because
		// HEAD an object always return 404 NotFound (instead
		// of NoSuchBucket)
		flags := *s3.flags
		config := *s3.config
		var err error
		cloud, err = NewS3(bucket, &flags, &config)
		t.Assert(err, IsNil)
	} else if _, ok := s.cloud.(*ADLv1); ok {
		config, _ := s.fs.flags.Backend.(*cfg.ADLv1Config)
		config.Authorizer = nil

		var err error
		cloud, err = NewADLv1(bucket, s.fs.flags, config)
		t.Assert(err, IsNil)
	} else if _, ok := s.cloud.(*ADLv2); ok {
		// ADLv2 currently doesn't detect bucket doesn't exist
		cloud = s.newBackend(t, bucket, false)
		adlCloud, _ := cloud.(*ADLv2)
		auth := adlCloud.client.BaseClient.Authorizer
		adlCloud.client.BaseClient.Authorizer = nil
		defer func() {
			adlCloud.client.BaseClient.Authorizer = auth
		}()
	} else {
		cloud = s.newBackend(t, bucket, false)
	}

	s.fs.MountAll([]*Mount{
		&Mount{"dir4/newerror", StorageBackendInitError{
			fmt.Errorf("foo"),
			Capabilities{},
		}, "errprefix1", false},
		&Mount{"dir4/initerror", &StorageBackendInitWrapper{
			StorageBackend: cloud,
			initKey:        "foobar",
		}, "errprefix2", false},
	})

	errfile, err := s.fs.LookupPath("dir4/newerror/"+INIT_ERR_BLOB)
	t.Assert(err, IsNil)
	t.Assert(errfile.isDir(), Equals, false)

	_, err = s.fs.LookupPath("dir4/newerror/not_there")
	t.Assert(err, Equals, syscall.ENOENT)

	errfile, err = s.fs.LookupPath("dir4/initerror/"+INIT_ERR_BLOB)
	t.Assert(err, IsNil)
	t.Assert(errfile.isDir(), Equals, false)

	_, err = s.fs.LookupPath("dir4/initerror/not_there")
	t.Assert(err, Equals, syscall.ENOENT)

	in, err := s.fs.LookupPath("dir4/initerror")
	t.Assert(err, IsNil)
	t.Assert(in, NotNil)

	t.Assert(in.dir.cloud.Capabilities().Name, Equals, cloud.Capabilities().Name)
}

func (s *GoofysTest) TestMountsMultiLevel(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Minute

	bucket := "goofys-test-" + RandStringBytesMaskImprSrc(16)
	cloud := s.newBackend(t, bucket, true)

	s.fs.MountAll([]*Mount{
		&Mount{"dir4/sub/dir", cloud, "", false},
	})

	sub, err := s.fs.LookupPath("dir4/sub")
	t.Assert(err, IsNil)
	t.Assert(sub.isDir(), Equals, true)

	s.assertEntries(t, sub, []string{"dir"})
}

func (s *GoofysTest) TestMountsNested(t *C) {
	s.testMountsNested(t, s.cloud, []*Mount{
		&Mount{"dir5/in/a/dir", s.cloud, "test_nested/1/dir/", false},
		&Mount{"dir5/in/", s.cloud, "test_nested/2/", false},
	})
}

// test that mount order doesn't matter for nested mounts
func (s *GoofysTest) TestMountsNestedReversed(t *C) {
	s.testMountsNested(t, s.cloud, []*Mount{
		&Mount{"dir5/in/", s.cloud, "test_nested/2/", false},
		&Mount{"dir5/in/a/dir", s.cloud, "test_nested/1/dir/", false},
	})
}

func (s *GoofysTest) testMountsNested(t *C, cloud StorageBackend,
	mounts []*Mount) {

	s.clearPrefix(t, cloud, "dir5")
	s.clearPrefix(t, cloud, "test_nested")

	_, err := s.fs.LookupPath("dir5")
	t.Assert(err, Equals, syscall.ENOENT)

	s.fs.MountAll(mounts)

	in, err := s.fs.LookupPath("dir5")
	t.Assert(err, IsNil)

	s.readDirIntoCache(t, in.Id)

	// make sure all the intermediate dirs never expire
	time.Sleep(time.Second)
	dir_in, err := s.fs.LookupPath("dir5/in")
	t.Assert(err, IsNil)
	t.Assert(dir_in.Name, Equals, "in")

	s.readDirIntoCache(t, dir_in.Id)

	dir_a, err := s.fs.LookupPath("dir5/in/a")
	t.Assert(err, IsNil)
	t.Assert(dir_a.Name, Equals, "a")

	s.assertEntries(t, dir_a, []string{"dir"})

	dir_dir, err := s.fs.LookupPath("dir5/in/a/dir")
	t.Assert(err, IsNil)
	t.Assert(dir_dir.Name, Equals, "dir")
	t.Assert(dir_dir.dir.cloud == cloud, Equals, true)

	_, err = s.fs.LookupPath("dir5/in/testfile")
	t.Assert(err, Equals, syscall.ENOENT)
	_, fh, err := dir_in.Create("testfile")
	t.Assert(err, IsNil)
	err = fh.inode.SyncFile()
	t.Assert(err, IsNil)

	resp, err := cloud.GetBlob(&GetBlobInput{Key: "test_nested/2/testfile"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()

	//_, err = s.fs.LookupPath("dir5/in/a/dir/testfile")
	//t.Assert(err, IsNil)
	_, fh, err = dir_dir.Create("testfile")
	t.Assert(err, IsNil)
	err = fh.inode.SyncFile()
	t.Assert(err, IsNil)

	resp, err = cloud.GetBlob(&GetBlobInput{Key: "test_nested/1/dir/testfile"})
	t.Assert(err, IsNil)
	defer resp.Body.Close()

	s.assertEntries(t, in, []string{"in"})

	for _, m := range mounts {
		s.fs.Unmount(m.name)
	}
}

func verifyFileData(t *C, mountPoint string, path string, content *string) {
	if !strings.HasSuffix(mountPoint, "/") {
		mountPoint = mountPoint + "/"
	}
	path = mountPoint + path
	data, err := ioutil.ReadFile(path)
	comment := Commentf("failed while verifying %v", path)
	if content != nil {
		t.Assert(err, IsNil, comment)
		t.Assert(strings.TrimSpace(string(data)), Equals, *content, comment)
	} else {
		t.Assert(err, Not(IsNil), comment)
		t.Assert(strings.Contains(err.Error(), "no such file or directory"), Equals, true, comment)
	}
}

// Checks if 2 sorted lists are equal. Returns a helpful error if they differ.
func checkSortedListsAreEqual(l1, l2 []string) error {
	i1, i2 := 0, 0
	onlyl1, onlyl2 := []string{}, []string{}
	for i1 < len(l1) && i2 < len(l2) {
		if l1[i1] == l2[i2] {
			i1++
			i2++
		} else if l1[i1] < l2[i2] {
			onlyl1 = append(onlyl1, fmt.Sprintf("%d:%v", i1, l1[i1]))
			i1++
		} else {
			onlyl2 = append(onlyl2, fmt.Sprintf("%d:%v", i2, l2[i2]))
			i2++
		}

	}
	for ; i1 < len(l1); i1++ {
		onlyl1 = append(onlyl1, fmt.Sprintf("%d:%v", i1, l1[i1]))
	}
	for ; i2 < len(l2); i2++ {
		onlyl2 = append(onlyl2, fmt.Sprintf("%d:%v", i2, l2[i2]))
	}

	if len(onlyl1)+len(onlyl2) == 0 {
		return nil
	}
	toString := func(l []string) string {
		ret := []string{}
		// The list can contain a lot of elements. Show only ten and say
		// "and x more".
		for i := 0; i < len(l) && i < 10; i++ {
			ret = append(ret, l[i])
		}
		if len(ret) < len(l) {
			ret = append(ret, fmt.Sprintf("and %d more", len(l)-len(ret)))
		}
		return strings.Join(ret, ", ")
	}
	return fmt.Errorf("only l1: %+v, only l2: %+v",
		toString(onlyl1), toString(onlyl2))
}

func (s *GoofysTest) TestReadDirDash(t *C) {
	if s.azurite {
		t.Skip("ADLv1 doesn't have pagination")
	}
	root := s.getRoot(t)
	root.dir.mountPrefix = "prefix"

	// SETUP
	// Add the following blobs
	// - prefix/2019/1
	// - prefix/2019-0000 to prefix/2019-4999
	// - prefix/20190000 to prefix/20194999
	// Fetching this result will need 3 pages in azure (pagesize 5k) and 11 pages
	// in amazon (pagesize 1k)
	// This setup will verify that we paginate and return results correctly before and after
	// seeing all contents that have a '-' ('-' < '/'). For more context read the comments in
	// dir.go::listBlobsSafe.
	blobs := make(map[string]*string)
	expect := []string{"2019"}
	blobs["prefix/2019/1"] = nil
	for i := 0; i < 5000; i++ {
		name := fmt.Sprintf("2019-%04d", i)
		expect = append(expect, name)
		blobs["prefix/"+name] = nil
	}
	for i := 0; i < 5000; i++ {
		name := fmt.Sprintf("2019%04d", i)
		expect = append(expect, name)
		blobs["prefix/"+name] = nil
	}
	s.setupBlobs(s.cloud, t, blobs)

	// Read the directory and verify its contents.
	dh := root.OpenDir()
	defer dh.CloseDir()

	children := namesOf(s.readDirFully(t, dh))
	t.Assert(checkSortedListsAreEqual(children, expect), IsNil)
}

func (s *GoofysTest) TestWriteListFlush(t *C) {
	root := s.getRoot(t)
	root.dir.mountPrefix = "list_flush/"

	dir, err := root.MkDir("dir")
	t.Assert(err, IsNil)

	in, fh, err := dir.Create("file1")
	t.Assert(err, IsNil)
	t.Assert(in, NotNil)
	t.Assert(fh, NotNil)

	s.assertEntries(t, dir, []string{"file1"})

	// in should still be valid
	t.Assert(in.Parent, NotNil)
	t.Assert(in.Parent, Equals, dir)
	fh.inode.SyncFile()

	s.assertEntries(t, dir, []string{"file1"})
}

type includes struct{}

func (c includes) Info() *CheckerInfo {
	return &CheckerInfo{Name: "includes", Params: []string{"obtained", "expected"}}
}

func (c includes) Check(params []interface{}, names []string) (res bool, error string) {
	arr := reflect.ValueOf(params[0])
	switch arr.Kind() {
	case reflect.Array, reflect.Slice, reflect.String:
	default:
		panic(fmt.Sprintf("%v is not an array", names[0]))
	}

	for i := 0; i < arr.Len(); i++ {
		v := arr.Index(i).Interface()
		res, error = DeepEquals.Check([]interface{}{v, params[1]}, names)
		if res {
			return
		} else {
			error = ""
		}

		res = false
	}
	return
}

func (s *GoofysTest) TestWriteUnlinkFlush(t *C) {
	root := s.getRoot(t)

	dir, err := root.MkDir("dir")
	t.Assert(err, IsNil)

	in, fh, err := dir.Create("deleted")
	t.Assert(err, IsNil)
	t.Assert(in, NotNil)
	t.Assert(fh, NotNil)

	err = dir.Unlink("deleted")
	t.Assert(err, IsNil)

	err = fh.inode.SyncFile()
	t.Assert(err, IsNil)

	s.disableS3()

	dh := dir.OpenDir()
	defer dh.CloseDir()
	t.Assert(namesOf(s.readDirFully(t, dh)), Not(includes{}), "deleted")
}

func (s *GoofysTest) TestIssue474(t *C) {
	s.fs.flags.StatCacheTTL = 1 * time.Second
	s.fs.flags.Cheap = true

	p := "this_test/"
	root := s.getRoot(t)
	root.dir.mountPrefix = "this_test/"
	root.dir.seqOpenDirScore = 2

	blobs := make(map[string]*string)

	in := []string{
		"1/a/b",
		"2/c/d",
	}

	for _, s := range in {
		blobs[p+s] = nil
	}

	s.setupBlobs(s.cloud, t, blobs)

	dir1, err := s.fs.LookupPath("1")
	t.Assert(err, IsNil)
	// this would list 1/ and slurp in 2/c/d at the same time
	s.assertEntries(t, dir1, []string{"a"})

	// 2/ will expire and require re-listing. ensure that we don't
	// remove any children as stale as we update
	time.Sleep(time.Second)

	dir2, err := s.fs.LookupPath("2")
	t.Assert(err, IsNil)
	s.assertEntries(t, dir2, []string{"c"})
}
