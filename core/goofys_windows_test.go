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

// Tests for a mounted Windows WinFSP-FUSE FS

//go:build windows

package core

import (
	"errors"
	. "gopkg.in/check.v1"
	"os"
	"syscall"
	"time"
)

func (s *GoofysTest) SetUpSuite(t *C) {
	s.tmp = os.Getenv("TMPDIR")
	if s.tmp == "" {
		s.tmp = os.TempDir()
	}
}

func (s *GoofysTest) mountCommon(t *C, mountPoint string, sameProc bool) {
	os.Remove(mountPoint)
	s.fs.flags.MountPoint = mountPoint
	mfs, err := mountFuseFS(s.fs)
	t.Assert(err, IsNil)
	// WinFSP doesn't wait for mounting correctly... Try to wait ourselves
	for i := 0; i < 20; i++ {
		_, err = os.Stat(mountPoint)
		if err != nil {
			time.Sleep(time.Second)
		} else {
			break
		}
	}
	t.Assert(err, IsNil)
	s.mfs = mfs
}

func (s *GoofysTest) umount(t *C, mountPoint string) {
	s.mfs.Unmount()
	s.mfs = nil
}

func FsyncDir(dir string) error {
	fh, err := os.Create(dir + "/.fsyncdir")
	if errors.Is(err, syscall.ENOENT) {
		return nil
	}
	if err == nil {
		fh.Close()
	}
	return err
}

func IsAccessDenied(err error) bool {
	return err == syscall.EACCES || err == syscall.ERROR_ACCESS_DENIED
}
