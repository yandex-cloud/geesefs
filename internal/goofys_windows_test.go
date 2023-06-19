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

// +build windows

package internal

import (
	"os"
	. "gopkg.in/check.v1"
)

func (s *GoofysTest) SetUpSuite(t *C) {
	s.tmp = os.Getenv("TMPDIR")
	if s.tmp == "" {
		s.tmp = os.TempDir()
	}
}

func (s *GoofysTest) mountCommon(t *C, mountPoint string, sameProc bool) {
	os.Remove(mountPoint)
	mfs, err := mountFuseFS(s.fs)
	t.Assert(err, IsNil)
	s.mfs = mfs
}

func (s *GoofysTest) umount(t *C, mountPoint string) {
	s.mfs.Unmount()
	s.mfs = nil
}
