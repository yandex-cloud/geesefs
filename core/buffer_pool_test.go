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

package core

import (
	"bytes"
	. "gopkg.in/check.v1"
	"io"
	"time"
)

type BufferTest struct {
}

var _ = Suite(&BufferTest{})

type SeqReader struct {
	cur int64
}

func (r *SeqReader) Read(p []byte) (n int, err error) {
	n = len(p)
	for i := range p {
		r.cur++
		p[i] = byte(r.cur)
	}

	return
}

func (r *SeqReader) Seek(offset int64, whence int) (int64, error) {
	switch whence {
	case 0:
		r.cur = offset
	case 1:
		r.cur += offset
	default:
		panic("unsupported whence")
	}

	return r.cur, nil

}

type SlowReader struct {
	r     io.Reader
	sleep time.Duration
}

func (r *SlowReader) Read(p []byte) (n int, err error) {
	time.Sleep(r.sleep)
	return r.r.Read(p[:MinInt(len(p), 1336)])
}

func (r *SlowReader) Close() error {
	if reader, ok := r.r.(io.ReadCloser); ok {
		return reader.Close()
	}
	return nil
}

func CompareReader(r1, r2 io.Reader, bufSize int) (int, error) {
	if bufSize == 0 {
		bufSize = 1337
	}
	buf1 := make([]byte, bufSize)
	buf2 := make([]byte, bufSize)

	for {
		nread, err := r1.Read(buf1[:])
		if err != nil && err != io.EOF {
			return -1, err
		}

		if nread == 0 {
			break
		}

		nread2, err2 := io.ReadFull(r2, buf2[:nread])
		if err2 != nil && err != err2 {
			return -1, err2
		}

		if bytes.Compare(buf1[:], buf2[:]) != 0 {
			// fallback to slow path to find the exact point of divergent
			for i, b := range buf1 {
				if buf2[i] != b {
					return i, nil
				}
			}

			if nread2 > nread {
				return nread, nil
			}
		}
	}

	// should have consumed all of r2
	nread2, err := r2.Read(buf2[:])
	if nread2 == 0 || err == io.ErrUnexpectedEOF {
		return -1, nil
	} else {
		if err == io.EOF {
			err = nil
		}
		return nread2, err
	}
}

func (s *BufferTest) TestMultiReader(t *C) {
	r := NewMultiReader()
	buf := make([]byte, 355)
	for i := 0; i < len(buf); i++ {
		buf[i] = 0xA5
	}
	r.AddBuffer(buf)
	r.AddZero(1299)
	buf = make([]byte, 567)
	for i := 0; i < len(buf); i++ {
		buf[i] = 0xB0
	}
	r.AddBuffer(buf)
	buf = make([]byte, 4096)

	b, err := r.Read(buf)
	t.Assert(b, Equals, 355+1299+567)
	t.Assert(err, IsNil)
	t.Assert(buf[0], Equals, byte(0xA5))
	t.Assert(buf[355], Equals, byte(0))
	t.Assert(buf[355+1299], Equals, byte(0xB0))

	b, err = r.Read(buf)
	t.Assert(b, Equals, 0)
	t.Assert(err, Equals, io.EOF)

	pos, err := r.Seek(-700, 2)
	t.Assert(pos, Equals, int64(355+1299+567-700))
	t.Assert(err, IsNil)

	b, err = r.Read(buf)
	t.Assert(b, Equals, 700)
	t.Assert(err, IsNil)
	t.Assert(buf[0], Equals, byte(0))
	t.Assert(buf[700-567], Equals, byte(0xB0))

	b, err = r.Read(buf)
	t.Assert(b, Equals, 0)
	t.Assert(err, Equals, io.EOF)
}

func (s *BufferTest) TestCGroupMemory(t *C) {
	//test getMemoryCgroupPath()
	test_input := `11:hugetlb:/
                    10:memory:/user.slice
                    9:cpuset:/
                    8:blkio:/user.slice
                    7:perf_event:/
                    6:net_prio,net_cls:/
                    5:cpuacct,cpu:/user.slice
                    4:devices:/user.slice
                    3:freezer:/
                    2:pids:/
                    1:name=systemd:/user.slice/user-1000.slice/session-1759.scope`
	mem_path, _ := getMemoryCgroupPath(test_input)
	t.Assert(mem_path, Equals, "/user.slice")
}
