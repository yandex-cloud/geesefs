package internal

import (
	. "gopkg.in/check.v1"
)

type BufferListTest struct{}

var _ = Suite(&BufferListTest{})

type TestBLHelpers struct {
}

func (t *TestBLHelpers) partNum(offset uint64) uint64 {
	return offset / (5 * 1024)
}

func (t *TestBLHelpers) addMemRecency(uint64) uint64 {
	return 0
}

func filledBuf(n int, c byte) []byte {
	b := make([]byte, n)
	for i := 0; i < n; i++ {
		b[i] = c
	}
	return b
}

func (s *BufferListTest) TestAppend(t *C) {
	l := BufferList{
		helpers: &TestBLHelpers{},
	}
	t.Assert(l.Add(0, filledBuf(1024, 1), BUF_DIRTY, true), Equals, int64(1024))
	t.Assert(l.Add(1024, filledBuf(1024, 2), BUF_DIRTY, true), Equals, int64(1024))
	t.Assert(l.Add(1536, filledBuf(1024, 3), BUF_DIRTY, true), Equals, int64(1024))
	data, _, err := l.GetData(0, 2048, false, true)
	t.Assert(err, IsNil)
	t.Assert(len(data), Equals, 1)
	t.Assert(len(data[0]), Equals, 2048)
	t.Assert(data[0][0:1024], DeepEquals, filledBuf(1024, 1))
	t.Assert(data[0][1024:1536], DeepEquals, filledBuf(512, 2))
	t.Assert(data[0][1536:], DeepEquals, filledBuf(512, 3))
}

func (s *BufferListTest) TestGetHoles(t *C) {
	l := BufferList{
		helpers: &TestBLHelpers{},
	}
	t.Assert(l.Add(0, make([]byte, 1024), BUF_DIRTY, false), Equals, int64(1024))
	t.Assert(l.Add(1024, make([]byte, 1024), BUF_DIRTY, false), Equals, int64(1024))
	data, ids, err := l.GetData(0, 2048, false, true)
	t.Assert(err, IsNil)
	t.Assert(len(data), Equals, 2)
	t.Assert(len(data[0]), Equals, 1024)
	t.Assert(len(data[1]), Equals, 1024)
	t.Assert(ids, DeepEquals, map[uint64]bool{
		1: true,
		2: true,
	})
	holes, loading, flcl := l.GetHoles(0, 2048)
	t.Assert(holes, DeepEquals, []Range(nil))
	t.Assert(loading, Equals, false)
	t.Assert(flcl, Equals, false)
}
