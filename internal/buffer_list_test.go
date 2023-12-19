package internal

import (
	. "gopkg.in/check.v1"
)

type BufferListTest struct{}

var _ = Suite(&BufferListTest{})

type TestBLHelpers struct {
}

func (t *TestBLHelpers) PartNum(offset uint64) uint64 {
	return offset / (5 * 1024)
}

func (t *TestBLHelpers) QueueCleanBuffer(buf *FileBuffer) {
}

func (t *TestBLHelpers) UnqueueCleanBuffer(buf *FileBuffer) {
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
	data, _, err := l.GetData(0, 2048, true)
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
	data, ids, err := l.GetData(0, 2048, true)
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

// Test targets the requeueSplit() function
func (s *BufferListTest) TestSplitDirtyQueue(t *C) {
	l := BufferList{
		helpers: &TestBLHelpers{},
	}
	zeroed, allocated := l.ZeroRange(0, 100*1024)
	t.Assert(zeroed, Equals, true)
	t.Assert(allocated, Equals, int64(0))
	// 6*1024 and 12*1024 isn't part boundary, refcnt of part 2 and 3 = 2, 1 and others = 1
	t.Assert(l.Add(0, make([]byte, 6*1024), BUF_DIRTY, false), Equals, int64(6*1024))
	t.Assert(l.Add(6*1024, make([]byte, 6*1024), BUF_DIRTY, false), Equals, int64(6*1024))
	data, ids, err := l.GetData(12*1024, (100-12)*1024, true)
	t.Assert(err, IsNil)
	t.Assert(len(data), Equals, 1)
	t.Assert(len(data[0]), Equals, (100-12)*1024)
	t.Assert(ids, DeepEquals, map[uint64]bool{
		4: true,
	})
	l.SetState(12*1024, (100-12)*1024, ids, BUF_CLEAN)
	data, ids, err = l.GetData(0, 12*1024, true)
	t.Assert(err, IsNil)
	t.Assert(len(data), Equals, 2)
	t.Assert(len(data[0]), Equals, 6*1024)
	t.Assert(len(data[1]), Equals, 6*1024)
	t.Assert(ids, DeepEquals, map[uint64]bool{
		3: true,
		5: true,
	})
	l.SetState(0, 12*1024, ids, BUF_CLEAN)
	// Now check dirty list - it should be empty
	// With incorrect refcounting it would either be non-empty or the code would panic()
	numDirty := 0
	l.IterateDirtyParts(func(partNum uint64) bool { numDirty++; return true; })
	t.Assert(numDirty, Equals, 0)
}
