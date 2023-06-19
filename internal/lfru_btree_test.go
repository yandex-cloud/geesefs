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

package internal

import (
	. "gopkg.in/check.v1"
	"github.com/jacobsa/fuse/fuseops"
)

type LFRUTest struct{}

var _ = Suite(&LFRUTest{})

func (s *LFRUTest) TestIterate(t *C) {
	l := NewLFRU(4, 16, 4, 1)
	l.Hit(29, 6)
	l.Hit(32, 0)
	l.Hit(34, 0)
	i := l.Pick(nil)
	t.Assert(i, NotNil)
	t.Assert(i.Id(), Equals, fuseops.InodeID(32))
	i2 := l.Pick(i)
	t.Assert(i2, NotNil)
	t.Assert(i2.Id(), Equals, fuseops.InodeID(34))
	i3 := l.Pick(i2)
	t.Assert(i3, NotNil)
	t.Assert(i3.Id(), Equals, fuseops.InodeID(29))
	i4 := l.Pick(i3)
	t.Assert(i4, IsNil)
}
