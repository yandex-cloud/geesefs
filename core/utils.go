// Copyright 2015 - 2017 Ka-Hing Cheung
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
	"fmt"
	"strings"
	"time"
	"unicode"
)

var TIME_MAX = time.Unix(1<<63-62135596801, 999999999)

func MaxInt(a, b int) int {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt(a, b int) int {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxInt64(a, b int64) int64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinInt64(a, b int64) int64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxUInt32(a, b uint32) uint32 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUInt32(a, b uint32) uint32 {
	if a < b {
		return a
	} else {
		return b
	}
}

func MaxUInt64(a, b uint64) uint64 {
	if a > b {
		return a
	} else {
		return b
	}
}

func MinUInt64(a, b uint64) uint64 {
	if a < b {
		return a
	} else {
		return b
	}
}

func PBool(v bool) *bool {
	return &v
}

func PInt32(v int32) *int32 {
	return &v
}

func PUInt32(v uint32) *uint32 {
	return &v
}

func PInt64(v int64) *int64 {
	return &v
}

func PUInt64(v uint64) *uint64 {
	return &v
}

func PString(v string) *string {
	return &v
}

func PTime(v time.Time) *time.Time {
	return &v
}

func NilUInt32(v *uint32) uint32 {
	if v == nil {
		return 0
	} else {
		return *v
	}
}

func NilInt64(v *int64) int64 {
	if v == nil {
		return 0
	} else {
		return *v
	}
}

func NilStr(v *string) string {
	if v == nil {
		return ""
	} else {
		return *v
	}
}

func PMetadata(m map[string]string) map[string]*string {
	metadata := make(map[string]*string)
	for k, _ := range m {
		k = strings.ToLower(k)
		v := m[k]
		metadata[k] = &v
	}
	return metadata
}

func xattrEscape(value string) (s string) {
	for _, c := range value {
		if c == '%' {
			s += "%25"
		} else if unicode.IsPrint(c) {
			s += string(c)
		} else {
			s += "%" + fmt.Sprintf("%02X", c)
		}
	}

	return
}

func Dup(value []byte) []byte {
	ret := make([]byte, len(value))
	copy(ret, value)
	return ret
}

type empty struct{}

// TODO(dotslash/khc): Remove this semaphore in favor of
// https://godoc.org/golang.org/x/sync/semaphore
type semaphore chan empty

func (sem semaphore) P(n int) {
	for i := 0; i < n; i++ {
		sem <- empty{}
	}
}

func (sem semaphore) V(n int) {
	for i := 0; i < n; i++ {
		<-sem
	}
}
