// Copyright 2015 - 2019 Ka-Hing Cheung
// Copyright 2015 - 2017 Google Inc. All Rights Reserved.
// Copyright 2019 Databricks
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

package common

import (
	"mime"
	"net"
	"net/http"
	"os"
	"strings"
	"time"
)

type PartSizeConfig struct {
	PartSize  uint64
	PartCount uint64
}

type FlagStorage struct {
	// File system
	MountOptions      map[string]string
	MountPoint        string
	MountPointArg     string
	MountPointCreated string

	DirMode  os.FileMode
	FileMode os.FileMode
	Uid      uint32
	Gid      uint32

	// Common Backend Config
	UseContentType bool

	Provider          string
	Capacity          uint64
	DiskUsageInterval uint64
	Endpoint          string
	Backend           interface{}

	// Tuning
	MemoryLimit           uint64
	GCInterval            uint64
	Cheap                 bool
	ExplicitDir           bool
	NoDirObject           bool
	MaxFlushers           int64
	MaxParallelParts      int
	MaxParallelCopy       int
	StatCacheTTL          time.Duration
	HTTPTimeout           time.Duration
	RetryInterval         time.Duration
	ReadAheadKB           uint64
	SmallReadCount        uint64
	SmallReadCutoffKB     uint64
	ReadAheadSmallKB      uint64
	LargeReadCutoffKB     uint64
	ReadAheadLargeKB      uint64
	ReadAheadParallelKB   uint64
	ReadMergeKB           uint64
	SinglePartMB          uint64
	MaxMergeCopyMB        uint64
	IgnoreFsync           bool
	SymlinkAttr           string
	CachePopularThreshold int64
	CacheMaxHits          int64
	CacheAgeInterval      int64
	CacheAgeDecrement     int64
	CacheToDiskHits       int64
	CachePath             string
	MaxDiskCacheFD        int64
	CacheFileMode         os.FileMode
	PartSizes             []PartSizeConfig

	// Debugging
	DebugMain  bool
	DebugFuse  bool
	DebugS3    bool
	Foreground bool
	LogFile    string
}

func (flags *FlagStorage) GetMimeType(fileName string) (retMime *string) {
	if flags.UseContentType {
		dotPosition := strings.LastIndex(fileName, ".")
		if dotPosition == -1 {
			return nil
		}
		mimeType := mime.TypeByExtension(fileName[dotPosition:])
		if mimeType == "" {
			return nil
		}
		semicolonPosition := strings.LastIndex(mimeType, ";")
		if semicolonPosition == -1 {
			return &mimeType
		}
		s := mimeType[:semicolonPosition]
		retMime = &s
	}

	return
}

func (flags *FlagStorage) Cleanup() {
	if flags.MountPointCreated != "" && flags.MountPointCreated != flags.MountPointArg {
		err := os.Remove(flags.MountPointCreated)
		if err != nil {
			log.Errorf("rmdir %v = %v", flags.MountPointCreated, err)
		}
	}
}

var defaultHTTPTransport = http.Transport{
	Proxy: http.ProxyFromEnvironment,
	DialContext: (&net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
		DualStack: true,
	}).DialContext,
	MaxIdleConns:          1000,
	MaxIdleConnsPerHost:   1000,
	IdleConnTimeout:       90 * time.Second,
	TLSHandshakeTimeout:   10 * time.Second,
	ExpectContinueTimeout: 10 * time.Second,
}

func GetHTTPTransport() *http.Transport {
	return &defaultHTTPTransport
}
