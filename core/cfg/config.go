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

package cfg

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

type NodeConfig struct {
	Id      uint64
	Address string
}

type FlagStorage struct {
	// File system
	MountOptions      []string
	MountPoint        string
	MountPointArg     string
	MountPointCreated string

	DirMode  os.FileMode
	FileMode os.FileMode
	Uid      uint32
	Gid      uint32
	Setuid   int
	Setgid   int

	// Common Backend Config
	UseContentType bool
	Endpoint       string
	Backend        interface{}

	// Staged write mode
	StagedWriteModeEnabled      bool
	StagedWritePath             string
	StagedWriteDebounce         time.Duration
	StagedWriteFlushSize        uint64
	StagedWriteFlushInterval    time.Duration
	StagedWriteFlushConcurrency int
	StagedWriteUploadCallback   func(fullPath string, fileSize int64)

	// External Caching
	ExternalCacheClient  ContentCache
	HashAttr             string
	HashTimeout          time.Duration
	MinFileSizeForHashKB uint64

	// Tuning
	MemoryLimit         uint64
	UseEnomem           bool
	EntryLimit          int
	GCInterval          uint64
	Cheap               bool
	ExplicitDir         bool
	NoDirObject         bool
	MaxFlushers         int64
	MaxParallelParts    int
	MaxParallelCopy     int
	StatCacheTTL        time.Duration
	HTTPTimeout         time.Duration
	ReadRetryInterval   time.Duration
	ReadRetryMultiplier float64
	ReadRetryMax        time.Duration
	ReadRetryAttempts   int
	RetryInterval       time.Duration
	FuseReadAheadKB     uint64
	ReadAheadKB         uint64
	SmallReadCount      uint64
	SmallReadCutoffKB   uint64
	ReadAheadSmallKB    uint64
	LargeReadCutoffKB   uint64
	ReadAheadLargeKB    uint64
	ReadAheadParallelKB uint64
	ReadMergeKB         uint64
	SinglePartMB        uint64
	MaxMergeCopyMB      uint64
	IgnoreFsync         bool
	FsyncOnClose        bool
	EnablePerms         bool
	EnableSpecials      bool
	EnableMtime         bool
	DisableXattr        bool
	UidAttr             string
	GidAttr             string
	FileModeAttr        string
	RdevAttr            string
	MtimeAttr           string
	SymlinkAttr         string
	SymlinkZeroed       bool
	RefreshAttr         string
	RefreshFilename     string
	FlushFilename       string
	CachePath           string
	MaxDiskCacheFD      int64
	CacheFileMode       os.FileMode
	PartSizes           []PartSizeConfig
	UsePatch            bool
	DropPatchConflicts  bool
	PreferPatchUploads  bool
	NoPreloadDir        bool
	NoVerifySSL         bool
	WinRefreshDirs      bool

	// Debugging
	DebugMain  bool
	DebugFuse  bool
	DebugS3    bool
	PProf      string
	Foreground bool
	LogFile    string
	DebugGrpc  bool

	StatsInterval time.Duration

	// Cluster Mode
	ClusterMode           bool
	ClusterGrpcReflection bool
	ClusterMe             *NodeConfig
	ClusterPeers          []*NodeConfig
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
