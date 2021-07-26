// Copyright 2015 - 2019 Ka-Hing Cheung
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

package goofys

import (
	. "github.com/yandex-cloud/geesefs/api/common"
	"github.com/yandex-cloud/geesefs/internal"

	"context"
	"fmt"

	"github.com/jacobsa/fuse"
	"github.com/jacobsa/fuse/fuseutil"
	"github.com/sirupsen/logrus"
)

var log = GetLogger("main")

func Mount(
	ctx context.Context,
	bucketName string,
	flags *FlagStorage) (fs *Goofys, mfs *fuse.MountedFileSystem, err error) {

	if flags.DebugS3 {
		SetCloudLogLevel(logrus.DebugLevel)
	}
	// Mount the file system.
	mountCfg := &fuse.MountConfig{
		FSName:                  bucketName,
		Options:                 flags.MountOptions,
		ErrorLogger:             GetStdLogger(NewLogger("fuse"), logrus.ErrorLevel),
		DisableWritebackCaching: true,
	}

	if flags.DebugFuse {
		fuseLog := GetLogger("fuse")
		fuseLog.Level = logrus.DebugLevel
		mountCfg.DebugLogger = GetStdLogger(fuseLog, logrus.DebugLevel)
	}

	if flags.DebugFuse || flags.DebugMain {
		log.Level = logrus.DebugLevel
	}

	if flags.Backend == nil {
		if spec, err := internal.ParseBucketSpec(bucketName); err == nil {
			switch spec.Scheme {
			case "adl":
				auth, err := AzureAuthorizerConfig{
					Log: GetLogger("adlv1"),
				}.Authorizer()
				if err != nil {
					err = fmt.Errorf("couldn't load azure credentials: %v",
						err)
					return nil, nil, err
				}
				flags.Backend = &ADLv1Config{
					Endpoint:   spec.Bucket,
					Authorizer: auth,
				}
				// adlv1 doesn't really have bucket
				// names, but we will rebuild the
				// prefix
				bucketName = ""
				if spec.Prefix != "" {
					bucketName = ":" + spec.Prefix
				}
			case "wasb":
				config, err := AzureBlobConfig(flags.Endpoint, spec.Bucket, "blob")
				if err != nil {
					return nil, nil, err
				}
				flags.Backend = &config
				if config.Container != "" {
					bucketName = config.Container
				} else {
					bucketName = spec.Bucket
				}
				if config.Prefix != "" {
					spec.Prefix = config.Prefix
				}
				if spec.Prefix != "" {
					bucketName += ":" + spec.Prefix
				}
			case "abfs":
				config, err := AzureBlobConfig(flags.Endpoint, spec.Bucket, "dfs")
				if err != nil {
					return nil, nil, err
				}
				flags.Backend = &config
				if config.Container != "" {
					bucketName = config.Container
				} else {
					bucketName = spec.Bucket
				}
				if config.Prefix != "" {
					spec.Prefix = config.Prefix
				}
				if spec.Prefix != "" {
					bucketName += ":" + spec.Prefix
				}

				flags.Backend = &ADLv2Config{
					Endpoint:   config.Endpoint,
					Authorizer: &config,
				}
				bucketName = spec.Bucket
				if spec.Prefix != "" {
					bucketName += ":" + spec.Prefix
				}
			}
		}
	}

	fs = NewGoofys(ctx, bucketName, flags)
	if fs == nil {
		err = fmt.Errorf("Mount: initialization failed")
		return
	}
	server := fuseutil.NewFileSystemServer(FusePanicLogger{fs})

	mfs, err = fuse.Mount(flags.MountPoint, server, mountCfg)
	if err != nil {
		err = fmt.Errorf("Mount: %v", err)
		return
	}

	if len(flags.Cache) != 0 {
		// FIXME: catfs removed. Implement local cache
	}

	return
}

// expose Goofys related functions and types for extending and mounting elsewhere
var (
	MassageMountFlags = internal.MassageMountFlags
	NewGoofys         = internal.NewGoofys
	TryUnmount        = internal.TryUnmount
	MyUserAndGroup    = internal.MyUserAndGroup
)

type (
	Goofys = internal.Goofys
)
