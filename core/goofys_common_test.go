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

// Common test initialization routines

package core

import (
	"github.com/yandex-cloud/geesefs/core/cfg"

	"bufio"
	"bytes"
	"fmt"
	"io"
	"net"
	"os"
	"os/user"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	"context"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/aws/aws-sdk-go/aws/corehandlers"

	"github.com/Azure/azure-storage-blob-go/azblob"
	"github.com/Azure/go-autorest/autorest"
	"github.com/Azure/go-autorest/autorest/azure"
	azureauth "github.com/Azure/go-autorest/autorest/azure/auth"

	"github.com/sirupsen/logrus"

	. "gopkg.in/check.v1"
	"runtime/debug"
)

// so I don't get complains about unused imports
var ignored = logrus.DebugLevel

const PerTestTimeout = 10 * time.Minute

func currentUid() uint32 {
	user, err := user.Current()
	if err != nil {
		panic(err)
	}

	uid, err := strconv.ParseUint(user.Uid, 10, 32)
	if err != nil {
		panic(err)
	}

	return uint32(uid)
}

func currentGid() uint32 {
	user, err := user.Current()
	if err != nil {
		panic(err)
	}

	gid, err := strconv.ParseUint(user.Gid, 10, 32)
	if err != nil {
		panic(err)
	}

	return uint32(gid)
}

type GoofysTest struct {
	fs        *Goofys
	mfs       MountedFS
	ctx       context.Context
	awsConfig *aws.Config
	cloud     StorageBackend
	emulator  bool
	azurite   bool
	tmp       string

	removeBucket []StorageBackend

	env map[string]*string

	timeout chan int
}

var _ = Suite(&GoofysTest{})

func logOutput(t *C, tag string, r io.ReadCloser) {
	in := bufio.NewScanner(r)

	for in.Scan() {
		t.Log(tag, in.Text())
	}
}

func waitFor(t *C, addr string) (err error) {
	// wait for it to listen on port
	for i := 0; i < 10; i++ {
		var conn net.Conn
		conn, err = net.Dial("tcp", addr)
		if err == nil {
			// we are done!
			conn.Close()
			return
		} else {
			t.Logf("Cound not connect: %v", err)
			time.Sleep(200 * time.Millisecond)
		}
	}

	return
}

func (t *GoofysTest) deleteBlobsParallelly(cloud StorageBackend, blobs []string) error {
	const concurrency = 10
	sem := make(semaphore, concurrency)
	sem.P(concurrency)
	var err error
	for _, blobOuter := range blobs {
		sem.V(1)
		go func(blob string) {
			defer sem.P(1)
			_, localerr := cloud.DeleteBlob(&DeleteBlobInput{blob})
			if localerr != nil && localerr != syscall.ENOENT {
				err = localerr
			}
		}(blobOuter)
		if err != nil {
			break
		}
	}
	sem.V(concurrency)
	return err
}

// groupByDecresingDepths takes a slice of path strings and returns the paths as
// groups where each group has the same `depth` - depth(a/b/c)=2, depth(a/b/)=1
// The groups are returned in decreasing order of depths.
//   - Inp: [] Out: []
//   - Inp: ["a/b1/", "a/b/c1", "a/b2", "a/b/c2"]
//     Out: [["a/b/c1", "a/b/c2"], ["a/b1/", "a/b2"]]
//   - Inp: ["a/b1/", "z/a/b/c1", "a/b2", "z/a/b/c2"]
//     Out: [["z/a/b/c1", "z/a/b/c2"], ["a/b1/", "a/b2"]
func groupByDecresingDepths(items []string) [][]string {
	depthToGroup := map[int][]string{}
	for _, item := range items {
		depth := len(strings.Split(strings.TrimRight(item, "/"), "/"))
		if _, ok := depthToGroup[depth]; !ok {
			depthToGroup[depth] = []string{}
		}
		depthToGroup[depth] = append(depthToGroup[depth], item)
	}
	decreasingDepths := []int{}
	for depth := range depthToGroup {
		decreasingDepths = append(decreasingDepths, depth)
	}
	sort.Sort(sort.Reverse(sort.IntSlice(decreasingDepths)))
	ret := [][]string{}
	for _, depth := range decreasingDepths {
		group, _ := depthToGroup[depth]
		ret = append(ret, group)
	}
	return ret
}

func (t *GoofysTest) DeleteADLBlobs(cloud StorageBackend, items []string) error {
	// If we delete a directory that's not empty, ADL{v1|v2} returns failure. That can
	// happen if we want to delete both "dir1" and "dir1/file" but delete them
	// in the wrong order.
	// So we group the items to delete into multiple groups. All items in a group
	// will have the same depth - depth(/a/b/c) = 2, depth(/a/b/) = 1.
	// We then iterate over the groups in desc order of depth and delete them parallelly.
	for _, group := range groupByDecresingDepths(items) {
		err := t.deleteBlobsParallelly(cloud, group)
		if err != nil {
			return err
		}
	}
	return nil
}

func (s *GoofysTest) selectTestConfig(t *C, flags *cfg.FlagStorage) (conf cfg.S3Config) {
	(&conf).Init()

	if hasEnv("AWS") {
		if isTravis() {
			conf.Region = "us-east-1"
		} else {
			conf.Region = "us-west-2"
		}
		profile := os.Getenv("AWS")
		if profile != "" {
			if profile != "-" {
				conf.Profile = profile
			} else {
				conf.AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
				conf.SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
			}
		}

		conf.BucketOwner = os.Getenv("BUCKET_OWNER")
		if conf.BucketOwner == "" {
			panic("BUCKET_OWNER is required on AWS")
		}
	} else if hasEnv("GCS") {
		conf.Region = "us-west1"
		conf.Profile = os.Getenv("GCS")
		flags.Endpoint = "http://storage.googleapis.com"
	} else if hasEnv("MINIO") {
		conf.Region = "us-east-1"
		conf.AccessKey = "Q3AM3UQ867SPQQA43P2F"
		conf.SecretKey = "zuf+tfteSlswRu7BJ86wekitnifILbZam1KYY3TG"
		flags.Endpoint = "https://play.minio.io:9000"
	} else {
		s.emulator = hasEnv("EMULATOR")

		conf.Region = "us-west-2"
		conf.AccessKey = "foo"
		conf.SecretKey = "bar"
		flags.Endpoint = "http://127.0.0.1:8080"

		if os.Getenv("REGION") != "" {
			conf.Region = os.Getenv("REGION")
		}
		if os.Getenv("AWS_ACCESS_KEY_ID") != "" {
			conf.AccessKey = os.Getenv("AWS_ACCESS_KEY_ID")
		}
		if os.Getenv("AWS_SECRET_ACCESS_KEY") != "" {
			conf.SecretKey = os.Getenv("AWS_SECRET_ACCESS_KEY")
		}
		if os.Getenv("ENDPOINT") != "" {
			flags.Endpoint = os.Getenv("ENDPOINT")
		}
	}

	return
}

func (s *GoofysTest) waitForEmulator(t *C, addr string) {
	if s.emulator {
		port := "80"
		p := strings.Index(addr, "://")
		if p >= 0 {
			addr = addr[p+3:]
			if strings.ToLower(addr[0:p]) == "https" {
				port = "443"
			}
		}
		p = strings.Index(addr, "/")
		if p >= 0 {
			addr = addr[0:p]
		}
		p = strings.Index(addr, ":")
		if p < 0 {
			addr = addr + ":" + port
		}
		err := waitFor(t, addr)
		t.Assert(err, IsNil)
	}
}

func (s *GoofysTest) deleteBucket(cloud StorageBackend) error {
	var err error
	for i := 0; i < 5; i++ {
		param := &ListBlobsInput{}

		// Azure need special handling.
		azureKeysToRemove := make([]string, 0)
		for {
			resp, err := cloud.ListBlobs(param)
			if err != nil {
				return err
			}
			if len(resp.Items) == 0 {
				// To overcome eventual consistency
				break
			}
			keysToRemove := []string{}
			for _, o := range resp.Items {
				keysToRemove = append(keysToRemove, *o.Key)
			}
			if len(keysToRemove) != 0 {
				switch cloud.(type) {
				case *ADLv1, *ADLv2, *AZBlob:
					// ADLV{1|2} and AZBlob (sometimes) supports directories. => dir can be removed only
					// after the dir is empty. So we will remove the blobs in reverse depth order via
					// DeleteADLBlobs after this for loop.
					azureKeysToRemove = append(azureKeysToRemove, keysToRemove...)
				default:
					_, err = cloud.DeleteBlobs(&DeleteBlobsInput{Items: keysToRemove})
					if err != nil {
						return err
					}
				}
			}
		}

		if len(azureKeysToRemove) != 0 {
			err = s.DeleteADLBlobs(cloud, azureKeysToRemove)
			if err != nil {
				return err
			}
		}

		_, err = cloud.MultipartExpire(&MultipartExpireInput{})

		_, err = cloud.RemoveBucket(&RemoveBucketInput{})
		if awsErr, ok := err.(awserr.Error); ok {
			if awsErr.Code() == "BucketNotEmpty" {
				log.Warnf("Retrying delete")
				continue
			} else if awsErr.Code() == "BucketNotExists" || awsErr.Code() == "NoSuchBucket" {
				// Bucket is already deleted (may happen after a bad retry)
				err = nil
			}
		}
		break
	}
	return err
}

func (s *GoofysTest) TearDownTest(t *C) {
	close(s.timeout)
	s.timeout = nil

	if strings.Index(t.TestName(), "NoCloud") >= 0 {
		return
	}

	if s.fs != nil {
		s.fs.SyncTree(nil)
		s.fs.Shutdown()
	}

	for _, cloud := range s.removeBucket {
		err := s.deleteBucket(cloud)
		t.Assert(err, IsNil)
	}
	s.removeBucket = nil
}

func (s *GoofysTest) removeBlob(cloud StorageBackend, t *C, blobPath string) {
	params := &DeleteBlobInput{
		Key: blobPath,
	}
	_, err := cloud.DeleteBlob(params)
	t.Assert(err, IsNil)
}

func (s *GoofysTest) setupBlobs(cloud StorageBackend, t *C, env map[string]*string) {
	const concurrency = 10
	throttler := make(semaphore, concurrency)
	throttler.P(concurrency)

	var globalErr error
	for path, c := range env {
		throttler.V(1)
		go func(path string, content *string) {
			dir := false
			if content == nil {
				if strings.HasSuffix(path, "/") {
					if cloud.Capabilities().DirBlob {
						path = strings.TrimRight(path, "/")
					}
					dir = true
					content = PString("")
				} else {
					content = &path
				}
			}
			defer throttler.P(1)
			params := &PutBlobInput{
				Key:  path,
				Body: bytes.NewReader([]byte(*content)),
				Size: PUInt64(uint64(len(*content))),
				Metadata: map[string]*string{
					"name": aws.String(path + "+/#%00"),
				},
				DirBlob: dir,
			}

			_, err := cloud.PutBlob(params)
			if err != nil {
				globalErr = err
			}
			t.Assert(err, IsNil)
		}(path, c)
	}
	throttler.V(concurrency)
	throttler = make(semaphore, concurrency)
	throttler.P(concurrency)
	t.Assert(globalErr, IsNil)

	// double check, except on AWS S3, because there we sometimes
	// hit 404 NoSuchBucket and there's no way to distinguish that
	// from 404 KeyNotFound
	if !hasEnv("AWS") {
		for path, c := range env {
			throttler.V(1)
			go func(path string, content *string) {
				defer throttler.P(1)
				params := &HeadBlobInput{Key: path}
				res, err := cloud.HeadBlob(params)
				if err != nil {
					log.Errorf("Unexpected error: HEAD %v returned %v", path, err)
					if err == syscall.ENOENT {
						time.Sleep(3 * time.Second)
						res, err = cloud.HeadBlob(params)
					}
				}
				t.Assert(err, IsNil)
				if content != nil {
					t.Assert(res.Size, Equals, uint64(len(*content)))
				} else if strings.HasSuffix(path, "/") || path == "zero" {
					t.Assert(res.Size, Equals, uint64(0))
				} else {
					t.Assert(res.Size, Equals, uint64(len(path)))
				}
			}(path, c)
		}
		throttler.V(concurrency)
		t.Assert(globalErr, IsNil)
	}
}

func (s *GoofysTest) setupDefaultEnv(t *C, prefix string) {
	s.env = map[string]*string{
		prefix + "file1":           nil,
		prefix + "file2":           nil,
		prefix + "dir1/file3":      nil,
		prefix + "dir2/":           nil,
		prefix + "dir2/dir3/":      nil,
		prefix + "dir2/dir3/file4": nil,
		prefix + "dir4/":           nil,
		prefix + "dir4/file5":      nil,
		prefix + "empty_dir/":      nil,
		prefix + "empty_dir2/":     nil,
		prefix + "zero":            PString(""),
	}

	s.setupBlobs(s.cloud, t, s.env)
}

func (s *GoofysTest) setUpTestTimeout(t *C, timeout time.Duration) {
	if s.timeout != nil {
		close(s.timeout)
	}
	s.timeout = make(chan int)
	debug.SetTraceback("all")
	started := time.Now()

	go func() {
		select {
		case _, ok := <-s.timeout:
			if !ok {
				return
			}
		case <-time.After(timeout):
			panic(fmt.Sprintf("timeout %v reached. Started %v now %v",
				timeout, started, time.Now()))
		}
	}()
}

func (s *GoofysTest) SetUpTest(t *C) {
	log.Infof("Starting %v at %v", t.TestName(), time.Now())

	s.setUpTestTimeout(t, PerTestTimeout)

	if strings.Index(t.TestName(), "NoCloud") >= 0 {
		return
	}

	var createBucket bool
	bucket := os.Getenv("BUCKET")
	if bucket == "" {
		bucket = "goofys-test-" + RandStringBytesMaskImprSrc(16)
		createBucket = true
	}

	flags := cfg.DefaultFlags()
	if strings.Index(t.TestName(), "Mem20M") >= 0 {
		// has to be set before create FS
		flags.MemoryLimit = 20 * 1024 * 1024
	} else if strings.Index(t.TestName(), "Mem100M") >= 0 {
		// has to be set before create FS
		flags.MemoryLimit = 100 * 1024 * 1024
	}
	if hasEnv("DEBUG") {
		flags.DebugS3 = true
		flags.DebugFuse = true
		cfg.SetCloudLogLevel(logrus.DebugLevel)
		l := cfg.GetLogger("fuse")
		l.Level = logrus.DebugLevel
		l = cfg.GetLogger("s3")
		l.Level = logrus.DebugLevel
	}

	cloud := os.Getenv("CLOUD")

	if cloud == "s3" {
		conf := s.selectTestConfig(t, flags)
		flags.Backend = &conf

		s.emulator = hasEnv("EMULATOR")
		s.waitForEmulator(t, flags.Endpoint)

		s3, err := NewS3(bucket, flags, &conf)
		t.Assert(err, IsNil)

		s.cloud = s3
		s3.config.ListV1Ext = hasEnv("YANDEX")
		if hasEnv("EVENTUAL_CONSISTENCY") {
			s.cloud = NewS3BucketEventualConsistency(s3)
		}

		if s.emulator {
			s3.Handlers.Sign.Clear()
			s3.Handlers.Sign.PushBack(SignV2)
			s3.Handlers.Sign.PushBackNamed(corehandlers.BuildContentLengthHandler)
		}
		_, err = s3.ListBuckets(nil)
		t.Assert(err, IsNil)

	} else if cloud == "gcs" {
		conf := s.selectTestConfig(t, flags)
		flags.Backend = &conf

		var err error
		s.cloud, err = NewGCS3(bucket, flags, &conf)
		t.Assert(s.cloud, NotNil)
		t.Assert(err, IsNil)
	} else if cloud == "azblob" {
		config, err := cfg.AzureBlobConfig(os.Getenv("ENDPOINT"), "", "blob")
		t.Assert(err, IsNil)

		if config.Endpoint == AzuriteEndpoint {
			s.azurite = true
			s.emulator = true
			s.waitForEmulator(t, config.Endpoint)
		}

		// Azurite's SAS is buggy, ex: https://github.com/Azure/Azurite/issues/216
		if os.Getenv("SAS_EXPIRE") != "" {
			expire, err := time.ParseDuration(os.Getenv("SAS_EXPIRE"))
			t.Assert(err, IsNil)

			config.TokenRenewBuffer = expire / 2
			credential, err := azblob.NewSharedKeyCredential(config.AccountName, config.AccountKey)
			t.Assert(err, IsNil)

			// test sas token config
			config.SasToken = func() (string, error) {
				sasQueryParams, err := azblob.AccountSASSignatureValues{
					Protocol:   azblob.SASProtocolHTTPSandHTTP,
					StartTime:  time.Now().UTC().Add(-1 * time.Hour),
					ExpiryTime: time.Now().UTC().Add(expire),
					Services:   azblob.AccountSASServices{Blob: true}.String(),
					ResourceTypes: azblob.AccountSASResourceTypes{
						Service:   true,
						Container: true,
						Object:    true,
					}.String(),
					Permissions: azblob.AccountSASPermissions{
						Read:   true,
						Write:  true,
						Delete: true,
						List:   true,
						Create: true,
					}.String(),
				}.NewSASQueryParameters(credential)
				if err != nil {
					return "", err
				}
				return sasQueryParams.Encode(), nil
			}
		}

		flags.Backend = &config

		s.cloud, err = NewAZBlob(bucket, &config)
		t.Assert(err, IsNil)
		t.Assert(s.cloud, NotNil)
	} else if cloud == "adlv1" {
		cred := azureauth.NewClientCredentialsConfig(
			os.Getenv("ADLV1_CLIENT_ID"),
			os.Getenv("ADLV1_CLIENT_CREDENTIAL"),
			os.Getenv("ADLV1_TENANT_ID"))
		auth, err := cred.Authorizer()
		t.Assert(err, IsNil)

		config := cfg.ADLv1Config{
			Endpoint:   os.Getenv("ENDPOINT"),
			Authorizer: auth,
		}
		config.Init()

		flags.Backend = &config

		s.cloud, err = NewADLv1(bucket, flags, &config)
		t.Assert(err, IsNil)
		t.Assert(s.cloud, NotNil)
	} else if cloud == "adlv2" {
		var err error
		var auth autorest.Authorizer

		if os.Getenv("AZURE_STORAGE_ACCOUNT") != "" && os.Getenv("AZURE_STORAGE_KEY") != "" {
			auth = &cfg.AZBlobConfig{
				AccountName: os.Getenv("AZURE_STORAGE_ACCOUNT"),
				AccountKey:  os.Getenv("AZURE_STORAGE_KEY"),
			}
		} else {
			cred := azureauth.NewClientCredentialsConfig(
				os.Getenv("ADLV2_CLIENT_ID"),
				os.Getenv("ADLV2_CLIENT_CREDENTIAL"),
				os.Getenv("ADLV2_TENANT_ID"))
			cred.Resource = azure.PublicCloud.ResourceIdentifiers.Storage
			auth, err = cred.Authorizer()
			t.Assert(err, IsNil)
		}

		config := cfg.ADLv2Config{
			Endpoint:   os.Getenv("ENDPOINT"),
			Authorizer: auth,
		}

		flags.Backend = &config

		s.cloud, err = NewADLv2(bucket, flags, &config)
		t.Assert(err, IsNil)
		t.Assert(s.cloud, NotNil)
	} else {
		t.Fatal("Unsupported backend")
	}

	if createBucket {
		_, err := s.cloud.MakeBucket(&MakeBucketInput{})
		t.Assert(err, IsNil)
		s.removeBucket = append(s.removeBucket, s.cloud)
	}

	s.setupDefaultEnv(t, "")

	if hasEnv("EVENTUAL_CONSISTENCY") {
		s.fs, _ = newGoofys(context.Background(), bucket, flags,
			func(bucket string, flags *cfg.FlagStorage) (StorageBackend, error) {
				cloud, err := NewBackend(bucket, flags)
				if err != nil {
					return nil, err
				}
				return NewS3BucketEventualConsistency(cloud.(*S3Backend)), nil
			})
	} else {
		s.fs, _ = NewGoofys(context.Background(), bucket, flags)
	}
	t.Assert(s.fs, NotNil)

	s.ctx = context.Background()

	if hasEnv("GCS") {
		flags.Endpoint = "http://storage.googleapis.com"
	}
}

func (s *GoofysTest) clearPrefix(t *C, cloud StorageBackend, prefix string) {
	for {
		res, err := cloud.ListBlobs(&ListBlobsInput{
			Prefix: PString(prefix + "/"),
		})
		t.Assert(err, IsNil)
		if len(res.Items) == 0 {
			break
		}
		names := make([]string, 1)
		names[0] = prefix
		for _, item := range res.Items {
			names = append(names, *item.Key)
		}
		err = s.deleteBlobsParallelly(cloud, names)
		t.Assert(err, IsNil)
	}
}
