// Copyright 2015 - 2017 Ka-Hing Cheung
// Copyright 2015 - 2017 Google Inc. All Rights Reserved.
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
	"io"
	"os"
	"runtime"
	"strconv"
	"strings"
	"syscall"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/urfave/cli"
)

const GEESEFS_VERSION = "0.43.0"

var flagCategories map[string]string

// Set up custom help text for goofys; in particular the usage section.
func filterCategory(flags []cli.Flag, category string) (ret []cli.Flag) {
	for _, f := range flags {
		if flagCategories[f.GetName()] == category {
			ret = append(ret, f)
		}
	}
	return
}

var VersionHash string
var FuseOptions string

func NewApp() (app *cli.App) {
	cli.AppHelpTemplate = `NAME:
   {{.Name}} - {{.Usage}}

USAGE:
   {{.Name}} {{if .Flags}}[global options]{{end}} bucket[:prefix] mountpoint
   {{if .Version}}
VERSION:
   {{.Version}}
   {{end}}{{if len .Authors}}
AUTHOR(S):
   {{range .Authors}}{{ . }}{{end}}
   {{end}}{{if .Commands}}
COMMANDS:
   {{range .Commands}}{{join .Names ", "}}{{ "\t" }}{{.Usage}}
   {{end}}{{end}}{{if .Flags}}
GLOBAL OPTIONS:
   {{range category .Flags ""}}{{.}}
   {{end}}
TUNING OPTIONS:
   {{range category .Flags "tuning"}}{{.}}
   {{end}}
S3 OPTIONS:
   {{range category .Flags "aws"}}{{.}}
   {{end}}
MISC OPTIONS:
   {{range category .Flags "misc"}}{{.}}
   {{end}}{{end}}
` + FuseOptions + `{{if .Copyright }}COPYRIGHT:
   {{.Copyright}}
   {{end}}
`

	uid, gid := MyUserAndGroup()

	s3Default := (&S3Config{}).Init()

	fsFlags := []cli.Flag{
		/////////////////////////
		// File system
		/////////////////////////

		cli.StringSliceFlag{
			Name:  "o",
			Usage: "Additional system-specific mount options. Be careful!",
		},

		cli.StringFlag{
			Name:  "cache",
			Usage: "Directory to use for data cache. (default: off)",
		},

		cli.IntFlag{
			Name:  "dir-mode",
			Value: 0755,
			Usage: "Permission bits for directories. (default: 0755)",
		},

		cli.IntFlag{
			Name:  "file-mode",
			Value: 0644,
			Usage: "Permission bits for files. (default: 0644)",
		},

		cli.IntFlag{
			Name:  "cache-file-mode",
			Value: 0644,
			Usage: "Permission bits for disk cache files. (default: 0644)",
		},

		cli.IntFlag{
			Name:  "uid",
			Value: uid,
			Usage: "UID owner of all inodes.",
		},

		cli.IntFlag{
			Name:  "gid",
			Value: gid,
			Usage: "GID owner of all inodes.",
		},

		cli.IntFlag{
			Name:  "setuid",
			Value: uid,
			Usage: "Drop root privileges and change to this user ID (defaults to --uid).",
		},

		cli.IntFlag{
			Name:  "setgid",
			Value: gid,
			Usage: "Drop root group and change to this group ID (defaults to --gid).",
		},

		cli.BoolFlag{
			Name:  "refresh-dirs",
			Usage: "Automatically refresh open directories using notifications under Windows",
		},
	}

	s3Flags := []cli.Flag{
		/////////////////////////
		// S3
		/////////////////////////

		cli.StringFlag{
			Name:  "endpoint",
			Value: "https://storage.yandexcloud.net",
			Usage: "The S3 endpoint to connect to." +
				" Possible values: http://127.0.0.1:8081/, https://s3.amazonaws.com",
		},

		cli.StringFlag{
			Name:  "project-id",
			Value: "",
			Usage: "Project ID for Ceph multi-tenancy bucket sharing (bucket syntax project-id:bucket-name)",
		},

		cli.BoolFlag{
			Name:  "iam",
			Usage: "Try to authenticate automatically using VM metadata service (Yandex Cloud / IMDSv1 / GCP)",
		},

		cli.StringFlag{
			Name:  "iam-header",
			Value: "X-YaCloud-SubjectToken",
			Usage: "The header to use for authenticating with IAM token",
		},

		cli.StringFlag{
			Name:  "iam-flavor",
			Value: "gcp",
			Usage: "Instance metadata service flavor: gcp or imdsv1",
		},

		cli.StringFlag{
			Name:  "iam-url",
			Value: "",
			Usage: "Custom instance metadata service URL",
		},

		cli.StringFlag{
			Name:  "region",
			Value: s3Default.Region,
			Usage: "The region to connect to. Usually this is auto-detected." +
				" Possible values: us-east-1, us-west-1, us-west-2, eu-west-1, " +
				"eu-central-1, ap-southeast-1, ap-southeast-2, ap-northeast-1, " +
				"sa-east-1, cn-north-1",
		},

		cli.BoolFlag{
			Name:  "requester-pays",
			Usage: "Whether to allow access to requester-pays buckets (default: off)",
		},

		cli.StringFlag{
			Name:  "storage-class",
			Value: s3Default.StorageClass,
			Usage: "The type of storage to use when writing objects." +
				" Possible values: REDUCED_REDUNDANCY, STANDARD, STANDARD_IA.",
		},

		cli.Uint64Flag{
			Name:  "cold-min-size",
			Usage: "Objects smaller than this size will be stored in STANDARD if STANDARD_IA (cold storage) is selected as default.",
		},

		cli.StringFlag{
			Name:  "profile",
			Usage: "Use a named profile from $HOME/.aws/credentials instead of \"default\"",
		},

		cli.StringSliceFlag{
			Name:  "shared-config",
			Usage: "Use different shared configuration file(s) instead of $HOME/.aws/credentials and $HOME/.aws/config",
		},

		cli.BoolFlag{
			Name:  "use-content-type",
			Usage: "Set Content-Type according to file extension and /etc/mime.types (default: off)",
		},

		/// http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectPUT.html
		/// See http://docs.aws.amazon.com/AmazonS3/latest/dev/UsingServerSideEncryption.html
		cli.BoolFlag{
			Name:  "sse",
			Usage: "Enable basic server-side encryption at rest (SSE-S3) in S3 for all writes (default: off)",
		},

		cli.StringFlag{
			Name:  "sse-kms",
			Usage: "Enable KMS encryption (SSE-KMS) for all writes using this particular KMS `key-id`. Leave blank to Use the account's CMK - customer master key (default: off)",
			Value: "",
		},

		cli.StringFlag{
			Name:  "sse-c",
			Usage: "Enable server-side encryption using this base64-encoded key (default: off)",
			Value: "",
		},

		cli.BoolFlag{
			Name:  "no-checksum",
			Usage: "Disable content MD5 and SHA256 checksums for performance (default: off)",
		},

		cli.StringFlag{
			Name:  "list-type",
			Usage: "Listing type to use: ext-v1 (yandex only), 2 or 1 (default: ext-v1 for yandex, 1 for others)",
			Value: "",
		},

		cli.BoolFlag{
			Name:  "no-detect",
			Usage: "Turn off autodetection of anonymous access, bucket location and signature algorithm on start",
		},

		cli.BoolFlag{
			Name:  "no-expire-multipart",
			Usage: "Do not expire multipart uploads older than --multipart-age on start",
		},

		cli.StringFlag{
			Name:  "multipart-age",
			Usage: "Multipart uploads older than this value will be deleted on start",
			Value: "48h",
		},

		cli.IntFlag{
			Name:  "multipart-copy-threshold",
			Usage: "Threshold for switching from single-part to multipart object copy in MB. Maximum for AWS S3 is 5 GB",
			Value: 128,
		},

		/// http://docs.aws.amazon.com/AmazonS3/latest/dev/acl-overview.html#canned-acl
		cli.StringFlag{
			Name:  "acl",
			Usage: "The canned ACL to apply to the object. Possible values: private, public-read, public-read-write, authenticated-read, aws-exec-read, bucket-owner-read, bucket-owner-full-control (default: off)",
			Value: "",
		},

		cli.BoolFlag{
			Name:  "subdomain",
			Usage: "Enable subdomain mode of S3",
		},

		cli.IntFlag{
			Name:  "sdk-max-retries",
			Value: 3,
			Usage: "Maximum number of AWS SDK request retries.",
		},

		cli.DurationFlag{
			Name:  "sdk-min-retry-delay",
			Value: 30 * time.Millisecond,
			Usage: "Minimum delay for AWS SDK retries of temporary request failures.",
		},

		cli.DurationFlag{
			Name:  "sdk-max-retry-delay",
			Value: 300 * time.Second,
			Usage: "Maximum delay for AWS SDK retries of temporary request failures.",
		},

		cli.DurationFlag{
			Name:  "sdk-min-throttle-delay",
			Value: 500 * time.Millisecond,
			Usage: "Minimum delay for AWS SDK retries of throttled requests (429, 502, 503, 504).",
		},

		cli.DurationFlag{
			Name:  "sdk-max-throttle-delay",
			Value: 300 * time.Second,
			Usage: "Maximum delay for AWS SDK retries of throttled requests.",
		},

		cli.BoolFlag{
			Name:  "no-verify-ssl",
			Usage: "Skip verify check ssl for s3",
		},
	}

	tuningFlags := []cli.Flag{
		/////////////////////////
		// Tuning
		/////////////////////////

		cli.IntFlag{
			Name:  "memory-limit",
			Usage: "Maximum memory in MB to use for data cache",
			Value: 1000,
		},

		cli.BoolFlag{
			Name:  "use-enomem",
			Usage: "Return ENOMEM errors to applications when trying to read too many large files in parallel",
		},

		cli.IntFlag{
			Name:  "entry-limit",
			Usage: "Maximum metadata entries to cache in memory (1 entry uses ~1 KB of memory)",
			Value: 100000,
		},

		cli.IntFlag{
			Name:  "gc-interval",
			Usage: "Force garbage collection after this amount of data buffer allocations",
			Value: 250,
		},

		cli.BoolFlag{
			Name:  "cheap",
			Usage: "Reduce S3 operation costs at the expense of some performance (default: off)",
		},

		cli.BoolFlag{
			Name: "no-preload-dir",
			Usage: "Disable directory listing pre-loading when you open individual files" +
				" and don't do any READDIR calls. Default is to always pre-load listing" +
				" which helps in a lot of cases, for example when you use rsync. Note" +
				" that you should also enable --no-implicit-dir if you want to fully avoid" +
				" ListObjects requests during file lookups.",
		},

		cli.BoolFlag{
			Name:  "no-implicit-dir",
			Usage: "Assume all directory objects (\"dir/\") exist (default: off)",
		},

		cli.BoolFlag{
			Name:  "no-dir-object",
			Usage: "Do not create and check directory objects (\"dir/\") (default: off)",
		},

		cli.IntFlag{
			Name:  "max-flushers",
			Value: 16,
			Usage: "How much parallel requests should be used for flushing changes to server",
		},

		cli.IntFlag{
			Name:  "max-parallel-parts",
			Value: 8,
			Usage: "How much parallel requests out of the total number can be used for large part uploads." +
				" Large parts take more bandwidth so they usually require less parallelism",
		},

		cli.IntFlag{
			Name:  "max-parallel-copy",
			Value: 16,
			Usage: "How much parallel unmodified part copy requests should be used." +
				" This limit is separate from max-flushers",
		},

		cli.IntFlag{
			Name:  "read-ahead",
			Value: 5 * 1024,
			Usage: "How much data in KB should be pre-loaded with every read by default",
		},

		cli.IntFlag{
			Name:  "small-read-count",
			Value: 4,
			Usage: "Number of last reads within a single file handle to be checked for being random",
		},

		cli.IntFlag{
			Name:  "small-read-cutoff",
			Value: 128,
			Usage: "Maximum average size of last reads in KB to trigger \"small\" readahead",
		},

		cli.IntFlag{
			Name:  "read-ahead-small",
			Value: 128,
			Usage: "Smaller readahead size in KB to be used when small random reads are detected",
		},

		cli.IntFlag{
			Name:  "large-read-cutoff",
			Value: 20 * 1024,
			Usage: "Amount of linear read in KB after which the \"large\" readahead should be triggered",
		},

		cli.IntFlag{
			Name:  "read-ahead-large",
			Value: 100 * 1024,
			Usage: "Larger readahead size in KB to be used when long linear reads are detected",
		},

		cli.IntFlag{
			Name:  "read-ahead-parallel",
			Value: 20 * 1024,
			Usage: "Larger readahead will be triggered in parallel chunks of this size in KB",
		},

		cli.IntFlag{
			Name:  "read-merge",
			Value: 512,
			Usage: "Two HTTP requests required to satisfy a read will be merged into one" +
				" if they're at most this number of KB away",
		},

		cli.IntFlag{
			Name:  "single-part",
			Value: 5,
			Usage: "Maximum size of an object in MB to upload it as a single part." +
				" Can't be less than 5 MB",
		},

		cli.StringFlag{
			Name:  "part-sizes",
			Value: "5:1000,25:1000,125",
			Usage: "Part sizes in MB. Total part count is always 10000 in S3." +
				" Default is 1000 5 MB parts, then 1000 25 MB parts" +
				" and then 125 MB for the rest of parts",
		},

		cli.BoolFlag{
			Name:  "enable-patch",
			Usage: "Use PATCH method to upload object data changes to S3. All PATCH related flags are Yandex only. (default: off)",
		},

		cli.BoolFlag{
			Name:  "drop-patch-conflicts",
			Usage: "Drop local changes in case of conflicting concurrent PATCH updates. (default: off)",
		},

		cli.BoolFlag{
			Name: "prefer-patch-uploads",
			Usage: "When uploading new objects, prefer PATCH requests to standard multipart upload process." +
				"This allows for changes to appear faster in exchange for slower upload speed due to limited parallelism." +
				"Must be used with --enable-patch flag (default: off)",
		},

		cli.IntFlag{
			Name:  "max-merge-copy",
			Value: 0,
			Usage: "If non-zero, allow to compose larger parts up to this number of megabytes" +
				" in size from existing unchanged parts when doing server-side part copy." +
				" Must be left at 0 for Yandex S3",
		},

		cli.BoolFlag{
			Name:  "ignore-fsync",
			Usage: "Do not wait until changes are persisted to the server on fsync() call (default: off)",
		},

		cli.BoolFlag{
			Name:  "fsync-on-close",
			Usage: "Wait until changes are persisted to the server when closing file (default: off)",
		},

		cli.BoolFlag{
			Name: "enable-perms",
			Usage: "Enable permissions, user and group ID." +
				" Only works correctly if your S3 returns UserMetadata in listings (default: off)",
		},

		cli.BoolFlag{
			Name: "enable-specials",
			Usage: "Enable special file support (sockets, devices, named pipes)." +
				" Only works correctly if your S3 returns UserMetadata in listings (default: on for Yandex, off for others)",
		},

		cli.BoolFlag{
			Name:  "no-specials",
			Usage: "Disable special file support (sockets, devices, named pipes).",
		},

		cli.BoolFlag{
			Name: "enable-mtime",
			Usage: "Enable modification time preservation." +
				" Only works correctly if your S3 returns UserMetadata in listings (default: off)",
		},

		cli.BoolFlag{
			Name:  "disable-xattr",
			Usage: "Disable extended attributes. Improves performance of very long directory listings",
		},

		cli.StringFlag{
			Name:  "uid-attr",
			Value: "uid",
			Usage: "User ID metadata attribute name",
		},

		cli.StringFlag{
			Name:  "gid-attr",
			Value: "gid",
			Usage: "Group ID metadata attribute name",
		},

		cli.StringFlag{
			Name:  "mode-attr",
			Value: "mode",
			Usage: "File mode (permissions & special file flags) metadata attribute name",
		},

		cli.StringFlag{
			Name:  "rdev-attr",
			Value: "rdev",
			Usage: "Block/character device number metadata attribute name",
		},

		cli.StringFlag{
			Name:  "mtime-attr",
			Value: "mtime",
			Usage: "File modification time (UNIX time) metadata attribute name",
		},

		cli.StringFlag{
			Name:  "symlink-attr",
			Value: "--symlink-target",
			Usage: "Symbolic link target metadata attribute name." +
				" Only works correctly if your S3 returns UserMetadata in listings" +
				" You can use --symlink-zeroed flag to make additional HEAD requests as a workaround.",
		},

		cli.StringFlag{
			Name:  "hash-attr",
			Value: "--content-sha256",
			Usage: "Hash metadata attribute name." +
				" If this attribute is present, the hash will be computed on the client side and pushed to user metadata." +
				" It can then be retrieved with HEAD requests.",
		},

		cli.IntFlag{
			Name:  "min-file-size-for-hash-kb",
			Value: 1024 * 10,
			Usage: "Minimum file size in KB for which a hash will be computed and stored in user metadata." +
				" This is to avoid the overhead of computing the hash for small files." +
				" NOTE: This only applies if hash-attr is also set to a valid attribute name.",
		},

		cli.BoolFlag{
			Name: "symlink-zeroed",
			Usage: "Strip content when creating symlink files and make an additional" +
				" HEAD request to fetch metadata when encountering files with size zero from ListObjectsV2.",
		},

		cli.StringFlag{
			Name:  "refresh-attr",
			Value: ".invalidate",
			Usage: "Setting xattr with this name, without user. prefix, " +
				" refreshes the cache of the file or directory.",
		},

		cli.DurationFlag{
			Name:  "stat-cache-ttl",
			Value: time.Minute,
			Usage: "How long to cache file metadata.",
		},

		cli.DurationFlag{
			Name:  "http-timeout",
			Value: 30 * time.Second,
			Usage: "Set the timeout on HTTP requests to S3",
		},

		cli.DurationFlag{
			Name:  "retry-interval",
			Value: 30 * time.Second,
			Usage: "Retry unsuccessful writes after this time",
		},

		cli.DurationFlag{
			Name:  "read-retry-interval",
			Value: 1 * time.Second,
			Usage: "Initial interval for retrying unsuccessful reads",
		},

		cli.Float64Flag{
			Name:  "read-retry-mul",
			Value: 2,
			Usage: "Increase read retry interval this number of times on each unsuccessful attempt",
		},

		cli.DurationFlag{
			Name:  "read-retry-max-interval",
			Value: 60 * time.Second,
			Usage: "Maximum interval for retrying unsuccessful reads",
		},

		cli.IntFlag{
			Name:  "read-retry-attempts",
			Value: 0,
			Usage: "Maximum read retry attempts (0 means unlimited)",
		},

		cli.IntFlag{
			Name:  "max-disk-cache-fd",
			Value: 512,
			Usage: "Simultaneously opened cache file descriptor limit",
		},
	}

	if runtime.GOOS == "windows" {
		tuningFlags = append(tuningFlags, cli.StringFlag{
			Name:  "refresh-filename",
			Value: ".invalidate",
			Usage: "Trying to open a file with this name refreshes the cache of its directory.",
		})
		tuningFlags = append(tuningFlags, cli.StringFlag{
			Name:  "flush-filename",
			Value: ".fsyncdir",
			Usage: "Trying to open a file with this name flushes the cache of its directory to server.",
		})
	}

	debugFlags := []cli.Flag{
		/////////////////////////
		// Debugging
		/////////////////////////

		cli.BoolFlag{
			Name:  "debug",
			Usage: "Enable generic debugging output.",
		},

		cli.BoolFlag{
			Name:  "debug_fuse",
			Usage: "Enable fuse-related debugging output.",
		},

		cli.BoolFlag{
			Name:  "debug_s3",
			Usage: "Enable S3-related debugging output.",
		},

		cli.StringFlag{
			Name:  "pprof",
			Usage: "Specify port or host:port to enable pprof HTTP profiler on that port.",
			Value: "",
		},

		cli.BoolFlag{
			Name:  "f",
			Usage: "Run geesefs in foreground.",
		},

		cli.StringFlag{
			Name:  "log-file",
			Usage: "Redirect logs to file, 'stderr' (default for foreground) or 'syslog' (default for background).",
			Value: "",
		},

		cli.DurationFlag{
			Name:  "print-stats",
			Value: 30 * time.Second,
			Usage: "I/O statistics printing interval. Set to 0 to disable.",
		},

		cli.BoolFlag{
			Name:  "debug_grpc",
			Usage: "Enable grpc logging in cluster mode.",
		},
	}

	clusterFlags := []cli.Flag{
		cli.BoolFlag{
			Name:  "cluster",
			Usage: "Enable cluster mode.",
		},

		cli.BoolFlag{
			Name:  "grpc-reflection",
			Usage: "Enable grpc reflection (--cluster flag required).",
		},

		cli.StringFlag{
			Name:  "cluster-me",
			Usage: "<node-id>:<address> to communicate with this node (--cluster flag required).",
		},

		cli.StringSliceFlag{
			Name:  "cluster-peer",
			Usage: "List of all cluster nodes in format <node-id>:<address> (--cluster flag required).",
		},
	}

	app = &cli.App{
		Name:     "geesefs",
		Version:  GEESEFS_VERSION,
		Usage:    "Mount an S3 bucket locally",
		HideHelp: true,
		Writer:   os.Stderr,
		Flags: append(append(append(append(append([]cli.Flag{
			cli.BoolFlag{
				Name:  "help, h",
				Usage: "Print this help text and exit successfully.",
			},
		}, fsFlags...), s3Flags...), tuningFlags...), debugFlags...), clusterFlags...),
	}

	var funcMap = template.FuncMap{
		"category": filterCategory,
		"join":     strings.Join,
	}

	flagCategories = map[string]string{}
	flagCategories["help"] = "misc"
	flagCategories["h"] = "misc"

	for _, f := range s3Flags {
		for _, n := range strings.Split(f.GetName(), ",") {
			flagCategories[strings.Trim(n, " ")] = "aws"
		}
	}
	for _, f := range tuningFlags {
		for _, n := range strings.Split(f.GetName(), ",") {
			flagCategories[strings.Trim(n, " ")] = "tuning"
		}
	}
	for _, f := range debugFlags {
		for _, n := range strings.Split(f.GetName(), ",") {
			flagCategories[strings.Trim(n, " ")] = "misc"
		}
	}

	cli.HelpPrinter = func(w io.Writer, templ string, data interface{}) {
		w = tabwriter.NewWriter(w, 1, 8, 2, ' ', 0)
		var tmplGet = template.Must(template.New("help").Funcs(funcMap).Parse(templ))
		tmplGet.Execute(w, app)
	}

	return
}

func parsePartSizes(s string) (result []PartSizeConfig) {
	partSizes := strings.Split(s, ",")
	totalCount := uint64(0)
	for pi, ps := range partSizes {
		a := strings.Split(ps, ":")
		size, err := strconv.ParseUint(a[0], 10, 32)
		if err != nil {
			panic("Incorrect syntax for --part-sizes")
		}
		count := uint64(0)
		if len(a) > 1 {
			count, err = strconv.ParseUint(a[1], 10, 32)
			if err != nil {
				panic("Incorrect syntax for --part-sizes")
			}
		}
		if count == 0 {
			if pi < len(partSizes)-1 {
				panic("Part count may be omitted only for the last interval")
			}
			count = 10000 - totalCount
		}
		totalCount += count
		if totalCount > 10000 {
			panic("Total part count must be 10000")
		}
		if size < 5 {
			panic("Minimum part size is 5 MB")
		}
		if size > 5*1024 {
			panic("Maximum part size is 5 GB")
		}
		result = append(result, PartSizeConfig{
			PartSize:  size * 1024 * 1024,
			PartCount: count,
		})
	}
	return
}

func parseNode(s string) *NodeConfig {
	parts := strings.SplitN(s, ":", 2)
	if len(parts) != 2 {
		panic("Incorrect syntax for node config, should be: <node-id>:<address>")
	}
	nodeId, err := strconv.ParseUint(parts[0], 10, 64)
	if err != nil {
		panic("Incorrect syntax for node config, <node-id> shoud be uint64")
	}
	return &NodeConfig{
		Id:      nodeId,
		Address: parts[1],
	}
}

// PopulateFlags adds the flags accepted by run to the supplied flag set, returning the
// variables into which the flags will parse.
func PopulateFlags(c *cli.Context) (ret *FlagStorage) {
	singlePart := c.Int("single-part")
	if singlePart < 5 {
		singlePart = 5
	}

	flags := &FlagStorage{
		// File system
		MountOptions:   c.StringSlice("o"),
		DirMode:        os.FileMode(c.Int("dir-mode")),
		FileMode:       os.FileMode(c.Int("file-mode")),
		Uid:            uint32(c.Int("uid")),
		Gid:            uint32(c.Int("gid")),
		Setuid:         c.Int("setuid"),
		Setgid:         c.Int("setgid"),
		WinRefreshDirs: c.Bool("refresh-dirs"),

		// Tuning,
		MemoryLimit:          uint64(1024 * 1024 * c.Int("memory-limit")),
		UseEnomem:            c.Bool("use-enomem"),
		EntryLimit:           c.Int("entry-limit"),
		GCInterval:           uint64(1024 * 1024 * c.Int("gc-interval")),
		Cheap:                c.Bool("cheap"),
		ExplicitDir:          c.Bool("no-implicit-dir"),
		NoDirObject:          c.Bool("no-dir-object"),
		MaxFlushers:          int64(c.Int("max-flushers")),
		MaxParallelParts:     c.Int("max-parallel-parts"),
		MaxParallelCopy:      c.Int("max-parallel-copy"),
		StatCacheTTL:         c.Duration("stat-cache-ttl"),
		HTTPTimeout:          c.Duration("http-timeout"),
		RetryInterval:        c.Duration("retry-interval"),
		ReadRetryInterval:    c.Duration("read-retry-interval"),
		ReadRetryMultiplier:  c.Float64("read-retry-mul"),
		ReadRetryMax:         c.Duration("read-retry-max-interval"),
		ReadRetryAttempts:    c.Int("read-retry-attempts"),
		ReadAheadKB:          uint64(c.Int("read-ahead")),
		SmallReadCount:       uint64(c.Int("small-read-count")),
		SmallReadCutoffKB:    uint64(c.Int("small-read-cutoff")),
		ReadAheadSmallKB:     uint64(c.Int("read-ahead-small")),
		LargeReadCutoffKB:    uint64(c.Int("large-read-cutoff")),
		ReadAheadLargeKB:     uint64(c.Int("read-ahead-large")),
		ReadAheadParallelKB:  uint64(c.Int("read-ahead-parallel")),
		ReadMergeKB:          uint64(c.Int("read-merge")),
		SinglePartMB:         uint64(singlePart),
		MaxMergeCopyMB:       uint64(c.Int("max-merge-copy")),
		IgnoreFsync:          c.Bool("ignore-fsync"),
		FsyncOnClose:         c.Bool("fsync-on-close"),
		EnablePerms:          c.Bool("enable-perms"),
		EnableSpecials:       c.Bool("enable-specials"),
		EnableMtime:          c.Bool("enable-mtime"),
		DisableXattr:         c.Bool("disable-xattr"),
		UidAttr:              c.String("uid-attr"),
		GidAttr:              c.String("gid-attr"),
		FileModeAttr:         c.String("mode-attr"),
		RdevAttr:             c.String("rdev-attr"),
		MtimeAttr:            c.String("mtime-attr"),
		HashAttr:             c.String("hash-attr"),
		MinFileSizeForHashKB: uint64(c.Int("min-file-size-for-hash-kb")),
		SymlinkAttr:          c.String("symlink-attr"),
		SymlinkZeroed:        c.Bool("symlink-zeroed"),
		RefreshAttr:          c.String("refresh-attr"),
		CachePath:            c.String("cache"),
		MaxDiskCacheFD:       int64(c.Int("max-disk-cache-fd")),
		CacheFileMode:        os.FileMode(c.Int("cache-file-mode")),
		UsePatch:             c.Bool("enable-patch"),
		DropPatchConflicts:   c.Bool("drop-patch-conflicts"),
		PreferPatchUploads:   c.Bool("prefer-patch-uploads"),
		NoPreloadDir:         c.Bool("no-preload-dir"),
		NoVerifySSL:          c.Bool("no-verify-ssl"),

		// Common Backend Config
		Endpoint:       c.String("endpoint"),
		UseContentType: c.Bool("use-content-type"),

		// Debugging,
		DebugMain:     c.Bool("debug"),
		DebugFuse:     c.Bool("debug_fuse"),
		DebugS3:       c.Bool("debug_s3"),
		Foreground:    c.Bool("f"),
		LogFile:       c.String("log-file"),
		StatsInterval: c.Duration("print-stats"),
		PProf:         c.String("pprof"),
		DebugGrpc:     c.Bool("debug_grpc"),

		// Cluster Mode
		ClusterMode:           c.Bool("cluster"),
		ClusterGrpcReflection: c.Bool("grpc-reflection"),
	}

	if runtime.GOOS == "windows" {
		flags.RefreshFilename = c.String("refresh-filename")
		flags.FlushFilename = c.String("flush-filename")
	}

	flags.PartSizes = parsePartSizes(c.String("part-sizes"))

	if flags.ClusterMode {
		flags.ClusterMe = parseNode(c.String("cluster-me"))

		for _, peer := range c.StringSlice("cluster-peer") {
			flags.ClusterPeers = append(flags.ClusterPeers, parseNode(peer))
		}
	}

	// S3 by default, if not initialized in api/api.go
	if flags.Backend == nil {
		flags.Backend = (&S3Config{}).Init()
		config, _ := flags.Backend.(*S3Config)
		config.Region = c.String("region")
		config.RegionSet = c.IsSet("region")
		config.ProjectId = c.String("project-id")
		config.RequesterPays = c.Bool("requester-pays")
		config.StorageClass = c.String("storage-class")
		config.ColdMinSize = c.Uint64("cold-min-size")
		config.Profile = c.String("profile")
		config.SharedConfig = c.StringSlice("shared-config")
		config.UseSSE = c.Bool("sse")
		config.UseKMS = c.IsSet("sse-kms")
		config.KMSKeyID = c.String("sse-kms")
		config.SseC = c.String("sse-c")
		config.ACL = c.String("acl")
		config.Subdomain = c.Bool("subdomain")
		config.NoChecksum = c.Bool("no-checksum")
		config.UseIAM = c.Bool("iam")
		config.IAMHeader = c.String("iam-header")
		config.IAMFlavor = c.String("iam-flavor")
		config.IAMUrl = c.String("iam-url")
		config.MultipartAge = c.Duration("multipart-age")
		if config.IAMFlavor != "gcp" && config.IAMFlavor != "imdsv1" {
			panic("Unknown --iam-flavor: " + config.IAMFlavor)
		}
		listType := c.String("list-type")
		isYandex := strings.Contains(flags.Endpoint, "yandex")
		if isYandex && !c.IsSet("no-specials") {
			flags.EnableSpecials = true
		}
		if listType == "" {
			if isYandex {
				listType = "ext-v1"
			} else {
				listType = "1"
			}
		}
		config.ListV1Ext = listType == "ext-v1"
		config.ListV2 = listType == "2"

		config.MultipartCopyThreshold = uint64(c.Int("multipart-copy-threshold")) * 1024 * 1024

		config.NoExpireMultipart = c.Bool("no-expire-multipart")
		config.NoDetect = c.Bool("no-detect")

		config.SDKMaxRetries = c.Int("sdk-max-retries")
		config.SDKMinRetryDelay = c.Duration("sdk-min-retry-delay")
		config.SDKMaxRetryDelay = c.Duration("sdk-max-retry-delay")
		config.SDKMinThrottleDelay = c.Duration("sdk-min-throttle-delay")
		config.SDKMaxThrottleDelay = c.Duration("sdk-max-throttle-delay")

		// KMS implies SSE
		if config.UseKMS {
			config.UseSSE = true
		}
	}

	if c.IsSet("no-specials") {
		flags.EnableSpecials = false
	}

	if syscall.Getuid() == 0 && !c.IsSet("setuid") && flags.Uid != 0 {
		flags.Setuid = int(flags.Uid)
	}
	if syscall.Getgid() == 0 && !c.IsSet("setgid") && flags.Gid != 0 {
		flags.Setgid = int(flags.Gid)
	}

	flags.MountPointArg = c.Args()[1]
	flags.MountPoint = flags.MountPointArg
	var err error

	defer func() {
		if err != nil {
			flags.Cleanup()
		}
	}()

	if !flags.ClusterMode && flags.ClusterGrpcReflection {
		return nil
	}

	if flags.ClusterMode != (flags.ClusterMe != nil) {
		return nil
	}

	if flags.ClusterMode != (flags.ClusterPeers != nil) {
		return nil
	}

	return flags
}

func MessageMountFlags(args []string) (ret []string) {
	if len(args) == 5 && args[3] == "-o" {
		// looks like it's coming from fstab!
		mountOptions := ""
		ret = append(ret, args[0])

		for _, p := range strings.Split(args[4], ",") {
			if strings.HasPrefix(p, "-") {
				ret = append(ret, p)
			} else {
				mountOptions += p
				mountOptions += ","
			}
		}

		if len(mountOptions) != 0 {
			// remove trailing ,
			mountOptions = mountOptions[:len(mountOptions)-1]
			ret = append(ret, "-o")
			ret = append(ret, mountOptions)
		}

		ret = append(ret, args[1])
		ret = append(ret, args[2])
	} else {
		return args
	}

	return
}

func DefaultFlags() *FlagStorage {
	uid, gid := MyUserAndGroup()
	return &FlagStorage{
		DirMode:              0755,
		FileMode:             0644,
		CacheFileMode:        0644,
		Uid:                  uint32(uid),
		Gid:                  uint32(gid),
		Setuid:               uid,
		Setgid:               gid,
		Endpoint:             "https://storage.yandexcloud.net",
		Backend:              (&S3Config{}).Init(),
		MemoryLimit:          1000 * 1024 * 1024,
		EntryLimit:           100000,
		GCInterval:           250 * 1024 * 1024,
		MaxFlushers:          16,
		MaxParallelParts:     8,
		MaxParallelCopy:      16,
		ReadAheadKB:          5 * 1024,
		SmallReadCount:       4,
		SmallReadCutoffKB:    128,
		ReadAheadSmallKB:     128,
		LargeReadCutoffKB:    20 * 1024,
		ReadAheadLargeKB:     100 * 1024,
		ReadAheadParallelKB:  20 * 1024,
		ReadMergeKB:          512,
		SinglePartMB:         5,
		MaxMergeCopyMB:       0,
		UidAttr:              "uid",
		GidAttr:              "gid",
		FileModeAttr:         "mode",
		RdevAttr:             "rdev",
		MtimeAttr:            "mtime",
		HashAttr:             "--content-sha256",
		HashTimeout:          60 * time.Second,
		MinFileSizeForHashKB: 1024 * 10,
		SymlinkAttr:          "--symlink-target",
		SymlinkZeroed:        false,
		RefreshAttr:          ".invalidate",
		StatCacheTTL:         30 * time.Second,
		HTTPTimeout:          30 * time.Second,
		RetryInterval:        30 * time.Second,
		MaxDiskCacheFD:       512,
		RefreshFilename:      ".invalidate",
		FlushFilename:        ".fsyncdir",
		PartSizes: []PartSizeConfig{
			{PartSize: 5 * 1024 * 1024, PartCount: 1000},
			{PartSize: 25 * 1024 * 1024, PartCount: 1000},
			{PartSize: 125 * 1024 * 1024, PartCount: 8000},
		},
		ExternalCacheClient:         nil,
		StagedWriteModeEnabled:      false,
		StagedWritePath:             "",
		StagedWriteDebounce:         30 * time.Second,
		StagedWriteFlushSize:        16 * 1024 * 1024,
		StagedWriteFlushInterval:    5 * time.Second,
		StagedWriteFlushConcurrency: 8,
		EventCallback:               nil,
	}
}
