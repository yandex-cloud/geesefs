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

package internal

import (
	. "github.com/yandex-cloud/geesefs/api/common"

	"io"
	"os"
	"strconv"
	"strings"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/urfave/cli"
)

const GEESEFS_VERSION = "0.31.5"

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

func init() {
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
   {{end}}{{end}}{{if .Copyright }}
COPYRIGHT:
   {{.Copyright}}
   {{end}}
`
}

var VersionHash string

func NewApp() (app *cli.App) {
	uid, gid := MyUserAndGroup()

	s3Default := (&S3Config{}).Init()

	fsFlags := []cli.Flag{
		/////////////////////////
		// File system
		/////////////////////////

		cli.StringFlag{
			Name:  "provider",
			Value: "unknow",
			Usage: "The specific provider of the object storage service.",
		},

		cli.Uint64Flag{
			Name:  "capacity",
			Value: 1 * 1024 * 1024 * 1024 * 1024 * 1024,
			Usage: "Limit the maximum capacity quota of mounted files.",
		},

		cli.Uint64Flag{
			Name:  "disk-usage-interval",
			Value: 60,
			Usage: "Time interval for statistics of disk usage, in seconds.",
		},

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

		cli.BoolFlag{
			Name:  "iam",
			Usage: "Try to authenticate automatically using VM metadata service (Yandex Cloud / IMDSv1)",
		},

		cli.StringFlag{
			Name:  "iam-header",
			Value: "X-YaCloud-SubjectToken",
			Usage: "The header to use for authenticating with IAM token",
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
			Value: "ext-v1",
		},

		cli.StringFlag{
			Name:  "multipart-age",
			Usage: "Multipart uploads older than this value will be deleted on start (default: 48 hours)",
			Value: "48h",
		},

		cli.IntFlag{
			Name:  "multipart-copy-threshold",
			Usage: "Threshold for switching from single-part to multipart object copy in MB. Maximum for AWS S3 is 5 GB (default: 128 MB)",
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
			Usage: "How much parallel requests should be used for flushing changes to server (default: 16)",
		},

		cli.IntFlag{
			Name:  "max-parallel-parts",
			Value: 8,
			Usage: "How much parallel requests out of the total number can be used for large part uploads." +
				" Large parts take more bandwidth so they usually require less parallelism (default: 8)",
		},

		cli.IntFlag{
			Name:  "max-parallel-copy",
			Value: 16,
			Usage: "How much parallel unmodified part copy requests should be used." +
				" This limit is separate from max-flushers (default: 16)",
		},

		cli.IntFlag{
			Name:  "read-ahead",
			Value: 5 * 1024,
			Usage: "How much data in KB should be pre-loaded with every read by default (default: 5 MB)",
		},

		cli.IntFlag{
			Name:  "small-read-count",
			Value: 4,
			Usage: "Number of last reads within a single file handle to be checked for being random",
		},

		cli.IntFlag{
			Name:  "small-read-cutoff",
			Value: 128,
			Usage: "Maximum average size of last reads in KB to trigger \"small\" readahead (default: 128 KB)",
		},

		cli.IntFlag{
			Name:  "read-ahead-small",
			Value: 128,
			Usage: "Smaller readahead size in KB to be used when small random reads are detected (default: 128 KB)",
		},

		cli.IntFlag{
			Name:  "large-read-cutoff",
			Value: 20 * 1024,
			Usage: "Amount of linear read in KB after which the \"large\" readahead should be triggered (default: 20 MB)",
		},

		cli.IntFlag{
			Name:  "read-ahead-large",
			Value: 100 * 1024,
			Usage: "Larger readahead size in KB to be used when long linear reads are detected (default: 100 MB)",
		},

		cli.IntFlag{
			Name:  "read-ahead-parallel",
			Value: 20 * 1024,
			Usage: "Larger readahead will be triggered in parallel chunks of this size in KB (default: 20 MB)",
		},

		cli.IntFlag{
			Name:  "read-merge",
			Value: 512,
			Usage: "Two HTTP requests required to satisfy a read will be merged into one" +
				" if they're at most this number of KB away (default: 512)",
		},

		cli.IntFlag{
			Name:  "single-part",
			Value: 5,
			Usage: "Maximum size of an object in MB to upload it as a single part." +
				" Can't be less than 5 MB (default: 5 MB)",
		},

		cli.StringFlag{
			Name:  "part-sizes",
			Value: "5:1000,25:1000,125",
			Usage: "Part sizes in MB. Total part count is always 10000 in S3." +
				" Default is 1000 5 MB parts, then 1000 25 MB parts" +
				" and then 125 MB for the rest of parts",
		},

		cli.IntFlag{
			Name:  "max-merge-copy",
			Value: 0,
			Usage: "If non-zero, allow to compose larger parts up to this number of megabytes" +
				" in size from existing unchanged parts when doing server-side part copy." +
				" Must be left at 0 for Yandex S3 (default: 0)",
		},

		cli.BoolFlag{
			Name:  "ignore-fsync",
			Usage: "Do not wait until changes are persisted to the server on fsync() call (default: off)",
		},

		cli.StringFlag{
			Name:  "symlink-attr",
			Value: "--symlink-target",
			Usage: "Symbolic link target metadata attribute (default: --symlink-target)",
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
			Usage: "Retry unsuccessful flushes after this amount of time",
		},

		cli.IntFlag{
			Name:  "cache-popular-threshold",
			Value: 3,
			Usage: "Value of the read counter after which a cached file is started being treated as 'popular'",
		},

		cli.IntFlag{
			Name:  "cache-max-hits",
			Value: 6,
			Usage: "Maximum value of the cache read counter for a file",
		},

		cli.IntFlag{
			Name:  "cache-age-interval",
			Value: 4096,
			Usage: "Number of reads after which read counters are decremented for all files",
		},

		cli.IntFlag{
			Name:  "cache-age-decrement",
			Value: 1,
			Usage: "Decrement amount",
		},

		cli.IntFlag{
			Name:  "cache-to-disk-hits",
			Value: 2,
			Usage: "Minimum value of the read counter to cache file on disk",
		},

		cli.IntFlag{
			Name:  "max-disk-cache-fd",
			Value: 512,
			Usage: "Simultaneously opened cache file descriptor limit",
		},
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

		cli.BoolFlag{
			Name:  "f",
			Usage: "Run geesefs in foreground.",
		},

		cli.StringFlag{
			Name:  "log-file",
			Usage: "Redirect logs to file, 'stderr' (default for foreground) or 'syslog' (default for background).",
			Value: "",
		},
	}

	app = &cli.App{
		Name:     "geesefs",
		Version:  GEESEFS_VERSION,
		Usage:    "Mount an S3 bucket locally",
		HideHelp: true,
		Writer:   os.Stderr,
		Flags: append(append(append(append([]cli.Flag{
			cli.BoolFlag{
				Name:  "help, h",
				Usage: "Print this help text and exit successfully.",
			},
		}, fsFlags...), s3Flags...), tuningFlags...), debugFlags...),
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

func parseOptions(m map[string]string, s string) {
	// NOTE(jacobsa): The man pages don't define how escaping works, and as far
	// as I can tell there is no way to properly escape or quote a comma in the
	// options list for an fstab entry. So put our fingers in our ears and hope
	// that nobody needs a comma.
	for _, p := range strings.Split(s, ",") {
		var name string
		var value string

		// Split on the first equals sign.
		if equalsIndex := strings.IndexByte(p, '='); equalsIndex != -1 {
			name = p[:equalsIndex]
			value = p[equalsIndex+1:]
		} else {
			name = p
		}

		m[name] = value
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

// PopulateFlags adds the flags accepted by run to the supplied flag set, returning the
// variables into which the flags will parse.
func PopulateFlags(c *cli.Context) (ret *FlagStorage) {
	singlePart := c.Int("single-part")
	if singlePart < 5 {
		singlePart = 5
	}

	flags := &FlagStorage{
		// File system
		MountOptions: make(map[string]string),
		DirMode:      os.FileMode(c.Int("dir-mode")),
		FileMode:     os.FileMode(c.Int("file-mode")),
		Uid:          uint32(c.Int("uid")),
		Gid:          uint32(c.Int("gid")),

		// Tuning,
		MemoryLimit:           uint64(1024 * 1024 * c.Int("memory-limit")),
		GCInterval:            uint64(1024 * 1024 * c.Int("gc-interval")),
		Cheap:                 c.Bool("cheap"),
		ExplicitDir:           c.Bool("no-implicit-dir"),
		NoDirObject:           c.Bool("no-dir-object"),
		MaxFlushers:           int64(c.Int("max-flushers")),
		MaxParallelParts:      c.Int("max-parallel-parts"),
		MaxParallelCopy:       c.Int("max-parallel-copy"),
		StatCacheTTL:          c.Duration("stat-cache-ttl"),
		HTTPTimeout:           c.Duration("http-timeout"),
		RetryInterval:         c.Duration("retry-interval"),
		ReadAheadKB:           uint64(c.Int("read-ahead")),
		SmallReadCount:        uint64(c.Int("small-read-count")),
		SmallReadCutoffKB:     uint64(c.Int("small-read-cutoff")),
		ReadAheadSmallKB:      uint64(c.Int("read-ahead-small")),
		LargeReadCutoffKB:     uint64(c.Int("large-read-cutoff")),
		ReadAheadLargeKB:      uint64(c.Int("read-ahead-large")),
		ReadAheadParallelKB:   uint64(c.Int("read-ahead-parallel")),
		ReadMergeKB:           uint64(c.Int("read-merge")),
		SinglePartMB:          uint64(singlePart),
		MaxMergeCopyMB:        uint64(c.Int("max-merge-copy")),
		IgnoreFsync:           c.Bool("ignore-fsync"),
		SymlinkAttr:           c.String("symlink-attr"),
		CachePopularThreshold: int64(c.Int("cache-popular-threshold")),
		CacheMaxHits:          int64(c.Int("cache-max-hits")),
		CacheAgeInterval:      int64(c.Int("cache-age-interval")),
		CacheAgeDecrement:     int64(c.Int("cache-age-decrement")),
		CacheToDiskHits:       int64(c.Int("cache-to-disk-hits")),
		CachePath:             c.String("cache"),
		MaxDiskCacheFD:        int64(c.Int("max-disk-cache-fd")),
		CacheFileMode:         os.FileMode(c.Int("cache-file-mode")),

		// Common Backend Config
		Provider:          c.String("provider"),
		Capacity:          c.Uint64("capacity"),
		DiskUsageInterval: c.Uint64("disk-usage-interval"),
		Endpoint:          c.String("endpoint"),
		UseContentType:    c.Bool("use-content-type"),

		// Debugging,
		DebugMain:  c.Bool("debug"),
		DebugFuse:  c.Bool("debug_fuse"),
		DebugS3:    c.Bool("debug_s3"),
		Foreground: c.Bool("f"),
		LogFile:    c.String("log-file"),
	}

	flags.PartSizes = parsePartSizes(c.String("part-sizes"))

	// S3 by default, if not initialized in api/api.go
	if flags.Backend == nil {
		flags.Backend = (&S3Config{}).Init()
		config, _ := flags.Backend.(*S3Config)
		config.Region = c.String("region")
		config.RegionSet = c.IsSet("region")
		config.RequesterPays = c.Bool("requester-pays")
		config.StorageClass = c.String("storage-class")
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
		config.MultipartAge = c.Duration("multipart-age")
		listType := c.String("list-type")
		config.ListV1Ext = listType == "ext-v1"
		config.ListV2 = listType == "2"

		config.MultipartCopyThreshold = uint64(c.Int("multipart-copy-threshold")) * 1024 * 1024

		// KMS implies SSE
		if config.UseKMS {
			config.UseSSE = true
		}
	}

	// Handle the repeated "-o" flag.
	for _, o := range c.StringSlice("o") {
		parseOptions(flags.MountOptions, o)
	}

	flags.MountPointArg = c.Args()[1]
	flags.MountPoint = flags.MountPointArg
	var err error

	defer func() {
		if err != nil {
			flags.Cleanup()
		}
	}()

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
