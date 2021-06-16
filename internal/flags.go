// Copyright 2015 - 2017 Ka-Hing Cheung
// Copyright 2015 - 2017 Google Inc. All Rights Reserved.
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
	. "github.com/kahing/goofys/api/common"

	"io"
	"os"
	"strings"
	"text/tabwriter"
	"text/template"
	"time"

	"github.com/urfave/cli"
)

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
AWS S3 OPTIONS:
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

	app = &cli.App{
		Name:     "goofys",
		Version:  "0.24.0-" + VersionHash,
		Usage:    "Mount an S3 bucket locally",
		HideHelp: true,
		Writer:   os.Stderr,
		Flags: []cli.Flag{

			cli.BoolFlag{
				Name:  "help, h",
				Usage: "Print this help text and exit successfully.",
			},

			/////////////////////////
			// File system
			/////////////////////////

			cli.StringSliceFlag{
				Name:  "o",
				Usage: "Additional system-specific mount options. Be careful!",
			},

			cli.StringFlag{
				Name: "cache",
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
				Name:  "uid",
				Value: uid,
				Usage: "UID owner of all inodes.",
			},

			cli.IntFlag{
				Name:  "gid",
				Value: gid,
				Usage: "GID owner of all inodes.",
			},

			/////////////////////////
			// S3
			/////////////////////////

			cli.StringFlag{
				Name:  "endpoint",
				Value: "",
				Usage: "The non-AWS endpoint to connect to." +
					" Possible values: http://127.0.0.1:8081/",
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

			/////////////////////////
			// Tuning
			/////////////////////////

			cli.IntFlag{
				Name:  "memory-limit",
				Usage: "Maximum memory in MB to use for data cache (default: use cgroup free memory limit)",
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
				Usage: "How much parallel requests out of the total number can be used for large part uploads."+
					" Large parts take more bandwidth so they usually require less parallelism (default: 8)",
			},

			cli.IntFlag{
				Name:  "max-parallel-copy",
				Value: 16,
				Usage: "How much parallel unmodified part copy requests should be used."+
					" This limit is separate from max-flushers (default: 16)",
			},

			cli.IntFlag{
				Name:  "read-ahead",
				Value: 4096,
				Usage: "How much data in KB should be pre-loaded with every HTTP request (default: 4096)",
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

			cli.IntFlag{
				Name:  "max-merge-copy",
				Value: 0,
				Usage: "If non-zero, allow to compose larger parts up to this number of megabytes" +
					" in size from existing unchanged parts when doing server-side part copy."+
					" Must be left at 0 for Yandex S3 (default: 0)",
			},

			cli.DurationFlag{
				Name:  "stat-cache-ttl",
				Value: time.Minute,
				Usage: "How long to cache StatObject results and inode attributes.",
			},

			cli.DurationFlag{
				Name:  "type-cache-ttl",
				Value: time.Minute,
				Usage: "How long to cache name -> file/dir mappings in directory " +
					"inodes.",
			},

			cli.DurationFlag{
				Name:  "http-timeout",
				Value: 30 * time.Second,
				Usage: "Set the timeout on HTTP requests to S3",
			},

			/////////////////////////
			// Debugging
			/////////////////////////

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
				Usage: "Run goofys in foreground.",
			},
		},
	}

	var funcMap = template.FuncMap{
		"category": filterCategory,
		"join":     strings.Join,
	}

	flagCategories = map[string]string{}

	for _, f := range []string{"region", "sse", "sse-kms", "sse-c", "storage-class", "acl", "requester-pays"} {
		flagCategories[f] = "aws"
	}

	for _, f := range []string{"cheap", "no-implicit-dir", "stat-cache-ttl", "type-cache-ttl", "http-timeout"} {
		flagCategories[f] = "tuning"
	}

	for _, f := range []string{"help, h", "debug_fuse", "debug_s3", "version, v", "f"} {
		flagCategories[f] = "misc"
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

// PopulateFlags adds the flags accepted by run to the supplied flag set, returning the
// variables into which the flags will parse.
func PopulateFlags(c *cli.Context) (ret *FlagStorage) {
	readAhead := c.Int("read-ahead")
	if readAhead < 0 {
		readAhead = 4096
	}

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
		MemoryLimit:  uint64(1024*1024*c.Int("memory-limit")),
		Cheap:        c.Bool("cheap"),
		ExplicitDir:  c.Bool("no-implicit-dir"),
		NoDirObject:  c.Bool("no-dir-object"),
		MaxFlushers:  int64(c.Int("max-flushers")),
		MaxParallelParts: c.Int("max-parallel-parts"),
		MaxParallelCopy: c.Int("max-parallel-copy"),
		StatCacheTTL: c.Duration("stat-cache-ttl"),
		TypeCacheTTL: c.Duration("type-cache-ttl"),
		HTTPTimeout:  c.Duration("http-timeout"),
		ReadAheadKB:  uint64(readAhead),
		ReadMergeKB:  uint64(c.Int("read-merge")),
		SinglePartMB: uint64(singlePart),
		MaxMergeCopyMB: uint64(c.Int("max-merge-copy")),

		// Common Backend Config
		Endpoint:       c.String("endpoint"),
		UseContentType: c.Bool("use-content-type"),

		// Debugging,
		DebugFuse:  c.Bool("debug_fuse"),
		DebugS3:    c.Bool("debug_s3"),
		Foreground: c.Bool("f"),
	}

	// S3
	if c.IsSet("region") || c.IsSet("requester-pays") || c.IsSet("storage-class") ||
		c.IsSet("profile") || c.IsSet("sse") || c.IsSet("sse-kms") ||
		c.IsSet("sse-c") || c.IsSet("acl") || c.IsSet("subdomain") {

		if flags.Backend == nil {
			flags.Backend = (&S3Config{}).Init()
		}
		config, _ := flags.Backend.(*S3Config)

		config.Region = c.String("region")
		config.RegionSet = c.IsSet("region")
		config.RequesterPays = c.Bool("requester-pays")
		config.StorageClass = c.String("storage-class")
		config.Profile = c.String("profile")
		config.UseSSE = c.Bool("sse")
		config.UseKMS = c.IsSet("sse-kms")
		config.KMSKeyID = c.String("sse-kms")
		config.SseC = c.String("sse-c")
		config.ACL = c.String("acl")
		config.Subdomain = c.Bool("subdomain")

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

	if c.IsSet("cache") {
		// FIXME: catfs removed. Implement local cache
	}

	return flags
}

func MassageMountFlags(args []string) (ret []string) {
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
