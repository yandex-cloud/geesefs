<img src="doc/geesefs.png" height="64" width="64" align="middle" />
GeeseFS is a high-performance, POSIX-ish S3
([Yandex](https://cloud.yandex.com/en-ru/services/storage), [Amazon](https://aws.amazon.com/s3/))
file system written in Go

# Overview

GeeseFS allows you to mount an S3 bucket as a file system.

FUSE file systems based on S3 typically have performance problems, especially with small files and metadata operations.

GeeseFS attempts to solve these problems by using aggressive parallelism and asynchrony.

# POSIX Compatibility Matrix

|                   | GeeseFS | rclone | Goofys | S3FS | gcsfuse |
| ----------------- | ------- | ------ | ------ | ---- | ------- |
| Read after write  |    +    |    +   |    -   |   +  |    +    |
| Partial writes    |    +    |    +   |    -   |   +  |    +    |
| Truncate          |    +    |    -   |    -   |   +  |    +    |
| chmod/chown       |    -    |    -   |    -   |   +  |    -    |
| fsync             |    +    |    -   |    -   |   +  |    +    |
| Symlinks          |    +    |    -   |    -   |   +  |    +    |
| xattr             |    +    |    -   |    +   |   +  |    -    |
| Directory renames |    +    |    +   |    *   |   +  |    +    |
| readdir & changes |    +    |    +   |    -   |   +  |    +    |

\* Directory renames are allowed in Goofys for directories with no more than 1000 entries and the limit is hardcoded

List of non-POSIX behaviors/limitations for GeeseFS:
* does not store file mode/owner/group, use `--(dir|file)-mode` or `--(uid|gid)` options
* does not support hard links
* does not support locking
* `ctime`, `atime` is always the same as `mtime`

In addition to the items above, the following are supportable but not yet implemented:
* creating files larger than 1TB

# Performance Features

|                                | GeeseFS | rclone | Goofys | S3FS | gcsfuse |
| ------------------------------ | ------- | ------ | ------ | ---- | ------- |
| Parallel readahead             |    +    |    -   |    +   |   +  |    -    |
| Parallel multipart uploads     |    +    |    -   |    +   |   +  |    -    |
| No readahead on random read    |    +    |    -   |    +   |   -  |    +    |
| Server-side copy on append     |    +    |    -   |    -   |   *  |    +    |
| Server-side copy on update     |    +    |    -   |    -   |   *  |    -    |
| xattrs without extra RTT       |    +    |    -   |    -   |   -  |    +    |
| Fast recursive listings        |    +    |    -   |    *   |   -  |    +    |
| Asynchronous write             |    +    |    +   |    -   |   -  |    -    |
| Asynchronous delete            |    +    |    -   |    -   |   -  |    -    |
| Asynchronous rename            |    +    |    -   |    -   |   -  |    -    |
| Disk cache for reads           |  TODO   |    +   |    -   |   +  |    +    |
| Disk cache for writes          |  TODO   |    +   |    -   |   +  |    -    |

\* Recursive listing optimisation in Goofys is buggy and may skip files under certain conditions

\* S3FS uses server-side copy, but it still downloads the whole file to update it. And it's buggy too :-)

# Installation

* Ure-built binaries:
  * [Linux amd64](https://github.com/yandex-cloud/geesefs/releases/latest/download/geesefs-linux-amd64). You may also need to install fuse-utils first.
  * [Mac amd64](https://github.com/yandex-cloud/geesefs/releases/latest/download/geesefs-mac-amd64). You also need `osxfuse` for GeeseFS to work.
* Or build from source with Go 1.13 or later:

```ShellSession
$ go get github.com/yandex-cloud/geesefs
```

# Usage

```ShellSession
$ cat ~/.aws/credentials
[default]
aws_access_key_id = AKID1234567890
aws_secret_access_key = MY-SECRET-KEY
$ $GOPATH/bin/geesefs <bucket> <mountpoint>
$ $GOPATH/bin/geesefs [--endpoint https://...] <bucket:prefix> <mountpoint> # if you only want to mount objects under a prefix
```

You can also supply credentials via the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables.

To mount an S3 bucket on startup, make sure the credential is
configured for `root`, and can add this to `/etc/fstab`:

```
geesefs#bucket   /mnt/mountpoint        fuse     _netdev,allow_other,--file-mode=0666,--dir-mode=0777    0       0
```

See also: [Instruction for Azure Blob Storage](https://github.com/yandex-cloud/geesefs/blob/master/README-azure.md).

# Configuration

There's a lot of tuning you can do. Consult `geesefs -h` to view the list of options.

# License

Licensed under the Apache License, Version 2.0

See `LICENSE` and `AUTHORS`

## Compatibility with S3

geesefs works with:
* Yandex Object Storage (default)
* Amazon S3
* Ceph (and also Ceph-based Digital Ocean Spaces, DreamObjects, gridscale etc)
* Minio
* OpenStack Swift
* Azure Blob Storage (even though it's not S3)

It should also work with any other S3 that implements multipart uploads and
multipart server-side copy (UploadPartCopy).

The following backends are inherited from Goofys code and still exist, but are broken:
* Google Cloud Storage
* Azure Data Lake Gen1
* Azure Data Lake Gen2

# References

* [Yandex Object Storage](https://cloud.yandex.com/en-ru/services/storage)
* [Amazon S3](https://aws.amazon.com/s3/)
* [Amazon SDK for Go](https://github.com/aws/aws-sdk-go)
* Other related fuse filesystems
  * [goofys](https://github.com/kahing/goofys): GeeseFS is a fork of Goofys.
  * [s3fs](https://github.com/s3fs-fuse/s3fs-fuse): another popular filesystem for S3.
  * [gcsfuse](https://github.com/googlecloudplatform/gcsfuse): filesystem for
    [Google Cloud Storage](https://cloud.google.com/storage/). Goofys
    borrowed some skeleton code from this project.
* [S3Proxy](https://github.com/andrewgaul/s3proxy)
* [fuse binding](https://github.com/jacobsa/fuse), also used by `gcsfuse`
