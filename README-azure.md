# Azure Blob Storage

GeeseFS still works with Azure because the support is inherited from Goofys and it
turns out that Azure Blob Storage supports all basic features required for GeeseFS.

The simplest way to mount AzBlob is by using `AZURE_STORAGE_ACCOUNT` and
`AZURE_STORAGE_KEY` environment variables:

```ShellSession
$ AZURE_STORAGE_ACCOUNT=xxx AZURE_STORAGE_KEY=yyy \
    $GOPATH/bin/geesefs wasb://container[:prefix] <mountpoint>
```

You can also use ~/.azure/config:

```ShellSession
$ cat ~/.azure/config
[storage]
account = "myblobstorage"
key = "MY-STORAGE-KEY"
$ $GOPATH/bin/geesefs wasb://container <mountpoint>
$ $GOPATH/bin/geesefs wasb://container:prefix <mountpoint> # if you only want to mount objects under a prefix
```

GeeseFS also accepts full `wasb` URIs and setting the account via `--endpoint`:

```ShellSession
$ $GOPATH/bin/geesefs wasb://container@myaccount.blob.core.windows.net/prefix <mountpoint>
$ $GOPATH/bin/geesefs --endpoint https://myaccount.blob.core.windows.net wasb://container:prefix <mountpoint>
```

Note that if full `wasb` URI is not specified, prefix separator is `:`.

# Azure Data Lake Storage Gen1 and Gen2

ADL v1 and v2 differ from Azure Blob so their support in GeeseFS is broken.
Patches are welcome if you really want to fix it.
