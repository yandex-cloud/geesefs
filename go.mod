module github.com/yandex-cloud/geesefs

go 1.16

require (
	cloud.google.com/go/storage v1.16.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-sdk-for-go v32.1.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.7.1-0.20190724222048-33c102d4ffd2
	github.com/Azure/go-autorest/autorest v0.11.18
	github.com/Azure/go-autorest/autorest/adal v0.9.13
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.7
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.2
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/aws/aws-sdk-go v1.38.7
	github.com/google/btree v1.0.0
	github.com/google/uuid v1.1.2
	github.com/jacobsa/fuse v0.0.0-20210818065549-10d864429bf7
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
	github.com/kr/pretty v0.1.1-0.20190720101428-71e7e4993750 // indirect
	github.com/mattn/go-ieproxy v0.0.0-20190805055040-f9202b1cfdeb // indirect
	github.com/minio/madmin-go v1.4.6
	github.com/mitchellh/go-homedir v1.1.0
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/sevlyar/go-daemon v0.1.5
	github.com/shirou/gopsutil v0.0.0-20190731134726-d80c43f9c984
	github.com/sirupsen/logrus v1.8.1
	github.com/urfave/cli v1.21.1-0.20190807111034-521735b7608a
	golang.org/x/sys v0.0.0-20220520151302-bc2c85ada10a
	google.golang.org/api v0.49.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127
	gopkg.in/ini.v1 v1.62.0
)

replace github.com/aws/aws-sdk-go => ./s3ext

replace github.com/jacobsa/fuse => github.com/vitalif/fusego v0.0.0-20220702223751-e304003d79bf
