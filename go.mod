module github.com/yandex-cloud/geesefs

go 1.21

toolchain go1.22.2

require (
	cloud.google.com/go/storage v1.16.0
	github.com/Azure/azure-pipeline-go v0.2.2
	github.com/Azure/azure-sdk-for-go v32.1.0+incompatible
	github.com/Azure/azure-storage-blob-go v0.7.1-0.20190724222048-33c102d4ffd2
	github.com/Azure/go-autorest/autorest v0.11.18
	github.com/Azure/go-autorest/autorest/adal v0.9.13
	github.com/Azure/go-autorest/autorest/azure/auth v0.5.7
	github.com/Azure/go-autorest/autorest/azure/cli v0.4.2
	github.com/aws/aws-sdk-go v1.38.7
	github.com/golang/protobuf v1.5.4
	github.com/google/uuid v1.1.2
	github.com/jacobsa/fuse v0.0.0-20230810134708-ab21db1af836
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
	github.com/mitchellh/go-homedir v1.1.0
	github.com/pkg/xattr v0.4.9
	github.com/prometheus/client_golang v1.20.5
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/sevlyar/go-daemon v0.1.6
	github.com/shirou/gopsutil v0.0.0-20190731134726-d80c43f9c984
	github.com/sirupsen/logrus v1.8.1
	github.com/tidwall/btree v1.7.0
	github.com/urfave/cli v1.21.1-0.20190807111034-521735b7608a
	github.com/winfsp/cgofuse v1.5.0
	golang.org/x/sync v0.7.0
	golang.org/x/sys v0.22.0
	google.golang.org/api v0.49.0
	google.golang.org/grpc v1.38.0
	google.golang.org/protobuf v1.35.1
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c
	gopkg.in/ini.v1 v1.46.0
)

require (
	cloud.google.com/go v0.84.0 // indirect
	github.com/Azure/go-autorest v14.2.0+incompatible // indirect
	github.com/Azure/go-autorest/autorest/date v0.3.0 // indirect
	github.com/Azure/go-autorest/autorest/to v0.4.0 // indirect
	github.com/Azure/go-autorest/autorest/validation v0.3.1 // indirect
	github.com/Azure/go-autorest/logger v0.2.1 // indirect
	github.com/Azure/go-autorest/tracing v0.6.0 // indirect
	github.com/StackExchange/wmi v0.0.0-20180116203802-5d049714c4a6 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/dimchansky/utfbom v1.1.1 // indirect
	github.com/form3tech-oss/jwt-go v3.2.2+incompatible // indirect
	github.com/go-ole/go-ole v1.2.1 // indirect
	github.com/golang/groupcache v0.0.0-20200121045136-8c9f03a8e57e // indirect
	github.com/googleapis/gax-go/v2 v2.0.5 // indirect
	github.com/gopherjs/gopherjs v0.0.0-20210202160940-bed99a852dfe // indirect
	github.com/jmespath/go-jmespath v0.4.0 // indirect
	github.com/jstemmer/go-junit-report v0.9.1 // indirect
	github.com/jtolds/gls v4.2.0+incompatible // indirect
	github.com/klauspost/compress v1.17.9 // indirect
	github.com/kr/pretty v0.3.1 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/mattn/go-ieproxy v0.0.0-20190805055040-f9202b1cfdeb // indirect
	github.com/munnerz/goautoneg v0.0.0-20191010083416-a7dc8b61c822 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/prometheus/client_model v0.6.1 // indirect
	github.com/prometheus/common v0.55.0 // indirect
	github.com/prometheus/procfs v0.15.1 // indirect
	github.com/rogpeppe/go-internal v1.10.0 // indirect
	github.com/smartystreets/assertions v0.0.0-20160201214316-443d812296a8 // indirect
	github.com/smartystreets/goconvey v1.6.1-0.20160119221636-995f5b2e021c // indirect
	go.opencensus.io v0.23.0 // indirect
	golang.org/x/crypto v0.24.0 // indirect
	golang.org/x/lint v0.0.0-20210508222113-6edffad5e616 // indirect
	golang.org/x/mod v0.17.0 // indirect
	golang.org/x/net v0.26.0 // indirect
	golang.org/x/oauth2 v0.21.0 // indirect
	golang.org/x/text v0.16.0 // indirect
	golang.org/x/tools v0.21.1-0.20240508182429-e35e4ccd0d2d // indirect
	google.golang.org/appengine v1.6.7 // indirect
	google.golang.org/genproto v0.0.0-20210624174822-c5cf32407d0a // indirect
)

replace github.com/aws/aws-sdk-go => ./s3ext

replace github.com/winfsp/cgofuse => github.com/vitalif/cgofuse v0.0.0-20230609211427-22e8fa44f6b8

replace github.com/jacobsa/fuse => github.com/vitalif/fusego v0.0.0-20241023174951-7a12c251bb93
