module github.com/kahing/goofys

go 1.14

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
	github.com/detailyang/go-fallocate v0.0.0-20180908115635-432fa640bd2e
	github.com/google/uuid v1.1.2
	github.com/gopherjs/gopherjs v0.0.0-20210202160940-bed99a852dfe // indirect
	github.com/jacobsa/fuse v0.0.0-20201216155545-e0296dec955f
	github.com/jacobsa/oglematchers v0.0.0-20150720000706-141901ea67cd
	github.com/jacobsa/oglemock v0.0.0-20150831005832-e94d794d06ff // indirect
	github.com/jacobsa/ogletest v0.0.0-20170503003838-80d50a735a11
	github.com/jacobsa/reqtrace v0.0.0-20150505043853-245c9e0234cb // indirect
	github.com/jacobsa/syncutil v0.0.0-20180201203307-228ac8e5a6c3
	github.com/jacobsa/timeutil v0.0.0-20170205232429-577e5acbbcf6
	github.com/jtolds/gls v4.2.0+incompatible // indirect
	github.com/kardianos/osext v0.0.0-20190222173326-2bc1f35cddc0
	github.com/kr/pretty v0.1.1-0.20190720101428-71e7e4993750 // indirect
	github.com/kylelemons/godebug v1.1.0
	github.com/mattn/go-ieproxy v0.0.0-20190805055040-f9202b1cfdeb // indirect
	github.com/mitchellh/go-homedir v1.1.0
	github.com/satori/go.uuid v1.2.1-0.20181028125025-b2ce2384e17b
	github.com/sevlyar/go-daemon v0.1.5
	github.com/shirou/gopsutil v0.0.0-20190731134726-d80c43f9c984
	github.com/sirupsen/logrus v1.8.1
	github.com/smartystreets/assertions v0.0.0-20160201214316-443d812296a8 // indirect
	github.com/smartystreets/goconvey v1.6.1-0.20160119221636-995f5b2e021c // indirect
	github.com/urfave/cli v1.21.1-0.20190807111034-521735b7608a
	golang.org/x/sys v0.0.0-20210616094352-59db8d763f22
	google.golang.org/api v0.49.0
	gopkg.in/check.v1 v1.0.0-20180628173108-788fd7840127
	gopkg.in/ini.v1 v1.46.0
)

replace github.com/jacobsa/fuse => github.com/vitalif/fusego v0.0.0-20210720164351-7e700197681e

replace github.com/aws/aws-sdk-go => ./s3ext
