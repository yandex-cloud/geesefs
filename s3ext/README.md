[![Apache V2 License](https://img.shields.io/badge/license-Apache%20V2-blue.svg)](https://github.com/aws/aws-sdk-go/blob/main/LICENSE.txt)

This is S3 SDK taken from aws-sdk-go version 1.38.7 with Yandex-specific additions

aws-sdk-go is the official AWS SDK for the Go programming language.

# Building API schema

```
go build -tags=codegen private/model/cli/gen-api/main.go -o gen-api
./gen-api -path service models/apis/s3/2006-03-01/api-2.json
```
