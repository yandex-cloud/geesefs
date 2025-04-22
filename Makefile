export CGO_ENABLED=0
tag := latest

run-test: s3proxy.jar
	./test/run-tests.sh

run-xfstests: s3proxy.jar xfstests
	./test/run-xfstests.sh

xfstests:
	git clone --depth=1 https://github.com/kdave/xfstests
	cd xfstests && patch -p1 -l < ../test/xfstests.diff

s3proxy.jar:
	wget https://github.com/gaul/s3proxy/releases/download/s3proxy-1.8.0/s3proxy -O s3proxy.jar

get-deps: s3proxy.jar
	go get -t ./...

build:
	go build -ldflags "-X main.Version=`git rev-parse HEAD`"

build-docker:
	docker build . --target build -f ./Dockerfile -t localhost:5001/geesefs:$(tag)
	docker push localhost:5001/geesefs:$(tag)

start:
	cd hack && okteto up -f okteto.yaml

install:
	go install -ldflags "-X main.Version=`git rev-parse HEAD`"


.PHONY: protoc
protoc:
	protoc --go_out=. --experimental_allow_proto3_optional --go_opt=paths=source_relative --go-grpc_out=. --go-grpc_opt=paths=source_relative core/pb/*.proto