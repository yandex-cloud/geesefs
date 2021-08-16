#!/bin/bash

#set -o xtrace
set -o errexit

cd $(dirname $0)/..

export CLOUD=${CLOUD:-s3}
PROXY_BIN=$PROXY_BIN
PROXY_PID=$PROXY_PID
PROXY_PORT=${PROXY_PORT:-8080}
TIMEOUT=${TIMEOUT:-10m}

T=
if [ $# == 1 ]; then
    T="-check.f $1"
fi

trap 'kill -9 $PROXY_PID' EXIT

if [ $CLOUD == "s3" ]; then
    mkdir -p /tmp/s3proxy
    sed 's/$PORT/'$PROXY_PORT'/' < test/s3proxy.properties > test/s3proxy_test.properties
    PROXY_BIN="java -jar s3proxy.jar --properties test/s3proxy_test.properties"
    export AWS_ACCESS_KEY_ID=foo
    export AWS_SECRET_ACCESS_KEY=bar
    export ENDPOINT=http://localhost:$PROXY_PORT
elif [ $CLOUD == "azblob" ]; then
    export AZURE_STORAGE_ACCOUNT=${AZURE_STORAGE_ACCOUNT:-devstoreaccount1}
    export AZURE_STORAGE_KEY=${AZURE_STORAGE_KEY:-Eby8vdM02xNOcqFlqUwJPLlmEtlCDXJ1OUzFT50uSRZ6IFsuFq2UVErCz4I6tq/K1SZFPTOtr/KBHBeksoGMGw==}
    export ENDPOINT=http://127.0.0.1:$PORT/$AZURE_STORAGE_ACCOUNT
    if [ ${AZURE_STORAGE_ACCOUNT} == "devstoreaccount1" ]; then
        if ! which azurite >/dev/null; then
            echo "Azurite missing, run:" >&1
            echo "npm install -g azurite" >&1
            exit 1
        fi
        rm -Rf /tmp/azblob
        mkdir -p /tmp/azblob
        PROXY_BIN="azurite-blob -l /tmp/azblob --blobPort $PORT -s"
    fi
fi

if [ "$PROXY_BIN" != "" ]; then
    stdbuf -oL -eL $PROXY_BIN &
    PROXY_PID=$!
    export EMULATOR=1
elif [ "$TIMEOUT" == "10m" ]; then
    # higher timeout for testing to real cloud
    TIMEOUT=45m
fi

# run test in `go test` local mode so streaming output works
cd internal
go test -v -timeout $TIMEOUT -check.vv $T
exit $?
