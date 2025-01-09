#!/bin/bash

#set -o xtrace
set -o errexit

T=
if [ $# == 1 ]; then
    T="-check.f $1"
fi

. `dirname $0`/run-proxy.sh

# run test in `go test` local mode so streaming output works
cd core
go test -v -timeout $TIMEOUT -check.vv $T
exit $?
