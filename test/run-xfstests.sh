#!/bin/bash

set -e

. `dirname $0`/run-proxy.sh

mkdir -p /tmp/geesefs
mkdir -p /tmp/geesefs2

sleep 5

[ -e /usr/bin/geesefs ] || sudo ln -s `pwd`/geesefs /usr/bin/geesefs

sudo apt-get update
sudo apt-get -y install s3cmd

s3cmd --signature-v2 --no-ssl --host-bucket= --access_key=foo --secret_key=bar --host=http://localhost:$PROXY_PORT mb s3://testbucket
s3cmd --signature-v2 --no-ssl --host-bucket= --access_key=foo --secret_key=bar --host=http://localhost:$PROXY_PORT mb s3://testbucket2

sed 's/\/home\/geesefs\/xfstests/$(pwd)/' <test/xfstests.config >xfstests/local.config

cp test/sync_unmount.py xfstests/

cd xfstests
sudo apt-get -y install xfslibs-dev uuid-dev libtool-bin \
	e2fsprogs automake gcc libuuid1 quota attr make \
	libacl1-dev libaio-dev xfsprogs libgdbm-dev gawk fio dbench \
	uuid-runtime python3 sqlite3 libcap-dev
make -j8
sudo -E ./check -fuse generic/001 generic/005 generic/006 generic/007 generic/011 generic/013 generic/014
