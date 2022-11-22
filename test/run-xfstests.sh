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

cat >xfstests/local.config <<EOF
FSTYP=fuse.geesefs
TEST_DIR=/tmp/geesefs
TEST_DEV=testbucket:
TEST_FS_MOUNT_OPTS="-o allow_other -o--retry-interval=5s -o--log-file=/tmp/geesefs.log -o--endpoint=http://localhost:$PROXY_PORT"
MOUNT_OPTIONS="-o allow_other -o--retry-interval=5s -o--log-file=/tmp/geesefs.log -o--endpoint=http://localhost:$PROXY_PORT"
SCRATCH_DEV=testbucket2:
SCRATCH_MNT=/tmp/geesefs2
UMOUNT_PROG=$(pwd)/xfstests/sync_unmount.py
EOF

cp `dirname $0`/sync_unmount.py xfstests/

cd xfstests
sudo apt-get -y install xfslibs-dev uuid-dev libtool-bin \
	e2fsprogs automake gcc libuuid1 quota attr make \
	libacl1-dev libaio-dev xfsprogs libgdbm-dev gawk fio dbench \
	uuid-runtime python3 sqlite3 libcap-dev
make -j8
sudo -E ./check generic/001 generic/005 generic/006 generic/007 generic/011 generic/013 generic/014
