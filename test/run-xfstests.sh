#!/bin/bash

set -e

TEST_DIR=`dirname $0`
SYNC_UNMOUNT="$(cd "$TEST_DIR" && pwd)/sync_unmount.py"

log() {
	echo "[xfstests $(date '+%H:%M:%S')] $*"
}

cleanup_fuse() {
	for spec in testbucket testbucket2; do
		python3 "$SYNC_UNMOUNT" "$spec" 2>/dev/null || true
	done
}

. "$TEST_DIR/run-proxy.sh"
trap 'cleanup_fuse; [ -n "$PROXY_PID" ] && kill -9 "$PROXY_PID" 2>/dev/null || true' EXIT

log "Starting xfstests setup"
mkdir -p /tmp/geesefs /tmp/geesefs2

sleep 5

[ -e /usr/bin/geesefs ] || sudo ln -s `pwd`/geesefs /usr/bin/geesefs

log "Installing s3cmd"
sudo apt-get update
sudo apt-get -y install --no-install-recommends s3cmd

log "Creating test buckets"
s3cmd --signature-v2 --no-ssl --host-bucket= --access_key=foo --secret_key=bar --host=http://localhost:$PROXY_PORT mb s3://testbucket
s3cmd --signature-v2 --no-ssl --host-bucket= --access_key=foo --secret_key=bar --host=http://localhost:$PROXY_PORT mb s3://testbucket2

sed 's/\/home\/geesefs\/xfstests/$(pwd)/' <test/xfstests.config >xfstests/local.config
cp "$SYNC_UNMOUNT" xfstests/
chmod +x xfstests/sync_unmount.py

cd xfstests
log "Installing xfstests build dependencies"
sudo apt-get -y install --no-install-recommends xfslibs-dev uuid-dev libtool-bin \
	e2fsprogs automake gcc libuuid1 quota attr make \
	libacl1-dev libaio-dev xfsprogs libgdbm-dev gawk fio dbench \
	uuid-runtime python3 sqlite3 libcap-dev

log "Building xfstests"
make -j8

log "Running xfstests check"
ret=0
timeout 10m sudo -E ./check -fuse generic/001 generic/005 generic/006 generic/007 generic/011 generic/013 generic/014 || ret=$?
if [ "$ret" -eq 124 ]; then
	log "Check timed out after 10 minutes"
elif [ "$ret" -ne 0 ]; then
	log "Check failed with exit code $ret"
else
	log "Check passed"
fi

log "Done, exit code $ret"
exit $ret
