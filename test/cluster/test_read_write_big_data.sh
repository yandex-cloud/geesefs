#!/bin/bash

# Test read and write BIG data from different node in random order to check consistency

. common.sh

_s3_setup() {
  :
}

_cluster_setup() {
  mkdir -p "$TEST_ARTIFACTS/test_read_write_big_data"
  
  MNT1=$(mktemp -d)
  _mount "$MNT1" --debug_fuse --debug_grpc --log-file="$TEST_ARTIFACTS/test_read_write_big_data/log1" --pprof=6060 --cluster-me=1:localhost:1337 --cluster-peer=1:localhost:1337 --cluster-peer=2:localhost:1338 --cluster-peer=3:localhost:1339

  MNT2=$(mktemp -d)
  _mount "$MNT2" --debug_fuse --debug_grpc --log-file="$TEST_ARTIFACTS/test_read_write_big_data/log2" --pprof=6070 --cluster-me=2:localhost:1338 --cluster-peer=1:localhost:1337 --cluster-peer=2:localhost:1338 --cluster-peer=3:localhost:1339

  MNT3=$(mktemp -d)
  _mount "$MNT3" --debug_fuse --debug_grpc --log-file="$TEST_ARTIFACTS/test_read_write_big_data/log3" --pprof=6080 --cluster-me=3:localhost:1339 --cluster-peer=1:localhost:1337 --cluster-peer=2:localhost:1338 --cluster-peer=3:localhost:1339
}

_cleanup() {
  _umount "$MNT3"
  _umount "$MNT2"
  _umount "$MNT1"
}

TMP=$(mktemp)

_test() {
  for I in {0..5}; do
      echo "=== Iteration $I"
      dd if=/dev/urandom of="$TMP" bs=20M count=5
      case "$((RANDOM%3))" in
      0)
        dd if="$TMP" of="$MNT1/big_file.txt" bs=20M count=5
        ;;
      1)
        dd if="$TMP" of="$MNT2/big_file.txt" bs=20M count=5
        ;;
      2)
        dd if="$TMP" of="$MNT3/big_file.txt" bs=20M count=5
        ;;
      esac
      diff "$TMP" "$MNT1/big_file.txt"
      diff "$TMP" "$MNT2/big_file.txt"
      diff "$TMP" "$MNT3/big_file.txt"
  done
}

_s3_validate() {
  diff <(_s3cmd get s3://test/big_file.txt -) "$TMP"
}

_check