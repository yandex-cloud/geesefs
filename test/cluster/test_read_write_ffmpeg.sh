#!/bin/bash

# Test start three processes on different nodes conversing the same .mp4 video to three new .mov videos 

. common.sh

sudo apt-get install -y ffmpeg

_s3_setup() {
  :
}

_cluster_setup() {
  mkdir -p "$TEST_ARTIFACTS/test_read_write_ffmpeg"
  touch "$TEST_ARTIFACTS/test_read_write_ffmpeg/log1" "$TEST_ARTIFACTS/test_read_write_ffmpeg/log2" "$TEST_ARTIFACTS/test_read_write_ffmpeg/log3"

  MNT1=$(mktemp -d)
  _mount "$MNT1" --debug_fuse --log-file="$TEST_ARTIFACTS/test_read_write_ffmpeg/log1" --cluster-me=1:localhost:1337 --cluster-peer=1:localhost:1337 --cluster-peer=2:localhost:1338 --cluster-peer=3:localhost:1339

  MNT2=$(mktemp -d)
  _mount "$MNT2" --debug_fuse --log-file="$TEST_ARTIFACTS/test_read_write_ffmpeg/log2" --cluster-me=2:localhost:1338 --cluster-peer=1:localhost:1337 --cluster-peer=2:localhost:1338 --cluster-peer=3:localhost:1339

  MNT3=$(mktemp -d)
  _mount "$MNT3" --debug_fuse --log-file="$TEST_ARTIFACTS/test_read_write_ffmpeg/log3" --cluster-me=3:localhost:1339 --cluster-peer=1:localhost:1337 --cluster-peer=2:localhost:1338 --cluster-peer=3:localhost:1339

  VALID_DIR=$(mktemp -d)
  echo "=== VALID_DIR=$VALID_DIR"
}

_cleanup() {
  _umount "$MNT3"
  _umount "$MNT2"
  _umount "$MNT1"
}

TMP=$(mktemp)

_test() {
  curl "https://storage.yandexcloud.net/udav318-geesefs-test-provision/video_cut.mp4" \
  -L --output "$VALID_DIR/video_cut.mp4"

  curl "https://storage.yandexcloud.net/udav318-geesefs-test-provision/video_cut.mp4" \
  -L --output "$MNT1/video_cut.mp4"

  ffmpeg -i "$VALID_DIR/video_cut.mp4" -f mov "$VALID_DIR/video_cut.mov" 2> "$TEST_ARTIFACTS/test_read_write_ffmpeg/ffmpeg_log_valid" &
  ffmpeg -i "$MNT1/video_cut.mp4" -f mov "$MNT1/video_cut_1.mov" 2> "$TEST_ARTIFACTS/test_read_write_ffmpeg/ffmpeg_log1" &
  ffmpeg -i "$MNT2/video_cut.mp4" -f mov "$MNT2/video_cut_2.mov" 2> "$TEST_ARTIFACTS/test_read_write_ffmpeg/ffmpeg_log2" &
  ffmpeg -i "$MNT3/video_cut.mp4" -f mov "$MNT3/video_cut_3.mov" 2> "$TEST_ARTIFACTS/test_read_write_ffmpeg/ffmpeg_log3" &
  
  wait < <(jobs -p)

  diff "$VALID_DIR/video_cut.mov" "$MNT1/video_cut_1.mov"
  diff "$VALID_DIR/video_cut.mov" "$MNT1/video_cut_2.mov"
  diff "$VALID_DIR/video_cut.mov" "$MNT1/video_cut_3.mov"

  diff "$MNT1/video_cut_1.mov" "$MNT2/video_cut_1.mov"
  diff "$MNT2/video_cut_1.mov" "$MNT3/video_cut_1.mov"

  diff "$MNT1/video_cut_2.mov" "$MNT2/video_cut_2.mov"
  diff "$MNT2/video_cut_2.mov" "$MNT3/video_cut_2.mov"

  diff "$MNT1/video_cut_3.mov" "$MNT2/video_cut_3.mov"
  diff "$MNT2/video_cut_3.mov" "$MNT3/video_cut_3.mov"

  diff "$MNT1/video_cut_1.mov" "$MNT1/video_cut_2.mov"
  diff "$MNT1/video_cut_2.mov" "$MNT3/video_cut_3.mov"

  cp "$MNT1/video_cut_1.mov" "$TMP"
}

_s3_validate() {
  diff "$TMP" <(_s3cmd get s3://test/video_cut_1.mov -)
  diff "$TMP" <(_s3cmd get s3://test/video_cut_2.mov -)
  diff "$TMP" <(_s3cmd get s3://test/video_cut_3.mov -)
}

_check