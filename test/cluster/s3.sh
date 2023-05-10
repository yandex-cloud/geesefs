#!/bin/bash

echo "=== Use Yandex S3"

export ENDPOINT=https://storage.yandexcloud.net

TEST_ARTIFACTS="${TEST_ARTIFACTS:-executions/$(date +"%Y-%m-%d-%H-%M-%S")}"
mkdir -p "$TEST_ARTIFACTS"

_kill_s3proxy() {
    :
}

_s3cmd() {
  s3cmd \
  --signature-v2 \
  --host-bucket="" \
  --host="https://storage.yandexcloud.net" \
  "$@"
}