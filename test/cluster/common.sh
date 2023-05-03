#!/bin/bash

. mount.sh

if [[ "$AWS_ACCESS_KEY_ID" != "" && "$AWS_SECRET_ACCESS_KEY" != "" ]]; then
  . s3.sh
else
  . proxy.sh
fi


TEST_ARTIFACTS="${TEST_ARTIFACTS:-executions/$(date +"%Y-%m-%d-%H-%M-%S")}"
mkdir -p "$TEST_ARTIFACTS"

_check_nolog() {
  echo "=== S3 bucket setup"
  _s3_setup

  echo "=== Cluster setup"
  _cluster_setup

  echo "=== Test"
  (set -ex; _test)
  EXIT_CODE=$?
  echo "=== Test, exit code = $EXIT_CODE"

  _cleanup

  echo "=== Validate S3"
  (set -ex; _s3_validate)
  S3_VALIDATE_EXIT_CODE=$?
  echo "=== Validate S3, exit code = $S3_VALIDATE_EXIT_CODE"

  _kill_s3proxy

  if [[ $EXIT_CODE == 1 ]]; then
    exit 1
  fi
  if [[ $S3_VALIDATE_EXIT_CODE == 1 ]]; then
    exit 1
  fi
  exit 0
}

_check() {
  _check_nolog 2>&1 | tee -a "$TEST_ARTIFACTS/test_log"
}