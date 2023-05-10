#!/bin/bash

PROXY_PORT=${PROXY_PORT:-8080}

cat <<EOF > s3proxy.properties
s3proxy.endpoint=http://localhost:$PROXY_PORT
s3proxy.authorization=aws-v2
s3proxy.identity=foo
s3proxy.credential=bar
jclouds.provider=transient
jclouds.identity=foo
jclouds.credential=bar
jclouds.regions=us-west-2
EOF

PROXY_BIN="java -DLOG_LEVEL=trace -Djclouds.wire=debug -jar s3proxy.jar --properties s3proxy.properties"
export S3CMD_ARGS="--access_key=foo --secret_key=bar"
export AWS_ACCESS_KEY_ID=foo
export AWS_SECRET_ACCESS_KEY=bar
export ENDPOINT=http://localhost:$PROXY_PORT

TEST_ARTIFACTS="${TEST_ARTIFACTS:-executions/$(date +"%Y-%m-%d-%H-%M-%S")}"
mkdir -p "$TEST_ARTIFACTS"

echo "=== Start s3proxy on $ENDPOINT"
$PROXY_BIN > "$TEST_ARTIFACTS/s3proxy_log" &
PROXY_PID=$!
sleep 15

_kill_s3proxy() {
  echo "=== Kill s3proxy"
  kill -9 $PROXY_PID
}

echo "=== Create s3://test"

_s3cmd() {
  s3cmd \
  $S3CMD_ARGS \
  --signature-v2 \
  --no-ssl \
  --host-bucket="" \
  --host="http://localhost:$PROXY_PORT" \
  "$@"
}

_s3cmd mb s3://test

export BUCKET_NAME="test"
