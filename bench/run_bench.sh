#!/bin/bash

set -o errexit
set -o nounset
set -o pipefail

: ${BUCKET:="geesefs-bench"}
: ${FAST:=""}
: ${CACHE:=""}
: ${ENDPOINT:="http://s3-us-west-2.amazonaws.com/"}
: ${AWS_ACCESS_KEY_ID:=""}
: ${AWS_SECRET_ACCESS_KEY:=""}
: ${PROG:="geesefs"}

if [ $# = 1 ]; then
    t=$1
else
    t=
fi

if [ "$PROG" = "s3fs" ]; then
    export AWSACCESSKEYID=$AWS_ACCESS_KEY_ID
    export AWSSECRETACCESSKEY=$AWS_SECRET_ACCESS_KEY
    OPT="-ourl=$ENDPOINT -ouse_path_request_style -osigv2"
    if [ "$CACHE" != "" ]; then
        OPT="$OPT -ouse_cache=$CACHE"
    fi
    MOUNTER="s3fs -ostat_cache_expire=1 $OPT $BUCKET bench-mnt"
elif [ "$PROG" = "geesefs" ]; then
    OPT="--endpoint $ENDPOINT"
    if [ "$CACHE" != "" ]; then
        OPT="$OPT --cache $CACHE -o allow_other"
    fi
    MOUNTER="geesefs --stat-cache-ttl 1s $OPT $BUCKET bench-mnt"
elif [ "$PROG" = "goofys" ]; then
    OPT="--endpoint $ENDPOINT"
    if [ "$CACHE" != "" ]; then
        OPT="$OPT --cache $CACHE -o allow_other"
    fi
    MOUNTER="goofys --stat-cache-ttl 1s --type-cache-ttl 1s $OPT $BUCKET bench-mnt"
elif [ "$PROG" = "blobfuse" ]; then
    export AZURE_STORAGE_ACCESS_KEY=${AZURE_STORAGE_KEY}
    MOUNTER="blobfuse bench-mnt --container-name=$BUCKET --tmp-path=/tmp/cache"
elif [ "$PROG" = "local" ]; then
    MOUNTER=""
else
    echo "Unknown mounter: $PROG"
    exit 1
fi

dir=$(dirname $0)
mkdir -p bench-mnt
rm -f $dir/bench.$PROG $dir/bench.png $dir/bench-cached.png

iter=10
if [ "$FAST" != "" ]; then
    iter=1
fi

function cleanup {
    kill $(jobs -p) &>/dev/null || true
    fusermount -u bench-mnt || true
    sleep 1
    rmdir bench-mnt
}
trap cleanup EXIT

function mount_and_bench {
    if [ "$MOUNTER" != "" ]; then
        $MOUNTER
        $dir/bench.sh $1 $2
        fusermount -u $1
    else
        $dir/bench.sh $1 $2
    fi
}

if mountpoint -q bench-mnt; then
    echo "bench-mnt is still mounted"
    exit 1
fi

if [ -e $dir/bench.$PROG ]; then
    rm $dir/bench.$PROG
fi

if [ "$t" = "" ]; then
    for tt in create create_parallel io; do
        mount_and_bench bench-mnt $tt |& tee -a $dir/bench.$PROG
        mount_and_bench bench-mnt cleanup |& tee -a $dir/bench.$PROG
    done
    mount_and_bench bench-mnt ls_create
    for i in $(seq 1 $iter); do
        mount_and_bench bench-mnt ls_ls |& tee -a $dir/bench.$PROG
    done
    mount_and_bench bench-mnt ls_rm
    mount_and_bench bench-mnt find_create |& tee -a $dir/bench.$PROG
    mount_and_bench bench-mnt find_find |& tee -a $dir/bench.$PROG
    mount_and_bench bench-mnt cleanup |& tee -a $dir/bench.$PROG
else
    if [ "$t" = "find" ]; then
        mount_and_bench bench-mnt find_create |& tee -a $dir/bench.$PROG
        mount_and_bench bench-mnt find_find |& tee -a $dir/bench.$PROG
        mount_and_bench bench-mnt cleanup |& tee -a $dir/bench.$PROG
    elif [ "$t" = "cleanup" ]; then
        mount_and_bench bench-mnt cleanup |& tee -a $dir/bench.$PROG
    else
        mount_and_bench bench-mnt $t |& tee $dir/bench.$PROG
    fi
fi
