#!/bin/bash

FAST=$FAST
test=$test
CLEANUP=${CLEANUP:-true}

iter=10
if [ "$FAST" != "" ]; then
    iter=1
fi

set -o errexit
set -o nounset

if [ $# -lt 1 ]; then
    echo "Usage: $0 <dir> [test name]"
    exit 1
fi

mnt=$1
if [ $# -gt 1 ]; then
    t=$2
else
    t=
fi

prefix=$mnt/test_dir

function wait_for_mount {
    for i in $(seq 1 10); do
        if grep -q $mnt /proc/mounts; then
            break
        fi
        sleep 1
    done
    if ! grep -q $mnt /proc/mounts; then
        echo "$mnt not mounted"
        exit 1
    fi
}

mkdir -p "$prefix"
cd "$prefix"

export TIMEFORMAT=%R

function fsync_dir {
    python3 -c "import sys, os; os.fsync(os.open('.', os.O_RDONLY))"
}

function run_test {
    test=$1
    shift
    sleep 2
    echo -n "$test "
    time $test $@
}

function get_howmany {
    if [ $# -ge 1 ]; then
        howmany=$1
    else
        howmany=100
    fi
}

function create_files {
    get_howmany $@

    for i in $(seq 1 $howmany); do
        echo $i > file$i
    done
    fsync_dir
}

function ls_files {
    get_howmany $@
    # people usually use ls in the terminal when color is on
    numfiles=$(ls -1 --color=always | wc -l)
    if [ "$numfiles" -ne "$howmany" ]; then
        echo "$numfiles != $howmany"
        false
    fi
}

function rm_files {
    get_howmany $@

    for i in $(seq 1 $howmany); do
        rm file$i >&/dev/null || true
    done
    fsync_dir
}

function find_files {
    numfiles=$(find . | wc -l)

    if [ "$numfiles" -ne 820 ]; then
        echo "$numfiles != 820"
        rm_tree
        exit 1
    fi
}

function create_tree_parallel {
    (for i in $(seq 1 9); do
        mkdir $i
        for j in $(seq 1 9); do
            mkdir $i/$j

            for k in $(seq 1 9); do
                 touch $i/$j/$k & true
             done
         done
    done
    wait)
    fsync_dir
}

function rm_tree {
    for i in $(seq 1 9); do
        rm -Rf $i
    done
    fsync_dir
}

function create_files_parallel {
    get_howmany $@

    (for i in $(seq 1 $howmany); do
        echo $i > file$i & true
    done
    wait)
    fsync_dir
}

function rm_files_parallel {
    get_howmany $@

    (for i in $(seq 1 $howmany); do
        rm file$i & true
    done
    wait)
    fsync_dir
}

function write_large_file {
    count=1000
    if [ "$FAST" == "true" ]; then
        count=100
    fi
    dd if=/dev/zero of=largefile bs=1MB count=$count oflag=direct
    fsync_dir
}

function read_large_file {
    dd if=largefile of=/dev/null bs=1MB iflag=direct
}

function read_first_byte {
    dd if=largefile of=/dev/null bs=512 count=1 iflag=direct
}

if [ "$t" = "" -o "$t" = "create" ]; then
    for i in $(seq 1 $iter); do
        run_test create_files
        run_test rm_files
    done
fi

if [ "$t" = "" -o "$t" = "create_parallel" ]; then
    for i in $(seq 1 $iter); do
        run_test create_files_parallel
        run_test rm_files_parallel
    done
fi

function write_md5 {
    if ! base64 -w 0 </dev/null; then
        # BSD base64
        seed=$(dd if=/dev/urandom bs=128 count=1 | base64)
    else
        seed=$(dd if=/dev/urandom bs=128 count=1 | base64 -w 0)
    fi
    random_cmd="openssl enc -aes-256-ctr -pass pass:$seed -nosalt"
    count=1000
    if [ "$FAST" == "true" ]; then
        count=100
    fi
    MD5=$(dd if=/dev/zero bs=1M count=$count | $random_cmd | \
        tee >(md5sum) >(dd of=largefile bs=1M oflag=direct) >/dev/null | cut -f 1 '-d ')
    fsync_dir
}

function read_md5 {
    READ_MD5=$(md5sum largefile | cut -f 1 '-d ')
    if [ "$READ_MD5" != "$MD5" ]; then
        echo "$READ_MD5 != $MD5" >&2
        exit 1
    fi
}

if [ "$t" = "" -o "$t" = "io" ]; then
    for i in $(seq 1 $iter); do
        run_test write_md5
        run_test read_md5
        run_test read_first_byte
        rm largefile
    done
fi

if [ "$t" = "" -o "$t" = "ls" ]; then
    rm -rf bench_ls
    mkdir bench_ls
    cd bench_ls
    create_files_parallel 2000
    for i in $(seq 1 $iter); do
        run_test ls_files 2000
    done
    if [ "$CLEANUP" = "true" ]; then
        rm_files 2000
    fi
    cd ..
fi

if [ "$t" = "ls_create" ]; then
    create_files_parallel 1000
    test=dummy
    sleep 10
fi

if [ "$t" = "ls_ls" ]; then
    run_test ls_files 1000
fi

if [ "$t" = "ls_rm" ]; then
    rm_files 1000
    test=dummy
fi

if [ "$t" = "" -o "$t" = "find" ]; then
    rm -rf bench_find
    mkdir bench_find
    cd bench_find
    create_tree_parallel
    for i in $(seq 1 $iter); do
        run_test find_files
    done
    rm_tree
    cd ..
fi

if [ "$t" = "find_create" ]; then
    create_tree_parallel
    test=dummy
    sleep 10
fi

if [ "$t" = "find_find" ]; then
    for i in $(seq 1 $iter); do
        run_test find_files
    done
fi

if [ "$t" = "issue231" ]; then
    run_test write_md5
    (for i in $(seq 1 20); do
         run_test read_md5 & true
    done; wait);
    rm largefile
fi


if [ "$t" = "cleanup" ]; then
    rm -Rf *
    test=dummy
fi

# for https://github.com/kahing/goofys/issues/64
# quote: There are 5 concurrent transfers going at a time.
# Data file size is often 100-400MB.
# Regarding the number of transfers, I think it's about 200 files.
# We read from the goofys mounted s3 bucket and write to a local spring webapp using curl.
if [ "$t" = "disable" -o "$t" = "issue64" ]; then
    # setup the files
    (for i in $(seq 0 9); do
        dd if=/dev/zero of=file$i bs=1MB count=300 oflag=direct & true
    done
    wait)
    if [ $? != 0 ]; then
        exit $?
    fi

    # 200 files and 5 concurrent transfer means 40 times, do 50 times for good measure
    (for i in $(seq 0 9); do
        dd if=file$i of=/dev/null bs=1MB iflag=direct &
    done

    for i in $(seq 10 300); do
        # wait for 1 to finish, then invoke more
        wait -n
        running=$(ps -ef | grep ' dd if=' | grep -v grep | sed 's/.*dd if=file\([0-9]\).*/\1/')
        for i in $(seq 0 9); do
            if echo $running | grep -v -q $i; then
                dd if=file$i of=/dev/null bs=1MB iflag=direct &
                break
            fi
        done
    done
    wait)
    if [ $? != 0 ]; then
        exit $?
    fi

    # cleanup
    (for i in $(seq 0 9); do
        rm -f file$i & true
    done
    wait)
fi

if [ "$test" = "" ]; then
    echo "No test was run: $t"
    exit 1
fi
