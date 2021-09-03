#!/bin/bash

set -e

PROG1=$1
PROG2=$2

if [ -z "$PROG1" -o -z "$PROG2" ]; then
    echo "USAGE: ./format_bench.sh PROG1 PROG2"
    echo "For example, ./format_bench.sh geesefs goofys"
    exit
fi

python3 ./bench_format.py <(paste bench.$PROG1 bench.$PROG2) > bench.data

OUT=bench_"$PROG1"_"$PROG2".png

gnuplot -c bench_graph.gnuplot bench.data "$OUT" "$PROG1" "$PROG2"

convert -rotate 90 "$OUT" "$OUT"
