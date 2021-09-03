#!/bin/bash

set -e

PROG1=geesefs
PROG2=goofys
PROG3=s3fs

python3 ./bench_format.py <(paste bench.$PROG1 bench.$PROG2 bench.$PROG3) > bench.data

OUT=bench3.png

gnuplot -c bench3.gnuplot bench.data "$OUT" "$PROG1" "$PROG2" "$PROG3"

convert -rotate 90 "$OUT" "$OUT"
