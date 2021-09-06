#!/usr/bin/python3

import sys, os, random

if len(sys.argv) < 2:
    print("USAGE: python3 gen_small.py DIR [COUNT] [MAX_SIZE]")
    print("Creates COUNT files under DIR, sharded over 1024 2-level deep subdirectories")
    print("Files will be 0.5 KB - MAX_SIZE KB, skewed to smaller sizes")
    print("Default COUNT and MAX_SIZE are 6400 and 300 KB")
    exit(1)
d = sys.argv[1]+'/'
if len(sys.argv) > 2:
    n = int(sys.argv[2])
else:
    n = 6400
if len(sys.argv) > 3:
    maxsize = int(sys.argv[3])*1000
else:
    maxsize = 300000

# N small files, 0.5-maxsize kb in size, 1/10 on average, sharded across 1024 dirs
for i in range(n):
    size = 512+int((random.random()**10)*maxsize)
    os.makedirs(d+str(i%32)+'/'+str(int(i/32)%32), 0o777, True)
    f = open(d+str(i%32)+'/'+str(int(i/32)%32)+'/'+str(i), 'wb')
    f.write(os.urandom(size))
    f.close()

# fsync
os.fsync(os.open(d, os.O_RDONLY))
