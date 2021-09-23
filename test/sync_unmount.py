#!/usr/bin/python3

import sys, os, threading, subprocess

def watchdog():
    print("fsync timed out")
    sys.exit(1)

path = subprocess.check_output("findmnt -f -o TARGET \""+sys.argv[1]+"\" | tail -n 1", shell = True).decode('utf-8').strip()
if not path:
    sys.exit(1)
timer = threading.Timer(60, watchdog)
timer.start()
fd = os.open(path, os.O_RDONLY)
os.fsync(fd)
os.close(fd)
subprocess.call("sudo /bin/umount "+path, shell = True)
timer.cancel()
