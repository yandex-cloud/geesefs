#!/usr/bin/python3

import os
import signal
import subprocess
import sys
import time

FSYNC_TIMEOUT = 30
UMOUNT_TIMEOUT = 15
LAZY_UMOUNT_TIMEOUT = 15


def find_mount_target(spec):
	try:
		path = subprocess.check_output(
			["findmnt", "-f", "-n", "-o", "TARGET", spec],
			stderr=subprocess.DEVNULL,
			text=True,
		).strip()
	except subprocess.CalledProcessError:
		return None
	return path or None


def run_with_timeout(cmd, timeout):
	# subprocess.run(..., timeout=...) can hang forever if the child is in
	# uninterruptible sleep (D state), e.g. blocking umount/fsync on FUSE.
	proc = subprocess.Popen(
		cmd,
		start_new_session=True,
		stdout=subprocess.DEVNULL,
		stderr=subprocess.DEVNULL,
	)
	deadline = time.monotonic() + timeout
	while time.monotonic() < deadline:
		rc = proc.poll()
		if rc is not None:
			return rc
		time.sleep(0.1)

	try:
		os.killpg(proc.pid, signal.SIGKILL)
	except ProcessLookupError:
		rc = proc.poll()
		return rc if rc is not None else -9

	grace = time.monotonic() + 2
	while time.monotonic() < grace:
		rc = proc.poll()
		if rc is not None:
			return rc
		time.sleep(0.1)

	print("command timed out: %s" % " ".join(cmd), file=sys.stderr, flush=True)
	return None


def fsync_path(path):
	script = (
		"import os\n"
		f"fd = os.open({path!r}, os.O_RDONLY)\n"
		"os.fsync(fd)\n"
		"os.close(fd)\n"
	)
	rc = run_with_timeout([sys.executable, "-c", script], FSYNC_TIMEOUT)
	if rc is None:
		print("fsync timed out", file=sys.stderr, flush=True)
		return False
	if rc != 0:
		print("fsync failed", file=sys.stderr, flush=True)
		return False
	return True


def umount_path(path):
	rc = run_with_timeout(["sudo", "/bin/umount", path], UMOUNT_TIMEOUT)
	if rc == 0:
		return 0
	if rc is None:
		print("umount timed out, trying lazy umount", file=sys.stderr, flush=True)
	elif rc != 0:
		print("umount failed, trying lazy umount", file=sys.stderr, flush=True)

	rc = run_with_timeout(["sudo", "/bin/umount", "-l", path], LAZY_UMOUNT_TIMEOUT)
	if rc is None:
		print("lazy umount timed out", file=sys.stderr, flush=True)
		return 1
	return rc


def main():
	if len(sys.argv) < 2:
		sys.exit(1)

	path = find_mount_target(sys.argv[1])
	if not path:
		sys.exit(1)

	# Best-effort fsync before umount; failure must not block teardown.
	if not fsync_path(path):
		print("continuing with umount despite fsync failure", file=sys.stderr, flush=True)
	sys.exit(umount_path(path))


if __name__ == "__main__":
	main()
