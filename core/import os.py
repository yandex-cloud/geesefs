import os
import re

MP_RE = r'^(?:\s+)?[^#\s]+\s+(/srv/(?:u[0-8]|storage/|angel/disks/)\d+)'

def sift(func, seq):
    res = list(filter(None, map(func, seq)))

    return res

def grep(regex, line, _dict=False):
    ret = re.search(regex, line) or {}
    if ret:
        if _dict:
            ret = ret.groupdict()
        else:
            ret = ret.groups() and ret.group(1) or ret.group()
    return ret

with open('/proc/mounts') as mounts_file:
    mounts = [x for x in mounts_file]

mounted_units = sift(lambda x: grep(MP_RE, x), mounts)
angel_mp = [x for x in mounted_units if 'angel' in x]

print(mounted_units)

print(angel_mp)

with open('/etc/fstab') as f:
    fstab_units = sift(lambda x: grep(MP_RE, x), f)

not_mounted = sift(lambda x: not os.path.ismount(x) and x, fstab_units)

print(not_mounted)

