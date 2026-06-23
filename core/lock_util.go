// Lock path helpers: sidecar naming, lock subject resolution, record interpretation.

package core

import (
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

const lockSidecarSuffix = ".geesefs-lock"

const (
	lockFlagOff int32 = 0
	lockFlagOn  int32 = 1
)

const modeWriteAll = os.FileMode(syscall.S_IWUSR | syscall.S_IWGRP | syscall.S_IWOTH)

func isLockSidecarName(name string) bool {
	return strings.HasPrefix(name, ".") && strings.HasSuffix(name, lockSidecarSuffix)
}

// shouldLockDataKey reports whether this object key may own a lock sidecar.
func shouldLockDataKey(fs *Goofys, dataKey string) bool {
	if dataKey == "" {
		return false
	}
	if fs != nil {
		if fs.locks.excluded(dataKey) {
			return false
		}
		if !fs.locks.included(dataKey) {
			return false
		}
	}
	_, base := path.Split(dataKey)
	return !isLockSidecarName(base)
}

func lockRecordBusy(rec *lockRecord, mySession string, expired func(*lockRecord) bool) bool {
	if rec == nil || !rec.Held || expired(rec) {
		return false
	}
	return rec.Session != mySession
}

func lockRecordReclaimable(rec *lockRecord, mySession, myOwner, myClient string, expired func(*lockRecord) bool) bool {
	if rec == nil || !rec.Held || expired(rec) {
		return true
	}
	if rec.Session == mySession {
		return true
	}
	return rec.Owner == myOwner && rec.Client == myClient
}

func shouldHideLockSidecar(flags *cfg.FlagStorage, name string) bool {
	return flags.HideLockSidecars && isLockSidecarName(name)
}

func lockKey(dataKey string) string {
	dir, base := path.Split(dataKey)
	if dir != "" && !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	return dir + "." + base + lockSidecarSuffix
}

func dataKeyFromLockSidecar(parentKey, sidecarName string) string {
	if !isLockSidecarName(sidecarName) {
		return ""
	}
	inner := strings.TrimSuffix(strings.TrimPrefix(sidecarName, "."), lockSidecarSuffix)
	if parentKey != "" {
		return parentKey + "/" + inner
	}
	return inner
}

func defaultLockOwner(flagOwner string) string {
	if flagOwner != "" {
		return flagOwner
	}
	if u := os.Getenv("USER"); u != "" {
		return u
	}
	if u := os.Getenv("USERNAME"); u != "" {
		return u
	}
	host, _ := os.Hostname()
	if host != "" {
		return host
	}
	return "geesefs"
}

func openWantsWrite(openFlags uint32) bool {
	return openFlags&syscall.O_ACCMODE != syscall.O_RDONLY
}

func joinDataKey(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return strings.TrimSuffix(prefix, "/") + "/" + name
}

// lockSubjectForChild returns the data key to check on create/mkdir, or "".
func lockSubjectForChild(parent *Inode, name string) string {
	var parentKey string
	var fs *Goofys
	if parent != nil {
		_, parentKey = parent.cloud()
		fs = parent.fs
	}
	full := joinDataKey(parentKey, name)
	if shouldLockDataKey(fs, full) {
		return full
	}
	return ""
}

// lockSubjectInode returns inode if it is a lock subject, else nil.
func lockSubjectInode(fs *Goofys, inode *Inode) *Inode {
	if inode == nil {
		return nil
	}
	_, dataKey := inode.cloud()
	if shouldLockDataKey(fs, dataKey) {
		return inode
	}
	return nil
}

// lockSubjectDataKey returns the object key governed by advisory lock, or "".
func lockSubjectDataKey(inode *Inode) string {
	if inode == nil {
		return ""
	}
	_, dataKey := inode.cloud()
	if dataKey == "" || !shouldLockDataKey(inode.fs, dataKey) {
		return ""
	}
	return dataKey
}
