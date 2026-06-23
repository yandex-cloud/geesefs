// Lock path helpers: sidecar naming, subject key resolution, record interpretation.

package core

import (
	"os"
	"path"
	"strings"
	"syscall"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

const lockSidecarSuffix = ".geesefs-lock"

// dataKey is the S3 object key of the locked file (e.g. "geesefs-test.docx"), not the sidecar.

const (
	lockFlagOff int32 = 0
	lockFlagOn  int32 = 1
)

// All write permission bits (user|group|other); stripped in FUSE attrs for foreign-busy viewers.
const modeWriteAll = os.FileMode(syscall.S_IWUSR | syscall.S_IWGRP | syscall.S_IWOTH)

func isLockSidecarName(name string) bool {
	return strings.HasPrefix(name, ".") && strings.HasSuffix(name, lockSidecarSuffix)
}

// shouldLockDataKey reports whether advisory locking applies to this object (main document only).
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

// lockRecordBusy reports whether another session holds a non-expired lock.
func lockRecordBusy(rec *lockRecord, mySession string, expired func(*lockRecord) bool) bool {
	if rec == nil || !rec.Held || expired(rec) {
		return false
	}
	return rec.Session != mySession
}

// lockRecordReclaimable reports whether this process may take over the sidecar
// (stale lock, same session, or geesefs restart on the same host).
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

// lockKey returns the S3 key for the sidecar lock object of a data object.
func lockKey(dataKey string) string {
	dir, base := path.Split(dataKey)
	if dir != "" && !strings.HasSuffix(dir, "/") {
		dir += "/"
	}
	return dir + "." + base + lockSidecarSuffix
}

// dataKeyFromLockSidecar inverts lockKey for a sidecar basename.
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

// lockSubjectForChild returns the data key whose sidecar governs a new child name.
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

// lockSubjectInode returns the main document inode that owns the advisory lock.
func lockSubjectInode(fs *Goofys, inode *Inode) *Inode {
	if inode == nil {
		return nil
	}
	_, dataKey := inode.cloud()
	if shouldLockDataKey(fs, dataKey) {
		return inode
	}
	subjectKey := lockSubjectDataKey(inode)
	if subjectKey == "" {
		return nil
	}
	if inode.Parent != nil {
		_, base := path.Split(subjectKey)
		inode.Parent.mu.Lock()
		child := inode.Parent.findChildUnlocked(base)
		inode.Parent.mu.Unlock()
		if child != nil {
			return child
		}
	}
	if fs != nil {
		child, err := fs.LookupPath(subjectKey)
		if err == nil {
			return child
		}
	}
	return nil
}

// lockSubjectDataKey returns the main document key whose lock governs this inode.
func lockSubjectDataKey(inode *Inode) string {
	if inode == nil {
		return ""
	}
	_, dataKey := inode.cloud()
	if dataKey == "" {
		return ""
	}
	if shouldLockDataKey(inode.fs, dataKey) {
		return dataKey
	}
	return ""
}
