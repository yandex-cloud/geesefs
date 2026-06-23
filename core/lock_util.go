// Copyright 2026 Yandex LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package core

import (
	"os"
	"path"
	"syscall"
	"strings"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

const lockSidecarSuffix = ".geesefs-lock"

func isLockSidecarName(name string) bool {
	return strings.HasPrefix(name, ".") && strings.HasSuffix(name, lockSidecarSuffix)
}

func isOfficeMarkerName(name string) bool {
	return strings.HasPrefix(name, "~$")
}

// isOfficeSandboxDir matches Word macOS sandbox folders: doc.docx.sb-{pid}-{token}
func isOfficeSandboxDir(name string) bool {
	return strings.Contains(name, ".sb-")
}

// isOfficeTempName matches Word scratch files inside sandbox (.~WRD*, .~WRL*, ..~WRD*).
func isOfficeTempName(name string) bool {
	return strings.HasPrefix(strings.TrimLeft(name, "."), "~WR")
}

// shouldLockDataKey reports whether advisory locking applies to this object.
// Only the main document is locked; Office sandbox dirs and temp files are excluded.
func shouldLockDataKey(dataKey string) bool {
	if dataKey == "" {
		return false
	}
	dir, base := path.Split(dataKey)
	if isLockSidecarName(base) || isOfficeMarkerName(base) {
		return false
	}
	if isOfficeTempName(base) {
		return false
	}
	if isOfficeSandboxDir(strings.TrimSuffix(base, "/")) {
		return false
	}
	if dir != "" {
		for _, part := range strings.Split(strings.Trim(dir, "/"), "/") {
			if isOfficeSandboxDir(part) {
				return false
			}
		}
	}
	return true
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

// officeSandboxBaseName extracts the document basename from a Word sandbox dir
// (e.g. geesefs-test.docx.sb-15d02470-CWByZs -> geesefs-test.docx).
func officeSandboxBaseName(name string) (string, bool) {
	i := strings.Index(name, ".sb-")
	if i <= 0 {
		return "", false
	}
	return name[:i], true
}

func joinDataKey(prefix, name string) string {
	if prefix == "" {
		return name
	}
	return strings.TrimSuffix(prefix, "/") + "/" + name
}

// sandboxSubjectDataKey maps a sandbox path to the locked main document key.
func sandboxSubjectDataKey(cloudKey string) string {
	parts := strings.Split(strings.Trim(cloudKey, "/"), "/")
	var prefix []string
	for _, part := range parts {
		if base, ok := officeSandboxBaseName(part); ok {
			return joinDataKey(strings.Join(prefix, "/"), base)
		}
		prefix = append(prefix, part)
	}
	return ""
}

// resolveOfficeMarkerDataKey maps ~$doc.xlsx to the locked data object key.
// Word on macOS may use ~$esefs-test.docx for geesefs-test.docx.
func resolveOfficeMarkerDataKey(parent *Inode, markerName string) string {
	if !isOfficeMarkerName(markerName) || parent == nil {
		return ""
	}
	_, parentKey := parent.cloud()
	suffix := strings.TrimPrefix(markerName, "~$")
	direct := joinDataKey(parentKey, suffix)

	parent.mu.Lock()
	defer parent.mu.Unlock()

	if parent.findChildUnlocked(suffix) != nil {
		return direct
	}
	var bestName string
	for _, child := range parent.dir.Children {
		if child == nil || child.isDir() || isLockSidecarName(child.Name) || isOfficeMarkerName(child.Name) {
			continue
		}
		if !strings.HasSuffix(child.Name, suffix) {
			continue
		}
		if bestName == "" || len(child.Name) > len(bestName) {
			bestName = child.Name
		}
	}
	if bestName == "" {
		return direct
	}
	return joinDataKey(parentKey, bestName)
}

// lockSubjectForChild returns the data key whose sidecar governs a new child name.
func lockSubjectForChild(parent *Inode, name string) string {
	if parent != nil && isOfficeMarkerName(name) {
		return resolveOfficeMarkerDataKey(parent, name)
	}
	var parentKey string
	if parent != nil {
		_, parentKey = parent.cloud()
	}
	if base, ok := officeSandboxBaseName(name); ok {
		return joinDataKey(parentKey, base)
	}
	full := joinDataKey(parentKey, name)
	if shouldLockDataKey(full) {
		return full
	}
	return ""
}

// lockSubjectInode returns the main document inode that owns the advisory lock.
// Office markers (~$) and sandbox files map to their parent document inode.
func lockSubjectInode(fs *Goofys, inode *Inode) *Inode {
	if inode == nil {
		return nil
	}
	_, dataKey := inode.cloud()
	if shouldLockDataKey(dataKey) {
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
	if shouldLockDataKey(dataKey) {
		return dataKey
	}
	if subject := sandboxSubjectDataKey(dataKey); subject != "" {
		return subject
	}
	dir, base := path.Split(dataKey)
	if isOfficeMarkerName(base) {
		if inode.Parent != nil {
			if subject := resolveOfficeMarkerDataKey(inode.Parent, base); subject != "" {
				return subject
			}
		}
		return joinDataKey(strings.TrimSuffix(dir, "/"), strings.TrimPrefix(base, "~$"))
	}
	return ""
}
