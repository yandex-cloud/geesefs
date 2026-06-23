// Microsoft Office on macOS (Word, Excel, PowerPoint, …): lock markers (~$),
// sandbox dirs (.sb-*), and sandbox temp files. Registers a lockSubjectRule.

package core

import (
	"path"
	"strings"
)

func init() {
	registerLockSubjectRule(msOfficeLockRule{})
}

type msOfficeLockRule struct{}

func (msOfficeLockRule) Excluded(dataKey string) bool {
	return msOfficeExcludedFromLock(dataKey)
}

func (msOfficeLockRule) SubjectDataKey(dataKey string, inode *Inode) string {
	if subject := msOfficeSandboxSubjectDataKey(dataKey); subject != "" {
		return subject
	}
	dir, base := path.Split(dataKey)
	if !isMSOfficeMarkerName(base) {
		return ""
	}
	if inode != nil && inode.Parent != nil {
		if subject := resolveMSOfficeMarkerDataKey(inode.Parent, base); subject != "" {
			return subject
		}
	}
	return joinDataKey(strings.TrimSuffix(dir, "/"), strings.TrimPrefix(base, "~$"))
}

func (msOfficeLockRule) SubjectForNewChild(parent *Inode, name string) string {
	if parent != nil && isMSOfficeMarkerName(name) {
		return resolveMSOfficeMarkerDataKey(parent, name)
	}
	if base, ok := msOfficeSandboxBaseName(name); ok {
		_, parentKey := parent.cloud()
		return joinDataKey(parentKey, base)
	}
	return ""
}

func isMSOfficeMarkerName(name string) bool {
	return strings.HasPrefix(name, "~$")
}

// isMSOfficeSandboxDir matches Office macOS sandbox dirs: {doc}.sb-{id}-{token}.
// The two suffix segments are Office-internal opaque strings (hex + alphanumeric in practice);
// we do not parse them as POSIX pid.
func isMSOfficeSandboxDir(name string) bool {
	return strings.Contains(name, ".sb-")
}

// isMSOfficeSandboxTemp matches scratch files inside sandbox (.~WRD*, .~WRL*, ..~WRD*).
func isMSOfficeSandboxTemp(name string) bool {
	return strings.HasPrefix(strings.TrimLeft(name, "."), "~WR")
}

// msOfficeSandboxBaseName: geesefs-test.docx.sb-15d02470-CWByZs -> geesefs-test.docx
func msOfficeSandboxBaseName(name string) (string, bool) {
	i := strings.Index(name, ".sb-")
	if i <= 0 {
		return "", false
	}
	return name[:i], true
}

func msOfficeExcludedFromLock(dataKey string) bool {
	if dataKey == "" {
		return true
	}
	dir, base := path.Split(dataKey)
	if isMSOfficeMarkerName(base) || isMSOfficeSandboxTemp(base) {
		return true
	}
	if isMSOfficeSandboxDir(strings.TrimSuffix(base, "/")) {
		return true
	}
	if dir != "" {
		for _, part := range strings.Split(strings.Trim(dir, "/"), "/") {
			if isMSOfficeSandboxDir(part) {
				return true
			}
		}
	}
	return false
}

func msOfficeSandboxSubjectDataKey(cloudKey string) string {
	parts := strings.Split(strings.Trim(cloudKey, "/"), "/")
	var prefix []string
	for _, part := range parts {
		if base, ok := msOfficeSandboxBaseName(part); ok {
			return joinDataKey(strings.Join(prefix, "/"), base)
		}
		prefix = append(prefix, part)
	}
	return ""
}

// resolveMSOfficeMarkerDataKey maps ~$doc.xlsx to the locked data object key.
// Office on macOS may truncate the basename (~$esefs-test.docx for geesefs-test.docx).
func resolveMSOfficeMarkerDataKey(parent *Inode, markerName string) string {
	if !isMSOfficeMarkerName(markerName) || parent == nil {
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
		if child == nil || child.isDir() || isLockSidecarName(child.Name) || isMSOfficeMarkerName(child.Name) {
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
