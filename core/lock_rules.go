// Lock path rules: include/exclude globs (--lock-include, --lock-exclude) and
// built-in subject mapping (auxiliary editor paths → main document key).

package core

import (
	"path"
	"strings"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// Built-in exclude globs for MS Office auxiliary paths on macOS (always active).
var defaultLockExcludeGlobs = []string{
	"~$*",
	"*.sb-*",
	"*~WR*",
}

// subjectMap describes how an auxiliary path maps to the main lock subject.
type subjectMap struct {
	glob string
	kind subjectMapKind
	arg  string
}

type subjectMapKind int

const (
	// Strip arg from the first matching path component, prepend prior components.
	subjectPathComponentStripAt subjectMapKind = iota
	// Strip arg prefix from basename; if no direct child, suffix-match siblings in parent.
	subjectParentPrefixMatch
)

// Built-in subject maps (Office on macOS). Not user-configurable for now.
var defaultLockSubjectMaps = []subjectMap{
	{glob: "*.sb-*", kind: subjectPathComponentStripAt, arg: ".sb-"},
	{glob: "~$*", kind: subjectParentPrefixMatch, arg: "~$"},
}

// lockRules holds include/exclude globs and subject-mapping rules for one mount.
// Embedded in FileLockManager (always present; path rules apply even when locking is off).
type lockRules struct {
	include    []string
	exclude    []string
	subjectMap []subjectMap
}

func newLockRules(flags *cfg.FlagStorage) *lockRules {
	exclude := append([]string(nil), defaultLockExcludeGlobs...)
	if flags != nil && flags.LockExclude != "" {
		exclude = append(exclude, parseLockGlobs(flags.LockExclude)...)
	}
	var include []string
	if flags != nil {
		include = parseLockGlobs(flags.LockInclude)
	}
	return &lockRules{
		include:    include,
		exclude:    exclude,
		subjectMap: append([]subjectMap(nil), defaultLockSubjectMaps...),
	}
}

func parseLockGlobs(s string) []string {
	if s == "" {
		return nil
	}
	parts := strings.Split(s, ",")
	out := make([]string, 0, len(parts))
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p != "" {
			out = append(out, p)
		}
	}
	return out
}

func matchLockGlob(pattern, name string) bool {
	ok, _ := path.Match(pattern, name)
	return ok
}

func (r *lockRules) excluded(dataKey string) bool {
	if r == nil {
		return false
	}
	_, base := path.Split(dataKey)
	for _, pat := range r.exclude {
		if matchLockGlob(pat, base) {
			return true
		}
	}
	for _, part := range strings.Split(strings.Trim(dataKey, "/"), "/") {
		for _, pat := range r.exclude {
			if matchLockGlob(pat, part) {
				return true
			}
		}
	}
	return false
}

// included reports whether dataKey may be a direct lock subject (sidecar owner).
// Empty include list means all non-excluded paths.
func (r *lockRules) included(dataKey string) bool {
	if r == nil || len(r.include) == 0 {
		return true
	}
	_, base := path.Split(dataKey)
	for _, pat := range r.include {
		if matchLockGlob(pat, base) {
			return true
		}
	}
	return false
}

func (r *lockRules) subjectDataKey(dataKey string, inode *Inode) string {
	if r == nil {
		return ""
	}
	for _, m := range r.subjectMap {
		if subject := subjectDataKeyFromMap(m, dataKey, inode); subject != "" {
			return subject
		}
	}
	return ""
}

func (r *lockRules) subjectForNewChild(parent *Inode, name string) string {
	if r == nil || parent == nil {
		return ""
	}
	for _, m := range r.subjectMap {
		if subject := subjectForNewChildFromMap(m, parent, name); subject != "" {
			return subject
		}
	}
	return ""
}

func subjectDataKeyFromMap(m subjectMap, dataKey string, inode *Inode) string {
	switch m.kind {
	case subjectPathComponentStripAt:
		return subjectByPathComponentStripAt(dataKey, m.glob, m.arg)
	case subjectParentPrefixMatch:
		dir, base := path.Split(dataKey)
		if !matchLockGlob(m.glob, base) {
			return ""
		}
		if inode != nil && inode.Parent != nil {
			if subject := resolvePrefixMarker(inode.Parent, base, m.arg); subject != "" {
				return subject
			}
		}
		return joinDataKey(strings.TrimSuffix(dir, "/"), strings.TrimPrefix(base, m.arg))
	}
	return ""
}

func subjectForNewChildFromMap(m subjectMap, parent *Inode, name string) string {
	switch m.kind {
	case subjectPathComponentStripAt:
		if matchLockGlob(m.glob, name) {
			if base, ok := stripAt(name, m.arg); ok {
				_, parentKey := parent.cloud()
				return joinDataKey(parentKey, base)
			}
		}
	case subjectParentPrefixMatch:
		if matchLockGlob(m.glob, name) {
			return resolvePrefixMarker(parent, name, m.arg)
		}
	}
	return ""
}

func subjectByPathComponentStripAt(cloudKey, glob, at string) string {
	parts := strings.Split(strings.Trim(cloudKey, "/"), "/")
	var prefix []string
	for _, part := range parts {
		if matchLockGlob(glob, part) {
			if base, ok := stripAt(part, at); ok {
				return joinDataKey(strings.Join(prefix, "/"), base)
			}
		}
		prefix = append(prefix, part)
	}
	return ""
}

func stripAt(name, at string) (string, bool) {
	i := strings.Index(name, at)
	if i <= 0 {
		return "", false
	}
	return name[:i], true
}

// resolvePrefixMarker maps ~$doc.xlsx to the locked data object key.
// Editors on macOS may truncate the marker (~$esefs-test.docx for geesefs-test.docx).
func resolvePrefixMarker(parent *Inode, markerName, prefix string) string {
	if !strings.HasPrefix(markerName, prefix) || parent == nil {
		return ""
	}
	_, parentKey := parent.cloud()
	suffix := strings.TrimPrefix(markerName, prefix)
	direct := joinDataKey(parentKey, suffix)

	parent.mu.Lock()
	defer parent.mu.Unlock()

	if parent.findChildUnlocked(suffix) != nil {
		return direct
	}
	var bestName string
	for _, child := range parent.dir.Children {
		if child == nil || child.isDir() || isLockSidecarName(child.Name) || strings.HasPrefix(child.Name, prefix) {
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
