// Lock path rules: include/exclude globs (--lock-include, --lock-exclude).
//
// Subject mapping (auxiliary editor paths → main document) was removed for
// manual testing. To restore: cp lock_rules.go.with-subject-map lock_rules.go

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

// lockRules holds include/exclude globs for one mount.
// Embedded in FileLockManager (always present; path rules apply even when locking is off).
type lockRules struct {
	include []string
	exclude []string
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
	return &lockRules{include: include, exclude: exclude}
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
