// Lock path rules: include/exclude globs (--lock-include, --lock-exclude).

package core

import (
	"path"
	"strings"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

// Built-in exclude globs for editor auxiliary paths (MS Office on macOS).
var defaultLockExcludeGlobs = []string{
	"~$*",
	"*.sb-*",
	"*~WR*",
}

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
	if len(r.include) == 0 {
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
