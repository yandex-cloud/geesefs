// Pluggable rules map auxiliary paths (editor temp files) to the main locked document.
// Product-specific logic lives in separate files (e.g. lock_office.go for MS Office) and registers here.

package core

// lockSubjectRule describes how an editor's auxiliary files relate to a lock subject.
type lockSubjectRule interface {
	// Excluded is true when dataKey must not be a direct lock subject (sidecar target).
	Excluded(dataKey string) bool
	// SubjectDataKey maps an auxiliary object key to the main document key, or "".
	SubjectDataKey(dataKey string, inode *Inode) string
	// SubjectForNewChild maps a create/mkdir name to the governed document key, or "".
	SubjectForNewChild(parent *Inode, name string) string
}

var lockSubjectRules []lockSubjectRule

func registerLockSubjectRule(r lockSubjectRule) {
	lockSubjectRules = append(lockSubjectRules, r)
}

func auxiliaryExcludedFromLock(dataKey string) bool {
	for _, r := range lockSubjectRules {
		if r.Excluded(dataKey) {
			return true
		}
	}
	return false
}

func resolveSubjectDataKey(dataKey string, inode *Inode) string {
	for _, r := range lockSubjectRules {
		if subject := r.SubjectDataKey(dataKey, inode); subject != "" {
			return subject
		}
	}
	return ""
}

func resolveSubjectForNewChild(parent *Inode, name string) string {
	for _, r := range lockSubjectRules {
		if subject := r.SubjectForNewChild(parent, name); subject != "" {
			return subject
		}
	}
	return ""
}
