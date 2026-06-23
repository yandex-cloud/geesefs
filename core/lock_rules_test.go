package core

import (
	"testing"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

func testGoofys(flags *cfg.FlagStorage) *Goofys {
	if flags == nil {
		flags = &cfg.FlagStorage{}
	}
	fs := &Goofys{flags: flags}
	fs.locks = NewFileLockManager(fs)
	return fs
}

func TestLockRulesExclude(t *testing.T) {
	r := newLockRules(nil)
	for _, key := range []string{
		"~$geesefs-test.docx",
		"geesefs-test.docx.sb-15d02470-gHvegY",
		"geesefs-test.docx.sb-15d02470-gHvegY/.~WRD0000",
		"geesefs-test.docx.sb-15d02470-gHvegY/..~WRD0002",
	} {
		if !r.excluded(key) {
			t.Fatalf("expected excluded: %q", key)
		}
	}
	if r.excluded("geesefs-test.docx") {
		t.Fatal("main document must not be excluded")
	}
}

func TestLockRulesInclude(t *testing.T) {
	r := newLockRules(&cfg.FlagStorage{LockInclude: "*.docx,*.xlsx"})
	if !r.included("report.docx") || !r.included("finance/Q1.xlsx") {
		t.Fatal("expected office extensions to match include")
	}
	if r.included("notes.txt") {
		t.Fatal("txt must not match include")
	}
	rAll := newLockRules(nil)
	if !rAll.included("anything.bin") {
		t.Fatal("empty include must allow all")
	}
}

func TestLockRulesUserExclude(t *testing.T) {
	r := newLockRules(&cfg.FlagStorage{LockExclude: "*.tmp,*.bak"})
	if !r.excluded("draft.tmp") || !r.excluded("archive.bak") {
		t.Fatal("user exclude patterns must apply")
	}
	if !r.excluded("~$file.docx") {
		t.Fatal("built-in office excludes must remain")
	}
}

func TestLockRulesSubjectMap(t *testing.T) {
	r := newLockRules(nil)
	if got := r.subjectDataKey("geesefs-test.docx.sb-15d02470-CWByZs/.~WRD0000", nil); got != "geesefs-test.docx" {
		t.Fatalf("sandbox temp: got %q", got)
	}
	if got := r.subjectDataKey("finance/Q1.xlsx.sb-1-abc/.~WRD0000", nil); got != "finance/Q1.xlsx" {
		t.Fatalf("nested sandbox: got %q", got)
	}

	fs := testGoofys(nil)
	parent := NewInode(fs, nil, "")
	parent.dir = &DirInodeData{Children: []*Inode{
		NewInode(fs, parent, "geesefs-test.docx"),
	}}
	if got := r.subjectForNewChild(parent, "geesefs-test.docx.sb-15d02470-CWByZs"); got != "geesefs-test.docx" {
		t.Fatalf("sandbox mkdir: got %q", got)
	}
	if got := r.subjectForNewChild(parent, "~$geesefs-test.docx"); got != "geesefs-test.docx" {
		t.Fatalf("marker create: got %q", got)
	}
	if got := r.subjectForNewChild(parent, "~$esefs-test.docx"); got != "geesefs-test.docx" {
		t.Fatalf("macOS truncated marker create: got %q", got)
	}
}
