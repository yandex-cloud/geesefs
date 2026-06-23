package core

import (
	"testing"
	"time"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

func TestLockKey(t *testing.T) {
	if got := lockKey("finance/Q1.xlsx"); got != "finance/.Q1.xlsx.geesefs-lock" {
		t.Fatalf("lockKey finance: got %q", got)
	}
	if got := lockKey("file.txt"); got != ".file.txt.geesefs-lock" {
		t.Fatalf("lockKey root file: got %q", got)
	}
}

func TestIsLockSidecarName(t *testing.T) {
	if !isLockSidecarName(".Q1.xlsx.geesefs-lock") {
		t.Fatal("expected sidecar name")
	}
	if isLockSidecarName("Q1.xlsx.geesefs-lock") {
		t.Fatal("suffix without dot prefix must not match")
	}
}

func TestShouldLockDataKey(t *testing.T) {
	fs := testGoofys(nil)
	if !shouldLockDataKey(fs, "geesefs-test.docx") {
		t.Fatal("main document should be locked")
	}
	if shouldLockDataKey(fs, "~$geesefs-test.docx") {
		t.Fatal("MS Office marker should be skipped")
	}
	if shouldLockDataKey(fs, "geesefs-test.docx.sb-15d02470-gHvegY/.~WRD0000") {
		t.Fatal("MS Office sandbox temp should be skipped")
	}
	if shouldLockDataKey(fs, "geesefs-test.docx.sb-15d02470-gHvegY/..~WRD0002") {
		t.Fatal("MS Office sandbox temp with extra dot should be skipped")
	}
	if shouldLockDataKey(fs, "geesefs-test.docx.sb-15d02470-ECklaz/.~WRL0001") {
		t.Fatal("MS Office write-lock temp should be skipped")
	}
	if !shouldLockDataKey(fs, "finance/Q1.xlsx") {
		t.Fatal("nested document should be locked")
	}
}

func TestShouldLockDataKeyInclude(t *testing.T) {
	fs := testGoofys(&cfg.FlagStorage{LockInclude: "*.docx"})
	if !shouldLockDataKey(fs, "report.docx") {
		t.Fatal("docx should match include")
	}
	if shouldLockDataKey(fs, "report.xlsx") {
		t.Fatal("xlsx should not match include")
	}
}

func TestLockRecordBusy(t *testing.T) {
	expired := func(rec *lockRecord) bool {
		return rec.ExpiresAt == "2000-01-01T00:00:00Z"
	}
	held := &lockRecord{
		Held:      true,
		Session:   "session-a",
		Owner:     "vbauer",
		Client:    "machine-a",
		ExpiresAt: "2099-01-01T00:00:00Z",
	}
	if !lockRecordBusy(held, "session-b", expired) {
		t.Fatal("different session must be busy")
	}
	if lockRecordBusy(held, "session-a", expired) {
		t.Fatal("same session must not be busy")
	}
	if lockRecordBusy(held, "session-b", func(*lockRecord) bool { return true }) {
		t.Fatal("expired lock must not be busy")
	}
	// Same OS username on another machine — must still be busy.
	if !lockRecordBusy(held, "session-b", expired) {
		t.Fatal("same owner different machine must be busy")
	}
}

func TestLockRecordReclaimable(t *testing.T) {
	expired := func(rec *lockRecord) bool { return false }
	held := &lockRecord{
		Held:    true,
		Session: "old-session",
		Owner:   "vbauer",
		Client:  "machine-a",
	}
	if !lockRecordReclaimable(held, "new-session", "vbauer", "machine-a", expired) {
		t.Fatal("same host remount should reclaim")
	}
	if lockRecordReclaimable(held, "new-session", "vbauer", "machine-b", expired) {
		t.Fatal("same owner different host must not reclaim")
	}
	if !lockRecordReclaimable(held, "old-session", "vbauer", "machine-b", expired) {
		t.Fatal("same session should reclaim")
	}
}

func TestOpenWantsWrite(t *testing.T) {
	if openWantsWrite(0) {
		t.Fatal("O_RDONLY should not want write")
	}
	if !openWantsWrite(1) || !openWantsWrite(2) {
		t.Fatal("O_WRONLY/O_RDWR should want write")
	}
}

func TestLockSubjectInode(t *testing.T) {
	fs := testGoofys(&cfg.FlagStorage{})
	parent := NewInode(fs, nil, "")
	doc := NewInode(fs, parent, "geesefs-test.docx")
	marker := NewInode(fs, parent, "~$esefs-test.docx")
	parent.dir = &DirInodeData{Children: []*Inode{doc, marker}}

	if got := lockSubjectInode(fs, doc); got != doc {
		t.Fatal("main document inode should map to itself")
	}
	if got := lockSubjectInode(fs, marker); got != doc {
		t.Fatalf("MS Office marker should map to main document, got %v", got)
	}
	if got := lockSubjectDataKey(marker); got != "geesefs-test.docx" {
		t.Fatalf("marker subject key: got %q", got)
	}
	_, ownKey := marker.cloud()
	if shouldLockDataKey(fs, ownKey) {
		t.Fatal("marker own key must not be a lock subject")
	}
}

func TestLockRecordStale(t *testing.T) {
	fs := testGoofys(&cfg.FlagStorage{EnableFileLocks: true, LockTTL: 30 * time.Minute})
	rec := &lockRecord{ExpiresAt: "2000-01-01T00:00:00Z", Held: true}
	if !fs.locks.lockExpired(rec) {
		t.Fatal("expected expired lock")
	}
}
