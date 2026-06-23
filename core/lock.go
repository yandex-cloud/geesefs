package core

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"syscall"
	"time"

	"github.com/aws/aws-sdk-go/aws/awserr"
	"github.com/google/uuid"
	"github.com/jacobsa/fuse/fuseops"
)

// Advisory file locking: one S3 sidecar per data object
// - FUSE hooks in this file
// - path/name mapping in lock_util.go
//
// Lifecycle:
//   - NewFileLockManager: once per mount; random session UUID identifies this geesefs process.
//   - Start: one heartbeat goroutine for the whole mount (not per file); renews every held sidecar.
//   - OnOpen/CheckWrite: read or create sidecar; held map tracks objects we locked.
//   - OnInodeClosed: clear sidecar when the last handle on the main document inode closes.
//   - ReleaseAll: on unmount, release all sidecars still in held.

type lockRecord struct {
	Version   int    `json:"version"`
	Held      bool   `json:"held"`
	Owner     string `json:"owner,omitempty"`
	Session   string `json:"session,omitempty"`
	Client    string `json:"client,omitempty"`
	ExpiresAt string `json:"expires_at,omitempty"`
}

type lockAcquireResult int

const (
	lockAcquired lockAcquireResult = iota
	lockBusy
)

type heldFileLock struct {
	lockKey string
	etag    string
}

// FileLockManager holds path rules (include/exclude/subject map) and, when enabled,
// coordinates advisory locks stored as S3 sidecar objects.
type FileLockManager struct {
	fs      *Goofys
	rules   lockRules
	enabled bool

	session string
	owner   string
	client  string

	mu        sync.Mutex
	held      map[string]*heldFileLock // dataKey (S3 object key) -> local holder state
	releaseWg sync.WaitGroup
}

func NewFileLockManager(fs *Goofys) FileLockManager {
	m := FileLockManager{
		fs:      fs,
		rules:   *newLockRules(fs.flags),
		enabled: fs.flags.EnableFileLocks,
	}
	if !m.enabled {
		return m
	}
	host, _ := os.Hostname()
	m.session = uuid.New().String()
	m.owner = defaultLockOwner(fs.flags.LockOwner)
	m.client = host
	m.held = make(map[string]*heldFileLock)
	lockLog.Infof("file locks enabled owner=%q session=%v client=%q include=%q exclude=%q",
		m.owner, m.session, m.client, fs.flags.LockInclude, fs.flags.LockExclude)
	return m
}

func (m *FileLockManager) excluded(dataKey string) bool {
	return m.rules.excluded(dataKey)
}

func (m *FileLockManager) included(dataKey string) bool {
	return m.rules.included(dataKey)
}

func (m *FileLockManager) subjectDataKey(dataKey string, inode *Inode) string {
	return m.rules.subjectDataKey(dataKey, inode)
}

func (m *FileLockManager) subjectForNewChild(parent *Inode, name string) string {
	return m.rules.subjectForNewChild(parent, name)
}

// Start runs a single background heartbeat for all locks held by this mount.
func (m *FileLockManager) Start() {
	if !m.enabled {
		return
	}
	interval := m.fs.flags.LockTTL / 3
	if interval < time.Minute {
		interval = time.Minute
	}
	go m.heartbeatLoop(interval)
}

func (m *FileLockManager) ReleaseAll() {
	if !m.enabled {
		return
	}
	m.mu.Lock()
	keys := make([]string, 0, len(m.held))
	for dataKey := range m.held {
		keys = append(keys, dataKey)
	}
	m.mu.Unlock()
	for _, dataKey := range keys {
		m.releaseKey(dataKey)
	}
	m.releaseWg.Wait()
}

func (m *FileLockManager) OnOpen(inode *Inode, writeIntent bool, fh *FileHandle) error {
	if !m.enabled || inode.isDir() {
		return nil
	}
	dataKey := lockSubjectDataKey(inode)
	if dataKey == "" {
		return nil
	}

	lockLog.Debugf("OnOpen %v writeIntent=%v", dataKey, writeIntent)

	if writeIntent {
		if m.inodeLockedLocally(inode) {
			fh.lockHeld = true
			m.setForeignBusy(inode, false)
			lockLog.Debugf("OnOpen %v: local inode lock re-entry", dataKey)
			return nil
		}
		busy, holder, err := m.isBusyByOther(dataKey)
		if err != nil {
			s3Log.Warnf("lock busy check on open %v: %v", dataKey, err)
			fh.writeBlocked = true
			m.setForeignBusy(inode, true)
		} else if busy {
			fh.writeBlocked = true
			m.setForeignBusy(inode, true)
			lockLog.Debugf("OnOpen %v: busy by %q, writeBlocked", dataKey, holder)
			return nil
		}
		m.setForeignBusy(inode, false)
		if fh.writeBlocked {
			return nil
		}
		ch := make(chan struct{})
		fh.lockWait = ch
		lockLog.Debugf("OnOpen %v: starting async acquire", dataKey)
		go m.acquireOnOpen(dataKey, fh, ch)
		return nil
	}

	if m.inodeLockedLocally(inode) {
		m.setForeignBusy(inode, false)
		return nil
	}
	busy, holder, err := m.isBusyByOther(dataKey)
	if err != nil {
		s3Log.Warnf("lock busy check on open %v: %v", dataKey, err)
		fh.writeBlocked = true
		m.setForeignBusy(inode, true)
		return nil
	}
	if busy {
		fh.writeBlocked = true
		m.setForeignBusy(inode, true)
		lockLog.Debugf("OnOpen %v: read-only open, busy by %q", dataKey, holder)
	} else {
		m.setForeignBusy(inode, false)
	}
	return nil
}

func (m *FileLockManager) acquireOnOpen(dataKey string, fh *FileHandle, done chan struct{}) {
	defer close(done)

	result, err := m.tryAcquire(dataKey)
	if err != nil {
		s3Log.Warnf("lock acquire failed for %v: %v", dataKey, err)
		fh.writeBlocked = true
		lockLog.Debugf("async acquire %v: error -> writeBlocked", dataKey)
		return
	}
	if result == lockBusy {
		fh.writeBlocked = true
		m.setForeignBusy(fh.inode, true)
		lockLog.Debugf("async acquire %v: BUSY -> writeBlocked", dataKey)
		return
	}
	fh.lockHeld = true
	m.markInodeLocked(fh.inode, dataKey)
	m.setForeignBusy(fh.inode, false)
	lockLog.Debugf("async acquire %v: ACQUIRED owner=%v session=%v", dataKey, m.owner, m.session)
}

func (m *FileLockManager) setForeignBusy(inode *Inode, busy bool) {
	subject := lockSubjectInode(m.fs, inode)
	if subject == nil {
		subject = inode
	}
	if busy {
		atomic.StoreInt32(&subject.lockForeignBusy, lockFlagOn)
	} else {
		atomic.StoreInt32(&subject.lockForeignBusy, lockFlagOff)
	}
}

func (m *FileLockManager) inodeLockedLocally(inode *Inode) bool {
	if subject := lockSubjectInode(m.fs, inode); subject != nil {
		if atomic.LoadInt32(&subject.lockInodeHeld) != lockFlagOff {
			return true
		}
	}
	return m.hasLocalRef(lockSubjectDataKey(inode))
}

func (m *FileLockManager) markInodeLocked(inode *Inode, dataKey string) {
	if subject := lockSubjectInode(m.fs, inode); subject != nil {
		atomic.StoreInt32(&subject.lockInodeHeld, lockFlagOn)
	}
	m.mu.Lock()
	if m.held[dataKey] == nil {
		m.held[dataKey] = &heldFileLock{lockKey: lockKey(dataKey)}
	}
	m.mu.Unlock()
}

func (m *FileLockManager) hasLocalRef(dataKey string) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.held[dataKey]
	return state != nil
}

func (m *FileLockManager) OnRelease(fh *FileHandle) {
	if !m.enabled {
		return
	}
	fh.waitLockReady()
	fh.lockHeld = false
}

// OnInodeClosed releases the sidecar when the last FUSE handle on the main document closes.
// Office markers (~$) and sandbox files must not trigger release while the document is open.
func (m *FileLockManager) OnInodeClosed(inode *Inode) {
	if !m.enabled {
		return
	}

	_, ownKey := inode.cloud()
	if !shouldLockDataKey(inode.fs, ownKey) {
		return
	}
	if atomic.LoadInt32(&inode.lockInodeHeld) == lockFlagOff && !m.hasLocalRef(ownKey) {
		return
	}
	if atomic.LoadInt32(&inode.fileHandles) > 0 {
		return
	}
	lockLog.Debugf("OnInodeClosed %v: releasing sidecar", ownKey)
	m.finalizeRelease(inode, ownKey)
}

func (m *FileLockManager) finalizeRelease(inode *Inode, dataKey string) {
	if atomic.LoadInt32(&inode.fileHandles) > 0 {
		lockLog.Debugf("finalizeRelease %v: skipped, handles reopened", dataKey)
		return
	}
	if atomic.LoadInt32(&inode.lockInodeHeld) == lockFlagOff && !m.hasLocalRef(dataKey) {
		return
	}
	atomic.StoreInt32(&inode.lockInodeHeld, lockFlagOff)
	m.mu.Lock()
	delete(m.held, dataKey)
	m.mu.Unlock()
	lockLog.Debugf("finalizeRelease %v: releasing sidecar", dataKey)
	m.releaseKeyAsync(dataKey)
}

func (m *FileLockManager) CheckWrite(inode *Inode, fh *FileHandle) error {
	if !m.enabled || inode.isDir() {
		return nil
	}
	if fh != nil {
		fh.waitLockReady()
	}
	dataKey := lockSubjectDataKey(inode)
	if dataKey == "" {
		return nil
	}
	if m.hasLocalRef(dataKey) {
		if fh != nil {
			fh.lockHeld = true
		}
		return nil
	}
	if subject := lockSubjectInode(m.fs, inode); subject != nil {
		if atomic.LoadInt32(&subject.lockInodeHeld) != lockFlagOff {
			if fh != nil {
				fh.lockHeld = true
			}
			return nil
		}
	}
	if fh != nil && fh.lockHeld {
		return nil
	}
	if fh != nil && fh.writeBlocked {
		lockLog.Debugf("CheckWrite %v: EACCES (writeBlocked)", dataKey)
		return syscallEACCES()
	}
	busy, _, err := m.isBusyByOther(dataKey)
	if err != nil {
		s3Log.Warnf("lock busy check on write %v: %v", dataKey, err)
		return syscallEACCES()
	}
	if busy {
		if fh != nil {
			fh.writeBlocked = true
		}
		lockLog.Debugf("CheckWrite %v: EACCES (busy)", dataKey)
		return syscallEACCES()
	}
	if fh != nil && !fh.lockHeld {
		result, err := m.tryAcquire(dataKey)
		if err != nil {
			s3Log.Warnf("lock acquire on write %v: %v", dataKey, err)
			return syscallEACCES()
		}
		if result == lockBusy {
			fh.writeBlocked = true
			lockLog.Debugf("CheckWrite %v: EACCES (acquire busy)", dataKey)
			return syscallEACCES()
		}
		fh.lockHeld = true
		m.markInodeLocked(inode, dataKey)
		lockLog.Debugf("CheckWrite %v: acquired on write", dataKey)
	}
	return nil
}

func (m *FileLockManager) CheckCreate(parent *Inode, name string) error {
	if !m.enabled {
		return nil
	}
	if isLockSidecarName(name) {
		return syscallEACCES()
	}
	return m.checkForeignLock(lockSubjectForChild(parent, name))
}

func (m *FileLockManager) CheckMkDir(parent *Inode, name string) error {
	if !m.enabled {
		return nil
	}
	return m.checkForeignLock(lockSubjectForChild(parent, name))
}

func (m *FileLockManager) checkForeignLock(dataKey string) error {
	if dataKey == "" {
		return nil
	}
	if m.hasLocalRef(dataKey) {
		return nil
	}
	busy, holder, err := m.isBusyByOther(dataKey)
	if err != nil {
		s3Log.Warnf("lock busy check on %v: %v", dataKey, err)
		return syscallEACCES()
	}
	if busy {
		lockLog.Debugf("foreign lock on %v (holder %q): EACCES", dataKey, holder)
		return syscallEACCES()
	}
	return nil
}

func (m *FileLockManager) isBusyByOther(dataKey string) (bool, string, error) {
	cloud, _ := m.fs.rootCloud()
	lk := lockKey(dataKey)

	_, err := cloud.HeadBlob(&HeadBlobInput{Key: lk})
	if err != nil {
		if mapAwsError(err) == syscallENOENT() {
			return false, "", nil
		}
		return false, "", err
	}

	rec, err := m.readLockRecord(cloud, lk)
	if err != nil {
		return false, "", err
	}
	if !rec.Held || m.lockExpired(rec) {
		return false, "", nil
	}
	if lockRecordReclaimable(rec, m.session, m.owner, m.client, m.lockExpired) {
		return false, "", nil
	}
	holder := rec.Owner
	if holder == "" {
		holder = rec.Client
	}
	return true, holder, nil
}

func (m *FileLockManager) tryAcquire(dataKey string) (lockAcquireResult, error) {
	cloud, _ := m.fs.rootCloud()
	lk := lockKey(dataKey)

	head, err := cloud.HeadBlob(&HeadBlobInput{Key: lk})
	if err != nil {
		if mapAwsError(err) == syscallENOENT() {
			lockLog.Debugf("tryAcquire %v: sidecar missing, create", dataKey)
			return m.createLock(cloud, lk)
		}
		return lockBusy, err
	}

	etag := ""
	if head.ETag != nil {
		etag = *head.ETag
	}

	rec, err := m.readLockRecord(cloud, lk)
	if err != nil {
		s3Log.Warnf("Invalid lock sidecar %v, treating as stale: %v", lk, err)
		return m.updateLock(cloud, lk, etag, true)
	}

	if !rec.Held || m.lockExpired(rec) {
		lockLog.Debugf("tryAcquire %v: stale lock, reclaim", dataKey)
		return m.updateLock(cloud, lk, etag, true)
	}
	if rec.Session == m.session {
		m.mu.Lock()
		state := m.held[dataKey]
		if state != nil {
			state.etag = etag
		}
		m.mu.Unlock()
		return lockAcquired, nil
	}
	if lockRecordReclaimable(rec, m.session, m.owner, m.client, m.lockExpired) {
		lockLog.Debugf("tryAcquire %v: reclaim on %q (was session %v)", dataKey, m.client, rec.Session)
		return m.updateLock(cloud, lk, etag, true)
	}
	lockLog.Debugf("tryAcquire %v: BUSY holder=%q client=%q session=%v", dataKey, rec.Owner, rec.Client, rec.Session)
	return lockBusy, nil
}

func (m *FileLockManager) createLock(cloud StorageBackend, lk string) (lockAcquireResult, error) {
	body, err := m.newLockBody()
	if err != nil {
		return lockBusy, err
	}
	ct := "application/json"
	tag := "geesefs-lock=true"
	resp, err := cloud.PutBlob(&PutBlobInput{
		Key:         lk,
		Body:        bytes.NewReader(body),
		Size:        PUInt64(uint64(len(body))),
		ContentType: &ct,
		IfNoneMatch: PString("*"),
		Tagging:     &tag,
	})
	if err != nil {
		if isPreconditionFailed(err) {
			return m.tryAcquire(dataKeyFromLockPath(lk))
		}
		return lockBusy, err
	}
	etag := ""
	if resp.ETag != nil {
		etag = *resp.ETag
	}
	m.storeHeldEtag(lk, etag)
	return lockAcquired, nil
}

func dataKeyFromLockPath(lk string) string {
	dir, base := pathSplitLock(lk)
	if !isLockSidecarName(base) {
		return lk
	}
	inner := strings.TrimSuffix(strings.TrimPrefix(base, "."), lockSidecarSuffix)
	if dir != "" {
		return dir + "/" + inner
	}
	return inner
}

func pathSplitLock(key string) (string, string) {
	i := len(key) - 1
	for i >= 0 && key[i] != '/' {
		i--
	}
	if i < 0 {
		return "", key
	}
	return key[:i], key[i+1:]
}

func (m *FileLockManager) updateLock(cloud StorageBackend, lk, etag string, held bool) (lockAcquireResult, error) {
	var body []byte
	var err error
	if held {
		body, err = m.newLockBody()
	} else {
		body, err = json.Marshal(lockRecord{Version: 1, Held: false})
	}
	if err != nil {
		return lockBusy, err
	}
	ct := "application/json"
	tag := "geesefs-lock=true"
	in := &PutBlobInput{
		Key:         lk,
		Body:        bytes.NewReader(body),
		Size:        PUInt64(uint64(len(body))),
		ContentType: &ct,
		Tagging:     &tag,
	}
	if etag != "" {
		in.IfMatch = &etag
	}
	resp, err := cloud.PutBlob(in)
	if err != nil {
		if isPreconditionFailed(err) {
			if held {
				return lockBusy, nil
			}
			return lockAcquired, nil
		}
		return lockBusy, err
	}
	if held {
		newEtag := ""
		if resp.ETag != nil {
			newEtag = *resp.ETag
		}
		m.storeHeldEtag(lk, newEtag)
		return lockAcquired, nil
	}
	return lockAcquired, nil
}

func (m *FileLockManager) releaseKeyAsync(dataKey string) {
	m.releaseWg.Add(1)
	go func() {
		defer m.releaseWg.Done()
		m.releaseKey(dataKey)
	}()
}

func (m *FileLockManager) releaseKey(dataKey string) {
	cloud, _ := m.fs.rootCloud()
	lk := lockKey(dataKey)

	head, err := cloud.HeadBlob(&HeadBlobInput{Key: lk})
	if err != nil {
		lockLog.Debugf("release %v: head failed: %v", dataKey, err)
		return
	}
	etag := ""
	if head.ETag != nil {
		etag = *head.ETag
	}
	rec, err := m.readLockRecord(cloud, lk)
	if err != nil {
		lockLog.Debugf("release %v: read record failed: %v", dataKey, err)
		return
	}
	if rec.Session != m.session {
		lockLog.Debugf("release %v: skip (session %v != ours %v)", dataKey, rec.Session, m.session)
		return
	}
	_, err = m.updateLock(cloud, lk, etag, false)
	if err != nil {
		lockLog.Debugf("release %v: update failed: %v", dataKey, err)
		return
	}
	lockLog.Debugf("release %v: sidecar released", dataKey)
}

func (m *FileLockManager) heartbeatLoop(interval time.Duration) {
	// One ticker per mount; each tick renews expires_at on every sidecar in held.
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for atomic.LoadInt32(&m.fs.shutdown) == 0 {
		select {
		case <-m.fs.shutdownCh:
			return
		case <-ticker.C:
			m.heartbeat()
		}
	}
}

func (m *FileLockManager) heartbeat() {
	m.mu.Lock()
	snapshot := make([]*heldFileLock, 0, len(m.held))
	dataKeys := make([]string, 0, len(m.held))
	for dataKey, state := range m.held {
		snapshot = append(snapshot, state)
		dataKeys = append(dataKeys, dataKey)
	}
	m.mu.Unlock()

	cloud, _ := m.fs.rootCloud()
	for i, state := range snapshot {
		head, err := cloud.HeadBlob(&HeadBlobInput{Key: state.lockKey})
		if err != nil {
			continue
		}
		etag := state.etag
		if head.ETag != nil {
			etag = *head.ETag
		}
		rec, err := m.readLockRecord(cloud, state.lockKey)
		if err != nil || rec.Session != m.session || !rec.Held {
			continue
		}
		result, err := m.updateLock(cloud, state.lockKey, etag, true)
		if err != nil || result == lockBusy {
			s3Log.Warnf("Lock heartbeat lost for %v", dataKeys[i])
		}
	}
}

func (m *FileLockManager) newLockBody() ([]byte, error) {
	expires := time.Now().UTC().Add(m.fs.flags.LockTTL).Format(time.RFC3339)
	rec := lockRecord{
		Version:   1,
		Held:      true,
		Owner:     m.owner,
		Session:   m.session,
		Client:    m.client,
		ExpiresAt: expires,
	}
	return json.Marshal(rec)
}

func (m *FileLockManager) readLockRecord(cloud StorageBackend, lk string) (*lockRecord, error) {
	resp, err := cloud.GetBlob(&GetBlobInput{Key: lk})
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}
	var rec lockRecord
	if err := json.Unmarshal(data, &rec); err != nil {
		return nil, err
	}
	if rec.Version != 1 {
		return nil, fmt.Errorf("unsupported lock version %d", rec.Version)
	}
	return &rec, nil
}

func (m *FileLockManager) lockExpired(rec *lockRecord) bool {
	if rec.ExpiresAt == "" {
		return false
	}
	t, err := time.Parse(time.RFC3339, rec.ExpiresAt)
	if err != nil {
		return true
	}
	return time.Now().UTC().After(t)
}

func (m *FileLockManager) storeHeldEtag(lk, etag string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, state := range m.held {
		if state.lockKey == lk {
			state.etag = etag
			return
		}
	}
}

func (fs *Goofys) locksCheckWrite(inode *Inode, fh *FileHandle) error {
	return fs.locks.CheckWrite(inode, fh)
}

func (fs *Goofys) locksOnOpen(inode *Inode, writeIntent bool, fh *FileHandle) error {
	return fs.locks.OnOpen(inode, writeIntent, fh)
}

func (fs *Goofys) locksCheckCreate(parent *Inode, name string) error {
	return fs.locks.CheckCreate(parent, name)
}

func (fs *Goofys) locksCheckMkDir(parent *Inode, name string) error {
	return fs.locks.CheckMkDir(parent, name)
}

func isPreconditionFailed(err error) bool {
	if reqErr, ok := err.(awserr.RequestFailure); ok {
		return reqErr.StatusCode() == http.StatusPreconditionFailed
	}
	return false
}

func (fs *Goofys) rootCloud() (StorageBackend, string) {
	fs.mu.RLock()
	root := fs.inodes[fuseops.RootInodeID]
	fs.mu.RUnlock()
	if root == nil {
		return nil, ""
	}
	return root.cloud()
}

// Helpers to avoid importing syscall in tests from multiple files - use existing syscall package
func syscallENOENT() error {
	return syscall.ENOENT
}

func syscallEACCES() error {
	return syscall.EACCES
}
