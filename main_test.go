package main

import (
	"errors"
	"reflect"
	"syscall"
	"testing"
)

// fakeProcess mimics the kernel's rules for a process that starts as root:
// setuid always works while root, and setgid fails with EPERM once the
// process is no longer root.
type fakeProcess struct {
	uid, gid  int
	calls     []string
	setuidErr error
}

func (p *fakeProcess) setuid(uid int) error {
	p.calls = append(p.calls, "setuid")
	if p.setuidErr != nil {
		return p.setuidErr
	}
	if p.uid != 0 && uid != p.uid {
		return syscall.EPERM
	}
	p.uid = uid
	return nil
}

func (p *fakeProcess) setgid(gid int) error {
	p.calls = append(p.calls, "setgid")
	if p.uid != 0 && gid != p.gid {
		return syscall.EPERM
	}
	p.gid = gid
	return nil
}

func TestDropPrivileges(t *testing.T) {
	errFailed := errors.New("setuid failed")

	cases := []struct {
		description string
		startUID    int
		uid, gid    int
		setuidErr   error
		expectedUID int
		expectedGID int
		calls       []string
		expectErr   bool
	}{
		// Positive: both ids are dropped, group first.
		{
			description: "positive: uid and gid are both dropped",
			uid:         1000, gid: 1000,
			expectedUID: 1000, expectedGID: 1000,
			calls: []string{"setgid", "setuid"},
		},
		{
			description: "positive: different uid and gid are both dropped",
			uid:         1000, gid: 2000,
			expectedUID: 1000, expectedGID: 2000,
			calls: []string{"setgid", "setuid"},
		},
		// Negative: nothing requested, nothing changed.
		{
			description: "negative: zero uid and gid make no calls",
			uid:         0, gid: 0,
			expectedUID: 0, expectedGID: 0,
			calls: nil,
		},
		{
			description: "negative: setuid error is returned",
			uid:         1000, gid: 1000, setuidErr: errFailed,
			expectedUID: 0, expectedGID: 1000,
			calls:     []string{"setgid", "setuid"},
			expectErr: true,
		},
		// Boundary: only one of the two ids is set.
		{
			description: "boundary: only gid is dropped",
			uid:         0, gid: 1000,
			expectedUID: 0, expectedGID: 1000,
			calls: []string{"setgid"},
		},
		{
			description: "boundary: only uid is dropped",
			uid:         1000, gid: 0,
			expectedUID: 1000, expectedGID: 0,
			calls: []string{"setuid"},
		},
		{
			description: "boundary: id 1 is dropped",
			uid:         1, gid: 1,
			expectedUID: 1, expectedGID: 1,
			calls: []string{"setgid", "setuid"},
		},
		// Corner: the nobody ids, and a uid that matches the gid's number.
		{
			description: "corner: nobody/nogroup 65534 are dropped",
			uid:         65534, gid: 65534,
			expectedUID: 65534, expectedGID: 65534,
			calls: []string{"setgid", "setuid"},
		},
		{
			description: "corner: already non-root, changing group reports EPERM",
			startUID:    1000,
			uid:         1000, gid: 2000,
			expectedUID: 1000, expectedGID: 0,
			calls:     []string{"setgid", "setuid"},
			expectErr: true,
		},
	}

	for _, tc := range cases {
		t.Run(tc.description, func(t *testing.T) {
			p := &fakeProcess{uid: tc.startUID, setuidErr: tc.setuidErr}
			err := dropPrivileges(tc.uid, tc.gid, p.setuid, p.setgid)
			if (err != nil) != tc.expectErr {
				t.Errorf("error = %v, expected error: %v", err, tc.expectErr)
			}
			if p.uid != tc.expectedUID || p.gid != tc.expectedGID {
				t.Errorf("uid/gid = %d/%d, expected %d/%d", p.uid, p.gid, tc.expectedUID, tc.expectedGID)
			}
			if !reflect.DeepEqual(p.calls, tc.calls) {
				t.Errorf("calls = %v, expected %v", p.calls, tc.calls)
			}
		})
	}
}
