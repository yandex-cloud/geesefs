// Just a check.v1 wrapper to allow running selected tests with:
// go test -v internal_test.go lfru_btree_test.go lfru_btree.go

package core

import (
	. "gopkg.in/check.v1"
	"testing"
)

func Test(t *testing.T) {
	TestingT(t)
}
