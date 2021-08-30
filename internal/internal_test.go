// Just a check.v1 wrapper to allow running selected tests with:
// go test -v internal_test.go lfru_btree_test.go lfru_btree.go

package internal

import (
	"testing"
	. "gopkg.in/check.v1"
)

func Test(t *testing.T) {
	TestingT(t)
}
