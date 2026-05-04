package core

import (
	"testing"

	"github.com/yandex-cloud/geesefs/core/cfg"
)

func TestPartRangeTierBoundaries(t *testing.T) {
	flags := cfg.DefaultFlags()
	fs := &Goofys{flags: flags}

	const MiB = uint64(1024 * 1024)

	checks := []struct {
		part       uint64
		wantOffset uint64
		wantSize   uint64
	}{
		{part: 999, wantOffset: 4995 * MiB, wantSize: 5 * MiB},
		{part: 1000, wantOffset: 5000 * MiB, wantSize: 25 * MiB},
		{part: 1001, wantOffset: 5025 * MiB, wantSize: 25 * MiB},
		{part: 1999, wantOffset: 29975 * MiB, wantSize: 25 * MiB},
		{part: 2000, wantOffset: 30000 * MiB, wantSize: 125 * MiB},
	}

	for _, check := range checks {
		offset, size := fs.partRange(check.part)
		if offset != check.wantOffset || size != check.wantSize {
			t.Fatalf("partRange(%d) = (%d, %d), want (%d, %d)",
				check.part, offset, size, check.wantOffset, check.wantSize)
		}
	}
}

func TestNumPartsOnPartBoundaries(t *testing.T) {
	flags := cfg.DefaultFlags()
	fs := &Goofys{flags: flags}

	const MiB = uint64(1024 * 1024)

	checks := []struct {
		size uint64
		want uint64
	}{
		{size: 0, want: 0},
		{size: 1, want: 1},
		{size: 5 * MiB, want: 1},
		{size: 5*MiB + 1, want: 2},
		{size: 5000 * MiB, want: 1000},
		{size: 5000*MiB + 1, want: 1001},
		{size: 5025 * MiB, want: 1001},
		{size: 30000 * MiB, want: 2000},
		{size: fs.getMaxFileSize(), want: 10000},
	}

	for _, check := range checks {
		parts := fs.numParts(check.size)
		if parts != check.want {
			t.Fatalf("numParts(%d) = %d, want %d", check.size, parts, check.want)
		}
	}
}

func TestNumPartsWithTenThousandFiveMiBParts(t *testing.T) {
	const MiB = uint64(1024 * 1024)

	flags := cfg.DefaultFlags()
	flags.PartSizes = []cfg.PartSizeConfig{
		{PartSize: 5 * MiB, PartCount: 10000},
	}
	fs := &Goofys{flags: flags}

	const fileSize = 50000 * MiB

	if got := fs.partNum(fileSize); got != 10000 {
		t.Fatalf("partNum(%d) = %d, want 10000", fileSize, got)
	}
	if got := fs.numParts(fileSize); got != 10000 {
		t.Fatalf("numParts(%d) = %d, want 10000", fileSize, got)
	}

	offset, size := fs.partRange(9999)
	if offset != 49995*MiB || size != 5*MiB {
		t.Fatalf("partRange(9999) = (%d, %d), want (%d, %d)",
			offset, size, uint64(49995)*MiB, uint64(5)*MiB)
	}

	assertPanics(t, func() {
		fs.partRange(10000)
	})
}

func assertPanics(t *testing.T, f func()) {
	t.Helper()

	defer func() {
		if recover() == nil {
			t.Fatalf("function did not panic")
		}
	}()

	f()
}
