package core

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"testing"

	"github.com/tidwall/btree"
)

func newMockBufferList(partSize uint64, numParts int) BufferList {
	m := BufferList{
		at: btree.Map[uint64, *FileBuffer]{},
	}

	for i := 0; i < numParts; i++ {
		offset := uint64(i) * partSize
		data := bytes.Repeat([]byte{byte(i)}, int(partSize))

		m.at.Set(offset+partSize, &FileBuffer{
			offset: offset,
			length: partSize,
			data:   data,
			state:  BUF_FL_CLEARED,
		})
	}

	return m
}

func TestHashFlushedPart_Order(t *testing.T) {
	const partSize = 100
	const numParts = 10
	const fileSize = partSize * numParts

	// Prepare some data
	var expected []byte
	for i := 0; i < numParts; i++ {
		expected = append(expected, bytes.Repeat([]byte{byte(i)}, partSize)...)
	}
	expectedHash := sha256.Sum256(expected)

	// In-order test
	inode := &Inode{
		Attributes: InodeAttributes{Size: fileSize},
		buffers:    newMockBufferList(partSize, numParts),
	}

	for i := 0; i < numParts; i++ {
		if err := inode.hashFlushedPart(uint64(i)*partSize, partSize); err != nil {
			t.Fatalf("in-order: error hashing part %d: %v", i, err)
		}
	}

	inode.hashLock.Lock()
	got := inode.hashInProgress.Sum(nil)
	inode.hashLock.Unlock()

	if hex.EncodeToString(got) != hex.EncodeToString(expectedHash[:]) {
		t.Errorf("in-order: hash mismatch: got %x, want %x", got, expectedHash)
	}

	// Out-of-order test
	inode = &Inode{
		Attributes: InodeAttributes{Size: fileSize},
		buffers:    newMockBufferList(partSize, numParts),
	}
	order := []int{3, 0, 2, 1, 4, 5, 6, 7, 8, 9}
	for _, i := range order {
		if err := inode.hashFlushedPart(uint64(i)*partSize, partSize); err != nil {
			t.Fatalf("out-of-order: error hashing part %d: %v", i, err)
		}
	}

	inode.hashLock.Lock()
	got = inode.hashInProgress.Sum(nil)
	inode.hashLock.Unlock()

	if hex.EncodeToString(got) != hex.EncodeToString(expectedHash[:]) {
		t.Errorf("out-of-order: hash mismatch: got %x, want %x", got, expectedHash)
	}
}

func TestHashFlushedPart_KnownHash(t *testing.T) {
	const partSize = 64000
	const numParts = 5
	const fileSize = partSize * numParts

	// Create a known pattern to check against
	var pattern []byte
	for i := 0; i < numParts; i++ {
		pattern = append(pattern, bytes.Repeat([]byte{byte(0xA0 + i)}, partSize)...)
	}
	expectedSHA256 := sha256.Sum256(pattern)

	newPatternBufferList := func() BufferList {
		m := BufferList{
			at: btree.Map[uint64, *FileBuffer]{},
		}
		for i := 0; i < numParts; i++ {
			offset := uint64(i) * partSize
			data := bytes.Repeat([]byte{byte(0xA0 + i)}, int(partSize))
			m.at.Set(offset+partSize, &FileBuffer{
				offset: offset,
				length: partSize,
				data:   data,
				state:  BUF_FL_CLEARED,
			})
		}
		return m
	}

	// In-order SHA-256 test
	inode := &Inode{
		Attributes: InodeAttributes{Size: fileSize},
		buffers:    newPatternBufferList(),
	}
	for i := 0; i < numParts; i++ {
		if err := inode.hashFlushedPart(uint64(i)*partSize, partSize); err != nil {
			t.Fatalf("sha1 in-order: error hashing part %d: %v", i, err)
		}
	}

	inode.hashLock.Lock()
	gotSHA256 := inode.hashInProgress.Sum(nil)
	inode.hashLock.Unlock()
	if hex.EncodeToString(gotSHA256) != hex.EncodeToString(expectedSHA256[:]) {
		t.Errorf("sha256 in-order: hash mismatch: got %x, want %x", gotSHA256, expectedSHA256)
	}

	// Out-of-order SHA-256 test
	inode = &Inode{
		Attributes: InodeAttributes{Size: fileSize},
		buffers:    newPatternBufferList(),
	}
	order := []int{2, 0, 1, 4, 3}
	for _, i := range order {
		if err := inode.hashFlushedPart(uint64(i)*partSize, partSize); err != nil {
			t.Fatalf("sha1 out-of-order: error hashing part %d: %v", i, err)
		}
	}

	inode.hashLock.Lock()
	gotSHA256 = inode.hashInProgress.Sum(nil)
	inode.hashLock.Unlock()

	if hex.EncodeToString(gotSHA256) != hex.EncodeToString(expectedSHA256[:]) {
		t.Errorf("sha256 out-of-order: hash mismatch: got %x, want %x", gotSHA256, expectedSHA256)
	}
}
