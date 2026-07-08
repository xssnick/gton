package liteserver

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

func TestCloneZeroStateCopiesHashes(t *testing.T) {
	zero := ton.ZeroStateIDExt{
		Workchain: -1,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}

	cloned := cloneZeroState(zero)
	cloned.RootHash[0] = 0xAA
	cloned.FileHash[0] = 0xBB

	if zero.RootHash[0] == 0xAA {
		t.Fatal("clone shares root hash backing array")
	}
	if zero.FileHash[0] == 0xBB {
		t.Fatal("clone shares file hash backing array")
	}
}
