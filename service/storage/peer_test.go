package storage

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

func TestServedBlockFullCloneCopiesBlockIDHashes(t *testing.T) {
	original := &ServedBlockFull{
		ID: ton.BlockIDExt{
			Workchain: 0,
			Shard:     masterchainShard,
			SeqNo:     21,
			RootHash:  bytes.Repeat([]byte{0x11}, 32),
			FileHash:  bytes.Repeat([]byte{0x22}, 32),
		},
	}

	cloned := original.Clone()
	cloned.ID.RootHash[0] = 0xAA
	cloned.ID.FileHash[0] = 0xBB

	if original.ID.RootHash[0] == 0xAA {
		t.Fatal("clone shares block id root hash backing array")
	}
	if original.ID.FileHash[0] == 0xBB {
		t.Fatal("clone shares block id file hash backing array")
	}
}
