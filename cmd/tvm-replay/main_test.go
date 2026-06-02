package main

import (
	"bytes"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

func TestReplayExecutionMasterUsesShardHeaderMasterRef(t *testing.T) {
	fallback := ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     102,
		RootHash:  bytes.Repeat([]byte{0x10}, 32),
		FileHash:  bytes.Repeat([]byte{0x11}, 32),
	}

	header := tlb.BlockHeader{}
	header.NotMaster = true
	header.MasterRef = &tlb.ExtBlkRef{
		SeqNo:    99,
		RootHash: bytes.Repeat([]byte{0x20}, 32),
		FileHash: bytes.Repeat([]byte{0x21}, 32),
	}
	got, err := replayExecutionMaster(loadedBlock{
		ID: ton.BlockIDExt{
			Workchain: 0,
			Shard:     masterchainShard,
			SeqNo:     7,
		},
		Parsed: &tlb.Block{BlockInfo: header},
	}, fallback)
	if err != nil {
		t.Fatalf("replayExecutionMaster failed: %v", err)
	}

	want := ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     99,
		RootHash:  bytes.Repeat([]byte{0x20}, 32),
		FileHash:  bytes.Repeat([]byte{0x21}, 32),
	}
	if !got.Equals(&want) {
		t.Fatalf("execution master = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(want))
	}
}

func TestReplayExecutionMasterKeepsMasterFallback(t *testing.T) {
	fallback := ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     102,
		RootHash:  bytes.Repeat([]byte{0x10}, 32),
		FileHash:  bytes.Repeat([]byte{0x11}, 32),
	}

	got, err := replayExecutionMaster(loadedBlock{
		ID:     fallback,
		Parsed: &tlb.Block{},
	}, fallback)
	if err != nil {
		t.Fatalf("replayExecutionMaster failed: %v", err)
	}
	if !got.Equals(&fallback) {
		t.Fatalf("execution master = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(fallback))
	}
}
