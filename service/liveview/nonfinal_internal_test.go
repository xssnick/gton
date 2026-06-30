package liveview

import (
	"bytes"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestNonfinalCellIndexUsesNearestPendingBlock(t *testing.T) {
	live := &Store{
		nonFinalPending:    map[storage.BlockRootHash]liveNonfinalPending{},
		nonFinalOrderIndex: map[storage.BlockRootHash]int{},
		nonFinalCellIndex:  map[cell.Hash][]liveNonfinalCellIndexEntry{},
	}

	var hash cell.Hash
	hash[0] = 0x91
	firstData := []byte{0x01}
	secondData := []byte{0x02}

	first := testNonfinalIndexBlock(11, masterchainShard)
	second := testNonfinalIndexBlock(12, masterchainShard)
	firstKey := storage.BlockKey(first)
	secondKey := storage.BlockKey(second)

	live.putNonfinalPendingLocked(firstKey, liveNonfinalPending{
		block: first,
		cells: storage.NewStateCellRecords([]storage.EncodedCellRecord{{Hash: hash, Data: firstData}}),
	})
	live.putNonfinalPendingLocked(secondKey, liveNonfinalPending{
		block: second,
		cells: storage.NewStateCellRecords([]storage.EncodedCellRecord{{Hash: hash, Data: secondData}}),
	})

	if got := live.nonfinalCellDataLocked(second, hash); !bytes.Equal(got, secondData) {
		t.Fatalf("second block cell data = %x, want %x", got, secondData)
	}
	if got := live.nonfinalCellDataLocked(first, hash); !bytes.Equal(got, firstData) {
		t.Fatalf("first block cell data = %x, want %x", got, firstData)
	}

	live.deleteNonfinalPendingLocked(secondKey)
	if got := live.nonfinalCellDataLocked(second, hash); got != nil {
		t.Fatalf("deleted second block cell data = %x, want nil", got)
	}
	if got := live.nonfinalCellDataLocked(first, hash); !bytes.Equal(got, firstData) {
		t.Fatalf("first block cell data after cleanup = %x, want %x", got, firstData)
	}
	if entries := live.nonFinalCellIndex[hash]; len(entries) != 1 || entries[0].block != firstKey {
		t.Fatalf("cell index after second cleanup = %#v, want only first block", entries)
	}

	live.deleteNonfinalPendingLocked(firstKey)
	if entries := live.nonFinalCellIndex[hash]; len(entries) != 0 {
		t.Fatalf("cell index after full cleanup = %#v, want empty", entries)
	}
}

func TestNonfinalOrderIndexTracksDeletes(t *testing.T) {
	live := &Store{
		nonFinalPending:    map[storage.BlockRootHash]liveNonfinalPending{},
		nonFinalOrder:      make([]storage.BlockRootHash, 0, 3),
		nonFinalOrderIndex: map[storage.BlockRootHash]int{},
	}
	blocks := []ton.BlockIDExt{
		testNonfinalIndexBlock(1, 1),
		testNonfinalIndexBlock(2, 2),
		testNonfinalIndexBlock(3, 3),
	}

	live.mu.Lock()
	for i, block := range blocks {
		key := storage.BlockKey(block)
		live.putNonfinalPendingLocked(key, liveNonfinalPending{block: block})
		if idx := live.nonfinalOrderIndexLocked(key); idx != i {
			t.Fatalf("pending index for %d = %d, want %d", i, idx, i)
		}
	}

	live.deleteNonfinalPendingLocked(storage.BlockKey(blocks[1]))
	if idx := live.nonfinalOrderIndexLocked(storage.BlockKey(blocks[2])); idx != 1 {
		t.Fatalf("tail pending index after middle delete = %d, want 1", idx)
	}
	if idx := live.nonfinalOrderIndexLocked(storage.BlockKey(blocks[1])); idx != -1 {
		t.Fatalf("deleted pending index = %d, want -1", idx)
	}

	live.deleteNonfinalPendingLocked(storage.BlockKey(blocks[0]))
	if idx := live.nonfinalOrderIndexLocked(storage.BlockKey(blocks[2])); idx != 0 {
		t.Fatalf("tail pending index after head delete = %d, want 0", idx)
	}
	live.mu.Unlock()
}

func testNonfinalIndexBlock(seqno uint32, shard int64) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{byte(seqno)}, 32),
		FileHash:  bytes.Repeat([]byte{byte(seqno + 0x10)}, 32),
	}
}
