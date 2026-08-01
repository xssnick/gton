package liveview

import (
	"bytes"
	"errors"
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

	if got, err := live.nonfinalCellDataLocked(second, hash); err != nil || !bytes.Equal(got, secondData) {
		t.Fatalf("second block cell data = %x, want %x", got, secondData)
	}
	if got, err := live.nonfinalCellDataLocked(first, hash); err != nil || !bytes.Equal(got, firstData) {
		t.Fatalf("first block cell data = %x, want %x", got, firstData)
	}

	live.deleteNonfinalPendingLocked(secondKey)
	if _, err := live.nonfinalCellDataLocked(second, hash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("deleted second block cell data error = %v, want ErrNotFound", err)
	}
	if got, err := live.nonfinalCellDataLocked(first, hash); err != nil || !bytes.Equal(got, firstData) {
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
		if idx, err := live.nonfinalOrderIndexLocked(key); err != nil || idx != i {
			t.Fatalf("pending index for %d = %d, want %d", i, idx, i)
		}
	}

	live.deleteNonfinalPendingLocked(storage.BlockKey(blocks[1]))
	if idx, err := live.nonfinalOrderIndexLocked(storage.BlockKey(blocks[2])); err != nil || idx != 1 {
		t.Fatalf("tail pending index after middle delete = %d, want 1", idx)
	}
	if _, err := live.nonfinalOrderIndexLocked(storage.BlockKey(blocks[1])); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("deleted pending index error = %v, want ErrNotFound", err)
	}

	live.deleteNonfinalPendingLocked(storage.BlockKey(blocks[0]))
	if idx, err := live.nonfinalOrderIndexLocked(storage.BlockKey(blocks[2])); err != nil || idx != 0 {
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
