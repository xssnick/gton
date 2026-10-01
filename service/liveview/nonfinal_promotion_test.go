package liveview

import (
	"bytes"
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestNonfinalCandidateRootLivesUntilCurrentStateCoversBlock(t *testing.T) {
	live, _ := acceptedStateStore(t, Options{
		MasterBlockCache: 4,
		ShardBlockCache:  64,
		NonFinalEnabled:  true,
		NonFinalCache:    64,
	})
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+1, 0xa1)
	previous := testLiveBlockID(0, acceptedStateShardID(), acceptedStateAppliedSeqno, 0x60)
	artifacts := fixture.ingestArtifacts(t, appliedShardTopState(0x60), previous)

	if err := live.PublishNonfinalBlockArtifacts(artifacts, storage.LiveBlockNonfinalCandidate); err != nil {
		t.Fatalf("publish non-final candidate: %v", err)
	}
	root, err := live.BlockRoot(context.Background(), fixture.block)
	if err != nil {
		t.Fatalf("read candidate block root: %v", err)
	}
	if root != fixture.root {
		t.Fatal("live view did not retain the decoded candidate root")
	}

	advanceAppliedShardTop(t, live, fixture.block.SeqNo, 901, 0xa2)

	live.mu.RLock()
	retained := live.blocks[storage.BlockKey(fixture.block)]
	live.mu.RUnlock()
	if retained != nil {
		t.Fatal("candidate root remained live after current state covered its slot")
	}
	if _, err = live.BlockRoot(context.Background(), fixture.block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("released candidate root error = %v, want ErrNotFound", err)
	}
}

func BenchmarkPromoteNonfinalWaitingBlockedCandidate(b *testing.B) {
	live, _ := testNonfinalBlockedCandidate(b, 5)

	b.ReportAllocs()
	b.ResetTimer()
	for b.Loop() {
		live.promoteNonfinalWaiting()
	}
}

func TestRememberNonfinalWaitingInvalidatesChangedStateUpdate(t *testing.T) {
	live := New(noopBacking{}, Options{NonFinalEnabled: true, NonFinalCache: 8})
	first := testNonfinalMerkleUpdate(t, 0)
	second := testNonfinalMerkleUpdate(t, 1)
	root := cell.BeginCell().MustStoreUInt(0x81, 8).EndCell()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     masterchainShard,
		SeqNo:     12,
		RootHash:  root.Hash(),
		FileHash:  bytes.Repeat([]byte{0x82}, 32),
	}
	artifacts := storage.LiveBlockArtifacts{
		Block:       block,
		Root:        root,
		StateUpdate: first,
	}
	key := storage.BlockKey(block)

	live.mu.Lock()
	defer live.mu.Unlock()

	live.rememberNonfinalWaitingLocked(artifacts, storage.LiveBlockNonfinalCandidate, first)
	if got := live.nonFinalWaiting[key].validatedStateUpdate; got != first {
		t.Fatalf("validated state update = %p, want first %p", got, first)
	}

	live.rememberNonfinalWaitingLocked(artifacts, storage.LiveBlockNonfinalCandidate, nil)
	if got := live.nonFinalWaiting[key].validatedStateUpdate; got != first {
		t.Fatalf("validated state update after same update = %p, want first %p", got, first)
	}

	artifacts.StateUpdate = second
	live.rememberNonfinalWaitingLocked(artifacts, storage.LiveBlockNonfinalCandidate, first)
	if got := live.nonFinalWaiting[key].validatedStateUpdate; got != nil {
		t.Fatalf("validated state update after replacement = %p, want nil", got)
	}
}

func TestNonfinalParseTrustedUpdateDoesNotCreateValidationMemo(t *testing.T) {
	update := testNonfinalMerkleUpdate(t, 0)
	root := cell.BeginCell().MustStoreUInt(0x91, 8).EndCell()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     masterchainShard,
		SeqNo:     12,
		RootHash:  root.Hash(),
		FileHash:  bytes.Repeat([]byte{0x92}, 32),
	}

	parsed, err := nonfinalParseStateUpdate(storage.LiveBlockArtifacts{
		Block:       block,
		Root:        root,
		StateUpdate: update,
		Meta: &storage.BlockMeta{
			ID:            block,
			StateRootHash: bytes.Repeat([]byte{0x93}, 32),
		},
	}, true, nil)
	if err != nil {
		t.Fatalf("parse trusted update: %v", err)
	}
	if parsed.validatedStateUpdate != nil {
		t.Fatal("trusted update created standalone validation memo")
	}
}

func testNonfinalBlockedCandidate(tb testing.TB, depth int) (*Store, storage.LiveBlockArtifacts) {
	tb.Helper()

	update := testNonfinalMerkleUpdate(tb, depth)
	root := cell.BeginCell().MustStoreUInt(0x7a, 8).EndCell()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     masterchainShard,
		SeqNo:     12,
		RootHash:  root.Hash(),
		FileHash:  bytes.Repeat([]byte{0x7b}, 32),
	}
	previous := testNonfinalIndexBlock(11, masterchainShard)
	artifacts := storage.LiveBlockArtifacts{
		Block:       block,
		Root:        root,
		StateUpdate: update,
		Meta: &storage.BlockMeta{
			ID:            block,
			PrevRefs:      []ton.BlockIDExt{previous},
			StateRootHash: bytes.Repeat([]byte{0x7c}, 32),
		},
	}

	live := New(noopBacking{}, Options{
		MasterBlockCache: 8,
		ShardBlockCache:  8,
		NonFinalEnabled:  true,
		NonFinalCache:    8,
	})
	if err := live.PublishNonfinalBlockArtifacts(artifacts, storage.LiveBlockNonfinalCandidate); err != nil {
		tb.Fatalf("publish blocked candidate: %v", err)
	}
	if _, ok := live.nonFinalWaiting[storage.BlockKey(block)]; !ok {
		tb.Fatal("blocked candidate was not remembered")
	}
	return live, artifacts
}

func testNonfinalMerkleUpdate(tb testing.TB, depth int) *cell.Cell {
	tb.Helper()

	var value uint64
	var build func(int, uint64) *cell.Cell
	build = func(remaining int, salt uint64) *cell.Cell {
		value++
		builder := cell.BeginCell().MustStoreUInt(value^salt, 64)
		if remaining > 0 {
			for i := 0; i < 4; i++ {
				builder.MustStoreRef(build(remaining-1, salt))
			}
		}
		return builder.EndCell()
	}

	from := build(depth, 0)
	to := build(depth, 1<<63)
	update, err := cell.CreateMerkleUpdate(from, to)
	if err != nil {
		tb.Fatalf("create merkle update: %v", err)
	}
	return update
}
