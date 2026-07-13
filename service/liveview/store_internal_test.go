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

func TestStoreCheckpointFlushDoesNotRememberMissingBlocks(t *testing.T) {
	missing := ton.BlockIDExt{
		Workchain: 0,
		Shard:     1 << 3,
		SeqNo:     35,
		RootHash:  bytes.Repeat([]byte{0x35}, 32),
		FileHash:  bytes.Repeat([]byte{0x36}, 32),
	}
	live := New(noopBacking{})

	live.MarkLiveBlockStatesFlushed([]ton.BlockIDExt{missing})
	if len(live.flushed) != 0 {
		t.Fatalf("missing checkpoint-flushed blocks remembered = %d, want 0", len(live.flushed))
	}
}

func TestStorePublishLiveBlockArtifactsRejectsInvalidReadFragments(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x31, 8).EndCell()
	stateRoot := cell.BeginCell().MustStoreUInt(0x32, 8).EndCell()
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     1 << 62,
		SeqNo:     35,
		RootHash:  root.Hash(),
		FileHash:  bytes.Repeat([]byte{0x36}, 32),
	}
	state := storage.BlockState{
		Block:         block,
		StateRootHash: stateRoot.Hash(),
		Cell:          stateRoot,
	}
	artifacts := storage.LiveBlockArtifacts{
		Block: block,
		Root:  root,
		State: &state,
	}

	t.Run("read artifacts", func(t *testing.T) {
		live := New(noopBacking{})

		if err := live.PublishLiveBlockArtifacts(artifacts); err == nil {
			t.Fatal("unparseable read fragments were accepted")
		}
		if _, err := live.BlockRoot(t.Context(), block); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("block root after rejected publish error = %v, want ErrNotFound", err)
		}
		if _, err := live.BlockState(t.Context(), block); !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("block state after rejected publish error = %v, want ErrNotFound", err)
		}
	})

	t.Run("availability only", func(t *testing.T) {
		live := New(noopBacking{})
		availabilityArtifacts := artifacts
		availabilityArtifacts.AvailabilityOnly = true

		if err := live.PublishLiveBlockArtifacts(availabilityArtifacts); err != nil {
			t.Fatalf("publish availability-only artifacts: %v", err)
		}
		if _, err := live.BlockRoot(t.Context(), block); err != nil {
			t.Fatalf("load availability-only block root: %v", err)
		}
	})
}

func TestStoreReleasesCurrentCachesWhenBlockLeavesCurrent(t *testing.T) {
	retiredBlock := testLiveBlockID(-1, masterchainShard, 40, 0x40)
	currentBlock := testLiveBlockID(-1, masterchainShard, 41, 0x41)
	keptShard := testLiveBlockID(0, int64(1)<<62, 70, 0x70)
	proof := cell.BeginCell().MustStoreUInt(1, 1).EndCell()

	retired := &BlockView{
		retainCurrentCaches: true,
		accountProofs: map[accountProofKey]accountProofValue{
			{}: {proof: []*cell.Cell{proof}},
		},
		shardHashesProofs: map[shardHashesProofKey]*cell.Cell{{}: proof},
		externalMsgAccounts: map[externalMessageAccountKey]externalMessageAccountValue{
			{}: {},
		},
	}
	kept := &BlockView{
		retainCurrentCaches: true,
		accountProofs: map[accountProofKey]accountProofValue{
			{}: {proof: []*cell.Cell{proof}},
		},
		externalMsgAccounts: map[externalMessageAccountKey]externalMessageAccountValue{
			{}: {},
		},
	}

	live := New(noopBacking{})
	live.blocks[storage.BlockKey(retiredBlock)] = &liveBlock{id: retiredBlock, fragments: retired}
	live.blocks[storage.BlockKey(keptShard)] = &liveBlock{id: keptShard, fragments: kept}
	previous := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: retiredBlock},
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(keptShard): {Block: keptShard},
		},
	}
	next := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: currentBlock},
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(keptShard): {Block: keptShard},
		},
	}

	live.releaseRetiredCurrentCachesLocked(previous, next)

	if retired.retainCurrentCaches || retired.accountProofs != nil || retired.shardHashesProofs != nil || retired.externalMsgAccounts != nil {
		t.Fatal("retired block retained current-view caches")
	}
	if !kept.retainCurrentCaches || len(kept.accountProofs) != 1 || len(kept.externalMsgAccounts) != 1 {
		t.Fatal("current shard cache was released")
	}

	retired.mu.Lock()
	retired.rememberAccountProofStateLocked(accountProofKey{}, []*cell.Cell{proof}, proof, false)
	retired.mu.Unlock()
	if retired.accountProofs != nil {
		t.Fatal("retired block repopulated its account proof cache")
	}
}

func testLiveBlockID(workchain int32, shard int64, seqno uint32, fill byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{fill}, 32),
		FileHash:  bytes.Repeat([]byte{fill + 1}, 32),
	}
}

type noopBacking struct{}

func (noopBacking) BlockData(context.Context, ton.BlockIDExt) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) BlockProof(context.Context, storage.ServedProofKind, ton.BlockIDExt) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) ZeroState(context.Context, ton.BlockIDExt) ([]byte, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) CurrentState(context.Context) (*storage.CurrentState, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) BlockState(context.Context, ton.BlockIDExt) (*storage.BlockState, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error) {
	return nil, storage.ErrNotFound
}

func (noopBacking) LookupBlockBySeqNo(context.Context, storage.BlockSeqRef) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByLT(context.Context, storage.BlockHistoryKey, uint64) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByAccountLT(context.Context, int32, []byte, uint64) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByUnixTime(context.Context, storage.BlockHistoryKey, uint32) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LazyCellLoader() cell.LazyCellLoader {
	return nil
}
