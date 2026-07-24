package liveview

import (
	"bytes"
	"context"
	"errors"
	"testing"
	"time"

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

func TestStoreRequiresFullBlockIDForRootKeyedCaches(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x51, 8).EndCell()
	block := testLiveBlockID(0, 1<<62, 51, 0x51)
	block.RootHash = root.Hash()
	forged := block
	forged.FileHash = bytes.Repeat([]byte{0xff}, 32)
	fragments := &BlockView{}
	meta := &storage.BlockMeta{ID: block}

	live := New(noopBacking{})
	key := storage.BlockKey(block)
	live.blocks[key] = &liveBlock{id: block, root: root, meta: meta, fragments: fragments}
	live.metas[key] = meta

	if _, err := live.BlockRoot(t.Context(), forged); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("forged block root error = %v, want ErrNotFound", err)
	}
	if _, err := live.BlockMeta(t.Context(), forged); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("forged block meta error = %v, want ErrNotFound", err)
	}
	if _, err := live.BlockFragments(t.Context(), forged); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("forged block fragments error = %v, want ErrNotFound", err)
	}

	if got, err := live.BlockRoot(t.Context(), block); err != nil || got != root {
		t.Fatalf("canonical block root = %p, %v, want %p, nil", got, err, root)
	}
	if got, err := live.BlockMeta(t.Context(), block); err != nil || got != meta {
		t.Fatalf("canonical block meta = %p, %v, want %p, nil", got, err, meta)
	}
	if got, err := live.BlockFragments(t.Context(), block); err != nil || got != fragments {
		t.Fatalf("canonical block fragments = %p, %v, want %p, nil", got, err, fragments)
	}
}

func TestStoreDoesNotSingleflightDifferentFullBlockIDs(t *testing.T) {
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Second)
	defer cancel()

	block := testLiveBlockID(0, 1<<62, 61, 0x61)
	forged := block
	forged.FileHash = bytes.Repeat([]byte{0xff}, 32)
	backing := &blockingBlockDataBacking{
		canonical: block,
		data:      []byte{0x61},
		started:   make(chan struct{}),
		release:   make(chan struct{}),
	}
	live := New(backing)
	defer func() {
		select {
		case <-backing.release:
		default:
			close(backing.release)
		}
	}()

	type result struct {
		data []byte
		err  error
	}
	canonicalResult := make(chan result, 1)
	go func() {
		data, err := live.BlockData(ctx, block)
		canonicalResult <- result{data: data, err: err}
	}()

	select {
	case <-backing.started:
	case <-ctx.Done():
		t.Fatalf("canonical block load did not start: %v", ctx.Err())
	}
	forgedResult := make(chan result, 1)
	go func() {
		data, err := live.BlockData(ctx, forged)
		forgedResult <- result{data: data, err: err}
	}()

	select {
	case got := <-forgedResult:
		if !errors.Is(got.err, storage.ErrNotFound) {
			t.Fatalf("forged concurrent block data = %x, %v, want ErrNotFound", got.data, got.err)
		}
	case <-ctx.Done():
		t.Fatalf("forged block load joined the canonical full block id: %v", ctx.Err())
	}

	close(backing.release)
	got := <-canonicalResult
	if got.err != nil || !bytes.Equal(got.data, backing.data) {
		t.Fatalf("canonical block data = %x, %v, want %x, nil", got.data, got.err, backing.data)
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

func (noopBacking) LookupBlockBySeqNoForPrefix(context.Context, storage.BlockSeqRef) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByLT(context.Context, storage.BlockHistoryKey, uint64) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByLTForPrefix(context.Context, storage.BlockHistoryKey, uint64) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByAccountLT(context.Context, int32, []byte, uint64) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByUnixTime(context.Context, storage.BlockHistoryKey, uint32) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LookupBlockByUnixTimeForPrefix(context.Context, storage.BlockHistoryKey, uint32) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, storage.ErrNotFound
}

func (noopBacking) LazyCellLoader() cell.LazyCellLoader {
	return nil
}

type blockingBlockDataBacking struct {
	noopBacking
	canonical ton.BlockIDExt
	data      []byte
	started   chan struct{}
	release   chan struct{}
}

func (b *blockingBlockDataBacking) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	if !blockIDEqual(block, b.canonical) {
		return nil, storage.ErrNotFound
	}

	close(b.started)
	select {
	case <-b.release:
		return b.data, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}
