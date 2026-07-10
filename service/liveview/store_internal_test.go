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
		Block:          block,
		Root:           root,
		State:          &state,
		MessageEntries: []storage.MessageTransactionIndexEntry{},
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

func (noopBacking) LookupMessageTransaction(context.Context, storage.MessageTransactionKind, storage.MessageTransactionKey) (storage.MessageTransactionRef, error) {
	return storage.MessageTransactionRef{}, storage.ErrNotFound
}

func (noopBacking) LazyCellLoader() cell.LazyCellLoader {
	return nil
}
