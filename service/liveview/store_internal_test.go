package liveview

import (
	"bytes"
	"context"
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
