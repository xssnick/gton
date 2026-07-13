package liveview

import (
	"bytes"
	"context"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	masterchainID    int32 = -1
	masterchainShard int64 = -1 << 63
)

type CurrentAccountBlockIDs struct {
	Master  ton.BlockIDExt
	Account ton.BlockIDExt
}

func cloneBlockID(id ton.BlockIDExt) *ton.BlockIDExt {
	cloned := id
	cloned.RootHash = bytes.Clone(id.RootHash)
	cloned.FileHash = bytes.Clone(id.FileHash)
	return &cloned
}

func blockIDEqual(a ton.BlockIDExt, b ton.BlockIDExt) bool {
	return a.Workchain == b.Workchain &&
		a.Shard == b.Shard &&
		a.SeqNo == b.SeqNo &&
		bytes.Equal(a.RootHash, b.RootHash) &&
		bytes.Equal(a.FileHash, b.FileHash)
}

type Backing interface {
	// BlockData returns bytes that remain valid for the duration of the call
	// chain. Callers treat the returned slice as read-only.
	BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	CurrentState(ctx context.Context) (*storage.CurrentState, error)
	BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	LookupBlockBySeqNo(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error)
	LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error)
	LazyCellLoader() cell.LazyCellLoader
}
