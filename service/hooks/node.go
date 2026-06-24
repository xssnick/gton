package hooks

import (
	"context"
	"time"

	"github.com/xssnick/gton/api/liteserver"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// Node exposes the node capabilities available to a statically linked
// extension.
type Node struct {
	Network Network
	Store   Store
	Logger  zerolog.Logger
}

// Network exposes external message sending without exposing the full p2p node.
type Network interface {
	SendExternalMessage(ctx context.Context, body []byte, dst *address.Address) error
	SendCheckedExternalMessage(ctx context.Context, body []byte, dst *address.Address, root *cell.Cell, msg *tlb.ExternalMessage) error
}

// Store exposes the same read-only live view used by the liteserver.
type Store interface {
	BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error)

	CurrentState(ctx context.Context) (*storage.CurrentState, error)
	CurrentAccountBlocks(ctx context.Context, workchain int32, account []byte) (liteserver.CurrentAccountBlockIDs, error)
	CurrentMasterchainInfo(ctx context.Context) (ton.BlockIDExt, []byte, uint32, error)

	BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error)

	LookupBlockBySeqNo(ctx context.Context, key storage.BlockHistoryKey, seqno uint32) (ton.BlockIDExt, error)
	LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error)

	WaitMasterchainSeqno(ctx context.Context, seqno uint32, timeout time.Duration) error
	NonfinalPendingShardBlocks(filter *storage.ShardKey) ([]ton.BlockIDExt, []ton.BlockIDExt)
	LazyCellLoader() cell.LazyCellLoader
}
