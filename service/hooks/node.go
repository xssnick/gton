package hooks

import (
	"context"
	"time"

	"github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// Node exposes the node capabilities available to a statically linked
// extension.
type Node struct {
	Network Network
	Store   Store
	TVM     *tvm.TVM
	Logger  zerolog.Logger
	Metrics any
}

// Network exposes external message sending without exposing the full p2p node.
type Network interface {
	SendExternalMessage(ctx context.Context, body []byte, dst *address.Address) error
	SendCheckedExternalMessage(ctx context.Context, body []byte, dst *address.Address, root *cell.Cell, msg *tlb.ExternalMessage) error
	CheckExternalMessage(ctx context.Context, body []byte, root *cell.Cell, msg *tlb.ExternalMessage) (externalmsg.CheckResult, error)
}

// Store exposes the shared read-only live view.
type Store interface {
	BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error)
	BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error)

	CurrentState(ctx context.Context) (*storage.CurrentState, error)
	CurrentAccountBlocks(ctx context.Context, workchain int32, account []byte) (liveview.CurrentAccountBlockIDs, error)
	CurrentMasterchainInfo(ctx context.Context) (ton.BlockIDExt, []byte, uint32, error)

	BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error)
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error)
	BlockFragments(ctx context.Context, block ton.BlockIDExt) (*liveview.BlockView, error)

	LookupBlockBySeqNo(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error)
	LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error)
	LookupMessageTransaction(ctx context.Context, kind storage.MessageTransactionKind, key storage.MessageTransactionKey) (storage.MessageTransactionRef, error)

	MasterchainSeqnoReady(seqno uint32) bool
	WaitMasterchainSeqno(ctx context.Context, seqno uint32, timeout time.Duration) error
	NonfinalPendingShardBlocks(filter *storage.ShardKey) ([]ton.BlockIDExt, []ton.BlockIDExt)
	LazyCellLoader() cell.LazyCellLoader
}
