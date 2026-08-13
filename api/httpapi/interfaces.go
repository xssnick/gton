package httpapi

import (
	"context"

	"github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// Store is the read-only node state consumed by the HTTP API.
type Store interface {
	CurrentState(ctx context.Context) (*storage.CurrentState, error)
	CurrentAccountBlocks(ctx context.Context, workchain int32, account []byte) (liveview.CurrentAccountBlockIDs, error)
	CurrentMasterchainInfo(ctx context.Context) (ton.BlockIDExt, []byte, uint32, error)

	BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	BlockRoot(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error)
	BlockFragments(ctx context.Context, block ton.BlockIDExt) (*liveview.BlockView, error)
	LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error)

	LookupBlockBySeqNo(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error)
	LookupBlockByLT(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error)
	LookupBlockByUnixTime(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error)
}

// Network is the external-message capability consumed by the HTTP API.
type Network interface {
	CheckExternalMessage(ctx context.Context, body []byte, root *cell.Cell, msg *tlb.ExternalMessage) (externalmsg.CheckResult, error)
	SendCheckedExternalMessage(ctx context.Context, body []byte, dst *address.Address, root *cell.Cell, msg *tlb.ExternalMessage) error
}
