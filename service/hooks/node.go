package hooks

import (
	"context"
	"time"

	"github.com/xssnick/gton/console"
	"github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/p2p"
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
	Network                Network
	MasterchainHead        MasterchainHead
	PrivateOverlays        *p2p.PrivateOverlayRegistry
	BlockBroadcasts        *p2p.BlockBroadcasts
	Store                  Store
	AccountPrewarmer       AccountPrewarmer
	AccountPrewarmCapacity int
	TVM                    *tvm.TVM
	Logger                 zerolog.Logger
	Metrics                any
	Commands               *console.Registry
}

// MasterchainHead exposes the newest masterchain block whose consensus
// signatures were verified by the node. An extension may use it to distinguish
// local catch-up from a network whose actual head is old.
type MasterchainHead interface {
	SeenMasterchainBlock() (ton.BlockIDExt, error)
}

// AccountPrewarmer schedules raw cell-record warming for account state and
// exact cell trees. Implementations are non-blocking: a saturated bounded
// queue may discard a hint because warming never participates in correctness.
type AccountPrewarmer interface {
	EnqueueRoot(cell.Hash) bool
	EnqueueAccount(workchain int32, account [32]byte) bool
	PrewarmAccountNow(workchain int32, account [32]byte) bool
}

// Network exposes node traffic operations without exposing the full p2p node.
type Network interface {
	SendExternalMessage(ctx context.Context, body []byte, dst *address.Address) error
	SendCheckedExternalMessage(ctx context.Context, body []byte, dst *address.Address, root *cell.Cell, msg *tlb.ExternalMessage) error
	CheckExternalMessage(ctx context.Context, body []byte, root *cell.Cell, msg *tlb.ExternalMessage) (externalmsg.CheckResult, error)
	// SubmitBlockLocally transfers an immutable, fully decoded local block to the
	// normal verification and apply pipeline. Submission is best-effort and
	// never waits for validation, application, or a storage checkpoint.
	SubmitBlockLocally(block p2p.DownloadedBlock)
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

	MasterchainSeqnoReady(seqno uint32) bool
	WaitMasterchainSeqno(ctx context.Context, seqno uint32, timeout time.Duration) error
	NonfinalPendingShardBlocks(filter *storage.ShardKey) ([]ton.BlockIDExt, []ton.BlockIDExt)
	LazyCellLoader() cell.LazyCellLoader

	// PublishAcceptedBlockState makes a block this node finalized itself, and the
	// state it computed for it, readable before either is committed. It is how a
	// consensus participant extends the live view rather than waiting for a flush
	// it does not need; see liveview.Store.PublishAcceptedBlockState for what may
	// be published and for how long it lives.
	PublishAcceptedBlockState(artifacts storage.LiveBlockArtifacts) error
	// BlockArtifactsSignal is the edge a reader waits on instead of polling for a
	// block that has not arrived yet. The signal says only "something was
	// published"; the caller's own read stays the predicate.
	BlockArtifactsSignal() <-chan struct{}
}

// AcceptedBlockStatePublisher is an optional Store capability: it tells an
// extension about every PublishAcceptedBlockState publication, with the
// artifacts as published, once the store can answer for them. The live view
// provides it (liveview.Store.ObserveAcceptedBlockStates); an extension that
// finds it absent keeps following the applied-block hook alone.
//
// The validator uses it to advance its internal-message pool from the states
// of the blocks it accepted in consensus, which are resident about a second
// before the same blocks reach OnBlockApplied under load. The observer runs
// synchronously on the acceptance path, so it must be as cheap as the applied
// hook's own bookkeeping.
type AcceptedBlockStatePublisher interface {
	ObserveAcceptedBlockStates(observe func(storage.LiveBlockArtifacts)) (stop func())
}
