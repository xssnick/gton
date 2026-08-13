package hooks

import (
	"context"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// Extension methods may be called concurrently because shard blocks are applied in
// parallel. Implementations must be thread-safe.
type Extension interface {
	// Start runs after the P2P node is ready and before the sync coordinator is
	// started. Its context is the extension runtime lifetime and is canceled
	// before network teardown, so network-dependent background work must stop
	// on ctx.Done rather than wait for Close. Factories must still initialize
	// event-handler state eagerly.
	Start(context.Context) error
	// Close releases everything created by the factory and waits until it exits
	// or the context is done. It may be called before Start or after Start
	// returns an error. After an error the owner may retry Close; successful
	// cleanup must be idempotent and must not be repeated by the implementation.
	Close(context.Context) error
	// OnBlockApplied runs after a block is applied and before that block flow
	// can continue to checkpoint/persist. Returning an error retries only this
	// block flow until nil or context cancellation.
	OnBlockApplied(context.Context, BlockAppliedEvent) error
	// OnExternalMessage runs after an external message is accepted by TVM
	// emulation and before it is rebroadcast further. Returning an error drops
	// that external message without retry.
	OnExternalMessage(context.Context, ExternalMessageEvent) error
	// OnBlockReceived runs when a block is received from broadcasts, downloads,
	// or archive imports.
	// Returning an error only logs it and does not affect block processing.
	OnBlockReceived(context.Context, BlockReceivedEvent) error
}

// ShardTopBlockDescriptionObserver is an optional extension capability. It is
// called only after the node has decoded the exact TopBlockDescr cell and
// verified its proof chain, current masterchain topology and ancestry,
// validator set and signatures. Both values are borrowed and must be treated
// as immutable.
type ShardTopBlockDescriptionObserver interface {
	OnShardTopBlockDescription(context.Context, *p2p.ShardBlockDescription, *cell.Cell) error
}

// ExtensionFactory initializes an extension from node capabilities.
type ExtensionFactory func(Node) (Extension, error)

type BlockAppliedEvent struct {
	// BlockBOC is the raw block BOC bytes that were applied.
	BlockBOC []byte
	// ProofBOC is the raw proof BOC bytes associated with the block.
	ProofBOC []byte
	// BlockRoot is the parsed root cell of the applied block.
	BlockRoot *cell.Cell
	// Meta describes the applied block identity and known block metadata.
	Meta *storage.BlockMeta
	// PreviousState is the state root that block.StateUpdate was applied to.
	// For shard merges this is a ShardStateSplit root containing both previous shards.
	PreviousState *cell.Cell
	// CurrentState is the state root after applying this block.
	CurrentState *cell.Cell
	// InclusionMasterRef is set for shard blocks to the including masterchain block.
	// It is nil for masterchain blocks.
	InclusionMasterRef *ton.BlockIDExt
	// InclusionMasterState is set for shard blocks to the including masterchain
	// state root. It is nil for masterchain blocks.
	InclusionMasterState *cell.Cell
}

type ExternalMessageEvent struct {
	// IsLocal is true when the message came from this node's local API path,
	// not from an overlay broadcast.
	IsLocal bool
	// MessageRoot is the parsed root cell of the external message.
	MessageRoot *cell.Cell
	// MessageParsed is the decoded external message when it was already
	// available on the receive path.
	MessageParsed *tlb.ExternalMessage
}

type BlockReceivedEvent struct {
	// IsSigned is true for signed block artifacts and downloaded/imported full blocks.
	IsSigned bool
	// BlockBOC is the raw received block BOC bytes.
	BlockBOC []byte
	// ProofBOC is the raw proof BOC bytes when available.
	ProofBOC []byte

	// BlockRoot is the parsed root cell of the received block when available.
	BlockRoot *cell.Cell
	// Meta describes the received block identity and known block metadata.
	Meta *storage.BlockMeta
}
