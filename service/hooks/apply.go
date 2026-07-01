package hooks

import (
	"context"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// Extension methods may be called concurrently because shard blocks are applied in
// parallel. Implementations must be thread-safe.
type Extension interface {
	// Start runs after the node services are started.
	Start(context.Context) error
	// Close stops the extension and waits until it exits or the context is done.
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
