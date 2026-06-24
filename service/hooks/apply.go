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

type BlockAppliedEvent struct {
	// BlockBOC is the raw block BOC bytes that were applied.
	BlockBOC []byte
	// ProofBOC is the raw proof BOC bytes associated with the block.
	ProofBOC []byte
	// BlockRoot is the parsed root cell of the applied block.
	BlockRoot *cell.Cell
	// Meta describes the applied block identity and known block metadata.
	Meta *storage.BlockMeta
	// State is the parsed state after applying this block.
	State *tlb.ShardStateUnsplit
	// MasterchainRef is set for shard blocks to the including masterchain block.
	// It is nil for masterchain blocks.
	MasterchainRef *ton.BlockIDExt
}

type ExternalMessageEvent struct {
	// IsLocal is true when the message came from this node's liteserver API,
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
