package service

import (
	"context"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// EventHandlers contains optional domain event consumers. Implementations may be
// called concurrently because shard blocks are processed in parallel.
type EventHandlers struct {
	BlockApplied             BlockAppliedProcessor
	ExternalMessage          ExternalMessageGate
	BlockReceived            BlockReceivedObserver
	ShardTopBlockDescription ShardTopBlockDescriptionObserver
}

type BlockAppliedProcessor interface {
	ProcessBlockApplied(context.Context, BlockAppliedEvent) error
}

type ExternalMessageGate interface {
	AcceptExternalMessage(context.Context, ExternalMessageEvent) error
}

type BlockReceivedObserver interface {
	ObserveBlockReceived(context.Context, BlockReceivedEvent) error
}

type ShardTopBlockDescriptionObserver interface {
	ObserveShardTopBlockDescription(context.Context, *p2p.ShardBlockDescription, *cell.Cell) error
}

type BlockAppliedEvent struct {
	BlockBOC             []byte
	ProofBOC             []byte
	BlockRoot            *cell.Cell
	Meta                 *storage.BlockMeta
	PreviousState        *cell.Cell
	CurrentState         *cell.Cell
	InclusionMasterRef   *ton.BlockIDExt
	InclusionMasterState *cell.Cell
}

type ExternalMessageEvent struct {
	IsLocal       bool
	MessageRoot   *cell.Cell
	MessageParsed *tlb.ExternalMessage
}

type BlockReceivedEvent struct {
	IsSigned  bool
	BlockBOC  []byte
	ProofBOC  []byte
	BlockRoot *cell.Cell
	Meta      *storage.BlockMeta
}
