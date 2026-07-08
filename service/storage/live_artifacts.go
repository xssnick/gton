package storage

import (
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type LiveBlockProofArtifact struct {
	Kind ServedProofKind
	Data []byte
}

type LiveBlockArtifacts struct {
	Block     ton.BlockIDExt
	Root      *cell.Cell
	BlockData []byte
	Meta      *BlockMeta
	State     *BlockState
	Proofs    []LiveBlockProofArtifact
	// MessageEntries carries the message->transaction index entries extracted by a
	// publisher that already TLB-parsed the block; nil makes the live store derive
	// them from Root itself, non-nil (including empty) is authoritative.
	MessageEntries []MessageTransactionIndexEntry
	// StateUpdate optionally carries the block's Merkle state update, already
	// extracted and validated by the publisher, so the non-final publish path
	// does not re-parse the block to obtain it.
	StateUpdate     *cell.Cell
	ArtifactFlushed bool
	StateFlushed    bool
	// AvailabilityOnly makes the block visible without preparing live read fragments.
	AvailabilityOnly bool
}

type LiveBlockNonfinalKind uint8

const (
	LiveBlockNonfinalSigned LiveBlockNonfinalKind = 1 << iota
	LiveBlockNonfinalCandidate
)
