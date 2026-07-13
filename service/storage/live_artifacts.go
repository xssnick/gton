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
	// StateUpdate optionally carries the block's Merkle state update, already
	// extracted by the publisher, so the non-final publish path does not
	// re-parse the block to obtain it. Whether the update can be trusted
	// without a standalone merkle validation is decided by the non-final
	// artifact kind: signed blocks are hash-anchored, candidates are not.
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
