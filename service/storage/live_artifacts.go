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
	Block            ton.BlockIDExt
	Root             *cell.Cell
	BlockData        []byte
	Meta             *BlockMeta
	State            *BlockState
	Proofs           []LiveBlockProofArtifact
	BlockDataFlushed bool
	StateFlushed     bool
	ProofsFlushed    bool
}
