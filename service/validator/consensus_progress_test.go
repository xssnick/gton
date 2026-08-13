package validator

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

func TestCollatorConsensusProgressCarriesAppliedAnchorState(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     -1 << 63,
		SeqNo:     7,
		RootHash:  bytes.Repeat([]byte{0x71}, 32),
		FileHash:  bytes.Repeat([]byte{0x72}, 32),
	}
	state := cell.BeginCell().MustStoreUInt(0x73, 8).EndCell()
	anchor := *block.Copy()
	chain := &ChainState{tips: []ChainTip{{
		ID:       *block.Copy(),
		BlockBOC: []byte{0x74},
		State:    state,
	}}}
	progress := sessionConsensusProgress{
		Window: simplex.Window{
			StartSlot: 4,
			EndSlot:   8,
		},
		FinalizedAnchor:    &anchor,
		AppliedAnchorState: chain,
	}
	converted, err := collatorConsensusProgress([32]byte{0x75}, 4, progress)
	if err != nil {
		t.Fatal(err)
	}
	if converted.FinalizedAnchorState == nil ||
		!sameBlockID(converted.FinalizedAnchorState.Block, block) ||
		converted.FinalizedAnchorState.State != state ||
		&converted.FinalizedAnchorState.BlockBOC[0] != &chain.tips[0].BlockBOC[0] {
		t.Fatal("applied anchor was not retained as one immutable collator handoff")
	}
}
