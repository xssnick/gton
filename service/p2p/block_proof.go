package p2p

import (
	"fmt"

	"flexserver/service/blockproof"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func parseBlockProofForBlock(id ton.BlockIDExt, proofRoot *cell.Cell) (*tlb.Block, error) {
	parsed, err := blockproof.ParseCell(id, proofRoot)
	if err != nil {
		return nil, fmt.Errorf("parse block proof: %w", err)
	}
	return parsed.Block, nil
}
