package p2p

import (
	"fmt"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func decodeCompressedBlockV2(data DataFullCompressedV2, state *cell.Cell) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof(data.Proof)
	if err != nil {
		return nil, fmt.Errorf("parse tonNode.dataFullCompressedV2 proof: %w", err)
	}
	return decodeCompressedBlockV2WithProofRoot(data, state, proofRoot)
}

func decodeBlockBroadcastCompressedV2(data tonnodeapi.BlockBroadcastCompressedV2, state *cell.Cell) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof(data.Proof)
	if err != nil {
		return nil, fmt.Errorf("parse tonNode.blockBroadcastCompressedV2 proof: %w", err)
	}
	block, _, err := decodeBlockBroadcastCompressedV2WithProofRoot(data, state, proofRoot, nil)
	return block, err
}
