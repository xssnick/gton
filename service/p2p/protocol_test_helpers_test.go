package p2p

import (
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func decodeCompressedBlockV2(data DataFullCompressedV2, state *cell.Cell) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof("tonNode.dataFullCompressedV2", data.Proof)
	if err != nil {
		return nil, err
	}
	return decodeCompressedBlockV2WithProofRoot(data, state, proofRoot)
}

func decodeBlockBroadcastCompressedV2(data tonnodeapi.BlockBroadcastCompressedV2, state *cell.Cell) (*DownloadedBlock, error) {
	proofRoot, err := parseDownloadedBlockProof("tonNode.blockBroadcastCompressedV2", data.Proof)
	if err != nil {
		return nil, err
	}
	block, _, err := decodeBlockBroadcastCompressedV2WithProofRoot(data, state, proofRoot)
	return block, err
}
