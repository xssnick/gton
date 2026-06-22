package p2p

import "github.com/xssnick/tonutils-go/ton"

func (n *Node) NotifyCompressedBlockStateReady(blocks ...ton.BlockIDExt) {
	if n == nil {
		return
	}

	for _, block := range blocks {
		n.processPendingBlockBroadcastDecodesForPrevAsync(block)
	}
}
