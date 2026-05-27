package p2p

func (n *Node) NotifyCompressedBlockStateReady() {
	if n == nil {
		return
	}

	n.processPendingBlockBroadcastDecodesAsync()
}
