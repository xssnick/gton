package p2p

func (n *Node) pinnedPeerIDs() map[PeerID]struct{} {
	if n == nil {
		return nil
	}

	n.peerUseMx.RLock()
	defer n.peerUseMx.RUnlock()

	if len(n.peerUse) == 0 {
		return nil
	}

	pinned := make(map[PeerID]struct{}, len(n.peerUse))
	for peerID, use := range n.peerUse {
		if use.pins > 0 {
			pinned[peerID] = struct{}{}
		}
	}
	return pinned
}
