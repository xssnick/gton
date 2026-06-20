package p2p

import "github.com/xssnick/gton/service/archive"

func (n *Node) prioritizeArchivePeers(shard archive.ShardID, peers []*overlayPeer) []*overlayPeer {
	if n == nil {
		return prioritizeArchivePeersWithLeases(shard, peers, nil)
	}
	return prioritizeArchivePeersWithLeases(shard, peers, n.downloadPeerLeaseSnapshot(peers))
}
