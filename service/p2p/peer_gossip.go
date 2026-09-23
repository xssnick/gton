package p2p

import (
	"crypto/ed25519"
	"math/rand/v2"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

const overlayMemberDoNotReceiveBroadcasts = uint32(1)

// V1 and zero-flag V2 descriptors sign the same overlay.node.toSign payload.
// Keep V2 internally so flags and their signed payload survive peer gossip.
func overlayNodeFromV1(node overlay.Node) overlay.NodeV2 {
	return overlay.NodeV2{
		ID:          node.ID,
		Overlay:     node.Overlay,
		Version:     node.Version,
		Signature:   node.Signature,
		Certificate: overlay.EmptyMemberCertificate{},
	}
}

func overlayNodesFromV1(nodes []overlay.Node) []overlay.NodeV2 {
	if len(nodes) > maxAdvertisedPeersPerQuery {
		nodes = nodes[:maxAdvertisedPeersPerQuery]
	}
	result := make([]overlay.NodeV2, 0, len(nodes))
	for _, node := range nodes {
		result = append(result, overlayNodeFromV1(node))
	}
	return result
}

func overlayNodesToV1(nodes []overlay.NodeV2) overlay.NodesList {
	result := overlay.NodesList{List: make([]overlay.Node, 0, len(nodes))}
	for _, node := range nodes {
		// Nonzero flags use overlay.node.toSignEx. Dropping them would send
		// an invalid signature; such descriptors can only be relayed in V2.
		if node.Flags != 0 {
			continue
		}
		result.List = append(result.List, overlay.Node{
			ID:        node.ID,
			Overlay:   node.Overlay,
			Version:   node.Version,
			Signature: node.Signature,
		})
	}
	return result
}

func (s *overlaySubscription) handleGetRandomPeersV2(
	source PeerID,
	peerAddr string,
	query overlay.GetRandomPeersV2,
) (overlay.NodesV2, error) {
	if s.fastSync != nil {
		return s.handleFastSyncRandomPeers(query)
	}
	s.learnRandomPeerRequest(source, peerAddr, query.Peers.Nodes)

	self := overlay.NodeV2{
		ID:          keys.PublicKeyED25519{Key: s.node.privKey.Public().(ed25519.PublicKey)},
		Overlay:     s.spec.ShortID,
		Version:     int32(time.Now().Unix()),
		Certificate: overlay.EmptyMemberCertificate{},
	}
	if s.chainBroadcastsPaused() {
		self.Flags = overlayMemberDoNotReceiveBroadcasts
	}
	if err := self.Sign(s.node.privKey); err != nil {
		return overlay.NodesV2{}, err
	}
	nodes := make([]overlay.NodeV2, 0, maxRandomPeerReply)
	nodes = append(nodes, self)
	nodes = append(nodes, s.randomOverlayNodes(maxRandomPeerReply-len(nodes))...)
	return overlay.NodesV2{Nodes: nodes}, nil
}

func (s *overlaySubscription) handleFastSyncRandomPeersV1(query overlay.GetRandomPeers) (overlay.NodesList, error) {
	s.learnFastSyncNodes(overlayNodesFromV1(query.List.List), time.Now())
	peers, err := s.fastSync.peers.RandomPeers(time.Now(), rand.Uint64())
	if err != nil {
		return overlay.NodesList{}, err
	}
	// Sign our V1 announcement separately: our V2 flags may be nonzero.
	self, err := overlay.NewNode(s.spec.FullID, s.node.privKey)
	if err != nil {
		return overlay.NodesList{}, err
	}
	peers.Nodes[0] = overlayNodeFromV1(*self)
	return overlayNodesToV1(peers.Nodes), nil
}
