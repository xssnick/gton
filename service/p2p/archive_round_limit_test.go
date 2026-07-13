package p2p

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/xssnick/gton/service/archive"
)

func TestArchiveDownloadRoundLimitsHedgeAndFallbackPeerAttempts(t *testing.T) {
	shard := archive.ShardID{Workchain: -1, Shard: topShard}
	node := &Node{peerUse: map[PeerID]peerUse{}}
	sub := testOverlaySubscription(&overlaySubscription{
		log:  discardLogger(),
		node: node,
		spec: overlaySpec{ShortID: []byte{1}},
	})
	pool := testArchivePool(t, sub)
	defer pool.Close()
	session := node.BeginArchiveSession()
	defer session.Close()

	const rosterSize = 40
	peers := make([]*overlayPeer, 0, rosterSize)
	candidates := make(map[PeerID]archiveCandidate, rosterSize)
	clients := make([]*testArchiveRLDP, 0, rosterSize)
	for i := range rosterSize {
		peer, client := testArchiveDownloadPeerWithRLDP(t, fmt.Sprintf("round-limit-%02d", i), int64(i+1), nil, 0)
		client.asyncErr = context.DeadlineExceeded
		if !addTestArchiveOnlyPeer(pool, peer) {
			t.Fatalf("add archive peer %d", i)
		}

		peers = append(peers, peer)
		candidates[peer.id] = archiveCandidate{peer: peer, archiveID: int64(i + 1)}
		clients = append(clients, client)
	}

	_, err := sub.downloadArchiveFromPeers(
		context.Background(),
		session,
		pool,
		resolvedArchive{MasterchainSeqno: 10, Shard: shard},
		peers,
		candidates,
		ArchiveDownloadOptions{Hedge: true},
	)
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("download error = %v, want context.DeadlineExceeded", err)
	}

	attempted := 0
	for _, client := range clients {
		if len(client.snapshot().asyncQueries) > 0 {
			attempted++
		}
	}
	if attempted != archiveDownloadRoundPeerLimit {
		t.Fatalf("attempted peers = %d, want %d", attempted, archiveDownloadRoundPeerLimit)
	}
}
