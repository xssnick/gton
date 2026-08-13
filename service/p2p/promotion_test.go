package p2p

import (
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/overlay"
)

func testPromotionSubscription(t *testing.T, liveRows, directoryRows int) *overlaySubscription {
	t.Helper()

	sub := testOverlaySubscription(&overlaySubscription{
		log:       discardLogger(),
		peers:     map[PeerID]*overlayPeer{},
		directory: map[PeerID]*directoryEntry{},
		spec:      overlaySpec{Kind: overlayKindPublicShard, FullID: make([]byte, 32)},
	})

	now := time.Now()
	for i := 0; i < liveRows; i++ {
		peer := testArchiveCandidate(fmt.Sprintf("live-%03d", i))
		sub.peers[peer.id] = peer
		sub.rememberDirectoryPeerLocked(peer.id, testDirectoryPub(t), "10.1.0.1:30303", "", nil, now, directoryProven)
		sub.markDirectoryLiveLocked(peer.id, true)
	}
	for i := 0; i < directoryRows; i++ {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		node, err := overlay.NewNode(sub.spec.FullID, priv)
		if err != nil {
			t.Fatalf("build overlay node: %v", err)
		}
		sub.rememberDirectoryPeerLocked(
			testPeerID(fmt.Sprintf("dir-%03d", i)),
			pub,
			fmt.Sprintf("10.2.0.%d:30303", i%250+1),
			"",
			node,
			now,
			directoryProven,
		)
	}
	return sub
}

func TestLiveTierRoomTracksTheLiveCap(t *testing.T) {
	sub := testPromotionSubscription(t, 10, 0)
	if got, want := sub.liveTierRoom(), maxLivePeersPerOverlay-10; got != want {
		t.Fatalf("live tier room = %d, want %d", got, want)
	}

	full := testPromotionSubscription(t, maxLivePeersPerOverlay, 0)
	if got := full.liveTierRoom(); got > 0 {
		t.Fatalf("a full live tier must report no room, got %d", got)
	}
}

// Promotion must never pick a row that is already live, that it cannot reach —
// no address filed and no transport pooled, as here — or that carries a stale
// announcement. Each of those would burn a handshake for nothing.
func TestPromotionCandidatesFilterUnusableRows(t *testing.T) {
	sub := testPromotionSubscription(t, 3, 5)

	// A row with no address and a row with a stale announcement.
	sub.mx.Lock()
	noAddr := testPeerID("no-addr")
	sub.rememberDirectoryPeerLocked(noAddr, testDirectoryPub(t), "", "", nil, time.Now(), directoryHearsay)
	staleID := testPeerID("stale-node")
	_, priv, _ := ed25519.GenerateKey(rand.Reader)
	staleNode, _ := overlay.NewNode(sub.spec.FullID, priv)
	staleNode.Version = int32(time.Now().Add(-2 * overlayPeerTTL).Unix())
	sub.rememberDirectoryPeerLocked(staleID, testDirectoryPub(t), "10.3.0.1:30303", "", staleNode, time.Now(), directoryProven)
	sub.mx.Unlock()

	candidates := sub.promotionCandidates()
	if len(candidates) != 5 {
		t.Fatalf("expected the 5 usable directory rows, got %d", len(candidates))
	}
	for _, entry := range candidates {
		if entry.live {
			t.Fatal("a live row must never be a promotion candidate")
		}
		if entry.adnlAddr == "" {
			t.Fatal("a row without an address must never be a promotion candidate")
		}
		if entry.id == staleID || entry.id == noAddr {
			t.Fatalf("unusable row %s selected", entry.id.String())
		}
	}
}

// A peer that reached us first never joins a public roster and the gossip that
// files its row carries no address, so requiring one hid exactly the peers we
// already hold an open transport to — and acquirePeerEndpoint reuses a pooled
// transport by id without ever looking at the endpoint.
func TestPromotionCandidatesAcceptRowsReachableThroughThePool(t *testing.T) {
	sub := testPromotionSubscription(t, 0, 0)
	now := time.Now()
	announced := freshDirectoryNode(t, sub.spec.FullID)

	pooled, _ := newIdleTestPooledPeer("promotion-pooled-row", now, 0)
	t.Cleanup(pooled.close)
	sub.node.pool = &peerPool{peers: map[PeerID]*pooledPeer{pooled.id: pooled}}

	unreachable := testPeerID("promotion-no-transport")
	sub.mx.Lock()
	sub.rememberDirectoryPeerLocked(pooled.id, testDirectoryPub(t), "", "", announced, now, directoryHearsay)
	sub.rememberDirectoryPeerLocked(unreachable, testDirectoryPub(t), "", "", announced, now, directoryHearsay)
	sub.mx.Unlock()

	candidates := sub.promotionCandidates()
	if len(candidates) != 1 {
		t.Fatalf("promotion candidates = %d, want only the pooled row", len(candidates))
	}
	if candidates[0].id != pooled.id {
		t.Fatalf("candidate %s selected, want the row with a pooled transport", candidates[0].id.String())
	}
}

func TestPromotionCandidatesAreShuffled(t *testing.T) {
	sub := testPromotionSubscription(t, 0, 40)

	first := sub.promotionCandidates()
	differs := false
	for i := 0; i < 20 && !differs; i++ {
		next := sub.promotionCandidates()
		for j := range next {
			if j < len(first) && next[j].id != first[j].id {
				differs = true
				break
			}
		}
	}
	if !differs {
		t.Fatal("promotion order must vary so nodes do not converge on the same peers")
	}
}

// Directory gossip must only target rows we hold no attachment for: attached
// peers are already polled through the normal refresh path.
func TestDirectoryGossipTargetsSkipLiveRows(t *testing.T) {
	sub := testPromotionSubscription(t, 5, 5)

	targets := sub.directoryGossipTargets()
	if len(targets) > directoryGossipFanout {
		t.Fatalf("gossip fanout = %d, want at most %d", len(targets), directoryGossipFanout)
	}
	for _, entry := range targets {
		if entry.live {
			t.Fatal("gossip must not target live rows")
		}
		sub.mx.Lock()
		_, attached := sub.peers[entry.id]
		sub.mx.Unlock()
		if attached {
			t.Fatal("gossip must not target attached peers")
		}
	}
}

// A node whose live tier is full must not promote: that is what bounds the
// number of transports.
func TestTopUpDoesNothingWhenLiveTierIsFull(t *testing.T) {
	sub := testPromotionSubscription(t, maxLivePeersPerOverlay, 20)
	before := len(sub.peersSnapshot())

	sub.topUpLiveTier(t.Context())

	if got := len(sub.peersSnapshot()); got != before {
		t.Fatalf("live tier grew past its cap: %d -> %d", before, got)
	}
}
