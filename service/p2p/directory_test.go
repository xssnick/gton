package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

func testDirectorySubscription(t *testing.T) *overlaySubscription {
	t.Helper()

	return testOverlaySubscription(&overlaySubscription{
		log:       discardLogger(),
		peers:     map[PeerID]*overlayPeer{},
		directory: map[PeerID]*directoryEntry{},
	})
}

func testDirectoryPub(t *testing.T) ed25519.PublicKey {
	t.Helper()

	pub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	return pub
}

func TestDirectoryRemembersAndRefreshes(t *testing.T) {
	sub := testDirectorySubscription(t)
	pub := testDirectoryPub(t)
	id := testPeerID("dir-1")
	now := time.Now()

	sub.mx.Lock()
	sub.rememberDirectoryPeerLocked(id, pub, "10.0.0.1:30303", "", nil, now)
	sub.rememberDirectoryPeerLocked(id, pub, "", "10.0.0.1:31303", nil, now.Add(time.Second))
	sub.mx.Unlock()

	if got := sub.directorySize(); got != 1 {
		t.Fatalf("directory size = %d, want 1 (refresh must not duplicate)", got)
	}
	entries := sub.directorySnapshot()
	if entries[0].adnlAddr != "10.0.0.1:30303" {
		t.Fatalf("adnl addr lost on refresh: %q", entries[0].adnlAddr)
	}
	if entries[0].quicAddr != "10.0.0.1:31303" {
		t.Fatalf("quic addr not recorded: %q", entries[0].quicAddr)
	}
}

// The directory is what makes eviction from the live tier safe: the peer stays
// known, advertised and promotable.
func TestDirectorySurvivesLiveEviction(t *testing.T) {
	sub := testDirectorySubscription(t)
	pub := testDirectoryPub(t)
	id := testPeerID("dir-2")

	sub.mx.Lock()
	sub.rememberDirectoryPeerLocked(id, pub, "10.0.0.2:30303", "", nil, time.Now())
	sub.markDirectoryLiveLocked(id, true)
	sub.markDirectoryLiveLocked(id, false)
	sub.mx.Unlock()

	if got := sub.directorySize(); got != 1 {
		t.Fatalf("directory row must survive demotion, size = %d", got)
	}
}

func TestDirectoryEvictionPrefersColdNonLiveRows(t *testing.T) {
	sub := testDirectorySubscription(t)
	base := time.Now().Add(-time.Hour)

	var liveID, coldID PeerID
	sub.mx.Lock()
	for i := 0; i < maxPeersPerOverlay; i++ {
		id := testPeerID(fmt.Sprintf("dir-fill-%03d", i))
		// The oldest row is live and must be kept; the second oldest is the
		// eviction target.
		sub.rememberDirectoryPeerLocked(id, testDirectoryPub(t), "", "", nil, base.Add(time.Duration(i)*time.Second))
		switch i {
		case 0:
			liveID = id
			sub.markDirectoryLiveLocked(id, true)
			sub.directory[id].lastSeenAt = base
		case 1:
			coldID = id
		}
	}
	// One more row forces an eviction.
	sub.rememberDirectoryPeerLocked(testPeerID("dir-new"), testDirectoryPub(t), "", "", nil, time.Now())
	_, liveKept := sub.directory[liveID]
	_, coldKept := sub.directory[coldID]
	size := len(sub.directory)
	sub.mx.Unlock()

	if !liveKept {
		t.Fatal("a live row must never be evicted from the directory")
	}
	if coldKept {
		t.Fatal("the coldest non-live row must be evicted")
	}
	if size > maxPeersPerOverlay {
		t.Fatalf("directory grew past its cap: %d", size)
	}
}

// Advertisement breadth is what keeps this node in other nodes' peer lists and
// therefore in their broadcast fanout, so it must follow the directory and not
// the small live tier.
func TestAdvertisementDrawsFromDirectory(t *testing.T) {
	sub := testDirectorySubscription(t)
	sub.spec.FullID = make([]byte, 32)
	now := time.Now()

	for i := 0; i < 5; i++ {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		node, err := overlay.NewNode(sub.spec.FullID, priv)
		if err != nil {
			t.Fatalf("build overlay node: %v", err)
		}
		sub.mx.Lock()
		sub.rememberDirectoryPeerLocked(testPeerID(fmt.Sprintf("adv-%d", i)), pub, "", "", node, now)
		sub.mx.Unlock()
	}

	if got := len(sub.overlayNodesSnapshot()); got != 5 {
		t.Fatalf("advertised %d directory peers, want 5 (none of them are live)", got)
	}
}

func TestAdvertisementSkipsStaleAnnouncements(t *testing.T) {
	sub := testDirectorySubscription(t)
	sub.spec.FullID = make([]byte, 32)

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	node, err := overlay.NewNode(sub.spec.FullID, priv)
	if err != nil {
		t.Fatalf("build overlay node: %v", err)
	}
	node.Version = int32(time.Now().Add(-2 * overlayPeerTTL).Unix())

	sub.mx.Lock()
	sub.rememberDirectoryPeerLocked(testPeerID("stale"), pub, "", "", node, time.Now())
	sub.mx.Unlock()

	if got := len(sub.overlayNodesSnapshot()); got != 0 {
		t.Fatalf("stale announcement must not be advertised, got %d", got)
	}
}

func TestLivePeerLimitIsSmallerThanDirectory(t *testing.T) {
	sub := testDirectorySubscription(t)
	sub.spec.Kind = overlayKindPublicShard

	if got := sub.livePeerLimit(); got != maxLivePeersPerOverlay {
		t.Fatalf("live limit = %d, want %d", got, maxLivePeersPerOverlay)
	}
	if sub.livePeerLimit() >= maxPeersPerOverlay {
		t.Fatal("the live tier must be strictly smaller than the directory")
	}
	// The live tier must still cover every subsystem that needs a transport at
	// the same time, or promotions would thrash.
	floor := maxQueryNeighbours + plumtreeActiveNeighbourLimit + proofDownloadPeerLimit
	if sub.livePeerLimit() < floor {
		t.Fatalf("live limit %d is below the concurrent-demand floor %d", sub.livePeerLimit(), floor)
	}
}

// signedOverlayNode builds the record a peer advertises for itself.
func signedOverlayNode(t *testing.T, private ed25519.PrivateKey, overlayID []byte, version time.Time) overlay.Node {
	t.Helper()

	node := overlay.Node{
		ID:      keys.PublicKeyED25519{Key: private.Public().(ed25519.PublicKey)},
		Overlay: overlayID,
		Version: int32(version.Unix()),
	}
	if err := node.Sign(private); err != nil {
		t.Fatalf("sign overlay node: %v", err)
	}
	return node
}

// A live peer's announcement expires after overlayPeerTTL, and the peer object
// is what isKnownOverlayPeer reads. Filing only the directory row on gossip left
// that announcement frozen at attach time, so a peer we gossip with every second
// still aged out of the known set after ten minutes - and with it out of
// neighbours, broadcast targets and the plumtree roster, with no way back, since
// refreshTargets draws from that same known set.
func TestLearnAdvertisedPeerRefreshesLivePeerAnnouncement(t *testing.T) {
	public, private, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	peerID, err := peerIDFromPublicKey(public)
	if err != nil {
		t.Fatalf("peer id: %v", err)
	}

	overlayID := testPeerID("gossip-refresh-overlay").Bytes()
	now := time.Now()

	peer := &overlayPeer{id: peerID, route: &peerRoute{}}
	stale := signedOverlayNode(t, private, overlayID, now.Add(-9*time.Minute))
	peer.mergeAnnouncement(&stale)

	_, localPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate local key: %v", err)
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node:      &Node{privKey: localPrivate},
		spec:      overlaySpec{Kind: overlayKindPublicShard, ShortID: overlayID},
		log:       discardLogger(),
		peers:     map[PeerID]*overlayPeer{peerID: peer},
		directory: map[PeerID]*directoryEntry{},
	})

	// Two minutes on, the attach-time announcement has aged past overlayPeerTTL.
	checkAt := now.Add(2 * time.Minute)
	if peer.isKnownOverlayPeer(checkAt) {
		t.Fatal("test setup: the stale announcement is still fresh at the check time")
	}

	sub.learnAdvertisedPeer(context.Background(), signedOverlayNode(t, private, overlayID, now))

	if !peer.isKnownOverlayPeer(checkAt) {
		t.Fatal("gossip did not refresh the live peer's announcement, so it drops out of the known set")
	}
}
