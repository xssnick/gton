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
	sub.rememberDirectoryPeerLocked(id, pub, "10.0.0.1:30303", "", nil, now, directoryProven)
	sub.rememberDirectoryPeerLocked(id, pub, "", "10.0.0.1:31303", nil, now.Add(time.Second), directoryProven)
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
	sub.rememberDirectoryPeerLocked(id, pub, "10.0.0.2:30303", "", nil, time.Now(), directoryProven)
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
		sub.rememberDirectoryPeerLocked(id, testDirectoryPub(t), "", "", nil, base.Add(time.Duration(i)*time.Second), directoryHearsay)
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
	sub.rememberDirectoryPeerLocked(testPeerID("dir-new"), testDirectoryPub(t), "", "", nil, time.Now(), directoryHearsay)
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

// freshDirectoryNode is the signed record a peer advertises for itself right
// now. Eviction only reads its version, so one record can stand in for many
// rows.
func freshDirectoryNode(t *testing.T, overlayID []byte) *overlay.Node {
	t.Helper()

	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	node, err := overlay.NewNode(overlayID, priv)
	if err != nil {
		t.Fatalf("build overlay node: %v", err)
	}
	return node
}

// A peer forwarding a list of strangers must not be able to flush the rows we
// gossip with and promote from: an identity costs a keypair and an announcement
// costs a signature, so hearsay may only ever recycle its own class.
func TestDirectoryFloodCannotEvictProvenRows(t *testing.T) {
	sub := testDirectorySubscription(t)
	sub.spec.FullID = make([]byte, 32)
	now := time.Now()
	announced := freshDirectoryNode(t, sub.spec.FullID)

	proven := make([]PeerID, 0, maxPeersPerOverlay)
	sub.mx.Lock()
	for i := range maxPeersPerOverlay {
		id := testPeerID(fmt.Sprintf("proven-%03d", i))
		if !sub.rememberDirectoryPeerLocked(id, testDirectoryPub(t), "10.0.0.1:30303", "", announced, now, directoryProven) {
			t.Fatalf("proven row %d was refused", i)
		}
		proven = append(proven, id)
	}
	sub.mx.Unlock()

	// Both ways in: records forwarded by a third party, and records a stranger
	// brings by handshaking and querying us itself. Neither buys a slot here.
	floodPub := testDirectoryPub(t)
	sub.mx.Lock()
	for i := range maxPeersPerOverlay {
		id := testPeerID(fmt.Sprintf("flood-%04d", i))
		if sub.rememberDirectoryPeerLocked(id, floodPub, "", "", announced, now, directoryHearsay) {
			t.Fatalf("hearsay row %d was filed, so it displaced a proven row", i)
		}
		contacted := testPeerID(fmt.Sprintf("handshake-flood-%04d", i))
		if sub.rememberDirectoryPeerLocked(contacted, floodPub, "1.2.3.4:1", "", announced, now, directoryContacted) {
			t.Fatalf("handshake flood row %d was filed, so it displaced a proven row", i)
		}
	}
	kept := 0
	for _, id := range proven {
		if sub.directory[id] != nil {
			kept++
		}
	}
	size := len(sub.directory)
	sub.mx.Unlock()

	if kept != len(proven) {
		t.Fatalf("the flood evicted %d proven rows", len(proven)-kept)
	}
	if size != maxPeersPerOverlay {
		t.Fatalf("directory size = %d, want %d", size, maxPeersPerOverlay)
	}
}

// The other half of the same rule: refusing everything would freeze the
// directory, so hearsay still takes the slot of a row that never proved itself.
func TestDirectoryHearsayRecyclesUnverifiedRows(t *testing.T) {
	sub := testDirectorySubscription(t)
	sub.spec.FullID = make([]byte, 32)
	now := time.Now()
	announced := freshDirectoryNode(t, sub.spec.FullID)

	sub.mx.Lock()
	for i := range maxPeersPerOverlay - 1 {
		id := testPeerID(fmt.Sprintf("proven-%03d", i))
		sub.rememberDirectoryPeerLocked(id, testDirectoryPub(t), "10.0.0.1:30303", "", announced, now, directoryProven)
	}
	unverified := testPeerID("unverified")
	sub.rememberDirectoryPeerLocked(unverified, testDirectoryPub(t), "", "", announced, now, directoryHearsay)

	learned := testPeerID("learned")
	filed := sub.rememberDirectoryPeerLocked(learned, testDirectoryPub(t), "", "", announced, now, directoryHearsay)
	_, unverifiedKept := sub.directory[unverified]
	_, learnedFiled := sub.directory[learned]
	sub.mx.Unlock()

	if !filed || !learnedFiled {
		t.Fatal("a full directory of proven rows must still recycle its unverified slots")
	}
	if unverifiedKept {
		t.Fatal("the unverified row was not the victim")
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
		sub.rememberDirectoryPeerLocked(testPeerID(fmt.Sprintf("adv-%d", i)), pub, "", "", node, now, directoryHearsay)
		sub.mx.Unlock()
	}

	if got := len(sub.overlayNodesSnapshot(maxPeersPerOverlay)); got != 5 {
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
	sub.rememberDirectoryPeerLocked(testPeerID("stale"), pub, "", "", node, time.Now(), directoryHearsay)
	sub.mx.Unlock()

	if got := len(sub.overlayNodesSnapshot(maxPeersPerOverlay)); got != 0 {
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

	peer := &overlayPeer{id: peerID, route: newTestPeerRoute("")}
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

// The advertisement answer is built while s.mx is held, and s.mx is taken from
// under the plumtree engine lock on every forwarded broadcast part. Copying the
// whole directory to hand back three entries put that cost on block
// propagation, so the sample must be bounded by the limit, not by the directory.
func TestAdvertisementSamplesOnlyUpToLimit(t *testing.T) {
	sub := testDirectorySubscription(t)
	sub.spec.FullID = make([]byte, 32)
	now := time.Now()

	const filed = 64
	for i := range filed {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("generate key: %v", err)
		}
		node, err := overlay.NewNode(sub.spec.FullID, priv)
		if err != nil {
			t.Fatalf("build overlay node: %v", err)
		}
		sub.mx.Lock()
		sub.rememberDirectoryPeerLocked(testPeerID(fmt.Sprintf("adv-%d", i)), pub, "", "", node, now, directoryHearsay)
		sub.mx.Unlock()
	}

	const limit = 3
	list := sub.overlayNodesSnapshot(limit)
	if len(list) != limit {
		t.Fatalf("advertised %d nodes, want %d", len(list), limit)
	}
	seen := map[string]struct{}{}
	for i := range list {
		if !overlayNodeHasSerializableID(&list[i]) {
			t.Fatal("sampled node is not serializable")
		}
		key, ok := list[i].ID.(keys.PublicKeyED25519)
		if !ok {
			t.Fatalf("sampled node has key type %T", list[i].ID)
		}
		if _, dup := seen[string(key.Key)]; dup {
			t.Fatal("sample repeated the same peer")
		}
		seen[string(key.Key)] = struct{}{}
	}
}

// The sample keeps pointers into directory rows while the lock is held and
// copies them afterwards; a shallow copy would hand the caller slices that the
// next announcement merge could swap underneath it.
func TestAdvertisementReturnsDeepCopies(t *testing.T) {
	sub := testDirectorySubscription(t)
	sub.spec.FullID = make([]byte, 32)
	now := time.Now()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	node, err := overlay.NewNode(sub.spec.FullID, priv)
	if err != nil {
		t.Fatalf("build overlay node: %v", err)
	}
	id := testPeerID("deep-copy")
	sub.mx.Lock()
	sub.rememberDirectoryPeerLocked(id, pub, "", "", node, now, directoryHearsay)
	sub.mx.Unlock()

	list := sub.overlayNodesSnapshot(4)
	if len(list) != 1 {
		t.Fatalf("advertised %d nodes, want 1", len(list))
	}

	sub.mx.Lock()
	stored := sub.directory[id].announced
	sub.mx.Unlock()

	if &list[0].Signature[0] == &stored.Signature[0] {
		t.Fatal("advertised node aliases the stored signature")
	}
	if &list[0].Overlay[0] == &stored.Overlay[0] {
		t.Fatal("advertised node aliases the stored overlay id")
	}
}

// noteDirectoryActivity runs before admission and before the rate limiter, on a
// path any peer reaches with one unauthenticated overlay query. Filing a row
// there let a stranger take one of maxPeersPerOverlay slots and evict a row that
// carries a signed announcement - and the stranger's own row, refreshed by every
// further query, would never be the victim.
func TestDirectoryActivityDoesNotFileUnknownPeer(t *testing.T) {
	sub := testDirectorySubscription(t)

	sub.noteDirectoryActivity(testPeerID("stranger"), "1.2.3.4:1")

	if size := sub.directorySize(); size != 0 {
		t.Fatalf("directory filed %d rows for an unauthenticated peer, want 0", size)
	}
}

// A stranger hammering the detached query path must not push the directory to
// its cap: that is what stops learning from gossip and parks DHT discovery.
func TestDirectoryActivityFloodDoesNotEvictAnnouncedRows(t *testing.T) {
	sub := testDirectorySubscription(t)
	sub.spec.FullID = make([]byte, 32)
	now := time.Now()

	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}
	node, err := overlay.NewNode(sub.spec.FullID, priv)
	if err != nil {
		t.Fatalf("build overlay node: %v", err)
	}
	announced := testPeerID("announced")
	sub.mx.Lock()
	sub.rememberDirectoryPeerLocked(announced, pub, "", "", node, now, directoryHearsay)
	sub.mx.Unlock()

	for i := range maxPeersPerOverlay * 2 {
		sub.noteDirectoryActivity(testPeerID(fmt.Sprintf("flood-%d", i)), "1.2.3.4:1")
	}

	if size := sub.directorySize(); size != 1 {
		t.Fatalf("directory holds %d rows after the flood, want 1", size)
	}
	sub.mx.Lock()
	kept := sub.directory[announced]
	sub.mx.Unlock()
	if kept == nil || kept.announced == nil {
		t.Fatal("the announced row was evicted by unauthenticated activity")
	}
}

// The refresh half must keep working: a row we already keep is the only thing
// that stops a peer talking to us constantly from ageing out of the directory.
func TestDirectoryActivityRefreshesKnownPeer(t *testing.T) {
	sub := testDirectorySubscription(t)
	id := testPeerID("known")
	stale := time.Now().Add(-time.Hour)

	sub.mx.Lock()
	sub.rememberDirectoryPeerLocked(id, testDirectoryPub(t), "", "", nil, stale, directoryHearsay)
	sub.mx.Unlock()

	sub.noteDirectoryActivity(id, "9.9.9.9:9")

	sub.mx.Lock()
	entry := sub.directory[id]
	sub.mx.Unlock()
	if entry == nil {
		t.Fatal("known row disappeared")
	}
	if !entry.lastSeenAt.After(stale) {
		t.Fatal("activity did not refresh the row")
	}
	if entry.adnlAddr != "9.9.9.9:9" {
		t.Fatalf("activity did not record the address, got %q", entry.adnlAddr)
	}
}
