package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"sync/atomic"
	"testing"
	"time"

	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

// One query used to be allowed to teach us maxPeersPerOverlay nodes, each
// costing a signature check and a directory write. Honest peers send four.
func TestBoundedAdvertisedNodesCapsOneExchange(t *testing.T) {
	nodes := make([]overlay.Node, maxAdvertisedPeersPerQuery*4)
	if got := len(boundedAdvertisedNodes(nodes)); got != maxAdvertisedPeersPerQuery {
		t.Fatalf("bounded %d nodes, want %d", got, maxAdvertisedPeersPerQuery)
	}

	short := make([]overlay.Node, maxRandomPeerReply)
	if got := len(boundedAdvertisedNodes(short)); got != maxRandomPeerReply {
		t.Fatalf("bounded an honest answer to %d nodes, want %d", got, maxRandomPeerReply)
	}
}

// The sender's own announcement plus the address its query arrived from is the
// one endpoint in an exchange that needs no DHT lookup — and the only way a peer
// that reached us first ever becomes reachable, since it never joins a public
// roster and overlay.node carries no address.
func TestLearnQuerySourceFilesSenderAddressAsVerified(t *testing.T) {
	sub, spec := testLearningSubscription(t)

	senderPublic, senderPrivate, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate sender key: %v", err)
	}
	sender, err := peerIDFromPublicKey(senderPublic)
	if err != nil {
		t.Fatalf("sender peer id: %v", err)
	}
	node, err := overlay.NewNode(spec.FullID, senderPrivate)
	if err != nil {
		t.Fatalf("build sender node: %v", err)
	}

	// C++ puts its own record first and fills the rest with other peers.
	sub.learnQuerySource(sender, "10.9.9.9:30303", []overlay.Node{
		*node,
		signedAdvertisedPeer(t, spec.FullID),
	})

	sub.mx.Lock()
	entry := sub.directory[sender]
	sub.mx.Unlock()
	if entry == nil {
		t.Fatal("the query source was not filed in the directory")
	}
	if entry.adnlAddr != "10.9.9.9:30303" {
		t.Fatalf("filed address = %q, want the address the query arrived from", entry.adnlAddr)
	}
	if !entry.verified {
		t.Fatal("a peer that queried us over its own transport must be filed as verified")
	}
	if entry.announced == nil {
		t.Fatal("the sender's announcement was not kept, so it cannot be advertised")
	}
	if size := sub.directorySize(); size != 1 {
		t.Fatalf("directory holds %d rows, want only the query source", size)
	}
}

// Only the sender's own record may claim the sender's address: everything else
// in the list is hearsay, and a record can be forged for any id.
func TestLearnQuerySourceRejectsRecordsItCannotAttribute(t *testing.T) {
	forged := func(t *testing.T, overlayID []byte) overlay.Node {
		node := signedAdvertisedPeer(t, overlayID)
		node.Signature[0] ^= 0xFF
		return node
	}

	tests := []struct {
		name string
		node func(t *testing.T, overlayID []byte) overlay.Node
	}{
		{name: "another peer's record", node: signedAdvertisedPeer},
		{name: "broken signature", node: forged},
		{
			name: "stale version",
			node: func(t *testing.T, overlayID []byte) overlay.Node {
				node := signedAdvertisedPeer(t, overlayID)
				node.Version = int32(time.Now().Add(-2 * overlayPeerTTL).Unix())
				return node
			},
		},
		{
			name: "another overlay",
			node: func(t *testing.T, _ []byte) overlay.Node {
				return signedAdvertisedPeer(t, make([]byte, PeerIDSize))
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			sub, spec := testLearningSubscription(t)
			sub.learnQuerySource(testPeerID("sender"), "10.9.9.9:30303", []overlay.Node{
				test.node(t, spec.FullID),
			})
			if size := sub.directorySize(); size != 0 {
				t.Fatalf("filed %d rows for a record it cannot attribute to the sender", size)
			}
		})
	}
}

func testLearningSubscription(t *testing.T) (*overlaySubscription, overlaySpec) {
	t.Helper()

	spec, err := buildOverlaySpec(make([]byte, PeerIDSize), -1, topShard, "masterchain")
	if err != nil {
		t.Fatalf("build overlay spec: %v", err)
	}
	_, localKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate local key: %v", err)
	}

	return testOverlaySubscription(&overlaySubscription{
		node:      &Node{log: discardLogger(), privKey: localKey},
		spec:      spec,
		log:       discardLogger(),
		peers:     map[PeerID]*overlayPeer{},
		directory: map[PeerID]*directoryEntry{},
	}), spec
}

func TestHandleGetRandomPeersLimitsAdvertisedPeerLearningJobs(t *testing.T) {
	spec, err := buildOverlaySpec(make([]byte, PeerIDSize), -1, topShard, "masterchain")
	if err != nil {
		t.Fatalf("build overlay spec: %v", err)
	}

	_, localKey, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate local key: %v", err)
	}
	first := signedAdvertisedPeer(t, spec.FullID)
	second := signedAdvertisedPeer(t, spec.FullID)

	runCtx, cancel := context.WithCancel(context.Background())
	dhtBackend := &blockingAdvertisedPeerDHT{
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	node := &Node{
		log:     discardLogger(),
		privKey: localKey,
		dht:     dhtBackend,
		runCtx:  runCtx,
	}
	sub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		spec:  spec,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	t.Cleanup(func() {
		cancel()
		node.wg.Wait()
	})

	sub.handleGetRandomPeers(context.Background(), PeerID{}, "", overlay.GetRandomPeers{
		List: overlay.NodesList{List: []overlay.Node{first}},
	})
	select {
	case <-dhtBackend.started:
	case <-time.After(time.Second):
		t.Fatal("first advertised-peer learning job did not start")
	}
	if !sub.beginRefreshPeers() {
		t.Fatal("advertised-peer learning suppressed normal peer refresh")
	}
	sub.endRefreshPeers()

	sub.handleGetRandomPeers(context.Background(), PeerID{}, "", overlay.GetRandomPeers{
		List: overlay.NodesList{List: []overlay.Node{second}},
	})
	if got := dhtBackend.calls.Load(); got != 1 {
		t.Fatalf("concurrent DHT lookup count = %d, want 1", got)
	}

	close(dhtBackend.release)
	node.wg.Wait()

	if sub.advertisedPeerLearning.Load() {
		t.Fatal("advertised-peer learning gate remained active after the job")
	}

	sub.handleGetRandomPeers(context.Background(), PeerID{}, "", overlay.GetRandomPeers{
		List: overlay.NodesList{List: []overlay.Node{second}},
	})
	node.wg.Wait()
	if got := dhtBackend.calls.Load(); got != 2 {
		t.Fatalf("DHT lookup count after completed job = %d, want 2", got)
	}
}

func TestHandleGetRandomPeersReleasesLearningGateForInvalidBatch(t *testing.T) {
	spec, err := buildOverlaySpec(make([]byte, PeerIDSize), -1, topShard, "masterchain")
	if err != nil {
		t.Fatalf("build overlay spec: %v", err)
	}

	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node:  node,
		spec:  spec,
		log:   discardLogger(),
		peers: map[PeerID]*overlayPeer{},
	})
	invalid := structurallyValidAdvertisedPeer()
	invalid.Overlay = append([]byte(nil), spec.ShortID...)
	invalid.Signature = invalid.Signature[:ed25519.SignatureSize-1]

	sub.handleGetRandomPeers(context.Background(), PeerID{}, "", overlay.GetRandomPeers{
		List: overlay.NodesList{List: []overlay.Node{invalid}},
	})
	if sub.advertisedPeerLearning.Load() {
		t.Fatal("invalid batch left advertised-peer learning gate active")
	}
}

func structurallyValidAdvertisedPeer() overlay.Node {
	return overlay.Node{
		ID:        keys.PublicKeyED25519{Key: make(ed25519.PublicKey, ed25519.PublicKeySize)},
		Overlay:   make([]byte, PeerIDSize),
		Version:   1,
		Signature: make([]byte, ed25519.SignatureSize),
	}
}

func signedAdvertisedPeer(t *testing.T, overlayID []byte) overlay.Node {
	t.Helper()

	_, key, err := ed25519.GenerateKey(nil)
	if err != nil {
		t.Fatalf("generate advertised peer key: %v", err)
	}
	node, err := overlay.NewNode(overlayID, key)
	if err != nil {
		t.Fatalf("create advertised peer: %v", err)
	}
	return *node
}

type blockingAdvertisedPeerDHT struct {
	calls   atomic.Int32
	started chan struct{}
	release chan struct{}
}

func (d *blockingAdvertisedPeerDHT) FindAddresses(
	ctx context.Context,
	_ []byte,
) (*adnladdr.List, ed25519.PublicKey, error) {
	if d.calls.Add(1) == 1 {
		close(d.started)
	}

	select {
	case <-ctx.Done():
		return nil, nil, ctx.Err()
	case <-d.release:
		return nil, nil, errors.New("test DHT lookup stopped")
	}
}

func (d *blockingAdvertisedPeerDHT) FindOverlayNodes(
	context.Context,
	[]byte,
	...*dht.Continuation,
) (*overlay.NodesList, *dht.Continuation, error) {
	return nil, nil, errors.New("unexpected overlay lookup")
}

func (d *blockingAdvertisedPeerDHT) FindValue(
	context.Context,
	*dht.Key,
	...*dht.Continuation,
) (*dht.Value, *dht.Continuation, error) {
	return nil, nil, errors.New("unexpected value lookup")
}

func (d *blockingAdvertisedPeerDHT) StoreAddress(
	context.Context,
	adnladdr.List,
	time.Duration,
	ed25519.PrivateKey,
) (int, []byte, error) {
	return 0, nil, errors.New("unexpected address store")
}

func (d *blockingAdvertisedPeerDHT) StoreOverlayNodes(
	context.Context,
	[]byte,
	*overlay.NodesList,
	time.Duration,
) (int, []byte, error) {
	return 0, nil, errors.New("unexpected overlay store")
}
