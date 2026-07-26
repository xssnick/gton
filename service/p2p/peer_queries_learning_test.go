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

func TestCopyAdvertisedPeerCandidatesRejectsMalformedFields(t *testing.T) {
	tests := []struct {
		name   string
		mutate func(*overlay.Node)
		want   int
	}{
		{
			name: "valid",
			want: 1,
		},
		{
			name: "unsupported key",
			mutate: func(node *overlay.Node) {
				node.ID = keys.PublicKeyAES{Key: make([]byte, ed25519.PublicKeySize)}
			},
		},
		{
			name: "short public key",
			mutate: func(node *overlay.Node) {
				node.ID = keys.PublicKeyED25519{Key: make(ed25519.PublicKey, ed25519.PublicKeySize-1)}
			},
		},
		{
			name: "oversized public key",
			mutate: func(node *overlay.Node) {
				node.ID = keys.PublicKeyED25519{Key: make(ed25519.PublicKey, ed25519.PublicKeySize+1)}
			},
		},
		{
			name: "short overlay id",
			mutate: func(node *overlay.Node) {
				node.Overlay = make([]byte, PeerIDSize-1)
			},
		},
		{
			name: "oversized overlay id",
			mutate: func(node *overlay.Node) {
				node.Overlay = make([]byte, PeerIDSize+1)
			},
		},
		{
			name: "different overlay id",
			mutate: func(node *overlay.Node) {
				node.Overlay[0] = 1
			},
		},
		{
			name: "short signature",
			mutate: func(node *overlay.Node) {
				node.Signature = make([]byte, ed25519.SignatureSize-1)
			},
		},
		{
			name: "oversized signature",
			mutate: func(node *overlay.Node) {
				node.Signature = make([]byte, ed25519.SignatureSize+1)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			overlayID := make([]byte, PeerIDSize)
			node := structurallyValidAdvertisedPeer()
			if test.mutate != nil {
				test.mutate(&node)
			}

			if got := len(copyAdvertisedPeerCandidates(overlayID, []overlay.Node{node})); got != test.want {
				t.Fatalf("candidate count = %d, want %d", got, test.want)
			}
		})
	}
}

func TestCopyAdvertisedPeerCandidatesBoundsAndOwnsData(t *testing.T) {
	node := structurallyValidAdvertisedPeer()
	publicKey := node.ID.(keys.PublicKeyED25519).Key
	publicKey[0] = 1
	node.Overlay[0] = 2
	node.Signature[0] = 3

	nodes := make([]overlay.Node, maxPeersPerOverlay+1)
	for i := range nodes {
		nodes[i] = node
	}

	peers := copyAdvertisedPeerCandidates(node.Overlay, nodes)
	if len(peers) != maxPeersPerOverlay {
		t.Fatalf("candidate count = %d, want %d", len(peers), maxPeersPerOverlay)
	}

	publicKey[0] = 4
	node.Overlay[0] = 5
	node.Signature[0] = 6
	if peers[0].publicKey[0] != 1 {
		t.Fatalf("copied public key changed to %d", peers[0].publicKey[0])
	}
	if peers[0].overlayID[0] != 2 {
		t.Fatalf("copied overlay id changed to %d", peers[0].overlayID[0])
	}
	if peers[0].signature[0] != 3 {
		t.Fatalf("copied signature changed to %d", peers[0].signature[0])
	}
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

	sub.handleGetRandomPeers(context.Background(), overlay.GetRandomPeers{
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

	sub.handleGetRandomPeers(context.Background(), overlay.GetRandomPeers{
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

	sub.handleGetRandomPeers(context.Background(), overlay.GetRandomPeers{
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

	sub.handleGetRandomPeers(context.Background(), overlay.GetRandomPeers{
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
