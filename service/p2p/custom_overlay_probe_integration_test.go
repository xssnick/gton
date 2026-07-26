package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

type stubDHTBackend struct {
	mx    sync.Mutex
	addrs map[PeerID]stubDHTEntry
}

type stubDHTEntry struct {
	addr string
	pub  ed25519.PublicKey
}

func newStubDHTBackend() *stubDHTBackend {
	return &stubDHTBackend{addrs: map[PeerID]stubDHTEntry{}}
}

func (s *stubDHTBackend) register(id PeerID, addr string, pub ed25519.PublicKey) {
	s.mx.Lock()
	s.addrs[id] = stubDHTEntry{addr: addr, pub: append(ed25519.PublicKey(nil), pub...)}
	s.mx.Unlock()
}

func (s *stubDHTBackend) FindAddresses(_ context.Context, key []byte) (*adnladdr.List, ed25519.PublicKey, error) {
	id, err := NewPeerID(key)
	if err != nil {
		return nil, nil, err
	}

	s.mx.Lock()
	entry, ok := s.addrs[id]
	s.mx.Unlock()
	if !ok {
		return nil, nil, errors.New("address is not registered in stub DHT")
	}

	host, portValue, err := net.SplitHostPort(entry.addr)
	if err != nil {
		return nil, nil, err
	}
	port, err := strconv.Atoi(portValue)
	if err != nil {
		return nil, nil, err
	}
	addr, err := adnladdr.NewAddress(net.ParseIP(host), int32(port))
	if err != nil {
		return nil, nil, err
	}
	switch value := addr.(type) {
	case *adnladdr.UDP:
		addr = *value
	case *adnladdr.UDP6:
		addr = *value
	}

	now := int32(time.Now().Unix())
	return &adnladdr.List{
		Addresses:  []adnladdr.Address{addr},
		Version:    now,
		ReinitDate: now,
	}, entry.pub, nil
}

func (s *stubDHTBackend) FindOverlayNodes(context.Context, []byte, ...*dht.Continuation) (*overlay.NodesList, *dht.Continuation, error) {
	return nil, nil, errors.New("not supported by stub DHT")
}

func (s *stubDHTBackend) FindValue(context.Context, *dht.Key, ...*dht.Continuation) (*dht.Value, *dht.Continuation, error) {
	return nil, nil, errors.New("not supported by stub DHT")
}

func (s *stubDHTBackend) StoreAddress(context.Context, adnladdr.List, time.Duration, ed25519.PrivateKey) (int, []byte, error) {
	return 0, nil, errors.New("not supported by stub DHT")
}

func (s *stubDHTBackend) StoreOverlayNodes(context.Context, []byte, *overlay.NodesList, time.Duration) (int, []byte, error) {
	return 0, nil, errors.New("not supported by stub DHT")
}

type probeNodeHarness struct {
	node   *Node
	sub    *overlaySubscription
	gw     *adnl.Gateway
	key    ed25519.PrivateKey
	id     PeerID
	addr   string
	cancel context.CancelFunc
}

func freeUDPPort(t *testing.T) int {
	t.Helper()

	conn, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("reserve udp port: %v", err)
	}
	port := conn.LocalAddr().(*net.UDPAddr).Port
	_ = conn.Close()
	return port
}

func probeTestPeerID(t *testing.T, key ed25519.PrivateKey) PeerID {
	t.Helper()

	raw, err := tl.Hash(keys.PublicKeyED25519{Key: key.Public().(ed25519.PublicKey)})
	if err != nil {
		t.Fatalf("hash node key: %v", err)
	}
	id, err := NewPeerID(raw)
	if err != nil {
		t.Fatalf("parse node id: %v", err)
	}
	return id
}

func startProbeNodeHarness(t *testing.T, key ed25519.PrivateKey, listenAddr string, backend dhtBackend, spec overlaySpec) *probeNodeHarness {
	t.Helper()

	gw := adnl.NewGateway(key)
	if err := gw.StartServer(listenAddr); err != nil {
		t.Fatalf("start gateway %s: %v", listenAddr, err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	node := &Node{
		log:     discardLogger(),
		privKey: key,
		localID: probeTestPeerID(t, key),
		gateway: gw,
		pool:    newPeerPool(gw, nil, nil),
		peerUse: map[PeerID]peerUse{},
		dht:     backend,
		runCtx:  ctx,
	}
	sub, err := node.newOverlaySubscription(spec)
	if err != nil {
		cancel()
		_ = gw.Close()
		t.Fatalf("create overlay subscription: %v", err)
	}

	harness := &probeNodeHarness{
		node:   node,
		sub:    sub,
		gw:     gw,
		key:    key,
		id:     node.localID,
		addr:   listenAddr,
		cancel: cancel,
	}
	t.Cleanup(func() {
		harness.stop()
	})
	return harness
}

func (h *probeNodeHarness) stop() {
	h.cancel()
	h.sub.close()
	_ = h.gw.Close()
	h.node.wg.Wait()
}

func (h *probeNodeHarness) connectTo(t *testing.T, target PeerID) {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := h.sub.connectFixedNode(ctx, target); err != nil {
		t.Fatalf("connect fixed node: %v", err)
	}
}

func (h *probeNodeHarness) peerFor(id PeerID) *overlayPeer {
	h.sub.mx.Lock()
	defer h.sub.mx.Unlock()
	return h.sub.peers[id]
}

func probeTestOverlaySpec(t *testing.T, members ...PeerID) overlaySpec {
	t.Helper()

	fixedIDs := map[PeerID]struct{}{}
	for _, id := range members {
		fixedIDs[id] = struct{}{}
	}
	shortID := make([]byte, 32)
	fullID := make([]byte, 32)
	shortID[0] = 0x77
	fullID[0] = 0x78
	return overlaySpec{
		Name:         "custom.probe-integration",
		Kind:         overlayKindCustomFixed,
		Workchain:    0,
		Shard:        topShard,
		FullID:       fullID,
		ShortID:      shortID,
		FixedNodes:   append([]PeerID(nil), members...),
		FixedNodeIDs: fixedIDs,
	}
}

func waitProbeCondition(t *testing.T, timeout time.Duration, what string, condition func() bool) {
	t.Helper()

	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", what)
}

// TestFixedProbeRecoveryEndToEnd drives the full watchdog ladder over real
// UDP loopback gateways: healthy probing, remote silence escalating through
// soft and hard recovery, and automatic reconnection once the remote returns
// on the same address and key.
func TestFixedProbeRecoveryEndToEnd(t *testing.T) {
	if testing.Short() {
		t.Skip("integration test")
	}

	probePolicy := fixedProbePolicy{
		minDelay:              40 * time.Millisecond,
		timeout:               150 * time.Millisecond,
		softFailures:          2,
		silence:               250 * time.Millisecond,
		softRecoveryCooldown:  200 * time.Millisecond,
		hardMinSoftAttempts:   2,
		hardSilence:           900 * time.Millisecond,
		hardRecoveryCooldown:  500 * time.Millisecond,
		pendingChannelTimeout: 900 * time.Millisecond,
		rxStaleThreshold:      time.Hour,
		rxStaleCooldown:       time.Hour,
	}

	keyASeed := make([]byte, ed25519.SeedSize)
	keyBSeed := make([]byte, ed25519.SeedSize)
	if _, err := rand.Read(keyASeed); err != nil {
		t.Fatalf("seed key A: %v", err)
	}
	if _, err := rand.Read(keyBSeed); err != nil {
		t.Fatalf("seed key B: %v", err)
	}
	keyA := ed25519.NewKeyFromSeed(keyASeed)
	keyB := ed25519.NewKeyFromSeed(keyBSeed)

	addrA := fmt.Sprintf("127.0.0.1:%d", freeUDPPort(t))
	addrB := fmt.Sprintf("127.0.0.1:%d", freeUDPPort(t))

	backend := newStubDHTBackend()
	idA := probeTestPeerID(t, keyA)
	idB := probeTestPeerID(t, keyB)
	backend.register(idA, addrA, keyA.Public().(ed25519.PublicKey))
	backend.register(idB, addrB, keyB.Public().(ed25519.PublicKey))

	spec := probeTestOverlaySpec(t, idA, idB)
	nodeA := startProbeNodeHarness(t, keyA, addrA, backend, spec)
	nodeB := startProbeNodeHarness(t, keyB, addrB, backend, spec)

	nodeA.connectTo(t, idB)
	nodeB.connectTo(t, idA)

	probeCtx, probeCancel := context.WithCancel(context.Background())
	probeDone := make(chan struct{})
	defer func() {
		probeCancel()
		<-probeDone
	}()
	go func() {
		defer close(probeDone)
		for probeCtx.Err() == nil {
			if nodeA.peerFor(idB) == nil {
				// The production run() loop re-seeds fixed peers on its DHT
				// ticker; mirror it so a transport closed by a transient UDP
				// write error (e.g. ENOBUFS under full-suite load) is
				// re-attached instead of stalling the harness.
				nodeA.sub.startPeerDiscovery(probeCtx)
			}
			nodeA.sub.probeFixedPeers(probeCtx, probePolicy)
			select {
			case <-probeCtx.Done():
			case <-time.After(probePolicy.minDelay):
			}
		}
	}()

	// Phase 1: the pair is healthy and pongs flow.
	waitProbeCondition(t, 20*time.Second, "first pong from node B", func() bool {
		peer := nodeA.peerFor(idB)
		if peer == nil {
			return false
		}
		return !peer.statsSnapshot().lastPongAt.IsZero()
	})

	// Phase 2: node B disappears without a disconnect; the watchdog must
	// escalate through soft recovery into a hard transport rebuild.
	nodeB.stop()

	waitProbeCondition(t, 15*time.Second, "soft recovery on node A", func() bool {
		soft, _ := nodeA.sub.recoverySnapshot()
		return soft >= 1
	})
	waitProbeCondition(t, 15*time.Second, "hard recovery on node A", func() bool {
		_, hard := nodeA.sub.recoverySnapshot()
		return hard >= 1
	})

	// Phase 3: node B returns with the same key and address; node A must
	// re-attach and see fresh pongs without a restart.
	revivedAt := time.Now()
	nodeBRevived := startProbeNodeHarness(t, keyB, addrB, backend, spec)
	nodeBRevived.connectTo(t, idA)

	waitProbeCondition(t, 20*time.Second, "pong after node B revival", func() bool {
		peer := nodeA.peerFor(idB)
		if peer == nil {
			// Between hard recoveries the peer can be briefly absent while
			// discovery re-attaches it.
			return false
		}
		return peer.statsSnapshot().lastPongAt.After(revivedAt)
	})
}
