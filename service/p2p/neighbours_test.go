package p2p

import (
	"context"
	"crypto/ed25519"
	"errors"
	"net"
	"testing"
	"time"

	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/overlay"
)

func TestOverlayPeerLivenessRecoversAfterInboundTraffic(t *testing.T) {
	peer := &overlayPeer{
		alive:         true,
		lastReceiveAt: time.Now().Add(-20 * time.Second),
	}

	peer.queryFailed()
	peer.queryFailed()
	peer.queryFailed()

	if peer.statsSnapshot().alive {
		t.Fatalf("expected peer to become dead after repeated missed pings")
	}

	peer.noteReceive()
	stats := peer.statsSnapshot()
	if !stats.alive {
		t.Fatalf("expected inbound traffic to mark peer alive again")
	}
	if stats.lastReceiveAt.IsZero() {
		t.Fatalf("expected inbound traffic to refresh last receive timestamp")
	}
}

func TestReloadNeighboursReplacesWorstPeer(t *testing.T) {
	sub := &overlaySubscription{
		log:        discardLogger(),
		peers:      map[string]*overlayPeer{},
		neighbours: make([]string, 0, maxQueryNeighbours),
	}

	now := int32(time.Now().Unix())
	for i := 0; i < maxQueryNeighbours; i++ {
		id := string(rune('a' + i))
		peer := &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
		}
		if i == 0 {
			peer.unreliability = peerStopUnreliability + 1
		}
		sub.peers[id] = peer
		sub.neighbours = append(sub.neighbours, id)
	}

	fresh := &overlayPeer{
		id:            "fresh",
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	sub.peers[fresh.id] = fresh

	sub.reloadNeighbours()

	if sub.hasNeighbourLocked("a") {
		t.Fatalf("expected worst neighbour to be replaced")
	}
	if !sub.hasNeighbourLocked(fresh.id) {
		t.Fatalf("expected fresh peer to be added to neighbours")
	}
}

func TestReloadNeighboursPrefersAliveKnownPeers(t *testing.T) {
	sub := &overlaySubscription{
		log:        discardLogger(),
		peers:      map[string]*overlayPeer{},
		neighbours: []string{"dead"},
	}

	now := int32(time.Now().Unix())
	sub.peers["dead"] = &overlayPeer{
		id:            "dead",
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         false,
		lastReceiveAt: time.Now().Add(-time.Minute),
	}
	sub.peers["alive"] = &overlayPeer{
		id:            "alive",
		overlay:       &overlay.ADNLOverlayWrapper{},
		announced:     &overlay.Node{Version: now},
		alive:         true,
		lastReceiveAt: time.Now(),
	}

	sub.reloadNeighbours()

	if sub.hasNeighbourLocked("dead") {
		t.Fatalf("expected dead known peer to be dropped from neighbours")
	}
	if !sub.hasNeighbourLocked("alive") {
		t.Fatalf("expected alive known peer to occupy neighbour slot")
	}
}

func TestPingTargetsRotateNeighbours(t *testing.T) {
	sub := &overlaySubscription{
		log:        discardLogger(),
		peers:      map[string]*overlayPeer{},
		neighbours: []string{},
	}

	now := int32(time.Now().Unix())
	for i := 0; i < 8; i++ {
		id := string(rune('a' + i))
		sub.peers[id] = &overlayPeer{
			id:            id,
			overlay:       &overlay.ADNLOverlayWrapper{},
			announced:     &overlay.Node{Version: now},
			alive:         true,
			lastReceiveAt: time.Now(),
			versionMajor:  3,
		}
		sub.neighbours = append(sub.neighbours, id)
	}

	first := sub.pingTargets()
	second := sub.pingTargets()
	if len(first) != peerPingFanout || len(second) != peerPingFanout {
		t.Fatalf("unexpected ping target count: first=%d second=%d", len(first), len(second))
	}
	if first[0].id == second[0].id {
		t.Fatalf("expected round-robin ping selection to advance, got %q twice", first[0].id)
	}
}

func TestAnnounceSelfRetriesAfterDHTWarmup(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{
		Logger:     &logger,
		ListenAddr: "127.0.0.1:30303",
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	node.externalIP = net.ParseIP("127.0.0.1").To4()
	node.gateway.SetAddressList([]adnladdr.Address{&adnladdr.UDP{IP: []byte{127, 0, 0, 1}, Port: 30303}})

	spec, err := buildOverlaySpec(make([]byte, 32), -1, topShard, "masterchain")
	if err != nil {
		t.Fatalf("build overlay spec: %v", err)
	}
	node.subscriptions = map[string]*overlaySubscription{
		"master": {
			node: node,
			spec: spec,
			log:  discardLogger(),
		},
	}

	fake := &fakeDHTClient{
		storeAddressErrs: []error{
			errNoAliveStore,
			nil,
		},
		storeOverlayErrs: []error{
			errNoAliveStore,
			nil,
		},
	}
	node.dht = fake

	if err := node.announceSelf(context.Background()); err != nil {
		t.Fatalf("announce self: %v", err)
	}
	if fake.storeAddressCalls != 2 {
		t.Fatalf("expected 2 address store attempts, got %d", fake.storeAddressCalls)
	}
	if fake.storeOverlayCalls != 2 {
		t.Fatalf("expected 2 overlay store attempts, got %d", fake.storeOverlayCalls)
	}
	if fake.findOverlayNodesCalls == 0 {
		t.Fatalf("expected DHT warmup to query overlay nodes before retry")
	}
	if got := timeoutDuration(t, fake.storeAddressDeadline); got < dhtStoreTimeout-time.Second {
		t.Fatalf("store address timeout too short: %s", got)
	}
	if got := timeoutDuration(t, fake.storeOverlayDeadline); got < dhtStoreTimeout-time.Second {
		t.Fatalf("store overlay timeout too short: %s", got)
	}
	if got := timeoutDuration(t, fake.findOverlayNodesDeadline); got < dhtFindTimeout-time.Second {
		t.Fatalf("find overlay timeout too short: %s", got)
	}
}

var errNoAliveStore = errors.New("no alive nodes found to store this key")

type fakeDHTClient struct {
	storeAddressCalls        int
	storeOverlayCalls        int
	findOverlayNodesCalls    int
	storeAddressErrs         []error
	storeOverlayErrs         []error
	storeAddressDeadline     time.Time
	storeOverlayDeadline     time.Time
	findOverlayNodesDeadline time.Time
}

func (f *fakeDHTClient) Close() {}

func (f *fakeDHTClient) FindOverlayNodes(ctx context.Context, _ []byte, _ ...*dht.Continuation) (*overlay.NodesList, *dht.Continuation, error) {
	f.findOverlayNodesCalls++
	f.findOverlayNodesDeadline, _ = ctx.Deadline()
	return &overlay.NodesList{}, nil, nil
}

func (f *fakeDHTClient) FindAddresses(context.Context, []byte) (*adnladdr.List, ed25519.PublicKey, error) {
	return nil, nil, nil
}

func (f *fakeDHTClient) FindValue(context.Context, *dht.Key, ...*dht.Continuation) (*dht.Value, *dht.Continuation, error) {
	return nil, nil, nil
}

func (f *fakeDHTClient) StoreAddress(ctx context.Context, _ adnladdr.List, _ time.Duration, _ ed25519.PrivateKey) (int, []byte, error) {
	f.storeAddressCalls++
	f.storeAddressDeadline, _ = ctx.Deadline()
	return 1, nil, popErr(&f.storeAddressErrs)
}

func (f *fakeDHTClient) StoreOverlayNodes(ctx context.Context, _ []byte, _ *overlay.NodesList, _ time.Duration) (int, []byte, error) {
	f.storeOverlayCalls++
	f.storeOverlayDeadline, _ = ctx.Deadline()
	return 1, nil, popErr(&f.storeOverlayErrs)
}

func timeoutDuration(tb testing.TB, deadline time.Time) time.Duration {
	tb.Helper()
	if deadline.IsZero() {
		tb.Fatal("expected context deadline")
	}
	return time.Until(deadline)
}

func popErr(errs *[]error) error {
	if len(*errs) == 0 {
		return nil
	}
	err := (*errs)[0]
	*errs = (*errs)[1:]
	return err
}
