package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"net"
	"runtime"
	"sync"
	"testing"
	"time"

	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
)

// requestBackgroundQUICDial (quic_outbound.go) bounds background relay dials to
// exactly one goroutine per route via the background dial claim, resetting it in
// the spawned closure's defer and on runAsync refusal. The tests below drive
// many concurrent requests against one route and assert only a single dial
// goroutine is ever in flight, and that the flag is always released.

// countGoroutinesContaining counts live goroutines whose stack mentions substr.
func countGoroutinesContaining(substr string) int {
	buf := make([]byte, 1<<20)
	n := runtime.Stack(buf, true)
	needle := []byte(substr)
	count := 0
	for _, g := range bytes.Split(buf[:n], []byte("\n\n")) {
		if bytes.Contains(g, needle) {
			count++
		}
	}
	return count
}

func TestRequestBackgroundQUICDialSingleFlightPerRoute(t *testing.T) {
	remotePub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate remote key: %v", err)
	}
	remoteID := peerIDForQUICOutboundTest(t, remotePub)

	dht := &blockingOutboundRouteDHT{
		addresses: &adnladdr.List{
			Addresses: []adnladdr.Address{
				adnladdr.UDP{IP: net.IPv4(127, 0, 0, 1), Port: 24099},
			},
		},
		pub:     remotePub,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}

	node := newTestNode(t)
	node.dht = dht
	runCtx, cancel := context.WithCancel(context.Background())
	node.runCtx = runCtx
	// An empty QUIC route forces resolveQUICRoute through the (blocking) DHT.
	peer := &overlayPeer{
		node:  node,
		id:    remoteID,
		pub:   remotePub,
		route: newTestPeerRoute(""),
	}

	t.Cleanup(func() {
		cancel()
		select {
		case <-dht.release:
		default:
			close(dht.release)
		}
		node.wg.Wait()
	})

	const callers = 16
	var launched sync.WaitGroup
	launched.Add(callers)
	for range callers {
		go func() {
			defer launched.Done()
			peer.requestBackgroundQUICDial()
		}()
	}
	launched.Wait()

	// The one dial that won the CAS reaches the blocking DHT resolve.
	select {
	case <-dht.started:
	case <-time.After(2 * time.Second):
		t.Fatal("background dial did not start DHT resolution")
	}

	// Sample the number of in-flight dial goroutines over a settle window. With
	// the CAS this stays at 1; without it every caller spawns a goroutine that
	// parks behind the gateway's per-peer dial mutex / waitReady, so the max
	// climbs toward `callers`.
	maxInFlight := 0
	deadline := time.Now().Add(400 * time.Millisecond)
	for time.Now().Before(deadline) {
		if inFlight := countGoroutinesContaining("requestBackgroundQUICDial"); inFlight > maxInFlight {
			maxInFlight = inFlight
		}
		time.Sleep(2 * time.Millisecond)
	}
	if maxInFlight != 1 {
		t.Fatalf("in-flight background dial goroutines = %d, want 1", maxInFlight)
	}
	if peer.route.ClaimBackgroundQUICDial() {
		peer.route.ReleaseBackgroundQUICDial()
		t.Fatal("in-flight dial did not hold the background dial claim")
	}

	// Completing the dial (context cancel -> DHT returns, dialGated finishes)
	// must release the claim so the next window can dial again.
	cancel()
	node.wg.Wait()
	if !peer.route.ClaimBackgroundQUICDial() {
		t.Fatal("background dial stayed claimed after the dial completed")
	}
	peer.route.ReleaseBackgroundQUICDial()
	if !peer.route.QUICDialPermitted(time.Now()) {
		t.Fatal("route did not permit a fresh dial after the previous one completed")
	}
}

func TestRequestBackgroundQUICDialResetsClaimWhenRunAsyncRefuses(t *testing.T) {
	remotePub, _, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate remote key: %v", err)
	}
	remoteID := peerIDForQUICOutboundTest(t, remotePub)

	node := newTestNode(t)
	// A node past shutdown refuses runAsync, so the dial goroutine never spawns.
	setAsyncStoppedLikeStop(node)
	peer := &overlayPeer{
		node:  node,
		id:    remoteID,
		pub:   remotePub,
		route: newTestPeerRoute(""),
	}

	peer.requestBackgroundQUICDial()

	if !peer.route.ClaimBackgroundQUICDial() {
		t.Fatal("refused background dial left its claim held")
	}
	peer.route.ReleaseBackgroundQUICDial()
	for range 100 {
		runtime.Gosched()
	}
	if inFlight := countGoroutinesContaining("requestBackgroundQUICDial"); inFlight != 0 {
		t.Fatalf("refused background dial spawned %d goroutines, want 0", inFlight)
	}
}
