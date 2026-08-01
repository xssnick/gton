package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"net"
	"sync/atomic"
	"testing"
	"time"

	adnladdr "github.com/xssnick/tonutils-go/adnl/address"
)

type countingPeerAddressDHT struct {
	dhtBackend

	list  *adnladdr.List
	pub   ed25519.PublicKey
	err   error
	calls atomic.Int32
}

func (d *countingPeerAddressDHT) FindAddresses(
	context.Context,
	[]byte,
) (*adnladdr.List, ed25519.PublicKey, error) {
	d.calls.Add(1)
	return d.list, d.pub, d.err
}

func TestPeerAddressResolverCachesRecentResult(t *testing.T) {
	id := testPeerID("cached-peer-address")
	pub := ed25519.PublicKey(make([]byte, ed25519.PublicKeySize))
	list := &adnladdr.List{
		Addresses: []adnladdr.Address{
			adnladdr.UDP{
				IP:   net.IPv4(127, 0, 0, 1),
				Port: 30303,
			},
		},
		ExpireAt: int32(time.Now().Add(time.Hour).Unix()),
	}
	backend := &countingPeerAddressDHT{
		list: list,
		pub:  pub,
	}
	node := &Node{dht: backend}

	for range 2 {
		gotList, gotPub, err := node.resolvePeerAddresses(context.Background(), id)
		if err != nil {
			t.Fatalf("resolve peer addresses: %v", err)
		}
		if gotList != list {
			t.Fatal("cached lookup returned a different immutable address list")
		}
		if !bytes.Equal(gotPub, pub) {
			t.Fatal("cached lookup returned a different public key")
		}
	}
	if got := backend.calls.Load(); got != 1 {
		t.Fatalf("DHT address lookups = %d, want 1", got)
	}
}

func TestPeerAddressResolverCollapsesConcurrentLookup(t *testing.T) {
	id := testPeerID("in-flight-peer-address")
	pub := ed25519.PublicKey(make([]byte, ed25519.PublicKeySize))
	backend := &blockingOutboundRouteDHT{
		addresses: &adnladdr.List{
			ExpireAt: int32(time.Now().Add(time.Hour).Unix()),
		},
		pub:     pub,
		started: make(chan struct{}),
		release: make(chan struct{}),
	}
	node := &Node{dht: backend}
	results := make(chan error, 2)

	go func() {
		_, _, err := node.resolvePeerAddresses(context.Background(), id)
		results <- err
	}()
	select {
	case <-backend.started:
	case <-time.After(time.Second):
		t.Fatal("first DHT address lookup did not start")
	}

	go func() {
		_, _, err := node.resolvePeerAddresses(context.Background(), id)
		results <- err
	}()
	time.Sleep(10 * time.Millisecond)
	if got := backend.calls.Load(); got != 1 {
		t.Fatalf("concurrent DHT address lookups = %d, want 1", got)
	}

	close(backend.release)
	for range 2 {
		if err := <-results; err != nil {
			t.Fatalf("resolve shared peer address: %v", err)
		}
	}
}

func TestPeerAddressResolverCachesRecentFailure(t *testing.T) {
	id := testPeerID("cached-peer-address-failure")
	lookupErr := errors.New("peer address is not published")
	backend := &countingPeerAddressDHT{err: lookupErr}
	node := &Node{dht: backend}

	for range 2 {
		_, _, err := node.resolvePeerAddresses(context.Background(), id)
		if !errors.Is(err, lookupErr) {
			t.Fatalf("resolve peer address error = %v, want %v", err, lookupErr)
		}
	}
	if got := backend.calls.Load(); got != 1 {
		t.Fatalf("failed DHT address lookups = %d, want 1", got)
	}
}

func TestPeerAddressResolverRefreshesExpiredEntry(t *testing.T) {
	id := testPeerID("expired-peer-address")
	backend := &countingPeerAddressDHT{
		list: &adnladdr.List{
			ExpireAt: int32(time.Now().Add(time.Hour).Unix()),
		},
	}
	node := &Node{dht: backend}

	if _, _, err := node.resolvePeerAddresses(context.Background(), id); err != nil {
		t.Fatalf("resolve initial peer address: %v", err)
	}
	node.peerAddresses.mx.Lock()
	entry := node.peerAddresses.cache[id]
	entry.expiresAt = time.Now().Add(-time.Second)
	node.peerAddresses.cache[id] = entry
	node.peerAddresses.mx.Unlock()

	if _, _, err := node.resolvePeerAddresses(context.Background(), id); err != nil {
		t.Fatalf("refresh expired peer address: %v", err)
	}
	if got := backend.calls.Load(); got != 2 {
		t.Fatalf("DHT address lookups after expiry = %d, want 2", got)
	}
}

func TestPeerAddressResolverDoesNotCacheCanceledLookup(t *testing.T) {
	id := testPeerID("canceled-peer-address")
	backend := &countingPeerAddressDHT{err: context.Canceled}
	node := &Node{dht: backend}

	for range 2 {
		_, _, err := node.resolvePeerAddresses(context.Background(), id)
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("resolve canceled peer address error = %v", err)
		}
	}
	if got := backend.calls.Load(); got != 2 {
		t.Fatalf("canceled DHT address lookups = %d, want 2", got)
	}
}
