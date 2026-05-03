package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"flexserver/service/storage"
	"net"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/ton"
)

func TestNodeWaitObservedMasterchainBlockAfterVerifiedSeen(t *testing.T) {
	node := newTestNode(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan ton.BlockIDExt, 1)
	go func() {
		block, err := node.WaitObservedMasterchainBlock(ctx)
		if err != nil {
			t.Errorf("wait observed masterchain block: %v", err)
			return
		}
		done <- block
	}()

	node.RememberSeenMasterchainBlock(ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     123,
	})

	select {
	case block := <-done:
		if block.SeqNo != 123 {
			t.Fatalf("unexpected block %+v", block)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for masterchain block")
	}
}

func TestStatusSnapshotTracksLatestBasechainBlock(t *testing.T) {
	node := newTestNode(t)

	node.trackUnverifiedBroadcastBlock(BroadcastEvent{
		Overlay: "basechain",
		Block: ton.BlockIDExt{
			Workchain: 0,
			Shard:     topShard,
			SeqNo:     77,
		},
	})

	snapshot := node.StatusSnapshot()
	if snapshot.LatestBasechain == nil {
		t.Fatal("expected latest basechain block")
	}
	if snapshot.LatestBasechain.SeqNo != 77 {
		t.Fatalf("unexpected latest basechain seqno %d", snapshot.LatestBasechain.SeqNo)
	}
}

func TestRawMasterchainBroadcastDoesNotMoveObservedOrSeen(t *testing.T) {
	logger := discardLogger()
	store := newTestPebbleStore(t)
	node, err := New(Options{Logger: &logger, Storage: store})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	block10 := testBlockID(-1, topShard, 10)
	block9 := testBlockID(-1, topShard, 9)

	node.trackUnverifiedBroadcastBlock(BroadcastEvent{Overlay: "masterchain", Block: block10})
	node.trackUnverifiedBroadcastBlock(BroadcastEvent{Overlay: "masterchain", Block: block9})

	if _, err = node.ObservedMasterchainBlock(); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("raw broadcast moved observed masterchain: %v", err)
	}

	if _, err = node.SeenMasterchainBlock(); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("raw broadcast moved seen masterchain: %v", err)
	}

	if _, err = store.SeenMasterchainBlock(context.Background()); err == nil {
		t.Fatal("broadcast seen block was persisted before validation")
	}
}

func TestTrustedInitBlockRequiresFullBlockID(t *testing.T) {
	node := newTestNode(t)

	if _, err := node.TrustedInitBlock(); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("empty trusted init block error = %v, want ErrNotFound", err)
	}

	node.trustedInitBlock = testBlockID(-1, topShard, 0)
	got, err := node.TrustedInitBlock()
	if err != nil {
		t.Fatalf("trusted init block: %v", err)
	}
	if !got.Equals(&node.trustedInitBlock) {
		t.Fatalf("trusted init block = %+v, want %+v", got, node.trustedInitBlock)
	}
}

func TestRememberSeenMasterchainKeepsNewestRuntimeHint(t *testing.T) {
	logger := discardLogger()
	store := newTestPebbleStore(t)
	node, err := New(Options{Logger: &logger, Storage: store})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	block10 := testBlockID(-1, topShard, 10)
	block9 := testBlockID(-1, topShard, 9)

	ctx := context.Background()
	node.RememberSeenMasterchainBlock(block10)
	node.RememberSeenMasterchainBlock(block9)

	latest, err := node.SeenMasterchainBlock()
	if err != nil {
		t.Fatalf("runtime latest masterchain: %v", err)
	}
	if !latest.Equals(&block10) {
		t.Fatalf("runtime latest = %+v, want %+v", latest, block10)
	}
	observed, err := node.ObservedMasterchainBlock()
	if err != nil {
		t.Fatalf("runtime observed masterchain: %v", err)
	}
	if !observed.Equals(&block10) {
		t.Fatalf("runtime observed = %+v, want %+v", observed, block10)
	}
	if _, err = store.SeenMasterchainBlock(ctx); err == nil {
		t.Fatal("runtime seen block was persisted by p2p node")
	}
}

func TestMasterchainDownloadCacheRequiresVerifiedPath(t *testing.T) {
	logger := discardLogger()
	store := newTestPeerStore()
	node, err := New(Options{Logger: &logger, PeerServingStorage: store})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	block := testBlockID(-1, topShard, 10)
	downloaded := &DownloadedBlock{
		ID:       block,
		BlockBOC: []byte{1, 2, 3},
		ProofBOC: []byte{4, 5, 6},
	}

	node.rememberDownloadedBlock(nil, downloaded)
	if _, err = store.BlockFull(context.Background(), block); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("unverified masterchain block cache = %v, want ErrNotFound", err)
	}

	node.RememberVerifiedBlockFull(nil, *downloaded)
	if _, err = store.BlockFull(context.Background(), block); err != nil {
		t.Fatalf("verified masterchain block was not cached: %v", err)
	}
}

func TestNodeWaitBasechainBlock(t *testing.T) {
	node := newTestNode(t)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan ton.BlockIDExt, 1)
	go func() {
		block, err := node.WaitBasechainBlock(ctx)
		if err != nil {
			t.Errorf("wait basechain block: %v", err)
			return
		}
		done <- block
	}()

	node.trackUnverifiedBroadcastBlock(BroadcastEvent{
		Overlay: "basechain",
		Block: ton.BlockIDExt{
			Workchain: 0,
			Shard:     topShard,
			SeqNo:     77,
		},
	})

	select {
	case block := <-done:
		if block.SeqNo != 77 {
			t.Fatalf("unexpected block %+v", block)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for basechain block")
	}
}

func TestNewUsesConfiguredADNLKeys(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate ADNL key: %v", err)
	}
	_, dhtPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate DHT key: %v", err)
	}

	node, err := New(Options{
		PrivateKey:         priv,
		DHTPrivateKey:      dhtPriv,
		DHTListenAddr:      "127.0.0.1:30304",
		PeerServingStorage: newTestPeerStore(),
	})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}
	t.Cleanup(func() {
		if node.storage != nil {
			_ = node.storage.Close()
		}
	})

	if !bytes.Equal(node.privKey, priv) {
		t.Fatal("expected configured ADNL key to be used")
	}
	if !bytes.Equal(node.dhtPrivKey, dhtPriv) {
		t.Fatal("expected configured DHT key to be used")
	}
}

func TestStartDHTClientUsesSeparateGateway(t *testing.T) {
	node := newTestNode(t)

	if err := node.startDHTClient(&liteclient.GlobalConfig{}); err != nil {
		t.Fatalf("start DHT client: %v", err)
	}
	t.Cleanup(func() {
		if node.dht != nil {
			node.dht.Close()
		}
	})

	if node.dht == nil {
		t.Fatal("expected DHT client to be initialized")
	}

	if node.dhtGateway == nil {
		t.Fatal("expected dedicated DHT gateway to be initialized")
	}

	if node.dhtGateway == node.gateway {
		t.Fatal("expected DHT to use a separate gateway")
	}

	if !bytes.Equal(node.dhtGateway.GetID(), node.gateway.GetID()) {
		t.Fatal("expected DHT gateway to reuse node identity")
	}

	if len(node.dhtGateway.GetAddressList().Addresses) != 0 {
		t.Fatal("expected DHT gateway to stay in client mode without published addresses")
	}
}

func TestStartDHTServerUsesDedicatedGatewayIDAndInnerClient(t *testing.T) {
	node := newTestNode(t)
	node.dhtListenAddr = freeUDPAddr(t)
	node.externalIP = net.ParseIP("127.0.0.1").To4()
	ctx, cancel := context.WithCancel(context.Background())

	if err := node.startDHTServer(ctx, &liteclient.GlobalConfig{}); err != nil {
		t.Fatalf("start DHT server: %v", err)
	}
	t.Cleanup(func() {
		cancel()
		if node.dhtServer != nil {
			_ = node.dhtServer.Close()
		}
	})

	if node.dhtServer == nil {
		t.Fatal("expected DHT server to be initialized")
	}

	client, ok := node.dht.(*dht.Client)
	if !ok {
		t.Fatalf("expected p2p DHT handle to use inner client, got %T", node.dht)
	}
	if client != node.dhtServer.Client {
		t.Fatal("expected p2p DHT handle to point at DHT server inner client")
	}

	if node.dhtGateway == nil {
		t.Fatal("expected dedicated DHT gateway to be initialized")
	}
	if node.dhtGateway == node.gateway {
		t.Fatal("expected DHT server to use a separate gateway")
	}
	if bytes.Equal(node.dhtGateway.GetID(), node.gateway.GetID()) {
		t.Fatal("expected DHT server to use its own ADNL id")
	}
	if len(node.dhtGateway.GetAddressList().Addresses) == 0 {
		t.Fatal("expected DHT server gateway to publish an address")
	}
}

func freeUDPAddr(tb testing.TB) string {
	tb.Helper()

	pc, err := net.ListenPacket("udp", "127.0.0.1:0")
	if err != nil {
		tb.Fatalf("listen packet: %v", err)
	}
	addr := pc.LocalAddr().String()
	if err := pc.Close(); err != nil {
		tb.Fatalf("close packet listener: %v", err)
	}
	return addr
}
