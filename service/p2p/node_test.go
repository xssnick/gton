package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"net"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func TestEventDeduperEvictsOldestWithCap(t *testing.T) {
	deduper := newEventDeduper(time.Hour, 2)
	now := time.Unix(100, 0)

	if !deduper.Mark("a", now) {
		t.Fatal("first a mark was not accepted")
	}
	if !deduper.Mark("b", now.Add(time.Second)) {
		t.Fatal("first b mark was not accepted")
	}
	if !deduper.Mark("c", now.Add(2*time.Second)) {
		t.Fatal("first c mark was not accepted")
	}

	if !deduper.Mark("a", now.Add(3*time.Second)) {
		t.Fatal("oldest entry was not evicted when cap was exceeded")
	}
	if deduper.Mark("c", now.Add(4*time.Second)) {
		t.Fatal("recent entry was not retained")
	}
}

func TestPendingBroadcastExpiryReleasesDeduper(t *testing.T) {
	node := newTestNode(t)
	now := time.Unix(100, 0)
	fingerprint := "pending-broadcast"

	if !node.deduper.Mark(fingerprint, now) {
		t.Fatal("pending broadcast was not marked")
	}

	node.pendingBroadcastMx.Lock()
	node.pendingBroadcasts[fingerprint] = pendingBlockBroadcastDecode{
		fingerprint: fingerprint,
		expiresAt:   now.Add(-time.Second),
		bytes:       1,
	}
	node.pendingBroadcastBytes = 1
	node.prunePendingBlockBroadcastDecodesLocked(now)
	node.pendingBroadcastMx.Unlock()

	if !node.deduper.Mark(fingerprint, now.Add(time.Second)) {
		t.Fatal("expired pending broadcast still blocked rebroadcast")
	}
}

func TestPendingBroadcastDecodeSnapshotForPrevUsesIndex(t *testing.T) {
	node := newTestNode(t)
	now := time.Unix(100, 0)
	prevA := testBlockID(-1, topShard, 10)
	prevB := testBlockID(-1, topShard, 11)

	node.schedulePendingBlockBroadcastDecode(pendingBlockBroadcastDecode{
		fingerprint: "pending-a",
		block:       testBlockID(-1, topShard, 12),
		prev:        prevA,
		receivedAt:  now,
		msg:         struct{}{},
	})
	node.schedulePendingBlockBroadcastDecode(pendingBlockBroadcastDecode{
		fingerprint: "pending-b",
		block:       testBlockID(-1, topShard, 13),
		prev:        prevB,
		receivedAt:  now,
		msg:         struct{}{},
	})

	reqs := node.pendingBlockBroadcastDecodeSnapshotForPrev(prevA, now)
	if len(reqs) != 1 || reqs[0].fingerprint != "pending-a" {
		t.Fatalf("snapshot for prev A = %+v, want pending-a", reqs)
	}

	node.forgetPendingBlockBroadcastDecode("pending-a")
	reqs = node.pendingBlockBroadcastDecodeSnapshotForPrev(prevA, now)
	if len(reqs) != 0 {
		t.Fatalf("snapshot for prev A after delete = %+v, want empty", reqs)
	}

	reqs = node.pendingBlockBroadcastDecodeSnapshotForPrev(prevB, now)
	if len(reqs) != 1 || reqs[0].fingerprint != "pending-b" {
		t.Fatalf("snapshot for prev B = %+v, want pending-b", reqs)
	}
}

func TestOverlayBlockForDownloadUsesMonitorMinSplitDepth(t *testing.T) {
	node := newTestNode(t)
	block := testBlockID(0, int64(-0x2000000000000000), 10)

	if got := node.overlayBlockForDownload(block); got.Shard != topShard {
		t.Fatalf("default overlay shard = %016x, want top shard", uint64(got.Shard))
	}

	node.SetMonitorMinSplitDepth(0, 1)
	if got := node.overlayBlockForDownload(block); got.Shard != int64(-0x4000000000000000) {
		t.Fatalf("depth 1 overlay shard = %016x, want c000000000000000", uint64(got.Shard))
	}

	node.SetMonitorMinSplitDepth(0, 2)
	if got := node.overlayBlockForDownload(block); got.Shard != block.Shard {
		t.Fatalf("depth 2 overlay shard = %016x, want exact %016x", uint64(got.Shard), uint64(block.Shard))
	}
}

func TestSetActiveShardOverlaysTracksMonitorPrefixes(t *testing.T) {
	node := newTestNode(t)
	node.zeroStateFileHash = make([]byte, 32)
	node.SetMonitorMinSplitDepth(0, 1)

	leftShard := int64(0x4000000000000000)
	rightShard := int64(-0x4000000000000000)
	if err := node.SetActiveShardOverlays([]ton.BlockIDExt{{Workchain: 0, Shard: leftShard}}); err != nil {
		t.Fatalf("set active left overlay: %v", err)
	}

	leftSub := testSubscriptionForOverlay(t, node, 0, leftShard)
	if !leftSub.isActive() {
		t.Fatal("left shard overlay should be active")
	}
	baseSub := testSubscriptionForOverlay(t, node, 0, topShard)
	if !baseSub.isActive() {
		t.Fatal("basechain parent overlay should be active")
	}

	if err := node.SetActiveShardOverlays([]ton.BlockIDExt{{Workchain: 0, Shard: rightShard}}); err != nil {
		t.Fatalf("set active right overlay: %v", err)
	}

	if leftSub.isActive() {
		t.Fatal("left shard overlay should be inactive after it leaves the active set")
	}
	deleteAt, ok := leftSub.inactiveExpiresAt()
	if !ok || !deleteAt.After(time.Now()) {
		t.Fatalf("left shard inactive expiry = %v, ok=%v", deleteAt, ok)
	}
	rightSub := testSubscriptionForOverlay(t, node, 0, rightShard)
	if !rightSub.isActive() {
		t.Fatal("right shard overlay should be active")
	}
	if !baseSub.isActive() {
		t.Fatal("basechain parent overlay should stay active")
	}

	node.stopExpiredInactiveSubscriptions(time.Now().Add(inactiveShardOverlayTTL + time.Second))
	if _, ok := findSubscriptionForOverlay(t, node, 0, leftShard); ok {
		t.Fatal("expired inactive left shard overlay should be deleted")
	}
}

func TestInactiveSubscriptionRejectsPeerQuery(t *testing.T) {
	node := newTestNode(t)
	node.runCtx = context.Background()
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "basechain",
			ProtoVersionMajor: shardchainProtoVersionMajor,
			ProtoVersionMinor: shardchainProtoVersionMinor,
		},
		log: discardLogger(),
	}
	sub.setActive(false, time.Now().Add(time.Minute))

	err := sub.answerPeerQuery(nil, GetCapabilities{}, func(context.Context, tl.Serializable) error {
		t.Fatal("inactive subscription should not answer peer query")
		return nil
	})
	if err == nil || err.Error() != "shard is inactive" {
		t.Fatalf("inactive query error = %v", err)
	}
}

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
	if len(snapshot.LatestBasechainShards) != 1 || snapshot.LatestBasechainShards[0].SeqNo != 77 {
		t.Fatalf("unexpected latest basechain shards %+v", snapshot.LatestBasechainShards)
	}
}

func TestStatusSnapshotTracksLatestSplitBasechainBlocks(t *testing.T) {
	node := newTestNode(t)

	left := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     77,
	}
	right := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-0x4000000000000000),
		SeqNo:     78,
	}
	node.trackUnverifiedBroadcastBlock(BroadcastEvent{Overlay: "basechain", Block: right})
	node.trackUnverifiedBroadcastBlock(BroadcastEvent{Overlay: "basechain", Block: left})

	snapshot := node.StatusSnapshot()
	if snapshot.LatestBasechain == nil || snapshot.LatestBasechain.SeqNo != right.SeqNo {
		t.Fatalf("unexpected latest basechain block %+v", snapshot.LatestBasechain)
	}
	if len(snapshot.LatestBasechainShards) != 2 {
		t.Fatalf("latest basechain shards = %d, want 2", len(snapshot.LatestBasechainShards))
	}
	if !snapshot.LatestBasechainShards[0].Equals(&left) || !snapshot.LatestBasechainShards[1].Equals(&right) {
		t.Fatalf("unexpected split basechain shards %+v", snapshot.LatestBasechainShards)
	}
}

func TestRawMasterchainBroadcastDoesNotMoveObservedOrSeen(t *testing.T) {
	logger := discardLogger()
	store := newTestPebbleStore(t)
	node, err := New(Options{Logger: &logger, Storage: store, StateFilesDir: t.TempDir()})
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
}

func TestZeroStateBlockRequiresConfiguredZeroState(t *testing.T) {
	node := newTestNode(t)

	if _, err := node.ZeroStateBlock(); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("empty zero state block error = %v, want ErrNotFound", err)
	}

	node.zeroStateBlock = testBlockID(0, 0, 0)
	if _, err := node.ZeroStateBlock(); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("basechain zero state block error = %v, want ErrNotFound", err)
	}

	node.zeroStateBlock = testBlockID(-1, topShard, 0)
	got, err := node.ZeroStateBlock()
	if err != nil {
		t.Fatalf("zero state block: %v", err)
	}
	if !got.Equals(&node.zeroStateBlock) {
		t.Fatalf("zero state block = %+v, want %+v", got, node.zeroStateBlock)
	}
}

func TestInitBlockRequiresConfiguredInitBlock(t *testing.T) {
	node := newTestNode(t)

	if _, err := node.InitBlock(); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("empty init block error = %v, want ErrNotFound", err)
	}

	node.initBlock = testBlockID(-1, topShard, 100)
	got, err := node.InitBlock()
	if err != nil {
		t.Fatalf("init block: %v", err)
	}
	if !got.Equals(&node.initBlock) {
		t.Fatalf("init block = %+v, want %+v", got, node.initBlock)
	}
}

func TestNextKeyBlocksRejectsNonMasterchainAnchor(t *testing.T) {
	node := newTestNode(t)

	if _, err := node.NextKeyBlocks(context.Background(), testBlockID(0, 0, 0), 8); err == nil {
		t.Fatal("expected non-masterchain anchor error")
	}
}

func TestInitBlockFromConfigFallsBackToZeroStateWhenMissing(t *testing.T) {
	zeroConfig := liteclient.ConfigBlock{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     0,
		RootHash:  bytes.Repeat([]byte{0x01}, 32),
		FileHash:  bytes.Repeat([]byte{0x02}, 32),
	}
	zero := blockIDFromConfig(zeroConfig)

	init, err := initBlockFromConfig(liteclient.ConfigBlock{}, zero, nil)
	if err != nil {
		t.Fatalf("init block from empty config: %v", err)
	}
	if !init.Equals(&zero) {
		t.Fatalf("init block = %+v, want zero %+v", init, zero)
	}
}

func TestInitBlockFromConfigUsesLatestHardfork(t *testing.T) {
	zero := testBlockID(-1, topShard, 0)
	init := testBlockID(-1, topShard, 100)
	hardfork90 := testBlockID(-1, topShard, 90)
	hardfork120 := testBlockID(-1, topShard, 120)

	got, err := initBlockFromConfig(configBlockFromID(init), zero, []ton.BlockIDExt{hardfork90, hardfork120})
	if err != nil {
		t.Fatalf("init block from config: %v", err)
	}
	if !got.Equals(&hardfork120) {
		t.Fatalf("init block = %+v, want latest hardfork %+v", got, hardfork120)
	}
}

func TestHardforksFromConfigInvalidatesOlderForkAtSameOrHigherSeqno(t *testing.T) {
	old := testBlockID(-1, topShard, 120)
	replacement := testBlockID(-1, topShard, 110)

	hardforks, set, err := hardforksFromConfig([]liteclient.ConfigBlock{
		configBlockFromID(old),
		configBlockFromID(replacement),
	})
	if err != nil {
		t.Fatalf("hardforks from config: %v", err)
	}
	if len(hardforks) != 1 || !hardforks[0].Equals(&replacement) {
		t.Fatalf("hardforks = %+v, want only replacement %+v", hardforks, replacement)
	}

	oldKey, _ := blockIDFullKeyFromBlock(old)
	if _, ok := set[oldKey]; ok {
		t.Fatal("old hardfork stayed active")
	}
	replacementKey, _ := blockIDFullKeyFromBlock(replacement)
	if _, ok := set[replacementKey]; !ok {
		t.Fatal("replacement hardfork is not active")
	}
}

func TestNodeIsHardforkRequiresExactFullBlockID(t *testing.T) {
	node := newTestNode(t)
	hardfork := testBlockID(-1, topShard, 120)
	key, _ := blockIDFullKeyFromBlock(hardfork)
	node.hardforkSet = map[blockIDFullKey]struct{}{key: struct{}{}}

	if !node.IsHardfork(hardfork) {
		t.Fatal("expected exact configured hardfork match")
	}

	other := hardfork
	other.FileHash = bytes.Repeat([]byte{0xff}, 32)
	if node.IsHardfork(other) {
		t.Fatal("hardfork lookup ignored file hash")
	}
}

func TestRememberSeenMasterchainKeepsNewestRuntimeHint(t *testing.T) {
	logger := discardLogger()
	store := newTestPebbleStore(t)
	node, err := New(Options{Logger: &logger, Storage: store, StateFilesDir: t.TempDir()})
	if err != nil {
		t.Fatalf("new node: %v", err)
	}

	block10 := testBlockID(-1, topShard, 10)
	block9 := testBlockID(-1, topShard, 9)

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
}

func configBlockFromID(block ton.BlockIDExt) liteclient.ConfigBlock {
	return liteclient.ConfigBlock{
		Workchain: block.Workchain,
		Shard:     block.Shard,
		SeqNo:     block.SeqNo,
		RootHash:  append([]byte(nil), block.RootHash...),
		FileHash:  append([]byte(nil), block.FileHash...),
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
		StateFilesDir:      t.TempDir(),
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

func testSubscriptionForOverlay(tb testing.TB, node *Node, workchain int32, shard int64) *overlaySubscription {
	tb.Helper()

	sub, ok := findSubscriptionForOverlay(tb, node, workchain, shard)
	if !ok {
		tb.Fatalf("missing overlay subscription for %d:%016x", workchain, uint64(shard))
	}
	return sub
}

func findSubscriptionForOverlay(tb testing.TB, node *Node, workchain int32, shard int64) (*overlaySubscription, bool) {
	tb.Helper()

	spec, err := buildOverlaySpec(node.zeroStateFileHash, workchain, shard, overlayName(workchain, shard))
	if err != nil {
		tb.Fatalf("build overlay spec: %v", err)
	}

	node.subscriptionsMx.RLock()
	sub := node.subscriptions[overlaySpecKey(spec)]
	node.subscriptionsMx.RUnlock()
	return sub, sub != nil
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
