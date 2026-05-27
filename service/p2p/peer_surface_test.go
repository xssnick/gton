package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestDispatchPeerQueryCapabilitiesAndStubs(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: newTestPeerStore(),
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:              "masterchain",
			ProtoVersionMajor: masterchainProtoVersionMajor,
			ProtoVersionMinor: masterchainProtoVersionMinor,
		},
		log: discardLogger(),
	}

	resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetCapabilities{})
	if err != nil {
		t.Fatalf("getCapabilities: %v", err)
	}

	caps, ok := resp.(Capabilities)
	if !ok {
		t.Fatalf("unexpected capabilities response %T", resp)
	}
	if caps.VersionMajor != masterchainProtoVersionMajor || caps.VersionMinor != masterchainProtoVersionMinor {
		t.Fatalf("unexpected capabilities version %d.%d", caps.VersionMajor, caps.VersionMinor)
	}

	block := ton.BlockIDExt{
		Workchain: -1,
		Shard:     topShard,
		SeqNo:     1,
		RootHash:  make([]byte, 32),
		FileHash:  make([]byte, 32),
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, tonnodeapi.DownloadBlockFull{Block: block})
	if err != nil {
		t.Fatalf("downloadBlockFull stub: %v", err)
	}
	if _, ok = resp.(tonnodeapi.DataFullEmpty); !ok {
		t.Fatalf("unexpected downloadBlockFull response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetArchiveInfo{MasterchainSeqno: 1})
	if err != nil {
		t.Fatalf("getArchiveInfo stub: %v", err)
	}
	if _, ok = resp.(ArchiveNotFound); !ok {
		t.Fatalf("unexpected getArchiveInfo response %T", resp)
	}

	resp, err = sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, GetOutMsgQueueProof{
		DstShard: archive.ShardID{Workchain: 0, Shard: topShard},
		Limits:   ImportedMsgQueueLimits{MaxBytes: 1 << 20, MaxMsgs: 128},
	})
	if err == nil || !strings.Contains(err.Error(), "not supported yet") {
		t.Fatalf("getOutMsgQueueProof error = %v, want not supported yet", err)
	}
	if resp != nil {
		t.Fatalf("unexpected getOutMsgQueueProof response %T", resp)
	}
}

func TestDispatchPeerQuerySendExtMessageEnqueuesBroadcast(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: newTestPeerStore(),
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	runCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	node.runCtx = runCtx
	node.zeroStateFileHash = make([]byte, 32)

	spec, err := buildOverlaySpec(node.zeroStateFileHash, 0, topShard, overlayName(0, topShard))
	if err != nil {
		t.Fatalf("build overlay spec: %v", err)
	}
	sub, _ := node.getOrCreateSubscription(spec)
	peer := testRebroadcastQueuePeer("peer")
	sub.mx.Lock()
	sub.peers[peer.id] = peer
	sub.mx.Unlock()

	data := testExternalMessageBOC(t)

	resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, SendExtMessage{
		Message: tonnodeapi.ExternalMessage{Data: data},
	})
	if err != nil {
		t.Fatalf("sendExtMessage: %v", err)
	}
	if _, ok := resp.(Success); !ok {
		t.Fatalf("unexpected sendExtMessage response %T", resp)
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	req, ok := peer.localRebroadcastQueue.Pop(ctx)
	if !ok {
		t.Fatal("expected external message rebroadcast request")
	}
	if req.kind != "tonNode.externalMessageBroadcast" {
		t.Fatalf("unexpected rebroadcast kind %q", req.kind)
	}
	if req.subscription.spec.Name != "basechain" {
		t.Fatalf("unexpected rebroadcast subscription %q", req.subscription.spec.Name)
	}

	var parsed any
	if _, err = tl.Parse(&parsed, req.payload, true); err != nil {
		t.Fatalf("parse broadcast payload: %v", err)
	}
	broadcast, ok := parsed.(tonnodeapi.NewExternalMessageBroadcast)
	if !ok {
		t.Fatalf("unexpected broadcast payload type %T", parsed)
	}
	if string(broadcast.Message.Data) != string(data) {
		t.Fatalf("unexpected external message data %x", broadcast.Message.Data)
	}
}

func TestLocalExternalRebroadcastQueueHasPriority(t *testing.T) {
	peer := testRebroadcastQueuePeer("peer")

	if !peer.rebroadcastQueue.Push(rebroadcastRequest{
		kind:    "tonNode.newShardBlockBroadcast",
		payload: []byte{0x01},
	}) {
		t.Fatal("enqueue ordinary rebroadcast")
	}
	if !peer.localRebroadcastQueue.Push(rebroadcastRequest{
		kind:    "tonNode.externalMessageBroadcast",
		payload: []byte{0x02},
		local:   true,
	}) {
		t.Fatal("enqueue local external rebroadcast")
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	first, ok := popPriority(ctx, peer.localRebroadcastQueue, peer.rebroadcastQueue)
	if !ok {
		t.Fatal("expected priority rebroadcast")
	}
	if first.kind != "tonNode.externalMessageBroadcast" {
		t.Fatalf("first rebroadcast kind = %q, want local external", first.kind)
	}

	second, ok := popPriority(ctx, peer.localRebroadcastQueue, peer.rebroadcastQueue)
	if !ok {
		t.Fatal("expected ordinary rebroadcast")
	}
	if second.kind != "tonNode.newShardBlockBroadcast" {
		t.Fatalf("second rebroadcast kind = %q, want ordinary broadcast", second.kind)
	}
}

func TestClassifyNewShardBlockBroadcastCarriesDescription(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{Name: "basechain"},
		log:  discardLogger(),
	}

	block := testBlockID(0, topShard, 42)
	data := []byte{0xAA, 0xBB, 0xCC}
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      block,
			CCSeqno: 7,
			Data:    data,
		},
	}

	accepted := sub.classifyBroadcast(nil, msg, []byte{0x01}, DeliverySimple, true, "peer")
	if accepted == nil || accepted.event == nil {
		t.Fatal("expected shard block broadcast event")
	}
	if accepted.event.ShardDescription == nil {
		t.Fatal("expected shard description payload")
	}
	if accepted.event.ShardDescription.CatchainSeqno != 7 {
		t.Fatalf("catchain seqno = %d, want 7", accepted.event.ShardDescription.CatchainSeqno)
	}
	if string(accepted.event.ShardDescription.Data) != string(data) {
		t.Fatalf("description data = %x, want %x", accepted.event.ShardDescription.Data, data)
	}

	data[0] = 0x11
	if accepted.event.ShardDescription.Data[0] != 0xAA {
		t.Fatal("description data was not cloned")
	}
}

func TestDispatchPeerQuerySendExtMessageRejectsInvalidMessage(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: newTestPeerStore(),
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{Name: "basechain"},
		log:  discardLogger(),
	}

	resp, err := sub.dispatchPeerQuery(context.Background(), &overlayPeer{addr: "peer"}, SendExtMessage{
		Message: tonnodeapi.ExternalMessage{Data: []byte{0xAA, 0xBB}},
	})
	if err == nil {
		t.Fatal("expected invalid external message error")
	}
	if resp != nil {
		t.Fatalf("unexpected sendExtMessage response %T", resp)
	}
}

func testExternalMessageBOC(t *testing.T) []byte {
	t.Helper()

	root, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr:   address.MustParseRawAddr("0:1111111111111111111111111111111111111111111111111111111111111111"),
		ImportFee: tlb.ZeroCoins,
		Body:      cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}
	return root.ToBOCWithFlags(false)
}

func TestHandleGetRandomPeersIncludesSelfAndKnownPeers(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		ListenAddr:         "127.0.0.1:30303",
		PeerServingStorage: newTestPeerStore(),
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	zeroHash := make([]byte, 32)
	spec, err := buildOverlaySpec(zeroHash, -1, topShard, "masterchain")
	if err != nil {
		t.Fatalf("build overlay spec: %v", err)
	}

	sub := &overlaySubscription{
		node:  node,
		spec:  spec,
		log:   discardLogger(),
		peers: map[string]*overlayPeer{},
	}

	_, foreignPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate foreign key: %v", err)
	}
	foreignNode, err := overlay.NewNode(spec.FullID, foreignPriv)
	if err != nil {
		t.Fatalf("build foreign overlay node: %v", err)
	}
	sub.peers["foreign"] = &overlayPeer{announced: foreignNode, alive: true, lastReceiveAt: time.Now()}

	res := sub.handleGetRandomPeers(context.Background(), overlay.GetRandomPeers{})
	if len(res.List) < 2 {
		t.Fatalf("expected self and known peer, got %d entries", len(res.List))
	}

	self, err := node.selfOverlayNode(spec)
	if err != nil {
		t.Fatalf("build self overlay node: %v", err)
	}
	selfID, err := tl.Hash(self.ID)
	if err != nil {
		t.Fatalf("hash self overlay node id: %v", err)
	}
	foreignID, err := tl.Hash(foreignNode.ID)
	if err != nil {
		t.Fatalf("hash foreign overlay node id: %v", err)
	}

	var hasSelf, hasForeign bool
	for _, node := range res.List {
		id, err := tl.Hash(node.ID)
		if err != nil {
			t.Fatalf("hash reply node id: %v", err)
		}
		switch fmt.Sprintf("%x", id) {
		case fmt.Sprintf("%x", selfID):
			hasSelf = true
		case fmt.Sprintf("%x", foreignID):
			hasForeign = true
		}
	}

	if !hasSelf || !hasForeign {
		t.Fatalf("missing nodes in getRandomPeers response: self=%v foreign=%v", hasSelf, hasForeign)
	}
}

func TestGetRandomPeersCapsAdvertisementLikeCppOverlay(t *testing.T) {
	node := newTestNode(t)

	zeroHash := make([]byte, 32)
	spec, err := buildOverlaySpec(zeroHash, -1, topShard, "masterchain")
	if err != nil {
		t.Fatalf("build overlay spec: %v", err)
	}

	sub := &overlaySubscription{
		node:  node,
		spec:  spec,
		log:   discardLogger(),
		peers: map[string]*overlayPeer{},
	}

	now := time.Now()
	for i := 0; i < maxRandomPeerReply*3; i++ {
		_, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("generate peer key: %v", err)
		}
		announced, err := overlay.NewNode(spec.FullID, priv)
		if err != nil {
			t.Fatalf("build overlay node: %v", err)
		}
		sub.peers[fmt.Sprintf("peer-%d", i)] = &overlayPeer{
			announced:     announced,
			alive:         true,
			lastReceiveAt: now,
		}
	}

	res := sub.handleGetRandomPeers(context.Background(), overlay.GetRandomPeers{})
	if len(res.List) != maxRandomPeerReply {
		t.Fatalf("getRandomPeers returned %d nodes, want %d", len(res.List), maxRandomPeerReply)
	}

	self, err := node.selfOverlayNode(spec)
	if err != nil {
		t.Fatalf("build self overlay node: %v", err)
	}
	selfID, err := tl.Hash(self.ID)
	if err != nil {
		t.Fatalf("hash self overlay node id: %v", err)
	}
	firstID, err := tl.Hash(res.List[0].ID)
	if err != nil {
		t.Fatalf("hash first reply node id: %v", err)
	}
	if fmt.Sprintf("%x", firstID) != fmt.Sprintf("%x", selfID) {
		t.Fatal("first getRandomPeers advertisement should be self")
	}
}

func TestConnectOverlayNodeSkipsSelf(t *testing.T) {
	node := newTestNode(t)

	zeroHash := make([]byte, 32)
	spec, err := buildOverlaySpec(zeroHash, -1, topShard, "masterchain")
	if err != nil {
		t.Fatalf("build overlay spec: %v", err)
	}

	sub := &overlaySubscription{
		node:  node,
		spec:  spec,
		log:   discardLogger(),
		peers: map[string]*overlayPeer{},
	}

	self, err := node.selfOverlayNode(spec)
	if err != nil {
		t.Fatalf("build self overlay node: %v", err)
	}

	attached, err := sub.connectOverlayNodeV1(context.Background(), *self)
	if err != nil {
		t.Fatalf("connect self overlay node: %v", err)
	}
	if attached {
		t.Fatal("self overlay node should not be attached")
	}
	if len(sub.peers) != 0 {
		t.Fatalf("self overlay node created peers: %#v", sub.peers)
	}
}

func TestHandleGetRandomPeersIncludesSelfForClientNode(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: newTestPeerStore(),
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	zeroHash := make([]byte, 32)
	spec, err := buildOverlaySpec(zeroHash, -1, topShard, "masterchain")
	if err != nil {
		t.Fatalf("build overlay spec: %v", err)
	}

	sub := &overlaySubscription{
		node:  node,
		spec:  spec,
		log:   discardLogger(),
		peers: map[string]*overlayPeer{},
	}

	_, foreignPriv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate foreign key: %v", err)
	}
	foreignNode, err := overlay.NewNode(spec.FullID, foreignPriv)
	if err != nil {
		t.Fatalf("build foreign overlay node: %v", err)
	}
	sub.peers["foreign"] = &overlayPeer{announced: foreignNode, alive: true, lastReceiveAt: time.Now()}

	res := sub.handleGetRandomPeers(context.Background(), overlay.GetRandomPeers{})
	if len(res.List) < 2 {
		t.Fatalf("expected self and known peer for client node, got %d entries", len(res.List))
	}

	self, err := node.selfOverlayNode(spec)
	if err != nil {
		t.Fatalf("build self overlay node: %v", err)
	}
	selfID, err := tl.Hash(self.ID)
	if err != nil {
		t.Fatalf("hash self overlay node id: %v", err)
	}
	foreignID, err := tl.Hash(foreignNode.ID)
	if err != nil {
		t.Fatalf("hash foreign overlay node id: %v", err)
	}

	var hasSelf, hasForeign bool
	for _, node := range res.List {
		id, err := tl.Hash(node.ID)
		if err != nil {
			t.Fatalf("hash reply node id: %v", err)
		}
		switch fmt.Sprintf("%x", id) {
		case fmt.Sprintf("%x", selfID):
			hasSelf = true
		case fmt.Sprintf("%x", foreignID):
			hasForeign = true
		}
	}

	if !hasSelf || !hasForeign {
		t.Fatalf("missing nodes in client getRandomPeers response: self=%v foreign=%v", hasSelf, hasForeign)
	}
}

func TestBuildOverlaySpecUsesCppNodeProtoVersions(t *testing.T) {
	zeroHash := make([]byte, 32)

	master, err := buildOverlaySpec(zeroHash, -1, topShard, "masterchain")
	if err != nil {
		t.Fatalf("build master overlay spec: %v", err)
	}
	if master.ProtoVersionMajor != masterchainProtoVersionMajor || master.ProtoVersionMinor != masterchainProtoVersionMinor {
		t.Fatalf("unexpected master proto version %d.%d", master.ProtoVersionMajor, master.ProtoVersionMinor)
	}

	base, err := buildOverlaySpec(zeroHash, 0, topShard, "basechain")
	if err != nil {
		t.Fatalf("build base overlay spec: %v", err)
	}
	if base.ProtoVersionMajor != shardchainProtoVersionMajor || base.ProtoVersionMinor != shardchainProtoVersionMinor {
		t.Fatalf("unexpected base proto version %d.%d", base.ProtoVersionMajor, base.ProtoVersionMinor)
	}
}

func TestAcceptBroadcastDropsWhenQueueIsFull(t *testing.T) {
	node := newTestNode(t)

	total := broadcastQueueMaxItems + 10
	for i := 0; i < total; i++ {
		node.acceptBroadcast(acceptedBroadcast{
			fingerprint: fmt.Sprintf("fp-%d", i),
			event: &BroadcastEvent{
				Overlay: "basechain",
				Kind:    "tonNode.blockBroadcast",
				Block: ton.BlockIDExt{
					Workchain: 0,
					Shard:     topShard,
					SeqNo:     uint32(i + 1),
					RootHash:  make([]byte, 32),
					FileHash:  make([]byte, 32),
				},
			},
		})
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	count := 0
	for count < broadcastQueueMaxItems {
		_, ok := node.eventQueue.Pop(ctx)
		if !ok {
			t.Fatalf("queue closed early after %d items", count)
		}
		count++
	}

	if count != broadcastQueueMaxItems {
		t.Fatalf("unexpected number of queued broadcasts: got %d want %d", count, broadcastQueueMaxItems)
	}

	shortCtx, shortCancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer shortCancel()
	if _, ok := node.eventQueue.Pop(shortCtx); ok {
		t.Fatal("expected broadcasts over queue limit to be dropped")
	}
}

func TestEventDeduperHasHardCap(t *testing.T) {
	deduper := newEventDeduper(time.Hour, 16)
	now := time.Now()

	for i := 0; i < 64; i++ {
		if !deduper.Mark(fmt.Sprintf("fp-%d", i), now) {
			t.Fatalf("unexpected duplicate for key %d", i)
		}
	}
	if len(deduper.seen) > 16 {
		t.Fatalf("deduper exceeded hard cap: got %d want <= 16", len(deduper.seen))
	}
}

func TestAcceptBroadcastDoesNotCacheShardBlockInPeerLayer(t *testing.T) {
	store := newTestPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: store,
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	node.blockCacheSlots = nil

	block := testBlockID(0, topShard, 42)
	downloaded := &DownloadedBlock{
		ID:               block,
		BlockBOC:         []byte{0xAA, 0xBB},
		ProofBOC:         []byte{0xCC},
		VerifiedFileHash: true,
	}
	node.acceptBroadcast(acceptedBroadcast{
		fingerprint: "shard-full-block",
		event: &BroadcastEvent{
			Overlay:    "basechain",
			Kind:       "tonNode.blockBroadcast",
			Block:      block,
			Downloaded: downloaded,
		},
	})

	if _, err := store.BlockFull(context.Background(), block); !errors.Is(err, tnstore.ErrNotFound) {
		t.Fatalf("cached shard block error = %v, want ErrNotFound", err)
	}
}

func TestAcceptBroadcastDoesNotCacheUnverifiedMasterchainBlock(t *testing.T) {
	store := newTestPeerStore()
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: store,
		StateFilesDir:      t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	node.blockCacheSlots = nil

	block := testBlockID(-1, topShard, 42)
	node.acceptBroadcast(acceptedBroadcast{
		fingerprint: "master-full-block",
		event: &BroadcastEvent{
			Overlay: "masterchain",
			Kind:    "tonNode.blockBroadcast",
			Block:   block,
			Downloaded: &DownloadedBlock{
				ID:               block,
				BlockBOC:         []byte{0xAA, 0xBB},
				ProofBOC:         []byte{0xCC},
				VerifiedFileHash: true,
			},
		},
	})

	if _, err := store.BlockFull(context.Background(), block); err == nil {
		t.Fatal("unverified masterchain broadcast was cached")
	}
}

func TestKnownPeerCountIgnoresInboundOnlyPeers(t *testing.T) {
	sub := &overlaySubscription{
		log: discardLogger(),
		peers: map[string]*overlayPeer{
			"inbound-only": {},
			"known-v1": {
				announced:     &overlay.Node{Version: int32(time.Now().Unix())},
				alive:         true,
				lastReceiveAt: time.Now(),
			},
		},
	}

	if got := len(sub.peers); got != 2 {
		t.Fatalf("unexpected total peer count: got %d want 2", got)
	}
	if got := countKnownPeers(sub.peers); got != 1 {
		t.Fatalf("unexpected known peer count: got %d want 1", got)
	}
	if got := sub.aliveKnownPeerCount(); got != 1 {
		t.Fatalf("unexpected alive known peer count: got %d want 1", got)
	}
}

func countKnownPeers(peers map[string]*overlayPeer) int {
	count := 0
	now := time.Now()
	for _, peer := range peers {
		if peer == nil || !peer.isKnownOverlayPeer(now) {
			continue
		}
		count++
	}
	return count
}

func testRebroadcastQueuePeer(id string) *overlayPeer {
	peer := &overlayPeer{
		id:      id,
		addr:    id,
		overlay: &overlay.ADNLOverlayWrapper{},
		announced: &overlay.Node{
			Version: int32(time.Now().Unix()),
		},
		alive:         true,
		lastReceiveAt: time.Now(),
	}
	peer.initRebroadcastQueues()
	return peer
}
