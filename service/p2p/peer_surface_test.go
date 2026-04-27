package p2p

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"fmt"
	"testing"
	"time"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func TestDispatchPeerQueryCapabilitiesAndStubs(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{Logger: &logger})
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
}

func TestHandleGetRandomPeersIncludesSelfAndKnownPeers(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{
		Logger:     &logger,
		ListenAddr: "127.0.0.1:30303",
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

func TestHandleGetRandomPeersIncludesSelfForClientNode(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{Logger: &logger})
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

func TestAcceptBroadcastUsesUnboundedBacklog(t *testing.T) {
	node := newTestNode(t)

	const total = 5000
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
	for count < total {
		_, ok := node.eventQueue.Pop(ctx)
		if !ok {
			t.Fatalf("queue closed early after %d items", count)
		}
		count++
	}

	if count != total {
		t.Fatalf("unexpected number of queued broadcasts: got %d want %d", count, total)
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
