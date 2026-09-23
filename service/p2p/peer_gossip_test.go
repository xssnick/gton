package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p/internal/fastsync"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

func newTestOverlayNode(id []byte, key ed25519.PrivateKey) (*overlay.NodeV2, error) {
	node, err := overlay.NewNode(id, key)
	if err != nil {
		return nil, err
	}
	return new(overlayNodeFromV1(*node)), nil
}

func TestPublicOverlayAnswersRandomPeersV2(t *testing.T) {
	sub, _ := testLearningSubscription(t)
	response, err := sub.dispatchPeerQuery(t.Context(), overlay.GetRandomPeersV2{})
	if err != nil {
		t.Fatal(err)
	}
	nodes, ok := response.(overlay.NodesV2)
	if !ok || len(nodes.Nodes) != 1 {
		t.Fatalf("response = %#v, want local NodeV2", response)
	}
	if err := nodes.Nodes[0].CheckSignature(); err != nil {
		t.Fatalf("local signature: %v", err)
	}
	wire, err := tl.Serialize(response, true)
	if err != nil {
		t.Fatal(err)
	}
	var decoded overlay.NodesV2
	if _, err := tl.Parse(&decoded, wire, true); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Nodes[0].CheckSignature(); err != nil {
		t.Fatalf("wire signature: %v", err)
	}
}

func TestFixedOverlaysRejectBothRandomPeerVersions(t *testing.T) {
	for _, kind := range []overlayKind{overlayKindPrivate, overlayKindCustomFixed} {
		for _, request := range []any{overlay.GetRandomPeers{}, overlay.GetRandomPeersV2{}} {
			sub := testOverlaySubscription(&overlaySubscription{
				spec: overlaySpec{Kind: kind, AcceptQueries: true},
			})
			response, err := sub.dispatchPeerQuery(t.Context(), request)
			if err == nil || err.Error() != "overlay is private" || response != nil {
				t.Fatalf("kind=%v request=%T: (%T, %v), want private rejection", kind, request, response, err)
			}
		}
	}
}

func TestPublicRandomPeersV2PreservesSignedDescriptor(t *testing.T) {
	sub, spec := testLearningSubscription(t)
	// Keep the source-learning path synchronous; this test needs no DHT dial.
	sub.advertisedPeerLearning.Store(true)
	key := peerRuntimeTestKey(0x81)
	node, err := newTestOverlayNode(spec.FullID, key)
	if err != nil {
		t.Fatal(err)
	}
	node.Flags = overlayMemberDoNotReceiveBroadcasts
	node.Certificate = overlay.MemberCertificate{
		IssuedBy:  keys.PublicKeyED25519{Key: bytes.Repeat([]byte{0x82}, ed25519.PublicKeySize)},
		Signature: bytes.Repeat([]byte{0x83}, ed25519.SignatureSize),
	}
	if err := node.Sign(key); err != nil {
		t.Fatal(err)
	}
	id := peerRuntimeTestPeerID(key.Public().(ed25519.PublicKey))
	peer := &overlayPeer{id: id, announced: &overlay.NodeV2{Version: node.Version - 1}}
	sub.peers[id] = peer
	response, err := sub.dispatchPeerQueryFrom(t.Context(), id, "10.1.1.1:30303", overlay.GetRandomPeersV2{
		Peers: overlay.NodesV2{Nodes: []overlay.NodeV2{*node}},
	})
	if err != nil {
		t.Fatal(err)
	}
	entry := sub.directory[id]
	if entry == nil || !entry.verified || entry.adnlAddr != "10.1.1.1:30303" {
		t.Fatalf("sender was not learned: %+v", entry)
	}
	if sub.PlumtreePeerReceivesBroadcasts(id) {
		t.Fatal("live sender's signed flags were not refreshed")
	}
	nodes := response.(overlay.NodesV2).Nodes
	if len(nodes) != 2 || nodes[1].Flags != node.Flags {
		t.Fatalf("V2 response lost signed flags: %+v", nodes)
	}
	wire, err := tl.Serialize(response, true)
	if err != nil {
		t.Fatal(err)
	}
	var decoded overlay.NodesV2
	if _, err := tl.Parse(&decoded, wire, true); err != nil {
		t.Fatal(err)
	}
	if err := decoded.Nodes[1].CheckSignature(); err != nil {
		t.Fatalf("relayed signature: %v", err)
	}
	legacy, err := sub.dispatchPeerQuery(t.Context(), overlay.GetRandomPeers{})
	if err != nil {
		t.Fatal(err)
	}
	if len(legacy.(overlay.NodesList).List) != 1 {
		t.Fatal("relayed a V2-only signature in a V1 response")
	}
	node.Signature[0] ^= 1
	node.Certificate.(overlay.MemberCertificate).Signature[0] ^= 1
	node.Certificate.(overlay.MemberCertificate).IssuedBy.(keys.PublicKeyED25519).Key[0] ^= 1
	if err := entry.announced.CheckSignature(); err != nil {
		t.Fatalf("directory retained borrowed signature: %v", err)
	}
	cert := entry.announced.Certificate.(overlay.MemberCertificate)
	if cert.Signature[0] != 0x83 || cert.IssuedBy.(keys.PublicKeyED25519).Key[0] != 0x82 {
		t.Fatal("directory retained borrowed member certificate")
	}
}

func TestPublicRandomPeersV2RejectsInvalidDescriptors(t *testing.T) {
	for _, name := range []string{"signature", "flags", "overlay", "stale", "future", "beyond intake limit"} {
		t.Run(name, func(t *testing.T) {
			sub, spec := testLearningSubscription(t)
			sub.advertisedPeerLearning.Store(true)
			key := peerRuntimeTestKey(0x91)
			node, err := newTestOverlayNode(spec.FullID, key)
			if err != nil {
				t.Fatal(err)
			}
			switch name {
			case "signature":
				node.Signature[0] ^= 1
			case "flags":
				node.Flags = 1 // The signature must cover these flags.
			case "overlay":
				node.Overlay[0] ^= 1
			case "stale":
				node.Version = int32(time.Now().Add(-2 * overlayPeerTTL).Unix())
			case "future":
				node.Version = int32(time.Now().Add(2 * overlayFutureSkew).Unix())
			}
			if name == "overlay" || name == "stale" || name == "future" {
				if err := node.Sign(key); err != nil {
					t.Fatal(err)
				}
			}
			nodes := []overlay.NodeV2{*node}
			if name == "beyond intake limit" {
				nodes = append(make([]overlay.NodeV2, maxAdvertisedPeersPerQuery), *node)
			}
			_, err = sub.dispatchPeerQueryFrom(t.Context(), peerRuntimeTestPeerID(key.Public().(ed25519.PublicKey)), "10.1.1.1:30303", overlay.GetRandomPeersV2{Peers: overlay.NodesV2{Nodes: nodes}})
			if err != nil {
				t.Fatal(err)
			}
			if len(sub.directory) != 0 {
				t.Fatal("invalid or excess descriptor entered the directory")
			}
		})
	}
}

func TestFastSyncAnswersBothRandomPeerVersions(t *testing.T) {
	sub, spec := testLearningSubscription(t)
	sub.spec.Kind = overlayKindFastSync
	sub.spec.AcceptQueries = true
	sub.advertisedPeerLearning.Store(true)
	now := time.Now()
	localID := peerRuntimeTestPeerID(sub.node.privKey.Public().(ed25519.PublicKey))
	remoteKey := peerRuntimeTestKey(0xa1)
	remoteID := peerRuntimeTestPeerID(remoteKey.Public().(ed25519.PublicKey))
	roster := peerRuntimeTestRoster(sub.node.privKey.Public().(ed25519.PublicKey), FastSyncValidator{ADNLID: localID}, FastSyncValidator{ADNLID: remoteID})
	membership := newFastSyncTestMembership(roster, 1)
	peers, err := fastsync.NewPeerRuntime(sub.node.privKey, FastSyncOverlayShortID(spec.ShortID), uint32(1), overlay.EmptyMemberCertificate{}, membership, now)
	if err != nil {
		t.Fatal(err)
	}
	sub.fastSync = &fastSyncOverlayRuntime{membership: membership, peers: peers}
	remote, err := overlay.NewNode(spec.FullID, remoteKey)
	if err != nil {
		t.Fatal(err)
	}
	response, err := sub.dispatchPeerQuery(t.Context(), overlay.GetRandomPeers{List: overlay.NodesList{List: []overlay.Node{*remote}}})
	if err != nil {
		t.Fatal(err)
	}
	legacy := response.(overlay.NodesList)
	if len(legacy.List) == 0 || !bytes.Equal(legacy.List[0].Overlay, spec.ShortID) {
		t.Fatal("FastSync V1 response has no local descriptor")
	}
	if err := legacy.List[0].CheckSignature(); err != nil {
		t.Fatalf("FastSync V1 local signature: %v", err)
	}
	if peers.Counts().Known != 1 || len(sub.directory) != 0 {
		t.Fatalf("V1 peers bypassed FastSync membership: %+v", peers.Counts())
	}
	// Four is the reply size, not a protocol limit on incoming requests.
	response, err = sub.dispatchPeerQuery(t.Context(), overlay.GetRandomPeersV2{
		Peers: overlay.NodesV2{Nodes: make([]overlay.NodeV2, 5)},
	})
	if err != nil {
		t.Fatal(err)
	}
	nodes := response.(overlay.NodesV2).Nodes
	if len(nodes) != 1 || nodes[0].Flags != 1 {
		t.Fatal("FastSync V2 response lost local flags")
	}
	if err := nodes[0].CheckSignature(); err != nil {
		t.Fatal(err)
	}
}

func TestPrivateRawOverlayControlQueriesBypassApplication(t *testing.T) {
	node := newPrivateOverlayTestNode(t)
	handle, err := node.PrivateOverlays().Open(PrivateOverlayConfig{
		FullID:  []byte("private overlay query control"),
		Members: []PeerID{node.localID},
	}, PrivateOverlayCallbacks{
		Query: func(context.Context, PeerID, tl.Serializable) (tl.Serializable, error) {
			t.Fatal("overlay control query reached the application")
			return nil, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	defer handle.Close()
	for _, query := range []any{overlay.Ping{}, overlay.GetRandomPeers{}, overlay.GetRandomPeersV2{}, RepairPlumtreePart{BroadcastID: make([]byte, 32)}} {
		wire, err := tl.Serialize(query, true)
		if err != nil {
			t.Fatal(err)
		}
		response, err := handle.sub.dispatchPeerQuery(t.Context(), tl.Raw(wire))
		switch query.(type) {
		case overlay.Ping:
			if _, ok := response.(overlay.Pong); err != nil || !ok {
				t.Fatalf("ping: %T, %v", response, err)
			}
		case RepairPlumtreePart:
			if !errors.Is(err, errPlumtreeDisabled) {
				t.Fatalf("repair: %v", err)
			}
		default:
			if err == nil || err.Error() != "overlay is private" {
				t.Fatalf("%T: %v", query, err)
			}
		}
	}
}

func TestPeerQueryDispatchServesPlumtreeRepair(t *testing.T) {
	node := newTestNode(t)
	spec, err := buildOverlaySpec(make([]byte, 32), 0, topShard, "basechain")
	if err != nil {
		t.Fatal(err)
	}
	sub, err := node.newOverlaySubscription(spec)
	if err != nil {
		t.Fatal(err)
	}
	defer sub.close()
	id := plumtreeEngineTestID(1)
	sub.plumtree.engine.delivered.add(id)
	query := RepairPlumtreePart{
		BroadcastID: id[:],
		Timestamp:   plumtreeUnixSeconds(time.Now()),
		PartIndex:   0,
		TreeIndex:   plumtreeSimpleTree,
	}
	wire, err := tl.Serialize(query, true)
	if err != nil {
		t.Fatal(err)
	}
	// Overlay repair remains available even when a custom overlay does not
	// accept application queries.
	for _, kind := range []overlayKind{overlayKindPublicShard, overlayKindCustomFixed} {
		sub.spec.Kind = kind
		sub.spec.AcceptQueries = false
		for _, request := range []any{query, tl.Raw(wire)} {
			response, err := sub.dispatchPeerQueryFrom(t.Context(), testPeerID("repair requester"), "", request)
			if err != nil {
				t.Fatal(err)
			}
			answer, err := tl.Serialize(response, true)
			if err != nil {
				t.Fatal(err)
			}
			var decoded BroadcastNotFound
			if _, err := tl.Parse(&decoded, answer, true); err != nil {
				t.Fatalf("repair answer was not broadcastNotFound: %v", err)
			}
		}
	}
}

func TestPublicV2FlagsExcludeBroadcastReceiver(t *testing.T) {
	id := testPeerID("public nonreceiver")
	peer := &overlayPeer{
		id:          id,
		fixedMember: true,
		alive:       true,
		route:       newTestPeerRoute("127.0.0.1:3000"),
		announced:   &overlay.NodeV2{Flags: overlayMemberDoNotReceiveBroadcasts},
	}
	sub := testOverlaySubscription(&overlaySubscription{peers: map[PeerID]*overlayPeer{id: peer}})
	if len(sub.buildBroadcastTargetsSnapshot().peers) != 0 || sub.PlumtreePeerReceivesBroadcasts(id) {
		t.Fatal("public V2 peer that declined broadcasts was selected")
	}
	result := []bool{true}
	sub.PlumtreePeersReceiveBroadcasts([]PeerID{id}, result)
	if result[0] {
		t.Fatal("batch Plumtree check ignored public V2 flags")
	}
	peer.mergeAnnouncement(&overlay.NodeV2{Version: 1})
	if len(sub.buildBroadcastTargetsSnapshot().peers) != 1 || !sub.PlumtreePeerReceivesBroadcasts(id) {
		t.Fatal("new signed descriptor did not restore broadcast eligibility")
	}
}
