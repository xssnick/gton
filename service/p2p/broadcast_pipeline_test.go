package p2p

import (
	"bytes"
	"testing"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

func TestClassifyDuplicateCompressedBlockBroadcastDedupesBeforeDecode(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "masterchain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}

	tests := []struct {
		name  string
		block ton.BlockIDExt
		msg   any
	}{
		{
			name:  "compressed",
			block: testBlockID(-1, topShard, 200),
			msg: tonnodeapi.BlockBroadcastCompressed{
				ID:         testBlockID(-1, topShard, 200),
				Compressed: []byte{0x01},
			},
		},
		{
			name:  "compressed-v2",
			block: testBlockID(-1, topShard, 201),
			msg: tonnodeapi.BlockBroadcastCompressedV2{
				ID: testBlockID(-1, topShard, 201),
				SignatureSet: tonnodeapi.SignatureSetOrdinary{
					Signatures: []tonnodeapi.BlockSignature{},
				},
				Proof:          []byte{0x02},
				DataCompressed: []byte{0x03},
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			payload, err := tl.Serialize(tt.msg, true)
			if err != nil {
				t.Fatalf("serialize broadcast: %v", err)
			}

			first := sub.classifyBroadcast(nil, tt.msg, payload, DeliverySimple, false, "peer")
			if first == nil || first.masterchainWake == nil {
				t.Fatal("first invalid masterchain broadcast should still wake the masterchain tracker")
			}
			if !first.deduped {
				t.Fatal("first full block broadcast should be fingerprint-deduped before decode")
			}
			if _, ready := node.MasterchainBroadcastAfter(tt.block.SeqNo - 1); !ready {
				t.Fatal("raw masterchain broadcast should wake before payload decode succeeds")
			}

			second := sub.classifyBroadcast(nil, tt.msg, payload, DeliverySimple, false, "peer")
			if second != nil {
				t.Fatalf("duplicate broadcast was accepted after fingerprint dedupe: %+v", second)
			}
		})
	}
}

func TestClassifyBroadcastUsesPeerAsFECSourceKey(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}
	peer := &overlayPeer{id: "peer-a", addr: "peer-a"}
	block := testBlockID(0, topShard, 202)
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      block,
			CCSeqno: 7,
			Data:    []byte{0x01},
		},
	}

	accepted := sub.classifyBroadcast(peer, msg, []byte{0x01}, DeliveryFEC, false, "")
	if accepted == nil || accepted.event == nil {
		t.Fatal("expected shard block broadcast event")
	}
	if accepted.event.SourceKey != "peer-a" {
		t.Fatalf("source key = %q, want peer-a", accepted.event.SourceKey)
	}
}

func TestAcceptedShardBlockBroadcastEnqueuesRebroadcast(t *testing.T) {
	node := newTestNode(t)
	source := testRebroadcastQueuePeer("source")
	target := testRebroadcastQueuePeer("target")
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
		peers: map[string]*overlayPeer{
			source.id: source,
			target.id: target,
		},
	}
	block := testBlockID(0, topShard, 203)
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      block,
			CCSeqno: 7,
			Data:    []byte{0x01, 0x02},
		},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize shard broadcast: %v", err)
	}

	accepted := sub.classifyBroadcast(source, msg, payload, DeliveryFEC, false, "")
	if accepted == nil {
		t.Fatal("expected shard block broadcast to be accepted")
	}
	node.acceptBroadcast(*accepted)

	if _, ok := source.rebroadcastQueue.TryPop(); ok {
		t.Fatal("source peer should not receive its own shard block rebroadcast")
	}
	got, ok := target.rebroadcastQueue.TryPop()
	if !ok {
		t.Fatal("expected target peer shard block rebroadcast")
	}
	if got.kind != "tonNode.newShardBlockBroadcast" {
		t.Fatalf("rebroadcast kind = %q, want tonNode.newShardBlockBroadcast", got.kind)
	}
	if got.sourcePeerID != source.id {
		t.Fatalf("source peer id = %q, want %q", got.sourcePeerID, source.id)
	}
	if !bytes.Equal(got.payload, payload) {
		t.Fatal("rebroadcast payload changed")
	}
	if got := testBroadcastStatCount(node, "accepted", "basechain", "tonNode.newShardBlockBroadcast"); got != 1 {
		t.Fatalf("accepted broadcast count = %d, want 1", got)
	}
}

func testBroadcastStatCount(node *Node, direction, overlay, kind string) uint64 {
	for _, stat := range node.broadcastStatusSnapshot() {
		if stat.Direction == direction && stat.Overlay == overlay && stat.Kind == kind {
			return stat.Count
		}
	}
	return 0
}
