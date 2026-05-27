package p2p

import (
	"bytes"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/tonutils-go/address"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestClassifyInvalidCompressedBlockBroadcastDoesNotWakeBeforeSignaturePrecheck(t *testing.T) {
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
			if first != nil {
				t.Fatalf("invalid masterchain broadcast was accepted before signature precheck: %+v", first)
			}
			if _, ready := node.MasterchainBroadcastAfter(tt.block.SeqNo - 1); ready {
				t.Fatal("invalid masterchain broadcast should not wake before signature precheck")
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

func TestClassifyShardBlockBroadcastDropsWhenSignaturePrecheckFails(t *testing.T) {
	node := newTestNode(t)
	node.SetBroadcastSignatureVerifier(testRejectBroadcastSignatureVerifier{err: errors.New("bad signatures")})
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}

	block := testBlockID(0, topShard, 202)
	msg := tonnodeapi.NewShardBlockBroadcast{
		Block: tonnodeapi.NewShardBlock{
			ID:      block,
			CCSeqno: 7,
			Data:    []byte{0x01},
		},
	}

	accepted := sub.classifyBroadcast(nil, msg, []byte{0x01}, DeliveryFEC, false, "peer")
	if accepted != nil {
		t.Fatalf("shard block broadcast with failed signature precheck was accepted: %+v", accepted)
	}
	if got := testBroadcastDropStatCount(node, "basechain", "tonNode.newShardBlockBroadcast", "signature_check_failed"); got != 1 {
		t.Fatalf("dropped broadcast count = %d, want 1", got)
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

	node.acceptBroadcast(*accepted)
	if got := testBroadcastDropStatCount(node, "basechain", "tonNode.newShardBlockBroadcast", "seen"); got != 1 {
		t.Fatalf("seen broadcast drop count = %d, want 1", got)
	}
	if got := testBroadcastStatCount(node, "accepted", "basechain", "tonNode.newShardBlockBroadcast"); got != 1 {
		t.Fatalf("duplicate accepted broadcast count = %d, want 1", got)
	}
}

func TestClassifyExternalMessageBroadcastCachesByBodyHash(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}
	data := testExternalMessageBOC(t)
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: data},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize external broadcast: %v", err)
	}

	first := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, "peer")
	if first == nil || first.rebroadcast == nil {
		t.Fatal("expected first external broadcast to be accepted")
	}

	second := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, "peer")
	if second != nil {
		t.Fatalf("duplicate external broadcast was accepted: %+v", second)
	}
}

func TestClassifyExternalMessageBroadcastLimitsPerAddress(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}

	for i := 0; i < extmsg.DefaultAddressLimit; i++ {
		data := testExternalMessageBOCWithBody(t, uint64(i))
		msg := tonnodeapi.NewExternalMessageBroadcast{
			Message: tonnodeapi.ExternalMessage{Data: data},
		}
		payload, err := tl.Serialize(msg, true)
		if err != nil {
			t.Fatalf("serialize external broadcast %d: %v", i, err)
		}
		accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, "peer")
		if accepted == nil || accepted.rebroadcast == nil {
			t.Fatalf("external broadcast %d was not accepted", i)
		}
	}

	data := testExternalMessageBOCWithBody(t, uint64(extmsg.DefaultAddressLimit))
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: data},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize external broadcast over limit: %v", err)
	}
	if accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, "peer"); accepted != nil {
		t.Fatalf("over-limit external broadcast was accepted: %+v", accepted)
	}
}

func TestClassifyExternalMessageBroadcastSkipsOwnMessage(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}
	data := []byte{0xAA, 0xBB}
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: data},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize external broadcast: %v", err)
	}

	node.myExternalMessages.Mark(externalMessageFingerprint(sub.spec.ShortID, data), time.Now())

	accepted := sub.classifyBroadcast(nil, msg, payload, DeliveryFEC, false, "peer")
	if accepted != nil {
		t.Fatalf("own external broadcast was accepted: %+v", accepted)
	}
}

func TestClassifyExternalMessageBroadcastRejectsOversizeData(t *testing.T) {
	node := newTestNode(t)
	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}
	msg := tonnodeapi.NewExternalMessageBroadcast{
		Message: tonnodeapi.ExternalMessage{Data: make([]byte, maxOverlayPayloadSize+1)},
	}

	accepted := sub.classifyBroadcast(nil, msg, []byte{0x01}, DeliveryFEC, false, "peer")
	if accepted != nil {
		t.Fatalf("oversize external broadcast was accepted: %+v", accepted)
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

func testBroadcastDropStatCount(node *Node, overlay, kind, reason string) uint64 {
	for _, stat := range node.broadcastDropStatusSnapshot() {
		if stat.Overlay == overlay && stat.Kind == kind && stat.Reason == reason {
			return stat.Count
		}
	}
	return 0
}

func testExternalMessageBOCWithBody(t *testing.T, value uint64) []byte {
	t.Helper()

	root, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr:   address.MustParseRawAddr("0:1111111111111111111111111111111111111111111111111111111111111111"),
		ImportFee: tlb.ZeroCoins,
		Body:      cell.BeginCell().MustStoreUInt(value, 64).EndCell(),
	})
	if err != nil {
		t.Fatalf("build external message: %v", err)
	}
	return root.ToBOCWithFlags(false)
}
