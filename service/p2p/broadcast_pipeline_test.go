package p2p

import (
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
