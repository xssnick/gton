package p2p

import (
	"testing"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
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
		name string
		msg  any
	}{
		{
			name: "compressed",
			msg: tonnodeapi.BlockBroadcastCompressed{
				ID:         testBlockID(-1, topShard, 200),
				Compressed: []byte{0x01},
			},
		},
		{
			name: "compressed-v2",
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

			second := sub.classifyBroadcast(nil, tt.msg, payload, DeliverySimple, false, "peer")
			if second != nil {
				t.Fatalf("duplicate broadcast was accepted after fingerprint dedupe: %+v", second)
			}
		})
	}
}
