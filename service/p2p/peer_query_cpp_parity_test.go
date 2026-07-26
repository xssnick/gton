package p2p

import (
	"context"
	"reflect"
	"strings"
	"testing"

	tnstore "github.com/xssnick/gton/service/storage"
	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
)

func TestCppFullNodeQueryOverloadsAreRegisteredAndDispatched(t *testing.T) {
	block := testStoredMasterBlockID(1)
	state := PersistentStateIDV2{
		Block:            block,
		MasterchainBlock: block,
	}
	queries := []struct {
		name  string
		query tl.Serializable
	}{
		{"getNextBlockDescription", GetNextBlockDescription{PrevBlock: block}},
		{"prepareBlock", PrepareBlock{Block: block}},
		{"downloadBlock", tonnodeapi.DownloadBlock{Block: block}},
		{"downloadBlockFull", tonnodeapi.DownloadBlockFull{Block: block}},
		{"downloadNextBlockFull", DownloadNextBlockFull{PrevBlock: block}},
		{"downloadNextBlocksFull", DownloadNextBlocksFull{PrevBlock: block}},
		{"prepareBlockProof", PrepareBlockProof{Block: block}},
		{"prepareKeyBlockProof", PrepareKeyBlockProof{Block: block}},
		{"downloadBlockProof", DownloadBlockProof{Block: block}},
		{"downloadBlockProofLink", DownloadBlockProofLink{Block: block}},
		{"downloadKeyBlockProof", DownloadKeyBlockProof{Block: block}},
		{"downloadKeyBlockProofLink", DownloadKeyBlockProofLink{Block: block}},
		{"prepareZeroState", PrepareZeroState{Block: block}},
		{"preparePersistentState", PreparePersistentState{
			Block:            block,
			MasterchainBlock: block,
		}},
		{"getNextKeyBlockIds", GetNextKeyBlockIDs{Block: block}},
		{"downloadZeroState", DownloadZeroState{Block: block}},
		{"getCapabilities", GetCapabilities{}},
		{"getArchiveInfo", GetArchiveInfo{}},
		{"getShardArchiveInfo", GetShardArchiveInfo{
			ShardPrefix: tonnodeapi.ShardID{
				Workchain: -1,
				Shard:     topShard,
			},
		}},
		{"getArchiveSlice", GetArchiveSlice{}},
		{"downloadPersistentStateSliceV2", DownloadPersistentStateSliceV2{
			State: state,
		}},
		{"getPersistentStateSizeV2", GetPersistentStateSizeV2{State: state}},
		{"slave.sendExtMessage", SendExtMessage{
			Message: tonnodeapi.ExternalMessage{},
		}},
	}
	if len(queries) != 23 {
		t.Fatalf("registered C++ overload count = %d, want 23", len(queries))
	}

	node := newTestNode(t)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name: "masterchain",
		},
		log: discardLogger(),
	})

	for _, test := range queries {
		t.Run(test.name, func(t *testing.T) {
			wire, err := tl.Serialize(test.query, true)
			if err != nil {
				t.Fatalf("serialize %T: %v", test.query, err)
			}

			var parsed any
			rest, err := tl.Parse(&parsed, wire, true)
			if err != nil {
				t.Fatalf("parse %T: %v", test.query, err)
			}
			if len(rest) != 0 {
				t.Fatalf("parse %T left %d trailing bytes", test.query, len(rest))
			}
			if reflect.TypeOf(parsed) != reflect.TypeOf(test.query) {
				t.Fatalf(
					"parsed query type = %T, want value %T",
					parsed,
					test.query,
				)
			}

			_, err = sub.dispatchPeerQuery(context.Background(), parsed)
			if err != nil &&
				strings.Contains(err.Error(), "unsupported peer query") {
				t.Fatalf("C++ overload %T is not dispatched", parsed)
			}
		})
	}
}

func TestCppNextBlockDescriptionSemantics(t *testing.T) {
	node := newTestNode(t)
	store := node.peerStorage.(*testPeerStore)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name: "masterchain",
		},
		log: discardLogger(),
	})

	shardBlock := testStoredBlockID(10)
	if response, err := sub.dispatchPeerQuery(
		context.Background(),
		GetNextBlockDescription{PrevBlock: shardBlock},
	); err == nil || response != nil {
		t.Fatalf(
			"shard next description = (%T, %v), want protocol error",
			response,
			err,
		)
	}

	prev := testStoredMasterBlockID(11)
	next := testStoredMasterBlockID(12)
	if err := store.SaveBlockFull(&tnstore.ServedBlockFull{
		ID:     next,
		Proof:  []byte{1},
		Block:  []byte{2},
		IsLink: true,
	}); err != nil {
		t.Fatalf("save link-only next block: %v", err)
	}
	if err := store.LinkNextBlock(prev, next); err != nil {
		t.Fatalf("link next block: %v", err)
	}

	response, err := sub.dispatchPeerQuery(
		context.Background(),
		GetNextBlockDescription{PrevBlock: prev},
	)
	if err != nil {
		t.Fatalf("link-only next description: %v", err)
	}
	if _, ok := response.(BlockDescriptionEmpty); !ok {
		t.Fatalf(
			"link-only next description = %T, want BlockDescriptionEmpty",
			response,
		)
	}
}

func TestCppProofAvailabilitySemantics(t *testing.T) {
	node := newTestNode(t)
	store := node.peerStorage.(*testPeerStore)
	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		log:  discardLogger(),
	})

	shardBlock := testStoredBlockID(20)
	if err := store.SaveBlockProof(
		tnstore.ServedProofBlock,
		shardBlock,
		[]byte{1},
		nil,
	); err != nil {
		t.Fatalf("save shard full proof: %v", err)
	}
	response, err := sub.dispatchPeerQuery(
		context.Background(),
		PrepareBlockProof{
			Block:        shardBlock,
			AllowPartial: false,
		},
	)
	if err != nil {
		t.Fatalf("prepare shard full proof: %v", err)
	}
	if _, ok := response.(PreparedProofLink); !ok {
		t.Fatalf(
			"prepare shard full proof = %T, want PreparedProofLink",
			response,
		)
	}

	masterBlock := testStoredMasterBlockID(21)
	if err := store.SaveBlockProof(
		tnstore.ServedProofBlock,
		masterBlock,
		[]byte{2},
		nil,
	); err != nil {
		t.Fatalf("save masterchain full proof: %v", err)
	}
	response, err = sub.dispatchPeerQuery(
		context.Background(),
		DownloadBlockProofLink{Block: masterBlock},
	)
	if err == nil || response != nil {
		t.Fatalf(
			"download unstored block proof link = (%T, %v), want unavailable",
			response,
			err,
		)
	}
}

func TestCppPreparePersistentStateAcceptsZeroSize(t *testing.T) {
	node := newTestNode(t)
	store := node.peerStorage.(*testPeerStore)
	block := testStoredBlockID(30)
	master := testStoredMasterBlockID(31)

	store.mu.Lock()
	store.stateFiles[store.persistentStateKey(block, master, 0)] = []byte{}
	store.mu.Unlock()

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		log:  discardLogger(),
	})
	response, err := sub.dispatchPeerQuery(
		context.Background(),
		PreparePersistentState{
			Block:            block,
			MasterchainBlock: master,
		},
	)
	if err != nil {
		t.Fatalf("prepare zero-size persistent state: %v", err)
	}
	if _, ok := response.(PreparedState); !ok {
		t.Fatalf(
			"prepare zero-size persistent state = %T, want PreparedState",
			response,
		)
	}
}
