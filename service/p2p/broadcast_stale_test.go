package p2p

import (
	"context"
	"sync/atomic"
	"testing"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type countingBroadcastSignatureVerifier struct {
	calls atomic.Int64
}

func (v *countingBroadcastSignatureVerifier) CheckBlockBroadcastSignatures(context.Context, BlockBroadcastSignatureCheck) error {
	v.calls.Add(1)
	return nil
}

func (v *countingBroadcastSignatureVerifier) CheckBlockFinalitySignatures(context.Context, BlockFinalitySignatureCheck) (*BlockFinalitySignatureCheckResult, error) {
	v.calls.Add(1)
	return &BlockFinalitySignatureCheckResult{SignaturesVerifiedKey: []byte("test-finality")}, nil
}

func (v *countingBroadcastSignatureVerifier) ValidateShardDescriptionBroadcast(_ context.Context, req ShardDescriptionSignatureCheck) (*ShardBlockDescription, error) {
	return &ShardBlockDescription{
		Block:         req.Block,
		CatchainSeqno: uint32(req.CatchainSeqno),
	}, nil
}

func TestNoteAppliedMasterchainSeqnoIsMonotonic(t *testing.T) {
	node := newTestNode(t)

	master := testBlockID(-1, topShard, 100)
	if node.alreadyAppliedMasterchainBroadcast(master) {
		t.Fatal("broadcast considered already applied before any applied seqno is known")
	}

	node.NoteAppliedMasterchainSeqno(100)
	node.NoteAppliedMasterchainSeqno(90)
	if got := node.appliedMasterchainSeqno.Load(); got != 100 {
		t.Fatalf("applied seqno regressed to %d", got)
	}

	if !node.alreadyAppliedMasterchainBroadcast(testBlockID(-1, topShard, 100)) {
		t.Fatal("broadcast at applied seqno is not already applied")
	}
	if !node.alreadyAppliedMasterchainBroadcast(testBlockID(-1, topShard, 99)) {
		t.Fatal("broadcast below applied seqno is not already applied")
	}
	if node.alreadyAppliedMasterchainBroadcast(testBlockID(-1, topShard, 101)) {
		t.Fatal("broadcast above applied seqno reported already applied")
	}
	if node.alreadyAppliedMasterchainBroadcast(testBlockID(0, topShard, 1)) {
		t.Fatal("shard broadcast reported already applied by masterchain seqno")
	}
}

// TestClassifyDropsAlreadyAppliedMasterchainBroadcastBeforeSignatureCheck pins
// the ordering: a masterchain broadcast at or below the applied seqno must be
// dropped before the validator-signature verifier is consulted.
func TestClassifyDropsAlreadyAppliedMasterchainBroadcastBeforeSignatureCheck(t *testing.T) {
	node := newTestNode(t)
	verifier := &countingBroadcastSignatureVerifier{}
	node.signatureVerifier = verifier

	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "masterchain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	}

	proofBOC := cell.BeginCell().EndCell().ToBOC()
	broadcast := func(seqno uint32, marker byte) tonnodeapi.BlockBroadcast {
		return tonnodeapi.BlockBroadcast{
			ID:    testBlockID(-1, topShard, seqno),
			Proof: proofBOC,
			Data:  []byte{marker},
			Signatures: []tonnodeapi.BlockSignature{{
				Who:       make([]byte, 32),
				Signature: make([]byte, 64),
			}},
		}
	}
	classify := func(msg tonnodeapi.BlockBroadcast) *acceptedBroadcast {
		payload, err := tl.Serialize(msg, true)
		if err != nil {
			t.Fatalf("serialize broadcast: %v", err)
		}
		return sub.classifyBroadcast(nil, msg, payload, DeliverySimple, false, testPeerID("peer"))
	}

	// Fresh broadcast reaches the signature verifier (the junk block payload
	// fails decode afterwards, which is fine for this test).
	if accepted := classify(broadcast(300, 1)); accepted != nil {
		t.Fatalf("junk broadcast was accepted: %+v", accepted)
	}
	if calls := verifier.calls.Load(); calls != 1 {
		t.Fatalf("fresh broadcast made %d signature checks, want 1", calls)
	}

	node.NoteAppliedMasterchainSeqno(300)

	if accepted := classify(broadcast(300, 2)); accepted != nil {
		t.Fatalf("already-applied broadcast at applied seqno was accepted: %+v", accepted)
	}
	if accepted := classify(broadcast(299, 3)); accepted != nil {
		t.Fatalf("already-applied broadcast below applied seqno was accepted: %+v", accepted)
	}
	if calls := verifier.calls.Load(); calls != 1 {
		t.Fatalf("already-applied broadcasts reached the signature verifier: %d calls, want 1", calls)
	}

	if accepted := classify(broadcast(301, 4)); accepted != nil {
		t.Fatalf("junk broadcast was accepted: %+v", accepted)
	}
	if calls := verifier.calls.Load(); calls != 2 {
		t.Fatalf("broadcast above applied seqno made %d signature checks, want 2", calls)
	}
}

func TestClassifyBlockFinalityBroadcastRejectsOrdinarySignatureSetBeforeSignatureCheck(t *testing.T) {
	node := newTestNode(t)
	verifier := &countingBroadcastSignatureVerifier{}
	node.signatureVerifier = verifier

	sub := &overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x04, 0x05, 0x06},
		},
		log: discardLogger(),
	}

	msg := BlockFinalityBroadcast{
		ID: testBlockID(0, topShard, 302),
		SignatureSet: tonnodeapi.SignatureSetOrdinary{
			CatchainSeqno:    10,
			ValidatorSetHash: 20,
		},
	}
	payload, err := tl.Serialize(msg, true)
	if err != nil {
		t.Fatalf("serialize block finality broadcast: %v", err)
	}

	if accepted := sub.classifyBroadcast(nil, msg, payload, DeliverySimple, false, testPeerID("peer")); accepted != nil {
		t.Fatalf("ordinary finality broadcast was accepted: %+v", accepted)
	}
	if calls := verifier.calls.Load(); calls != 0 {
		t.Fatalf("ordinary finality broadcast reached the signature verifier: %d calls", calls)
	}
}
