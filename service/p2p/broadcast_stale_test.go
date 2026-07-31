package p2p

import (
	"context"
	"sync/atomic"
	"testing"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
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
	if node.alreadyAppliedBroadcast(master) {
		t.Fatal("broadcast considered already applied before any applied seqno is known")
	}

	node.NoteAppliedMasterchainSeqno(100)
	node.NoteAppliedMasterchainSeqno(90)
	if got := node.appliedMasterchainSeqno.Load(); got != 100 {
		t.Fatalf("applied seqno regressed to %d", got)
	}

	if !node.alreadyAppliedBroadcast(testBlockID(-1, topShard, 100)) {
		t.Fatal("broadcast at applied seqno is not already applied")
	}
	if !node.alreadyAppliedBroadcast(testBlockID(-1, topShard, 99)) {
		t.Fatal("broadcast below applied seqno is not already applied")
	}
	if node.alreadyAppliedBroadcast(testBlockID(-1, topShard, 101)) {
		t.Fatal("broadcast above applied seqno reported already applied")
	}
	if node.alreadyAppliedBroadcast(testBlockID(0, topShard, 1)) {
		t.Fatal("shard broadcast reported already applied by masterchain seqno")
	}
}

func TestNoteAppliedShardHeadsGatesShardBroadcastsPerShard(t *testing.T) {
	node := newTestNode(t)

	const (
		leftShard  int64 = 0x2000000000000000
		rightShard int64 = 0x6000000000000000
	)

	if node.alreadyAppliedBroadcast(testBlockID(0, leftShard, 10)) {
		t.Fatal("shard broadcast considered already applied before any head is known")
	}

	node.NoteAppliedShardHeads([]ton.BlockIDExt{
		testBlockID(0, leftShard, 10),
		testBlockID(0, rightShard, 20),
		// the masterchain keeps its own seqno gate and must not enter the table
		testBlockID(-1, topShard, 500),
	})

	if !node.alreadyAppliedBroadcast(testBlockID(0, leftShard, 10)) {
		t.Fatal("shard broadcast at the applied head is not already applied")
	}
	if !node.alreadyAppliedBroadcast(testBlockID(0, leftShard, 9)) {
		t.Fatal("shard broadcast below the applied head is not already applied")
	}
	if node.alreadyAppliedBroadcast(testBlockID(0, leftShard, 11)) {
		t.Fatal("shard broadcast above the applied head reported already applied")
	}
	// each shard is gated by its own head: the same seqno is already applied in
	// one shard and still wanted in the other
	if !node.alreadyAppliedBroadcast(testBlockID(0, rightShard, 15)) {
		t.Fatal("shard broadcast below its own applied head is not already applied")
	}
	if node.alreadyAppliedBroadcast(testBlockID(0, leftShard, 15)) {
		t.Fatal("shard broadcast above its own applied head gated by another shard's head")
	}
	// a shard the table does not know (a fresh split or merge) stays ungated
	if node.alreadyAppliedBroadcast(testBlockID(0, 0x1000000000000000, 1)) {
		t.Fatal("broadcast for an unknown shard reported already applied")
	}
	if node.alreadyAppliedBroadcast(testBlockID(-1, topShard, 500)) {
		t.Fatal("masterchain broadcast gated by the shard head table")
	}

	// a state published out of order must not walk a shard head backwards
	node.NoteAppliedShardHeads([]ton.BlockIDExt{
		testBlockID(0, leftShard, 8),
		testBlockID(0, rightShard, 25),
	})
	if !node.alreadyAppliedBroadcast(testBlockID(0, leftShard, 10)) {
		t.Fatal("applied shard head regressed")
	}
	if !node.alreadyAppliedBroadcast(testBlockID(0, rightShard, 25)) {
		t.Fatal("applied shard head did not advance")
	}

	// shards missing from a committed state fall out instead of lingering as
	// dead prefixes after a split or a merge
	node.NoteAppliedShardHeads([]ton.BlockIDExt{testBlockID(0, rightShard, 26)})
	if node.alreadyAppliedBroadcast(testBlockID(0, leftShard, 10)) {
		t.Fatal("shard dropped from the committed state is still gated")
	}
}

// TestClassifyDropsAlreadyAppliedMasterchainBroadcastBeforeSignatureCheck pins
// the ordering: a masterchain broadcast at or below the applied seqno must be
// dropped before the validator-signature verifier is consulted.
func TestClassifyDropsAlreadyAppliedMasterchainBroadcastBeforeSignatureCheck(t *testing.T) {
	node := newTestNode(t)
	verifier := &countingBroadcastSignatureVerifier{}
	node.signatureVerifier = verifier

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "masterchain",
			ShortID: []byte{0x01, 0x02, 0x03},
		},
		log: discardLogger(),
	})

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

	// Fresh broadcast reaches the signature verifier. With verified
	// signatures the decode is offloaded, so classify acknowledges the
	// broadcast without an event; the junk payload fails decode later on the
	// pool, which is fine for this test.
	if accepted := classify(broadcast(300, 1)); accepted == nil || accepted.event != nil {
		t.Fatalf("fresh broadcast should be acknowledged for async decode without an event, got %+v", accepted)
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

	if accepted := classify(broadcast(301, 4)); accepted == nil || accepted.event != nil {
		t.Fatalf("broadcast above applied seqno should be acknowledged for async decode, got %+v", accepted)
	}
	if calls := verifier.calls.Load(); calls != 2 {
		t.Fatalf("broadcast above applied seqno made %d signature checks, want 2", calls)
	}
}

// TestClassifyDropsAlreadyAppliedShardBroadcastBeforeSignatureCheck pins the
// same ordering for shards: a shard block at or below the applied head of that
// exact shard is dropped before the verifier is consulted, so a replayed shard
// payload buys neither an ed25519 pass nor a decode worker.
func TestClassifyDropsAlreadyAppliedShardBroadcastBeforeSignatureCheck(t *testing.T) {
	node := newTestNode(t)
	verifier := &countingBroadcastSignatureVerifier{}
	node.signatureVerifier = verifier

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x07, 0x08, 0x09},
		},
		log: discardLogger(),
	})

	const (
		appliedShard int64 = 0x2000000000000000
		otherShard   int64 = 0x6000000000000000
	)
	proofBOC := cell.BeginCell().EndCell().ToBOC()
	classify := func(shard int64, seqno uint32, marker byte) *acceptedBroadcast {
		msg := tonnodeapi.BlockBroadcast{
			ID:    testBlockID(0, shard, seqno),
			Proof: proofBOC,
			Data:  []byte{marker},
			Signatures: []tonnodeapi.BlockSignature{{
				Who:       make([]byte, 32),
				Signature: make([]byte, 64),
			}},
		}
		payload, err := tl.Serialize(msg, true)
		if err != nil {
			t.Fatalf("serialize broadcast: %v", err)
		}
		return sub.classifyBroadcast(nil, msg, payload, DeliverySimple, false, testPeerID("peer"))
	}

	node.NoteAppliedShardHeads([]ton.BlockIDExt{testBlockID(0, appliedShard, 400)})

	if accepted := classify(appliedShard, 400, 1); accepted != nil {
		t.Fatalf("already-applied shard broadcast at the applied head was accepted: %+v", accepted)
	}
	if accepted := classify(appliedShard, 399, 2); accepted != nil {
		t.Fatalf("already-applied shard broadcast below the applied head was accepted: %+v", accepted)
	}
	if calls := verifier.calls.Load(); calls != 0 {
		t.Fatalf("already-applied shard broadcasts reached the signature verifier: %d calls", calls)
	}

	// the head of one shard must not gate another shard at the same seqno
	if accepted := classify(otherShard, 400, 3); accepted == nil || accepted.event != nil {
		t.Fatalf("broadcast for an ungated shard should be acknowledged for async decode, got %+v", accepted)
	}
	if accepted := classify(appliedShard, 401, 4); accepted == nil || accepted.event != nil {
		t.Fatalf("shard broadcast above the applied head should be acknowledged for async decode, got %+v", accepted)
	}
	if calls := verifier.calls.Load(); calls != 2 {
		t.Fatalf("wanted-shard broadcasts made %d signature checks, want 2", calls)
	}
}

func TestClassifyBlockFinalityBroadcastRejectsOrdinarySignatureSetBeforeSignatureCheck(t *testing.T) {
	node := newTestNode(t)
	verifier := &countingBroadcastSignatureVerifier{}
	node.signatureVerifier = verifier

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{
			Name:    "basechain",
			ShortID: []byte{0x04, 0x05, 0x06},
		},
		log: discardLogger(),
	})

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
