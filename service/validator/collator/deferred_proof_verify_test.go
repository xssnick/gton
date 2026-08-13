package collator

import (
	"bytes"
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// TestFullCollatedDeferredMessageVerifiesOnProofs is the regression for the
// rejection this node produced on a live network once validation began
// executing on collated proofs: a block that defers a newly generated message
// was rejected with "dict has special cells in tree structure", raised while
// proving that message's routed key absent from the predecessor outbound queue.
//
// Neither the block nor the proof was at fault. The reference validator never
// proves that key absent — for a deferred message it builds its queue lookup
// out of a next_prefix it deliberately does not compute — so a reference
// collator has no reason to carry that path, and demanding it rejects blocks
// the rest of the network accepts. The comment in applyOutQueueChanges has the
// line references.
//
// What this test does and does not establish is worth stating plainly. It runs
// a deferring block through proof-backed validation end to end, so the path
// stays covered and a reintroduced lookup on some other predecessor cell would
// surface here. It does not reproduce the pruned branch itself: collation
// scans this fixture's outbound queue during cleanup, which pulls the whole
// small queue into the proof, and no reachable knob makes that queue large
// enough to stay pruned without the fixture becoming its own experiment. The
// evidence for the change is the reference source quoted above; production was
// the reproduction.
func TestFullCollatedDeferredMessageVerifiesOnProofs(t *testing.T) {
	req := emptyCandidateRequest(t)
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0x71}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x72}, 32))
	outbound, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     receiver,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: sender, code: externalSendTwiceCode(t, outbound), balance: 100_000_000_000},
	))
	req.Previous.State = previousStateWithDenseOutQueue(t, req.Previous.State, 1_024)
	denseQueueSize := uint64(1_024)
	req.Previous.OutQueueSize = &denseQueueSize
	// Collating one block over this state is what gives the candidate a
	// predecessor with a real block root, which full collated data binds its
	// state proof to.
	req.Internals = &msgpool.Cut{}
	req = advanceCandidateRequestApplyingUpdate(t, req)

	req.Internals = &msgpool.Cut{More: true}
	req.Masterchain.Config.capabilities |= capDeferMessages | capFullCollatedData
	req.Dispatch = DispatchPolicy{
		DeferringEnabled:       true,
		DeferMessagesAfter:     1,
		DeferOutQueueSizeLimit: 2_048,
	}
	attachFullCollatedTestNeighbors(t, &req)

	external, err := tlb.ToCell(&tlb.ExternalMessage{DstAddr: sender, Body: cell.BeginCell().EndCell()})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, external)}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("collate deferring candidate: %v", err)
	}
	// Without a deferred message this would pass on an untested path: the
	// descriptor whose validation demanded the pruned branch would simply not
	// be in the block.
	if candidate.Stats.DeferredMessages == 0 {
		t.Fatalf("fixture produced no deferred message: %+v", candidate.Stats)
	}

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	verification.Previous.State = narrowedStateRoot(t, req.Previous.State)
	verification.Neighbors = collatedNeighborQueues(t, req, candidate)

	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify deferring candidate on its own proofs: %v", err)
	}
}
