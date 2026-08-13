package collator

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// A newly enqueued entry must satisfy emitted_lt <= enqueued_lt on top of the
// block lt window: precheck_one_message_queue_update reads the emitted lt back
// out of the stored envelope and rejects an entry stamped earlier than the
// transaction that emitted the message (validate-query.cpp:3527-3536).
//
// The candidate below emits one new outbound message from a transaction that
// runs after the block start, so its natural enqueued lt sits strictly above
// BlockInfo.StartLt. Rewriting it down to StartLt keeps the entry inside
// [StartLt, EndLt) -- the only window the window check can see -- while pushing
// it below the message's own created lt. The untampered candidate is verified
// first as a positive control, so a rejection caused by the fixture rather than
// by the rewrite cannot pass for protection.
func TestSemanticVerifierRejectsQueueEnqueuedLTBelowEmittedLT(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{More: true}
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0xc1}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0xd1}, 32))
	outbound, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     destination,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(
		t,
		req.Header.GenUtime,
		activeContract{address: sender, code: externalSendCode(t, outbound), balance: 100_000_000_000},
	))
	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: sender,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, external)}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.EnqueuedMessages != 1 || candidate.Stats.OutQueueSize != 1 {
		t.Fatalf("unexpected queue fixture stats: %+v", candidate.Stats)
	}

	control := cloneVerificationCandidate(candidate)
	controlVerification := shardVerificationRequest(req, control)
	controlVerification.NeighborShardEndLT = req.NeighborShardEndLT
	controlVerification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	if err = VerifyShardCandidate(context.Background(), controlVerification); err != nil {
		t.Fatalf("verify untampered enqueued lt: %v", err)
	}

	tampered := cloneVerificationCandidate(candidate)

	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, tampered)); err != nil {
		t.Fatal(err)
	}
	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, tampered.State); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err = parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	iterator, err := queue.OutQueue.IteratorExtra(false, false)
	if err != nil {
		t.Fatal(err)
	}
	if !iterator.Next() {
		t.Fatalf("candidate outbound queue is empty: %v", iterator.Err())
	}
	item := iterator.View()
	key, err := item.Key.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	var enqueued tlb.EnqueuedMsg
	if err = loadExactSlice(&enqueued, &item.Value); err != nil {
		t.Fatal(err)
	}
	if enqueued.EnqueuedLT == block.BlockInfo.StartLt {
		t.Fatal("new-message fixture already uses block start as enqueued lt")
	}
	envelope, err := parseSemanticEnvelope(enqueued.Msg)
	if err != nil {
		t.Fatal(err)
	}
	// The rewrite is only meaningful while the block start stays below the lt
	// the reference derives for this entry; otherwise nothing is being loosened.
	if block.BlockInfo.StartLt >= envelope.bound.lt {
		t.Fatalf("fixture emitted lt %d is not above block start %d", envelope.bound.lt, block.BlockInfo.StartLt)
	}
	enqueued.EnqueuedLT = block.BlockInfo.StartLt
	value, err := enqueued.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	if err = queue.OutQueue.Set(key, value); err != nil {
		t.Fatal(err)
	}
	if err = iterator.Err(); err != nil {
		t.Fatal(err)
	}

	state.OutMsgQueueInfo, err = queue.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	stateRoot, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}
	stateUpdate, err := cell.CreateMerkleUpdate(req.Previous.State, stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	block.StateUpdate = stateUpdate
	blockRoot, err := tlb.ToCell(&block)
	if err != nil {
		t.Fatal(err)
	}
	tampered.State = stateRoot
	tampered.StateUpdate = stateUpdate
	rewriteVerificationCandidateBOC(t, tampered, blockRoot)

	verification := shardVerificationRequest(req, tampered)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	err = VerifyShardCandidate(context.Background(), verification)
	if !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "enqueued lt is below the message emitted lt") {
		t.Fatalf("enqueued lt below emitted lt error = %v, want invalid input naming the emitted lt", err)
	}
}
