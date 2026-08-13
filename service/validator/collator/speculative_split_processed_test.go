package collator

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// TestFullCollatedSpeculativeSplitQueuePrefixRequiresExactImports pins the
// production chain which used to be covered only one piece at a time:
//
//   - a committed parent queue contains one early unprocessed message;
//   - a speculative before-split candidate appends a later message;
//   - the first child preserves both messages;
//   - the next child candidate imports the complete speculative queue prefix;
//   - a receiving validator reopens the serialized block/collated BOCs and
//     validates exclusively on their FullCollated proofs.
//
// The negative control bypasses msgpool and hands Builder only the later
// message. Builder deliberately trusts its acquired Cut, but the proof-backed
// validator must reject the resulting ProcessedInfo advance because the early
// queue entry has no exact InMsg. The ordinary msgpool path below supplies the
// complete canonical prefix instead.
func TestFullCollatedSpeculativeSplitQueuePrefixRequiresExactImports(t *testing.T) {
	parentRequest := emptyCandidateRequest(t)
	parentSource := blockShardIdent(parentRequest.Previous.ID)
	leftShard := groups.ShardID{
		Workchain: parentRequest.Shard.Workchain,
		Shard:     mustPredecessorChild(t, parentRequest.Shard.Shard, true),
	}
	left := targetShardIdent(leftShard)
	startLT := requestStartLT(t, parentRequest)
	fee := tlb.FromNanoTONU(100_000)

	early, enqueuedEarly := queuedInternal(
		t,
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x21}, 32)),
		address.NewAddress(0, 0, bytes.Repeat([]byte{0x31}, 32)),
		startLT-20,
		parentRequest.Header.GenUtime-1,
		fee,
		fee,
		0,
		parentSource,
	)
	parentRequest.Previous.State = stateWithQueueMessage(
		t,
		parentRequest.Previous.State,
		early.Key,
		enqueuedEarly,
	)
	baseQueueSize := uint64(1)
	parentRequest.Previous.OutQueueSize = &baseQueueSize

	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0x41}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x51}, 32))
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
	parentRequest.Previous.State = stateWithAccounts(
		t,
		parentRequest.Previous.State,
		activeContracts(t, parentRequest.Header.GenUtime, activeContract{
			address: sender,
			code:    externalSendCode(t, outbound),
			balance: 100_000_000_000,
		}),
	)
	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: sender,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	parentRequest.Externals = []ExternalInput{externalInput(t, external)}
	// The incomplete inbound window keeps both the old queue entry and the
	// transaction's new output queued in the before-split candidate.
	parentRequest.Internals = &msgpool.Cut{More: true}
	parentSession := &parentRequest.Masterchain.Groups.Active[0]
	parentSession.Registered[0].FSM = groups.ShardFSM{
		Kind:     groups.ShardFSMSplit,
		UTime:    parentRequest.Header.GenUtime,
		Interval: 10,
	}
	parentRequest.BeforeSplit = true

	parentCandidate, err := testBuilder().BuildShard(context.Background(), parentRequest)
	if err != nil {
		t.Fatalf("build speculative before-split candidate: %v", err)
	}
	if parentCandidate.Stats.EnqueuedMessages != 1 || parentCandidate.Stats.OutQueueSize != 2 {
		t.Fatalf("parent queue stats = %+v, want one new message and two queued", parentCandidate.Stats)
	}
	parent := previousFromCandidate(t, parentCandidate)

	childRequest := parentRequest
	childRequest.Shard = leftShard
	childRequest.Previous = parent
	childRequest.BeforeSplit = false
	childRequest.Externals = nil
	childRequest.Internals = &msgpool.Cut{More: true}
	childRequest.StorageStats = parentCandidate.StorageStats
	childRequest.Header.GenUtime++
	childRequest.Header.GenUtimeMS = uint64(childRequest.Header.GenUtime) * 1_000
	childRequest.Masterchain.Config.capabilities |= capFullCollatedData
	childRequest.Masterchain.Groups.Active = []groups.Session{{
		Shard:            childRequest.Shard,
		CatchainSeqno:    parentSession.CatchainSeqno,
		ValidatorSetHash: parentSession.ValidatorSetHash,
		Genesis:          []ton.BlockIDExt{*parent.ID.Copy()},
		Registered: []groups.ShardDescription{{
			Shard:       parentRequest.Shard,
			Block:       *parent.ID.Copy(),
			BeforeSplit: true,
		}},
	}}
	parentNeighbor := fullCollatedTestNeighbor(t, parent)
	childRequest.Neighbors = append([]Neighbor{parentNeighbor}, masterchainNeighbor(childRequest)...)
	childRequest.NeighborShardEndLT = func(uint32, int32, uint64) uint64 {
		return parentNeighbor.EndLT
	}

	childCandidate, err := testBuilder().BuildShard(context.Background(), childRequest)
	if err != nil {
		t.Fatalf("build first split child: %v", err)
	}
	if childCandidate.Stats.OutQueueSize != 2 || childCandidate.Stats.InternalsImported != 0 {
		t.Fatalf("first child queue stats = %+v, want both messages preserved", childCandidate.Stats)
	}
	verifySerializedFullCollatedCandidate(t, childRequest, childCandidate)
	child := previousFromCandidate(t, childCandidate)

	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{left}); err != nil {
		t.Fatal(err)
	}
	childSource := blockShardIdent(child.ID)
	childRef, err := localSourceRef(child.ID)
	if err != nil {
		t.Fatal(err)
	}
	seeds, total, err := pool.Internals().SeedsFromStateRoot(childSource, childRef, child.State)
	if err != nil {
		t.Fatal(err)
	}
	childMessages := routedSeedMessages(t, seeds, left)
	if err = pool.Internals().Seed(left, childSource, childRef, childMessages, total); err != nil {
		t.Fatal(err)
	}

	cut, err := pool.Internals().Cut(left, msgpool.CutRequest{
		Sources: map[msgpool.ShardIdent]msgpool.CutSource{
			childSource: {Visible: childRef},
		},
	})
	if err != nil {
		t.Fatalf("cut speculative child queue: %v", err)
	}
	if len(cut.Messages) != 2 || cut.More {
		t.Fatalf("speculative cut = %+v, want a complete two-message prefix", cut)
	}
	if cut.Messages[0].Root.HashKey() != early.Root.HashKey() {
		t.Fatalf("first speculative message = %x, want early %x",
			cut.Messages[0].Root.HashKey(), early.Root.HashKey())
	}
	if msgpool.CompareLtHash(cut.Messages[0], cut.Messages[1]) >= 0 {
		t.Fatal("speculative queue prefix is not in canonical order")
	}

	nextRequest := childRequest
	nextRequest.Previous = child
	nextRequest.Internals = cut
	nextRequest.StorageStats = childCandidate.StorageStats
	nextRequest.Header.GenUtime++
	nextRequest.Header.GenUtimeMS = uint64(nextRequest.Header.GenUtime) * 1_000
	// The masterchain still registers the pre-split parent, so the next child
	// needs its separate neighbor proof in addition to the immediate child
	// predecessor proof built by the collator itself.
	blockProof, err := cell.CreateMerkleProof(parent.Block)
	if err != nil {
		t.Fatal(err)
	}
	stateProof, err := cell.CreateMerkleProof(parent.State)
	if err != nil {
		t.Fatal(err)
	}
	nextRequest.FullCollatedProofs = &staticFullCollatedProofProvider{
		roots: []*cell.Cell{stateProof, blockProof},
	}

	badRequest := nextRequest
	badRequest.Internals = &msgpool.Cut{Messages: cut.Messages[1:]}
	badCandidate, err := testBuilder().BuildShard(context.Background(), badRequest)
	if err != nil {
		t.Fatalf("build negative-control candidate: %v", err)
	}
	if badCandidate.Stats.InternalsImported != 1 || badCandidate.Stats.OutQueueSize != 1 {
		t.Fatalf("later-only candidate stats = %+v, want one import and the early message retained", badCandidate.Stats)
	}
	badVerification := serializedFullCollatedVerification(t, badRequest, badCandidate)
	err = VerifyShardCandidate(context.Background(), badVerification)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "no exact InMsg") {
		t.Fatalf("later-only ProcessedInfo error = %v, want missing exact InMsg", err)
	}

	nextCandidate, err := testBuilder().BuildShard(context.Background(), nextRequest)
	if err != nil {
		t.Fatalf("build complete-prefix child candidate: %v", err)
	}
	if nextCandidate.Stats.InternalsImported != 2 || nextCandidate.Stats.OutQueueSize != 0 {
		t.Fatalf("complete-prefix candidate stats = %+v, want two imports and an empty queue", nextCandidate.Stats)
	}
	inMessages, _ := candidateMessageDescriptors(
		t,
		nextCandidate,
		nextRequest.Masterchain.Config.globalVersion,
	)
	for _, message := range cut.Messages {
		hash := message.Root.HashKey()
		descriptor, parseErr := parseSemanticInDescriptor(
			*descriptorByHash(t, inMessages.AugmentedDictionary, hash),
			hash,
		)
		if parseErr != nil {
			t.Fatalf("parse exact InMsg %x: %v", hash, parseErr)
		}
		if descriptor.tag != semanticInFinal && descriptor.tag != semanticInTransit {
			t.Fatalf("InMsg %x tag = %d, want final or transit", hash, descriptor.tag)
		}
		if descriptor.envelope == nil || descriptor.envelope.root.HashKey() != message.EnvelopeCell.HashKey() {
			t.Fatalf("InMsg %x does not carry the exact acquired envelope", hash)
		}
	}
	verifySerializedFullCollatedCandidate(t, nextRequest, nextCandidate)
}

func previousFromCandidate(t *testing.T, candidate *Candidate) PreviousBlock {
	t.Helper()

	queueSize := candidate.Stats.OutQueueSize
	return PreviousBlock{
		ID:           candidate.ID,
		Block:        candidateBlock(t, candidate),
		State:        candidate.State,
		OutQueueSize: &queueSize,
	}
}

func routedSeedMessages(t *testing.T, seeds []msgpool.RoutedSeed, destination msgpool.ShardIdent) []*msgpool.InternalMessage {
	t.Helper()

	for i := range seeds {
		if seeds[i].Destination == destination {
			return seeds[i].Messages
		}
	}
	t.Fatalf("routed seeds have no destination (%d,%016x)", destination.Workchain, destination.Shard)
	return nil
}

func verifySerializedFullCollatedCandidate(t *testing.T, req ShardRequest, candidate *Candidate) {
	t.Helper()

	verification := serializedFullCollatedVerification(t, req, candidate)
	if err := VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify serialized full-collated candidate: %v", err)
	}
}

func serializedFullCollatedVerification(
	t *testing.T,
	req ShardRequest,
	candidate *Candidate,
) ShardVerificationRequest {
	t.Helper()

	blockRoot, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, blockRoot); err != nil {
		t.Fatal(err)
	}
	update, err := cell.FromBOC(block.StateUpdate.ToBOC())
	if err != nil {
		t.Fatal(err)
	}
	state, err := cell.ApplyMerkleUpdate(req.Previous.State.WithoutTrace(), update)
	if err != nil {
		t.Fatal(err)
	}
	reopened := *candidate
	reopened.BlockBOC = bytes.Clone(candidate.BlockBOC)
	reopened.CollatedData = bytes.Clone(candidate.CollatedData)
	reopened.State = state
	reopened.StateUpdate = update
	reopened.StorageStats = nil
	reopened.Externals = nil

	verification := shardVerificationRequest(req, &reopened)
	verification.Previous.State = narrowedStateRoot(t, req.Previous.State)
	verification.Neighbors = collatedNeighborQueues(t, req, &reopened)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())

	return verification
}
