package collator

import (
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/tvm"
)

func TestSemanticVerifierAcceptsBuilderShardCandidate(t *testing.T) {
	req := emptyCandidateRequest(t)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())

	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify Builder shard candidate with production semantics: %v", err)
	}
}

func TestSemanticVerifierAcceptsBuilderShardTransaction(t *testing.T) {
	replay, _ := semanticOrdinaryReplay(t)
	verification := ShardVerificationRequest{
		Previous:           replay.transition.Previous,
		Previous2:          replay.transition.Previous2,
		Masterchain:        *replay.transition.Masterchain,
		Neighbors:          replay.transition.Neighbors,
		NeighborShardEndLT: replay.transition.NeighborShardEndLT,
		Semantics:          NewSemanticVerifier(tvm.NewTVM()),
		Candidate:          replay.transition.Candidate,
	}

	if err := VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify Builder transaction candidate with production semantics: %v", err)
	}
}

func TestSemanticVerifierAcceptsBuilderMasterCandidate(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	verification := MasterVerificationRequest{
		Previous:           fixture.request.Previous,
		Config:             fixture.request.Config,
		Groups:             fixture.request.Groups,
		ShardTops:          fixture.request.ShardTops,
		Neighbors:          fixture.request.Neighbors,
		NeighborShardEndLT: fixture.request.NeighborShardEndLT,
		Semantics:          NewSemanticVerifier(tvm.NewTVM()),
		Candidate:          candidate,
	}

	if err = VerifyMasterCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify Builder masterchain candidate with production semantics: %v", err)
	}
}

func TestSemanticVerifierAcceptsBuilderDeferredTransit(t *testing.T) {
	req := emptyCandidateRequest(t)
	dispatch := nonEmptyDispatchQueue(t, req.Header.GenUtime-1, requestStartLT(t, req)-10)
	req.Previous.State = previousStateWithDispatchQueue(t, req.Previous.State, dispatch)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.DispatchedMessages != 1 || candidate.Stats.EnqueuedMessages != 1 {
		t.Fatalf("dispatch/enqueue stats = %d/%d, want 1/1", candidate.Stats.DispatchedMessages, candidate.Stats.EnqueuedMessages)
	}
	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())

	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify Builder deferred transit with production semantics: %v", err)
	}
}
