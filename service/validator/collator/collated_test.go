package collator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

type staticFullCollatedProofProvider struct {
	roots  []*cell.Cell
	err    error
	called int
}

func (p *staticFullCollatedProofProvider) BuildFullCollatedProofs(
	_ context.Context,
	_ FullCollatedProofRequest,
) (FullCollatedProofs, error) {
	p.called++
	return FullCollatedProofs{Roots: p.roots, ScanExhausted: true}, p.err
}

type recordingCandidateTransitionVerifier struct {
	transition CandidateTransition
}

func (v *recordingCandidateTransitionVerifier) VerifyCandidateTransition(
	_ context.Context,
	transition CandidateTransition,
) error {
	v.transition = transition
	return nil
}

func TestVerifyCollatedRootsAccountStorageProofs(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0x77, 8).EndCell()
	proof, err := cell.CreateMerkleProof(root)
	if err != nil {
		t.Fatal(err)
	}
	wrapper := cell.BeginCell().
		MustStoreUInt(accountStorageDictProofTag, 32).
		MustStoreRef(proof).
		EndCell()
	consensus := collatedTestConsensus(100)

	verified, err := verifyCollatedRoots([]*cell.Cell{consensus, proof, wrapper}, 100)
	if err != nil {
		t.Fatal(err)
	}
	if !verified.full || len(verified.virtualRoots) != 1 || len(verified.accountStorage) != 1 {
		t.Fatalf("unexpected full collated classification: %+v", verified)
	}

	_, err = verifyCollatedRoots([]*cell.Cell{consensus, wrapper, wrapper}, 100)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "duplicate account storage proof") {
		t.Fatalf("duplicate account storage proof error = %v", err)
	}

	malformed := cell.BeginCell().
		MustStoreUInt(accountStorageDictProofTag, 32).
		MustStoreBoolBit(true).
		MustStoreRef(proof).
		EndCell()
	_, err = verifyCollatedRoots([]*cell.Cell{consensus, malformed}, 100)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "malformed account storage proof") {
		t.Fatalf("malformed account storage proof error = %v", err)
	}
}

func TestVerifyCollatedRootsAllowsConsensusFlags(t *testing.T) {
	consensus := cell.BeginCell().
		MustStoreUInt(consensusExtraDataTag, 32).
		MustStoreUInt(0xa5a5a5a5, 32).
		MustStoreUInt(100_999, 64).
		EndCell()

	if _, err := verifyCollatedRoots([]*cell.Cell{consensus}, 100); err != nil {
		t.Fatalf("valid non-zero consensus flags rejected: %v", err)
	}
}

func TestCanonicalCollatedProofsRejectsNil(t *testing.T) {
	if _, err := canonicalCollatedProofs([]*cell.Cell{nil}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("nil provider proof error = %v, want ErrInvalidInput", err)
	}
}

func TestVerifyShardCandidateBindsFullCollatedPredecessorProofs(t *testing.T) {
	req := advanceCandidateRequest(t, emptyCandidateRequest(t))
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	verification := shardVerificationRequest(req, candidate)
	recorder := new(recordingCandidateTransitionVerifier)
	verification.Semantics = recorder
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify full collated candidate: %v", err)
	}
	if !recorder.transition.FullCollatedData {
		t.Fatal("semantic verifier was not told that the candidate uses full collated data")
	}
	if recorder.transition.CollatedProofs == nil || !recorder.transition.CollatedProofs.Full() {
		t.Fatal("semantic verifier did not receive the verified collated proof view")
	}
	provenState, err := recorder.transition.CollatedProofs.StateRoot(req.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}
	if provenState.HashKey(0) != req.Previous.State.HashKey(0) {
		t.Fatal("verified proof view returned another predecessor state")
	}
	if _, err = recorder.transition.CollatedProofs.AccountStorageRoot(cell.Hash{1}); !errors.Is(err, ErrCollatedRootNotFound) {
		t.Fatalf("missing account storage root error = %v", err)
	}

	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) != 3 {
		t.Fatalf("collated roots = %d, want consensus, block proof, state proof", len(roots))
	}
	tests := []struct {
		name    string
		roots   []*cell.Cell
		wantErr string
	}{
		{name: "missing block proof", roots: []*cell.Cell{roots[0], roots[2]}, wantErr: "block proof is absent"},
		{name: "missing state proof", roots: []*cell.Cell{roots[0], roots[1]}, wantErr: "state proof is absent"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tampered := cloneVerificationCandidate(candidate)
			rewriteVerificationCollatedData(t, tampered, test.roots...)
			changed := verification
			changed.Candidate = tampered
			err := VerifyShardCandidate(context.Background(), changed)
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("verification error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestFullCollatedDataBindsMasterchainProcessedFrontier(t *testing.T) {
	req := advanceCandidateRequest(t, emptyCandidateRequest(t))
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	missingQueue := req
	missingQueue.Masterchain.OutMsgQueueInfo = nil
	if _, err := testBuilder().BuildShard(context.Background(), missingQueue); !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "authenticated output queue") {
		t.Fatalf("missing masterchain queue error = %v", err)
	}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	forged := req
	forged.Neighbors = append([]Neighbor(nil), req.Neighbors...)
	for i := range forged.Neighbors {
		neighbor := &forged.Neighbors[i]
		if neighbor.Block.Workchain != address.MasterchainID {
			continue
		}
		neighbor.Processed = []tlb.ProcessedUptoRecord{{
			ShardPrefix: uint64(neighbor.Block.Shard),
			MCSeqno:     req.Masterchain.ID.SeqNo,
			LastMsgLT:   1,
			LastMsgHash: processedInfinityHash,
		}}
	}
	if err = VerifyShardCandidate(context.Background(), shardVerificationRequest(forged, candidate)); !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "processed frontier") {
		t.Fatalf("forged masterchain frontier error = %v", err)
	}
}

func TestFullCollatedNeighborSetOmitsMasterchainZerostate(t *testing.T) {
	target := tlb.ShardIdent{WorkchainID: 0}
	previous := testBlockID(0, int64(target.GetShardID()), 0, 0x31)
	masterchain := testBlockID(address.MasterchainID, -1<<63, 0, 0x32)
	context := MasterchainContext{
		ID: masterchain,
		Groups: &groups.Snapshot{Active: []groups.Session{{
			Registered: []groups.ShardDescription{{
				Shard: groups.ShardID{Workchain: 0, Shard: int64(target.GetShardID())},
				Block: previous,
			}},
		}}},
	}
	neighbors := []Neighbor{{
		Block: previous,
		Shard: msgpool.ShardIdent{Workchain: 0, Shard: uint64(target.GetShardID())},
	}}

	if err := verifyFullCollatedNeighborSet(context, target, neighbors); err != nil {
		t.Fatalf("masterchain zerostate must not be required as a neighbor: %v", err)
	}
}

func TestBuildShardRequiresAndValidatesNeighborProofProvider(t *testing.T) {
	parentRequest := emptyCandidateRequest(t)
	parentSession := &parentRequest.Masterchain.Groups.Active[0]
	parentSession.Registered[0].FSM = groups.ShardFSM{
		Kind:     groups.ShardFSMSplit,
		UTime:    parentRequest.Header.GenUtime,
		Interval: 10,
	}
	parentRequest.BeforeSplit = true
	parentCandidate, err := testBuilder().BuildShard(context.Background(), parentRequest)
	if err != nil {
		t.Fatalf("build parent before split: %v", err)
	}
	parentBlock := candidateBlock(t, parentCandidate)
	parentQueueSize := parentCandidate.Stats.OutQueueSize
	parent := PreviousBlock{
		ID:           parentCandidate.ID,
		Block:        parentBlock,
		State:        parentCandidate.State,
		OutQueueSize: &parentQueueSize,
	}

	childRequest := parentRequest
	childRequest.Shard = groups.ShardID{
		Workchain: parentRequest.Shard.Workchain,
		Shard:     mustPredecessorChild(t, parentRequest.Shard.Shard, true),
	}
	childRequest.Previous = parent
	childRequest.BeforeSplit = false
	childRequest.Header.GenUtime++
	childRequest.Header.GenUtimeMS = uint64(childRequest.Header.GenUtime) * 1_000
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
	childCandidate, err := testBuilder().BuildShard(context.Background(), childRequest)
	if err != nil {
		t.Fatalf("build first child block: %v", err)
	}

	childQueueSize := childCandidate.Stats.OutQueueSize
	req := childRequest
	req.Previous = PreviousBlock{
		ID:           childCandidate.ID,
		Block:        candidateBlock(t, childCandidate),
		State:        childCandidate.State,
		OutQueueSize: &childQueueSize,
	}
	req.Header.GenUtime++
	req.Header.GenUtimeMS = uint64(req.Header.GenUtime) * 1_000
	req.Masterchain.Config.capabilities |= capFullCollatedData
	parentNeighbor := fullCollatedTestNeighbor(t, parent)
	req.Neighbors = append([]Neighbor{parentNeighbor}, masterchainNeighbor(req)...)
	req.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return parentNeighbor.EndLT }

	blockProof, err := cell.CreateMerkleProof(parent.Block)
	if err != nil {
		t.Fatal(err)
	}
	stateProof, err := cell.CreateMerkleProof(parent.State)
	if err != nil {
		t.Fatal(err)
	}

	if _, err = testBuilder().BuildShard(context.Background(), req); !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "neighbor 0") {
		t.Fatalf("missing neighbor proof provider error = %v", err)
	}

	provider := &staticFullCollatedProofProvider{roots: []*cell.Cell{stateProof, blockProof}}
	req.FullCollatedProofs = provider
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("build with neighbor proof provider: %v", err)
	}
	if provider.called != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.called)
	}
	if err = VerifyShardCandidate(context.Background(), shardVerificationRequest(req, candidate)); err != nil {
		t.Fatalf("verify candidate with supplied neighbor proofs: %v", err)
	}

	withoutCapability := emptyCandidateRequest(t)
	withoutCapability.FullCollatedProofs = provider
	if _, err = testBuilder().BuildShard(context.Background(), withoutCapability); !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "without capability") {
		t.Fatalf("provider without capability error = %v", err)
	}
}

func fullCollatedTestNeighbor(t *testing.T, previous PreviousBlock) Neighbor {
	t.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, previous.State); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, uint64(previous.ID.Shard))
	if err != nil {
		t.Fatal(err)
	}

	return Neighbor{
		Block: previous.ID,
		Shard: msgpool.ShardIdent{
			Workchain: previous.ID.Workchain,
			Shard:     uint64(previous.ID.Shard),
		},
		EndLT:     state.GenLT,
		Processed: records,
	}
}

func collatedTestConsensus(genUtime uint32) *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(consensusExtraDataTag, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(uint64(genUtime)*1_000, 64).
		EndCell()
}
