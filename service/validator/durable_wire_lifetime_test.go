package validator

import (
	"bytes"
	"context"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
)

// TestFinalizedCandidateStillNeedsItsCollatedData is a guard, not a test of new
// behaviour. It exists so a later pass does not re-derive the appealing but
// wrong conclusion that a finalized candidate's collated half is dead weight
// and can be dropped to halve the durable record.
//
// The block half becomes reachable through ordinary block storage once the
// candidate is accepted, so it is tempting to read the collated half as
// validation-only — and nobody re-validates a finalized block. But the state
// resolver reads it on every finalized parent it resolves, to recover the
// exact millisecond generation time that the block header does not carry, and
// it does so before it ever looks at whether the candidate is finalized. The
// reference does the same thing in the same order, calling
// get_candidate_gen_utime_exact — which deserializes collated_data — ahead of
// its own is_finalized branch. Dropping the collated half would break parent
// resolution for any finalized candidate still inside the lineage floor, and
// would leave us unable to answer a lagging peer's candidate request for a
// block the reference can always serve.
func TestFinalizedCandidateStillNeedsItsCollatedData(t *testing.T) {
	storage := newRuntimeTestStorage()
	resolver := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()

	config, privateKey := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	slot := uint32(2)
	artifact := runtimeOrdinaryArtifact(t, config, privateKey, slot, simplex.Genesis())
	wire, err := simplex.SerializeCandidate(artifact.Candidate, artifact.BlockBOC, artifact.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	want, err := candidateGenUtime(artifact.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	collated := append([]byte(nil), artifact.CollatedData...)

	if err = resolver.stage(artifact, wire); err != nil {
		t.Fatal(err)
	}
	if err = resolver.store(context.Background(), artifact.Candidate.ID); err != nil {
		t.Fatal(err)
	}
	resolver.observeNotarization(
		artifact.Candidate.ID,
		resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)),
	)

	// Finalize past the retained margin, which is what releases the in-memory
	// payload and leaves the durable record as the only copy.
	resolver.notifyFinalized(slot+candidateCacheRetainedSlots+1, retentionFloorNone)
	resolver.mu.Lock()
	retained := resolver.entries[artifact.Candidate.ID].candidate
	resolver.mu.Unlock()
	if retained != nil {
		t.Fatal("finalization did not release the in-memory payload")
	}

	resolution, err := resolver.resolve(context.Background(), artifact.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if storage.loadCount() == 0 {
		t.Fatal("resolving a finalized candidate did not read the durable wire")
	}
	if !bytes.Equal(resolution.Candidate.CollatedData, collated) {
		t.Fatal("the durable record no longer carries the finalized candidate's collated data")
	}
	got, err := candidateGenUtime(resolution.Candidate.CollatedData)
	if err != nil {
		t.Fatalf("exact generation time is unrecoverable after finalization: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("generation time after finalization = %v, want %v", got, want)
	}
}
