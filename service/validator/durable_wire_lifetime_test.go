package validator

import (
	"bytes"
	"context"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
)

// TestDurableCandidateRestoresGenerationTimeProvenance guards the recovery
// path: a released artifact keeps no process-local scalar, so its durable wire
// is decoded once and the roots from that decode must seed the replacement.
//
// CollatedData remains part of the canonical candidate served to peers, but
// state resolution must use the recovered scalar instead of decoding those
// bytes a second time.
func TestDurableCandidateRestoresGenerationTimeProvenance(t *testing.T) {
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
	want, err := artifact.generationTime()
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
	if !resolution.Candidate.generationTimeKnown {
		t.Fatal("durable decode did not restore generation time provenance")
	}
	withoutBOC := *resolution.Candidate
	withoutBOC.CollatedData = []byte("not a BOC")
	got, err := withoutBOC.generationTime()
	if err != nil {
		t.Fatalf("recovered generation time re-decoded CollatedData: %v", err)
	}
	if !got.Equal(want) {
		t.Fatalf("generation time after finalization = %v, want %v", got, want)
	}
}
