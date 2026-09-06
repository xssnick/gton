package validator

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
)

func TestCandidateLineageRequiresExactNotarization(t *testing.T) {
	storage := newRuntimeTestStorage()
	candidates := newResolverForTest(storage, nil, 1, simplex.DefaultParams())
	t.Cleanup(candidates.close)
	config, key := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	artifact := runtimeBlockArtifact(t, config, key, 7, simplex.Genesis(), 1, 0xaa)
	wire, _, err := candidates.codec.encodeForBroadcast(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if err := candidates.stage(artifact, wire); err != nil {
		t.Fatal(err)
	}
	id := artifact.Candidate.ID
	if _, err := candidates.lineage(context.Background(), id); !errors.Is(err, ErrCandidateUnavailable) {
		t.Fatalf("uncertified ancestry error = %v, want unavailable", err)
	}
	candidates.observeNotarization(id, simplex.VerifiedCertificate{})
	if _, err := candidates.lineage(context.Background(), id); !errors.Is(err, ErrCandidateUnavailable) {
		t.Fatalf("zero-certificate ancestry error = %v, want unavailable", err)
	}
	other := id
	other.Hash[0] ^= 1
	candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(other)))
	if _, err := candidates.lineage(context.Background(), id); err == nil {
		t.Fatal("ancestry accepted a certificate for another candidate")
	}
	candidates.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
	lineage, err := candidates.lineage(context.Background(), id)
	if err != nil || !lineage.matches(artifact.Candidate.Block) {
		t.Fatalf("certified ancestry = %+v, %v", lineage, err)
	}
	canceled, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := candidates.lineage(canceled, id); !errors.Is(err, context.Canceled) {
		t.Fatalf("cached ancestry ignored cancellation: %v", err)
	}
	candidates.close()
	if _, err := candidates.lineage(context.Background(), id); !errors.Is(err, ErrResolverClosed) {
		t.Fatalf("closed ancestry error = %v", err)
	}
}

// The hash slices in a decoded identity can share the canonical wire. A copy
// of the whole BlockIDExt would accidentally retain that large backing array.
func TestCandidateLineageOwnsOnlyFixedIdentity(t *testing.T) {
	resolver, candidates, artifacts := lineageResolverForTest(t, false)
	t.Cleanup(resolver.close)
	t.Cleanup(candidates.close)
	artifact := artifacts[2]
	lineage, err := candidates.lineage(context.Background(), artifact.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	want := *lineage
	artifact.Candidate.Block.RootHash[0] ^= 1
	artifact.Candidate.Block.FileHash[0] ^= 1
	artifact.Candidate.Parent = simplex.Genesis()
	if *lineage != want {
		t.Fatal("compact ancestry aliases the admitted artifact")
	}
	candidates.mu.Lock()
	candidates.releasePayloadLocked(artifact.Candidate.ID, candidates.entries[artifact.Candidate.ID])
	candidates.mu.Unlock()
	if got, err := candidates.lineage(context.Background(), artifact.Candidate.ID); err != nil || got != lineage {
		t.Fatalf("eviction replaced immutable ancestry: %p, %v", got, err)
	}
}

func TestCandidateLineageRestoresAndAuthenticatesOnce(t *testing.T) {
	storage, candidates, artifacts := storedLineageForTest(t, 8)
	candidates.close()
	ids := make([]simplex.CandidateID, len(artifacts))
	for i, artifact := range artifacts {
		ids[i] = artifact.Candidate.ID
	}
	restored := newRestoredResolverForTest(storage, nil, 1, simplex.DefaultParams(), StoredSessionState{CandidateIDs: ids})
	t.Cleanup(restored.close)
	for _, id := range ids {
		restored.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
	}
	for i := len(ids) - 1; i >= 0; i-- {
		if got := restored.lineageResidency(ids[i]); got != LineageStepStorage {
			t.Fatalf("cold source = %v, want storage", got)
		}
		lineage, err := restored.lineage(context.Background(), ids[i])
		if err != nil || lineage.parent != artifacts[i].Candidate.Parent || !lineage.matches(artifacts[i].Candidate.Block) {
			t.Fatalf("restored lineage at %d = %+v, %v", i, lineage, err)
		}
	}
	if storage.loadCount() != len(ids) {
		t.Fatalf("cold reads = %d, want %d", storage.loadCount(), len(ids))
	}
	releaseLineagePayloads(restored, artifacts)
	for _, id := range ids {
		if got := restored.lineageResidency(id); got != LineageStepMemory {
			t.Fatalf("evicted source = %v, want memory", got)
		}
		if _, err := restored.lineage(context.Background(), id); err != nil {
			t.Fatal(err)
		}
	}
	if storage.loadCount() != len(ids) || restored.cacheStats().Candidates != 0 {
		t.Fatalf("known lineage restored payloads: reads %d, cache %+v", storage.loadCount(), restored.cacheStats())
	}
}

func TestCandidateLineageRejectsMismatchedDurableIdentity(t *testing.T) {
	storage, candidates, artifacts := storedLineageForTest(t, 2)
	candidates.close()
	id := artifacts[0].Candidate.ID
	other, err := storage.Candidate(context.Background(), SessionStorageID{}, artifacts[1].Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	// The record claims the requested ID, but its signed wire belongs elsewhere.
	other.ID = id
	storage.mu.Lock()
	storage.candidates[validatorTestNamespaceOf(SessionStorageID{})][id] = other
	storage.mu.Unlock()
	restored := newRestoredResolverForTest(storage, nil, 1, simplex.DefaultParams(), StoredSessionState{CandidateIDs: []simplex.CandidateID{id}})
	t.Cleanup(restored.close)
	restored.observeNotarization(id, resolverTestSeal(t, simplex.NotarizeVote(id)))
	if _, err := restored.lineage(context.Background(), id); err == nil {
		t.Fatal("wrong signed candidate installed ancestry under a durable index ID")
	}
	restored.mu.Lock()
	lineage := restored.entries[id].lineage
	restored.mu.Unlock()
	if lineage != nil {
		t.Fatal("failed durable authentication left usable ancestry")
	}
}

func TestCandidateLineageConcurrentEvictionAndReads(t *testing.T) {
	storage, candidates, artifacts := storedLineageForTest(t, 16)
	t.Cleanup(candidates.close)
	var wg sync.WaitGroup
	for range 4 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range 30 {
				for _, artifact := range artifacts {
					lineage, err := candidates.lineage(context.Background(), artifact.Candidate.ID)
					if err != nil || !lineage.matches(artifact.Candidate.Block) {
						t.Errorf("concurrent lineage = %+v, %v", lineage, err)
						return
					}
				}
			}
		}()
	}
	releaseLineagePayloads(candidates, artifacts)
	wg.Wait()
	if storage.loadCount() != 0 || candidates.cacheStats().Candidates != 0 {
		t.Fatalf("evicted lineage read payloads: reads %d, cache %+v", storage.loadCount(), candidates.cacheStats())
	}
}

func storedLineageForTest(t testing.TB, length int) (*runtimeTestStorage, *candidateResolver, []*CandidateArtifact) {
	t.Helper()
	storage := newRuntimeTestStorage()
	candidates := newResolverForTest(storage, nil, 1, simplex.DefaultParams())
	config, key := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	artifacts := make([]*CandidateArtifact, length)
	parent := simplex.Genesis()
	for i := range artifacts {
		artifact := runtimeBlockArtifact(t, config, key, uint32(i), parent, uint32(i)+1, uint64(i%256))
		wire, _, err := candidates.codec.encodeForBroadcast(artifact)
		if err != nil {
			t.Fatal(err)
		}
		if err := candidates.stage(artifact, wire); err != nil {
			t.Fatal(err)
		}
		if err := candidates.store(context.Background(), artifact.Candidate.ID); err != nil {
			t.Fatal(err)
		}
		candidates.observeNotarization(artifact.Candidate.ID, resolverTestSeal(t, simplex.NotarizeVote(artifact.Candidate.ID)))
		artifacts[i] = artifact
		parent = simplex.Parent(artifact.Candidate.ID)
	}
	return storage, candidates, artifacts
}

func releaseLineagePayloads(candidates *candidateResolver, artifacts []*CandidateArtifact) {
	candidates.mu.Lock()
	defer candidates.mu.Unlock()
	for _, artifact := range artifacts {
		id := artifact.Candidate.ID
		candidates.releasePayloadLocked(id, candidates.entries[id])
	}
}

// Both variants walk the same signed, durable chain after each payload sweep.
// The baseline is the former guard's resolve-and-read-parent path. Including
// the same sweep in both measurements avoids stopping the clock per tiny walk.
func BenchmarkReleasedCandidateLineage(b *testing.B) {
	for _, length := range []int{16, 128} {
		for _, compact := range []bool{false, true} {
			name := "payload"
			if compact {
				name = "compact"
			}
			b.Run(fmt.Sprintf("%d/%s", length, name), func(b *testing.B) {
				storage, candidates, artifacts := storedLineageForTest(b, length)
				defer candidates.close()
				releaseLineagePayloads(candidates, artifacts)
				anchor := artifacts[0].Candidate.Block
				base := simplex.Parent(artifacts[len(artifacts)-1].Candidate.ID)
				b.ReportAllocs()
				iterations := 0
				for b.Loop() {
					iterations++
					parent := base
					for parent.Exists {
						if compact {
							lineage, err := candidates.lineage(context.Background(), parent.ID)
							if err != nil {
								b.Fatal(err)
							}
							if lineage.matches(anchor) {
								break
							}
							parent = lineage.parent
						} else {
							resolution, err := candidates.resolve(context.Background(), parent.ID)
							if err != nil {
								b.Fatal(err)
							}
							if sameBlockID(resolution.Candidate.Candidate.Block, anchor) {
								break
							}
							parent = resolution.Candidate.Candidate.Parent
						}
					}
					releaseLineagePayloads(candidates, artifacts)
				}
				b.ReportMetric(float64(storage.loadCount())/float64(iterations), "payload_reads/op")
			})
		}
	}
}

var benchmarkCandidateLineage *candidateLineage

func BenchmarkCandidateLineageRetention(b *testing.B) {
	config, key := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	artifact := runtimeBlockArtifact(b, config, key, 7, simplex.Genesis(), 1, 0xaa)
	b.ReportAllocs()
	for b.Loop() {
		entry := candidateEntry{}
		entry.retainLineage(artifact)
		benchmarkCandidateLineage = entry.lineage
	}
}
