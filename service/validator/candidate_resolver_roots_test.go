package validator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"fmt"
	"runtime"
	"testing"
	"weak"

	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type rootCacheCandidate struct {
	artifact *CandidateArtifact
	lazy     *lazyCandidateWire
	wire     []byte
}

func rootCacheCandidateForTest(t testing.TB, r *candidateResolver, slot uint32) rootCacheCandidate {
	t.Helper()
	config, key := runtimeTestConfig(resolverTestSessionTag, &runtimeTestJournal{})
	original := runtimeOrdinaryArtifact(t, config, key, slot, simplex.Genesis())
	wire, err := simplex.SerializeCandidate(original.Candidate, original.BlockBOC, original.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	artifact, lazy, err := r.codec.decodeDeferred(wire, &original.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	return rootCacheCandidate{artifact: artifact, lazy: lazy, wire: wire}
}

func stageRootCacheCandidate(t testing.TB, r *candidateResolver, candidate rootCacheCandidate) {
	t.Helper()
	if err := r.stageDeferred(candidate.artifact, candidate.lazy); err != nil {
		t.Fatal(err)
	}
}

func assertRootCacheAccounting(t testing.TB, r *candidateResolver) {
	t.Helper()
	if running, recomputed := r.cacheProjection(), r.cacheStats(); running != recomputed {
		t.Fatalf("running cache %+v differs from entries %+v", running, recomputed)
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	var total int64
	var previous *candidateEntry
	seen := make(map[*candidateEntry]bool)
	for entry := r.decodedHead; entry != nil; entry = entry.decodedNext {
		if seen[entry] || entry.decodedPrev != previous || entry.decodedRootBytes <= 0 {
			t.Fatal("invalid decoded cache list")
		}
		seen[entry] = true
		total += entry.decodedRootBytes
		previous = entry
	}
	if previous != r.decodedTail || total != r.cache.DecodedBytes {
		t.Fatal("decoded cache list disagrees with byte charge")
	}
}

func TestCandidateDecodedCacheDemotesWithoutFinalization(t *testing.T) {
	r := newResolverForTest(newRuntimeTestStorage(), nil, 1, simplex.DefaultParams())
	defer r.close()
	first := rootCacheCandidateForTest(t, r, 0)
	second := rootCacheCandidateForTest(t, r, 1)
	r.decodedBudget = max(first.artifact.decodedRootBytes, second.artifact.decodedRootBytes)
	stageRootCacheCandidate(t, r, first)
	stageRootCacheCandidate(t, r, second)

	entry := r.entries[first.artifact.Candidate.ID]
	if first.lazy.roots.Load() != nil || entry.validationRoots != nil {
		t.Fatal("cold candidate kept a decoded graph")
	}
	if first.lazy.wire != nil || entry.candidate == nil || entry.lineage == nil {
		t.Fatal("demotion built wire or discarded payload/lineage")
	}
	if second.lazy.roots.Load() == nil {
		t.Fatal("newest candidate lost its fast path")
	}
	if stats := r.cacheProjection(); stats.DecodedDemotions != 1 || stats.Candidates != 2 {
		t.Fatalf("unexpected cache stats: %+v", stats)
	}
	assertRootCacheAccounting(t, r)

	response, err := r.response(context.Background(), simplex.PeerID{}, CandidateRequest{
		SessionID: r.sessionID, ID: first.artifact.Candidate.ID, WantCandidate: true,
	})
	if err != nil || !bytes.Equal(response.CandidateWire, first.wire) {
		t.Fatalf("cold request changed canonical wire: %v", err)
	}
	if err := r.store(context.Background(), first.artifact.Candidate.ID); err != nil {
		t.Fatal(err)
	}
	if !entry.durable || entry.wireHash != sha256.Sum256(first.wire) {
		t.Fatal("cold candidate was not stored under the original canonical identity")
	}
	assertRootCacheAccounting(t, r)
}

func TestCandidateDecodedCachePreservesBorrowedRoots(t *testing.T) {
	r := newResolverForTest(newRuntimeTestStorage(), nil, 1, simplex.DefaultParams())
	defer r.close()
	r.validateCandidates = true
	first := rootCacheCandidateForTest(t, r, 0)
	second := rootCacheCandidateForTest(t, r, 1)
	r.decodedBudget = first.artifact.decodedRootBytes
	stageRootCacheCandidate(t, r, first)
	borrowed, err := r.candidate(context.Background(), first.artifact.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	root := borrowed.validationRoots.block
	stageRootCacheCandidate(t, r, second)
	if first.lazy.roots.Load() != nil {
		t.Fatal("wire cache retained the borrowed graph")
	}
	if borrowed.validationRoots.block != root || !bytes.Equal(root.Hash(), borrowed.Candidate.Block.RootHash) {
		t.Fatal("demotion invalidated the active validation borrower")
	}
	if len(borrowed.validationRoots.collated) == 0 {
		t.Fatal("demotion discarded the borrower's proof")
	}
	assertRootCacheAccounting(t, r)
}

func TestCandidateDecodedCacheProtectsClaimsAndOversizedMRU(t *testing.T) {
	for _, claim := range []string{"load", "store", "store build"} {
		t.Run(claim, func(t *testing.T) {
			r := newResolverForTest(newRuntimeTestStorage(), nil, 1, simplex.DefaultParams())
			defer r.close()
			r.decodedBudget = 1
			first := rootCacheCandidateForTest(t, r, 0)
			second := rootCacheCandidateForTest(t, r, 1)
			stageRootCacheCandidate(t, r, first)
			if first.lazy.roots.Load() == nil {
				t.Fatal("oversized newest candidate immediately lost its decode")
			}
			entry := r.entries[first.artifact.Candidate.ID]
			switch claim {
			case "load":
				entry.load = &resolverFlight{}
			case "store":
				entry.store = &resolverFlight{}
			case "store build":
				entry.storeBuilds = 1
			}
			stageRootCacheCandidate(t, r, second)
			if first.lazy.roots.Load() == nil || second.lazy.roots.Load() == nil {
				t.Fatal("an in-flight claim or newest candidate was demoted")
			}
			entry.load, entry.store, entry.storeBuilds = nil, nil, 0
			r.mu.Lock()
			r.trimDecodedRootsLocked()
			r.mu.Unlock()
			if first.lazy.roots.Load() != nil || second.lazy.roots.Load() == nil {
				t.Fatal("finished claim did not become evictable")
			}
			assertRootCacheAccounting(t, r)
		})
	}
}

func TestCandidateDecodedCacheAccountingAcrossHandoffAndRelease(t *testing.T) {
	r := newResolverForTest(newRuntimeTestStorage(), nil, 1, simplex.DefaultParams())
	defer r.close()
	first := rootCacheCandidateForTest(t, r, 0)
	second := rootCacheCandidateForTest(t, r, 1)
	stageRootCacheCandidate(t, r, first)
	stageRootCacheCandidate(t, r, second)
	if _, err := r.candidate(context.Background(), first.artifact.Candidate.ID); err != nil {
		t.Fatal(err)
	}
	if r.decodedHead != r.entries[first.artifact.Candidate.ID] {
		t.Fatal("hot candidate was not promoted")
	}
	if err := r.store(context.Background(), first.artifact.Candidate.ID); err != nil {
		t.Fatal(err)
	}
	assertRootCacheAccounting(t, r)
	if got := r.entries[first.artifact.Candidate.ID].decodedRootBytes; got != 0 {
		t.Fatalf("consumed roots still charged %d bytes", got)
	}
	r.mu.Lock()
	r.releasePayloadLocked(second.artifact.Candidate.ID, r.entries[second.artifact.Candidate.ID])
	r.mu.Unlock()
	assertRootCacheAccounting(t, r)
	if r.cacheProjection().DecodedBytes != 0 {
		t.Fatal("released roots remained in the cache budget")
	}
}

type decodedGraphWeakRoots struct {
	block    weak.Pointer[cell.Cell]
	collated weak.Pointer[cell.Cell]
}

func stageWeakRootForTest(t testing.TB, r *candidateResolver, slot uint32) decodedGraphWeakRoots {
	t.Helper()
	candidate := rootCacheCandidateForTest(t, r, slot)
	roots := candidate.lazy.roots.Load()
	weakRoots := decodedGraphWeakRoots{
		block: weak.Make(roots.block), collated: weak.Make(roots.collated[0]),
	}
	stageRootCacheCandidate(t, r, candidate)
	return weakRoots
}

func TestCandidateDecodedCacheActuallyReleasesArena(t *testing.T) {
	for protocol := uint8(0); protocol <= simplex.MaxProtocolVersion; protocol++ {
		t.Run(fmt.Sprintf("protocol_%d", protocol), func(t *testing.T) {
			r := newResolverForTest(newRuntimeTestStorage(), nil, 1, simplex.DefaultParams())
			defer r.close()
			r.codec.protocolVersion = protocol
			r.decodedBudget = 1
			first := stageWeakRootForTest(t, r, 0)
			second := stageWeakRootForTest(t, r, 1)
			runtime.GC()
			if first.block.Value() != nil || first.collated.Value() != nil {
				t.Fatal("cold candidate's parsed arena is still reachable")
			}
			if second.block.Value() == nil || second.collated.Value() == nil {
				t.Fatal("hot candidate's parsed arena was lost")
			}
			r.close()
			runtime.GC()
			if second.block.Value() != nil || second.collated.Value() != nil {
				t.Fatal("closed resolver kept its parsed cache")
			}
			runtime.KeepAlive(r)
		})
	}
}

func TestCandidateDecodedBOCChargeUsesUnclampedCellCount(t *testing.T) {
	// A policy weight must not reuse the serializer hint's 2^18 presize cap.
	boc := []byte{0xb5, 0xee, 0x9c, 0x72, 3, 4, 0x08, 0x00, 0x00}
	want := int64(1<<19)*256 + int64(len(boc))
	if got := candidateDecodedBOCBytes(boc); got != want {
		t.Fatalf("weight=%d want=%d", got, want)
	}
}
