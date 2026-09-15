package validator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"reflect"
	"strings"
	"testing"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

type acceptanceDigestCase struct {
	name     string
	digested bool
	resident bool
	rehashed bool
}

// TestDurableCandidateLoadHandsOnNoParsedRoots pins what a read of the durable
// copy leaves behind. Finalization, lineage, replay and state reconstruction
// read it back and none of them claims parsed roots, so roots handed on would
// sit on the entry outside cache.Bytes until the payload is released again. The
// validation or observer successor that does reach a reloaded candidate gets
// the canonical bytes and decodes them itself.
func TestDurableCandidateLoadHandsOnNoParsedRoots(t *testing.T) {
	for _, validate := range []bool{false, true} {
		name := "observer"
		if validate {
			name = "validator"
		}
		t.Run(name, func(t *testing.T) {
			storage := newRuntimeTestStorage()
			seed := newResolverForTest(
				storage,
				&retryCandidateProvider{called: make(chan struct{}, 1)},
				1,
				simplex.DefaultParams(),
			)
			stored := stageStoredRealCandidateForTest(t, seed, 3)
			seed.close()

			id := stored.Candidate.ID
			resolver := newRestoredResolverForTest(
				storage,
				&retryCandidateProvider{called: make(chan struct{}, 1)},
				1,
				simplex.DefaultParams(),
				StoredSessionState{CandidateIDs: []simplex.CandidateID{id}},
			)
			defer resolver.close()
			resolver.validateCandidates = validate

			loaded, err := resolver.loadCandidate(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if storage.loadCount() != 1 {
				t.Fatalf("storage reads = %d, want the one durable load", storage.loadCount())
			}

			resolver.mu.Lock()
			entry := resolver.entries[id]
			roots := entry.validationRoots
			retained := entry.candidate
			held := resolver.cache.Bytes
			wire := len(entry.wire)
			resolver.mu.Unlock()
			if roots != nil {
				t.Fatal("a durable reload left parsed roots on the entry, outside the retention budget")
			}
			if retained != loaded || retained.validationRoots != nil || retained.preparedBlock != nil {
				t.Fatal("the reloaded artifact the resolver retains carries a parsed capsule")
			}
			if held != int64(wire+len(loaded.BlockBOC)+len(loaded.CollatedData)) {
				t.Fatalf("cache bytes = %d, want the wire and both BOCs of the one reloaded payload", held)
			}

			validated, err := resolver.candidate(context.Background(), id)
			if err != nil {
				t.Fatal(err)
			}
			if validated.validationRoots != nil {
				t.Fatal("candidate() handed out roots after a durable reload")
			}
			fileHash := sha256.Sum256(validated.BlockBOC)
			if validated.Candidate.ID != id || !validated.digested ||
				!bytes.Equal(validated.Candidate.Block.FileHash, fileHash[:]) ||
				validated.Candidate.CollatedFileHash != sha256.Sum256(validated.CollatedData) {
				t.Fatal("the reloaded candidate is not the stored one with its digest provenance")
			}
		})
	}
}

// TestBlockAccepterResidentDigestedBlockIsNotRehashed pins the one route that
// skips the file hash. A resident tip of a digested candidate is the block this
// node decoded or built from these exact bytes, under the file hash taken of
// them there. The wire decode without a matching tip and an artifact without
// the seal both still take the sha256.
func TestBlockAccepterResidentDigestedBlockIsNotRehashed(t *testing.T) {
	shard := groups.ShardID{Workchain: 0, Shard: -1 << 63}

	honest := newAcceptanceTestFixture(t, shard)
	honest.candidate.digested = true
	accepter, err := newAcceptanceTestAccepter(honest, &acceptanceTestNode{})
	if err != nil {
		t.Fatal(err)
	}
	acceptance := honest.acceptance(simplex.VoteFinalize, false)
	decoded, err := accepter.Prepare(t.Context(), acceptance, nil)
	if err != nil {
		t.Fatal(err)
	}
	acceptance.state = acceptanceResidentState(t, acceptance.Candidate)
	resident, err := accepter.Prepare(t.Context(), acceptance, nil)
	if err != nil {
		t.Fatal(err)
	}
	if resident.block.Block != acceptance.state.tips[0].Block {
		t.Fatal("acceptance of a sealed candidate decoded a root already held by its exact tip")
	}
	if !bytes.Equal(resident.block.ProofBOC, decoded.block.ProofBOC) ||
		!bytes.Equal(resident.block.SignaturesVerifiedKey, decoded.block.SignaturesVerifiedKey) ||
		!reflect.DeepEqual(resident.block.Meta, decoded.block.Meta) {
		t.Fatal("the sealed resident route produced different proof evidence or metadata")
	}

	// The same bytes under a file hash that is not their digest. Only the sha256
	// can see it, so whether the mismatch is reported is whether it was taken.
	const mismatch = "file hash mismatch"
	for _, test := range []acceptanceDigestCase{
		{name: "sealed resident root", digested: true, resident: true, rehashed: false},
		{name: "sealed wire decode", digested: true, resident: false, rehashed: true},
		{name: "unsealed resident root", digested: false, resident: true, rehashed: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			fixture := newAcceptanceTestFixture(t, shard)
			fixture.candidate.Candidate.Block.FileHash = bytes.Repeat([]byte{0xf1}, 32)
			fixture.candidate.Candidate.ID = fixture.candidate.Candidate.ComputeID(3)
			fixture.candidate.digested = test.digested
			acceptance := fixture.acceptance(simplex.VoteFinalize, false)
			if test.resident {
				acceptance.state = acceptanceResidentState(t, acceptance.Candidate)
			}
			accepter, err := newAcceptanceTestAccepter(fixture, &acceptanceTestNode{})
			if err != nil {
				t.Fatal(err)
			}

			_, err = accepter.Prepare(t.Context(), acceptance, nil)
			rehashed := err != nil && strings.Contains(err.Error(), mismatch)
			if rehashed != test.rehashed {
				t.Fatalf("prepare error = %v, want file hash taken = %v", err, test.rehashed)
			}
			if !test.rehashed && err != nil {
				t.Fatalf("sealed resident acceptance failed for another reason: %v", err)
			}
		})
	}
}

// BenchmarkBlockAcceptanceResidentDigest isolates the file hash on the resident
// route: the same full-collated block and the same exact tip, with and without
// the digest seal.
func BenchmarkBlockAcceptanceResidentDigest(b *testing.B) {
	fixture := newAcceptanceBenchmarkFixture(b)
	for _, digested := range []bool{false, true} {
		mode := "unsealed"
		if digested {
			mode = "sealed"
		}
		b.Run(mode, func(b *testing.B) {
			artifact := *fixture.acceptance.Candidate
			artifact.digested = digested
			acceptance := fixture.acceptance
			acceptance.Candidate = &artifact
			acceptance.CertifiedCandidate = &artifact
			acceptance.state = &ChainState{tips: []ChainTip{{
				ID:       artifact.Candidate.Block,
				BlockBOC: artifact.BlockBOC,
				Block:    fixture.blockRoot,
			}}}
			b.ReportAllocs()
			for b.Loop() {
				if _, err := fixture.accepter.Prepare(b.Context(), acceptance, nil); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
