package collator

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

// TestBuildStampsItsOwnDigests pins the one statement every digest relaxation
// rests on: finish() returns a candidate whose ID.FileHash and CollatedFileHash
// really are the sha256 of the two buffers beside them, and says so through a
// field only finish() can set.
func TestBuildStampsItsOwnDigests(t *testing.T) {
	req, _ := benchMainnetRequest(t, 0)
	built, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !built.digested {
		t.Fatal("a candidate this builder produced does not carry its digest provenance")
	}
	fileHash := sha256.Sum256(built.BlockBOC)
	if string(fileHash[:]) != string(built.ID.FileHash) {
		t.Fatal("built candidate file hash is not the digest of its block boc")
	}
	if sha256.Sum256(built.CollatedData) != built.CollatedFileHash {
		t.Fatal("built collated file hash is not the digest of its collated data")
	}
}

func TestClonePipelineCandidateOwnsMutableFields(t *testing.T) {
	storageHash := cell.Hash{0x51}
	source := &Candidate{
		ID: ton.BlockIDExt{
			RootHash: []byte{0x11},
			FileHash: []byte{0x12},
		},
		BlockBOC:     []byte{0x21},
		CollatedData: []byte{0x22},
		StorageStats: AccountStorageStats{
			storageHash: cell.BeginCell().EndCell(),
		},
		Externals: []msgpool.ExternalFeedback{{Outcome: msgpool.ExternalIncluded}},
	}

	owned := clonePipelineCandidate(source)
	source.ID.RootHash[0] = 0xa1
	source.ID.FileHash[0] = 0xa2
	source.BlockBOC[0] = 0xa3
	source.CollatedData[0] = 0xa4
	delete(source.StorageStats, storageHash)
	source.Externals[0].Outcome = msgpool.ExternalInvalid

	if owned.ID.RootHash[0] != 0x11 || owned.ID.FileHash[0] != 0x12 ||
		owned.BlockBOC[0] != 0x21 || owned.CollatedData[0] != 0x22 {
		t.Fatal("owned pipeline candidate aliases mutable block payload fields")
	}
	if owned.StorageStats[storageHash] == nil {
		t.Fatal("owned pipeline candidate aliases storage stats map")
	}
	if owned.Externals[0].Outcome != msgpool.ExternalIncluded {
		t.Fatal("owned pipeline candidate aliases external feedback slice")
	}
}

// TestVerifyCollatedDataChecksTheDigestWithoutProvenance is the F7 gate from
// both sides. Without provenance the digest is taken and a mismatch is refused
// — that is the exported entry point and every caller that assembled a
// Candidate itself. With provenance it is not taken at all, which is what makes
// the claim load-bearing and why only this package can make it.
//
// The reference never re-derives this digest on the validate path either:
// ValidateQuery::unpack_block_candidate re-verifies the block file hash and
// reads collated_file_hash for statistics only.
func TestVerifyCollatedDataChecksTheDigestWithoutProvenance(t *testing.T) {
	collatedData, err := cell.ToBOCWithOptionsErr(
		[]*cell.Cell{cell.BeginCell().MustStoreUInt(0xc011a, 64).EndCell()},
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		t.Fatal(err)
	}

	const mismatch = "collated data file hash mismatch"
	foreign := &Candidate{CollatedData: collatedData, CollatedFileHash: sha256.Sum256(collatedData)}
	if _, err = verifyCollatedData(foreign, nil, 0); err != nil && strings.Contains(err.Error(), mismatch) {
		t.Fatalf("an honest digest was reported as a mismatch: %v", err)
	}

	tampered := *foreign
	tampered.CollatedFileHash[0] ^= 1
	if _, err = verifyCollatedData(&tampered, nil, 0); err == nil || !strings.Contains(err.Error(), mismatch) {
		t.Fatalf("collated digest mismatch without provenance = %v, want a rejection", err)
	}

	// The same bytes and the same wrong hash, now claimed to have been digested
	// inside this package. The sha256 is not taken, so the mismatch is not seen;
	// whatever this run reports comes from the roots and never from the digest.
	// That is precisely the work being skipped, and precisely why the field is
	// unexported.
	claimed := tampered
	claimed.digested = true
	if _, err = verifyCollatedData(&claimed, nil, 0); err != nil && strings.Contains(err.Error(), mismatch) {
		t.Fatalf("a provenance-carrying candidate re-derived its collated digest: %v", err)
	}
}

// TestSignArtifactRehashesAcrossThePublicPipelineBoundary pins the ownership
// boundary: an external Pipeline decorator can retain every private capsule by
// returning the canonical Candidate pointer after mutating its exported bytes.
// The unexported digested bit therefore cannot suppress either hash here.
func TestSignArtifactRehashesAcrossThePublicPipelineBoundary(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	session, _ := fixture.session(0x74, 1, 0, time.Now())
	window := productionWindow{
		ID:         WindowID{SessionID: session.ID, StartSlot: 0},
		Leader:     0,
		Authority:  CandidateAuthoritySelf,
		SelfSigner: &runtimeCountingSigner{private: fixture.leaderPriv},
	}
	built := preparedRouteCandidate(t, session, 0, session.Validators[0].PublicKey)

	foreignCollated := *built
	foreignCollated.CollatedFileHash[0] ^= 1
	if _, err := fixture.service.signArtifact(
		session, window, 0, simplex.Genesis(), &foreignCollated,
	); err == nil {
		t.Fatal("a foreign candidate was signed with a collated hash that does not match its data")
	}

	foreignBlock := *built
	foreignBlock.ID.FileHash = append([]byte(nil), built.ID.FileHash...)
	foreignBlock.ID.FileHash[0] ^= 1
	if _, err := fixture.service.signArtifact(
		session, window, 0, simplex.Genesis(), &foreignBlock,
	); err == nil {
		t.Fatal("a foreign candidate was signed with a file hash that does not match its block")
	}

	// Same tampering with every private provenance marker preserved must still
	// fail: this is exactly what a Pipeline decorator can return.
	ours := *built
	ours.digested = true
	ours.CollatedFileHash[0] ^= 1
	if _, err := fixture.service.signArtifact(session, window, 0, simplex.Genesis(), &ours); err == nil {
		t.Fatal("a mutated canonical candidate bypassed the public Pipeline digest boundary")
	}

	// Cell-backed fields and commit metadata are not part of the signed wire.
	// A genuinely foreign candidate has no seal and takes the replay path, but
	// mutating a canonical result must be rejected before it can poison local
	// lineage, queue hints, storage caches or external-message completion.
	swappedState := preparedRouteCandidate(t, session, 0, session.Validators[0].PublicKey)
	swappedState.State = cell.BeginCell().MustStoreUInt(0x5a7e, 32).EndCell()
	if _, err := fixture.service.signArtifact(
		session, window, 0, simplex.Genesis(), swappedState,
	); err == nil {
		t.Fatal("a canonical candidate with a swapped successor state was signed")
	}

	mutations := map[string]func(*Candidate){
		"sequence":   func(candidate *Candidate) { candidate.ID.SeqNo++ },
		"queue size": func(candidate *Candidate) { candidate.Stats.OutQueueSize++ },
		"storage stats": func(candidate *Candidate) {
			candidate.StorageStats = AccountStorageStats{
				cell.Hash{0x51}: cell.BeginCell().EndCell(),
			}
		},
		"external feedback": func(candidate *Candidate) {
			candidate.Externals = append(candidate.Externals, msgpool.ExternalFeedback{})
		},
	}
	for name, mutate := range mutations {
		candidate := preparedRouteCandidate(t, session, 0, session.Validators[0].PublicKey)
		mutate(candidate)
		if _, err := fixture.service.signArtifact(
			session, window, 0, simplex.Genesis(), candidate,
		); err == nil {
			t.Fatalf("canonical candidate with mutated %s was signed", name)
		}
	}
}

// TestSignedArtifactCarriesItsDigestProvenance closes the loop between the two
// gates above: what signArtifact hands the validator must say that its hashes
// are the digests of its payloads, or the leader's own validation of its own
// candidate takes the sha256 back.
func TestSignedArtifactCarriesItsDigestProvenance(t *testing.T) {
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, nil)
	defer fixture.close(t)

	session, _ := fixture.session(0x75, 1, 0, time.Now())
	window := productionWindow{
		ID:         WindowID{SessionID: session.ID, StartSlot: 0},
		Leader:     0,
		Authority:  CandidateAuthoritySelf,
		SelfSigner: &runtimeCountingSigner{private: fixture.leaderPriv},
	}
	built := preparedRouteCandidate(t, session, 0, session.Validators[0].PublicKey)
	artifact, err := fixture.service.signArtifact(session, window, 0, simplex.Genesis(), built)
	if err != nil {
		t.Fatal(err)
	}
	if !artifact.Digested() {
		t.Fatal("a signed artifact does not carry its digest provenance")
	}
}
