package collator

import (
	"context"
	"crypto/sha256"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"

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
	if _, err = verifyCollatedData(foreign, 0); err != nil && strings.Contains(err.Error(), mismatch) {
		t.Fatalf("an honest digest was reported as a mismatch: %v", err)
	}

	tampered := *foreign
	tampered.CollatedFileHash[0] ^= 1
	if _, err = verifyCollatedData(&tampered, 0); err == nil || !strings.Contains(err.Error(), mismatch) {
		t.Fatalf("collated digest mismatch without provenance = %v, want a rejection", err)
	}

	// The same bytes and the same wrong hash, now claimed to have been digested
	// inside this package. The sha256 is not taken, so the mismatch is not seen;
	// whatever this run reports comes from the roots and never from the digest.
	// That is precisely the work being skipped, and precisely why the field is
	// unexported.
	claimed := tampered
	claimed.digested = true
	if _, err = verifyCollatedData(&claimed, 0); err != nil && strings.Contains(err.Error(), mismatch) {
		t.Fatalf("a provenance-carrying candidate re-derived its collated digest: %v", err)
	}
}

// TestSignArtifactHashesOnlyWhatItDidNotBuild is the F5 gate. A Pipeline
// supplied from outside this package hands over hashes that are a claim, and
// signing an id derived from a wrong claim burns the whole leader window, so
// there both digests are re-derived. Our own builder's are not: the reference
// producer takes the collator's hashes exactly as they come out of
// collator.cpp:6445-6450 and re-derives neither.
func TestSignArtifactHashesOnlyWhatItDidNotBuild(t *testing.T) {
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

	// Same tampering, now on a candidate that claims this package digested it.
	// Neither hash is recomputed, so neither mismatch is seen.
	ours := *built
	ours.digested = true
	ours.CollatedFileHash[0] ^= 1
	if _, err := fixture.service.signArtifact(session, window, 0, simplex.Genesis(), &ours); err != nil {
		t.Fatalf("our own candidate was re-hashed on the way to its signature: %v", err)
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
