package validator

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/xssnick/gton/service/validator/simplex"
)

// TestStoredCandidateReusesTheAdmissionDigest is the F6 change. The resolver
// hashes a candidate wire once, to admit it and to refuse a conflicting
// duplicate under that identity; the store then hashed the same multi-megabyte
// payload again to derive the content key it is written under. The reference
// has no content-addressed indirection at all here — it keys candidate content
// by candidate id (candidate-resolver.cpp:373) and hashes no wire on the store
// path — so the second digest was not merely duplicated work, it was work for a
// structure C++ does without.
func TestStoredCandidateReusesTheAdmissionDigest(t *testing.T) {
	storage := newRuntimeTestStorage()
	records := make(chan CandidateRecord, 1)
	storage.saveHook = func(_ SessionStorageID, record CandidateRecord, done func(error)) {
		records <- record
		done(nil)
	}
	resolver := newResolverForTest(
		storage,
		&retryCandidateProvider{called: make(chan struct{}, 1)},
		1,
		simplex.DefaultParams(),
	)
	defer resolver.close()

	wire := bytes.Repeat([]byte{0x5a}, 4096)
	id := simplex.CandidateID{Slot: 3, Hash: [32]byte{0x33}}
	if err := resolver.stage(&CandidateArtifact{Candidate: simplex.Candidate{ID: id}}, wire); err != nil {
		t.Fatal(err)
	}
	if _, err := resolver.storeAsync(id, nil); err != nil {
		t.Fatal(err)
	}

	record := <-records
	if record.ContentHash == ([32]byte{}) {
		t.Fatal("the resolver handed the store no content hash, so the store must derive it again")
	}
	if record.ContentHash != sha256.Sum256(wire) {
		t.Fatal("the content hash the resolver handed over is not the digest of the wire beside it")
	}
}

// TestDecodedCandidateCarriesItsDigestProvenance is the validator half of F7.
// The codec does not merely check that a candidate's file hashes match its
// payloads — it derives them from those payloads and folds them into the
// candidate id the signature covers. Saying so is what lets the collator skip
// re-deriving the collated digest when it validates.
func TestDecodedCandidateCarriesItsDigestProvenance(t *testing.T) {
	config, leaderKey := runtimeTestConfig(0x62, &runtimeTestJournal{})
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	ordinary := runtimeOrdinaryArtifact(t, config, leaderKey, 0, simplex.Genesis())
	if !ordinary.digested {
		t.Fatal("a locally produced artifact fixture lost its digest provenance")
	}
	wire, err := simplex.SerializeCandidate(ordinary.Candidate, ordinary.BlockBOC, ordinary.CollatedData)
	if err != nil {
		t.Fatal(err)
	}

	decoded, err := codec.decode(wire, &ordinary.Candidate.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !decoded.digested {
		t.Fatal("a wire-decoded candidate does not carry its digest provenance")
	}
	if decoded.Candidate.CollatedFileHash != sha256.Sum256(decoded.CollatedData) {
		t.Fatal("the decoded collated file hash is not the digest of the decoded collated data")
	}
	blockHash := sha256.Sum256(decoded.BlockBOC)
	if !bytes.Equal(decoded.Candidate.Block.FileHash, blockHash[:]) {
		t.Fatal("the decoded block file hash is not the digest of the decoded block boc")
	}
}
