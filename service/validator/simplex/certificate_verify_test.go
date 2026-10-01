package simplex

import (
	"bytes"
	"crypto/ed25519"
	"runtime"
	"sync"
	"testing"
)

func TestVerifyCertificateWithoutEngine(t *testing.T) {
	var sessionID [32]byte
	sessionID[0] = 0xa7
	vote := NotarizeVote(CandidateID{Slot: 3, Hash: [32]byte{0x31}})
	payload := DataToSign(sessionID, VoteBytes(vote))

	validators := make([]Validator, 4)
	privateKeys := make([]ed25519.PrivateKey, 4)
	for i := range validators {
		seed := bytes.Repeat([]byte{byte(i + 1)}, ed25519.SeedSize)
		privateKeys[i] = ed25519.NewKeyFromSeed(seed)
		validators[i] = Validator{
			PublicKey: privateKeys[i].Public().(ed25519.PublicKey),
			Weight:    1,
		}
	}
	certificate := &Certificate{Vote: vote}
	for i := range 3 {
		certificate.Signatures = append(certificate.Signatures, VoteSignature{
			ValidatorIndex: uint32(i),
			Signature:      ed25519.Sign(privateKeys[i], payload),
		})
	}
	sealed, err := VerifyCertificate(sessionID, validators, certificate)
	if err != nil {
		t.Fatal(err)
	}
	carried := sealed.Certificate()
	if carried == certificate || !certificatesEqual(carried, certificate) || sealed.Vote() != vote {
		t.Fatal("verified certificate does not carry the certificate it verified")
	}
	binding, err := NewCertificateBinding(sessionID, validators)
	if err != nil {
		t.Fatal(err)
	}
	if err = binding.Check(sealed); err != nil {
		t.Fatalf("seal rejected by its own binding: %v", err)
	}

	bad := &Certificate{Vote: vote, Signatures: append([]VoteSignature(nil), certificate.Signatures...)}
	bad.Signatures[1].Signature = bytes.Clone(bad.Signatures[1].Signature)
	bad.Signatures[1].Signature[0] ^= 0xff
	if _, err = VerifyCertificate(sessionID, validators, bad); err == nil {
		t.Fatal("invalid signature passed standalone certificate verification")
	}
	if _, err = VerifyCertificate(sessionID, validators, nil); err == nil {
		t.Fatal("nil certificate passed standalone verification")
	}
}

func TestCertificateVerifierOwnsValidatorRoster(t *testing.T) {
	sessionID := [32]byte{0xa1}
	validators, keys := testValidators(4)
	verifier, err := NewCertificateVerifier(sessionID, validators)
	if err != nil {
		t.Fatal(err)
	}
	originalBinding := verifier.Binding()

	attacker := testKey(99)
	validators[0].Weight = 100
	validators[0].PublicKey = bytes.Clone(attacker.Public().(ed25519.PublicKey))
	copy(validators[1].PublicKey, attacker.Public().(ed25519.PublicKey))

	vote := NotarizeVote(CandidateID{Slot: 11, Hash: [32]byte{0x71}})
	forged := &Certificate{
		Vote: vote,
		Signatures: []VoteSignature{{
			ValidatorIndex: 0,
			Signature:      ed25519.Sign(attacker, DataToSign(sessionID, VoteBytes(vote))),
		}},
	}
	if _, err = verifier.Verify(forged); err == nil {
		t.Fatal("one attacker signature passed after mutating the verifier source roster")
	}
	if verifier.Binding() != originalBinding {
		t.Fatal("source roster mutation changed the verifier binding")
	}

	legitimate := testCertificateFor(t, validators, keys, vote)
	if _, err = verifier.Verify(legitimate); err != nil {
		t.Fatalf("source roster mutation corrupted the verifier-owned snapshot: %v", err)
	}
}

func TestCertificateVerifierRosterSnapshotIsRaceIndependent(t *testing.T) {
	sessionID := [32]byte{0xa1}
	validators, keys := testValidators(4)
	verifier, err := NewCertificateVerifier(sessionID, validators)
	if err != nil {
		t.Fatal(err)
	}
	vote := FinalizeVote(CandidateID{Slot: 12, Hash: [32]byte{0x82}})
	certificate := testCertificateFor(t, validators, keys, vote)

	start := make(chan struct{})
	var mutations sync.WaitGroup
	mutations.Add(1)
	go func() {
		defer mutations.Done()
		<-start
		for range 10_000 {
			validators[0].Weight++
			validators[0].PublicKey[0] ^= 0xff
			runtime.Gosched()
		}
	}()
	close(start)

	for range 200 {
		if _, err = verifier.Verify(certificate); err != nil {
			t.Fatalf("verification observed a concurrent source roster mutation: %v", err)
		}
	}
	mutations.Wait()
}

func TestVerifiedCertificateOwnsAndDoesNotExposeCertificate(t *testing.T) {
	sessionID := [32]byte{0xa1}
	validators, keys := testValidators(4)
	vote := NotarizeVote(CandidateID{Slot: 13, Hash: [32]byte{0x93}})
	certificate := testCertificateFor(t, validators, keys, vote)
	expected := cloneCertificate(certificate)

	sealed, err := VerifyCertificate(sessionID, validators, certificate)
	if err != nil {
		t.Fatal(err)
	}

	certificate.Vote = SkipVote(99)
	certificate.Signatures[0].ValidatorIndex = 3
	certificate.Signatures[0].Signature[0] ^= 0xff

	exposed := sealed.Certificate()
	exposed.Vote = SkipVote(100)
	exposed.Signatures[1].ValidatorIndex = 3
	exposed.Signatures[1].Signature[0] ^= 0xff

	carried := sealed.Certificate()
	if carried == certificate || carried == exposed || !certificatesEqual(carried, expected) {
		t.Fatal("input or accessor mutation changed the sealed certificate")
	}
	if sealed.Vote() != vote {
		t.Fatalf("sealed vote = %s, want %s", sealed.Vote(), vote)
	}
	if _, err = VerifyCertificate(sessionID, validators, carried); err != nil {
		t.Fatalf("sealed certificate no longer verifies: %v", err)
	}
}

func TestVerifyCertificateRejectsInvalidVoteShape(t *testing.T) {
	validators := []Validator{{PublicKey: make(ed25519.PublicKey, ed25519.PublicKeySize), Weight: 1}}

	invalidKind := &Certificate{Vote: Vote{Kind: VoteKind(0xff)}}
	if _, err := VerifyCertificate([32]byte{}, validators, invalidKind); err == nil {
		t.Fatal("invalid vote kind was accepted")
	}

	invalidSkip := &Certificate{Vote: SkipVote(7)}
	invalidSkip.Vote.ID.Hash[0] = 1
	if _, err := VerifyCertificate([32]byte{}, validators, invalidSkip); err == nil {
		t.Fatal("skip vote with candidate hash was accepted")
	}
}

// TestEngineVerifyCertificatePrecondition: the in-session twin enforces the
// same precondition as the free function. Without it a malformed vote kind
// reaches votePayload -> voteToTL, which panics rather than returning an
// error.
func TestEngineVerifyCertificatePrecondition(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	if _, err := env.eng.verifyCertificate(nil); err == nil {
		t.Fatal("nil certificate was accepted")
	}
	if _, err := env.eng.verifyCertificate(&Certificate{Vote: Vote{Kind: VoteKind(0xff)}}); err == nil {
		t.Fatal("invalid vote kind was accepted")
	}
	invalidSkip := &Certificate{Vote: SkipVote(1)}
	invalidSkip.Vote.ID.Hash[0] = 1
	if _, err := env.eng.verifyCertificate(invalidSkip); err == nil {
		t.Fatal("skip vote with candidate hash was accepted")
	}
	env.requireNoFatal()
}
