package simplex

import (
	"bytes"
	"crypto/ed25519"
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
	if sealed.Certificate() != certificate || sealed.Vote() != vote {
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
