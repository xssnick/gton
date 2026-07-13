package blockproof

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

func TestValidatorSignatureSetContentKeyCanonical(t *testing.T) {
	block := testBlockID(-1, 123)
	validators, privateKeys := testValidators(t, 3)
	catchainSeqno := uint32(7)
	setHash, err := ValidatorSetHash(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("validator set hash: %v", err)
	}

	base := NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, nil)
	payload, err := signaturePayload(block, base)
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	signatures := testSignatures(t, validators, privateKeys, payload)

	ordered := NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, signatures)
	reversed := make([]ton.Signature, 0, len(signatures))
	for i := len(signatures) - 1; i >= 0; i-- {
		reversed = append(reversed, signatures[i])
	}
	shuffled := NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, reversed)

	key := ordered.ContentKey(block)
	if !bytes.Equal(key, shuffled.ContentKey(block)) {
		t.Fatal("content key depends on signature order")
	}

	otherBlock := testBlockID(-1, 124)
	otherBlock.RootHash[0] = 0xAA
	if bytes.Equal(key, ordered.ContentKey(otherBlock)) {
		t.Fatal("content key ignores block root hash")
	}

	if bytes.Equal(key, NewOrdinaryValidatorSignatureSet(catchainSeqno+1, setHash, signatures).ContentKey(block)) {
		t.Fatal("content key ignores catchain seqno")
	}
	if bytes.Equal(key, NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash+1, signatures).ContentKey(block)) {
		t.Fatal("content key ignores validator set hash")
	}

	mutated := make([]ton.Signature, len(signatures))
	copy(mutated, signatures)
	mutated[0].Signature = append([]byte(nil), mutated[0].Signature...)
	mutated[0].Signature[0] ^= 1
	if bytes.Equal(key, NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, mutated).ContentKey(block)) {
		t.Fatal("content key ignores signature bytes")
	}

	subset := NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, signatures[:2])
	if bytes.Equal(key, subset.ContentKey(block)) {
		t.Fatal("content key ignores signature count")
	}

	sessionID := make([]byte, 32)
	simplex := NewSimplexValidatorSignatureSet(catchainSeqno, setHash, signatures, true, sessionID, 3, testSimplexCandidate(t, block))
	if bytes.Equal(key, simplex.ContentKey(block)) {
		t.Fatal("content key ignores simplex payload")
	}
}

func TestCloneBlockIDCopiesHashes(t *testing.T) {
	block := testBlockID(-1, 123)

	cloned := CloneBlockID(block)
	cloned.RootHash[0] = 0xAA
	cloned.FileHash[0] = 0xBB

	if block.RootHash[0] == 0xAA {
		t.Fatal("clone shares root hash backing array")
	}
	if block.FileHash[0] == 0xBB {
		t.Fatal("clone shares file hash backing array")
	}
}

func testWeightedValidators(t *testing.T, weights []uint64) ([]*tlb.ValidatorAddr, []ed25519.PrivateKey) {
	t.Helper()

	validators := make([]*tlb.ValidatorAddr, len(weights))
	privateKeys := make([]ed25519.PrivateKey, len(weights))
	for i, weight := range weights {
		pub, priv, err := ed25519.GenerateKey(rand.Reader)
		if err != nil {
			t.Fatalf("generate validator key: %v", err)
		}
		adnl := make([]byte, 32)
		adnl[0] = byte(i + 1)
		validators[i] = &tlb.ValidatorAddr{
			PublicKey: tlb.SigPubKeyED25519{Key: pub},
			Weight:    weight,
			ADNLAddr:  adnl,
		}
		privateKeys[i] = priv
	}
	return validators, privateKeys
}

// TestCheckPreparedSignaturesVerifiesHeaviestFirst pins the weight-descending
// verification order: once verified weight passes 2/3, remaining (lighter)
// signatures are not verified, so a corrupted light signature after the
// threshold does not fail the set, while a corrupted heavy one does.
func TestCheckPreparedSignaturesVerifiesHeaviestFirst(t *testing.T) {
	block := testBlockID(-1, 55)
	validators, privateKeys := testWeightedValidators(t, []uint64{100, 1})
	catchainSeqno := uint32(3)
	setHash, err := ValidatorSetHash(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("validator set hash: %v", err)
	}
	prepared, err := PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}

	base := NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, nil)
	payload, err := signaturePayload(block, base)
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	signatures := testSignatures(t, validators, privateKeys, payload)

	corruptLight := make([]ton.Signature, len(signatures))
	copy(corruptLight, signatures)
	corruptLight[1].Signature = append([]byte(nil), corruptLight[1].Signature...)
	corruptLight[1].Signature[0] ^= 1
	set := NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, corruptLight)
	if err = CheckPreparedSignatures(block, set, prepared); err != nil {
		t.Fatalf("corrupted below-threshold light signature failed the set: %v", err)
	}

	corruptHeavy := make([]ton.Signature, len(signatures))
	copy(corruptHeavy, signatures)
	corruptHeavy[0].Signature = append([]byte(nil), corruptHeavy[0].Signature...)
	corruptHeavy[0].Signature[0] ^= 1
	set = NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, corruptHeavy)
	if err = CheckPreparedSignatures(block, set, prepared); err == nil {
		t.Fatal("corrupted heavy signature was accepted")
	}
}

func TestCheckPreparedSignaturesRejectsDuplicatesAndUnknownUpFront(t *testing.T) {
	block := testBlockID(-1, 56)
	validators, privateKeys := testWeightedValidators(t, []uint64{100, 1})
	catchainSeqno := uint32(4)
	setHash, err := ValidatorSetHash(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("validator set hash: %v", err)
	}
	prepared, err := PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}

	base := NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, nil)
	payload, err := signaturePayload(block, base)
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	signatures := testSignatures(t, validators, privateKeys, payload)

	// A duplicate is rejected even when the threshold is reachable before the
	// duplicate would be verified.
	duplicated := append([]ton.Signature{signatures[0]}, signatures[0])
	set := NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, duplicated)
	if err = CheckPreparedSignatures(block, set, prepared); err == nil {
		t.Fatal("duplicated signature was accepted")
	}

	// A signature from an unknown validator is rejected up front, even after
	// the threshold would already be reached by the heavy validator.
	unknownValidators, unknownKeys := testWeightedValidators(t, []uint64{1})
	unknown := testSignatures(t, unknownValidators, unknownKeys, payload)
	withUnknown := append([]ton.Signature{signatures[0]}, unknown[0])
	set = NewOrdinaryValidatorSignatureSet(catchainSeqno, setHash, withUnknown)
	if err = CheckPreparedSignatures(block, set, prepared); err == nil {
		t.Fatal("signature of unknown validator was accepted")
	}
}

func TestCheckPreparedSignaturesIdentifiesFirstInvalidByWeight(t *testing.T) {
	block := testBlockID(-1, 57)
	validators, privateKeys := testWeightedValidators(t, []uint64{40, 35, 25})
	catchainSeqno := uint32(5)
	prepared, err := PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}

	base := NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), nil)
	payload, err := signaturePayload(block, base)
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	signatures := testSignatures(t, validators, privateKeys, payload)
	for _, idx := range []int{0, 1} {
		signatures[idx].Signature = bytes.Clone(signatures[idx].Signature)
		signatures[idx].Signature[0] ^= 1
	}

	set := NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), []ton.Signature{
		signatures[2],
		signatures[1],
		signatures[0],
	})
	err = CheckPreparedSignatures(block, set, prepared)
	if err == nil {
		t.Fatal("invalid batch was accepted")
	}
	wantValidator := hex.EncodeToString(signatures[0].NodeIDShort)
	if !strings.Contains(err.Error(), wantValidator) {
		t.Fatalf("error %q does not identify first invalid validator %s", err, wantValidator)
	}
}
