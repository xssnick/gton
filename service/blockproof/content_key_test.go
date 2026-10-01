package blockproof

import (
	"bytes"
	"crypto/ed25519"
	"crypto/rand"
	"encoding/hex"
	"math"
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
	otherBlock = testBlockID(-1, 123)
	otherBlock.FileHash[0] = 0xBB
	if bytes.Equal(key, ordered.ContentKey(otherBlock)) {
		t.Fatal("content key ignores block file hash")
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
	blockKey := (&BlockSignatureSet{ValidatorSignatures: ordered, DeclaredWeight: 3}).ContentKey(block)
	forgedWeightKey := (&BlockSignatureSet{ValidatorSignatures: ordered, DeclaredWeight: 4}).ContentKey(block)
	if bytes.Equal(blockKey, forgedWeightKey) {
		t.Fatal("block signature content key ignores declared weight")
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

// A signature listed after enough valid weight to pass the threshold is still
// part of the set and must be verified.
func TestCheckPreparedSignaturesRejectsCorruptTrailingSignature(t *testing.T) {
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
	if err = CheckPreparedSignatures(block, set, prepared); err == nil {
		t.Fatal("corrupted trailing light signature was accepted after the threshold")
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

func TestCheckPreparedBlockSignaturesRejectsForgedDeclaredWeight(t *testing.T) {
	block := testBlockID(0, 58)
	validators, privateKeys := testWeightedValidators(t, []uint64{100, 1})
	catchainSeqno := uint32(6)
	prepared, err := PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}

	base := NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), nil)
	payload, err := signaturePayload(block, base)
	if err != nil {
		t.Fatalf("payload: %v", err)
	}
	signed := NewOrdinaryValidatorSignatureSet(
		catchainSeqno,
		prepared.Hash(),
		testSignatures(t, validators, privateKeys, payload),
	)
	blockSignatures := &BlockSignatureSet{
		ValidatorSignatures: signed,
		DeclaredWeight:      100,
	}

	err = CheckPreparedBlockSignatures(block, blockSignatures, prepared)
	if err == nil || !strings.Contains(err.Error(), "declared=100 actual=101") {
		t.Fatalf("forged signature weight error = %v", err)
	}
}

func TestHasSignatureThresholdAvoidsUint64Overflow(t *testing.T) {
	tests := []struct {
		name   string
		signed uint64
		total  uint64
		want   bool
	}{
		{name: "maximum unanimous", signed: math.MaxUint64, total: math.MaxUint64, want: true},
		{name: "maximum half", signed: math.MaxUint64 / 2, total: math.MaxUint64},
		{name: "exact two thirds", signed: 2, total: 3},
		{name: "above two thirds", signed: 3, total: 3, want: true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := hasSignatureThreshold(tt.signed, tt.total); got != tt.want {
				t.Fatalf("threshold(%d, %d) = %t, want %t", tt.signed, tt.total, got, tt.want)
			}
		})
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

func TestCheckPreparedSignaturesIdentifiesFirstInvalidListedSignature(t *testing.T) {
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
	wantValidator := hex.EncodeToString(signatures[1].NodeIDShort)
	if !strings.Contains(err.Error(), wantValidator) {
		t.Fatalf("error %q does not identify first invalid validator %s", err, wantValidator)
	}
}
