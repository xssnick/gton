package blockproof

import (
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

// TestCheckPreparedSignatureWeightIsTheNonCryptoHalf pins the exact difference
// between the two entry points, in both directions.
//
// The Ed25519 pass is the only thing CheckPreparedSignatureWeight drops: a
// corrupt signature must still be rejected by CheckPreparedSignatures, which is
// what every network-facing caller uses, and must be accepted by the weaker one,
// which is what the Simplex block accepter uses because it holds a
// simplex.VerifiedCertificate proving the same quorum was already verified.
//
// Everything that is not cryptography must be rejected by both.
func TestCheckPreparedSignatureWeightIsTheNonCryptoHalf(t *testing.T) {
	block := testBlockID(-1, 91)
	validators, privateKeys := testValidators(t, 4)
	const catchainSeqno = uint32(9)

	prepared, err := PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}
	base := NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), nil)
	payload, err := signaturePayload(block, base)
	if err != nil {
		t.Fatalf("signature payload: %v", err)
	}
	signatures := testSignatures(t, validators[:3], privateKeys[:3], payload)

	signed := NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), signatures)
	weight, err := CheckPreparedSignatureWeight(block, signed, prepared)
	if err != nil {
		t.Fatalf("weight check of a valid set: %v", err)
	}
	if weight != 3 {
		t.Fatalf("signed weight = %d, want 3", weight)
	}
	if err = CheckPreparedSignatures(block, signed, prepared); err != nil {
		t.Fatalf("full check of a valid set: %v", err)
	}

	// One flipped trailing bit: cryptography and nothing else.
	corrupted := cloneTestSignatures(signatures)
	corrupted[2].Signature[len(corrupted[2].Signature)-1] ^= 0x01
	corruptSet := NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), corrupted)

	if err = CheckPreparedSignatures(block, corruptSet, prepared); err == nil {
		t.Fatal("CheckPreparedSignatures accepted a corrupt signature")
	}
	if _, err = CheckPreparedSignatureWeight(block, corruptSet, prepared); err != nil {
		t.Fatalf("CheckPreparedSignatureWeight rejected a corrupt signature it must not inspect: %v", err)
	}

	// Everything the weight half owns is rejected by both halves.
	for _, test := range []struct {
		name string
		set  *ValidatorSignatureSet
	}{
		{
			name: "insufficient weight",
			set:  NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), signatures[:2]),
		},
		{
			name: "wrong catchain seqno",
			set:  NewOrdinaryValidatorSignatureSet(catchainSeqno+1, prepared.Hash(), signatures),
		},
		{
			name: "wrong validator set hash",
			set:  NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash()^1, signatures),
		},
		{
			name: "duplicated validator",
			set: NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), []ton.Signature{
				signatures[0], signatures[1], signatures[1], signatures[2],
			}),
		},
		{
			name: "unknown validator",
			set: NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), []ton.Signature{
				signatures[0], signatures[1], {NodeIDShort: make([]byte, 32), Signature: signatures[2].Signature},
			}),
		},
		{
			name: "truncated signature",
			set: NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), []ton.Signature{
				signatures[0], signatures[1],
				{NodeIDShort: signatures[2].NodeIDShort, Signature: signatures[2].Signature[:32]},
			}),
		},
		{
			name: "no signatures",
			set:  NewOrdinaryValidatorSignatureSet(catchainSeqno, prepared.Hash(), nil),
		},
		{
			name: "absent set",
			set:  nil,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if _, err := CheckPreparedSignatureWeight(block, test.set, prepared); err == nil {
				t.Fatal("weight check accepted it")
			}
			if err := CheckPreparedSignatures(block, test.set, prepared); err == nil {
				t.Fatal("full check accepted it")
			}
		})
	}
}

func cloneTestSignatures(signatures []ton.Signature) []ton.Signature {
	out := make([]ton.Signature, len(signatures))
	for i, signature := range signatures {
		out[i] = ton.Signature{
			NodeIDShort: signature.NodeIDShort,
			Signature:   append([]byte(nil), signature.Signature...),
		}
	}

	return out
}

// BenchmarkCheckPreparedSignatureWeight is the counterpart of
// BenchmarkCheckPreparedSignatures: the difference between the two is exactly
// what one accepted block (and every one-second retry of it) stops paying.
func BenchmarkCheckPreparedSignatureWeight(b *testing.B) {
	block := testBlockID(-1, 90)
	validators, privateKeys := testValidators(b, 100)
	prepared, err := PrepareValidatorSet(9, validators)
	if err != nil {
		b.Fatal(err)
	}

	base := NewOrdinaryValidatorSignatureSet(9, prepared.Hash(), nil)
	payload, err := signaturePayload(block, base)
	if err != nil {
		b.Fatal(err)
	}
	signatures := testSignatures(b, validators, privateKeys, payload)
	signed := NewOrdinaryValidatorSignatureSet(9, prepared.Hash(), signatures)

	b.ReportAllocs()
	for b.Loop() {
		if _, err := CheckPreparedSignatureWeight(block, signed, prepared); err != nil {
			b.Fatal(err)
		}
	}
}
