package simplex

import (
	"bytes"
	"hash/crc32"
	"testing"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

// TestInteropNodeIDShortMatchesTonutils: the validator short id must equal
// the tl.Hash of the registered pub.ed25519 key type used across the node.
func TestInteropNodeIDShortMatchesTonutils(t *testing.T) {
	vals, _ := testValidators(3)
	for i, v := range vals {
		ref, err := tl.Hash(keys.PublicKeyED25519{Key: v.PublicKey})
		if err != nil {
			t.Fatal(err)
		}
		short := KeyNodeIDShort(v.PublicKey)
		if !bytes.Equal(short[:], ref) {
			t.Fatalf("validator %d short id mismatch", i)
		}
	}
}

// Note: the former cross-check against blockproof's notarize payload became
// tautological — service/blockproof now builds simplex approve payloads from
// this package's registered ConsensusSimplexNotarizeVote type directly.

// TestInteropFinalCertificatePassesTonProofCheck drives a full consensus
// round in the engine and feeds the resulting finalization certificate into
// the existing tonutils-go simplex proof verification — the exact path used
// for real TON block proofs. This proves end-to-end bit compatibility of the
// candidate hash data, the vote payload, the dataToSign wrapping and the
// signatures.
func TestInteropFinalCertificatePassesTonProofCheck(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	cand := env.makeCandidate(0, Genesis(), 0x5d)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	env.completeValidation(cand.ID, nil)
	env.deliverVote(2, NotarizeVote(cand.ID))
	env.deliverVote(3, NotarizeVote(cand.ID))
	env.deliverVote(2, FinalizeVote(cand.ID))
	env.deliverVote(3, FinalizeVote(cand.ID))
	requireEqual(t, len(env.hooks.finalized), 1, "finalized")
	finalCert := env.hooks.finalizedCerts[0]
	requireEqual(t, finalCert.Vote.Kind, VoteFinalize, "final cert kind")

	// Session validator set in the tonutils-go representation.
	ccSeqno := uint32(77)
	var tlbVals []*tlb.ValidatorAddr
	for _, v := range env.vals {
		tlbVals = append(tlbVals, &tlb.ValidatorAddr{
			PublicKey: tlb.SigPubKeyED25519{Key: v.PublicKey},
			Weight:    v.Weight,
			ADNLAddr:  make([]byte, 32),
		})
	}
	var hashable ton.ValidatorSetHashable
	hashable.CCSeqno = ccSeqno
	for _, v := range tlbVals {
		hashable.Validators = append(hashable.Validators, ton.ValidatorItemHashable{
			Key: v.PublicKey.Key, Weight: v.Weight, Addr: v.ADNLAddr,
		})
	}
	serialized, err := tl.Serialize(hashable, true)
	if err != nil {
		t.Fatal(err)
	}
	setHash := crc32.Checksum(serialized, crc32.MakeTable(crc32.Castagnoli))

	// Certificate -> liteServer.signatureSet.simplex ingredients.
	var sigs []ton.Signature
	for _, s := range finalCert.Signatures {
		short := KeyNodeIDShort(env.vals[s.ValidatorIndex].PublicKey)
		sigs = append(sigs, ton.Signature{NodeIDShort: short[:], Signature: s.Signature})
	}
	blockID := &cand.Block
	candidateData := cand.HashDataBytes()

	err = ton.CheckBlockSignaturesSimplex(blockID, ccSeqno, setHash, env.session[:],
		int32(cand.ID.Slot), candidateData, sigs, tlbVals)
	if err != nil {
		t.Fatalf("tonutils-go rejected our finalization certificate: %v", err)
	}

	// Sanity: tampered inputs must fail.
	bad := append([]byte(nil), candidateData...)
	bad[len(bad)-1] ^= 1
	if err = ton.CheckBlockSignaturesSimplex(blockID, ccSeqno, setHash, env.session[:],
		int32(cand.ID.Slot), bad, sigs, tlbVals); err == nil {
		t.Fatal("tampered candidate data must fail")
	}
	if err = ton.CheckBlockSignaturesSimplex(blockID, ccSeqno, setHash, env.session[:],
		int32(cand.ID.Slot)+1, candidateData, sigs, tlbVals); err == nil {
		t.Fatal("wrong slot must fail")
	}
	wrongSession := append([]byte(nil), env.session[:]...)
	wrongSession[0] ^= 1
	if err = ton.CheckBlockSignaturesSimplex(blockID, ccSeqno, setHash, wrongSession,
		int32(cand.ID.Slot), candidateData, sigs, tlbVals); err == nil {
		t.Fatal("wrong session id must fail")
	}
}
