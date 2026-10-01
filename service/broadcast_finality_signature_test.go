package service

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"os"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/p2p"
)

// TestCheckBlockFinalitySignaturesStillVerifiesEverySignature guards the trust
// boundary the sealed-certificate change deliberately did not touch.
//
// validator.BlockAccepter no longer re-verifies the Ed25519 quorum of a block it
// accepts, because there the certificate came out of this node's own consensus
// engine and arrives as a simplex.VerifiedCertificate. This is the *receiving*
// side of exactly what that accepter publishes: the finality broadcast arrives
// from an arbitrary peer, no engine of ours ever saw its certificate, and the
// full check must stay. A regression here — swapping in
// blockproof.CheckPreparedSignatureWeight, say — would let any peer publish a
// block under a forged quorum.
func TestCheckBlockFinalitySignaturesStillVerifiesEverySignature(t *testing.T) {
	const catchainSeqno = uint32(7)

	publicKey, privateKey, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate validator key: %v", err)
	}
	var validatorKey p2p.FastSyncValidatorPublicKey
	copy(validatorKey[:], publicKey)
	validator := fastSyncConfigTestValidator{
		publicKey: validatorKey,
		adnlID:    fastSyncConfigTestPeerID(0x41),
	}
	cfg := fastSyncConfigTestBlockchainConfig(t, map[uint32]*cell.Cell{
		tlb.ConfigParamCatchainConfig:    broadcastValidatorTestCatchainConfig(),
		tlb.ConfigParamPrevValidators:    fastSyncConfigTestValidatorSetCell(t, validator),
		tlb.ConfigParamCurrentValidators: fastSyncConfigTestValidatorSetCell(t, validator),
		tlb.ConfigParamNextValidators:    fastSyncConfigTestValidatorSetCell(t, validator),
	})

	block := testBlockID(0, topShard, 10)
	validators, err := blockproof.CurrentValidatorsForBlock(cfg, &block, catchainSeqno)
	if err != nil {
		t.Fatalf("load current validators: %v", err)
	}
	prepared, err := blockproof.PrepareValidatorSet(catchainSeqno, validators)
	if err != nil {
		t.Fatalf("prepare validator set: %v", err)
	}

	var coordinator SyncCoordinator
	coordinator.broadcastValidatorCache.putConfig(
		testBlockID(-1, topShard, 100),
		broadcastValidatorConfig{rootHash: cfg.Root.HashKey(), cfg: cfg},
	)

	sessionID := make([]byte, 32)
	sessionID[0] = 0x63
	const slot = int32(11)
	candidate := finalityTestSimplexCandidate(t, block)
	payload, err := blockproof.SimplexSignaturePayload(block, true, sessionID, slot, candidate)
	if err != nil {
		t.Fatalf("build simplex signature payload: %v", err)
	}
	nodeID, err := tl.Hash(keys.PublicKeyED25519{Key: publicKey})
	if err != nil {
		t.Fatalf("hash validator key: %v", err)
	}

	signatureSet := func(signature []byte) *blockproof.ValidatorSignatureSet {
		return blockproof.NewSimplexValidatorSignatureSet(
			catchainSeqno,
			prepared.Hash(),
			[]ton.Signature{{NodeIDShort: nodeID, Signature: signature}},
			true,
			sessionID,
			slot,
			candidate,
		)
	}

	valid := ed25519.Sign(privateKey, payload)
	result, err := coordinator.CheckBlockFinalitySignatures(context.Background(), p2p.BlockFinalitySignatureCheck{
		Kind:       "tonNode.blockFinalityBroadcast",
		Block:      block,
		Signatures: signatureSet(valid),
	})
	if err != nil {
		t.Fatalf("genuine finality broadcast rejected: %v", err)
	}
	if len(result.SignaturesVerifiedKey) == 0 {
		t.Fatal("accepted finality broadcast carries no verification key")
	}

	// One flipped trailing bit — the weight half of the check cannot see it, so
	// this passes only while the Ed25519 pass is still here.
	corrupt := append([]byte(nil), valid...)
	corrupt[len(corrupt)-1] ^= 0x01
	if _, err = coordinator.CheckBlockFinalitySignatures(context.Background(), p2p.BlockFinalitySignatureCheck{
		Kind:       "tonNode.blockFinalityBroadcast",
		Block:      block,
		Signatures: signatureSet(corrupt),
	}); err == nil || !strings.Contains(err.Error(), "incorrect signature") {
		t.Fatalf("corrupt finality broadcast error = %v, want an incorrect-signature rejection", err)
	}

	// A signature that is valid, but for another session: the payload commits to
	// the session id, so the network side rejects it for the same reason the
	// accepter's binding check does.
	otherSession := append([]byte(nil), sessionID...)
	otherSession[0] ^= 0xff
	otherPayload, err := blockproof.SimplexSignaturePayload(block, true, otherSession, slot, candidate)
	if err != nil {
		t.Fatalf("build foreign session payload: %v", err)
	}
	if _, err = coordinator.CheckBlockFinalitySignatures(context.Background(), p2p.BlockFinalitySignatureCheck{
		Kind:       "tonNode.blockFinalityBroadcast",
		Block:      block,
		Signatures: signatureSet(ed25519.Sign(privateKey, otherPayload)),
	}); err == nil {
		t.Fatal("a signature made for another consensus session was accepted")
	}
}

// TestCheckBlockBroadcastSignaturesUsesTheFullCheck pins the source-level fact
// that the two network-facing verifiers call blockproof.CheckPreparedSignatures
// and never the weaker weight-only entry point.
func TestCheckBlockBroadcastSignaturesUsesTheFullCheck(t *testing.T) {
	raw, err := os.ReadFile("broadcast_signature_verifier.go")
	if err != nil {
		t.Fatalf("read verifier source: %v", err)
	}
	source := string(raw)
	if strings.Contains(source, "blockproof.CheckPreparedSignatureWeight") {
		t.Fatal("a network-facing signature verifier reaches for the weight-only check")
	}
	if strings.Count(source, "blockproof.CheckPreparedSignatures(") != 2 {
		t.Fatal("CheckBlockBroadcastSignatures/CheckBlockFinalitySignatures no longer run the full signature check")
	}
}

func finalityTestSimplexCandidate(t *testing.T, block ton.BlockIDExt) []byte {
	t.Helper()

	data, err := tl.Serialize(ton.ConsensusCandidateHashDataOrdinary{
		Block:            block,
		CollatedFileHash: make([]byte, 32),
		Parent:           ton.ConsensusCandidateWithoutParents{},
	}, true)
	if err != nil {
		t.Fatalf("serialize simplex candidate: %v", err)
	}

	return data
}
