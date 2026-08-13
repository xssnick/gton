package collator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// TestSemanticVerifierRejectsMasterSpecialV2Envelope splices a genuine mint or
// fee recovery descriptor back into a real candidate with emitted_lt set on its
// envelope, which is the one thing check_special_message forbids outright
// (validate-query.cpp:6577). The candidate is otherwise untouched, and the
// unspliced original is verified first as a positive control, so the rejection
// is attributable to the v2 payload rather than to the splice.
func TestSemanticVerifierRejectsMasterSpecialV2Envelope(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	control := cloneVerificationCandidate(candidate)
	tampered := cloneVerificationCandidate(candidate)

	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, tampered)); err != nil {
		t.Fatal(err)
	}
	if block.Extra == nil || block.Extra.Custom == nil {
		t.Fatal("masterchain candidate has no custom extra")
	}
	descriptor := block.Extra.Custom.Details.MintMsg
	isMint := true
	if descriptor == nil {
		descriptor = block.Extra.Custom.Details.RecoverCreateMsg
		isMint = false
	}
	if descriptor == nil {
		t.Fatal("masterchain fixture produced no special message")
	}

	loader := descriptor.MustBeginParse()
	if tag := loader.MustLoadUInt(3); tag != semanticInImmediate {
		t.Fatalf("special inbound tag = %d", tag)
	}
	envelopeRoot, transactionRoot, err := semanticLoadTwoRefs(loader)
	if err != nil {
		t.Fatal(err)
	}
	fee, err := semanticLoadCoins(loader)
	if err != nil || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		t.Fatalf("decode special descriptor fee: %v", err)
	}
	var envelope tlb.MsgEnvelope
	if err = parseExact(&envelope, envelopeRoot); err != nil {
		t.Fatal(err)
	}
	var message tlb.InternalMessage
	if err = parseExact(&message, envelope.Msg); err != nil {
		t.Fatal(err)
	}
	emittedLT := message.CreatedLT
	envelope.V2 = true
	envelope.EmittedLT = &emittedLT
	envelopeRoot, err = envelope.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err = descriptorFee(semanticInImmediate, 3, envelopeRoot, transactionRoot, fee)
	if err != nil {
		t.Fatal(err)
	}

	inMessages, err := loadInMsgDescriptors(block.Extra.InMsgDesc, fixture.request.Config.globalVersion)
	if err != nil {
		t.Fatal(err)
	}
	messageHash := envelope.Msg.HashKey()
	key := cell.BeginCell().MustStoreSlice(messageHash[:], 256).EndCell()
	if err = inMessages.Set(key, descriptor); err != nil {
		t.Fatal(err)
	}
	block.Extra.InMsgDesc, err = inMessages.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	if isMint {
		block.Extra.Custom.Details.MintMsg = descriptor
	} else {
		block.Extra.Custom.Details.RecoverCreateMsg = descriptor
	}
	masterExtra, err := serializeMcBlockExtra(block.Extra.Custom)
	if err != nil {
		t.Fatal(err)
	}
	var randSeed, createdBy [32]byte
	copy(randSeed[:], block.Extra.RandSeed)
	copy(createdBy[:], block.Extra.CreatedBy)
	extra, err := serializeMasterBlockExtra(
		block.Extra.InMsgDesc,
		block.Extra.OutMsgDesc,
		block.Extra.ShardAccountBlocks,
		randSeed,
		createdBy,
		masterExtra,
	)
	if err != nil {
		t.Fatal(err)
	}
	root, err := serializeBlock(
		block.GlobalID,
		block.BlockInfo,
		block.ValueFlow,
		block.StateUpdate,
		extra,
	)
	if err != nil {
		t.Fatal(err)
	}
	rewriteVerificationCandidateBOC(t, tampered, root)

	verification := MasterVerificationRequest{
		Previous:           fixture.request.Previous,
		Config:             fixture.request.Config,
		Groups:             fixture.request.Groups,
		ShardTops:          fixture.request.ShardTops,
		Neighbors:          fixture.request.Neighbors,
		NeighborShardEndLT: fixture.request.NeighborShardEndLT,
		Semantics:          NewSemanticVerifier(tvm.NewTVM()),
		Candidate:          control,
	}
	if err = VerifyMasterCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify untampered master candidate: %v", err)
	}

	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	verification.Candidate = tampered
	err = VerifyMasterCandidate(context.Background(), verification)
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("master special msg_envelope_v2 verification = %v, want %v", err, ErrInvalidInput)
	}
	if !strings.Contains(err.Error(), "custom emitted lt") {
		t.Fatalf("master special msg_envelope_v2 rejected for the wrong reason: %v", err)
	}
}
