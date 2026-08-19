package collator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

type acceptingCandidateTransitionVerifier struct{}

func (acceptingCandidateTransitionVerifier) VerifyCandidateTransition(context.Context, CandidateTransition) error {
	return nil
}

var testCandidateTransitionVerifier acceptingCandidateTransitionVerifier

func shardVerificationRequest(req ShardRequest, candidate *Candidate) ShardVerificationRequest {
	return ShardVerificationRequest{
		Previous:    req.Previous,
		Previous2:   req.Previous2,
		Masterchain: req.Masterchain,
		Neighbors:   req.Neighbors,
		Semantics:   testCandidateTransitionVerifier,
		Candidate:   candidate,
	}
}

func TestVerifyShardCandidate(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	verification := shardVerificationRequest(req, candidate)
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify valid shard candidate: %v", err)
	}
	missingSemantics := verification
	missingSemantics.Semantics = nil
	if err = VerifyShardCandidate(context.Background(), missingSemantics); err == nil ||
		!strings.Contains(err.Error(), "transition verifier is absent") {
		t.Fatalf("VerifyShardCandidate missing semantic verifier error = %v", err)
	}
	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	if err = VerifyShardCandidate(cancelled, verification); !errors.Is(err, context.Canceled) {
		t.Fatalf("VerifyShardCandidate cancelled context error = %v", err)
	}
	missingConfig := verification
	missingConfig.Masterchain.Config = nil
	if err = VerifyShardCandidate(context.Background(), missingConfig); err == nil ||
		!strings.Contains(err.Error(), "verification config is absent") {
		t.Fatalf("VerifyShardCandidate nil config error = %v", err)
	}
	tests := []struct {
		name    string
		wantErr string
		mutate  func(*testing.T, *Candidate)
	}{
		{
			name:    "declared root hash",
			wantErr: "root hash mismatch",
			mutate: func(_ *testing.T, candidate *Candidate) {
				candidate.ID.RootHash[0] ^= 1
			},
		},
		{
			name:    "declared file hash",
			wantErr: "file hash mismatch",
			mutate: func(_ *testing.T, candidate *Candidate) {
				candidate.ID.FileHash[0] ^= 1
			},
		},
		{
			name:    "candidate id",
			wantErr: "candidate id differs",
			mutate: func(_ *testing.T, candidate *Candidate) {
				candidate.ID.SeqNo++
			},
		},
		{
			name:    "candidate state",
			wantErr: "state hash differs",
			mutate: func(_ *testing.T, candidate *Candidate) {
				candidate.State = req.Previous.State
			},
		},
		{
			name:    "candidate state update",
			wantErr: "state update differs",
			mutate: func(_ *testing.T, candidate *Candidate) {
				candidate.StateUpdate = req.Previous.State
			},
		},
		{
			name:    "candidate creator",
			wantErr: "creator differs",
			mutate: func(_ *testing.T, candidate *Candidate) {
				candidate.CreatedBy[0] ^= 1
			},
		},
		{
			name:    "collated data hash",
			wantErr: "collated data file hash mismatch",
			mutate: func(_ *testing.T, candidate *Candidate) {
				candidate.CollatedFileHash[0] ^= 1
			},
		},
		{
			name:    "state header",
			wantErr: "state header differs",
			mutate: func(t *testing.T, candidate *Candidate) {
				rewriteVerificationShardBlock(t, candidate, func(block *tlb.Block) {
					block.BlockInfo.EndLt++
				})
			},
		},
		{
			name:    "descriptor dictionary",
			wantErr: "invalid inbound message descriptors",
			mutate: func(t *testing.T, candidate *Candidate) {
				rewriteVerificationShardBlock(t, candidate, func(block *tlb.Block) {
					block.Extra.InMsgDesc = cell.BeginCell().MustStoreUInt(1, 1).EndCell()
				})
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			tampered := cloneVerificationCandidate(candidate)
			test.mutate(t, tampered)

			changed := verification
			changed.Candidate = tampered
			err := VerifyShardCandidate(context.Background(), changed)
			if err == nil || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("VerifyShardCandidate error = %v, want containing %q", err, test.wantErr)
			}
		})
	}

	t.Run("known collated roots may be reordered", func(t *testing.T) {
		prefixed := cloneVerificationCandidate(candidate)
		roots, rootsErr := cell.FromBOCMultiRoot(prefixed.CollatedData)
		if rootsErr != nil {
			t.Fatal(rootsErr)
		}
		rewriteVerificationCollatedData(
			t,
			prefixed,
			append([]*cell.Cell{verificationTopBlockDescrSet(t, nil)}, roots...)...,
		)
		changed := verification
		changed.Candidate = prefixed
		if verifyErr := VerifyShardCandidate(context.Background(), changed); verifyErr != nil {
			t.Fatalf("verify candidate with reordered collated roots: %v", verifyErr)
		}
	})

	t.Run("duplicate proof virtual roots", func(t *testing.T) {
		tampered := cloneVerificationCandidate(candidate)
		roots, rootsErr := cell.FromBOCMultiRoot(tampered.CollatedData)
		if rootsErr != nil {
			t.Fatal(rootsErr)
		}
		proof, proofErr := cell.CreateMerkleProof(cell.BeginCell().MustStoreUInt(7, 8).EndCell())
		if proofErr != nil {
			t.Fatal(proofErr)
		}
		withUniqueProof := cloneVerificationCandidate(candidate)
		rewriteVerificationCollatedData(
			t,
			withUniqueProof,
			append([]*cell.Cell{proof}, roots...)...,
		)
		unique := verification
		unique.Candidate = withUniqueProof
		if verifyErr := VerifyShardCandidate(context.Background(), unique); verifyErr == nil ||
			!strings.Contains(verifyErr.Error(), "predecessor block proof is absent") {
			t.Fatalf("unbound full collated proof error = %v", verifyErr)
		}

		rewriteVerificationCollatedData(
			t,
			tampered,
			append([]*cell.Cell{proof, proof}, roots...)...,
		)
		changed := verification
		changed.Candidate = tampered
		if verifyErr := VerifyShardCandidate(context.Background(), changed); verifyErr == nil ||
			!strings.Contains(verifyErr.Error(), "duplicate collated proof virtual root") {
			t.Fatalf("duplicate proof error = %v", verifyErr)
		}
	})

	t.Run("candidate size limits", func(t *testing.T) {
		blockConfig := *req.Masterchain.Config
		blockConfig.maxBlockBytes = uint32(len(candidate.BlockBOC) - 1)
		oversizedBlock := verification
		oversizedBlock.Masterchain.Config = &blockConfig
		if verifyErr := VerifyShardCandidate(context.Background(), oversizedBlock); !errors.Is(verifyErr, ErrSizeLimit) {
			t.Fatalf("oversized block error = %v", verifyErr)
		}

		collatedConfig := *req.Masterchain.Config
		collatedConfig.maxCollatedBytes = uint32(len(candidate.CollatedData) - 1)
		oversizedCollated := verification
		oversizedCollated.Masterchain.Config = &collatedConfig
		if verifyErr := VerifyShardCandidate(context.Background(), oversizedCollated); !errors.Is(verifyErr, ErrSizeLimit) {
			t.Fatalf("oversized collated data error = %v", verifyErr)
		}
	})
}

func TestVerifyShardValueFlowRejectsMasterchainOnlyComponents(t *testing.T) {
	req := emptyCandidateRequest(t)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyCandidate(context.Background(), req.Masterchain.Config, candidate)
	if err != nil {
		t.Fatal(err)
	}
	predecessor, err := preparePredecessor(req, predecessorReadSet(t, req))
	if err != nil {
		t.Fatal(err)
	}
	if err = verifyShardValueFlow(req.Masterchain.Config, &predecessor, &verified); err != nil {
		t.Fatalf("valid shard flow: %v", err)
	}

	nonZero := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(1)}
	tests := []struct {
		name   string
		mutate func(*tlb.ValueFlow)
	}{
		{name: "minted", mutate: func(flow *tlb.ValueFlow) { flow.Minted = nonZero }},
		{name: "recovered", mutate: func(flow *tlb.ValueFlow) { flow.Recovered = nonZero }},
		{name: "burned", mutate: func(flow *tlb.ValueFlow) { flow.Burned = nonZero }},
		{name: "fees imported", mutate: func(flow *tlb.ValueFlow) { flow.FeesImported = nonZero }},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := verified
			test.mutate(&changed.flow)
			if err := verifyShardValueFlow(req.Masterchain.Config, &predecessor, &changed); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("validation error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestVerifyShardValueFlowBindsStaticComponents(t *testing.T) {
	req := emptyCandidateRequest(t)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyCandidate(context.Background(), req.Masterchain.Config, candidate)
	if err != nil {
		t.Fatal(err)
	}
	predecessor, err := preparePredecessor(req, predecessorReadSet(t, req))
	if err != nil {
		t.Fatal(err)
	}
	nonZero := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(1)}
	tests := []struct {
		name   string
		mutate func(*verifiedCandidate)
	}{
		{name: "creation fee", mutate: func(candidate *verifiedCandidate) { candidate.flow.Created = tlb.CurrencyCollection{} }},
		{name: "predecessor balance", mutate: func(candidate *verifiedCandidate) { candidate.flow.FromPrevBlock = nonZero }},
		{name: "imported value", mutate: func(candidate *verifiedCandidate) { candidate.flow.Imported = nonZero }},
		{name: "exported value", mutate: func(candidate *verifiedCandidate) { candidate.flow.Exported = nonZero }},
		{name: "collected fees", mutate: func(candidate *verifiedCandidate) { candidate.flow.FeesCollected = tlb.CurrencyCollection{} }},
		{name: "validator fees", mutate: func(candidate *verifiedCandidate) { candidate.stats.TotalValidatorFees = tlb.CurrencyCollection{} }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := verified
			test.mutate(&changed)
			if err := verifyShardValueFlow(req.Masterchain.Config, &predecessor, &changed); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("validation error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func TestVerifyShardMinRefMCSeqnoUsesProcessedMinimum(t *testing.T) {
	const masterSeqno = uint32(50)
	queue := tlb.OutMsgQueueInfo{
		ProcInfo: processedDictionary(t, 0x8000000000000000, masterSeqno-7, processedValue(100)),
	}
	var header tlb.BlockHeader
	header.Shard = mustPredecessorIdent(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	header.MasterRef = &tlb.ExtBlkRef{SeqNo: masterSeqno}
	header.MinRefMcSeqno = masterSeqno - 7
	if err := verifyShardMinRefMCSeqno(&header, &queue); err != nil {
		t.Fatal(err)
	}

	header.MinRefMcSeqno++
	if err := verifyShardMinRefMCSeqno(&header, &queue); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("wrong min_ref_mc_seqno error = %v", err)
	}
}

func TestVerifyAccountsShardPrefix(t *testing.T) {
	account := address.NewAddress(0, 0, bytes.Repeat([]byte{0x80}, 32))
	accounts := accountsWithActiveContract(t, account, 100, 1_000_000_000)
	root := mustPredecessorIdent(t, groups.ShardID{Workchain: 0, Shard: -1 << 63})
	if err := verifyAccountsShardPrefix(root, accounts); err != nil {
		t.Fatal(err)
	}
	left := mustPredecessorIdent(t, groups.ShardID{Workchain: 0, Shard: 0x4000000000000000})
	if err := verifyAccountsShardPrefix(left, accounts); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("out-of-shard account error = %v", err)
	}
}

func TestVerifyOutQueueSizePresenceFollowsCapability(t *testing.T) {
	config := loadMainnetConfig(t)
	withoutSize := tlb.OutMsgQueueInfo{}
	config.capabilities &^= capStoreOutMsgQueueSize
	if err := verifyOutQueueSizePresence(config, &withoutSize); err != nil {
		t.Fatal(err)
	}

	size := uint64(0)
	withSize := tlb.OutMsgQueueInfo{Extra: &tlb.OutMsgQueueExtra{OutQueueSize: &size}}
	if err := verifyOutQueueSizePresence(config, &withSize); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unexpected stored size error = %v", err)
	}
	config.capabilities |= capStoreOutMsgQueueSize
	if err := verifyOutQueueSizePresence(config, &withSize); err != nil {
		t.Fatal(err)
	}
	if err := verifyOutQueueSizePresence(config, &withoutSize); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing stored size error = %v", err)
	}
}

func TestVerifyShardCandidateWithTransactions(t *testing.T) {
	req := emptyCandidateRequest(t)
	account := address.NewAddress(0, 0, bytes.Repeat([]byte{0x51}, 32))
	req.Previous.State = stateWithAccounts(
		t,
		req.Previous.State,
		accountsWithActiveContract(t, account, req.Header.GenUtime, 100_000_000_000),
	)
	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: account,
		Body:    cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, message)}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if err = VerifyShardCandidate(context.Background(), shardVerificationRequest(req, candidate)); err != nil {
		t.Fatalf("verify candidate with descriptors and account block: %v", err)
	}
}

func TestVerifyMessageDescriptorsUseConfigVersion(t *testing.T) {
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x61}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x62}, 32))
	message, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     source,
		DstAddr:     destination,
		Amount:      tlb.FromNanoTONU(1_000_000),
		IHRFee:      tlb.FromNanoTONU(30),
		FwdFee:      tlb.FromNanoTONU(20),
		CreatedLT:   1_000_000,
		CreatedAt:   1_900_000_000,
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{},
		NextAddr:        tlb.IntermediateAddress{UseDestBits: 96},
		FwdFeeRemaining: tlb.FromNanoTONU(11),
		Msg:             message,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	dummyTransaction := cell.BeginCell().MustStoreUInt(0, 1).EndCell()
	inDescriptor, err := descriptorFee(
		0b100,
		3,
		envelope,
		dummyTransaction,
		tlb.FromNanoTONU(11),
	)
	if err != nil {
		t.Fatal(err)
	}
	outDescriptor, err := descriptor(0b001, 3, envelope, dummyTransaction)
	if err != nil {
		t.Fatal(err)
	}

	inMessages, err := tlb.NewInMsgDescrAugDict(11)
	if err != nil {
		t.Fatal(err)
	}
	if err = insertDescriptor(inMessages.AugmentedDictionary, message, inDescriptor); err != nil {
		t.Fatal(err)
	}
	inRoot, err := inMessages.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = verifyInMsgDescriptors(inRoot, 11); err != nil {
		t.Fatalf("verify version 11 inbound descriptor: %v", err)
	}
	if _, err = verifyInMsgDescriptors(inRoot, 12); err == nil ||
		!strings.Contains(err.Error(), "augmentation is invalid") {
		t.Fatalf("version 11 inbound descriptor under version 12 error = %v", err)
	}

	outMessages, err := tlb.NewOutMsgDescrAugDict(11)
	if err != nil {
		t.Fatal(err)
	}
	if err = insertDescriptor(outMessages.AugmentedDictionary, message, outDescriptor); err != nil {
		t.Fatal(err)
	}
	outRoot, err := outMessages.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	if _, err = verifyOutMsgDescriptors(outRoot, 11); err != nil {
		t.Fatalf("verify version 11 outbound descriptor: %v", err)
	}
	if _, err = verifyOutMsgDescriptors(outRoot, 12); err == nil ||
		!strings.Contains(err.Error(), "augmentation is invalid") {
		t.Fatalf("version 11 outbound descriptor under version 12 error = %v", err)
	}
}

// TestVerifyRequeuedTransitOutDescriptor pins msg_export_tr_req$111 to the shape
// block.tlb gives it — out_msg:^MsgEnvelope imported:^InMsg, no trailing value.
// The descriptor is built through the same helper the requeue branch of
// transitMessage uses, so the test breaks if either side of the pair moves.
// Nothing else reaches that branch: it only fires when a transit message lands
// back in our own shard, which is why a validator that demanded an extra
// CurrencyCollection here rejected our own blocks unnoticed. The assertion runs
// against the semantic decoders because they are the only ones left that decode
// descriptor entries on the validation path.
func TestVerifyRequeuedTransitOutDescriptor(t *testing.T) {
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x63}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x64}, 32))
	message, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     source,
		DstAddr:     destination,
		Amount:      tlb.FromNanoTONU(2_000_000),
		IHRFee:      tlb.FromNanoTONU(30),
		FwdFee:      tlb.FromNanoTONU(20),
		CreatedLT:   2_000_000,
		CreatedAt:   1_900_000_000,
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	incoming, err := (tlb.MsgEnvelope{
		NextAddr:        tlb.IntermediateAddress{UseDestBits: 96},
		FwdFeeRemaining: tlb.FromNanoTONU(11),
		Msg:             message,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	forwarded, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{UseDestBits: 96},
		NextAddr:        tlb.IntermediateAddress{UseDestBits: 96},
		FwdFeeRemaining: tlb.FromNanoTONU(7),
		Msg:             message,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	// The pair transitMessage writes when the next hop re-enters our shard:
	// msg_import_tr$101 inbound, msg_export_tr_req$111 outbound.
	inDescriptor, err := descriptorFee(0b101, 3, incoming, forwarded, tlb.FromNanoTONU(4))
	if err != nil {
		t.Fatal(err)
	}
	outDescriptor, err := descriptor(0b111, 3, forwarded, inDescriptor)
	if err != nil {
		t.Fatal(err)
	}

	key := [32]byte(message.HashKey())
	out, err := parseSemanticOutDescriptor(*outDescriptor.MustBeginParse(), key)
	if err != nil {
		t.Fatalf("verify requeued transit outbound descriptor: %v", err)
	}
	if out.tag != semanticOutTransitRequest {
		t.Fatalf("requeued transit outbound tag = %d, want %d", out.tag, semanticOutTransitRequest)
	}

	in, err := parseSemanticInDescriptor(*inDescriptor.MustBeginParse(), key)
	if err != nil {
		t.Fatalf("verify requeued transit inbound descriptor: %v", err)
	}
	if in.tag != semanticInTransit {
		t.Fatalf("requeued transit inbound tag = %d, want %d", in.tag, semanticInTransit)
	}

	// And the other direction: a trailing CurrencyCollection is not part of the
	// constructor, so a descriptor carrying one must be rejected rather than
	// tolerated. That is the exact shape the deleted structural parser demanded.
	withFee, err := descriptorFee(0b111, 3, forwarded, inDescriptor, tlb.FromNanoTONU(4))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = parseSemanticOutDescriptor(*withFee.MustBeginParse(), key); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("requeued transit outbound descriptor with a fee error = %v, want invalid input", err)
	}
}

func TestVerifyLoadHistory(t *testing.T) {
	tests := []struct {
		name            string
		previous        tlb.ShardStateStats
		next            tlb.ShardStateStats
		header          tlb.BlockHeader
		topologyChanged bool
		wantErr         string
	}{
		{name: "linear", previous: tlb.ShardStateStats{OverloadHistory: 3, UnderloadHistory: 4}, next: tlb.ShardStateStats{OverloadHistory: 6, UnderloadHistory: 8}},
		{name: "topology reset", next: tlb.ShardStateStats{OverloadHistory: 1}, topologyChanged: true},
		{name: "both load classes", next: tlb.ShardStateStats{OverloadHistory: 1, UnderloadHistory: 1}, topologyChanged: true, wantErr: "both overloaded"},
		{name: "stale topology history", next: tlb.ShardStateStats{OverloadHistory: 2}, topologyChanged: true, wantErr: "not cleared"},
		{name: "linear history mismatch", previous: tlb.ShardStateStats{OverloadHistory: 2}, next: tlb.ShardStateStats{OverloadHistory: 2}, wantErr: "does not follow"},
		{name: "header wish mismatch", header: func() tlb.BlockHeader {
			var header tlb.BlockHeader
			header.WantSplit = true
			return header
		}(), wantErr: "wishes disagree"},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := verifyLoadHistory(&test.previous, &test.next, &test.header, test.topologyChanged)
			if test.wantErr == "" && err != nil {
				t.Fatal(err)
			}
			if test.wantErr != "" && (err == nil || !strings.Contains(err.Error(), test.wantErr)) {
				t.Fatalf("error = %v, want containing %q", err, test.wantErr)
			}
		})
	}
}

func TestVerifyBlockLTDeltaUsesRawConfigLimitForBothChains(t *testing.T) {
	config := loadMainnetConfig(t)

	for _, master := range []bool{false, true} {
		limits := config.basechain.limits
		if master {
			limits = config.masterchain.limits
		}
		var header tlb.BlockHeader
		header.StartLt = 1_000_000
		header.EndLt = 1_000_201
		if err := verifyBlockLTDelta(config, &header, master); err != nil {
			t.Fatalf("raw limit rejected delayed-policy delta (master=%v): %v", master, err)
		}
		header.EndLt = header.StartLt + limits.ltDelta[3] + 1
		if err := verifyBlockLTDelta(config, &header, master); err == nil ||
			!strings.Contains(err.Error(), "hard limit") {
			t.Fatalf("raw hard limit error (master=%v) = %v", master, err)
		}
	}
}

func TestVerifyShardCandidateUsesMechanicalMergeRoot(t *testing.T) {
	fixture := newMergePredecessorFixture(t)
	candidate, err := testBuilder().BuildShard(context.Background(), fixture.req)
	if err != nil {
		t.Fatal(err)
	}
	verification := shardVerificationRequest(fixture.req, candidate)
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify valid merge candidate: %v", err)
	}

	withoutSecondRequest := verification
	withoutSecondRequest.Previous2 = nil
	withoutSecond := VerifyShardCandidate(context.Background(), withoutSecondRequest)
	if withoutSecond == nil {
		t.Fatal("merge candidate verified without its second predecessor")
	}

	swappedFirst := *fixture.req.Previous2
	swappedSecond := fixture.req.Previous
	swapped := verification
	swapped.Previous = swappedFirst
	swapped.Previous2 = &swappedSecond
	if err = VerifyShardCandidate(context.Background(), swapped); err == nil {
		t.Fatal("merge candidate verified with swapped predecessor order")
	}

	tampered := cloneVerificationCandidate(candidate)
	leftRootUpdate, err := cell.CreateMerkleUpdate(fixture.req.Previous.State, candidate.State)
	if err != nil {
		t.Fatal(err)
	}
	tampered.StateUpdate = leftRootUpdate
	rewriteVerificationShardBlock(t, tampered, func(block *tlb.Block) {
		block.StateUpdate = leftRootUpdate
	})
	tamperedRequest := verification
	tamperedRequest.Candidate = tampered
	err = VerifyShardCandidate(context.Background(), tamperedRequest)
	if err == nil || !strings.Contains(err.Error(), "does not apply to predecessor") {
		t.Fatalf("merge update rooted at one child error = %v", err)
	}
}

func TestVerifyMasterCandidate(t *testing.T) {
	request := newVerificationMasterCandidate(t)
	candidate := request.Candidate
	if err := VerifyMasterCandidate(context.Background(), request); err != nil {
		t.Fatalf("verify valid master candidate: %v", err)
	}
	missingSemantics := request
	missingSemantics.Semantics = nil
	if err := VerifyMasterCandidate(context.Background(), missingSemantics); err == nil ||
		!strings.Contains(err.Error(), "transition verifier is absent") {
		t.Fatalf("VerifyMasterCandidate missing semantic verifier error = %v", err)
	}
	missingConfig := request
	missingConfig.Config = nil
	if err := VerifyMasterCandidate(context.Background(), missingConfig); err == nil ||
		!strings.Contains(err.Error(), "verification config is absent") {
		t.Fatalf("VerifyMasterCandidate nil config error = %v", err)
	}

	t.Run("top block descriptor prefix", func(t *testing.T) {
		prefixed := cloneVerificationCandidate(candidate)
		roots, err := cell.FromBOCMultiRoot(prefixed.CollatedData)
		if err != nil {
			t.Fatal(err)
		}
		rewriteVerificationCollatedData(
			t,
			prefixed,
			append([]*cell.Cell{verificationTopBlockDescrSet(t, nil)}, roots...)...,
		)
		verification := request
		verification.Candidate = prefixed
		if err = VerifyMasterCandidate(context.Background(), verification); err != nil {
			t.Fatalf("verify master candidate with top block descriptor set: %v", err)
		}
	})

	t.Run("arbitrary collated prefix", func(t *testing.T) {
		prefixed := cloneVerificationCandidate(candidate)
		roots, err := cell.FromBOCMultiRoot(prefixed.CollatedData)
		if err != nil {
			t.Fatal(err)
		}
		unknown := cell.BeginCell().MustStoreUInt(0xdeadbeef, 32).EndCell()
		rewriteVerificationCollatedData(t, prefixed, append([]*cell.Cell{unknown}, roots...)...)
		verification := request
		verification.Candidate = prefixed
		if err = VerifyMasterCandidate(context.Background(), verification); err != nil {
			t.Fatalf("verify master candidate with unknown collated root: %v", err)
		}
	})

	t.Run("malformed top block descriptor set", func(t *testing.T) {
		prefixed := cloneVerificationCandidate(candidate)
		roots, err := cell.FromBOCMultiRoot(prefixed.CollatedData)
		if err != nil {
			t.Fatal(err)
		}
		malformed := cell.BeginCell().MustStoreUInt(0xdeadbeef, 32).EndCell()
		badValue := cell.BeginCell().MustStoreRef(malformed).EndCell()
		rewriteVerificationCollatedData(
			t,
			prefixed,
			append([]*cell.Cell{verificationTopBlockDescrSet(t, badValue)}, roots...)...,
		)
		verification := request
		verification.Candidate = prefixed
		err = VerifyMasterCandidate(context.Background(), verification)
		if err == nil || !strings.Contains(err.Error(), "invalid descriptor reference") {
			t.Fatalf("VerifyMasterCandidate malformed descriptor set error = %v", err)
		}
	})

	t.Run("masterchain id", func(t *testing.T) {
		tampered := cloneVerificationCandidate(candidate)
		tampered.ID.Workchain = 0
		verification := request
		verification.Candidate = tampered
		if err := VerifyMasterCandidate(context.Background(), verification); err == nil {
			t.Fatal("master candidate verified with a basechain id")
		}
	})

	t.Run("missing masterchain block extra", func(t *testing.T) {
		tampered := cloneVerificationCandidate(candidate)
		rewriteVerificationMasterBlock(t, tampered, nil)
		verification := request
		verification.Candidate = tampered
		err := VerifyMasterCandidate(context.Background(), verification)
		if err == nil || !strings.Contains(err.Error(), "has no masterchain block extra") {
			t.Fatalf("VerifyMasterCandidate error = %v", err)
		}
	})

	t.Run("state update source", func(t *testing.T) {
		tampered := cloneVerificationCandidate(candidate)
		other := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
		update, err := cell.CreateMerkleUpdate(other, candidate.State)
		if err != nil {
			t.Fatal(err)
		}
		tampered.StateUpdate = update
		rewriteVerificationMasterBlock(t, tampered, verificationMasterCustomCell(t, candidate), func(block *tlb.Block) {
			block.StateUpdate = update
		})
		verification := request
		verification.Candidate = tampered
		err = VerifyMasterCandidate(context.Background(), verification)
		if err == nil || !strings.Contains(err.Error(), "does not apply to predecessor") {
			t.Fatalf("VerifyMasterCandidate error = %v", err)
		}
	})
}

func TestVerifyTopBlockDescrSetEnforcesSharedTLBBudget(t *testing.T) {
	descriptor := masterBuildTopBlockDescr(t, testBlockID(0, -1<<63, 1, 0x5a))
	dict := cell.NewDict(96)
	for i := 0; i < topBlockDescrValidationBudget/3+1; i++ {
		key := cell.BeginCell().MustStoreUInt(uint64(i), 96).EndCell()
		value := cell.BeginCell().MustStoreRef(descriptor).EndCell()
		if err := dict.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	root := cell.BeginCell().
		MustStoreUInt(masterTopBlockDescrSetTag, 32).
		MustStoreDict(dict).
		EndCell()

	if err := verifyTopBlockDescrSet(root); err == nil || !strings.Contains(err.Error(), "exceeds") {
		t.Fatalf("over-budget TopBlockDescrSet error = %v", err)
	}
}

func cloneVerificationCandidate(candidate *Candidate) *Candidate {
	cloned := *candidate
	cloned.ID.RootHash = bytes.Clone(candidate.ID.RootHash)
	cloned.ID.FileHash = bytes.Clone(candidate.ID.FileHash)
	cloned.BlockBOC = bytes.Clone(candidate.BlockBOC)
	cloned.CollatedData = bytes.Clone(candidate.CollatedData)
	// These clones exist to be tampered with and handed to VerifyShardCandidate,
	// which is the exported entry point: a caller there assembles the Candidate
	// itself and cannot assert that this package took its file hashes, so the
	// clone must not inherit that claim from the build it was copied from.
	// Dropping it is what keeps every hash-tampering case in this file honest.
	cloned.digested = false
	return &cloned
}

func rewriteVerificationShardBlock(t *testing.T, candidate *Candidate, mutate func(*tlb.Block)) {
	t.Helper()

	root, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, root); err != nil {
		t.Fatal(err)
	}
	mutate(&block)
	root, err = tlb.ToCell(&block)
	if err != nil {
		t.Fatal(err)
	}
	rewriteVerificationCandidateBOC(t, candidate, root)
}

func rewriteVerificationCandidateBOC(t *testing.T, candidate *Candidate, root *cell.Cell) {
	t.Helper()

	boc, err := root.ToBOCWithOptionsErr(cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	fileHash := sha256.Sum256(boc)
	rootHash := root.HashKey(0)
	candidate.BlockBOC = boc
	candidate.ID.RootHash = bytes.Clone(rootHash[:])
	candidate.ID.FileHash = bytes.Clone(fileHash[:])
}

func rewriteVerificationCollatedData(t *testing.T, candidate *Candidate, roots ...*cell.Cell) {
	t.Helper()

	boc, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	candidate.CollatedData = boc
	candidate.CollatedFileHash = sha256.Sum256(boc)
}

func verificationTopBlockDescrSet(t *testing.T, value *cell.Cell) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(96)
	if value != nil {
		key := cell.BeginCell().MustStoreUInt(0, 32).MustStoreUInt(0, 64).EndCell()
		if err := dict.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}
	builder := cell.BeginCell().MustStoreUInt(0x4ac789f3, 32)
	if err := builder.StoreDict(dict); err != nil {
		t.Fatal(err)
	}

	return builder.EndCell()
}

func newVerificationMasterCandidate(t *testing.T) MasterVerificationRequest {
	t.Helper()

	fixture := newMasterBuildFixture(t, false)
	fixture.request.ShardTops = nil
	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}

	return MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    fixture.request.Config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Semantics: testCandidateTransitionVerifier,
		Candidate: candidate,
	}
}

func verificationBlockExtraCell(
	t *testing.T,
	inMessages *cell.Cell,
	outMessages *cell.Cell,
	accountBlocks *cell.Cell,
	seed [32]byte,
	creator [32]byte,
	custom *cell.Cell,
) *cell.Cell {
	t.Helper()

	builder := cell.BeginCell().
		MustStoreUInt(0x4a33f6fd, 32).
		MustStoreRef(inMessages).
		MustStoreRef(outMessages).
		MustStoreRef(accountBlocks).
		MustStoreSlice(seed[:], 256).
		MustStoreSlice(creator[:], 256)
	if err := builder.StoreMaybeRef(custom); err != nil {
		t.Fatal(err)
	}
	return builder.EndCell()
}

func verificationMasterCustomCell(t *testing.T, candidate *Candidate) *cell.Cell {
	t.Helper()

	root, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	extra := root.MustPeekRef(3)
	if extra.RefsNum() != 4 {
		t.Fatalf("master block extra refs = %d, want 4", extra.RefsNum())
	}
	return extra.MustPeekRef(3)
}

func rewriteVerificationMasterBlock(
	t *testing.T,
	candidate *Candidate,
	custom *cell.Cell,
	mutate ...func(*tlb.Block),
) {
	t.Helper()

	root, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		t.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, root); err != nil {
		t.Fatal(err)
	}
	for _, fn := range mutate {
		fn(&block)
	}
	headerRoot, err := block.BlockInfo.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	var seed [32]byte
	var creator [32]byte
	copy(seed[:], block.Extra.RandSeed)
	copy(creator[:], block.Extra.CreatedBy)
	extraRoot := verificationBlockExtraCell(
		t,
		block.Extra.InMsgDesc,
		block.Extra.OutMsgDesc,
		block.Extra.ShardAccountBlocks,
		seed,
		creator,
		custom,
	)
	blockRoot := cell.BeginCell().
		MustStoreUInt(0x11ef55aa, 32).
		MustStoreInt(int64(block.GlobalID), 32).
		MustStoreRef(headerRoot).
		MustStoreRef(block.ValueFlow).
		MustStoreRef(block.StateUpdate).
		MustStoreRef(extraRoot).
		EndCell()
	rewriteVerificationCandidateBOC(t, candidate, blockRoot)
}

// verifyStateTransition stopped materialising the applied state: it now rests
// on ValidateMerkleUpdate having proven the update internally consistent, plus
// the two endpoint pins. These are the pins — each must still reject on its
// own, because a candidate that slipped past either would be a block we sign
// and the network refuses to apply.
func TestVerifyStateTransitionPinsBothEndpoints(t *testing.T) {
	strangerState := benchmarkVerifyStrangerState(t)

	req, _ := benchMainnetRequest(t, 0)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("genuine candidate rejected: %v", err)
	}

	// A predecessor the update was not built from must fail the source pin.
	tampered := verification
	tampered.Previous.State = strangerState
	err = VerifyShardCandidate(context.Background(), tampered)
	if err == nil {
		t.Fatal("candidate accepted against a foreign predecessor state")
	}
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("foreign predecessor rejected with %v, want ErrInvalidInput", err)
	}

	// A candidate claiming a state the update does not produce must fail the
	// destination pin.
	tampered = verification
	forged := *candidate
	forged.State = strangerState
	tampered.Candidate = &forged
	err = VerifyShardCandidate(context.Background(), tampered)
	if err == nil {
		t.Fatal("candidate accepted with a state its update does not produce")
	}
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("forged state rejected with %v, want ErrInvalidInput", err)
	}
}

// benchmarkVerifyStrangerState is a valid shard state that no candidate in
// these tests was built from.
func benchmarkVerifyStrangerState(tb testing.TB) *cell.Cell {
	tb.Helper()

	req := emptyCandidateRequest(tb)
	return req.Previous.State
}
