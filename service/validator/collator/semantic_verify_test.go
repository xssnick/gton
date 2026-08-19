package collator

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/vmerr"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestSemanticVerifierReplaysOrdinaryTransaction(t *testing.T) {
	replay, message := semanticOrdinaryReplay(t)

	if err := replay.verifyAccounts(); err != nil {
		t.Fatalf("replay account transition: %v", err)
	}
	if len(replay.accounts) != 1 || len(replay.consumedIn) != 1 {
		t.Fatalf("replayed accounts=%d consumed inbound=%d", len(replay.accounts), len(replay.consumedIn))
	}
	if _, ok := replay.consumedIn[message.HashKey()]; !ok {
		t.Fatal("transaction inbound descriptor was not consumed")
	}
	if replay.replayedAccounts.RootCell().HashKey() != replay.candidate.state.Accounts.ShardAccounts.RootCell().HashKey() {
		t.Fatal("replayed account dictionary differs from candidate state")
	}
}

func TestSemanticVerifierUsesExactInboundMessageCell(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	account := address.NewAddress(0, 0, bytes.Repeat([]byte{0x91}, 32))
	req.Previous.State = stateWithAccounts(
		t,
		req.Previous.State,
		accountsWithActiveContract(t, account, req.Header.GenUtime, 100_000_000_000),
	)
	body := cell.BeginCell().MustStoreUInt(0xabcdef, 24).EndCell()
	message := cell.BeginCell().
		MustStoreUInt(0b10, 2).
		MustStoreAddr(nil).
		MustStoreAddr(account).
		MustStoreCoins(0).
		MustStoreBoolBit(false).
		MustStoreBoolBit(true).
		MustStoreRef(body).
		EndCell()
	req.Externals = []ExternalInput{externalInput(t, message)}

	replay := semanticPreparedShardReplay(t, req)
	if err := replay.verifyAccounts(); err != nil {
		t.Fatalf("replay transaction with explicit referenced body: %v", err)
	}
}

func TestSemanticVerifierAcceptsEmptyAccountDictionary(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	replay := semanticPreparedShardReplay(t, req)

	if err := replay.verifyAccounts(); err != nil {
		t.Fatalf("replay empty account dictionary: %v", err)
	}
}

func TestSemanticInternalMessageValueMatchesCXXIHRRules(t *testing.T) {
	extra := semanticTestCurrency(t, 0, 9).ExtraCurrencies
	message := &tlb.InternalMessage{
		Amount:          tlb.FromNanoTONU(100),
		ExtraCurrencies: extra,
		IHRFee:          tlb.FromNanoTONU(7),
	}

	legacy, err := semanticInternalMessageValue(message, tlb.FromNanoTONU(3), 11)
	if err != nil {
		t.Fatal(err)
	}
	if !legacy.Equals(semanticTestCurrency(t, 110, 9)) {
		t.Fatalf("legacy internal value = %s, want 110", legacy.Coins.String())
	}

	current, err := semanticInternalMessageValue(message, tlb.FromNanoTONU(3), 12)
	if err != nil {
		t.Fatal(err)
	}
	if !current.Equals(semanticTestCurrency(t, 103, 9)) {
		t.Fatalf("current internal value = %s, want 103", current.Coins.String())
	}
}

func TestSemanticTransactionValueFlowIsPerTransaction(t *testing.T) {
	previous := semanticTestCurrency(t, 100, 10)
	imported := semanticTestCurrency(t, 0, 2)
	next := semanticTestCurrency(t, 60, 7)
	exported := semanticTestCurrency(t, 35, 4)
	fees := semanticTestCurrency(t, 5, 1)

	if err := verifySemanticTransactionValueFlow(
		previous,
		next,
		imported,
		exported,
		fees,
		tlb.CurrencyCollection{},
	); err != nil {
		t.Fatalf("valid transaction flow: %v", err)
	}

	exported.Coins = tlb.FromNanoTONU(34)
	err := verifySemanticTransactionValueFlow(
		previous,
		next,
		imported,
		exported,
		fees,
		tlb.CurrencyCollection{},
	)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "currency flow mismatch") {
		t.Fatalf("imbalanced transaction flow error = %v", err)
	}
}

func TestSemanticVerifierRejectsUnacceptedExternalMessage(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	account := address.NewAddress(0, 0, bytes.Repeat([]byte{0x92}, 32))
	req.Previous.State = stateWithAccounts(
		t,
		req.Previous.State,
		activeContracts(t, req.Header.GenUtime, activeContract{
			address: account,
			code:    externalRejectCode(t),
			balance: 100_000_000_000,
		}),
	)
	replay := semanticPreparedShardReplay(t, req)

	var key [32]byte
	copy(key[:], account.Data())
	original, _, err := semanticLoadAccount(replay.previous.Accounts.ShardAccounts, key)
	if err != nil {
		t.Fatal(err)
	}
	prepared, err := tvm.PrepareAccount(original, account)
	if err != nil {
		t.Fatal(err)
	}
	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: account,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	var parsed tlb.Message
	if err = parseExact(&parsed, message); err != nil {
		t.Fatal(err)
	}
	transaction := &tlb.TransactionLean{
		LT:    replay.candidate.block.BlockInfo.StartLt + 1,
		Kind:  tlb.TransactionKindOrdinary,
		InMsg: message,
	}

	_, _, err = replay.replayTransaction(prepared, message, transaction)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "not accepted") {
		t.Fatalf("unaccepted external error = %v", err)
	}
}

func semanticTestCurrency(t *testing.T, nano, extra uint64) tlb.CurrencyCollection {
	t.Helper()

	value := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(nano)}
	if extra == 0 {
		return value
	}
	dictionary := cell.NewDict(32)
	if err := dictionary.SetIntKey(
		big.NewInt(1),
		cell.BeginCell().MustStoreBigVarUInt(new(big.Int).SetUint64(extra), 32).EndCell(),
	); err != nil {
		t.Fatal(err)
	}
	value.ExtraCurrencies = dictionary

	return value
}

func TestSemanticVerifierRejectsDescriptorTransactionMismatch(t *testing.T) {
	replay, message := semanticOrdinaryReplay(t)
	otherTransaction := cell.BeginCell().MustStoreUInt(0x77, 8).EndCell()

	_, err := replay.verifyInboundTransactionDescriptor(&semanticAccountLane{}, message, otherTransaction)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "binding mismatch") {
		t.Fatalf("descriptor mismatch error = %v", err)
	}
}

func TestSemanticVerifierExplicitlyRejectsUnproducedTransactions(t *testing.T) {
	replay, _ := semanticOrdinaryReplay(t)
	tests := []struct {
		name string
		kind tlb.TransactionKind
	}{
		{name: "storage", kind: tlb.TransactionKindStorage},
		{name: "split prepare", kind: tlb.TransactionKindSplitPrepare},
		{name: "split install", kind: tlb.TransactionKindSplitInstall},
		{name: "merge prepare", kind: tlb.TransactionKindMergePrepare},
		{name: "merge install", kind: tlb.TransactionKindMergeInstall},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, _, err := replay.replayTransaction(nil, nil, &tlb.TransactionLean{
				LT:   replay.candidate.block.BlockInfo.StartLt + 1,
				Kind: test.kind,
			})
			if !errors.Is(err, ErrUnsupported) {
				t.Fatalf("error = %v, want ErrUnsupported", err)
			}
		})
	}

	_, _, err := replay.replayTransaction(nil, nil, &tlb.TransactionLean{
		LT:   replay.candidate.block.BlockInfo.StartLt + 1,
		Kind: tlb.TransactionKindTickTock,
	})
	if !errors.Is(err, ErrUnsupported) || !strings.Contains(err.Error(), "shard tick/tock") {
		t.Fatalf("shard tick/tock error = %v", err)
	}
}

func TestSemanticVerifierRequiresStructurallyPreparedTransition(t *testing.T) {
	verifier := NewSemanticVerifier(tvm.NewTVM())
	err := verifier.VerifyCandidateTransition(context.Background(), CandidateTransition{})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "not prepared") {
		t.Fatalf("unprepared transition error = %v", err)
	}

	cancelled, cancel := context.WithCancel(context.Background())
	cancel()
	err = verifier.VerifyCandidateTransition(cancelled, CandidateTransition{})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled transition error = %v", err)
	}
}

func TestSemanticVerifierExecutionErrorTaxonomy(t *testing.T) {
	replay, message := semanticOrdinaryReplay(t)
	transaction := &tlb.TransactionLean{
		LT:    replay.candidate.block.BlockInfo.StartLt + 1,
		Kind:  tlb.TransactionKindOrdinary,
		InMsg: message,
	}

	_, _, err := replay.replayTransaction(nil, message, transaction)
	if !errors.Is(err, ErrSemanticExecution) {
		t.Fatalf("emulator failure error = %v, want ErrSemanticExecution", err)
	}
	if errors.Is(err, ErrInvalidInput) {
		t.Fatalf("emulator failure was classified as invalid candidate: %v", err)
	}

	err = classifySemanticExecutionError(vmerr.Virtualization(1))
	if !errors.Is(err, ErrInvalidInput) || errors.Is(err, ErrSemanticExecution) {
		t.Fatalf("pruned candidate proof error = %v, want ErrInvalidInput", err)
	}
}

// Gas is summed per account lane. A lane's own share never falls, so the block
// allowance is still enforced per transaction; the merge only catches blocks
// that cross it once the lanes are summed.
func TestSemanticVerifierGasAccounting(t *testing.T) {
	replay, _ := semanticOrdinaryReplay(t)
	replay.normalGasLimit = 10
	root := cell.BeginCell().MustStoreUInt(0x55, 8).EndCell()
	transaction := &tlb.TransactionLean{Kind: tlb.TransactionKindOrdinary}
	result := &tvm.TransactionExecutionResult{
		ExecutionResult: tvm.ExecutionResult{GasUsed: 11},
	}

	lane := &semanticAccountLane{}
	err := replay.recordTransactionGas(lane, root, transaction, result)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "aggregate gas") {
		t.Fatalf("gas limit error = %v", err)
	}

	lane = &semanticAccountLane{}
	err = replay.recordTransactionGas(lane, root, &tlb.TransactionLean{
		Kind: tlb.TransactionKindTickTock,
	}, result)
	if err != nil || lane.normalGas != 0 {
		t.Fatalf("tick/tock gas accounting: used=%d err=%v", lane.normalGas, err)
	}

	replay.transition.Config.globalVersion = 4
	replay.specialTxs[root.HashKey()] = struct{}{}
	lane = &semanticAccountLane{}
	err = replay.recordTransactionGas(lane, root, transaction, result)
	if err != nil || lane.normalGas != 0 {
		t.Fatalf("pre-v5 special transaction gas accounting: used=%d err=%v", lane.normalGas, err)
	}

	delete(replay.specialTxs, root.HashKey())
	replay.transition.Config.globalVersion = 5
	replay.candidate.block.BlockInfo.GenUtime = 1_700_000_000
	replay.candidate.block.BlockInfo.Shard.WorkchainID = 0
	override := address.MustParseRawAddr("0:FFBFD8F5AE5B2E1C7C3614885CB02145483DFAEE575F0DD08A72C366369211CD")
	var account [32]byte
	copy(account[:], override.Data())
	lane = &semanticAccountLane{key: account}
	err = replay.recordTransactionGas(lane, root, transaction, result)
	if err != nil || lane.normalGas != 0 {
		t.Fatalf("historical gas override accounting: used=%d err=%v", lane.normalGas, err)
	}

	replay.specials = newMasterSpecials([][32]byte{account})
	replay.specialGasLimit = 10
	lane = &semanticAccountLane{key: account}
	err = replay.recordTransactionGas(lane, root, transaction, result)
	if !errors.Is(err, ErrInvalidInput) || lane.specialGas != 11 {
		t.Fatalf("special account historical override accounting: used=%d err=%v", lane.specialGas, err)
	}
	replay.specials = masterSpecials{}

	replay.candidate.block.BlockInfo.GenUtime = 1_709_164_800
	lane = &semanticAccountLane{key: account}
	err = replay.recordTransactionGas(lane, root, transaction, result)
	if !errors.Is(err, ErrInvalidInput) || lane.normalGas != 11 {
		t.Fatalf("expired gas override accounting: used=%d err=%v", lane.normalGas, err)
	}
}

func TestMasterSpecialEnvelopeRoutingAndTagRules(t *testing.T) {
	var zero, destination [32]byte
	destination[0] = 0x51
	amount := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(7)}
	message := &tlb.InternalMessage{
		IHRDisabled: true,
		Bounce:      true,
		SrcAddr:     address.NewAddress(0, 0xff, zero[:]),
		DstAddr:     address.NewAddress(0, 0xff, destination[:]),
		Amount:      amount.Coins,
		CreatedLT:   100,
		CreatedAt:   200,
		Body:        cell.BeginCell().EndCell(),
	}
	messageRoot, err := message.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	// The canonical envelope is msg_envelope#4. A v2 envelope carrying neither
	// emitted_lt nor metadata spends the newer tag on an empty payload, which
	// MsgEnvelope::unpack refuses outright (block-parse.cpp:919), so the
	// reference rejects the block before check_special_message ever runs. The
	// control below is the same envelope under the #4 tag and must be accepted,
	// so the rejection is attributable to the tag and not to the fixture.
	canonical := tlb.MsgEnvelope{
		CurAddr:  tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
		NextAddr: tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
		Msg:      messageRoot,
	}
	envelopeRoot, err := canonical.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifyMasterSpecialEnvelope(envelopeRoot, amount, destination[:], msgpool.ShardIdent{
		Workchain: address.MasterchainID,
		Shard:     msgpool.ShardAll,
	}, message.CreatedLT, message.CreatedAt)
	if err != nil {
		t.Fatalf("canonical msg_envelope rejected: %v", err)
	}

	emptyV2 := canonical
	emptyV2.V2 = true
	envelopeRoot, err = emptyV2.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifyMasterSpecialEnvelope(envelopeRoot, amount, destination[:], msgpool.ShardIdent{
		Workchain: address.MasterchainID,
		Shard:     msgpool.ShardAll,
	}, message.CreatedLT, message.CreatedAt)
	if err == nil {
		t.Fatal("msg_envelope_v2 without emitted lt or metadata was accepted")
	}
	assertSemanticImmediateEmittedLTRejected(t, 99)

	nonRegular := tlb.IntermediateAddress{
		Type:      tlb.IntermediateAddressSimple,
		Workchain: address.MasterchainID,
		AddrPfx:   0x5100000000000000,
	}
	envelopeRoot, err = (tlb.MsgEnvelope{
		CurAddr:  nonRegular,
		NextAddr: nonRegular,
		Msg:      messageRoot,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifyMasterSpecialEnvelope(envelopeRoot, amount, destination[:], msgpool.ShardIdent{
		Workchain: address.MasterchainID,
		Shard:     msgpool.ShardAll,
	}, message.CreatedLT, message.CreatedAt)
	if err == nil {
		t.Fatal("special message with non-regular routing addresses was accepted")
	}

	anycastSource := zero
	anycastSource[0] = 0x80
	message.SrcAddr = address.NewAddress(0, 0xff, anycastSource[:]).WithAnycast(address.NewAnycast(1, []byte{0}))
	message.DstAddr = address.NewAddressVar(0, address.MasterchainID, 256, destination[:])
	messageRoot, err = message.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	envelopeRoot, err = (tlb.MsgEnvelope{
		CurAddr:  tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
		NextAddr: tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
		Msg:      messageRoot,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifyMasterSpecialEnvelope(envelopeRoot, amount, destination[:], msgpool.ShardIdent{
		Workchain: address.MasterchainID,
		Shard:     msgpool.ShardAll,
	}, message.CreatedLT, message.CreatedAt)
	if err != nil {
		t.Fatalf("valid rewritten anycast/addr_var rejected: %v", err)
	}

	message.SrcAddr = address.NewAddress(0, 0xff, zero[:]).WithAnycast(address.NewAnycast(1, []byte{0x80}))
	messageRoot, err = message.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	envelopeRoot, err = (tlb.MsgEnvelope{
		CurAddr:  tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
		NextAddr: tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
		Msg:      messageRoot,
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifyMasterSpecialEnvelope(envelopeRoot, amount, destination[:], msgpool.ShardIdent{
		Workchain: address.MasterchainID,
		Shard:     msgpool.ShardAll,
	}, message.CreatedLT, message.CreatedAt)
	if err == nil {
		t.Fatal("special message with non-zero rewritten source was accepted")
	}

	message.SrcAddr = address.NewAddress(0, 0xff, zero[:])
	message.DstAddr = address.NewAddress(0, 0xff, destination[:])
	messageBuilder := cell.BeginCell()
	err = tlb.StoreMessageWithLayout(messageBuilder, &tlb.Message{
		MsgType: tlb.MsgTypeInternal,
		Msg:     message,
	}, tlb.MessageLayout{BodyInRef: true})
	if err != nil {
		t.Fatal(err)
	}
	envelopeRoot, err = (tlb.MsgEnvelope{
		CurAddr:  tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
		NextAddr: tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: routingAddressBits},
		Msg:      messageBuilder.EndCell(),
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	_, err = verifyMasterSpecialEnvelope(envelopeRoot, amount, destination[:], msgpool.ShardIdent{
		Workchain: address.MasterchainID,
		Shard:     msgpool.ShardAll,
	}, message.CreatedLT, message.CreatedAt)
	if err == nil {
		t.Fatal("special message with referenced empty body was accepted")
	}
}

// assertSemanticImmediateEmittedLTRejected pins the msg_import_imm rule to being
// unconditional. The masterchain fee recovery and mint descriptors used to be
// exempt from it here, which let a candidate carrying emitted_lt on a special
// envelope through; check_special_message rejects exactly that shape
// (validate-query.cpp:6577), so the exemption was a consensus-safety gap.
func assertSemanticImmediateEmittedLTRejected(t *testing.T, emittedLT uint64) {
	t.Helper()

	specialRoot := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	extra := &tlb.McBlockExtra{}
	extra.Details.RecoverCreateMsg = specialRoot
	prefix := msgpool.AccountPrefix{Workchain: address.MasterchainID, Prefix: 0x5100000000000000}
	replay := &semanticReplay{candidate: &verifiedCandidate{block: tlb.Block{
		Extra: &tlb.BlockExtra{Custom: extra},
	}}}
	validation := semanticQueueValidation{
		replay: replay,
		target: msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll},
	}
	descriptor := &semanticInDescriptor{
		tag:         semanticInImmediate,
		root:        specialRoot,
		transaction: cell.BeginCell().EndCell(),
		envelope: &semanticEnvelope{
			value:       tlb.MsgEnvelope{EmittedLT: &emittedLT},
			source:      msgpool.AccountPrefix{Workchain: address.MasterchainID},
			destination: prefix,
			current:     prefix,
			next:        prefix,
		},
	}
	if err := validation.verifyInboundEnvelope(cell.Hash{}, descriptor); err == nil {
		t.Fatal("special immediate emitted_lt was accepted")
	}

	descriptor.root = cell.BeginCell().MustStoreUInt(0, 1).EndCell()
	if err := validation.verifyInboundEnvelope(cell.Hash{}, descriptor); err == nil {
		t.Fatal("ordinary immediate emitted_lt was accepted")
	}

	// Positive control: the very same special descriptor without emitted_lt is
	// still accepted, so the two rejections above are attributable to the field
	// and not to the fixture.
	descriptor.root = specialRoot
	descriptor.envelope.value.EmittedLT = nil
	if err := validation.verifyInboundEnvelope(cell.Hash{}, descriptor); err != nil {
		t.Fatalf("special immediate without emitted_lt rejected: %v", err)
	}
}

func TestSemanticVerifierBuildsMasterchainExecutionContext(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	recorder := &recordingCandidateTransitionVerifier{}
	if err = VerifyMasterCandidate(context.Background(), MasterVerificationRequest{
		Previous:           fixture.request.Previous,
		Config:             fixture.request.Config,
		Groups:             fixture.request.Groups,
		ShardTops:          fixture.request.ShardTops,
		Neighbors:          fixture.request.Neighbors,
		NeighborShardEndLT: fixture.request.NeighborShardEndLT,
		Semantics:          recorder,
		Candidate:          candidate,
	}); err != nil {
		t.Fatalf("prepare masterchain transition: %v", err)
	}
	replay, err := newSemanticReplay(context.Background(), NewSemanticVerifier(tvm.NewTVM()), recorder.transition)
	if err != nil {
		t.Fatalf("prepare masterchain semantic replay: %v", err)
	}
	if replay.blockContext == nil || replay.blockContext.BlockLT() != int64(replay.candidate.block.BlockInfo.StartLt) {
		t.Fatal("masterchain block context does not bind block logical time")
	}
	queues, err := replay.prepareQueueValidation()
	if err != nil {
		t.Fatalf("prepare masterchain queue validation: %v", err)
	}
	if err = replay.verifyAccounts(); err != nil {
		t.Fatalf("replay masterchain accounts: %v", err)
	}
	if err = queues.verifyAfterReplay(); err != nil {
		t.Fatalf("verify masterchain queues: %v", err)
	}
	if err = replay.verifyMasterSemantics(); err != nil {
		t.Fatalf("verify masterchain semantics: %v", err)
	}

	originalRecovered := replay.candidate.flow.Recovered
	replay.candidate.flow.Recovered, err = originalRecovered.Add(tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(1)})
	if err != nil {
		t.Fatal(err)
	}
	err = replay.verifyMasterSpecialMessages()
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("altered recovery message error = %v", err)
	}
	replay.candidate.flow.Recovered = originalRecovered
}

func TestSemanticMessageProcessingOrder(t *testing.T) {
	var account [32]byte
	account[0] = 0x11
	err := checkSemanticMessageOrder([]semanticMessageProcessing{
		{account: account, transactionLT: 101, messageLT: 20},
		{account: account, transactionLT: 102, messageLT: 19},
	}, nil)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "earlier message") {
		t.Fatalf("message processing order error = %v", err)
	}

	err = checkSemanticMessageOrder(nil, []semanticMessageEmission{
		{source: account, createdLT: 20, emittedLT: 30},
		{source: account, createdLT: 21, emittedLT: 30},
	})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "non-increasing") {
		t.Fatalf("message emission order error = %v", err)
	}
}

func TestSemanticMasterTickTockConstraints(t *testing.T) {
	var account [32]byte
	account[0] = 0x22
	replay := &semanticReplay{
		candidate: &verifiedCandidate{},
		specials:  newMasterSpecials([][32]byte{account}),
	}
	replay.candidate.block.BlockInfo.StartLt = 100

	valid := &semanticAccountResult{
		sequence: []semanticTransactionResult{{
			lt:           101,
			tickTock:     true,
			beforeActive: true,
			afterActive:  true,
			tickEnabled:  true,
		}},
	}
	if err := replay.verifyMasterAccountTickTock(account, valid); err != nil {
		t.Fatalf("valid tick transaction: %v", err)
	}

	err := replay.verifyMasterAccountTickTock(account, &semanticAccountResult{
		sequence: []semanticTransactionResult{{
			lt:           102,
			tickTock:     true,
			beforeActive: true,
			afterActive:  true,
			tickEnabled:  true,
		}},
	})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "block lt + 1") {
		t.Fatalf("tick logical time error = %v", err)
	}

	err = replay.verifyMasterAccountTickTock(account, &semanticAccountResult{
		sequence: []semanticTransactionResult{{
			lt:           101,
			beforeActive: true,
			afterActive:  true,
			tockEnabled:  true,
		}},
	})
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "does not end with tock") {
		t.Fatalf("mandatory tock error = %v", err)
	}
}

func TestSemanticMasterRequiresConfiguredTickTockAccountBlock(t *testing.T) {
	var account [32]byte
	account[0] = 0x24
	addr := address.NewAddress(0, 0xff, account[:])
	accountRoot, err := (tlb.AccountState{
		IsValid: true,
		Address: addr,
		StorageInfo: tlb.StorageInfo{
			StorageUsed: tlb.StorageUsed{
				CellsUsed: new(big.Int),
				BitsUsed:  new(big.Int),
			},
			StorageExtra: tlb.StorageExtraNone{},
		},
		AccountStorage: tlb.AccountStorage{
			Status:  tlb.AccountStatusActive,
			Balance: tlb.FromNanoTONU(1),
			StateInit: &tlb.StateInit{
				TickTock: &tlb.TickTock{Tick: true},
			},
		},
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	shardAccount, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       accountRoot,
		LastTransHash: make([]byte, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	if err = accounts.Set(
		cell.BeginCell().MustStoreSlice(account[:], 256).EndCell(),
		shardAccount,
	); err != nil {
		t.Fatal(err)
	}
	previous := &tlb.ShardStateUnsplit{}
	previous.Accounts.ShardAccounts = accounts
	replay := &semanticReplay{
		previous: previous,
		accounts: make(map[[32]byte]*semanticAccountResult),
		specials: newMasterSpecials([][32]byte{account}),
	}

	err = replay.verifyMasterTickTock()
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "has no account block") {
		t.Fatalf("missing configured tick account error = %v", err)
	}
}

func TestSemanticMasterRejectsUnexplainedLibraryPublisherDelta(t *testing.T) {
	previousStatsRoot, err := (tlb.ShardStateStats{Libraries: cell.NewDict(256)}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	library := cell.BeginCell().MustStoreUInt(0x77, 8).EndCell()
	libraryKey := library.HashKey()
	publisher := [32]byte{0x33}
	publishers := cell.NewDict(256)
	if err = publishers.Set(
		cell.BeginCell().MustStoreSlice(publisher[:], 256).EndCell(),
		cell.BeginCell().EndCell(),
	); err != nil {
		t.Fatal(err)
	}
	descriptor, err := serializeGlobalLibrary(globalLibrary{root: library, publishers: publishers})
	if err != nil {
		t.Fatal(err)
	}
	libraries := cell.NewDict(256)
	if err = libraries.Set(
		cell.BeginCell().MustStoreSlice(libraryKey[:], 256).EndCell(),
		descriptor,
	); err != nil {
		t.Fatal(err)
	}
	replay := &semanticReplay{
		previous:  &tlb.ShardStateUnsplit{Stats: previousStatsRoot},
		candidate: &verifiedCandidate{stats: tlb.ShardStateStats{Libraries: libraries}},
		accounts:  make(map[[32]byte]*semanticAccountResult),
	}

	err = replay.verifyMasterPublicLibraries()
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "publisher delta") {
		t.Fatalf("library publisher delta error = %v", err)
	}
}

func TestSemanticMasterBurnedValueIncludesBlackholeReplay(t *testing.T) {
	replay := &semanticReplay{
		transition: CandidateTransition{prepared: &preparedCandidateTransition{
			minimumBurned: tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(5)},
		}},
		candidate: &verifiedCandidate{flow: tlb.ValueFlow{
			Burned: tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(7)},
		}},
		blackholeBurned: tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(2)},
	}
	if err := replay.verifyMasterBurnedValue(); err != nil {
		t.Fatalf("verify exact burned value: %v", err)
	}

	replay.candidate.flow.Burned = tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(8)}
	err := replay.verifyMasterBurnedValue()
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "burned value") {
		t.Fatalf("excess burned value error = %v", err)
	}
}

func BenchmarkSemanticVerifierOrdinaryTransaction(b *testing.B) {
	replay, _ := semanticOrdinaryReplay(b)
	transition := replay.transition
	verifier := NewSemanticVerifier(tvm.NewTVM())
	b.ReportAllocs()
	b.ResetTimer()

	for range b.N {
		if err := verifier.VerifyCandidateTransition(context.Background(), transition); err != nil {
			b.Fatal(err)
		}
	}
}

func semanticOrdinaryReplay(tb testing.TB) (*semanticReplay, *cell.Cell) {
	tb.Helper()
	req := emptyCandidateRequest(tb)
	req.Internals = &msgpool.Cut{}
	account := address.NewAddress(0, 0, bytes.Repeat([]byte{0x90}, 32))
	req.Previous.State = stateWithAccounts(
		tb,
		req.Previous.State,
		accountsWithActiveContract(tb, account, req.Header.GenUtime, 100_000_000_000),
	)
	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: account,
		Body:    cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell(),
	})
	if err != nil {
		tb.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(tb, message)}

	return semanticPreparedShardReplay(tb, req), message
}

func semanticPreparedShardReplay(tb testing.TB, req ShardRequest) *semanticReplay {
	tb.Helper()
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		tb.Fatal(err)
	}
	recorder := &recordingCandidateTransitionVerifier{}
	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = recorder
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		tb.Fatalf("prepare shard transition: %v", err)
	}
	replay, err := newSemanticReplay(context.Background(), NewSemanticVerifier(tvm.NewTVM()), recorder.transition)
	if err != nil {
		tb.Fatalf("prepare shard semantic replay: %v", err)
	}
	// The lanes read the descriptor maps this fills. Skipping it would let the
	// tests replay against descriptor semantics production never sees.
	if _, err = replay.prepareQueueValidation(); err != nil {
		tb.Fatalf("prepare shard queue validation: %v", err)
	}

	return replay
}

// TestPrecheckedAccountMatchesLoadedAccount pins the equivalence the account
// cache rests on. replayAccount consumes whatever precheckAccountUpdates
// decoded out of the structural diff instead of descending the shard's account
// trie again, so the two decoders have to agree on the value — and on the
// existence bit, which the value alone cannot carry: the placeholder built for
// an absent leaf is byte-identical to a real empty account.
func TestPrecheckedAccountMatchesLoadedAccount(t *testing.T) {
	previous := testAccountDiffDictionary(t, map[byte]uint64{0x01: 5, 0x02: 7})
	candidate := testAccountDiffDictionary(t, map[byte]uint64{0x01: 9, 0x03: 11})

	cached := make(map[[32]byte]semanticCachedAccount)
	err := previous.ScanDiff(candidate.AugmentedDictionary, true, func(
		keyCell *cell.Cell,
		oldValueExtra, newValueExtra *cell.Slice,
	) error {
		keyLoader, err := keyCell.BeginParse()
		if err != nil {
			return err
		}
		var key [32]byte
		if err = keyLoader.LoadSliceInto(key[:], 256); err != nil {
			return err
		}
		account, err := semanticDiffAccount(oldValueExtra)
		if err != nil {
			return err
		}
		cached[key] = semanticCachedAccount{account: account, exists: oldValueExtra != nil}

		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	// One changed leaf, one the candidate drops and one it creates: the third is
	// the case where the existence bit is the only difference.
	if len(cached) != 3 {
		t.Fatalf("diff reported %d changed accounts, want 3", len(cached))
	}

	for key, entry := range cached {
		loaded, exists, err := semanticLoadAccount(previous, key)
		if err != nil {
			t.Fatalf("load predecessor account %x: %v", key, err)
		}
		if exists != entry.exists {
			t.Fatalf("account %x exists = %v from the diff, %v from the dictionary", key, entry.exists, exists)
		}
		if !equalCell(entry.account.Account, loaded.Account) ||
			entry.account.LastTransLT != loaded.LastTransLT ||
			!bytes.Equal(entry.account.LastTransHash, loaded.LastTransHash) {
			t.Fatalf("account %x decoded differently from the diff and from the dictionary", key)
		}
	}
}
