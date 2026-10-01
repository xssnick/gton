package collator

import (
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
	stackop "github.com/xssnick/tonutils-go/tvm/op/stack"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestBuildCandidateProcessesPendingDispatchQueue(t *testing.T) {
	req := emptyCandidateRequest(t)
	dispatchQueue := nonEmptyDispatchQueue(t, req.Header.GenUtime-1, requestStartLT(t, req)-10)
	var sourceID [32]byte
	copy(sourceID[:], bytes.Repeat([]byte{0x91}, 32))
	req.Previous.State = previousStateWithDispatchQueue(t, req.Previous.State, dispatchQueue)
	previousStateRoot := req.Previous.State.HashKey()

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if req.Previous.State.HashKey() != previousStateRoot {
		t.Fatal("build mutated the previous state root")
	}
	if candidate.Stats.DispatchedMessages != 1 || candidate.Stats.EnqueuedMessages != 1 {
		t.Fatalf("dispatch/enqueue stats = %d/%d, want 1/1", candidate.Stats.DispatchedMessages, candidate.Stats.EnqueuedMessages)
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err = parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	if queue.Extra == nil || !queue.Extra.DispatchQueue.IsEmpty() {
		t.Fatal("processed dispatch message remains in the next state")
	}
	if count, countErr := queue.OutQueue.Count(); countErr != nil || count != 1 {
		t.Fatalf("outbound queue count = %d, err = %v, want 1", count, countErr)
	}

	var queued tlb.EnqueuedMsg
	ok, err := queue.OutQueue.CheckForEachExtra(func(value, _ *cell.Slice, _ *cell.Cell) (bool, error) {
		return true, loadExactSlice(&queued, value)
	}, false)
	if err != nil || !ok {
		t.Fatalf("load dispatched outbound entry: ok=%t err=%v", ok, err)
	}
	var emittedEnvelope tlb.MsgEnvelope
	if err = parseExact(&emittedEnvelope, queued.Msg); err != nil {
		t.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	wantEmittedLT := block.BlockInfo.StartLt + 1
	if emittedEnvelope.EmittedLT == nil || *emittedEnvelope.EmittedLT != wantEmittedLT || queued.EnqueuedLT != wantEmittedLT {
		t.Fatalf("emitted/enqueued lt = %v/%d, want %d", emittedEnvelope.EmittedLT, queued.EnqueuedLT, wantEmittedLT)
	}

	inMessages, outMessages := candidateMessageDescriptors(t, candidate, req.Masterchain.Config.globalVersion)
	if count, countErr := inMessages.Count(); countErr != nil || count != 1 {
		t.Fatalf("deferred transit InMsg count = %d, err = %v, want 1", count, countErr)
	}
	if count, countErr := outMessages.Count(); countErr != nil || count != 1 {
		t.Fatalf("deferred transit OutMsg count = %d, err = %v, want 1", count, countErr)
	}
	var inDescriptor, outDescriptor *cell.Cell
	ok, err = inMessages.CheckForEachExtra(func(value, _ *cell.Slice, _ *cell.Cell) (bool, error) {
		var cellErr error
		inDescriptor, cellErr = value.ToCell()
		return true, cellErr
	}, false)
	if err != nil || !ok {
		t.Fatalf("load deferred transit InMsg: ok=%t err=%v", ok, err)
	}
	ok, err = outMessages.CheckForEachExtra(func(value, _ *cell.Slice, _ *cell.Cell) (bool, error) {
		var cellErr error
		outDescriptor, cellErr = value.ToCell()
		return true, cellErr
	}, false)
	if err != nil || !ok {
		t.Fatalf("load deferred transit OutMsg: ok=%t err=%v", ok, err)
	}

	inLoader := inDescriptor.MustBeginParse()
	if tag := inLoader.MustLoadUInt(5); tag != 0b00101 {
		t.Fatalf("deferred transit InMsg tag = %05b", tag)
	}
	oldEnvelopeCell := inLoader.MustLoadRef().MustToCell()
	newEnvelopeCell := inLoader.MustLoadRef().MustToCell()
	if inLoader.BitsLeft() != 0 || inLoader.RefsNum() != 0 || newEnvelopeCell.HashKey() != queued.Msg.HashKey() {
		t.Fatal("deferred transit InMsg has a fee tail or mismatched routed envelope")
	}
	var oldEnvelope tlb.MsgEnvelope
	if err = parseExact(&oldEnvelope, oldEnvelopeCell); err != nil {
		t.Fatal(err)
	}
	if oldEnvelope.EmittedLT == nil || *oldEnvelope.EmittedLT != wantEmittedLT ||
		oldEnvelope.CurAddr.UseDestBits != 0 || oldEnvelope.NextAddr.UseDestBits != 0 {
		t.Fatal("deferred transit input envelope lost zero-route emitted form")
	}

	outLoader := outDescriptor.MustBeginParse()
	if tag := outLoader.MustLoadUInt(5); tag != 0b10101 {
		t.Fatalf("deferred transit OutMsg tag = %05b", tag)
	}
	if outLoader.MustLoadRef().MustToCell().HashKey() != queued.Msg.HashKey() ||
		outLoader.MustLoadRef().MustToCell().HashKey() != inDescriptor.HashKey() ||
		outLoader.BitsLeft() != 0 || outLoader.RefsNum() != 0 {
		t.Fatal("deferred transit OutMsg references or tail differ")
	}

	if _, err = loadAccountDispatchQueue(queue.Extra.DispatchQueue, sourceID); !isMissingKey(err) {
		t.Fatalf("dispatch source remains after processing: %v", err)
	}
}

func TestBuildCandidateDeliversDispatchMessageImmediately(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{}
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x92}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, destination, req.Header.GenUtime, 10_000_000_000))
	dispatchQueue := nonEmptyDispatchQueue(t, req.Header.GenUtime-1, requestStartLT(t, req)-10)
	req.Previous.State = previousStateWithDispatchQueue(t, req.Previous.State, dispatchQueue)

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.DispatchedMessages != 1 || candidate.Stats.Transactions != 1 ||
		candidate.Stats.ImmediateDelivered != 1 || candidate.Stats.EnqueuedMessages != 0 ||
		candidate.Stats.OutQueueSize != 0 {
		t.Fatalf("unexpected immediate dispatch stats: %+v", candidate.Stats)
	}
	queue := candidateQueueInfo(t, candidate)
	if !queue.OutQueue.IsEmpty() || queue.Extra == nil || !queue.Extra.DispatchQueue.IsEmpty() {
		t.Fatal("immediately delivered dispatch message remains in a queue")
	}

	inMessages, outMessages := candidateMessageDescriptors(t, candidate, req.Masterchain.Config.globalVersion)
	if count, countErr := inMessages.Count(); countErr != nil || count != 1 {
		t.Fatalf("deferred-final InMsg count = %d, err = %v, want 1", count, countErr)
	}
	if count, countErr := outMessages.Count(); countErr != nil || count != 0 {
		t.Fatalf("deferred-final OutMsg count = %d, err = %v, want 0", count, countErr)
	}
	var descriptor *cell.Cell
	ok, err := inMessages.CheckForEachExtra(func(value, _ *cell.Slice, _ *cell.Cell) (bool, error) {
		var cellErr error
		descriptor, cellErr = value.ToCell()
		return true, cellErr
	}, false)
	if err != nil || !ok {
		t.Fatalf("load deferred-final descriptor: ok=%t err=%v", ok, err)
	}
	loader := descriptor.MustBeginParse()
	if tag := loader.MustLoadUInt(5); tag != 0b00100 {
		t.Fatalf("deferred-final InMsg tag = %05b", tag)
	}
	envelopeCell := loader.MustLoadRef().MustToCell()
	transactionCell := loader.MustLoadRef().MustToCell()
	if fee := loader.MustLoadBigCoins(); fee.Cmp(tlb.FromNanoTONU(1_000).Nano()) != 0 ||
		loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		t.Fatalf("deferred-final fee/tail = %s, %d bits, %d refs", fee, loader.BitsLeft(), loader.RefsNum())
	}
	var envelope tlb.MsgEnvelope
	if err = parseExact(&envelope, envelopeCell); err != nil {
		t.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	wantEmittedLT := block.BlockInfo.StartLt + 1
	if envelope.EmittedLT == nil || *envelope.EmittedLT != wantEmittedLT ||
		envelope.CurAddr.UseDestBits != 0 || envelope.NextAddr.UseDestBits != 0 {
		t.Fatal("deferred-final envelope lost its emitted logical time or zero route")
	}
	var transaction tlb.Transaction
	if err = parseExact(&transaction, transactionCell); err != nil {
		t.Fatal(err)
	}
	if transaction.LT <= wantEmittedLT {
		t.Fatalf("dispatch transaction lt = %d, want greater than emitted lt %d", transaction.LT, wantEmittedLT)
	}
}

func TestBuildQueueInfoPreservesDispatchWithoutQueueSizeCapability(t *testing.T) {
	dispatchQueue := nonEmptyDispatchQueue(t, 1_900_000_000, 1_000_000)
	outQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	collation := &collation{
		config:        &Config{},
		outQueue:      outQueue,
		processed:     cell.NewDict(processedInfoKeyBits),
		dispatchQueue: dispatchQueue,
	}

	root, err := collation.buildQueueInfo()
	if err != nil {
		t.Fatal(err)
	}
	var queueInfo tlb.OutMsgQueueInfo
	if err = parseExact(&queueInfo, root); err != nil {
		t.Fatal(err)
	}
	if queueInfo.Extra == nil || queueInfo.Extra.DispatchQueue.IsEmpty() {
		t.Fatal("non-empty dispatch queue was dropped from queue info")
	}
	if queueInfo.Extra.OutQueueSize != nil {
		t.Fatal("out-queue size was stored without its capability")
	}
}

func TestBuildCandidateDefersEligibleGeneratedMessage(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{More: true}
	req.Masterchain.Config.capabilities |= capDeferMessages
	req.Dispatch = DispatchPolicy{
		DeferringEnabled:       true,
		DeferMessagesAfter:     1,
		DeferOutQueueSizeLimit: 2_048,
	}

	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0x71}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x72}, 32))
	outbound, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     receiver,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: sender, code: externalSendTwiceCode(t, outbound), balance: 100_000_000_000},
	))
	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: sender,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, external)}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.NewMessages != 2 ||
		candidate.Stats.DeferredMessages != 1 || candidate.Stats.EnqueuedMessages != 1 ||
		candidate.Stats.OutQueueSize != 1 {
		t.Fatalf("unexpected generated-message deferral stats: %+v", candidate.Stats)
	}

	queue := candidateQueueInfo(t, candidate)
	if count, countErr := queue.OutQueue.Count(); countErr != nil || count != 1 {
		t.Fatalf("outbound queue count = %d, err = %v, want 1", count, countErr)
	}
	if queue.Extra == nil {
		t.Fatal("deferred message did not create outbound-queue extra")
	}

	var senderID [32]byte
	copy(senderID[:], sender.Data())
	accountQueue, err := loadAccountDispatchQueue(queue.Extra.DispatchQueue, senderID)
	if err != nil {
		t.Fatal(err)
	}
	items, err := accountQueue.Messages.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	if accountQueue.Count != 1 || len(items) != 1 {
		t.Fatalf("deferred account count/items = %d/%d, want 1/1", accountQueue.Count, len(items))
	}
	var deferredEntry tlb.EnqueuedMsg
	if err = loadExactSlice(&deferredEntry, items[0].Value); err != nil {
		t.Fatal(err)
	}
	var deferredEnvelope tlb.MsgEnvelope
	if err = parseExact(&deferredEnvelope, deferredEntry.Msg); err != nil {
		t.Fatal(err)
	}
	if deferredEnvelope.CurAddr.UseDestBits != 0 || deferredEnvelope.NextAddr.UseDestBits != 0 || deferredEnvelope.EmittedLT != nil {
		t.Fatal("new deferred envelope has non-zero routing or emitted logical time")
	}

	_, outMessages := candidateMessageDescriptors(t, candidate, req.Masterchain.Config.globalVersion)
	var normal, deferred int
	ok, err := outMessages.CheckForEachExtra(func(value, _ *cell.Slice, _ *cell.Cell) (bool, error) {
		tag, loadErr := value.LoadUInt(3)
		if loadErr != nil {
			return false, loadErr
		}
		switch tag {
		case 0b001:
			normal++
		case 0b101:
			suffix, suffixErr := value.LoadUInt(2)
			if suffixErr != nil {
				return false, suffixErr
			}
			if suffix != 0 {
				t.Fatalf("deferred export suffix = %02b, want 00", suffix)
			}
			envelope, refErr := value.LoadRefCell()
			if refErr != nil {
				return false, refErr
			}
			if envelope.HashKey() != deferredEntry.Msg.HashKey() {
				t.Fatal("deferred descriptor and DispatchQueue refer to different envelopes")
			}
			deferred++
		default:
			t.Fatalf("unexpected generated OutMsg tag prefix %03b", tag)
		}
		return true, nil
	}, false)
	if err != nil || !ok {
		t.Fatalf("iterate generated OutMsg descriptors: ok=%t err=%v", ok, err)
	}
	if normal != 1 || deferred != 1 {
		t.Fatalf("normal/deferred descriptors = %d/%d, want 1/1", normal, deferred)
	}
}

func nonEmptyDispatchQueue(tb testing.TB, createdAt uint32, lt uint64) *tlb.DispatchQueueAugDict {
	tb.Helper()

	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x91}, 32))
	destination := address.NewAddress(0, 0, bytes.Repeat([]byte{0x92}, 32))
	message, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     source,
		DstAddr:     destination,
		Amount:      tlb.FromNanoTONU(1_000_000),
		FwdFee:      tlb.FromNanoTONU(1_000),
		CreatedLT:   lt,
		CreatedAt:   createdAt,
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		tb.Fatalf("serialize deferred message: %v", err)
	}
	envelope, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
		FwdFeeRemaining: tlb.FromNanoTONU(1_000),
		Msg:             message,
	}).ToCell()
	if err != nil {
		tb.Fatalf("serialize deferred message envelope: %v", err)
	}
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: lt, Msg: envelope}).ToCell()
	if err != nil {
		tb.Fatalf("serialize deferred queue entry: %v", err)
	}

	messages := cell.NewDict(64)
	messageKey := cell.BeginCell().MustStoreUInt(lt, 64).EndCell()
	if err = messages.Set(messageKey, enqueued); err != nil {
		tb.Fatalf("insert deferred queue entry: %v", err)
	}
	accountQueue, err := (tlb.AccountDispatchQueue{Messages: messages, Count: 1}).ToCell()
	if err != nil {
		tb.Fatalf("serialize account dispatch queue: %v", err)
	}
	dictionary, err := cell.NewAugDict(256, tlb.AugDispatchQueue{})
	if err != nil {
		tb.Fatalf("create dispatch queue: %v", err)
	}
	accountKey := cell.BeginCell().MustStoreSlice(source.Data(), 256).EndCell()
	if err = dictionary.Set(accountKey, accountQueue); err != nil {
		tb.Fatalf("insert account dispatch queue: %v", err)
	}
	return &tlb.DispatchQueueAugDict{AugmentedDictionary: dictionary}
}

func previousStateWithDispatchQueue(
	tb testing.TB,
	root *cell.Cell,
	dispatchQueue *tlb.DispatchQueueAugDict,
) *cell.Cell {
	tb.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		tb.Fatalf("decode previous state: %v", err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		tb.Fatalf("decode previous outbound queue: %v", err)
	}
	queue.Extra = &tlb.OutMsgQueueExtra{DispatchQueue: dispatchQueue}

	var err error
	state.OutMsgQueueInfo, err = queue.ToCell()
	if err != nil {
		tb.Fatalf("serialize previous outbound queue: %v", err)
	}
	next, err := tlb.ToCell(&state)
	if err != nil {
		tb.Fatalf("serialize previous state: %v", err)
	}
	return next
}

func externalSendTwiceCode(tb testing.TB, message *cell.Cell) *cell.Cell {
	tb.Helper()

	code := externalAcceptCode(tb).ToBuilder()
	for range 2 {
		if err := code.StoreBuilder(stackop.PUSHREF(message).Serialize()); err != nil {
			tb.Fatal(err)
		}
		if err := code.StoreBuilder(stackop.PUSHINT(big.NewInt(0)).Serialize()); err != nil {
			tb.Fatal(err)
		}
		if err := code.StoreBuilder(funcsop.SENDRAWMSG().Serialize()); err != nil {
			tb.Fatal(err)
		}
	}
	return code.EndCell()
}
