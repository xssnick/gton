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
	"github.com/xssnick/tonutils-go/tvm/cell"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
	stackop "github.com/xssnick/tonutils-go/tvm/op/stack"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// queuedInternal builds one pooled inbound message together with its exact
// queue envelope and EnqueuedMsg cells.
func queuedInternal(
	tb testing.TB,
	src, dst *address.Address,
	createdLT uint64,
	createdAt uint32,
	fwdFee, remainingFee tlb.Coins,
	curBits uint8,
	source msgpool.ShardIdent,
) (*msgpool.InternalMessage, *cell.Cell) {
	tb.Helper()

	message, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     src,
		DstAddr:     dst,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		FwdFee:      fwdFee,
		CreatedLT:   createdLT,
		CreatedAt:   createdAt,
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		tb.Fatal(err)
	}
	envelopeCell, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: curBits},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		FwdFeeRemaining: remainingFee,
		Msg:             message,
	}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	var envelope tlb.MsgEnvelope
	if err = parseExact(&envelope, envelopeCell); err != nil {
		tb.Fatal(err)
	}
	srcPrefix, err := accountPrefixFromAddress(src)
	if err != nil {
		tb.Fatal(err)
	}
	dstPrefix, err := accountPrefixFromAddress(dst)
	if err != nil {
		tb.Fatal(err)
	}
	nextHop := msgpool.InterpolatePrefix(srcPrefix, dstPrefix, 96)
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: createdLT, Msg: envelopeCell}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}

	return &msgpool.InternalMessage{
		Key:          msgpool.MakeQueueKey(nextHop, message.HashKey()),
		EnqueuedLT:   createdLT,
		QueueLT:      createdLT,
		EnvHash:      envelopeCell.HashKey(),
		Envelope:     envelope,
		EnvelopeCell: envelopeCell,
		Root:         message,
		Source:       source,
		SourceSeqno:  1,
	}, enqueued
}

func candidateMessageDescriptors(tb testing.TB, candidate *Candidate, globalVersion uint32) (*tlb.InMsgDescrAugDict, *tlb.OutMsgDescrAugDict) {
	tb.Helper()

	var block tlb.Block
	if err := parseExact(&block, candidateBlock(tb, candidate)); err != nil {
		tb.Fatal(err)
	}
	inMessages, err := tlb.LoadInMsgDescrAugDict(block.Extra.InMsgDesc.MustBeginParse(), globalVersion)
	if err != nil {
		tb.Fatal(err)
	}
	outMessages, err := tlb.LoadOutMsgDescrAugDict(block.Extra.OutMsgDesc.MustBeginParse(), globalVersion)
	if err != nil {
		tb.Fatal(err)
	}
	return inMessages, outMessages
}

func descriptorByHash(tb testing.TB, dict *cell.AugmentedDictionary, hash [32]byte) *cell.Slice {
	tb.Helper()

	key := cell.BeginCell().MustStoreSlice(hash[:], 256).EndCell()
	value, err := dict.LoadValue(key)
	if err != nil {
		tb.Fatalf("message descriptor %x: %v", hash, err)
	}
	return value
}

func candidateQueueInfo(tb testing.TB, candidate *Candidate) tlb.OutMsgQueueInfo {
	tb.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, candidate.State); err != nil {
		tb.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		tb.Fatal(err)
	}
	return queue
}

func TestBuildCandidateImportsOwnQueueMessage(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x91}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x92}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, receiver, req.Header.GenUtime, 10_000_000_000))

	createdLT := requestStartLT(t, req) - 10
	fee := tlb.FromNanoTONU(100_000)
	msg, enqueued := queuedInternal(t, source, receiver, createdLT, req.Header.GenUtime-1,
		fee, fee, 96, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
	req.Previous.State = stateWithQueueMessage(t, req.Previous.State, msg.Key, enqueued)
	queueSize := uint64(1)
	req.Previous.OutQueueSize = &queueSize
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{msg}}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.InternalsImported != 1 ||
		candidate.Stats.InternalsSkipped != 0 || candidate.Stats.OutQueueSize != 0 {
		t.Fatalf("unexpected import stats: %+v", candidate.Stats)
	}

	hash := msg.Root.HashKey()
	inMessages, outMessages := candidateMessageDescriptors(t, candidate, req.Masterchain.Config.globalVersion)
	in := descriptorByHash(t, inMessages.AugmentedDictionary, hash)
	if tag := in.MustLoadUInt(3); tag != 0b100 {
		t.Fatalf("in descriptor tag = %03b, want msg_import_fin$100", tag)
	}
	importedEnvelope, err := in.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if importedEnvelope.HashKey() != msg.EnvelopeCell.HashKey() {
		t.Fatal("msg_import_fin does not reference the exact queued envelope cell")
	}
	consuming, err := in.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	fwdFee, err := in.LoadBigCoins()
	if err != nil {
		t.Fatal(err)
	}
	if fwdFee.Cmp(fee.Nano()) != 0 {
		t.Fatalf("import fwd fee = %s, want %s", fwdFee, fee.Nano())
	}

	out := descriptorByHash(t, outMessages.AugmentedDictionary, hash)
	if tag := out.MustLoadUInt(3); tag != 0b100 {
		t.Fatalf("out descriptor tag = %03b, want msg_export_deq_imm$100", tag)
	}
	dequeuedEnvelope, err := out.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if dequeuedEnvelope.HashKey() != msg.EnvelopeCell.HashKey() {
		t.Fatal("msg_export_deq_imm does not reference the queued envelope cell")
	}
	reimport, err := out.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	reimportLoader := reimport.MustBeginParse()
	if tag := reimportLoader.MustLoadUInt(3); tag != 0b100 {
		t.Fatalf("reimport tag = %03b, want msg_import_fin$100", tag)
	}
	if _, err = reimportLoader.LoadRefCell(); err != nil {
		t.Fatal(err)
	}
	reimportTransaction, err := reimportLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if reimportTransaction.HashKey() != consuming.HashKey() {
		t.Fatal("dequeue reimport references a different transaction than the import")
	}

	queue := candidateQueueInfo(t, candidate)
	if count, err := queue.OutQueue.Count(); err != nil || count != 0 {
		t.Fatalf("outbound queue count = %d, err = %v", count, err)
	}
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, msgpool.ShardAll)
	if err != nil {
		t.Fatal(err)
	}
	want := tlb.ProcessedUptoRecord{
		ShardPrefix: 0x8000000000000000,
		MCSeqno:     req.Masterchain.ID.SeqNo,
		LastMsgLT:   createdLT,
		LastMsgHash: hash,
	}
	if len(records) != 1 || records[0] != want {
		t.Fatalf("processed info records = %+v, want %+v", records, want)
	}
}

func TestBuildCandidateImportsForeignQueueMessage(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x93}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x94}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, receiver, req.Header.GenUtime, 10_000_000_000))

	// A masterchain-enqueued envelope keeps cur_addr at the source and routes
	// straight to the destination; the remaining fee is below the original.
	createdLT := requestStartLT(t, req) - 20
	msg, _ := queuedInternal(t, source, receiver, createdLT, req.Header.GenUtime-1,
		tlb.FromNanoTONU(100_000), tlb.FromNanoTONU(60_000), 0,
		msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll})
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{msg}}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.InternalsImported != 1 ||
		candidate.Stats.OutQueueSize != 0 {
		t.Fatalf("unexpected import stats: %+v", candidate.Stats)
	}

	hash := msg.Root.HashKey()
	inMessages, outMessages := candidateMessageDescriptors(t, candidate, req.Masterchain.Config.globalVersion)
	in := descriptorByHash(t, inMessages.AugmentedDictionary, hash)
	if tag := in.MustLoadUInt(3); tag != 0b100 {
		t.Fatalf("in descriptor tag = %03b, want msg_import_fin$100", tag)
	}
	importedEnvelope, err := in.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if importedEnvelope.HashKey() != msg.EnvelopeCell.HashKey() {
		t.Fatal("msg_import_fin does not reference the exact queued envelope cell")
	}
	if _, err = in.LoadRefCell(); err != nil {
		t.Fatal(err)
	}
	fwdFee, err := in.LoadBigCoins()
	if err != nil {
		t.Fatal(err)
	}
	if fwdFee.Cmp(tlb.FromNanoTONU(60_000).Nano()) != 0 {
		t.Fatalf("import fwd fee = %s, want the envelope remaining fee", fwdFee)
	}
	if count, err := outMessages.Count(); err != nil || count != 0 {
		t.Fatalf("out descriptor count = %d, err = %v: a foreign import must not dequeue", count, err)
	}

	queue := candidateQueueInfo(t, candidate)
	if count, err := queue.OutQueue.Count(); err != nil || count != 0 {
		t.Fatalf("outbound queue count = %d, err = %v", count, err)
	}
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, msgpool.ShardAll)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].LastMsgLT != createdLT || records[0].LastMsgHash != hash ||
		records[0].MCSeqno != req.Masterchain.ID.SeqNo {
		t.Fatalf("processed info records = %+v", records)
	}
}

func TestBuildCandidateImportsTransitEnqueuedMessage(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x9a}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x9b}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, receiver, req.Header.GenUtime, 10_000_000_000))

	// A transit re-enqueue enters the relaying queue at that block's start lt:
	// the queue entry lt exceeds the creation lt. The processed bound must
	// carry the canonical creation lt, not the queue entry lt.
	createdLT := requestStartLT(t, req) - 20
	msg, _ := queuedInternal(t, source, receiver, createdLT, req.Header.GenUtime-1,
		tlb.FromNanoTONU(100_000), tlb.FromNanoTONU(60_000), 0,
		msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll})
	msg.QueueLT = createdLT + 15
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{msg}}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.InternalsImported != 1 || candidate.Stats.InternalsSkipped != 0 {
		t.Fatalf("unexpected import stats: %+v", candidate.Stats)
	}
	queue := candidateQueueInfo(t, candidate)
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, msgpool.ShardAll)
	if err != nil {
		t.Fatal(err)
	}
	want := tlb.ProcessedUptoRecord{
		ShardPrefix: 0x8000000000000000,
		MCSeqno:     req.Masterchain.ID.SeqNo,
		LastMsgLT:   createdLT,
		LastMsgHash: msg.Root.HashKey(),
	}
	if len(records) != 1 || records[0] != want {
		t.Fatalf("processed info records = %+v, want the canonical-lt bound %+v", records, want)
	}
}

func TestStandardTransitFeeUsesBasechainPrices(t *testing.T) {
	config := &Config{}
	config.basechain.fwdPrices.NextFrac = 1 << 15
	config.masterchain.fwdPrices.NextFrac = 1 << 14

	fee := standardTransitFee(config, big.NewInt(100))
	if fee.Cmp(big.NewInt(50)) != 0 {
		t.Fatalf("transit fee = %s, want basechain-priced 50", fee)
	}
}

func TestBuildCandidateRejectsQueueLTBelowEmissionLT(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x9e}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x9f}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, receiver, req.Header.GenUtime, 10_000_000_000))

	createdLT := requestStartLT(t, req) - 20
	msg, _ := queuedInternal(t, source, receiver, createdLT, req.Header.GenUtime-1,
		tlb.FromNanoTONU(100_000), tlb.FromNanoTONU(60_000), 0,
		msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll})
	msg.QueueLT = createdLT - 1
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{msg}}

	if _, err := testBuilder().BuildShard(context.Background(), req); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("error = %v, want ErrInvalidInput", err)
	}
}

func TestBuildCandidateSkipsProcessedInternalMessage(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x95}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x96}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, receiver, req.Header.GenUtime, 10_000_000_000))

	createdLT := requestStartLT(t, req) - 10
	fee := tlb.FromNanoTONU(100_000)
	msg, enqueued := queuedInternal(t, source, receiver, createdLT, req.Header.GenUtime-1,
		fee, fee, 96, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
	req.Previous.State = stateWithQueueMessage(t, req.Previous.State, msg.Key, enqueued)
	queueSize := uint64(1)
	req.Previous.OutQueueSize = &queueSize

	// A parent record at the message lt with an all-ones hash covers it.
	value := cell.BeginCell().MustStoreUInt(createdLT, 64).MustStoreSlice(bytes.Repeat([]byte{0xff}, 32), 256)
	covering := processedDictionary(t, 0x8000000000000000, req.Masterchain.ID.SeqNo-1, value)
	rewritePreviousShardState(t, &req, func(state *tlb.ShardStateUnsplit) {
		var queue tlb.OutMsgQueueInfo
		if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
			t.Fatal(err)
		}
		queue.ProcInfo = covering
		rewritten, err := queue.ToCell()
		if err != nil {
			t.Fatal(err)
		}
		state.OutMsgQueueInfo = rewritten
	})
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{msg}}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 0 || candidate.Stats.InternalsImported != 0 ||
		candidate.Stats.InternalsSkipped != 1 || candidate.Stats.OutQueueSize != 1 {
		t.Fatalf("unexpected skip stats: %+v", candidate.Stats)
	}

	queue := candidateQueueInfo(t, candidate)
	if count, err := queue.OutQueue.Count(); err != nil || count != 1 {
		t.Fatalf("outbound queue count = %d, err = %v: a skipped message keeps its entry", count, err)
	}
	// The bound advances over skipped messages too, so the
	// record carries the skipped message's canonical (lt, hash).
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, msgpool.ShardAll)
	if err != nil {
		t.Fatal(err)
	}
	found := false
	for _, rec := range records {
		if rec.MCSeqno == req.Masterchain.ID.SeqNo && rec.LastMsgLT == msg.EnqueuedLT &&
			rec.LastMsgHash == msg.Root.HashKey() {
			found = true
		}
	}
	if !found {
		t.Fatalf("skipped message must advance the ProcessedUpto bound: %+v", records)
	}
}

func TestBuildCandidateRejectsProcessedCheckWithoutShardEndLT(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0xff, bytes.Repeat([]byte{0xa6}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa7}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, receiver, req.Header.GenUtime, 10_000_000_000))

	createdLT := requestStartLT(t, req) - 10
	fee := tlb.FromNanoTONU(100_000)
	msg, _ := queuedInternal(
		t,
		source,
		receiver,
		createdLT,
		req.Header.GenUtime-1,
		fee,
		fee,
		0,
		msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll},
	)
	covering := processedDictionary(
		t,
		msgpool.ShardAll,
		req.Masterchain.ID.SeqNo-1,
		cell.BeginCell().MustStoreUInt(createdLT, 64).MustStoreSlice(bytes.Repeat([]byte{0xff}, 32), 256),
	)
	replacePreviousProcessedInfo(t, &req, covering)
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{msg}}

	_, err := testBuilder().BuildShard(context.Background(), req)
	if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "shard end lt") {
		t.Fatalf("error = %v, want an invalid missing shard end lt resolver error", err)
	}
}

func TestBuildCandidateStopsImportsAtBlockLimit(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x97}, 32))
	first := address.NewAddress(0, 0, bytes.Repeat([]byte{0x98}, 32))
	second := address.NewAddress(0, 0, bytes.Repeat([]byte{0x99}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: first, code: externalAcceptCode(t), balance: 10_000_000_000},
		activeContract{address: second, code: externalAcceptCode(t), balance: 10_000_000_000},
	))

	startLT := requestStartLT(t, req)
	fee := tlb.FromNanoTONU(100_000)
	owner := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	msgA, enqueuedA := queuedInternal(t, source, first, startLT-20, req.Header.GenUtime-1, fee, fee, 96, owner)
	msgB, enqueuedB := queuedInternal(t, source, second, startLT-10, req.Header.GenUtime-1, fee, fee, 96, owner)
	req.Previous.State = stateWithQueueMessage(t, req.Previous.State, msgA.Key, enqueuedA)
	req.Previous.State = stateWithQueueMessage(t, req.Previous.State, msgB.Key, enqueuedB)
	queueSize := uint64(2)
	req.Previous.OutQueueSize = &queueSize
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{msgA, msgB}}
	// The first import exhausts the normal gas class, leaving the tail
	// unimported.
	req.Masterchain.Config.basechain.limits.gas = limitThresholds{0, 1, 1 << 40, 1 << 41}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.InternalsImported != 1 ||
		candidate.Stats.OutQueueSize != 1 {
		t.Fatalf("unexpected cutoff stats: %+v", candidate.Stats)
	}

	queue := candidateQueueInfo(t, candidate)
	if count, err := queue.OutQueue.Count(); err != nil || count != 1 {
		t.Fatalf("outbound queue count = %d, err = %v", count, err)
	}
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, msgpool.ShardAll)
	if err != nil {
		t.Fatal(err)
	}
	hash := msgA.Root.HashKey()
	if len(records) != 1 || records[0].LastMsgLT != msgA.EnqueuedLT || records[0].LastMsgHash != hash {
		t.Fatalf("processed info records = %+v, want the first import only", records)
	}
}

func TestBuildCandidateDeliversInShardMessageImmediately(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{} // drained queues: immediate delivery allowed
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa1}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa2}, 32))

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
		activeContract{address: sender, code: externalSendCode(t, outbound), balance: 100_000_000_000},
		activeContract{address: receiver, code: externalAcceptCode(t), balance: 10_000_000_000},
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
	if candidate.Stats.Transactions != 2 || candidate.Stats.NewMessages != 1 ||
		candidate.Stats.EnqueuedMessages != 0 || candidate.Stats.ImmediateDelivered != 1 ||
		candidate.Stats.OutQueueSize != 0 {
		t.Fatalf("unexpected delivery stats: %+v", candidate.Stats)
	}

	inMessages, outMessages := candidateMessageDescriptors(t, candidate, req.Masterchain.Config.globalVersion)
	creatingDescr := descriptorByHash(t, inMessages.AugmentedDictionary, external.HashKey())
	if tag := creatingDescr.MustLoadUInt(3); tag != 0b000 {
		t.Fatalf("external descriptor tag = %03b", tag)
	}
	if _, err = creatingDescr.LoadRefCell(); err != nil {
		t.Fatal(err)
	}
	creating, err := creatingDescr.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}

	// The generated message hash comes from the delivered import entry.
	var deliveredHash [32]byte
	found := 0
	ok, err := inMessages.CheckForEachExtra(func(_, _ *cell.Slice, key *cell.Cell) (bool, error) {
		keyBits, err := key.BeginParse()
		if err != nil {
			return false, err
		}
		bits, err := keyBits.LoadSlice(256)
		if err != nil {
			return false, err
		}
		var hash [32]byte
		copy(hash[:], bits)
		if hash != external.HashKey() {
			deliveredHash = hash
			found++
		}
		return true, nil
	}, false)
	if err != nil || !ok {
		t.Fatalf("iterate in descriptors: ok=%t err=%v", ok, err)
	}
	if found != 1 {
		t.Fatalf("in descriptor count beyond the external = %d, want 1", found)
	}

	in := descriptorByHash(t, inMessages.AugmentedDictionary, deliveredHash)
	if tag := in.MustLoadUInt(3); tag != 0b011 {
		t.Fatalf("in descriptor tag = %03b, want msg_import_imm$011", tag)
	}
	envelopeCell, err := in.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	consuming, err := in.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if consuming.HashKey() == creating.HashKey() {
		t.Fatal("import must reference the consuming transaction, not the creating one")
	}
	var envelope tlb.MsgEnvelope
	if err = parseExact(&envelope, envelopeCell); err != nil {
		t.Fatal(err)
	}
	if envelope.CurAddr.UseDestBits != 96 || envelope.NextAddr.UseDestBits != 96 {
		t.Fatalf("immediate envelope addresses = %d/%d, want 96/96", envelope.CurAddr.UseDestBits, envelope.NextAddr.UseDestBits)
	}
	var delivered tlb.InternalMessage
	if err = parseExact(&delivered, envelope.Msg); err != nil {
		t.Fatal(err)
	}
	if envelope.FwdFeeRemaining.Nano().Cmp(delivered.FwdFee.Nano()) != 0 {
		t.Fatal("immediate envelope must carry the full forward fee")
	}
	importFee, err := in.LoadBigCoins()
	if err != nil {
		t.Fatal(err)
	}
	if importFee.Cmp(delivered.FwdFee.Nano()) != 0 {
		t.Fatal("msg_import_imm fwd_fee must equal the full forward fee")
	}

	out := descriptorByHash(t, outMessages.AugmentedDictionary, deliveredHash)
	if tag := out.MustLoadUInt(3); tag != 0b010 {
		t.Fatalf("out descriptor tag = %03b, want msg_export_imm$010", tag)
	}
	exportEnvelope, err := out.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if exportEnvelope.HashKey() != envelopeCell.HashKey() {
		t.Fatal("export and import reference different envelopes")
	}
	exportTransaction, err := out.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if exportTransaction.HashKey() != creating.HashKey() {
		t.Fatal("export must reference the creating transaction")
	}
	reimport, err := out.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	reimportLoader := reimport.MustBeginParse()
	if tag := reimportLoader.MustLoadUInt(3); tag != 0b011 {
		t.Fatalf("reimport tag = %03b, want msg_import_imm$011", tag)
	}
	reimportEnvelope, err := reimportLoader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if reimportEnvelope.HashKey() != envelopeCell.HashKey() {
		t.Fatal("reimport references a different envelope")
	}
	if outCount, err := outMessages.Count(); err != nil || outCount != 1 {
		t.Fatalf("out descriptor count = %d, err = %v", outCount, err)
	}

	queue := candidateQueueInfo(t, candidate)
	if count, err := queue.OutQueue.Count(); err != nil || count != 0 {
		t.Fatalf("outbound queue count = %d, err = %v", count, err)
	}
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, msgpool.ShardAll)
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 1 || records[0].LastMsgLT != delivered.CreatedLT || records[0].LastMsgHash != deliveredHash {
		t.Fatalf("processed info records = %+v, want the delivered message bound", records)
	}
}

func TestBuildCandidateDeliversCascade(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{} // drained queues: immediate delivery allowed
	first := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa3}, 32))
	second := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa4}, 32))
	third := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa5}, 32))

	hop2, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     third,
		Amount:      tlb.FromNanoTONU(500_000_000),
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	hop1, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     second,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: first, code: externalSendCode(t, hop1), balance: 100_000_000_000},
		activeContract{address: second, code: externalSendCode(t, hop2), balance: 10_000_000_000},
		activeContract{address: third, code: externalAcceptCode(t), balance: 10_000_000_000},
	))
	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: first,
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
	if candidate.Stats.Transactions != 3 || candidate.Stats.NewMessages != 2 ||
		candidate.Stats.ImmediateDelivered != 2 || candidate.Stats.EnqueuedMessages != 0 ||
		candidate.Stats.OutQueueSize != 0 {
		t.Fatalf("unexpected cascade stats: %+v", candidate.Stats)
	}

	inMessages, outMessages := candidateMessageDescriptors(t, candidate, req.Masterchain.Config.globalVersion)
	if count, err := inMessages.Count(); err != nil || count != 3 {
		t.Fatalf("in descriptor count = %d, err = %v", count, err)
	}
	if count, err := outMessages.Count(); err != nil || count != 2 {
		t.Fatalf("out descriptor count = %d, err = %v", count, err)
	}
	if req.Masterchain.Config.capabilities&capMsgMetadata != 0 {
		// The second hop extends the first hop's metadata chain by one.
		depths := map[uint32]int{}
		ok, err := inMessages.CheckForEachExtra(func(value, _ *cell.Slice, _ *cell.Cell) (bool, error) {
			tag, err := value.LoadUInt(3)
			if err != nil || tag != 0b011 {
				return true, err
			}
			envCell, err := value.LoadRefCell()
			if err != nil {
				return false, err
			}
			var envelope tlb.MsgEnvelope
			if err = parseExact(&envelope, envCell); err != nil {
				return false, err
			}
			if envelope.Metadata == nil {
				t.Fatal("immediate envelope lacks metadata with capMsgMetadata enabled")
			}
			if envelope.Metadata.Initiator.String() != first.String() {
				t.Fatalf("metadata initiator = %s, want %s", envelope.Metadata.Initiator, first)
			}
			depths[envelope.Metadata.Depth]++
			return true, nil
		}, false)
		if err != nil || !ok {
			t.Fatalf("iterate in descriptors: ok=%t err=%v", ok, err)
		}
		if depths[0] != 1 || depths[1] != 1 {
			t.Fatalf("metadata depths = %v, want one depth-0 and one depth-1 hop", depths)
		}
	}
}

func TestBuildCandidateEnqueuesWhenBlockFull(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{} // drained queues: immediate delivery allowed
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa6}, 32))
	firstReceiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa7}, 32))
	secondReceiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa8}, 32))

	firstMessage, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     firstReceiver,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	secondMessage, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     secondReceiver,
		Amount:      tlb.FromNanoTONU(1_000_000_000),
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	code := externalSendCode(t, firstMessage).ToBuilder()
	if err = code.StoreBuilder(stackop.PUSHREF(secondMessage).Serialize()); err != nil {
		t.Fatal(err)
	}
	if err = code.StoreBuilder(stackop.PUSHINT(big.NewInt(0)).Serialize()); err != nil {
		t.Fatal(err)
	}
	if err = code.StoreBuilder(funcsop.SENDRAWMSG().Serialize()); err != nil {
		t.Fatal(err)
	}
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: sender, code: code.EndCell(), balance: 100_000_000_000},
		activeContract{address: firstReceiver, code: externalAcceptCode(t), balance: 10_000_000_000},
		activeContract{address: secondReceiver, code: externalAcceptCode(t), balance: 10_000_000_000},
	))
	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: sender,
		Body:    cell.BeginCell().EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, external)}
	// The first delivery exhausts the normal gas class: the second message
	// falls back to the outbound queue.
	req.Masterchain.Config.basechain.limits.gas = limitThresholds{0, 1, 1 << 40, 1 << 41}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 2 || candidate.Stats.NewMessages != 2 ||
		candidate.Stats.ImmediateDelivered != 1 || candidate.Stats.EnqueuedMessages != 1 ||
		candidate.Stats.OutQueueSize != 1 {
		t.Fatalf("unexpected fallback stats: %+v", candidate.Stats)
	}

	queue := candidateQueueInfo(t, candidate)
	if count, err := queue.OutQueue.Count(); err != nil || count != 1 {
		t.Fatalf("outbound queue count = %d, err = %v", count, err)
	}
}

func TestBuildCandidateRejectsUnsortedInternals(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa9}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0xaa}, 32))
	startLT := requestStartLT(t, req)
	fee := tlb.FromNanoTONU(100_000)
	owner := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	msgA, _ := queuedInternal(t, source, receiver, startLT-20, req.Header.GenUtime-1, fee, fee, 96, owner)
	msgB, _ := queuedInternal(t, source, receiver, startLT-10, req.Header.GenUtime-1, fee, fee, 96, owner)
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{msgB, msgA}}

	if _, err := testBuilder().BuildShard(context.Background(), req); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("error = %v, want ErrInvalidInput", err)
	}
}

func TestBuildCandidateRejectsImportWithForeignQueueKey(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xab}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0xac}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, receiver, req.Header.GenUtime, 10_000_000_000))

	fee := tlb.FromNanoTONU(100_000)
	msg, _ := queuedInternal(t, source, receiver, requestStartLT(t, req)-10, req.Header.GenUtime-1,
		fee, fee, 96, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
	msg.Key = msgpool.MakeQueueKey(msgpool.AccountPrefix{Workchain: 0, Prefix: 0x1234}, [32]byte{0x77})
	req.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{msg}}

	if _, err := testBuilder().BuildShard(context.Background(), req); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("error = %v, want ErrInvalidInput", err)
	}
}
