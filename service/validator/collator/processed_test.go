package collator

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestMinRecordedMCSeqno(t *testing.T) {
	dict := cell.NewDict(processedInfoKeyBits)
	for _, seqno := range []uint32{9, 3, 5} {
		key := cell.BeginCell().MustStoreUInt(0x4000000000000000, 64).MustStoreUInt(uint64(seqno), 32).EndCell()
		value := cell.BeginCell().MustStoreUInt(100, 64).MustStoreSlice(make([]byte, 32), 256).EndCell()
		if err := dict.Set(key, value); err != nil {
			t.Fatal(err)
		}
	}

	records, err := tlb.LoadProcessedUptoRecords(dict, 0x4000000000000000)
	if err != nil {
		t.Fatal(err)
	}
	if minimum := minRecordedMCSeqno(records); minimum != 3 {
		t.Fatalf("minimum = %d, want 3", minimum)
	}

	// An empty collection contributes no bound, so callers fall back to their
	// own seqno rather than clamping to zero.
	empty, err := tlb.LoadProcessedUptoRecords(cell.NewDict(processedInfoKeyBits), 0x8000000000000000)
	if err != nil {
		t.Fatal(err)
	}
	if minimum := minRecordedMCSeqno(empty); minimum != math.MaxUint32 {
		t.Fatalf("empty minimum = %d, want %d", minimum, uint32(math.MaxUint32))
	}
}

func TestMasterProcessedReferenceUsesResultingBlockSeqno(t *testing.T) {
	var header tlb.BlockHeader
	header.SeqNo = 41
	c := collation{
		header: header,
		req: collationRequest{
			previous: PreviousBlock{ID: ton.BlockIDExt{SeqNo: 40}},
		},
		oldState: tlb.ShardStateUnsplit{GenLT: 12_000_000},
		master:   &masterCollation{},
	}

	seqno, endLT := c.processedReference()
	if seqno != 41 || endLT != c.oldState.GenLT {
		t.Fatalf("processed reference = (%d, %d), want (41, %d)", seqno, endLT, c.oldState.GenLT)
	}
}

func TestProcessedUptoRecordsRejectMalformedDictionary(t *testing.T) {
	tests := []struct {
		name string
		dict *cell.Dictionary
	}{
		{name: "wrong key size", dict: cell.NewDict(95)},
		{name: "zero shard", dict: processedDictionary(t, 0, 1, processedValue(100))},
		{name: "shard outside owner", dict: processedDictionary(t, 0xc000000000000000, 1, processedValue(100))},
		{
			name: "short value",
			dict: processedDictionary(t, 0x4000000000000000, 1,
				cell.BeginCell().MustStoreSlice(make([]byte, 40), 319)),
		},
		{
			name: "value with reference",
			dict: processedDictionary(t, 0x4000000000000000, 1,
				processedValue(100).MustStoreRef(cell.BeginCell().EndCell())),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := tlb.LoadProcessedUptoRecords(test.dict, 0x4000000000000000); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestBuildCandidatePreservesProcessedInfo(t *testing.T) {
	req := emptyCandidateRequest(t)
	// An incomplete cut reports undrained inbound queues: without processed
	// messages the parent dictionary must pass through byte-identical.
	req.Internals = &msgpool.Cut{More: true}
	dict := processedDictionary(t, 0x8000000000000000, req.Masterchain.ID.SeqNo, processedValue(100))
	want, err := dict.ToCell()
	if err != nil {
		t.Fatal(err)
	}

	rewritePreviousShardState(t, &req, func(state *tlb.ShardStateUnsplit) {
		var queue tlb.OutMsgQueueInfo
		if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
			t.Fatal(err)
		}
		queue.ProcInfo = dict
		state.OutMsgQueueInfo, err = queue.ToCell()
		if err != nil {
			t.Fatal(err)
		}
	})

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err = parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	got, err := queue.ProcInfo.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	if got.HashKey() != want.HashKey() {
		t.Fatal("processed info changed without imported messages")
	}
}

// replacePreviousProcessedInfo rewrites the parent state ProcessedInfo.
func replacePreviousProcessedInfo(t *testing.T, req *ShardRequest, dict *cell.Dictionary) {
	t.Helper()

	rewritePreviousShardState(t, req, func(state *tlb.ShardStateUnsplit) {
		var queue tlb.OutMsgQueueInfo
		if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
			t.Fatal(err)
		}
		queue.ProcInfo = dict
		var err error
		state.OutMsgQueueInfo, err = queue.ToCell()
		if err != nil {
			t.Fatal(err)
		}
	})
}

func TestBuildCandidateInsertsInfinityProcessedRecord(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Internals = &msgpool.Cut{} // queues inspected and drained
	// A parent record five masterchain blocks back: the infinity record must
	// replace it after compaction, and the minimum referenced masterchain
	// seqno must follow the surviving records, not the parent minimum.
	replacePreviousProcessedInfo(t, &req,
		processedDictionary(t, 0x8000000000000000, req.Masterchain.ID.SeqNo-5, processedValue(100)))

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	queue := candidateQueueInfo(t, candidate)
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, msgpool.ShardAll)
	if err != nil {
		t.Fatal(err)
	}
	want := tlb.ProcessedUptoRecord{
		ShardPrefix: 0x8000000000000000,
		MCSeqno:     req.Masterchain.ID.SeqNo,
		LastMsgLT:   req.Masterchain.EndLT - 1,
		LastMsgHash: processedInfinityHash,
	}
	if len(records) != 1 || records[0] != want {
		t.Fatalf("processed info records = %+v, want the infinity record %+v", records, want)
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	if state.MinRefMCSeqno != req.Masterchain.ID.SeqNo {
		t.Fatalf("state min referenced masterchain seqno = %d, want %d", state.MinRefMCSeqno, req.Masterchain.ID.SeqNo)
	}
	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	if block.BlockInfo.MinRefMcSeqno != req.Masterchain.ID.SeqNo {
		t.Fatalf("header min referenced masterchain seqno = %d, want %d", block.BlockInfo.MinRefMcSeqno, req.Masterchain.ID.SeqNo)
	}
}

func TestBuildCandidateRefreshesMinRefMCSeqnoAfterImport(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0x9c}, 32))
	receiver := address.NewAddress(0, 0, bytes.Repeat([]byte{0x9d}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, receiver, req.Header.GenUtime, 10_000_000_000))
	// A parent record at an older masterchain seqno with a bound below the
	// import: the inserted record dominates it, so the minimum referenced
	// masterchain seqno must be recomputed from the compacted records.
	replacePreviousProcessedInfo(t, &req,
		processedDictionary(t, 0x8000000000000000, req.Masterchain.ID.SeqNo-5, processedValue(50)))

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
	if candidate.Stats.InternalsImported != 1 {
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
		t.Fatalf("processed info records = %+v, want %+v", records, want)
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	if state.MinRefMCSeqno != req.Masterchain.ID.SeqNo {
		t.Fatalf("state min referenced masterchain seqno = %d, want %d", state.MinRefMCSeqno, req.Masterchain.ID.SeqNo)
	}
	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	if block.BlockInfo.MinRefMcSeqno != req.Masterchain.ID.SeqNo {
		t.Fatalf("header min referenced masterchain seqno = %d, want %d", block.BlockInfo.MinRefMcSeqno, req.Masterchain.ID.SeqNo)
	}
}

func processedDictionary(t *testing.T, shard uint64, mcSeqno uint32, value *cell.Builder) *cell.Dictionary {
	t.Helper()
	dict := cell.NewDict(processedInfoKeyBits)
	key := cell.BeginCell().MustStoreUInt(shard, 64).MustStoreUInt(uint64(mcSeqno), 32).EndCell()
	if err := dict.SetBuilder(key, value); err != nil {
		t.Fatal(err)
	}
	return dict
}

func processedValue(lastLT uint64) *cell.Builder {
	return cell.BeginCell().MustStoreUInt(lastLT, 64).MustStoreSlice(make([]byte, 32), 256)
}

// A shard that holds a queued message has produced a block, so a zero end lt
// means the historical shard configuration could not answer. Neither verdict is
// safe there: reporting "not processed" leaves queue cleanup incomplete, while
// reporting "processed" would skip a message ValidateQuery expects us to
// import. The resolver must therefore fail the collation.
func TestShardEndLTResolverFailsClosedOnUnknownShard(t *testing.T) {
	rootShard := uint64(1) << 63
	records := []tlb.ProcessedUptoRecord{{
		ShardPrefix: rootShard,
		MCSeqno:     5,
		LastMsgLT:   1_000,
		LastMsgHash: [32]byte{0xff},
	}}
	// A message whose current hop is in another workchain is the branch that
	// consults the registered end lt.
	descr := &tlb.ProcessedMsgDescr{
		CurWorkchain:  1,
		CurPrefix:     rootShard,
		NextWorkchain: 0,
		NextPrefix:    rootShard,
		LT:            500,
		EnqueuedLT:    400,
		Hash:          [32]byte{0x01},
	}

	resolved := newShardEndLTResolver(func(uint32, int32, uint64) uint64 { return 900 })
	processed, err := resolved.alreadyProcessed(records, 0, rootShard, descr)
	if err != nil {
		t.Fatalf("resolvable shard end lt was rejected: %v", err)
	}
	if !processed {
		t.Fatal("message enqueued before the registered end lt was not reported as processed")
	}

	unknown := newShardEndLTResolver(func(uint32, int32, uint64) uint64 { return 0 })
	if _, err = unknown.alreadyProcessed(records, 0, rootShard, descr); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("unknown shard end lt error = %v, want ErrInvalidInput", err)
	} else if !strings.Contains(err.Error(), "masterchain block 5") {
		t.Fatalf("unknown shard end lt error = %v, want the unresolved masterchain block", err)
	}

	// A latched miss must not survive into the next check.
	unknown.resolve = func(uint32, int32, uint64) uint64 { return 900 }
	if _, err = unknown.alreadyProcessed(records, 0, rootShard, descr); err != nil {
		t.Fatalf("resolver stayed failed after a miss: %v", err)
	}

	// Requests without neighbors carry no resolver; that contract is unchanged.
	absent := newShardEndLTResolver(nil)
	if _, err = absent.alreadyProcessed(records, 0, rootShard, descr); err == nil {
		t.Fatal("missing resolver was accepted")
	} else if errors.Is(err, ErrInvalidInput) {
		t.Fatalf("missing resolver reported as an unresolved shard: %v", err)
	}
}
