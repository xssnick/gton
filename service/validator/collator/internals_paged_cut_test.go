package collator

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func branchPagedInternalCut(t *testing.T, request ShardRequest) (*msgpool.Cut, int) {
	t.Helper()
	messageCount := 0
	if request.Internals != nil {
		messageCount = len(request.Internals.Messages)
	}
	if request.Internals == nil || len(request.Internals.Messages) <= internalCutPageSize {
		t.Fatalf("fixture has %d internals, want more than one %d-message page",
			messageCount, internalCutPageSize)
	}

	messages := request.Internals.Messages
	source := messages[0].Source
	visible := msgpool.SourceRef{RootHash: [32]byte{0xfa, 0xce}}
	for index, message := range messages {
		if message.Source != source {
			t.Fatalf("internal %d belongs to source %+v, want %+v", index, message.Source, source)
		}
		visible.Seqno = max(visible.Seqno, message.SourceSeqno)
	}

	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	destination := targetShardIdent(request.Shard)
	if err := pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	if err := pool.Internals().Seed(destination, source, visible, messages, uint64(len(messages))); err != nil {
		t.Fatal(err)
	}
	branch, err := pool.Internals().OpenBranch(destination)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(branch.Close)
	cut, err := branch.Cut(msgpool.CutRequest{
		Sources: map[msgpool.ShardIdent]msgpool.CutSource{source: {Visible: visible}},
		Limit:   internalCutPageSize,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != internalCutPageSize || !cut.More || !cut.CanLoadMore() {
		t.Fatalf("initial cut = %d messages, more=%t, pageable=%t; want %d and a continuation",
			len(cut.Messages), cut.More, cut.CanLoadMore(), internalCutPageSize)
	}

	return cut, len(messages)
}

func candidateProcessedRecords(t *testing.T, request ShardRequest, candidate *Candidate) []tlb.ProcessedUptoRecord {
	t.Helper()

	queue := candidateQueueInfo(t, candidate)
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, uint64(request.Shard.Shard))
	if err != nil {
		t.Fatal(err)
	}

	return records
}

func requireProcessedBoundCovers(t *testing.T, records []tlb.ProcessedUptoRecord, message *msgpool.InternalMessage) {
	t.Helper()

	wantHash := message.Key.MsgHash()
	for _, record := range records {
		if record.LastMsgLT > message.EnqueuedLT ||
			(record.LastMsgLT == message.EnqueuedLT && bytes.Compare(record.LastMsgHash[:], wantHash[:]) >= 0) {
			return
		}
	}
	t.Fatalf("processed records %+v do not cover final input (%d, %x)",
		records, message.EnqueuedLT, wantHash)
}

func TestPagedInternalCutProducesTheSequentialBlockInEveryWaveMode(t *testing.T) {
	arms := []struct {
		name    string
		workers int
		paged   bool
	}{
		{name: "eager sequential", workers: -1},
		{name: "paged sequential", workers: -1, paged: true},
		{name: "paged waves inline", workers: 1, paged: true},
		{name: "paged waves concurrent", workers: 16, paged: true},
	}

	var (
		reference        *Candidate
		referenceRecords []tlb.ProcessedUptoRecord
	)
	for _, arm := range arms {
		t.Run(arm.name, func(t *testing.T) {
			request := fullCollatedMainnetRequest(t)
			request.internalWaveWorkers = arm.workers
			allMessages := request.Internals.Messages
			messageCount := len(allMessages)
			if arm.paged {
				request.Internals, messageCount = branchPagedInternalCut(t, request)
			}

			candidate, err := testBuilder().BuildShard(context.Background(), request)
			if err != nil {
				t.Fatal(err)
			}
			if candidate.Stats.InternalsImported != uint32(messageCount) {
				t.Fatalf("imported %d of %d internals", candidate.Stats.InternalsImported, messageCount)
			}
			if arm.paged {
				if request.Internals.More || request.Internals.CanLoadMore() {
					t.Fatalf("drained cut still reports more=%t, pageable=%t",
						request.Internals.More, request.Internals.CanLoadMore())
				}
				if len(request.Internals.Messages) != messageCount {
					t.Fatalf("drained cut materialized %d of %d messages",
						len(request.Internals.Messages), messageCount)
				}
			}

			records := candidateProcessedRecords(t, request, candidate)
			requireProcessedBoundCovers(t, records, allMessages[len(allMessages)-1])
			if reference == nil {
				reference = candidate
				referenceRecords = slices.Clone(records)
				return
			}
			if !bytes.Equal(candidate.BlockBOC, reference.BlockBOC) {
				t.Fatalf("block differs from eager sequential reference: %d bytes against %d",
					len(candidate.BlockBOC), len(reference.BlockBOC))
			}
			if !bytes.Equal(candidate.CollatedData, reference.CollatedData) {
				t.Fatalf("collated data differs from eager sequential reference: %d bytes against %d",
					len(candidate.CollatedData), len(reference.CollatedData))
			}
			if !slices.Equal(records, referenceRecords) {
				t.Fatalf("processed records differ from eager sequential reference: %+v against %+v",
					records, referenceRecords)
			}

			got, want := candidate.Stats, reference.Stats
			got.InternalsSpeculated, got.InternalsDiscarded, got.InternalsChained = 0, 0, 0
			want.InternalsSpeculated, want.InternalsDiscarded, want.InternalsChained = 0, 0, 0
			if got != want {
				t.Fatalf("stats differ from eager sequential reference: %+v against %+v", got, want)
			}
		})
	}
}

func TestPagedInternalCutCanBeReusedAfterSequentialDrain(t *testing.T) {
	request := fullCollatedMainnetRequest(t)
	request.internalWaveWorkers = -1
	request.Internals, _ = branchPagedInternalCut(t, request)

	first, err := testBuilder().BuildShard(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if request.Internals.More || request.Internals.CanLoadMore() {
		t.Fatalf("first build left the cut incomplete: more=%t, pageable=%t",
			request.Internals.More, request.Internals.CanLoadMore())
	}

	second, err := testBuilder().BuildShard(context.Background(), request)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(second.BlockBOC, first.BlockBOC) {
		t.Fatalf("sequential reuse produced a different block: %d bytes against %d",
			len(second.BlockBOC), len(first.BlockBOC))
	}
	if !bytes.Equal(second.CollatedData, first.CollatedData) {
		t.Fatalf("sequential reuse produced different collated data: %d bytes against %d",
			len(second.CollatedData), len(first.CollatedData))
	}
	if records := candidateProcessedRecords(t, request, second); !slices.Equal(records, candidateProcessedRecords(t, request, first)) {
		t.Fatalf("sequential reuse produced different ProcessedInfo: %+v", records)
	}
}
