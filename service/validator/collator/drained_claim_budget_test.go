package collator

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// A block that imports nothing may still claim the drained bound — that
// everything below the reference lt is already processed — and proving that
// claim means walking every neighbour's whole queue prefix. The reference only
// makes the claim when its own merger reached the end of the queues while
// importing, so the walk is already paid for; ours comes from the message pool
// and can therefore be made over a backlog the pool never saw.
//
// On the stand that cost 3.0-3.4 MB of collated proof on blocks carrying 2.3 kB
// of content, against 1.15-1.30 MB of proof on 489-593 kB blocks from the
// reference nodes in the same shards and the same minutes. Because the collated
// estimate is charged before admission, and this network's config gives collated
// data the same band as block bytes, those blocks were over the limit before a
// message could be admitted — and with nothing admitted the next block claimed
// the same bound again. Thirty-two consecutive near-empty blocks in each of two
// shards.
//
// The budget is what keeps the claim to the case where it is cheap, which is the
// case where the reference would have reached the end of the queues too.
func TestDrainedClaimWalkStopsAtItsBudget(t *testing.T) {
	for _, test := range []struct {
		name      string
		messages  int
		exhausted bool
	}{
		{name: "a shallow prefix is walked to its end and the claim stands", messages: 4, exhausted: true},
		{name: "a prefix deeper than the budget abandons the claim", messages: drainedQueueScanBudget + 8, exhausted: false},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := emptyCandidateRequest(t)
			source := blockShardIdent(request.Previous.ID)
			previous := request.Previous
			startLT := requestStartLT(t, request)
			var last *msgpool.InternalMessage
			for i := range test.messages {
				message, enqueued := queuedInternalWithReferencedBody(
					t,
					address.NewAddress(0, 0, append([]byte{byte(i)}, bytes.Repeat([]byte{0x71}, 31)...)),
					address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x72}, 32)),
					startLT-uint64(10+i),
					request.Header.GenUtime-1,
					tlb.FromNanoTONU(100_000),
					tlb.FromNanoTONU(100_000),
					96,
					source,
				)
				previous.State = stateWithQueueMessage(t, previous.State, message.Key, enqueued)
				last = message
			}
			view, err := localViewFromPrevious(previous, true, true)
			if err != nil {
				t.Fatal(err)
			}

			// The drained bound: everything below the reference lt, claimed with
			// no message to show for it. This is the shape fullCollatedQueueScan
			// produces when the acquired cut came back empty and complete.
			target := msgpool.ShardIdent{Workchain: last.Key.NextHop().Workchain, Shard: msgpool.ShardAll}
			exhausted, err := traceInternalCut(
				FullCollatedQueueScan{
					Target: target,
					LT:     startLT,
					Hash:   processedInfinityHash,
					Budget: drainedQueueScanBudget,
				},
				nil,
				map[msgpool.ShardIdent]*localNeighborView{source: view},
			)
			if err != nil {
				t.Fatalf("trace drained claim: %v", err)
			}
			if exhausted != test.exhausted {
				t.Fatalf("scan exhausted = %v, want %v over %d queued messages", exhausted, test.exhausted, test.messages)
			}

			// Whatever the verdict, the walk itself stays bounded: the proof it
			// leaves behind is the budget's cost, not the backlog's.
			proof, err := view.proof.CreateProof()
			if err != nil {
				t.Fatal(err)
			}
			if size := len(proof.ToBOC()); size > 256<<10 {
				t.Fatalf("budgeted scan left a %d-byte proof", size)
			}
		})
	}
}

// An unbudgeted scan is the one bounded by an imported message: that prefix is
// exactly what the block consumed, the validator will open all of it, and it is
// owed in full. The budget must not touch it.
func TestImportedBoundScanIsNotBudgeted(t *testing.T) {
	request := emptyCandidateRequest(t)
	source := blockShardIdent(request.Previous.ID)
	previous := request.Previous
	startLT := requestStartLT(t, request)

	var last *msgpool.InternalMessage
	for i := range drainedQueueScanBudget + 8 {
		message, enqueued := queuedInternalWithReferencedBody(
			t,
			address.NewAddress(0, 0, append([]byte{byte(i)}, bytes.Repeat([]byte{0x71}, 31)...)),
			address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x72}, 32)),
			startLT-uint64(10+i),
			request.Header.GenUtime-1,
			tlb.FromNanoTONU(100_000),
			tlb.FromNanoTONU(100_000),
			96,
			source,
		)
		previous.State = stateWithQueueMessage(t, previous.State, message.Key, enqueued)
		last = message
	}
	view, err := localViewFromPrevious(previous, true, true)
	if err != nil {
		t.Fatal(err)
	}

	target := msgpool.ShardIdent{Workchain: last.Key.NextHop().Workchain, Shard: msgpool.ShardAll}
	exhausted, err := traceInternalCut(
		FullCollatedQueueScan{Target: target, LT: startLT, Hash: last.Root.HashKey()},
		nil,
		map[msgpool.ShardIdent]*localNeighborView{source: view},
	)
	if err != nil {
		t.Fatalf("trace imported bound: %v", err)
	}
	if !exhausted {
		t.Fatal("an unbudgeted scan reported itself unfinished")
	}
}

// The proof walk stops at the bound the block claims, so the completeness check
// inside it can only be asked about messages that lie under that bound — the
// ones the block actually imported. Handing it the whole acquired cut reports
// every message the block declined to take as missing from its source and kills
// the window; on the stand that cost five of seven leader windows, one of them
// reporting 5,307 "absent" messages that were simply never imported.
func TestTraceInternalCutAcceptsOnlyTheImportedPrefix(t *testing.T) {
	const queued = 12

	request := emptyCandidateRequest(t)
	source := blockShardIdent(request.Previous.ID)
	previous := request.Previous
	startLT := requestStartLT(t, request)

	messages := make([]*msgpool.InternalMessage, 0, queued)
	for i := range queued {
		message, enqueued := queuedInternalWithReferencedBody(
			t,
			address.NewAddress(0, 0, append([]byte{byte(i)}, bytes.Repeat([]byte{0x71}, 31)...)),
			address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x72}, 32)),
			startLT-uint64(64-i),
			request.Header.GenUtime-1,
			tlb.FromNanoTONU(100_000),
			tlb.FromNanoTONU(100_000),
			96,
			source,
		)
		previous.State = stateWithQueueMessage(t, previous.State, message.Key, enqueued)
		messages = append(messages, message)
	}

	// The block took half of them; its claim ends at the last one it took.
	imported := messages[:queued/2]
	bound := imported[len(imported)-1]
	target := msgpool.ShardIdent{Workchain: bound.Key.NextHop().Workchain, Shard: msgpool.ShardAll}
	scan := FullCollatedQueueScan{Target: target, LT: bound.EnqueuedLT, Hash: bound.Root.HashKey()}

	view, err := localViewFromPrevious(previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = traceInternalCut(scan, imported, map[msgpool.ShardIdent]*localNeighborView{source: view}); err != nil {
		t.Fatalf("trace the imported prefix: %v", err)
	}

	// The whole cut against the same bound is what the regression did.
	wide, err := localViewFromPrevious(previous, true, true)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = traceInternalCut(scan, messages, map[msgpool.ShardIdent]*localNeighborView{source: wide}); err == nil {
		t.Fatal("tracing the whole cut against an imported-prefix bound was accepted")
	}
}
