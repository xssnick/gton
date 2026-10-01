package collator

import (
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// A deep outbound queue is what makes a later split expensive: the split
// rewrites the queue trie's spine in proportion to the number of entries, and
// that rewrite lands in the state update, which carries no transactions and so
// cannot be trimmed by any byte budget. Externals are the intake that feeds the
// queue, so they are the one thing a collator can still decline. Collator
// declines them past SKIP_EXTERNALS_QUEUE_SIZE (collator.cpp:4276) and this
// must too.
func TestBuildShardSkipsExternalsWhileOutboundQueueIsDeep(t *testing.T) {
	for _, test := range []struct {
		name         string
		queued       int
		wantIncluded uint32
		wantSkipped  uint32
	}{
		{name: "under the brake", queued: 8, wantIncluded: 1},
		{name: "over the brake", queued: int(skipExternalsQueueSize) + 1, wantSkipped: 1},
	} {
		t.Run(test.name, func(t *testing.T) {
			req := queueDepthRequest(t, test.queued)
			candidate, err := testBuilder().BuildShard(context.Background(), req)
			if err != nil {
				t.Fatalf("BuildShard: %v", err)
			}
			if candidate.Stats.ExternalIncluded != test.wantIncluded {
				t.Fatalf("included %d externals over a queue of %d, want %d",
					candidate.Stats.ExternalIncluded, test.queued, test.wantIncluded)
			}
			if candidate.Stats.ExternalSkippedLimit != test.wantSkipped {
				t.Fatalf("skipped %d externals over a queue of %d, want %d",
					candidate.Stats.ExternalSkippedLimit, test.queued, test.wantSkipped)
			}
			// Whichever way it went, the external must not have been consumed:
			// a skipped message stays pooled for a later block, and a message
			// rejected outright would be dropped instead.
			if candidate.Stats.ExternalInvalid != 0 || candidate.Stats.ExternalNotAccepted != 0 {
				t.Fatalf("external was neither included nor skipped: %+v", candidate.Stats)
			}
		})
	}
}

// queueDepthRequest builds a shard request whose predecessor already holds
// queued entries outbound messages, and which offers one external to an account
// that accepts it.
func queueDepthRequest(t *testing.T, queued int) ShardRequest {
	t.Helper()

	req := emptyCandidateRequest(t)
	baseState := predecessorTestState(t, req.Previous.State)
	destination := predecessorAddress(0x11)
	fee := tlb.FromNanoTONU(100_000)

	entries := make([]predecessorQueueEntry, 0, queued)
	for i := 0; i < queued; i++ {
		message, value := queuedInternal(t,
			predecessorAddress(byte(0x21+i%64)), predecessorAddress(byte(0x31+i%64)),
			baseState.GenLT-uint64(queued+10)+uint64(i), req.Header.GenUtime-1,
			fee, fee, 0, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
		entries = append(entries, predecessorQueueEntry{key: message.Key, value: value})
	}

	req.Previous.State = predecessorStateRoot(t, req.Previous.State, predecessorStateOptions{
		ident:     mustPredecessorIdent(t, req.Shard),
		seqno:     req.Previous.ID.SeqNo,
		vertSeqno: baseState.VertSeqno,
		genUtime:  baseState.GenUTime,
		genLT:     baseState.GenLT,
		minRefMC:  baseState.MinRefMCSeqno,
		accounts: activeContracts(t, req.Header.GenUtime,
			activeContract{address: destination, code: externalAcceptCode(t), balance: 100_000_000_000}),
		fees:      5,
		masterRef: predecessorTestStats(t, &baseState).MasterRef,
		queue:     entries,
	})
	size := uint64(queued)
	req.Previous.OutQueueSize = &size

	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: destination,
		Body:    cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, message)}
	return req
}
