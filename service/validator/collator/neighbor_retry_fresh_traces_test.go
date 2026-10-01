package collator

import (
	"bytes"
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// A size-limit retry rebuilds the block under a narrower cap, so it imports less
// and claims a lower processed bound than the attempt it replaces, and the
// neighbour proof it carries must be the one that bound needs. The reference
// repeats with a new Collator (collator.cpp:359-364) and so with fresh neighbour
// proof builders (collator.cpp:1052). Reusing the traced views instead left the
// earlier attempt's deeper queue walk in the retry's proof: exactly the collated
// bytes the retry exists to shed.
func TestNeighborProofRetryMatchesFreshAttempt(t *testing.T) {
	const queued = 12

	request := emptyCandidateRequest(t)
	source := blockShardIdent(request.Previous.ID)
	neighbor := request.Previous
	neighbor.ID = testBlockID(source.Workchain, int64(source.Shard), 0, 0x61)
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
		message.SourceSeqno = 0
		neighbor.State = stateWithQueueMessage(t, neighbor.State, message.Key, enqueued)
		messages = append(messages, message)
	}
	target := msgpool.ShardIdent{Workchain: messages[0].Key.NextHop().Workchain, Shard: msgpool.ShardAll}

	fresh := func() *localFullProofProvider {
		t.Helper()

		view, err := localViewFromPrevious(neighbor, true, true)
		if err != nil {
			t.Fatal(err)
		}
		views := map[msgpool.ShardIdent]*localNeighborView{source: view}

		return &localFullProofProvider{proofViews: views, messageViews: views}
	}

	retried := fresh()
	first := neighborRetryProof(t, retried, request.Previous, neighbor.ID, source, target, messages)
	retry := neighborRetryProof(t, retried, request.Previous, neighbor.ID, source, target, messages[:queued/2])

	if single := neighborRetryProof(t, fresh(), request.Previous, neighbor.ID, source, target, messages); !bytes.Equal(first, single) {
		t.Fatal("the first attempt's neighbour proof differs from a single attempt's")
	}
	narrow := neighborRetryProof(t, fresh(), request.Previous, neighbor.ID, source, target, messages[:queued/2])
	if bytes.Equal(first, narrow) {
		t.Fatal("fixture does not narrow the neighbour proof with the imported prefix")
	}
	if !bytes.Equal(retry, narrow) {
		t.Fatalf("retry neighbour proof is %d bytes, a fresh attempt at the same bound is %d", len(retry), len(narrow))
	}
}

// A still-registered ancestor's proof also carries the virtual sibling queue
// acquisition cut out of it (prepareShardNeighborQueues), and nothing in the
// retry's own walk reads that cut again. A rebuilt view has to repeat it, or the
// retry ships a proof the validator cannot narrow to our sibling.
func TestNeighborProofRetryRepeatsVirtualSiblingCut(t *testing.T) {
	const queued = 12

	request := emptyCandidateRequest(t)
	ancestor := blockShardIdent(request.Previous.ID)
	left, err := shard.Child(request.Previous.ID.Shard, true)
	if err != nil {
		t.Fatal(err)
	}
	target := msgpool.ShardIdent{Workchain: ancestor.Workchain, Shard: uint64(left)}
	neighbor := request.Previous
	neighbor.ID = testBlockID(ancestor.Workchain, int64(ancestor.Shard), 0, 0x62)
	startLT := requestStartLT(t, request)
	fee := tlb.FromNanoTONU(100_000)

	messages := make([]*msgpool.InternalMessage, 0, queued)
	for i := range queued {
		// Sent from the sibling half into ours: the entries the virtual sibling
		// queue keeps and the inbound walk of our half reads.
		message, enqueued := queuedInternalWithReferencedBody(
			t,
			address.NewAddress(0, 0, append([]byte{0xe1, byte(i)}, bytes.Repeat([]byte{0xe1}, 30)...)),
			address.NewAddress(0, 0, bytes.Repeat([]byte{0x21}, 32)),
			startLT-uint64(64-i),
			request.Header.GenUtime-1,
			fee,
			fee,
			0,
			ancestor,
		)
		message.SourceSeqno = 0
		neighbor.State = stateWithQueueMessage(t, neighbor.State, message.Key, enqueued)
		messages = append(messages, message)
	}

	acquired := func() *localFullProofProvider {
		t.Helper()

		view, err := localViewFromPrevious(neighbor, true, true)
		if err != nil {
			t.Fatal(err)
		}
		views := map[msgpool.ShardIdent]*localNeighborView{ancestor: view}
		if err = prepareShardNeighborQueues(target, []Neighbor{localNeighbor(view, ancestor)}, views, false); err != nil {
			t.Fatal(err)
		}

		return &localFullProofProvider{proofViews: views, messageViews: views}
	}

	retried := acquired()
	neighborRetryProof(t, retried, request.Previous, neighbor.ID, ancestor, target, messages)
	retry := neighborRetryProof(t, retried, request.Previous, neighbor.ID, ancestor, target, messages[:queued/2])
	narrow := neighborRetryProof(t, acquired(), request.Previous, neighbor.ID, ancestor, target, messages[:queued/2])
	if !bytes.Equal(retry, narrow) {
		t.Fatalf("retried ancestor proof is %d bytes, a fresh attempt at the same bound is %d", len(retry), len(narrow))
	}
}

// neighborRetryProof asks provider for the neighbour proofs of one attempt that
// imported exactly imported, claiming the bound of its last message, and returns
// them serialized in order.
func neighborRetryProof(
	t *testing.T,
	provider *localFullProofProvider,
	previous PreviousBlock,
	neighbor ton.BlockIDExt,
	source, target msgpool.ShardIdent,
	imported []*msgpool.InternalMessage,
) []byte {
	t.Helper()

	bound := imported[len(imported)-1]
	built, err := provider.BuildFullCollatedProofs(context.Background(), FullCollatedProofRequest{
		Previous:  previous,
		Neighbors: []Neighbor{{Block: neighbor, Shard: source}},
		Internals: imported,
		QueueScan: &FullCollatedQueueScan{Target: target, LT: bound.EnqueuedLT, Hash: bound.Root.HashKey()},
	})
	if err != nil {
		t.Fatal(err)
	}

	var serialized []byte
	for _, root := range built.Roots {
		serialized = append(serialized, root.ToBOC()...)
	}

	return serialized
}
