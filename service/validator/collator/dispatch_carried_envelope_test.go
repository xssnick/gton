package collator

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// A dispatched message carries the envelope processDeferredMessage built, and
// retirement takes the fee and the routing rewrite from it instead of from a
// reparse of the emitted cell. It has to be that reparse in everything the block
// sees: the same wire bytes, the same fee, the very message cell the dispatch
// pass read out of the traced predecessor, and the same routed envelope — for an
// envelope deferred with metadata and for one deferred without, whose v1 tag the
// emitted lt promotes. The reparse itself recorded nothing, so the read set is
// what it was without it.
func TestDispatchCarriedEnvelopeMatchesEmittedCell(t *testing.T) {
	metadata := &tlb.MsgMetadata{
		Depth:       3,
		Initiator:   address.NewAddress(0, 0, bytes.Repeat([]byte{0x55}, 32)),
		InitiatorLT: 777,
	}
	plain, withMetadata := repeatedDispatchAccount(0x61), repeatedDispatchAccount(0x62)
	queueRoot, err := makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: plain, lts: []uint64{10, 20}},
		dispatchFixtureAccount{accountID: withMetadata, lts: []uint64{11, 21}, metadata: metadata, bodyInRef: true},
	).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	predecessor := cell.NewReadSet(queueRoot)
	var queue tlb.DispatchQueueAugDict
	if err = parseExact(&queue, predecessor.Root()); err != nil {
		t.Fatal(err)
	}
	c := dispatchTestCollation(t, &queue, DispatchPolicy{
		DeferMessagesAfter:    100,
		Phase2MaxTotal:        100,
		Phase2MaxPerInitiator: 100,
	})
	if err = c.processDispatchQueue(); err != nil {
		t.Fatal(err)
	}
	if c.new.Len() != 4 {
		t.Fatalf("dispatched %d messages, want 4", c.new.Len())
	}
	if predecessor.Size() == 0 {
		t.Fatal("the dispatch pass recorded nothing: the predecessor queue is not traced")
	}

	for i := range c.new {
		item := &c.new[i]
		carried := item.dispatchParsed

		recorded := predecessor.Size()
		var reparsed tlb.MsgEnvelope
		if err = parseExact(&reparsed, item.dispatchEnvelope); err != nil {
			t.Fatal(err)
		}
		if predecessor.Size() != recorded {
			t.Fatalf("reparsing emitted envelope %x recorded %d cells", item.hash, predecessor.Size()-recorded)
		}

		if carried.Msg != item.root || reparsed.Msg != item.root {
			t.Fatalf("envelope %x: carried and reparsed message cells are not the item's root", item.hash)
		}
		if carried.EmittedLT == nil || *carried.EmittedLT != item.lt {
			t.Fatalf("envelope %x carries emitted lt %v, item lt %d", item.hash, carried.EmittedLT, item.lt)
		}
		if carried.FwdFeeRemaining.Nano().Cmp(reparsed.FwdFeeRemaining.Nano()) != 0 {
			t.Fatalf("envelope %x carries fee %s, reparse %s",
				item.hash, carried.FwdFeeRemaining.Nano(), reparsed.FwdFeeRemaining.Nano())
		}

		for _, bits := range [][2]uint8{{0, 0}, {0, 4}, {96, 96}} {
			routed, routedReparse := *carried, reparsed
			for _, envelope := range []*tlb.MsgEnvelope{&routed, &routedReparse} {
				envelope.CurAddr = tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: bits[0]}
				envelope.NextAddr = tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: bits[1]}
			}
			got, err := routed.ToCell()
			if err != nil {
				t.Fatal(err)
			}
			want, err := routedReparse.ToCell()
			if err != nil {
				t.Fatal(err)
			}
			if got.HashKey() != want.HashKey() {
				t.Fatalf("envelope %x routed %v serializes differently from its reparse", item.hash, bits)
			}
		}

		// The zero route is the emitted form itself, so the carried envelope is
		// the very cell the descriptors reference, and the rewrites above
		// happened on copies.
		emitted, err := carried.ToCell()
		if err != nil {
			t.Fatal(err)
		}
		if emitted.HashKey() != item.dispatchEnvelope.HashKey() {
			t.Fatalf("carried envelope %x serializes differently from the emitted cell", item.hash)
		}
	}
}

// Covers the whole life of a dispatched message inside a build: the dispatch
// pass that emits it and the retirement that either delivers it in the block
// (msg_import_deferred_fin) or routes it into the outbound queue
// (msg_import_deferred_tr). The predecessor is built outside the timed loop.
func BenchmarkBuildShardDispatchDelivery(b *testing.B) {
	const accounts = 64

	for _, immediate := range []bool{false, true} {
		b.Run(fmt.Sprintf("accounts=%d/immediate=%t", accounts, immediate), func(b *testing.B) {
			req := dispatchDeliveryRequest(b, accounts, immediate)
			builder := testBuilder()
			ctx := context.Background()

			b.ReportAllocs()
			for b.Loop() {
				candidate, err := builder.BuildShard(ctx, req)
				if err != nil {
					b.Fatal(err)
				}
				if candidate.Stats.DispatchedMessages != accounts {
					b.Fatalf("dispatched %d messages, want %d", candidate.Stats.DispatchedMessages, accounts)
				}
			}
			b.ReportMetric(accounts, "messages/op")
		})
	}
}

// dispatchDeliveryRequest is a shard request whose predecessor holds one
// deferred message with metadata for each of accounts sources. With immediate
// the cut is complete and every destination is a live account of the shard, so
// the messages are delivered in the block; without it the cut is absent and
// every message is routed into the outbound queue instead.
func dispatchDeliveryRequest(tb testing.TB, accounts int, immediate bool) ShardRequest {
	tb.Helper()

	req := emptyCandidateRequest(tb)
	createdAt := req.Header.GenUtime - 1
	firstLT := requestStartLT(tb, req) - uint64(10+accounts)

	queue, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	destinations := make([]activeContract, 0, accounts)
	for i := range accounts {
		var sourceID [32]byte
		copy(sourceID[:], bytes.Repeat([]byte{byte(0x10 + i)}, 32))
		source := address.NewAddress(0, 0, sourceID[:])
		destination := address.NewAddress(0, 0, bytes.Repeat([]byte{byte(0x80 + i)}, 32))
		lt := firstLT + uint64(i)

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
			tb.Fatal(err)
		}
		envelope, err := (tlb.MsgEnvelope{
			CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
			NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
			FwdFeeRemaining: tlb.FromNanoTONU(1_000),
			Msg:             message,
			Metadata:        &tlb.MsgMetadata{Depth: 1, Initiator: source, InitiatorLT: lt},
		}).ToCell()
		if err != nil {
			tb.Fatal(err)
		}
		enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: lt, Msg: envelope}).ToCell()
		if err != nil {
			tb.Fatal(err)
		}

		messages := cell.NewDict(64)
		if err = messages.Set(dispatchLTKey(lt), enqueued); err != nil {
			tb.Fatal(err)
		}
		accountQueue, err := (tlb.AccountDispatchQueue{Messages: messages, Count: 1}).ToCell()
		if err != nil {
			tb.Fatal(err)
		}
		if err = queue.Set(dispatchAccountKey(sourceID), accountQueue); err != nil {
			tb.Fatal(err)
		}
		destinations = append(destinations, activeContract{
			address: destination,
			code:    externalAcceptCode(tb),
			balance: 10_000_000_000,
		})
	}

	if immediate {
		req.Internals = &msgpool.Cut{}
		req.Previous.State = stateWithAccounts(tb, req.Previous.State,
			activeContracts(tb, req.Header.GenUtime, destinations...))
	}
	req.Previous.State = previousStateWithDispatchQueue(tb, req.Previous.State, queue)
	return req
}
