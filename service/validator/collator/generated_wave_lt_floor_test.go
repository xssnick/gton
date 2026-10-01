package collator

import (
	"bytes"
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// generatedWaveFloorDispatched is how many deferred messages the floored
// destination emits in the dispatch phase. Three put its floor at StartLt+3,
// above the StartLt+2 every generated message of the fixture carries.
const generatedWaveFloorDispatched = 3

func TestGeneratedWavesApplyTheDispatchLTFloor(t *testing.T) {
	arms := []struct {
		name    string
		workers int
	}{
		{"sequential", -1},
		{"waves-inline", 1},
		{"waves-4", 4},
	}

	widths := []struct {
		name    string
		senders int
	}{
		{"below-threshold", generatedWaveMinParallelWidth - 1},
		{"parallel-threshold", generatedWaveMinParallelWidth},
	}
	for _, width := range widths {
		t.Run(width.name, func(t *testing.T) {
			var reference *Candidate
			for _, arm := range arms {
				req, sender, floored := generatedWaveDispatchFloorFixtureRequest(t, width.senders)
				req.internalWaveWorkers = arm.workers

				candidate, err := testBuilder().BuildShard(context.Background(), req)
				if err != nil {
					t.Fatalf("%s: %v", arm.name, err)
				}
				if candidate.Stats.DispatchedMessages != generatedWaveFloorDispatched {
					t.Fatalf("%s: dispatched %d deferred messages, want %d", arm.name,
						candidate.Stats.DispatchedMessages, generatedWaveFloorDispatched)
				}

				// The generated message reaches the floored destination below its
				// floor, so the floor and not the message decides the lt, as in
				// create_ordinary_transaction (collator.cpp:3320-3323).
				startLT := requestStartLT(t, req)
				if got := transactionLTOf(t, candidate, sender); got != startLT+1 {
					t.Fatalf("%s: sender transaction lt = %d, want %d", arm.name, got, startLT+1)
				}
				if got, want := transactionLTOf(t, candidate, floored), startLT+generatedWaveFloorDispatched+1; got != want {
					t.Errorf("%s: floored destination transaction lt = %d, want %d", arm.name, got, want)
				}

				if reference == nil {
					reference = candidate
					continue
				}
				if !bytes.Equal(candidate.BlockBOC, reference.BlockBOC) {
					t.Errorf("%s produced a different block (%d B against %d B)", arm.name,
						len(candidate.BlockBOC), len(reference.BlockBOC))
				}
				if !bytes.Equal(candidate.CollatedData, reference.CollatedData) {
					t.Errorf("%s produced different collated data", arm.name)
				}
			}
		})
	}
}

// generatedWaveDispatchFloorFixtureRequest is the multi-source fixture whose
// first receiver drained generatedWaveFloorDispatched deferred messages to a
// bystander before the first sender's generated message reaches it. It returns
// that sender and the floored receiver.
func generatedWaveDispatchFloorFixtureRequest(tb testing.TB, senders int) (ShardRequest, *address.Address, *address.Address) {
	tb.Helper()

	req := generatedWaveMultiSourceFixtureRequest(tb, senders)
	req.Dispatch = DispatchPolicy{
		DeferMessagesAfter:    10,
		Phase2MaxTotal:        150,
		Phase2MaxPerInitiator: 20,
	}
	sender := generatedWaveAddress(0xc1, 0)
	floored := generatedWaveAddress(0xd1, 0)
	bystander := generatedWaveAddress(0xe1, 0)

	accounts := loadPreviousShardState(tb, req).Accounts.ShardAccounts
	bystanderAccount := activeShardAccount(tb, activeContract{
		address: bystander,
		code:    externalAcceptCode(tb),
		balance: 10_000_000_000,
	}, req.Header.GenUtime)
	if err := accounts.Set(cell.BeginCell().MustStoreSlice(bystander.Data(), 256).EndCell(), bystanderAccount); err != nil {
		tb.Fatal(err)
	}
	req.Previous.State = stateWithAccounts(tb, req.Previous.State, accounts)

	queue := generatedWaveFloorDispatchQueue(tb, floored, bystander, req.Header.GenUtime-1, requestStartLT(tb, req)-10)
	req.Previous.State = previousStateWithDispatchQueue(tb, req.Previous.State, queue)
	return req, sender, floored
}

func generatedWaveFloorDispatchQueue(
	tb testing.TB,
	source, destination *address.Address,
	createdAt uint32,
	lt uint64,
) *tlb.DispatchQueueAugDict {
	tb.Helper()

	messages := cell.NewDict(64)
	for i := range uint64(generatedWaveFloorDispatched) {
		// The middle message is emitted at StartLt+2, the lt of every generated
		// message, and would split their wave wherever its hash sorts among
		// them. A zero first hash byte puts it ahead.
		var message *cell.Cell
		for nonce := uint64(0); ; nonce++ {
			var err error
			message, err = tlb.ToCell(&tlb.InternalMessage{
				IHRDisabled: true,
				SrcAddr:     source,
				DstAddr:     destination,
				Amount:      tlb.FromNanoTONU(1_000_000),
				FwdFee:      tlb.FromNanoTONU(1_000),
				CreatedLT:   lt + i,
				CreatedAt:   createdAt,
				Body:        cell.BeginCell().MustStoreUInt(nonce, 64).EndCell(),
			})
			if err != nil {
				tb.Fatal(err)
			}
			if message.HashKey()[0] == 0 {
				break
			}
		}

		envelope, err := (tlb.MsgEnvelope{
			CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
			NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
			FwdFeeRemaining: tlb.FromNanoTONU(1_000),
			Msg:             message,
		}).ToCell()
		if err != nil {
			tb.Fatal(err)
		}
		enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: lt + i, Msg: envelope}).ToCell()
		if err != nil {
			tb.Fatal(err)
		}
		if err = messages.Set(dispatchLTKey(lt+i), enqueued); err != nil {
			tb.Fatal(err)
		}
	}

	accountQueue, err := (tlb.AccountDispatchQueue{Messages: messages, Count: generatedWaveFloorDispatched}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	queue, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		tb.Fatal(err)
	}
	var sourceID [32]byte
	copy(sourceID[:], source.Data())
	if err = queue.Set(dispatchAccountKey(sourceID), accountQueue); err != nil {
		tb.Fatal(err)
	}
	return queue
}
