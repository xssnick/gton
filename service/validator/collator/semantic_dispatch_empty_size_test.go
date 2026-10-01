package collator

import (
	"bytes"
	"errors"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type dispatchStoredSizeCase struct {
	name        string
	nonempty    bool
	storedSize  *uint64
	wantInvalid bool
}

func TestVerifyShardCandidateDispatchWithoutStoredQueueSize(t *testing.T) {
	zero, one := uint64(0), uint64(1)
	tests := []dispatchStoredSizeCase{
		{name: "empty queue infers zero", wantInvalid: true},
		{name: "empty queue stores zero", storedSize: &zero, wantInvalid: true},
		{name: "nonempty queue has unknown stored size", nonempty: true},
		{name: "nonempty queue stores a small size", nonempty: true, storedSize: &one, wantInvalid: true},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				request := generatedWaveMultiSourceFixtureRequest(t, 1)
				time.Sleep(time.Unix(int64(request.Header.GenUtime), 0).Sub(time.Now()))
				if test.nonempty {
					start := requestStartLT(t, request)
					masterchainQueueMessage(t, &request,
						address.NewAddress(0, 0, bytes.Repeat([]byte{0xb1}, 32)),
						address.NewAddress(0, 0xff, bytes.Repeat([]byte{0xb3}, 32)), start-30, start-30)
					request.Previous.OutQueueSize = &one
				}

				candidate, err := testBuilder().BuildShard(t.Context(), request)
				if err != nil {
					t.Fatal(err)
				}
				if candidate.Stats.ImmediateDelivered != 1 {
					t.Fatalf("immediate imports = %d, want 1", candidate.Stats.ImmediateDelivered)
				}
				verification := shardVerificationRequest(request, candidate)
				verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
				if err = VerifyShardCandidate(t.Context(), verification); err != nil {
					t.Fatalf("candidate without pending dispatch: %v", err)
				}

				// Add the same untouched account queue to both endpoints. The
				// ordinary import stays valid except for the required dispatch pass.
				pending := makeDispatchQueue(t, dispatchFixtureAccount{
					accountID: [32]byte{0x81}, lts: []uint64{11},
				})
				request.Previous.State = stateWithDispatchAndQueueSize(t, request.Previous.State, pending, test.storedSize)
				if !test.nonempty {
					request.Previous.OutQueueSize = nil
				}
				candidate.State = stateWithDispatchAndQueueSize(t, candidate.State, pending, &candidate.Stats.OutQueueSize)
				update, err := cell.CreateMerkleUpdate(request.Previous.State, candidate.State)
				if err != nil {
					t.Fatal(err)
				}
				candidate.StateUpdate = update
				rewriteVerificationShardBlock(t, candidate, func(block *tlb.Block) { block.StateUpdate = update })
				verification.Previous = request.Previous
				err = VerifyShardCandidate(t.Context(), verification)
				if !test.wantInvalid {
					if err != nil {
						t.Fatalf("unknown nonempty queue size enables dispatch gate: %v", err)
					}
					return
				}
				if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "before every dispatch account advances") {
					t.Fatalf("unprocessed dispatch queue error = %v", err)
				}
			})
		})
	}
}

func TestVerifyShardCandidateDispatchProgressWithoutStoredQueueSize(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		request := generatedWaveMultiSourceFixtureRequest(t, 1)
		time.Sleep(time.Unix(int64(request.Header.GenUtime), 0).Sub(time.Now()))
		pending := nonEmptyDispatchQueue(t, request.Header.GenUtime-1, requestStartLT(t, request)-10)
		request.Previous.State = stateWithDispatchAndQueueSize(t, request.Previous.State, pending, nil)
		request.Previous.OutQueueSize = nil
		candidate, err := testBuilder().BuildShard(t.Context(), request)
		if err != nil {
			t.Fatal(err)
		}
		if candidate.Stats.DispatchedMessages != 1 || candidate.Stats.ImmediateDelivered != 2 {
			t.Fatalf("dispatched/immediate messages = %d/%d, want 1/2", candidate.Stats.DispatchedMessages, candidate.Stats.ImmediateDelivered)
		}
		verification := shardVerificationRequest(request, candidate)
		verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
		if err = VerifyShardCandidate(t.Context(), verification); err != nil {
			t.Fatalf("dispatch progress followed by ordinary import: %v", err)
		}
	})
}

func stateWithDispatchAndQueueSize(t *testing.T, root *cell.Cell, dispatch *tlb.DispatchQueueAugDict, size *uint64) *cell.Cell {
	t.Helper()
	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	queue.Extra = &tlb.OutMsgQueueExtra{DispatchQueue: dispatch, OutQueueSize: size}
	var err error
	state.OutMsgQueueInfo, err = queue.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	result, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}
	return result
}
