package collator

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type externalImportFeeClock struct {
	now time.Time
}

func (c *externalImportFeeClock) Now() time.Time { return c.now }

func TestExternalImportFeeRejectionDoesNotAbortCandidate(t *testing.T) {
	type executionMode struct {
		name    string
		workers int
	}
	for _, mode := range []executionMode{
		{name: "sequential", workers: -1},
		{name: "waves_inline", workers: 1},
		{name: "waves_parallel", workers: 4},
	} {
		for _, intake := range []string{"batch", "ready_stream"} {
			t.Run(mode.name+"/"+intake, func(t *testing.T) {
				req := emptyCandidateRequest(t)
				req.internalWaveWorkers = mode.workers
				poor := address.NewAddress(0, 0, bytes.Repeat([]byte{0x71}, 32))
				funded := address.NewAddress(0, 0, bytes.Repeat([]byte{0x72}, 32))
				req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
					activeContract{address: poor, code: externalAcceptCode(t), balance: 1},
					activeContract{address: funded, code: externalAcceptCode(t), balance: 100_000_000_000},
				))

				clock := &externalImportFeeClock{now: time.Unix(1_900_000_000, 0)}
				pool := msgpool.New(msgpool.Config{Clock: clock, IncludedRetryDelay: time.Hour})
				t.Cleanup(pool.Close)
				var inputs []ExternalInput
				// Revisit both accounts so the wave path also reuses a rejected lane.
				for i, dst := range []*address.Address{poor, funded, poor, funded} {
					root, err := tlb.ToCell(&tlb.ExternalMessage{
						DstAddr: dst,
						Body:    cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell(),
					})
					if err != nil {
						t.Fatal(err)
					}
					// Descending priorities make the pool preserve this order.
					if _, err = pool.AddExternal(len(root.ToBOC()), root, nil, 4-i); err != nil {
						t.Fatal(err)
					}
				}
				shard := targetShardIdent(req.Shard)
				for _, selected := range pool.SelectForBlock(shard, 4) {
					input, err := NewExternalInput(selected)
					if err != nil {
						t.Fatal(err)
					}
					inputs = append(inputs, input)
				}
				if len(inputs) != 4 {
					t.Fatalf("selected %d messages, want 4", len(inputs))
				}
				req.MaxExternalAttempts = len(inputs)

				var candidate *Candidate
				var err error
				if intake == "batch" {
					req.Externals = inputs
					candidate, err = testBuilder().BuildShard(t.Context(), req)
				} else {
					stream, openErr := pool.OpenExternalStream(shard, 4)
					if openErr != nil {
						t.Fatal(openErr)
					}
					t.Cleanup(func() {
						if err := stream.Close(); err != nil {
							t.Error(err)
						}
					})
					candidate, _, err = testBuilder().buildShardWithReadyExternals(
						t.Context(), req, stream, time.Time{}, time.Time{}, 2, time.Time{}, time.Time{},
					)
				}
				if err != nil {
					t.Fatalf("underfunded external aborted collation: %v", err)
				}
				stats := candidate.Stats
				if stats.ExternalAttempts != 4 || stats.ExternalNotAccepted != 2 || stats.ExternalIncluded != 2 ||
					stats.Transactions != 2 || stats.ExternalInvalid != 0 || stats.ExternalSkippedLimit != 0 {
					t.Fatalf("unexpected stats: %+v", stats)
				}
				if len(candidate.Externals) != len(inputs) {
					t.Fatalf("feedback count = %d, want %d", len(candidate.Externals), len(inputs))
				}
				for i, feedback := range candidate.Externals {
					want := msgpool.ExternalIncluded
					if i%2 == 0 {
						want = msgpool.ExternalNotAccepted
					}
					if feedback.Ref != inputs[i].Ref || feedback.Outcome != want {
						t.Fatalf("feedback[%d] = %+v, want ref %v outcome %v", i, feedback, inputs[i].Ref, want)
					}
				}

				// Rejected imports must not change balances, transaction pointers,
				// descriptors or logical times compared with processing only funded messages.
				req.Externals = []ExternalInput{inputs[1], inputs[3]}
				acceptedOnly, err := testBuilder().BuildShard(t.Context(), req)
				if err != nil {
					t.Fatal(err)
				}
				if !bytes.Equal(candidate.BlockBOC, acceptedOnly.BlockBOC) ||
					candidate.State.HashKey() != acceptedOnly.State.HashKey() {
					t.Fatal("rejected imports changed the block or account state")
				}

				if err = pool.Complete(candidate.Externals); err != nil {
					t.Fatal(err)
				}
				if next := pool.SelectForBlock(shard, 4); len(next) != 0 {
					t.Fatal("processed externals were immediately selected again")
				}
				if stats := pool.Stats(); stats.RejectedDelayed != 2 {
					t.Fatalf("pool did not receive both import rejections: %+v", stats)
				}

				// Exercise the real retry generations: unpaid imports must eventually
				// leave the pool even though their TTL has not expired.
				for range msgpool.DefaultAccountRejectRetryLimit {
					clock.now = clock.now.Add(msgpool.DefaultAccountRejectRetryDelay)
					selected := pool.SelectForBlock(shard, 4)
					if len(selected) != 2 {
						t.Fatalf("retry selected %d messages, want 2 unpaid imports", len(selected))
					}
					req.Externals = nil
					for _, snapshot := range selected {
						input, err := NewExternalInput(snapshot)
						if err != nil {
							t.Fatal(err)
						}
						req.Externals = append(req.Externals, input)
					}
					retry, err := testBuilder().BuildShard(t.Context(), req)
					if err != nil {
						t.Fatal(err)
					}
					if err = pool.Complete(retry.Externals); err != nil {
						t.Fatal(err)
					}
				}
				if stats := pool.Stats(); stats.RejectedExhausted != 2 || stats.Pooled != 2 {
					t.Fatalf("unpaid imports survived their retry limit: %+v", stats)
				}
			})
		}
	}
}

func TestExternalImportFeeHandlingPreservesFatalErrors(t *testing.T) {
	req := emptyCandidateRequest(t)
	addr := address.NewAddress(0, 0, bytes.Repeat([]byte{0x73}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, addr, req.Header.GenUtime, 1))
	root, err := tlb.ToCell(&tlb.ExternalMessage{DstAddr: addr, Body: cell.BeginCell().EndCell()})
	if err != nil {
		t.Fatal(err)
	}
	message, err := tvm.PrepareMessage(root)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	c, err := testBuilder().prepare(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	lane, err := c.account(addr)
	if err != nil {
		t.Fatal(err)
	}

	// A storage failure takes precedence over the account's unpaid import.
	malformedStat := cell.BeginCell().EndCell()
	result, err := c.emulateFrom(lane, lane.current, malformedStat, message, 0)
	if err == nil || !strings.Contains(err.Error(), "invalid account storage stat") || result != nil {
		t.Fatalf("storage failure became a message rejection: result=%+v err=%v", result, err)
	}

	// Cancellation must survive even when TVM returns the known rejection.
	cancel()
	result, err = c.emulate(lane, message, 0)
	if !errors.Is(err, context.Canceled) || result != nil {
		t.Fatalf("cancelled import became a message rejection: result=%+v err=%v", result, err)
	}
}
