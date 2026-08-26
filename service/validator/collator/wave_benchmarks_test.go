package collator

import (
	"bytes"
	"context"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type waveBenchmarkMode struct {
	name    string
	workers int
}

var internalWaveBenchmarkModes = []waveBenchmarkMode{
	{name: "sequential", workers: -1},
	{name: "waves-inline", workers: 1},
	{name: "waves-4", workers: 4},
}

var generatedWaveBenchmarkModes = []waveBenchmarkMode{
	{name: "sequential", workers: -1},
	{name: "waves-inline", workers: 1},
	{name: "waves-4", workers: 4},
}

var generatedWaveBenchmarkWidths = [...]int{7, 8, 16, 32}

// BenchmarkInboundInternalWaveLookAhead keeps four account lanes busy while
// preserving the A,B,A,C,D order that used to end planning at the second A.
// Twelve repetitions fit in one capped wave and make the dependency chains
// long enough that scheduler overhead does not dominate the comparison.
func BenchmarkInboundInternalWaveLookAhead(b *testing.B) {
	const cycles = 12
	wantImported := uint32(cycles * 5)

	for _, mode := range internalWaveBenchmarkModes {
		b.Run(mode.name, func(b *testing.B) {
			req := internalWaveLookAheadBenchmarkRequest(b, cycles)
			req.internalWaveWorkers = mode.workers
			builder := testBuilder()
			ctx := context.Background()

			candidate, err := builder.BuildShard(ctx, req)
			if err != nil {
				b.Fatal(err)
			}
			if candidate.Stats.InternalsImported != wantImported || candidate.Stats.Transactions != wantImported {
				b.Fatalf("fixture imported %d internals in %d transactions, want %d",
					candidate.Stats.InternalsImported, candidate.Stats.Transactions, wantImported)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err = builder.BuildShard(ctx, req); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}

// BenchmarkGeneratedMultiSourceWave measures the real collation path after
// independent sender transactions emit messages at the same logical time to
// distinct receivers. Every generated run is safe and conflict-free. Width 7
// pins the below-threshold planned-serial path; the remaining widths exercise
// the worker pool.
// Preparation and the sender phase run with the timer stopped; the measurement
// starts with the populated heap and includes generated pool startup and join.
func BenchmarkGeneratedMultiSourceWave(b *testing.B) {
	for _, senders := range generatedWaveBenchmarkWidths {
		b.Run(fmt.Sprintf("width=%d", senders), func(b *testing.B) {
			wantTransactions := uint32(senders * 2)

			for _, mode := range generatedWaveBenchmarkModes {
				b.Run(mode.name, func(b *testing.B) {
					req := generatedMultiSourceWaveBenchmarkRequest(b, senders)
					setupReq := req
					// The untimed sender phase runs inline but leaves its outputs
					// certified; the measured drain installs the requested mode below.
					setupReq.internalWaveWorkers = 1
					builder := testBuilder()
					ctx := context.Background()

					assertGeneratedWaveBenchmarkEligible(b, setupReq, senders)

					candidateReq := req
					candidateReq.internalWaveWorkers = mode.workers
					candidate, err := builder.BuildShard(ctx, candidateReq)
					if err != nil {
						b.Fatal(err)
					}
					if candidate.Stats.ExternalIncluded != uint32(senders) ||
						candidate.Stats.NewMessages != uint32(senders) ||
						candidate.Stats.ImmediateDelivered != uint32(senders) ||
						candidate.Stats.Transactions != wantTransactions ||
						candidate.Stats.EnqueuedMessages != 0 {
						b.Fatalf("fixture produced %d included externals, %d new messages, %d immediate deliveries, "+
							"%d transactions and %d enqueues; want %d, %d, %d, %d and 0",
							candidate.Stats.ExternalIncluded, candidate.Stats.NewMessages,
							candidate.Stats.ImmediateDelivered, candidate.Stats.Transactions,
							candidate.Stats.EnqueuedMessages, senders, senders, senders, wantTransactions)
					}

					b.ReportAllocs()
					b.ResetTimer()
					for b.Loop() {
						b.StopTimer()
						c, prepareErr := builder.prepare(ctx, setupReq)
						if prepareErr == nil {
							prepareErr = c.processExternals()
						}
						if prepareErr != nil {
							b.Fatal(prepareErr)
						}
						if c.new.Len() != senders {
							b.Fatalf("fixture generated %d pending messages, want %d", c.new.Len(), senders)
						}
						c.req.internalWaveWorkers = mode.workers

						b.StartTimer()
						err = c.processNewMessages(false)
						c.stopWaves()
						b.StopTimer()
						if err != nil {
							b.Fatal(err)
						}
						if c.stats.ImmediateDelivered != uint32(senders) ||
							c.limits.transactions != uint64(wantTransactions) {
							b.Fatalf("drain delivered %d messages in %d transactions, want %d and %d",
								c.stats.ImmediateDelivered, c.limits.transactions, senders, wantTransactions)
						}
						b.StartTimer()
					}
				})
			}
		})
	}
}

func generatedMultiSourceWaveBenchmarkRequest(tb testing.TB, senders int) ShardRequest {
	tb.Helper()

	req := generatedWaveMultiSourceFixtureRequest(tb, senders)
	state := loadPreviousShardState(tb, req)
	receiverCode := benchWorkCode(tb, benchReceiverLoops, nil)
	for i := range senders {
		sender := generatedWaveAddress(0xc1, i)
		receiver := generatedWaveAddress(0xd1, i)
		benchSetAccount(tb, state.Accounts.ShardAccounts, activeContract{
			address: sender,
			code:    benchWorkCode(tb, benchSenderLoops, inShardMessage(tb, receiver, 1_000_000_000)),
			balance: 100_000_000_000,
		}, req.Header.GenUtime)
		benchSetAccount(tb, state.Accounts.ShardAccounts, activeContract{
			address: receiver,
			code:    receiverCode,
			balance: 10_000_000_000,
		}, req.Header.GenUtime)
	}
	req.Previous.State = stateWithAccounts(tb, req.Previous.State, state.Accounts.ShardAccounts)

	return req
}

func assertGeneratedWaveBenchmarkEligible(
	tb testing.TB,
	req ShardRequest,
	want int,
) {
	tb.Helper()

	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		tb.Fatal(err)
	}
	defer c.stopWaves()

	if err = c.processExternals(); err != nil {
		tb.Fatal(err)
	}
	if c.new.Len() != want {
		tb.Fatalf("fixture generated %d pending messages, want %d", c.new.Len(), want)
	}
	for i, message := range c.new {
		if !message.parallelSafe {
			tb.Fatalf("generated message %d is not certified for parallel execution", i)
		}
	}

	c.generatedWaves.start(c, 1)
	plans := c.planGeneratedWave()
	if len(plans) != want {
		tb.Fatalf("fixture planned %d messages, want %d", len(plans), want)
	}
}

func internalWaveLookAheadBenchmarkRequest(tb testing.TB, cycles int) ShardRequest {
	tb.Helper()

	req := emptyCandidateRequest(tb)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xe1}, 32))
	receivers := []*address.Address{
		address.NewAddress(0, 0, bytes.Repeat([]byte{0xe2}, 32)),
		address.NewAddress(0, 0, bytes.Repeat([]byte{0xe3}, 32)),
		address.NewAddress(0, 0, bytes.Repeat([]byte{0xe4}, 32)),
		address.NewAddress(0, 0, bytes.Repeat([]byte{0xe5}, 32)),
	}
	receiverCode := benchWorkCode(tb, benchReceiverLoops, nil)
	contracts := make([]activeContract, len(receivers))
	for i, receiver := range receivers {
		contracts[i] = activeContract{
			address: receiver,
			code:    receiverCode,
			balance: 100_000_000_000,
		}
	}
	req.Previous.State = stateWithAccounts(tb, req.Previous.State,
		activeContracts(tb, req.Header.GenUtime, contracts...))

	order := [...]int{0, 1, 0, 2, 3}
	count := cycles * len(order)
	startLT := requestStartLT(tb, req)
	fee := tlb.FromNanoTONU(100_000)
	owner := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	messages := make([]*msgpool.InternalMessage, 0, count)
	for i := range count {
		receiver := receivers[order[i%len(order)]]
		createdLT := startLT - uint64(count-i)
		msg, enqueued := queuedInternal(tb, source, receiver, createdLT,
			req.Header.GenUtime-1, fee, fee, 96, owner)
		req.Previous.State = stateWithQueueMessage(tb, req.Previous.State, msg.Key, enqueued)
		messages = append(messages, msg)
	}

	queueSize := uint64(count)
	req.Previous.OutQueueSize = &queueSize
	req.Internals = &msgpool.Cut{Messages: messages}

	return req
}
