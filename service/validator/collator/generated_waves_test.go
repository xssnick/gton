package collator

import (
	"bytes"
	"context"
	"errors"
	"runtime"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestGeneratedWavesProduceTheSequentialBlockMultiSource(t *testing.T) {
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
				req := generatedWaveMultiSourceFixtureRequest(t, width.senders)
				req.internalWaveWorkers = arm.workers

				candidate, err := testBuilder().BuildShard(context.Background(), req)
				if err != nil {
					t.Fatalf("%s: %v", arm.name, err)
				}
				if candidate.Stats.ImmediateDelivered != uint32(width.senders) {
					t.Fatalf("%s: immediate deliveries = %d, want %d", arm.name,
						candidate.Stats.ImmediateDelivered, width.senders)
				}
				if candidate.Stats.EnqueuedMessages != 0 {
					t.Fatalf("%s: enqueued %d generated messages, want all %d delivered in-block", arm.name,
						candidate.Stats.EnqueuedMessages, width.senders)
				}

				if reference == nil {
					reference = candidate
					continue
				}
				if !bytes.Equal(candidate.BlockBOC, reference.BlockBOC) {
					t.Fatalf("%s produced a different block (%d B against %d B)", arm.name,
						len(candidate.BlockBOC), len(reference.BlockBOC))
				}
				if !bytes.Equal(candidate.CollatedData, reference.CollatedData) {
					t.Fatalf("%s produced different collated data", arm.name)
				}

				got, want := candidate.Stats, reference.Stats
				got.InternalsSpeculated, got.InternalsDiscarded = 0, 0
				want.InternalsSpeculated, want.InternalsDiscarded = 0, 0
				if got != want {
					t.Fatalf("%s produced different stats:\n got  %+v\n want %+v", arm.name, got, want)
				}
			}
		})
	}
}

func TestGeneratedWaveDefaultSingleWorkerUsesSequentialPath(t *testing.T) {
	previous := runtime.GOMAXPROCS(1)
	t.Cleanup(func() { runtime.GOMAXPROCS(previous) })

	c := &collation{}
	if got := c.generatedWaveParallelism(false); got != 0 {
		t.Fatalf("default generated parallelism = %d, want sequential", got)
	}

	c.req.internalWaveWorkers = 1
	if got := c.generatedWaveParallelism(false); got != 1 {
		t.Fatalf("explicit parity parallelism = %d, want inline wave", got)
	}
}

func TestGeneratedWaveReusesSenderPlanningScratch(t *testing.T) {
	req := emptyCandidateRequest(t)
	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	var sourceID [32]byte
	sourceID[0] = 1
	state := c.newGeneratedWavePlanState()
	state.bumpSenderGenerated(c, sourceID)
	scratch := c.generatedWaves.senderGenerated

	state = c.newGeneratedWavePlanState()
	if len(scratch) != 0 {
		t.Fatalf("sender planning scratch retained %d entries", len(scratch))
	}
	state.bumpSenderGenerated(c, sourceID)
	if got := scratch[sourceID]; got != 1 {
		t.Fatalf("reused sender planning scratch holds %d, want 1", got)
	}
}

func generatedWaveMultiSourceFixtureRequest(tb testing.TB, senders int) ShardRequest {
	tb.Helper()

	req := emptyCandidateRequest(tb)
	req.Internals = &msgpool.Cut{}
	req.Dispatch = DispatchPolicy{}

	contracts := make([]activeContract, 0, senders*2)
	externals := make([]ExternalInput, 0, senders)
	for i := 0; i < senders; i++ {
		sender := generatedWaveAddress(0xc1, i)
		receiver := generatedWaveAddress(0xd1, i)
		contracts = append(contracts, activeContract{
			address: sender,
			code:    externalSendCode(tb, inShardMessage(tb, receiver, 1_000_000_000)),
			balance: 100_000_000_000,
		}, activeContract{
			address: receiver,
			code:    externalAcceptCode(tb),
			balance: 10_000_000_000,
		})

		external, err := tlb.ToCell(&tlb.ExternalMessage{
			DstAddr: sender,
			Body:    cell.BeginCell().MustStoreUInt(uint64(i+1), 32).EndCell(),
		})
		if err != nil {
			tb.Fatal(err)
		}
		externals = append(externals, externalInput(tb, external))
	}
	req.Previous.State = stateWithAccounts(tb, req.Previous.State, activeContracts(tb, req.Header.GenUtime, contracts...))
	req.Externals = externals
	req.MaxExternalAttempts = senders
	return req
}

func generatedWaveFixtureRequest(tb testing.TB, receivers int) ShardRequest {
	tb.Helper()

	req := emptyCandidateRequest(tb)
	req.Internals = &msgpool.Cut{}
	req.Dispatch = DispatchPolicy{}

	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0xc1}, 32))
	messages := make([]*cell.Cell, 0, receivers)
	contracts := make([]activeContract, 0, receivers+1)
	contracts = append(contracts, activeContract{
		address: sender,
		balance: 100_000_000_000,
	})
	for i := 0; i < receivers; i++ {
		var raw [32]byte
		raw[0] = 0xd0
		raw[31] = byte(i + 1)
		receiver := address.NewAddress(0, 0, raw[:])
		messages = append(messages, inShardMessage(tb, receiver, 1_000_000_000))
		contracts = append(contracts, activeContract{
			address: receiver,
			code:    externalAcceptCode(tb),
			balance: 10_000_000_000,
		})
	}
	contracts[0].code = externalSendManyCode(tb, messages...)
	req.Previous.State = stateWithAccounts(tb, req.Previous.State, activeContracts(tb, req.Header.GenUtime, contracts...))

	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: sender,
		Body:    cell.BeginCell().MustStoreUInt(1, 32).EndCell(),
	})
	if err != nil {
		tb.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(tb, external)}
	req.MaxExternalAttempts = 1
	return req
}

func generatedWaveAddress(prefix byte, index int) *address.Address {
	var raw [32]byte
	raw[0] = prefix
	raw[31] = byte(index + 1)
	return address.NewAddress(0, 0, raw[:])
}

func TestGeneratedWaveDispatchErrorCleansUpStartedPlans(t *testing.T) {
	req := emptyCandidateRequest(t)
	sender := address.NewAddress(0, 0, bytes.Repeat([]byte{0xc1}, 32))
	recvA := address.NewAddress(0, 0, bytes.Repeat([]byte{0xc2}, 32))
	recvB := address.NewAddress(0, 0, bytes.Repeat([]byte{0xc3}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: recvA, code: externalAcceptCode(t), balance: 10_000_000_000},
		activeContract{address: recvB, code: externalAcceptCode(t), balance: 10_000_000_000},
	))

	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	laneA, err := c.account(recvA)
	if err != nil {
		t.Fatal(err)
	}
	c.generatedWaves.start(c, 2)
	defer c.generatedWaves.stop()

	for i, dst := range []*address.Address{recvA, recvB} {
		root, parsed, transaction := generatedWaveTestMessage(t, sender, dst, 100)
		c.new.push(newMessage{
			lt:           uint64(100 + i),
			hash:         root.HashKey(),
			root:         root,
			parsed:       parsed,
			transaction:  transaction,
			index:        uint32(i),
			parallelSafe: true,
		})
	}

	plans := c.planGeneratedWave()
	if len(plans) != 2 {
		t.Fatalf("planned %d generated messages, want 2", len(plans))
	}

	c.ctx = &failOnSecondErrContext{Context: context.Background()}
	err = c.runGeneratedPlans(plans, 2, new(bool), false)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runGeneratedPlans error = %v, want context.Canceled", err)
	}
	if !plans[0].started {
		t.Fatal("first generated plan never started")
	}
	if plans[1].started {
		t.Fatal("second generated plan started despite injected dispatch failure")
	}
	if laneA.tracer.buffering {
		t.Fatal("cleanup left the started generated lane tracer buffering")
	}
	if c.new.Len() != 2 {
		t.Fatalf("generated heap len = %d, want 2 pushed back plans", c.new.Len())
	}
}

func TestGeneratedWaveStartsAndRetiresFourMultiSourcePlans(t *testing.T) {
	req := emptyCandidateRequest(t)
	contracts := make([]activeContract, 0, 4)
	for i := 0; i < 4; i++ {
		contracts = append(contracts, activeContract{
			address: generatedWaveAddress(0xe1, i),
			code:    externalAcceptCode(t),
			balance: 10_000_000_000,
		})
	}
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime, contracts...))

	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	c.generatedWaves.start(c, 2)
	defer c.generatedWaves.stop()

	for i := 0; i < 4; i++ {
		sender := generatedWaveAddress(0xf1, i)
		receiver := generatedWaveAddress(0xe1, i)
		root, parsed, transaction := generatedWaveTestMessage(t, sender, receiver, 100)
		c.new.push(newMessage{
			lt:           100,
			hash:         root.HashKey(),
			root:         root,
			parsed:       parsed,
			transaction:  transaction,
			index:        uint32(i),
			parallelSafe: true,
		})
	}

	plans := c.planGeneratedWave()
	if len(plans) != 4 {
		t.Fatalf("planned %d generated messages, want 4", len(plans))
	}

	enqueueOnly := false
	if err = c.runGeneratedPlans(plans, 2, &enqueueOnly, false); err != nil {
		t.Fatal(err)
	}
	if enqueueOnly {
		t.Fatal("generated multi-source wave unexpectedly switched to enqueue-only")
	}
	if c.new.Len() != 0 {
		t.Fatalf("generated heap len = %d, want 0 after retirement", c.new.Len())
	}
	for i, plan := range plans {
		if !plan.started {
			t.Fatalf("plan %d was not started", i)
		}
	}
}

func TestGeneratedWaveThresholdFallsBackToSequential(t *testing.T) {
	req := emptyCandidateRequest(t)
	const messages = generatedWaveMinParallelWidth - 1
	contracts := make([]activeContract, 0, messages)
	for i := 0; i < messages; i++ {
		contracts = append(contracts, activeContract{
			address: generatedWaveAddress(0xa1, i),
			code:    externalAcceptCode(t),
			balance: 10_000_000_000,
		})
	}
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		activeContracts(t, req.Header.GenUtime, contracts...))

	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	for i := 0; i < messages; i++ {
		root, parsed, transaction := generatedWaveTestMessage(t, generatedWaveAddress(0xb1, i), generatedWaveAddress(0xa1, i), 200)
		c.new.push(newMessage{
			lt:           200,
			hash:         root.HashKey(),
			root:         root,
			parsed:       parsed,
			transaction:  transaction,
			index:        uint32(i),
			parallelSafe: true,
		})
	}

	if err = c.processNewMessagesInWaves(false, 16); err != nil {
		t.Fatal(err)
	}
	if c.generatedWaves.queue != nil {
		t.Fatal("generated worker pool started below the min parallel width")
	}
	for i, plan := range c.generatedWaves.plans {
		if plan.started {
			t.Fatalf("plan %d speculated below the min parallel width", i)
		}
	}
	if got := c.stats.ImmediateDelivered; got != messages {
		t.Fatalf("ImmediateDelivered = %d, want %d", got, messages)
	}
	if c.new.Len() != 0 {
		t.Fatalf("generated heap len = %d, want 0", c.new.Len())
	}
}

func TestGeneratedWaveThresholdStartsThePool(t *testing.T) {
	req := generatedWaveMultiSourceFixtureRequest(t, generatedWaveMinParallelWidth)
	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	defer c.stopWaves()

	if err = c.processExternals(); err != nil {
		t.Fatal(err)
	}
	if err = c.processNewMessagesInWaves(false, 4); err != nil {
		t.Fatal(err)
	}
	if c.generatedWaves.queue == nil {
		t.Fatal("generated worker pool did not start at the minimum parallel width")
	}
	if got := c.stats.ImmediateDelivered; got != generatedWaveMinParallelWidth {
		t.Fatalf("ImmediateDelivered = %d, want %d", got, generatedWaveMinParallelWidth)
	}
}

func generatedWaveTestMessage(tb testing.TB, sender, receiver *address.Address, lt uint64) (*cell.Cell, *tlb.Message, *cell.Cell) {
	tb.Helper()

	root, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		Bounce:      true,
		SrcAddr:     sender,
		DstAddr:     receiver,
		Amount:      tlb.MustFromTON("0.1"),
		FwdFee:      tlb.FromNanoTONU(1),
		CreatedLT:   lt,
		Body:        cell.BeginCell().MustStoreUInt(0, 32).EndCell(),
	})
	if err != nil {
		tb.Fatal(err)
	}
	var parsed tlb.Message
	if err = parseExact(&parsed, root); err != nil {
		tb.Fatal(err)
	}
	transaction := cell.BeginCell().MustStoreUInt(0, 1).EndCell()
	return root, &parsed, transaction
}
