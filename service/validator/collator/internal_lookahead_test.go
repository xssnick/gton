package collator

import (
	"bytes"
	"context"
	"errors"
	"sync/atomic"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestInternalWaveLooksPastTheFirstRepeatedDestination(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa1}, 32))
	recvA := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa2}, 32))
	recvB := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa3}, 32))
	recvC := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa4}, 32))
	recvD := address.NewAddress(0, 0, bytes.Repeat([]byte{0xa5}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: recvA, code: externalAcceptCode(t), balance: 10_000_000_000},
		activeContract{address: recvB, code: externalAcceptCode(t), balance: 10_000_000_000},
		activeContract{address: recvC, code: externalAcceptCode(t), balance: 10_000_000_000},
		activeContract{address: recvD, code: externalAcceptCode(t), balance: 10_000_000_000},
	))

	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	c.waves.start(c, 1)
	defer c.waves.stop()

	startLT := requestStartLT(t, req)
	fee := tlb.FromNanoTONU(100_000)
	owner := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	msgA1, _ := queuedInternal(t, source, recvA, startLT-50, req.Header.GenUtime-1, fee, fee, 96, owner)
	msgB, _ := queuedInternal(t, source, recvB, startLT-40, req.Header.GenUtime-1, fee, fee, 96, owner)
	msgA2, _ := queuedInternal(t, source, recvA, startLT-30, req.Header.GenUtime-1, fee, fee, 96, owner)
	msgC, _ := queuedInternal(t, source, recvC, startLT-20, req.Header.GenUtime-1, fee, fee, 96, owner)
	msgD, _ := queuedInternal(t, source, recvD, startLT-10, req.Header.GenUtime-1, fee, fee, 96, owner)

	plans, ready := c.planWave([]*msgpool.InternalMessage{msgA1, msgB, msgA2, msgC, msgD}, nil)
	if len(plans) != 5 {
		t.Fatalf("planned %d messages, want the whole A,B,A,C,D window", len(plans))
	}
	if ready != 4 {
		t.Fatalf("ready heads = %d, want A,B,C,D", ready)
	}
	if plans[0].dependsOn != nil || plans[0].follows != plans[2] {
		t.Fatal("first A plan is not the head of A's chain")
	}
	if plans[2].dependsOn != plans[0] || plans[2].follows != nil {
		t.Fatal("second A plan is not chained behind the first")
	}
	for i, plan := range plans {
		if !plan.executes {
			t.Fatalf("plan %d does not execute, want every fixture message to hit an account", i)
		}
		if plan.started {
			t.Fatalf("plan %d was started during planning", i)
		}
	}
}

func TestDispatchInternalPlanStopsOnCanceledContext(t *testing.T) {
	req := emptyCandidateRequest(t)
	recv := address.NewAddress(0, 0, bytes.Repeat([]byte{0xb2}, 32))
	req.Previous.State = stateWithAccounts(t, req.Previous.State,
		accountsWithActiveContract(t, recv, req.Header.GenUtime, 10_000_000_000))

	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	lane, err := c.account(recv)
	if err != nil {
		t.Fatal(err)
	}

	cancelledCtx, cancel := context.WithCancel(context.Background())
	cancel()
	c.ctx = cancelledCtx

	plan := &internalPlan{
		executes: true,
		key:      lane.key,
		lane:     lane,
	}
	if err = c.dispatchInternalPlan(plan, 2); !errors.Is(err, context.Canceled) {
		t.Fatalf("dispatchInternalPlan error = %v, want context.Canceled", err)
	}
	if plan.started {
		t.Fatal("dispatchInternalPlan started a canceled plan")
	}
	if lane.tracer.buffering {
		t.Fatal("dispatchInternalPlan left the lane tracer buffering after cancellation")
	}
}

type failOnSecondErrContext struct {
	context.Context
	calls atomic.Int32
}

func (c *failOnSecondErrContext) Err() error {
	if c.calls.Add(1) >= 2 {
		return context.Canceled
	}
	return nil
}

func TestInternalWaveDispatchErrorCleansUpStartedHeads(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xc1}, 32))
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
	c.waves.start(c, 2)
	defer c.waves.stop()
	c.ctx = &failOnSecondErrContext{Context: context.Background()}

	startLT := requestStartLT(t, req)
	fee := tlb.FromNanoTONU(100_000)
	owner := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	msgA, _ := queuedInternal(t, source, recvA, startLT-20, req.Header.GenUtime-1, fee, fee, 96, owner)
	msgB, _ := queuedInternal(t, source, recvB, startLT-10, req.Header.GenUtime-1, fee, fee, 96, owner)

	plans, ready := c.planWave([]*msgpool.InternalMessage{msgA, msgB}, nil)
	if ready != 2 {
		t.Fatalf("ready heads = %d, want 2", ready)
	}

	_, err = c.runWave(plans, ready, 2, 0)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("runWave error = %v, want context.Canceled", err)
	}
	if !plans[0].started {
		t.Fatal("first head never started, so the cleanup path was not exercised")
	}
	if plans[1].started {
		t.Fatal("second head started despite the injected dispatch failure")
	}
	if laneA.tracer.buffering {
		t.Fatal("cleanup left the started head's tracer buffering")
	}
	if got := c.stats.InternalsDiscarded; got != 1 {
		t.Fatalf("InternalsDiscarded = %d, want 1 started head cleaned up", got)
	}
}

func TestInternalWavePrefersReadySuccessors(t *testing.T) {
	w := waveState{
		queue:      make(chan *internalPlan, 1),
		successors: make(chan *internalPlan, 1),
	}
	normal := &internalPlan{}
	successor := &internalPlan{}
	w.queue <- normal
	w.successors <- successor

	plan, ok := w.next()
	if !ok {
		t.Fatal("worker queue stopped with a ready successor")
	}
	if plan != successor {
		t.Fatal("worker took speculative look-ahead before a ready successor")
	}
}

func TestInternalWavePoolStartsLazilyAndGrows(t *testing.T) {
	c := &collation{}
	c.waves.start(c, 1)
	if c.waves.queue != nil || c.waves.workerCount != 0 {
		t.Fatal("single ready account started the inbound worker pool")
	}

	c.waves.start(c, 2)
	queue := c.waves.queue
	if queue == nil || c.waves.workerCount != 2 {
		t.Fatalf("two ready accounts started %d workers", c.waves.workerCount)
	}
	c.waves.start(c, 4)
	if c.waves.queue != queue {
		t.Fatal("growing the inbound pool replaced its queue")
	}
	if c.waves.workerCount != 4 {
		t.Fatalf("grown inbound worker count = %d, want 4", c.waves.workerCount)
	}

	c.waves.stop()
	if c.waves.queue != nil || c.waves.workerCount != 0 {
		t.Fatal("stopping the inbound pool left workers behind")
	}
}
