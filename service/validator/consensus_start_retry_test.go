package validator

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// failingOnceCollator fails its first start the way the local collator runtime
// fails a transient load of its durable sessions, and starts normally after.
type failingOnceCollator struct {
	*validatorTestCollator

	starts int
}

func (c *failingOnceCollator) Start(ctx context.Context) error {
	c.mu.Lock()
	c.starts++
	first := c.starts == 1
	c.mu.Unlock()
	if first {
		return errors.New("collator runtime: load sessions: transient failure")
	}

	return c.validatorTestCollator.Start(ctx)
}

func (c *failingOnceCollator) startCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()

	return c.starts
}

// Admission is a one-way latch, and the services it admits can fail to start.
// The start used to run only on the admission transition, so once that single
// attempt failed, the runner's retry of the event and every later masterchain
// block found consensus already admitted and left the validator without
// consensus sessions until a restart.
func TestConsensusServicesStartIsRetriedAfterAdmission(t *testing.T) {
	config := groupReplayTestConfig(t, groupReplayTestBytes(51))
	head := groupReplayTestState(t, 100, true, true, 0, config, 52)
	store := newGroupReplayTestStore()
	store.add(head, groupReplayTestParent(99, 53))
	store.setCurrent(head)

	localCollator := &failingOnceCollator{validatorTestCollator: &validatorTestCollator{}}
	extension, err := New(validatorTestOptions(Options{
		DisableInternals: true,
		EnableGroups:     true,
		LocalCollator:    localCollator,
		PrepareSession:   newSupervisorTestPreparer().prepare,
		HeadSettleDelay:  time.Hour,
		StatsInterval:    -1,
	}))(hooks.Node{Store: store, Logger: zerolog.Nop()})
	if err != nil {
		t.Fatal(err)
	}
	service := extension.(*Service)
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer closeGroupReplayTestService(t, service)

	if !service.consensusDeferred.Load() || localCollator.startCount() != 0 {
		t.Fatal("stale startup head did not defer consensus services")
	}

	fresh := consensusStartRetryFreshState(t, groupReplayTestState(t, 101, false, false, 100, config, 54))
	event := groupReplayTestEvent(fresh, head.Block)
	if err = service.OnBlockApplied(context.Background(), event); err == nil {
		t.Fatal("failed consensus services start was not reported to the apply runner")
	}
	if !service.consensusAdmitted.Load() {
		t.Fatal("fresh masterchain head did not admit consensus")
	}

	// The apply runner retries a failed hook with the same event.
	if err = service.OnBlockApplied(context.Background(), event); err != nil {
		t.Fatal(err)
	}
	service.consensusStartMu.Lock()
	started := service.consensusStarted
	service.consensusStartMu.Unlock()
	if !started || localCollator.startCount() != 2 {
		t.Fatalf("consensus services after the retried event: started=%t collator starts=%d, want true and 2",
			started, localCollator.startCount())
	}

	next := consensusStartRetryFreshState(t, groupReplayTestState(t, 102, false, false, 100, config, 55))
	if err = service.OnBlockApplied(context.Background(), groupReplayTestEvent(next, fresh.Block)); err != nil {
		t.Fatal(err)
	}
	if got := localCollator.startCount(); got != 2 {
		t.Fatalf("collator starts after a later masterchain block = %d, want 2", got)
	}
}

// consensusStartRetryFreshState re-stamps a masterchain state with the current
// generation time, so the consensus catch-up gate reads it as a synchronized
// head. gen_utime follows the magic, global_id, the 104-bit shard_ident, seq_no
// and vert_seq_no.
func consensusStartRetryFreshState(t *testing.T, input groups.StateInput) groups.StateInput {
	t.Helper()

	slice, err := input.Root.BeginParse()
	if err != nil {
		t.Fatal(err)
	}
	prefix := slice.MustLoadSlice(232)
	slice.MustLoadUInt(32)
	input.Root = cell.BeginCell().
		MustStoreSlice(prefix, 232).
		MustStoreUInt(uint64(time.Now().Unix()), 32).
		MustStoreBuilder(slice.ToBuilder()).
		EndCell()

	return input
}
