package validator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// The producer may fail terminally between the backend's last lifecycle call
// and a finalized block: the block has already crossed local ingress, so the
// watermark refusal quarantines the producer instead of failing acceptance.
func TestLocalBlockAcceptanceQuarantinesProducerThatFailedAfterIngress(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: math.MinInt64})
	node := &acceptanceTestNode{}
	accepter, err := newAcceptanceTestAccepter(fixture, node)
	if err != nil {
		t.Fatal(err)
	}

	observations := 0
	backend := &LocalSessionBackend{
		config:    fixture.config,
		accepter:  accepter,
		validator: &ValidatorIdentity{},
		finalized: func(context.Context, ton.BlockIDExt) error {
			observations++
			return fmt.Errorf("session journal failed: %w", collator.ErrSessionUnavailable)
		},
	}
	prepared, err := backend.PrepareBlockAcceptance(t.Context(), fixture.acceptance(simplex.VoteFinalize, false))
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 1; attempt <= 2; attempt++ {
		if err = prepared.Submit(t.Context()); err != nil {
			t.Fatalf("submit %d after the producer failed = %v, want accepted", attempt, err)
		}
	}
	if observations != 1 || len(node.blocks) != 2 {
		t.Fatalf("observations/submissions = %d/%d, want 1/2", observations, len(node.blocks))
	}
}

// A leader-window handoff or a consensus-progress barrier wait holds controlMu;
// finalized blocks must keep flowing through ingress and description meanwhile.
func TestLocalBlockAcceptanceDoesNotWaitForBackendControl(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: math.MinInt64})
	acceptance := fixture.acceptance(simplex.VoteFinalize, false)
	acceptance.Replay = true
	node := &acceptanceTestNode{}
	accepter, err := newAcceptanceTestAccepter(fixture, node)
	if err != nil {
		t.Fatal(err)
	}
	view := fixture.view(t)
	backend := &LocalSessionBackend{
		config: fixture.config,
		state: SessionState{
			MasterchainBlock: view.MasterchainBlock,
			Registered:       view.Registered,
		},
		groups: localBackendTestGroups{snapshot: &groups.Snapshot{
			MasterchainBlock: view.MasterchainBlock,
			Active: []groups.Session{{
				Shard:      fixture.config.Shard,
				Registered: view.Registered,
			}},
		}},
		accepter: accepter,
	}

	backend.controlMu.Lock()
	defer backend.controlMu.Unlock()
	accepted := make(chan error, 1)
	go func() {
		accepted <- acceptLocalBackendTestBlock(context.Background(), backend, acceptance)
	}()
	select {
	case err = <-accepted:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("block acceptance waited for the backend control lock")
	}
}

// Every masterchain block reaches every session; unchanged simplex parameters
// must not queue behind votes on the engine loop while lifecycleMu is held.
func TestSessionRuntimeUpdateSkipsUnchangedSimplexParams(t *testing.T) {
	runtime, _ := prepareRuntimeTest(t, 0x7a, newRuntimeTestStorage(), newRuntimeTestNetwork(), newRuntimeTestBackend())

	changed := runtimeTestState()
	changed.Params.TargetRate += time.Millisecond
	changed.Params.CandidateResolveTimeoutCap += time.Millisecond
	if err := runtime.Update(t.Context(), changed); err != nil {
		t.Fatal(err)
	}
	runtime.states.mu.Lock()
	targetRate, resolveCap := runtime.states.targetRate, runtime.states.resolveTimeoutCap
	runtime.states.mu.Unlock()
	runtime.candidates.mu.Lock()
	candidateParams := runtime.candidates.params
	runtime.candidates.mu.Unlock()
	if targetRate != changed.Params.TargetRate || resolveCap != changed.Params.CandidateResolveTimeoutCap ||
		candidateParams != changed.Params {
		t.Fatal("changed simplex params did not reach the resolvers")
	}

	// Nothing serves this runner's loop: a params round trip would never return.
	runtime.lifecycleMu.Lock()
	runtime.runnerLaunched = true
	runtime.lifecycleMu.Unlock()
	unchanged := changed
	unchanged.MasterchainBlock.SeqNo++
	updated := make(chan error, 1)
	go func() { updated <- runtime.Update(context.Background(), unchanged) }()
	select {
	case err := <-updated:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("update with unchanged simplex params waited for the engine loop")
	}
	runtime.stateMu.RLock()
	seqno := runtime.state.MasterchainBlock.SeqNo
	runtime.stateMu.RUnlock()
	if seqno != unchanged.MasterchainBlock.SeqNo {
		t.Fatalf("runtime state MC seqno = %d, want %d", seqno, unchanged.MasterchainBlock.SeqNo)
	}
	if err := runtime.Close(); err != nil {
		t.Fatal(err)
	}
}

type observerBlockingCloseTestRuntime struct {
	observerSessionRuntime
	once    sync.Once
	entered chan struct{}
	release chan struct{}
}

func (r *observerBlockingCloseTestRuntime) Close() error {
	r.once.Do(func() {
		close(r.entered)
		<-r.release
	})

	return r.observerSessionRuntime.Close()
}

// While a dead runtime drains in Close, masterchain updates belong to the
// restart owner; the dead runtime's terminal error must not reach MC apply.
func TestConsensusObserverUpdateWhileDeadRuntimeClosesQueuesForRestart(t *testing.T) {
	t.Parallel()
	fixture, network := newObserverRecoveryFixture(t)
	if err := fixture.observer.PrepareSession(t.Context(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	session, err := fixture.observer.session(fixture.activation.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	blocking := &observerBlockingCloseTestRuntime{entered: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(blocking.release) }) }
	t.Cleanup(release)
	session.mu.Lock()
	blocking.observerSessionRuntime = session.runtime
	session.runtime = blocking
	session.mu.Unlock()
	if err = fixture.observer.ActivateSession(t.Context(), fixture.activation); err != nil {
		t.Fatal(err)
	}

	network.mu.Lock()
	endpoint := network.endpoints[len(network.endpoints)-1]
	network.mu.Unlock()
	endpoint.runFailures <- errors.New("private overlay receiver failed")
	select {
	case <-blocking.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("dead runtime was not closed")
	}

	updated := fixture.descriptor
	updated.Update.MasterchainBlock.SeqNo++
	if err = fixture.observer.UpdateSession(t.Context(), updated); err != nil {
		t.Fatalf("update while the dead runtime closes: %v", err)
	}
	release()
	waitObserverRecovery(t, session)

	session.mu.Lock()
	current := session.runtime.(*sessionRuntime)
	session.mu.Unlock()
	current.stateMu.RLock()
	seqno := current.state.MasterchainBlock.SeqNo
	current.stateMu.RUnlock()
	if seqno != updated.Update.MasterchainBlock.SeqNo {
		t.Fatalf("recovered runtime MC seqno = %d, want %d", seqno, updated.Update.MasterchainBlock.SeqNo)
	}
}
