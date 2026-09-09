package validator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/collator"
)

// Every preparation gets a fresh endpoint, matching network.Manager's
// single-use receiver contract. Reusing the failed endpoint cannot pass.
type observerRecoveryTestNetwork struct {
	*observerTestNetwork
	mu        sync.Mutex
	endpoints []*observerTestSessionNetwork
	attempts  []time.Time
	prepare   func(context.Context, int) error
	retire    func(context.Context) error
}

func (n *observerRecoveryTestNetwork) PrepareSession(
	ctx context.Context,
	_ collator.OverlaySession,
) (SessionNetwork, error) {
	n.mu.Lock()
	n.attempts = append(n.attempts, time.Now())
	attempt := len(n.attempts)
	n.mu.Unlock()
	if n.prepare != nil {
		if err := n.prepare(ctx, attempt); err != nil {
			return nil, err
		}
	}
	endpoint := newObserverTestSessionNetwork()
	n.mu.Lock()
	n.endpoints = append(n.endpoints, endpoint)
	n.mu.Unlock()
	return endpoint, nil
}

func (n *observerRecoveryTestNetwork) RetireSession(ctx context.Context, id [32]byte) error {
	if n.retire != nil {
		if err := n.retire(ctx); err != nil {
			return err
		}
	}
	return n.observerTestNetwork.RetireSession(ctx, id)
}

func newObserverRecoveryFixture(t *testing.T) (*observerFixture, *observerRecoveryTestNetwork) {
	t.Helper()
	fixture := newObserverFixture(t, nil, nil)
	network := &observerRecoveryTestNetwork{observerTestNetwork: fixture.network}
	fixture.observer.network = network
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		if err := fixture.observer.Close(ctx); err != nil {
			t.Error(err)
		}
	})
	return fixture, network
}

func startObserverRecoveryFixture(t *testing.T, fixture *observerFixture) *observerSession {
	t.Helper()
	if err := fixture.observer.PrepareSession(t.Context(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	if err := fixture.observer.ActivateSession(t.Context(), fixture.activation); err != nil {
		t.Fatal(err)
	}
	session, err := fixture.observer.session(fixture.activation.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	return session
}

func failObserverRecoveryFixture(t *testing.T, fixture *observerFixture, network *observerRecoveryTestNetwork) {
	t.Helper()
	network.mu.Lock()
	endpoint := network.endpoints[len(network.endpoints)-1]
	network.mu.Unlock()
	endpoint.runFailures <- errors.New("private overlay receiver failed")
	waitObserverPhase(t, fixture.observer, fixture.activation.SessionID, observerSessionFailed)
}

func waitObserverRecovery(t *testing.T, session *observerSession) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		session.mu.Lock()
		ready := session.phase == observerSessionActive && session.restartCancel == nil
		session.mu.Unlock()
		if ready {
			return
		}
		time.Sleep(time.Millisecond)
	}
	t.Fatal("observer did not recover")
}

func TestConsensusObserverRestartsWithFreshTransportAndLatestState(t *testing.T) {
	t.Parallel()
	fixture, network := newObserverRecoveryFixture(t)
	session := startObserverRecoveryFixture(t, fixture)
	session.mu.Lock()
	original := session.runtime
	storageID := session.config.StorageID
	session.mu.Unlock()
	failObserverRecoveryFixture(t, fixture, network)

	updated := fixture.descriptor
	for seqno := uint32(101); seqno <= 103; seqno++ {
		updated.Update.MasterchainBlock.SeqNo = seqno
		if err := fixture.observer.UpdateSession(t.Context(), updated); err != nil {
			t.Fatal(err)
		}
	}
	if err := fixture.observer.ActivateSession(t.Context(), fixture.activation); !errors.Is(err, collator.ErrAcquisitionNotReady) {
		t.Fatalf("activation during recovery = %v", err)
	}
	artifact := collator.CandidateArtifact{SessionID: fixture.activation.SessionID}
	artifact.WindowID.SessionID = fixture.activation.SessionID
	if err := fixture.observer.BroadcastCandidate(t.Context(), artifact); !errors.Is(err, collator.ErrSessionUnavailable) {
		t.Fatalf("publish during recovery = %v", err)
	}
	waitObserverRecovery(t, session)

	session.mu.Lock()
	current := session.runtime.(*sessionRuntime)
	gotStorageID := session.config.StorageID
	gotDescriptor := session.descriptor
	session.mu.Unlock()
	current.stateMu.RLock()
	gotState := current.state
	current.stateMu.RUnlock()
	if current == original || gotStorageID != storageID || fixture.storage.deletes() != 0 {
		t.Fatal("recovery did not replace runtime while retaining durable identity")
	}
	if !gotDescriptor.Equal(updated) || gotState.MasterchainBlock.SeqNo != 103 {
		t.Fatalf("recovery did not use latest MC state: %d", gotState.MasterchainBlock.SeqNo)
	}
	network.mu.Lock()
	defer network.mu.Unlock()
	if len(network.endpoints) != 2 {
		t.Fatalf("endpoint count = %d, want 2", len(network.endpoints))
	}
	for _, endpoint := range network.endpoints {
		start, run, _ := endpoint.counts()
		if start != 1 || run != 1 {
			t.Fatalf("endpoint reused: start=%d run=%d", start, run)
		}
	}
}

func TestConsensusObserverRecoveryRetriesAreSpaced(t *testing.T) {
	t.Parallel()
	fixture, network := newObserverRecoveryFixture(t)
	network.prepare = func(_ context.Context, attempt int) error {
		if attempt == 2 || attempt == 3 {
			return errors.New("overlay temporarily unavailable")
		}
		return nil
	}
	session := startObserverRecoveryFixture(t, fixture)
	failObserverRecoveryFixture(t, fixture, network)
	waitObserverRecovery(t, session)
	network.mu.Lock()
	defer network.mu.Unlock()
	if len(network.attempts) != 4 {
		t.Fatalf("prepare attempts = %d, want 4", len(network.attempts))
	}
	for i := 2; i < len(network.attempts); i++ {
		if gap := network.attempts[i].Sub(network.attempts[i-1]); gap < sessionRestartDelay {
			t.Fatalf("recovery retried without backoff: %s", gap)
		}
	}
	if fixture.storage.deletes() != 0 {
		t.Fatal("failed restart deleted durable state")
	}
}

func TestConsensusObserverRecoveryAcceptsUpdatesDuringSlowPreparation(t *testing.T) {
	t.Parallel()
	fixture, network := newObserverRecoveryFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	network.prepare = func(ctx context.Context, attempt int) error {
		if attempt == 2 {
			close(entered)
			select {
			case <-release:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		return nil
	}
	session := startObserverRecoveryFixture(t, fixture)
	failObserverRecoveryFixture(t, fixture, network)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery preparation did not start")
	}
	updated := fixture.descriptor
	updated.Update.MasterchainBlock.SeqNo++
	ctx, cancel := context.WithTimeout(t.Context(), 100*time.Millisecond)
	defer cancel()
	if err := fixture.observer.UpdateSession(ctx, updated); err != nil {
		t.Fatalf("update blocked behind recovery: %v", err)
	}
	if err := fixture.observer.ActivateSession(ctx, fixture.activation); !errors.Is(err, collator.ErrAcquisitionNotReady) {
		t.Fatalf("activation blocked behind recovery: %v", err)
	}
	close(release)
	waitObserverRecovery(t, session)
	session.mu.Lock()
	current := session.runtime.(*sessionRuntime)
	session.mu.Unlock()
	current.stateMu.RLock()
	seqno := current.state.MasterchainBlock.SeqNo
	current.stateMu.RUnlock()
	if seqno != updated.Update.MasterchainBlock.SeqNo {
		t.Fatalf("queued update lost: MC seqno=%d", seqno)
	}
	network.mu.Lock()
	defer network.mu.Unlock()
	if len(network.endpoints) != 2 {
		t.Fatal("queued update needlessly restarted recovered runtime")
	}
}

func TestConsensusObserverRetirementCancelsRecovery(t *testing.T) {
	t.Parallel()
	fixture, network := newObserverRecoveryFixture(t)
	startObserverRecoveryFixture(t, fixture)
	failObserverRecoveryFixture(t, fixture, network)
	if err := fixture.observer.RetireSession(t.Context(), fixture.activation.SessionID); err != nil {
		t.Fatal(err)
	}
	time.Sleep(sessionRestartDelay + 20*time.Millisecond)
	network.mu.Lock()
	defer network.mu.Unlock()
	if len(network.attempts) != 1 || fixture.storage.deletes() != 1 {
		t.Fatal("retired observer restarted or did not retire its durable namespace")
	}
}

func TestConsensusObserverCloseCanRetryWhileRecoveryCleanupIsBlocked(t *testing.T) {
	t.Parallel()
	fixture, network := newObserverRecoveryFixture(t)
	entered := make(chan struct{})
	release := make(chan struct{})
	var once sync.Once
	network.retire = func(context.Context) error {
		once.Do(func() { close(entered) })
		<-release
		return nil
	}
	startObserverRecoveryFixture(t, fixture)
	failObserverRecoveryFixture(t, fixture, network)
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("recovery cleanup did not start")
	}
	ctx, cancel := context.WithTimeout(t.Context(), 50*time.Millisecond)
	defer cancel()
	if err := fixture.observer.Close(ctx); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("blocked close = %v, want deadline", err)
	}
	close(release)
	if err := fixture.observer.Close(t.Context()); err != nil {
		t.Fatalf("close retry: %v", err)
	}
	if fixture.storage.deletes() != 0 {
		t.Fatal("close deleted restartable durable state")
	}
}

func TestConsensusObserverRestartsProtocolOneBlockSyncRuntime(t *testing.T) {
	t.Parallel()
	fixture, network := newObserverRecoveryFixture(t)
	fixture.descriptor.Overlay.Session.ProtocolVersion = 1
	fixture.descriptor.Overlay.Role = collator.OverlayRoleObserver
	fixture.descriptor.Overlay.CollatorsByValidator = nil
	fixture.descriptor.Overlay.AllCollators = nil
	fixture.descriptor.Overlay.BroadcastMode = collator.CandidateBroadcastBlockSyncOverlay
	fixture.descriptor.Overlay.ObserversInPrivateOverlay = false
	session := startObserverRecoveryFixture(t, fixture)
	failObserverRecoveryFixture(t, fixture, network)
	waitObserverRecovery(t, session)
	session.mu.Lock()
	_, blockSync := session.runtime.(*blockSyncObserverRuntime)
	session.mu.Unlock()
	if !blockSync {
		t.Fatal("block-sync observer restarted as a consensus runtime")
	}
}

type observerRecoveryCloseTestRuntime struct {
	observerSessionRuntime
	mu      sync.Mutex
	calls   int
	failure error
}

func (r *observerRecoveryCloseTestRuntime) Close() error {
	err := r.observerSessionRuntime.Close()
	r.mu.Lock()
	r.calls++
	first := r.calls == 1
	r.mu.Unlock()
	if first {
		return errors.Join(err, r.failure)
	}
	return err
}

func TestConsensusObserverStartupCleanupFailureHasRecoveryOwner(t *testing.T) {
	t.Parallel()
	fixture, network := newObserverRecoveryFixture(t)
	if err := fixture.observer.PrepareSession(t.Context(), fixture.descriptor); err != nil {
		t.Fatal(err)
	}
	session, err := fixture.observer.session(fixture.activation.SessionID)
	if err != nil {
		t.Fatal(err)
	}
	cleanupErr := errors.New("temporary backend cleanup failure")
	session.mu.Lock()
	session.runtime = &observerRecoveryCloseTestRuntime{
		observerSessionRuntime: session.runtime,
		failure:                cleanupErr,
	}
	session.mu.Unlock()
	network.mu.Lock()
	network.endpoints[0].startErrors = []error{errors.New("initial start failed")}
	network.mu.Unlock()
	if err = fixture.observer.ActivateSession(t.Context(), fixture.activation); !errors.Is(err, cleanupErr) {
		t.Fatalf("initial cleanup error = %v", err)
	}
	if err = fixture.observer.ActivateSession(t.Context(), fixture.activation); !errors.Is(err, collator.ErrAcquisitionNotReady) {
		t.Fatalf("activation after startup cleanup failure = %v", err)
	}
	waitObserverRecovery(t, session)
}
