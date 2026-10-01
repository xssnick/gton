package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func newTestStateLifecycle(store testStorage, opts StateLifecycleOptions) *StateLifecycle {
	status := newTestStatusTracker(store, nil)
	state := NewStateLifecycle(
		zerolog.Nop(),
		store,
		status,
		opts,
	)
	maintenance := NewMaintenanceRunner(zerolog.Nop(), store, status, MaintenanceRunnerOptions{})
	svc := &SyncCoordinator{
		log:              zerolog.Nop(),
		storage:          store,
		status:           status,
		state:            state,
		maintenance:      maintenance,
		currentStateWake: make(chan struct{}),
	}
	if err := state.BindTransitions(svc, svc, svc, svc, maintenance); err != nil {
		panic(err)
	}
	if err := maintenance.Bind(state, svc); err != nil {
		panic(err)
	}

	return state
}

type testComponentStore interface {
	SyncStore
	stateLifecycleStore
	MaintenanceStore
}

type emptyTestComponentStore struct {
	testStorage
}

func bindTestStateLifecycle(t testing.TB, svc *SyncCoordinator, opts StateLifecycleOptions) *StateLifecycle {
	state, _ := bindTestStateAndMaintenance(t, svc, opts, MaintenanceRunnerOptions{})
	return state
}

func bindTestMaintenanceRunner(t testing.TB, svc *SyncCoordinator, stateOpts StateLifecycleOptions, maintenanceOpts MaintenanceRunnerOptions) *MaintenanceRunner {
	_, maintenance := bindTestStateAndMaintenance(t, svc, stateOpts, maintenanceOpts)
	return maintenance
}

func bindTestStateAndMaintenance(t testing.TB, svc *SyncCoordinator, stateOpts StateLifecycleOptions, maintenanceOpts MaintenanceRunnerOptions) (*StateLifecycle, *MaintenanceRunner) {
	t.Helper()

	if svc.status == nil {
		svc.status = newTestStatusTracker(svc.storage, nil)
	}
	if svc.currentStateWake == nil {
		svc.currentStateWake = make(chan struct{})
	}
	var store testComponentStore = emptyTestComponentStore{}
	if svc.storage != nil {
		var ok bool
		store, ok = svc.storage.(testComponentStore)
		if !ok {
			t.Fatal("test coordinator storage does not satisfy state and maintenance contracts")
		}
	}

	lifecycle := NewStateLifecycle(svc.log, store, svc.status, stateOpts)
	maintenance := NewMaintenanceRunner(svc.log, store, svc.status, maintenanceOpts)
	if err := lifecycle.BindTransitions(svc, svc, svc, svc, maintenance); err != nil {
		t.Fatalf("bind state lifecycle transitions: %v", err)
	}
	if err := maintenance.Bind(lifecycle, svc); err != nil {
		t.Fatalf("bind maintenance runner: %v", err)
	}
	svc.state = lifecycle
	svc.maintenance = maintenance

	return lifecycle, maintenance
}

func TestStateLifecycleBindTransitionsRejectsSecondBind(t *testing.T) {
	svc := &SyncCoordinator{log: zerolog.Nop(), currentStateWake: make(chan struct{})}
	state := NewStateLifecycle(zerolog.Nop(), nil, newTestStatusTracker(nil, nil), StateLifecycleOptions{})
	maintenance := NewMaintenanceRunner(zerolog.Nop(), nil, state.status, MaintenanceRunnerOptions{})
	svc.state = state
	svc.maintenance = maintenance

	if err := state.BindTransitions(svc, svc, svc, svc, maintenance); err != nil {
		t.Fatalf("bind state lifecycle transitions: %v", err)
	}
	if err := state.BindTransitions(svc, svc, svc, svc, maintenance); !errors.Is(err, errStateLifecycleAlreadyBound) {
		t.Fatalf("second bind error = %v, want already bound", err)
	}
}

func TestStateLifecycleBindTransitionsRejectsBindAfterStart(t *testing.T) {
	svc := &SyncCoordinator{log: zerolog.Nop(), currentStateWake: make(chan struct{})}
	state := NewStateLifecycle(zerolog.Nop(), nil, newTestStatusTracker(nil, nil), StateLifecycleOptions{})
	maintenance := NewMaintenanceRunner(zerolog.Nop(), nil, state.status, MaintenanceRunnerOptions{})
	svc.state = state
	svc.maintenance = maintenance

	if err := state.BindTransitions(svc, svc, svc, svc, maintenance); err != nil {
		t.Fatalf("bind state lifecycle transitions: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := state.Start(ctx); err != nil {
		t.Fatalf("start state lifecycle: %v", err)
	}
	state.Wait()

	if err := state.BindTransitions(svc, svc, svc, svc, maintenance); !errors.Is(err, errStateLifecycleStarted) {
		t.Fatalf("late bind error = %v, want lifecycle started", err)
	}
}

func TestStateLifecycleStartAndWaitAreIdempotent(t *testing.T) {
	svc := &SyncCoordinator{log: zerolog.Nop(), currentStateWake: make(chan struct{})}
	state := NewStateLifecycle(zerolog.Nop(), nil, newTestStatusTracker(nil, nil), StateLifecycleOptions{})
	maintenance := NewMaintenanceRunner(zerolog.Nop(), nil, state.status, MaintenanceRunnerOptions{})
	svc.state = state
	svc.maintenance = maintenance

	if err := state.BindTransitions(svc, svc, svc, svc, maintenance); err != nil {
		t.Fatalf("bind state lifecycle transitions: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := state.Start(ctx); err != nil {
		t.Fatalf("start state lifecycle: %v", err)
	}
	if err := state.Start(ctx); err != nil {
		t.Fatalf("start state lifecycle again: %v", err)
	}
	cancel()

	done := make(chan struct{})
	go func() {
		state.Wait()
		state.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("state lifecycle workers did not stop")
	}
}
