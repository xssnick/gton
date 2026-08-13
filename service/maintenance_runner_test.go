package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestMaintenanceRunnerBindRejectsSecondBind(t *testing.T) {
	status := newTestStatusTracker(nil, nil)
	state := NewStateLifecycle(zerolog.Nop(), nil, status, StateLifecycleOptions{})
	maintenance := NewMaintenanceRunner(zerolog.Nop(), nil, status, MaintenanceRunnerOptions{})
	svc := &SyncCoordinator{state: state, maintenance: maintenance}

	if err := maintenance.Bind(state, svc); err != nil {
		t.Fatalf("bind maintenance runner: %v", err)
	}
	if err := maintenance.Bind(state, svc); !errors.Is(err, errMaintenanceRunnerAlreadyBound) {
		t.Fatalf("second bind error = %v, want already bound", err)
	}
}

func TestMaintenanceRunnerBindRejectsBindAfterStart(t *testing.T) {
	status := newTestStatusTracker(nil, nil)
	state := NewStateLifecycle(zerolog.Nop(), nil, status, StateLifecycleOptions{})
	maintenance := NewMaintenanceRunner(zerolog.Nop(), nil, status, MaintenanceRunnerOptions{})
	svc := &SyncCoordinator{state: state, maintenance: maintenance}

	if err := maintenance.Bind(state, svc); err != nil {
		t.Fatalf("bind maintenance runner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if err := maintenance.Start(ctx); err != nil {
		t.Fatalf("start maintenance runner: %v", err)
	}
	maintenance.Wait()

	if err := maintenance.Bind(state, svc); !errors.Is(err, errMaintenanceRunnerStarted) {
		t.Fatalf("late bind error = %v, want runner started", err)
	}
}

func TestMaintenanceRunnerStartAndWaitAreIdempotent(t *testing.T) {
	status := newTestStatusTracker(nil, nil)
	state := NewStateLifecycle(zerolog.Nop(), nil, status, StateLifecycleOptions{})
	maintenance := NewMaintenanceRunner(zerolog.Nop(), nil, status, MaintenanceRunnerOptions{})
	svc := &SyncCoordinator{state: state, maintenance: maintenance}

	if err := maintenance.Bind(state, svc); err != nil {
		t.Fatalf("bind maintenance runner: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := maintenance.Start(ctx); err != nil {
		t.Fatalf("start maintenance runner: %v", err)
	}
	if err := maintenance.Start(ctx); err != nil {
		t.Fatalf("start maintenance runner again: %v", err)
	}
	cancel()

	done := make(chan struct{})
	go func() {
		maintenance.Wait()
		maintenance.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("maintenance runner did not stop")
	}
}

func TestMaintenanceRunnerOwnsConfiguredShutdownContext(t *testing.T) {
	shutdownCtx, cancel := context.WithCancel(context.Background())
	defer cancel()

	maintenance := NewMaintenanceRunner(
		zerolog.Nop(),
		nil,
		newTestStatusTracker(nil, nil),
		MaintenanceRunnerOptions{ShutdownContext: shutdownCtx},
	)
	if maintenance.shutdownContext != shutdownCtx {
		t.Fatal("maintenance runner did not retain its graceful-shutdown context")
	}
}

func TestStateLifecycleStartDoesNotStartMaintenanceRunner(t *testing.T) {
	status := newTestStatusTracker(nil, nil)
	state := NewStateLifecycle(zerolog.Nop(), nil, status, StateLifecycleOptions{})
	maintenance := NewMaintenanceRunner(zerolog.Nop(), nil, status, MaintenanceRunnerOptions{})
	svc := &SyncCoordinator{state: state, maintenance: maintenance, currentStateWake: make(chan struct{})}

	if err := state.BindTransitions(svc, svc, svc, svc, maintenance); err != nil {
		t.Fatalf("bind state lifecycle: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	if err := state.Start(ctx); err != nil {
		t.Fatalf("start state lifecycle: %v", err)
	}
	cancel()
	state.Wait()

	maintenance.bindMu.Lock()
	started := maintenance.started
	maintenance.bindMu.Unlock()
	if started {
		t.Fatal("state lifecycle started the maintenance runner")
	}
}
