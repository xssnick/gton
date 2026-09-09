package collator

import (
	"context"
	"errors"
	"testing"
	"time"
)

func TestControllerAdvancesMasterchainWhileObserverRestarts(t *testing.T) {
	controller, _, observer := newControllerTestFixture(t)
	snapshot := controllerTestSnapshot(observer.id)
	controller.tracker = controllerTestTracker{snapshot: snapshot}
	if err := controller.ApplyMasterchainState(t.Context(), AppliedMasterchainState{
		Block: snapshot.MasterchainBlock, AsOf: time.Now(),
	}); err != nil {
		t.Fatal(err)
	}
	observer.mu.Lock()
	observer.activationHook = func(context.Context) error {
		return errors.Join(ErrAcquisitionNotReady, errors.New("observer runtime restarting"))
	}
	observer.mu.Unlock()
	for range 3 {
		snapshot.MasterchainBlock.SeqNo++
		if err := controller.ApplyMasterchainState(t.Context(), AppliedMasterchainState{
			Block: snapshot.MasterchainBlock, AsOf: time.Now(),
		}); err != nil {
			t.Fatalf("observer restart blocked MC apply: %v", err)
		}
	}
	sessionID := snapshot.Active[0].ID
	controller.mu.RLock()
	managed := controller.managed[sessionID]
	controller.mu.RUnlock()
	managed.mu.Lock()
	seqno := managed.projection.update.MasterchainBlock.SeqNo
	reconciled := managed.reconciled
	managed.mu.Unlock()
	if seqno != snapshot.MasterchainBlock.SeqNo || reconciled {
		t.Fatalf("recovering projection seqno=%d reconciled=%t", seqno, reconciled)
	}
	observer.mu.Lock()
	observer.activationHook = nil
	observer.mu.Unlock()
	if err := controller.ApplyMasterchainState(t.Context(), AppliedMasterchainState{
		Block: snapshot.MasterchainBlock, AsOf: time.Now(),
	}); err != nil {
		t.Fatalf("apply after recovery: %v", err)
	}
	managed.mu.Lock()
	reconciled = managed.reconciled
	managed.mu.Unlock()
	observer.mu.Lock()
	prepared, retired := observer.prepared, observer.retired
	observer.mu.Unlock()
	if !reconciled || prepared != 1 || retired != 0 {
		t.Fatalf("recovered=%t prepare=%d retire=%d", reconciled, prepared, retired)
	}
}
