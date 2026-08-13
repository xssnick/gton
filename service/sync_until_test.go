package service

import (
	"testing"

	"github.com/rs/zerolog"
)

// A node that went offline because a subsystem died must not look like a
// finished sync: reading node.IsOffline as "frozen" parked persistent-state GC,
// archive GC and state serialization behind the incident, and logged it as a
// completed sync_until.
func TestSyncUntilFrozenIgnoresUnrelatedOffline(t *testing.T) {
	svc := &SyncCoordinator{
		log:       zerolog.Nop(),
		node:      newServiceTestNode(t),
		syncUntil: 200,
	}

	svc.node.EnterOffline("QUIC gateway stopped unexpectedly")

	if svc.syncUntilFrozen() {
		t.Fatal("an unrelated offline was mistaken for a completed sync_until")
	}
}

func TestSyncUntilFrozenAfterDeliberateStop(t *testing.T) {
	svc := &SyncCoordinator{
		log:         zerolog.Nop(),
		node:        newServiceTestNode(t),
		syncUntil:   200,
		maintenance: &MaintenanceRunner{maintenanceWake: make(chan struct{}, 1)},
	}

	svc.enterSyncUntilOffline(nil, PreparedBlock{})

	if !svc.syncUntilFrozen() {
		t.Fatal("service is not frozen after reaching sync_until")
	}
	if !svc.node.IsOffline() {
		t.Fatal("node did not enter offline mode at sync_until")
	}
}

// Without ton.sync_until configured nothing ever freezes, whatever the node does.
func TestSyncUntilFrozenRequiresConfiguredLimit(t *testing.T) {
	svc := &SyncCoordinator{
		log:  zerolog.Nop(),
		node: newServiceTestNode(t),
	}
	svc.syncUntilReached.Store(true)

	if svc.syncUntilFrozen() {
		t.Fatal("froze without ton.sync_until configured")
	}
}
