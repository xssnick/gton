package pebblestore

import (
	"sync/atomic"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
)

type pebbleCompactionTestDB struct {
	waiting  func() (bool, pebble.WaitingCompaction)
	schedule func(pebble.CompactionGrantHandle) bool
}

func (d *pebbleCompactionTestDB) GetAllowedWithoutPermission() int {
	return 0
}

func (d *pebbleCompactionTestDB) GetWaitingCompaction() (bool, pebble.WaitingCompaction) {
	return d.waiting()
}

func (d *pebbleCompactionTestDB) Schedule(handle pebble.CompactionGrantHandle) bool {
	return d.schedule(handle)
}

func TestPebbleCompactionControllerCoalescesGrantRequests(t *testing.T) {
	controller := newPebbleCompactionController(1)
	scheduler := controller.newScheduler()

	releaseFirstCall := make(chan struct{})
	waitingCalls := make(chan int32, 1024)
	var calls atomic.Int32
	var active atomic.Int32
	var maxActive atomic.Int32
	var scheduleCalls atomic.Int32
	db := &pebbleCompactionTestDB{
		waiting: func() (bool, pebble.WaitingCompaction) {
			currentActive := active.Add(1)
			updateAtomicMax(&maxActive, currentActive)
			defer active.Add(-1)

			call := calls.Add(1)
			waitingCalls <- call
			if call == 1 {
				<-releaseFirstCall
			}
			return false, pebble.WaitingCompaction{}
		},
		schedule: func(pebble.CompactionGrantHandle) bool {
			scheduleCalls.Add(1)
			return false
		},
	}
	scheduler.Register(1, db)
	controller.start()

	if call := receiveCompactionTestCall(t, waitingCalls); call != 1 {
		t.Fatalf("first GetWaitingCompaction call = %d, want 1", call)
	}
	for range 1000 {
		scheduler.UpdateGetAllowedWithoutPermission()
	}
	close(releaseFirstCall)

	if call := receiveCompactionTestCall(t, waitingCalls); call != 2 {
		t.Fatalf("second GetWaitingCompaction call = %d, want 2", call)
	}
	scheduler.Unregister()

	if got := calls.Load(); got != 2 {
		t.Fatalf("GetWaitingCompaction calls = %d, want 2", got)
	}
	if got := maxActive.Load(); got != 1 {
		t.Fatalf("concurrent DBForCompaction calls = %d, want 1", got)
	}
	if got := scheduleCalls.Load(); got != 0 {
		t.Fatalf("Schedule calls = %d, want 0", got)
	}
	select {
	case <-controller.done:
	default:
		t.Fatal("controller granter did not stop after the last scheduler unregistered")
	}
}

func TestPebbleCompactionControllerRejectedScheduleDoesNotRequestGrant(t *testing.T) {
	controller := newPebbleCompactionController(1)
	scheduler := controller.newScheduler()

	var waitingCalls atomic.Int32
	var scheduleCalls atomic.Int32
	db := &pebbleCompactionTestDB{
		waiting: func() (bool, pebble.WaitingCompaction) {
			waitingCalls.Add(1)
			return true, pebble.WaitingCompaction{}
		},
		schedule: func(pebble.CompactionGrantHandle) bool {
			scheduleCalls.Add(1)
			return false
		},
	}
	scheduler.Register(1, db)
	<-controller.grantPokes

	controller.mu.Lock()
	controller.paused = false
	controller.mu.Unlock()
	controller.tryGrant()

	if got := waitingCalls.Load(); got != 1 {
		t.Fatalf("GetWaitingCompaction calls = %d, want 1", got)
	}
	if got := scheduleCalls.Load(); got != 1 {
		t.Fatalf("Schedule calls = %d, want 1", got)
	}
	if got := len(controller.grantPokes); got != 0 {
		t.Fatalf("queued grant requests after rejected Schedule = %d, want 0", got)
	}

	scheduler.Unregister()
}

func TestPebbleCompactionSchedulerUnregisterWaitsForGetWaitingCompaction(t *testing.T) {
	controller := newPebbleCompactionController(1)
	scheduler := controller.newScheduler()

	waitingStarted := make(chan struct{})
	releaseWaiting := make(chan struct{})
	var scheduleCalls atomic.Int32
	db := &pebbleCompactionTestDB{
		waiting: func() (bool, pebble.WaitingCompaction) {
			close(waitingStarted)
			<-releaseWaiting
			return false, pebble.WaitingCompaction{}
		},
		schedule: func(pebble.CompactionGrantHandle) bool {
			scheduleCalls.Add(1)
			return false
		},
	}
	scheduler.Register(1, db)
	controller.start()
	<-waitingStarted

	unregistered := make(chan struct{})
	go func() {
		scheduler.Unregister()
		close(unregistered)
	}()
	waitForCompactionSchedulerUnregistered(t, scheduler)

	select {
	case <-unregistered:
		t.Fatal("Unregister returned while GetWaitingCompaction was blocked")
	default:
	}

	close(releaseWaiting)
	select {
	case <-unregistered:
	case <-time.After(time.Second):
		t.Fatal("Unregister did not return after GetWaitingCompaction completed")
	}
	select {
	case <-controller.done:
	default:
		t.Fatal("controller granter did not stop after Unregister")
	}
	if got := scheduleCalls.Load(); got != 0 {
		t.Fatalf("Schedule calls = %d, want 0", got)
	}
}

func TestPebbleCompactionSchedulerUnregisterWaitsForSchedule(t *testing.T) {
	controller := newPebbleCompactionController(1)
	scheduler := controller.newScheduler()

	scheduleStarted := make(chan struct{})
	releaseSchedule := make(chan struct{})
	db := &pebbleCompactionTestDB{
		waiting: func() (bool, pebble.WaitingCompaction) {
			return true, pebble.WaitingCompaction{}
		},
		schedule: func(pebble.CompactionGrantHandle) bool {
			close(scheduleStarted)
			<-releaseSchedule
			return false
		},
	}
	scheduler.Register(1, db)
	controller.start()
	<-scheduleStarted

	unregistered := make(chan struct{})
	go func() {
		scheduler.Unregister()
		close(unregistered)
	}()
	waitForCompactionSchedulerUnregistered(t, scheduler)

	select {
	case <-unregistered:
		t.Fatal("Unregister returned while Schedule was blocked")
	default:
	}

	close(releaseSchedule)
	select {
	case <-unregistered:
	case <-time.After(time.Second):
		t.Fatal("Unregister did not return after Schedule completed")
	}
}

func waitForCompactionSchedulerUnregistered(t *testing.T, scheduler *pebbleCompactionScheduler) {
	t.Helper()

	deadline := time.Now().Add(time.Second)
	for {
		scheduler.controller.mu.Lock()
		unregistered := scheduler.unregistered
		scheduler.controller.mu.Unlock()
		if unregistered {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("scheduler did not enter Unregister")
		}
		time.Sleep(time.Millisecond)
	}
}

func receiveCompactionTestCall(t *testing.T, calls <-chan int32) int32 {
	t.Helper()

	select {
	case call := <-calls:
		return call
	case <-time.After(time.Second):
		t.Fatal("timed out waiting for DBForCompaction call")
		return 0
	}
}

func updateAtomicMax(maximum *atomic.Int32, candidate int32) {
	for {
		current := maximum.Load()
		if candidate <= current || maximum.CompareAndSwap(current, candidate) {
			return
		}
	}
}
