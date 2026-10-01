package collator

import (
	"context"
	"sync"
	"testing"
	"time"
)

// A window on its own schedule stays on it. Every underloaded slot here waits
// out its slot for externals and finishes its block a little after the slot
// boundary, which is when it hands its successor over. The successor's floor is
// taken from where the predecessor's wait ended, so it is the successor's own
// slot boundary and changes nothing. Taken from that handoff instead, each slot
// would start its wait later than the last by the predecessor's tail, until the
// soft deadline turned one of them empty — which SoftTimeout answers here the
// way LocalAcquisition does.
func TestPipelinedFloorKeepsAWindowOnItsSchedule(t *testing.T) {
	const slots = 10
	const tail = 15 * time.Millisecond

	var mu sync.Mutex
	pipelined := map[uint32]BuildRequest{}
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		if request.PreviousPending != nil {
			mu.Lock()
			if _, seen := pipelined[request.Slot]; !seen {
				pipelined[request.Slot] = request
			}
			mu.Unlock()
		}

		// An underloaded block: nothing fills it, so it waits for externals
		// until its slot boundary, then finishes the block.
		wait := time.NewTimer(time.Until(request.ExternalWaitUntil))
		defer wait.Stop()
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wait.C:
		}
		time.Sleep(tail)

		return runtimeBuiltCandidate(request), nil
	}
	pipeline.soft = func(_ context.Context, request SoftTimeoutRequest) (SoftTimeoutDecision, error) {
		if !request.Current.Parent.Exists {
			return SoftTimeoutDecision{Action: SoftTimeoutWait}, nil
		}

		return SoftTimeoutDecision{
			Action: SoftTimeoutEmitEmpty,
			Block: runtimeTestBlockID(
				request.Current.Session.Shard.Workchain,
				request.Current.Session.Shard.Shard,
				request.Current.Slot,
			),
		}, nil
	}

	emitted := make(chan CandidateArtifact, slots)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	// Opened ahead of its schedule, so the first slot's build starts before its
	// boundary too and nothing but the floor could move the window off it.
	session, update := fixture.session(0x60, slots, 0, time.Now().Add(300*time.Millisecond))
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	for slot := uint32(0); slot < slots; slot++ {
		if artifact := runtimeAwaitArtifact(t, emitted); artifact.Candidate.Empty {
			t.Fatalf("slot %d of a window on its schedule was emitted empty", artifact.Candidate.ID.Slot)
		}
	}

	mu.Lock()
	defer mu.Unlock()
	for slot := uint32(1); slot < slots; slot++ {
		request, found := pipelined[slot]
		if !found {
			t.Fatalf("slot %d was never pipelined; this test says nothing about its floor", slot)
		}
		boundary := update.CurrentWindowStartAt.Add(time.Duration(slot) * update.TargetRate)
		if !request.ExternalWaitUntil.Equal(boundary) {
			t.Fatalf("pipelined slot %d waits for externals until %v, %v past its slot boundary",
				slot, request.ExternalWaitUntil, request.ExternalWaitUntil.Sub(boundary))
		}
	}
}

// In a window whose schedule has passed, the floor is what spaces the slots,
// and a pipelined slot takes it from its predecessor's wait for externals: one
// rate past where that wait ended, not past the slot before it. Slot 0 runs
// with no floor and no wait, so its gathering ends where its build starts;
// slot 1 gathers one rate past that, and slot 2 one rate past slot 1.
//
// The floor is also taken from the offer alone. Slot 1 hands over only once
// slot 0's emission has begun, and that emission then outlasts slot 1's whole
// wait, so slot 2's floor is computed on slot 1's build goroutine while the
// producer goroutine is still inside persistAndEmit for slot 0 — and writes its
// emission bookkeeping when it leaves. The emission is held on a timer rather
// than on slot 2's start: a channel between the two would order that write
// after the floor and hide a read of it from -race.
//
// The build slot 2's floor starts is abandoned: slot 1's build still holds the
// successor slot, which the producer collects only once slot 0 is out, and the
// loop builds slot 2 itself. What is checked is the floor acceptHandoff
// computed. The fixture's soft-timeout answer stays Wait: the loop's soft
// deadlines are the stale schedule's, and an empty slot there is not what this
// test is about.
func TestPipelinedFloorIsTakenFromThePredecessorsExternalWait(t *testing.T) {
	var mu sync.Mutex
	pipelined := map[uint32]BuildRequest{}
	pipelinedAt := map[uint32]time.Time{}
	var slotZeroBuilt time.Time
	slotZeroEmitting := make(chan struct{})

	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		if request.PreviousPending == nil {
			if request.Slot == 0 {
				mu.Lock()
				slotZeroBuilt = time.Now()
				mu.Unlock()
			}

			return runtimeBuiltCandidate(request), nil
		}

		mu.Lock()
		if _, seen := pipelined[request.Slot]; !seen {
			pipelined[request.Slot] = request
			pipelinedAt[request.Slot] = time.Now()
		}
		mu.Unlock()
		if request.Slot == 1 {
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-slotZeroEmitting:
			}
			wait := time.NewTimer(time.Until(request.ExternalWaitUntil))
			defer wait.Stop()
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-wait.C:
			}
		}

		return runtimeBuiltCandidate(request), nil
	}

	// Written by the producer goroutine alone, and read only after later slots
	// were emitted.
	var slotZeroReturned time.Time
	emitted := make(chan CandidateArtifact, 4)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		if artifact.Candidate.ID.Slot == 0 {
			close(slotZeroEmitting)
			time.Sleep(400 * time.Millisecond)
			slotZeroReturned = time.Now()
		}
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	started := time.Now()
	session, update := fixture.session(0x5f, 4, 0, started.Add(-2*time.Second))
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	for slot := uint32(0); slot < 4; slot++ {
		runtimeAwaitArtifact(t, emitted)
	}

	mu.Lock()
	defer mu.Unlock()
	slotOne, foundOne := pipelined[1]
	slotTwo, foundTwo := pipelined[2]
	if !foundOne || !foundTwo {
		t.Fatal("slots 1 and 2 were not both pipelined; this test says nothing about their floors")
	}
	if !pipelinedAt[2].Before(slotZeroReturned) {
		t.Fatal("slot 2's floor was taken after slot 0 was emitted; this test says nothing about the overlap")
	}
	earliest, latest := started.Add(update.TargetRate), slotZeroBuilt.Add(update.TargetRate)
	if slotOne.ExternalWaitUntil.Before(earliest) || slotOne.ExternalWaitUntil.After(latest) {
		t.Fatalf("pipelined slot 1 waits for externals until %v, want one rate past slot 0's build, "+
			"between %v and %v", slotOne.ExternalWaitUntil, earliest, latest)
	}
	if want := slotOne.ExternalWaitUntil.Add(update.TargetRate); !slotTwo.ExternalWaitUntil.Equal(want) {
		t.Fatalf("pipelined slot 2 waits for externals until %v, want one rate past slot 1's wait, %v",
			slotTwo.ExternalWaitUntil, want)
	}
}
