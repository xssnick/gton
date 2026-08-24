package collator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/groups"
)

// pipelineHookProbe records, per slot, the order in which the runtime committed
// candidate states and entered builds. Both are recorded under one lock so the
// happens-before between a commit and a later build is a fact of the record and
// not of the reader.
type pipelineHookProbe struct {
	mu        sync.Mutex
	committed []uint32
	built     []uint32
	emitted   []uint32
	leads     int
}

func (p *pipelineHookProbe) noteCommit(slot uint32) {
	p.mu.Lock()
	p.committed = append(p.committed, slot)
	p.mu.Unlock()
}

func (p *pipelineHookProbe) noteBuild(slot uint32) {
	p.mu.Lock()
	p.built = append(p.built, slot)
	p.mu.Unlock()
}

func (p *pipelineHookProbe) noteEmit(slot uint32) {
	p.mu.Lock()
	p.emitted = append(p.emitted, slot)
	p.mu.Unlock()
}

func (p *pipelineHookProbe) committedBefore(slot uint32) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	for _, done := range p.committed {
		if done == slot {
			return true
		}
	}

	return false
}

func (p *pipelineHookProbe) snapshot() (committed, built, emitted []uint32) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]uint32(nil), p.committed...),
		append([]uint32(nil), p.built...),
		append([]uint32(nil), p.emitted...)
}

// The hook exists to move one boundary: the next slot's build must begin while
// this slot's emission is still running. The emission below blocks until the
// successor's build has been entered, so a runtime that starts builds only after
// Emit returns deadlocks here rather than failing an assertion — which is the
// honest shape for this property, because there is no intermediate state in
// which it is half true.
func TestRuntimeStartsTheNextBuildBeforeThisSlotIsEmitted(t *testing.T) {
	successorBuilding := make(chan struct{})
	var once sync.Once
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		if request.Slot == 1 {
			once.Do(func() { close(successorBuilding) })
		}

		return runtimeBuiltCandidate(request), nil
	}

	emitted := make(chan CandidateArtifact, 4)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		if artifact.Candidate.ID.Slot == 0 {
			select {
			case <-successorBuilding:
			case <-time.After(4 * time.Second):
				return errors.New("the successor build was not started before this slot was emitted")
			}
		}
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(71, 2, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}

	first := runtimeAwaitArtifact(t, emitted)
	second := runtimeAwaitArtifact(t, emitted)
	if first.Candidate.ID.Slot != 0 || second.Candidate.ID.Slot != 1 {
		t.Fatalf("emitted slots = %d, %d, want 0, 1", first.Candidate.ID.Slot, second.Candidate.ID.Slot)
	}
}

// The commit-boundary version of this file asserted that a successor started
// only after its predecessor's state was installed. That property is gone by
// construction: the handoff now happens inside the predecessor's own build, a
// long way before any commit, and what replaces it is the adoption check — the
// producer takes a parked successor only if the predecessor it went on to commit
// is the one the offer was built on. That is
// TestRetriedPredecessorIsNeverAdoptedBySuccessor. The external half of the old
// test's duty — a successor must not re-execute what its predecessor consumed —
// belongs to the external-stream fence and is not yet implemented.

// Constraint 1, stated as a property of the record rather than of the code that
// happens to implement it today: a candidate is never committed before its
// predecessor has been emitted. This is what stops any future attempt to move
// the commit into the build goroutine, where it would be ordered by whichever
// build finished first.
func TestRuntimeCommitOrderStaysStrictAcrossPipelinedBuilds(t *testing.T) {
	probe := &pipelineHookProbe{}
	var violation error
	var violationMu sync.Mutex
	pipeline := &runtimeTestPipeline{}
	pipeline.commit = func(_ context.Context, commit CandidateCommit) error {
		slot := commit.Artifact.Candidate.ID.Slot
		committed, _, emittedSoFar := probe.snapshot()
		violationMu.Lock()
		if uint32(len(committed)) != slot {
			violation = errors.New("a slot was committed out of order")
		}
		if slot > 0 && (len(emittedSoFar) == 0 || emittedSoFar[len(emittedSoFar)-1] != slot-1) {
			violation = errors.New("a slot was committed before its predecessor was emitted")
		}
		violationMu.Unlock()
		probe.noteCommit(slot)

		return nil
	}

	emitted := make(chan CandidateArtifact, 8)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		probe.noteEmit(artifact.Candidate.ID.Slot)
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(73, 4, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	for slot := uint32(0); slot < 4; slot++ {
		runtimeAwaitArtifact(t, emitted)
	}

	violationMu.Lock()
	defer violationMu.Unlock()
	if violation != nil {
		committed, _, emittedSlots := probe.snapshot()
		t.Fatalf("%v: commits %v, emissions %v", violation, committed, emittedSlots)
	}
}

// Constraint 2. A successor started by the hook is owned by the same local that
// the producer's defer stops, so a window that ends between the hook and the
// slot the build was for must take the build down with it. The build below
// parks until its context is cancelled, so a future the defer cannot see hangs
// the window instead of leaking quietly.
func TestRuntimePipelinedBuildIsCancelledWhenEmissionFails(t *testing.T) {
	cancelled := make(chan struct{})
	var once sync.Once
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		if request.Slot == 0 {
			return runtimeBuiltCandidate(request), nil
		}
		<-ctx.Done()
		once.Do(func() { close(cancelled) })

		return nil, ctx.Err()
	}

	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		if artifact.Candidate.ID.Slot == 0 {
			return errors.New("emission refused")
		}

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(74, 4, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}

	select {
	case <-cancelled:
	case <-time.After(5 * time.Second):
		t.Fatal("the pipelined build outlived the window whose emission failed")
	}
}

// Masterchain is excluded deliberately, and the exclusion is invisible in the
// produced block: a masterchain build started early takes its external snapshot
// early and measures its phase budgets from its own start, so it would admit a
// different set of messages rather than the same set sooner. Nothing rejects
// such a block; it is simply not the block the reference would have built.
func TestRuntimeDoesNotPipelineMasterchainSlots(t *testing.T) {
	probe := &pipelineHookProbe{}
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		probe.noteBuild(request.Slot)

		return runtimeBuiltCandidate(request), nil
	}

	emitted := make(chan CandidateArtifact, 8)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		probe.noteEmit(artifact.Candidate.ID.Slot)
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	observer := &runtimeScheduleObserver{events: make(map[ScheduleEvent]int)}
	fixture.service.opts.Observer = observer

	session, update := fixture.session(75, 3, 0, time.Now())
	session.Shard = groups.ShardID{Workchain: -1, Shard: -1 << 63}
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	for slot := uint32(0); slot < 3; slot++ {
		runtimeAwaitArtifact(t, emitted)
	}

	if got := observer.count(ScheduleEventBuildLead); got != 0 {
		t.Fatalf("masterchain reported %d pipelined build leads, want none", got)
	}
}

// The hook makes the only empty-policy decision the next slot gets, because a
// pipelined future skips the loop's scheduled check exactly as a carry-over
// future does. If the hook stopped consulting the policy, a slot the session is
// required to leave empty would be collated instead.
func TestRuntimeDoesNotPipelineASlotThePolicyWantsEmpty(t *testing.T) {
	probe := &pipelineHookProbe{}
	pipeline := &runtimeTestPipeline{}
	// Both halves: the loop asks state for a slot it is about to schedule, and
	// the handoff carries the same verdict in the offer for the slot a block has
	// just made possible. A gate that consulted only one of them would let the
	// other through.
	pipeline.state = func(_ context.Context, request BuildRequest) (CandidateState, error) {
		block := cloneBlockID(request.Previous.Candidate.Block)

		return CandidateState{Block: block, NextSeqno: block.SeqNo + 1, BeforeSplit: true}, nil
	}
	pipeline.successorPolicy = func(_ BuildRequest, candidate *Candidate) CandidateState {
		return CandidateState{
			Block:       cloneBlockID(candidate.ID),
			NextSeqno:   candidate.ID.SeqNo + 1,
			BeforeSplit: true,
		}
	}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		probe.noteBuild(request.Slot)

		return runtimeBuiltCandidate(request), nil
	}

	emitted := make(chan CandidateArtifact, 8)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	observer := &runtimeScheduleObserver{events: make(map[ScheduleEvent]int)}
	fixture.service.opts.Observer = observer

	session, update := fixture.session(76, 3, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	for slot := uint32(0); slot < 3; slot++ {
		runtimeAwaitArtifact(t, emitted)
	}

	if got := observer.count(ScheduleEventBuildLead); got != 0 {
		t.Fatalf("a slot the policy wants empty was pipelined %d times", got)
	}
	_, built, _ := probe.snapshot()
	for _, slot := range built {
		if slot > 0 {
			t.Fatalf("built slot %d although the policy wants it empty; builds = %v", slot, built)
		}
	}
}

// No build may begin more than one target rate before its slot. A build that
// opens further ahead than that holds an external stream across a slot that is
// not its own and measures a pace whose head start is larger than the slot it
// describes.
//
// Measured, not assumed: the property holds here without the explicit cap in
// maxPipelinedBuildLead, because the emission of each slot waits for that slot's
// broadcast instant and the next commit cannot precede it, so every slot
// re-anchors the lead instead of accumulating it. The cap is kept as a backstop
// for a loop shape that no longer re-anchors — and this test deliberately pins
// the property rather than the backstop, so it stays meaningful either way.
//
// The window is long and the build instant, which is the fastest a lead could
// possibly accumulate.
func TestRuntimePipelinedBuildLeadIsCapped(t *testing.T) {
	var mu sync.Mutex
	var leads []time.Duration
	observer := &pipelineLeadObserver{
		runtimeScheduleObserver: runtimeScheduleObserver{events: make(map[ScheduleEvent]int)},
		record: func(event ScheduleEvent, lead time.Duration) {
			if event != ScheduleEventBuildLead {
				return
			}
			mu.Lock()
			leads = append(leads, lead)
			mu.Unlock()
		},
	}

	emitted := make(chan CandidateArtifact, 16)
	fixture := newRuntimeFixture(t, 1, 1, nil, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)
	fixture.service.opts.Observer = observer

	session, update := fixture.session(77, 8, 0, time.Now())
	fixture.prepare(t, session, update)
	record := SessionRecord{Session: session, Update: update}
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	for slot := uint32(0); slot < 8; slot++ {
		runtimeAwaitArtifact(t, emitted)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(leads) == 0 {
		t.Fatal("no slot was pipelined; this test cannot say anything about the cap")
	}
	cap := maxPipelinedBuildLead(record)
	var ahead int
	for _, lead := range leads {
		if lead > cap {
			t.Fatalf("a build began %v ahead of its schedule, one target rate is %v; leads = %v",
				lead, cap, leads)
		}
		if lead > 0 {
			ahead++
		}
	}
	if ahead == 0 {
		t.Fatal("every pipelined build was already late; the cap was never exercised")
	}
}

type pipelineLeadObserver struct {
	runtimeScheduleObserver
	record func(ScheduleEvent, time.Duration)
}

func (o *pipelineLeadObserver) ObserveScheduleLateness(
	chain MetricChain,
	event ScheduleEvent,
	lateness time.Duration,
) {
	o.runtimeScheduleObserver.ObserveScheduleLateness(chain, event, lateness)
	if o.record != nil {
		o.record(event, lateness)
	}
}

// A slot the producer decides to emit empty keeps the build the previous slot
// started for it: that build extends the same committed block, and the empty
// candidate does not change what it will read. Cancelling it would throw away a
// slot's worth of collation for nothing, which is precisely what a stale-slot
// fence on the future would have done — and it would have done it on the common
// path, because emitting empty on a soft timeout is the default decision.
func TestRuntimePipelinedFutureSurvivesAnEmittedEmptySlot(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	pipeline.soft = func(_ context.Context, request SoftTimeoutRequest) (SoftTimeoutDecision, error) {
		if request.Current.Slot == 1 {
			return SoftTimeoutDecision{
				Action: SoftTimeoutEmitEmpty,
				Block: runtimeTestBlockID(
					request.Current.Session.Shard.Workchain,
					request.Current.Session.Shard.Shard,
					78,
				),
			}, nil
		}

		return SoftTimeoutDecision{Action: SoftTimeoutWait}, nil
	}
	built := make(chan uint32, 32)
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		built <- request.Slot
		if request.Slot == 1 {
			// Slow enough that slot 1 hits its soft timeout and is emitted empty
			// while this build is still running for it.
			select {
			case <-time.After(300 * time.Millisecond):
			case <-ctx.Done():
				return nil, ctx.Err()
			}
		}

		return runtimeBuiltCandidate(request), nil
	}

	emitted := make(chan CandidateArtifact, 8)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(78, 3, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	empties := 0
	for slot := uint32(0); slot < 3; slot++ {
		artifact := runtimeAwaitArtifact(t, emitted)
		if artifact.Candidate.Empty {
			empties++
		}
	}
	if empties != 1 {
		t.Fatalf("the window emitted %d empty slots, want exactly 1; without one this test says nothing "+
			"about a future surviving an empty slot", empties)
	}

	// Drained rather than closed: a successor handed over by the last slot of the
	// window is still running when the assertions above finish, and it would send
	// into a closed channel.
	var slots []uint32
	for draining := true; draining; {
		select {
		case slot := <-built:
			slots = append(slots, slot)
		default:
			draining = false
		}
	}
	// One build per slot and not one more. The slot emitted empty had a build
	// running for it; discarding that build would show up here as a fourth,
	// because the loop would have had to start one for the slot after it.
	starts := map[uint32]int{}
	for _, slot := range slots {
		starts[slot]++
	}
	for slot, count := range starts {
		if count > 1 {
			t.Errorf("slot %d was built %d times; the surviving future was discarded and rebuilt",
				slot, count)
		}
	}
	if cancelled := len(slots) - len(starts); cancelled != 0 {
		t.Errorf("builds = %v, distinct slots = %d", slots, len(starts))
	}
}

// The one property that makes handing a predecessor over before it is committed
// safe at all: the producer adopts a parked successor only if the predecessor it
// went on to commit is the block the offer was built on.
//
// The case is not hypothetical and it is not a stale read. A block that
// overflows the consensus size limit is rebuilt under escalating concessions,
// and the rebuild has a different root — so an offer made by the attempt that
// overflowed names a block that will never exist. A successor adopted against it
// would be collating on a state no validator can reach, and the failure is
// silent: the block it produces is well formed, signs cleanly, and forks this
// node from the network.
func TestRetriedPredecessorIsNeverAdoptedBySuccessor(t *testing.T) {
	pipeline := &runtimeTestPipeline{}
	// Slot 0 hands over an offer naming a root no candidate carries, which is
	// exactly the shape a rebuilt predecessor leaves behind.
	pipeline.successorRoot = func(request BuildRequest, candidate *Candidate) []byte {
		if request.Slot != 0 {
			return append([]byte(nil), candidate.ID.RootHash...)
		}
		stale := make([]byte, 32)
		stale[0] = 0xAA

		return stale
	}
	built := make(chan BuildRequest, 16)
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		built <- request

		return runtimeBuiltCandidate(request), nil
	}

	emitted := make(chan CandidateArtifact, 8)
	fixture := newRuntimeFixture(t, 1, 1, pipeline, nil, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	session, update := fixture.session(79, 3, 0, time.Now())
	fixture.prepare(t, session, update)
	if err := fixture.service.CommitDelegation(context.Background(), fixture.request(t, session, 0)); err != nil {
		t.Fatal(err)
	}
	for slot := uint32(0); slot < 3; slot++ {
		artifact := runtimeAwaitArtifact(t, emitted)
		if artifact.Candidate.ID.Slot != slot {
			t.Fatalf("emitted slot %d, want %d", artifact.Candidate.ID.Slot, slot)
		}
	}

	var requests []BuildRequest
	for draining := true; draining; {
		select {
		case request := <-built:
			requests = append(requests, request)
		default:
			draining = false
		}
	}

	// Slot 1 must have been built by the loop against the committed artifact, not
	// adopted from the stale offer.
	var slotOne []BuildRequest
	for _, request := range requests {
		if request.Slot == 1 {
			slotOne = append(slotOne, request)
		}
	}
	if len(slotOne) == 0 {
		t.Fatal("slot 1 was never built")
	}
	adopted := 0
	for _, request := range slotOne {
		if request.PreviousPending != nil {
			adopted++
		}
	}
	if adopted == len(slotOne) {
		t.Fatal("every build of slot 1 came from the stale offer; the producer adopted a successor " +
			"built on a predecessor root it never committed")
	}
	committed := slotOne[len(slotOne)-1]
	if committed.Previous == nil {
		t.Fatal("the slot that produced the emitted block was not chained to the committed predecessor")
	}
}

// A pipelined successor may not seed a source queue: the walk is thousands of
// entries deep and it holds the acquisition session's mutex, which is the mutex
// its own predecessor is about to want to commit the block it is speculating
// on. It gives the slot back instead, with ErrAcquisitionNotReady.
//
// The producer must not adopt that refusal as the slot's own failure. Doing so
// sends the window back through produceWindowWithRetry, which re-restores and
// re-emits every slot already on the wire — a far worse outcome than losing a
// head start.
func TestProducerDeclinesASuccessorThatGaveTheSlotBack(t *testing.T) {
	slot := &successorSlot{}
	root := [32]byte{0xd1}

	for _, test := range []struct {
		name   string
		result candidateBuildResult
		adopt  bool
	}{
		{
			name:   "an acquisition that refused to seed",
			result: candidateBuildResult{err: ErrAcquisitionNotReady},
		},
		{
			name:   "a build that succeeded",
			result: candidateBuildResult{elapsed: time.Millisecond},
			adopt:  true,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			done := make(chan struct{})
			close(done)
			future := &candidateBuildFuture{
				result:       make(chan candidateBuildResult, 1),
				done:         done,
				cancel:       func() {},
				hardDeadline: time.Now().Add(time.Minute),
			}
			future.result <- test.result
			if !slot.install(future, root) {
				t.Fatal("the successor was not parked")
			}
			parked, gotRoot := slot.take()
			if parked == nil || gotRoot != root {
				t.Fatal("the parked successor was not handed back")
			}
			if got := parked.viable(time.Now()); got != test.adopt {
				t.Fatalf("viable = %v, want %v", got, test.adopt)
			}
			// Whatever the verdict, the result is still there for whoever owns
			// the future next.
			select {
			case result := <-parked.result:
				if !errors.Is(result.err, test.result.err) {
					t.Fatalf("result = %v, want %v", result.err, test.result.err)
				}
			default:
				t.Fatal("the viability check consumed the build's result")
			}
		})
	}
}
