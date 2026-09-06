package collator

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

// speculationProbe records every build the runtime entered, with the slot it was
// for and whether it carried a speculative predecessor. Counting builds is the
// whole point: a bet that is adopted means ONE build for the first slot, and a
// bet that is dropped means two — one thrown away and one on the real base.
type speculationProbe struct {
	mu          sync.Mutex
	slots       []uint32
	speculative []bool
	entered     chan uint32
}

func newSpeculationProbe() *speculationProbe {
	return &speculationProbe{entered: make(chan uint32, 8)}
}

func (p *speculationProbe) note(request BuildRequest) {
	p.mu.Lock()
	p.slots = append(p.slots, request.Slot)
	p.speculative = append(p.speculative, request.speculative != nil)
	p.mu.Unlock()
	select {
	case p.entered <- request.Slot:
	default:
	}
}

func (p *speculationProbe) snapshot() ([]uint32, []bool) {
	p.mu.Lock()
	defer p.mu.Unlock()

	return append([]uint32(nil), p.slots...), append([]bool(nil), p.speculative...)
}

// speculationFixture is one session in the window before ours, with a validated
// candidate ready to bet on. windowStart is the window the bet is for.
type speculationFixture struct {
	*runtimeFixture
	session    Session
	update     SessionUpdate
	windowSize uint32
}

func newSpeculationFixture(
	t *testing.T,
	pipeline *runtimeTestPipeline,
	emit EmitCandidate,
) *speculationFixture {
	t.Helper()
	const windowSize = 2
	fixture := newRuntimeSelfFixture(t, pipeline, nil, nil, emit)
	// One validator leads every window, so the window after the current one is
	// this node's — which is the condition the bet is placed under.
	session, update := fixture.session(83, windowSize, 0, time.Now().Add(-time.Second))
	fixture.prepare(t, session, update)

	return &speculationFixture{
		runtimeFixture: fixture,
		session:        session,
		update:         update,
		windowSize:     windowSize,
	}
}

// rawCandidate binds a base without touching the empty-block watermark, which
// leaves the session in the state the fixture's canned block really implies: a
// shard running far ahead of its masterchain inclusion, where the policy demands
// an empty first block.
func (f *speculationFixture) rawCandidate(t *testing.T, slot uint32, seed byte) (simplex.CandidateID, *SelectedBaseState) {
	t.Helper()
	id := simplex.CandidateID{Slot: slot}
	id.Hash[0] = seed

	return id, runtimeSelectedBase(t, f.session, id)
}

func (f *speculationFixture) candidate(t *testing.T, slot uint32, seed byte) (simplex.CandidateID, *SelectedBaseState) {
	t.Helper()
	id, base := f.rawCandidate(t, slot, seed)
	// The fixture's block comes from a canned collation whose seqno is far ahead
	// of the session's masterchain watermark, which would make the empty-block
	// policy refuse every bet for a reason that has nothing to do with what these
	// tests are about. Move the watermark to where a live session would have it.
	managed, err := f.service.runningSession(f.session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.policyMu.Lock()
	managed.emptyPolicy.ObserveMCFinalized(base.successor.NextSeqno)
	managed.policyMu.Unlock()

	return id, base
}

func (f *speculationFixture) speculate(
	t *testing.T,
	base *SelectedBaseState,
	startSlot uint32,
) error {
	t.Helper()

	return f.service.SpeculateWindow(context.Background(), SpeculativeWindowRequest{
		SessionID: f.session.ID,
		StartSlot: startSlot,
		Leader:    0,
		Base:      base,
		StartAt:   time.Now(),
		Deadline:  time.Now().Add(5 * time.Second),
	})
}

// openWindow drives the two calls a real observation makes, in the order it
// makes them: the consensus progress that installs the base, then the self
// window that starts the producer.
func (f *speculationFixture) openWindow(
	t *testing.T,
	startSlot uint32,
	base simplex.CandidateID,
	baseState *SelectedBaseState,
) {
	t.Helper()
	progress := ConsensusProgress{
		SessionID: f.session.ID,
		Window: simplex.Window{
			Base:         simplex.Parent(base),
			ObservedSlot: startSlot,
			StartSlot:    startSlot,
			EndSlot:      startSlot + f.windowSize,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
		StartAt: time.Now(),
		Base:    baseState,
	}
	if err := f.service.ApplyConsensusProgress(context.Background(), progress); err != nil {
		t.Fatal(err)
	}
	if err := f.service.ActivateSelfWindow(
		context.Background(),
		f.selfRequest(f.session, startSlot, time.Now().Add(5*time.Second)),
	); err != nil {
		t.Fatal(err)
	}
}

// The property the whole change exists for: when the window opens on the very
// candidate the bet was placed on, the first slot is NOT collated again. The
// build the producer emits is the one that started before the window existed.
func TestSpeculativeFirstSlotIsAdoptedWhenTheWindowOpensOnItsBase(t *testing.T) {
	probe := newSpeculationProbe()
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		probe.note(request)

		return runtimeBuiltCandidate(request), nil
	}
	emitted := make(chan CandidateArtifact, 4)
	fixture := newSpeculationFixture(t, pipeline, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	base, baseState := fixture.candidate(t, fixture.windowSize-1, 0x11)
	if err := fixture.speculate(t, baseState, fixture.windowSize); err != nil {
		t.Fatal(err)
	}
	select {
	case slot := <-probe.entered:
		if slot != fixture.windowSize {
			t.Fatalf("speculative build slot = %d, want %d", slot, fixture.windowSize)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the speculative build never started")
	}

	fixture.openWindow(t, fixture.windowSize, base, baseState)
	first := runtimeAwaitArtifact(t, emitted)
	if first.Candidate.ID.Slot != fixture.windowSize {
		t.Fatalf("first emitted slot = %d, want %d", first.Candidate.ID.Slot, fixture.windowSize)
	}

	slots, speculative := probe.snapshot()
	firstSlotBuilds := 0
	for i := range slots {
		if slots[i] != fixture.windowSize {
			continue
		}
		firstSlotBuilds++
		if !speculative[i] {
			t.Fatalf("the first slot was collated again on the ordinary path: builds %v, speculative %v",
				slots, speculative)
		}
	}
	if firstSlotBuilds != 1 {
		t.Fatalf("first slot builds = %d, want exactly the speculative one: %v / %v",
			firstSlotBuilds, slots, speculative)
	}
}

// The bet the producer is allowed to lose. When the window opens on a different
// base, the speculative build must be dropped and the slot collated again on the
// base consensus actually selected — anything else signs a block for a parent
// the network did not notarize.
func TestSpeculativeFirstSlotIsDroppedWhenTheWindowOpensOnAnotherBase(t *testing.T) {
	probe := newSpeculationProbe()
	release := make(chan struct{})
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		probe.note(request)
		if request.speculative != nil {
			// Held open so the drop is observable as a cancellation rather than
			// as a build that happened to finish first.
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-release:
			}
		}

		return runtimeBuiltCandidate(request), nil
	}
	emitted := make(chan CandidateArtifact, 4)
	fixture := newSpeculationFixture(t, pipeline, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer func() {
		close(release)
		fixture.close(t)
	}()

	_, betState := fixture.candidate(t, fixture.windowSize-1, 0x22)
	selected, selectedState := fixture.candidate(t, fixture.windowSize-1, 0x33)
	if err := fixture.speculate(t, betState, fixture.windowSize); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the speculative build never started")
	}

	fixture.openWindow(t, fixture.windowSize, selected, selectedState)
	first := runtimeAwaitArtifact(t, emitted)
	if first.Candidate.ID.Slot != fixture.windowSize {
		t.Fatalf("first emitted slot = %d, want %d", first.Candidate.ID.Slot, fixture.windowSize)
	}
	if first.Candidate.Parent != simplex.Parent(selected) {
		t.Fatalf("first emitted parent = %v, want the selected base %v",
			first.Candidate.Parent, simplex.Parent(selected))
	}

	slots, speculative := probe.snapshot()
	ordinary := 0
	for i := range slots {
		if slots[i] == fixture.windowSize && !speculative[i] {
			ordinary++
		}
	}
	if ordinary != 1 {
		t.Fatalf("ordinary first-slot builds = %d, want 1: %v / %v", ordinary, slots, speculative)
	}
}

// A bet installs nothing. This is the property that makes losing one free, and
// it is the one the session-transition rules force: a window start that is
// already fixed may never change its StartAt, and a base may never move to an
// earlier slot, so a guess that had been installed would make the real window
// unrepresentable.
func TestSpeculationLeavesTheSessionUntouched(t *testing.T) {
	var updates, advances int
	var mu sync.Mutex
	probe := newSpeculationProbe()
	pipeline := &runtimeTestPipeline{}
	pipeline.update = func(context.Context, Session, SessionUpdate) error {
		mu.Lock()
		updates++
		mu.Unlock()

		return nil
	}
	pipeline.advance = func(context.Context, ConsensusBaseUpdate) error {
		mu.Lock()
		advances++
		mu.Unlock()

		return nil
	}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		probe.note(request)
		<-ctx.Done()

		return nil, ctx.Err()
	}
	fixture := newSpeculationFixture(t, pipeline, nil)
	defer fixture.close(t)

	before, err := fixture.service.runningSession(fixture.session.ID)
	if err != nil {
		t.Fatal(err)
	}
	before.mu.Lock()
	recorded := cloneSessionUpdate(before.record.Update)
	before.mu.Unlock()

	_, baseState := fixture.candidate(t, fixture.windowSize-1, 0x44)
	if err = fixture.speculate(t, baseState, fixture.windowSize); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the speculative build never started")
	}

	before.mu.Lock()
	after := cloneSessionUpdate(before.record.Update)
	before.mu.Unlock()
	if !after.Equal(recorded) {
		t.Fatalf("speculation moved the session update:\n before %+v\n after  %+v", recorded, after)
	}
	mu.Lock()
	defer mu.Unlock()
	if updates != 0 || advances != 0 {
		t.Fatalf("speculation touched the pipeline session view: updates=%d advances=%d", updates, advances)
	}
}

// Every gate that refuses a bet, each checked by the build never starting. The
// refusals are silent by design — a bet is an optimization, and a session that
// cannot take one must simply produce the window the ordinary way.
func TestSpeculationIsRefusedOutsideTheWindowBeforeOurs(t *testing.T) {
	for _, test := range []struct {
		name      string
		startSlot func(windowSize uint32) uint32
	}{
		{
			name:      "the window already in progress",
			startSlot: func(uint32) uint32 { return 0 },
		},
		{
			name:      "a window beyond the next one",
			startSlot: func(windowSize uint32) uint32 { return windowSize * 2 },
		},
		{
			name:      "a slot that does not start a window",
			startSlot: func(windowSize uint32) uint32 { return windowSize + 1 },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			probe := newSpeculationProbe()
			pipeline := &runtimeTestPipeline{}
			pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
				probe.note(request)

				return runtimeBuiltCandidate(request), nil
			}
			fixture := newSpeculationFixture(t, pipeline, nil)
			defer fixture.close(t)

			_, baseState := fixture.candidate(t, fixture.windowSize-1, 0x55)
			if err := fixture.speculate(t, baseState, test.startSlot(fixture.windowSize)); err != nil {
				t.Fatal(err)
			}
			select {
			case slot := <-probe.entered:
				t.Fatalf("a refused speculation started a build for slot %d", slot)
			case <-time.After(200 * time.Millisecond):
			}
		})
	}
}

// One bet at a time. A second one for the same window would put two collations
// on the same CPU in front of the window they are both for, and the producer can
// only ever collect one.
func TestSpeculationRefusesASecondBet(t *testing.T) {
	probe := newSpeculationProbe()
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		probe.note(request)
		<-ctx.Done()

		return nil, ctx.Err()
	}
	fixture := newSpeculationFixture(t, pipeline, nil)
	defer fixture.close(t)

	_, first := fixture.candidate(t, fixture.windowSize-1, 0x66)
	_, second := fixture.candidate(t, fixture.windowSize-1, 0x77)
	if err := fixture.speculate(t, first, fixture.windowSize); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the first speculative build never started")
	}
	if err := fixture.speculate(t, second, fixture.windowSize); err != nil {
		t.Fatal(err)
	}
	select {
	case slot := <-probe.entered:
		t.Fatalf("a second bet started a build for slot %d", slot)
	case <-time.After(200 * time.Millisecond):
	}
}

// The acquisition half, driven directly: a speculative request resolves the
// predecessor it carries, and does so while the session still points at the
// window before — which is the whole reason the predecessor travels by value.
func TestSpeculativeChainResolvesTheCarriedBaseWithoutTheSession(t *testing.T) {
	built, err := testBuilder().BuildShard(context.Background(), emptyCandidateRequest(t))
	if err != nil {
		t.Fatal(err)
	}
	var sessionID [32]byte
	sessionID[0] = 0x5a
	candidate := simplex.CandidateID{Slot: 3, Hash: [32]byte{0x5b}}
	base, err := NewSelectedBaseState(
		sessionID,
		candidate,
		built.ID,
		built.BlockBOC,
		candidateBlock(t, built),
		built.State,
	)
	if err != nil {
		t.Fatal(err)
	}
	// Empty on purpose: the resolution must not consult it. A session in the
	// window before ours holds that window's base here, never the one a bet is
	// placed on. The pool carries the base's own position as applied, which is
	// the one case the speculative resolution still takes the plain path —
	// no lineage, no candidate tip; the lineage cases live in
	// speculative_lineage_test.go.
	destination := blockShardIdent(built.ID)
	pool := msgpool.New(msgpool.Config{})
	t.Cleanup(pool.Close)
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	baseRef, err := localSourceRef(built.ID)
	if err != nil {
		t.Fatal(err)
	}
	if err = pool.Internals().Seed(destination, destination, baseRef, nil, 0); err != nil {
		t.Fatal(err)
	}
	managed := &localAcquisitionSession{
		branch:     openLocalTestBranch(t, pool, destination),
		candidates: map[simplex.CandidateID]localCandidateState{},
		blocks:     map[[32]byte]localCandidateState{},
	}
	request := BuildRequest{
		Slot: 4,
		// Still the previous window, with a base that is not the one carried.
		Update: SessionUpdate{
			HasCurrentWindow:   true,
			CurrentWindowStart: 0,
			CurrentBase:        simplex.Parent(simplex.CandidateID{Slot: 1, Hash: [32]byte{0x5c}}),
		},
		Parent:      simplex.Parent(candidate),
		speculative: &speculativeBase{state: base, at: time.Unix(1787464000, 0)},
	}

	chain, err := (&LocalAcquisition{messages: pool}).resolveChain(context.Background(), managed, request)
	if err != nil {
		t.Fatal(err)
	}
	if len(chain.previous) != 1 || !chain.previous[0].ID.Equals(&built.ID) {
		t.Fatalf("speculative predecessor = %+v, want the carried base %v", chain.previous, built.ID)
	}
	if chain.candidateTip != nil || chain.queueBase != nil {
		t.Fatalf("speculative chain carried a queue lineage: tip=%v base=%+v", chain.candidateTip, chain.queueBase)
	}
	if len(managed.candidates) != 0 || len(managed.blocks) != 0 {
		t.Fatal("speculative resolution installed something in the session")
	}
}

// A speculative predecessor and an installed one describe different states, and
// a request carrying both says nothing coherent about which block is extended.
func TestSpeculativeChainRefusesASecondPredecessor(t *testing.T) {
	managed := &localAcquisitionSession{
		candidates: map[simplex.CandidateID]localCandidateState{},
		blocks:     map[[32]byte]localCandidateState{},
	}
	for _, test := range []struct {
		name   string
		mutate func(*BuildRequest)
	}{
		{
			name: "an installed previous artifact",
			mutate: func(request *BuildRequest) {
				request.Previous = &CandidateArtifact{}
			},
		},
		{
			name: "a pending successor offer",
			mutate: func(request *BuildRequest) {
				request.PreviousPending = &SuccessorOffer{}
			},
		},
		{
			name: "no base at all",
			mutate: func(request *BuildRequest) {
				request.speculative = &speculativeBase{}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			request := BuildRequest{
				Slot:        4,
				speculative: &speculativeBase{state: &SelectedBaseState{}},
			}
			test.mutate(&request)
			if _, err := (&LocalAcquisition{}).resolveChain(
				context.Background(),
				managed,
				request,
			); !errors.Is(err, ErrCandidateConflict) {
				t.Fatalf("resolve error = %v, want %v", err, ErrCandidateConflict)
			}
		})
	}
}

// The block's time comes from the estimate the bet was placed with, not from
// slot arithmetic over the window the session is still in — which would date the
// block by a whole window and leave clampLocalHeaderTime to rescue it.
func TestSpeculativeHeaderComesFromTheEstimatedWindowStart(t *testing.T) {
	at := time.Unix(1787464321, 456_000_000)
	acquisition := &LocalAcquisition{}
	request := BuildRequest{
		Slot: 4,
		Update: SessionUpdate{
			TargetRate:           400 * time.Millisecond,
			HasCurrentWindow:     true,
			CurrentWindowStart:   0,
			CurrentWindowStartAt: at.Add(-2 * time.Second),
		},
		speculative: &speculativeBase{state: &SelectedBaseState{}, at: at},
	}

	header, err := acquisition.requestHeader(request)
	if err != nil {
		t.Fatal(err)
	}
	if header.GenUtimeMS != uint64(at.UnixMilli()) || header.GenUtime != uint32(at.Unix()) {
		t.Fatalf("speculative header = %+v, want the estimate %d/%d",
			header, at.Unix(), at.UnixMilli())
	}
	// The same request without the bet is the window-relative arithmetic, and it
	// is a different instant — which is what makes the branch above load-bearing.
	ordinary := request
	ordinary.speculative = nil
	plain, err := acquisition.requestHeader(ordinary)
	if err != nil {
		t.Fatal(err)
	}
	if plain.GenUtimeMS == header.GenUtimeMS {
		t.Fatal("the speculative instant matched the window arithmetic; the test proves nothing")
	}
}

// A bet nobody comes to collect. The producer for a window this node does not
// open never runs, so the consensus progress that observed the window has to be
// what takes the bet down.
func TestSpeculationIsDroppedByConsensusProgressWithoutAProducer(t *testing.T) {
	probe := newSpeculationProbe()
	cancelled := make(chan struct{}, 1)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		probe.note(request)
		<-ctx.Done()
		select {
		case cancelled <- struct{}{}:
		default:
		}

		return nil, ctx.Err()
	}
	fixture := newSpeculationFixture(t, pipeline, nil)
	defer fixture.close(t)

	_, betState := fixture.candidate(t, fixture.windowSize-1, 0x88)
	selected, selectedState := fixture.candidate(t, fixture.windowSize-1, 0x89)
	if err := fixture.speculate(t, betState, fixture.windowSize); err != nil {
		t.Fatal(err)
	}
	select {
	case <-probe.entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the speculative build never started")
	}

	// Only the progress: no self window is activated, so nothing else can drop it.
	if err := fixture.service.ApplyConsensusProgress(context.Background(), ConsensusProgress{
		SessionID: fixture.session.ID,
		Window: simplex.Window{
			Base:         simplex.Parent(selected),
			ObservedSlot: fixture.windowSize,
			StartSlot:    fixture.windowSize,
			EndSlot:      fixture.windowSize * 2,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
		StartAt: time.Now(),
		Base:    selectedState,
	}); err != nil {
		t.Fatal(err)
	}
	select {
	case <-cancelled:
	case <-time.After(2 * time.Second):
		t.Fatal("consensus progress left the superseded bet running")
	}
}

// stoppedSpeculation is a parked bet whose build is already finished, so stop()
// returns immediately and the slot's own decisions are what the test observes.
// The cancel is recorded through a channel rather than a flag: a refused bet is
// cancelled at once but joined in the background, so a plain bool would be read
// by the test while the abandoning goroutine writes it.
func stoppedSpeculation(startSlot uint32, base simplex.CandidateID) (*speculativeProduction, chan struct{}) {
	stopped := make(chan struct{})
	done := make(chan struct{})
	close(done)
	future := &candidateBuildFuture{
		result:       make(chan candidateBuildResult, 1),
		done:         done,
		cancel:       sync.OnceFunc(func() { close(stopped) }),
		hardDeadline: time.Now().Add(time.Minute),
	}

	return &speculativeProduction{startSlot: startSlot, base: base, future: future}, stopped
}

func awaitStopped(t *testing.T, stopped chan struct{}, want bool) {
	t.Helper()
	if want {
		select {
		case <-stopped:
		case <-time.After(2 * time.Second):
			t.Fatal("the bet was left running")
		}

		return
	}
	select {
	case <-stopped:
		t.Fatal("the bet was stopped")
	case <-time.After(100 * time.Millisecond):
	}
}

// The adoption rule on its own. It is the last line of defence rather than the
// first — the consensus progress that observes a window normally settles a bet
// before any producer reaches it — so it needs its own gate: an end-to-end test
// cannot tell this check from the one that already ran.
func TestSpeculationIsAdoptedOnlyForItsOwnWindowAndBase(t *testing.T) {
	base := simplex.CandidateID{Slot: 3, Hash: [32]byte{0xa1}}
	other := simplex.CandidateID{Slot: 3, Hash: [32]byte{0xa2}}
	for _, test := range []struct {
		name      string
		startSlot uint32
		parent    simplex.ParentID
		adopted   bool
	}{
		{name: "same window and base", startSlot: 4, parent: simplex.Parent(base), adopted: true},
		{name: "another base", startSlot: 4, parent: simplex.Parent(other)},
		{name: "consensus genesis", startSlot: 4, parent: simplex.Genesis()},
		{name: "another window", startSlot: 6, parent: simplex.Parent(base)},
	} {
		t.Run(test.name, func(t *testing.T) {
			slot := &speculationSlot{}
			spec, stopped := stoppedSpeculation(4, base)
			if !slot.install(spec) {
				t.Fatal("the bet was not parked")
			}
			future, _, taken := slot.takeMatching(test.startSlot, test.parent)
			if taken != test.adopted {
				t.Fatalf("adopted = %v, want %v", taken, test.adopted)
			}
			if test.adopted {
				if future != spec.future {
					t.Fatal("adoption returned a different build")
				}
				awaitStopped(t, stopped, false)

				return
			}
			if future != nil {
				t.Fatal("a refused adoption returned a build")
			}
			awaitStopped(t, stopped, true)
			if _, _, pending := slot.pending(); pending {
				t.Fatal("a refused bet stayed parked")
			}
		})
	}
}

// The settlement rule on its own: a bet survives exactly the window and base it
// was placed for, and is taken down by anything else the session observes.
func TestSpeculationSurvivesOnlyTheWindowItBetOn(t *testing.T) {
	base := simplex.CandidateID{Slot: 3, Hash: [32]byte{0xb1}}
	other := simplex.CandidateID{Slot: 3, Hash: [32]byte{0xb2}}
	for _, test := range []struct {
		name      string
		startSlot uint32
		parent    simplex.ParentID
		dropped   bool
	}{
		{name: "its own window and base", startSlot: 4, parent: simplex.Parent(base)},
		{name: "its window on another base", startSlot: 4, parent: simplex.Parent(other), dropped: true},
		{name: "a later window", startSlot: 6, parent: simplex.Parent(base), dropped: true},
		{name: "consensus genesis", startSlot: 4, parent: simplex.Genesis(), dropped: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			slot := &speculationSlot{}
			spec, stopped := stoppedSpeculation(4, base)
			if !slot.install(spec) {
				t.Fatal("the bet was not parked")
			}
			if dropped := slot.dropOutdated(test.startSlot, test.parent); dropped != test.dropped {
				t.Fatalf("dropped = %v, want %v", dropped, test.dropped)
			}
			_, _, pending := slot.pending()
			if pending == test.dropped {
				t.Fatalf("parked = %v after dropped = %v", pending, test.dropped)
			}
			awaitStopped(t, stopped, test.dropped)
		})
	}
}

// A window the producer would open with an empty candidate is not worth
// collating for. The verdict is taken here, early, from the state the base
// produced — safe in one direction, and this is the direction that holds: a
// verdict of "collate" cannot become "empty" while the bet is in flight.
func TestSpeculationIsRefusedWhenThePolicyWantsAnEmptyBlock(t *testing.T) {
	probe := newSpeculationProbe()
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		probe.note(request)

		return runtimeBuiltCandidate(request), nil
	}
	fixture := newSpeculationFixture(t, pipeline, nil)
	defer fixture.close(t)

	// Deliberately not armed: the session's masterchain watermark is far behind
	// this base's seqno, which is exactly when the producer must emit an empty
	// candidate instead of a collated one.
	_, base := fixture.rawCandidate(t, fixture.windowSize-1, 0x91)
	managed, err := fixture.service.runningSession(fixture.session.ID)
	if err != nil {
		t.Fatal(err)
	}
	if !managed.shouldGenerateEmpty(false, base.successor) {
		t.Fatal("the fixture no longer reproduces a window the policy would empty")
	}
	if err = fixture.speculate(t, base, fixture.windowSize); err != nil {
		t.Fatal(err)
	}
	select {
	case slot := <-probe.entered:
		t.Fatalf("a window the policy would empty started a speculative build for slot %d", slot)
	case <-time.After(200 * time.Millisecond):
	}
}

// A bet whose build is no longer usable is not this slot's build. Adopting one
// would make its failure the slot's failure — and a build abandoned on its own
// deadline fails with an error the producer treats as terminal, so the window
// would be lost rather than collated the ordinary way.
func TestSpeculationIsNotAdoptedOnceItsBuildCannotServe(t *testing.T) {
	base := simplex.CandidateID{Slot: 3, Hash: [32]byte{0xc1}}
	for _, test := range []struct {
		name    string
		prepare func(*speculativeProduction)
	}{
		{
			name: "its lifetime ran out before the window opened",
			prepare: func(spec *speculativeProduction) {
				spec.expiry = time.Now().Add(-time.Millisecond)
			},
		},
		{
			name: "its build's own deadline passed before the window opened",
			prepare: func(spec *speculativeProduction) {
				spec.future.hardDeadline = time.Now().Add(-time.Millisecond)
			},
		},
		{
			name: "the build already failed",
			prepare: func(spec *speculativeProduction) {
				spec.future.result <- candidateBuildResult{err: context.DeadlineExceeded}
			},
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			slot := &speculationSlot{}
			spec, stopped := stoppedSpeculation(4, base)
			test.prepare(spec)
			if !slot.install(spec) {
				t.Fatal("the bet was not parked")
			}
			if future, _, taken := slot.takeMatching(4, simplex.Parent(base)); taken || future != nil {
				t.Fatal("a build that cannot serve was adopted")
			}
			awaitStopped(t, stopped, true)
		})
	}
}

// The other half of the same rule: a build that finished successfully before the
// window opened is exactly what the mechanism is for, and its result must still
// be there for the producer to read after the viability check looked at it.
func TestSpeculationAdoptsAFinishedBuildWithItsResultIntact(t *testing.T) {
	base := simplex.CandidateID{Slot: 3, Hash: [32]byte{0xc2}}
	slot := &speculationSlot{}
	spec, stopped := stoppedSpeculation(4, base)
	spec.future.result <- candidateBuildResult{elapsed: time.Millisecond}
	if !slot.install(spec) {
		t.Fatal("the bet was not parked")
	}
	future, _, taken := slot.takeMatching(4, simplex.Parent(base))
	if !taken || future == nil {
		t.Fatal("a finished build was not adopted")
	}
	awaitStopped(t, stopped, false)
	select {
	case result := <-future.result:
		if result.err != nil || result.elapsed != time.Millisecond {
			t.Fatalf("adopted result = %+v, want the one the build produced", result)
		}
	default:
		t.Fatal("the adopted build lost its result")
	}
}

// The two external instants of a speculative first slot say different things,
// and a change that collapses them silently costs the block its externals — the
// difference measured in the field between ~260 and ~400 transactions in the
// first slot of a window.
//
// The wait must stay at the estimate: a build racing a window that may open at
// any moment must never idle for messages to arrive. The processing budget must
// not, or the deadline has already passed when the first ready batch is offered
// and every one of them is refused.
func TestSpeculativeFirstSlotMayExecuteReadyExternalsButNeverWaitsForThem(t *testing.T) {
	requests := make(chan BuildRequest, 4)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		requests <- request
		<-ctx.Done()

		return nil, ctx.Err()
	}
	fixture := newSpeculationFixture(t, pipeline, nil)
	defer fixture.close(t)

	_, base := fixture.candidate(t, fixture.windowSize-1, 0xa9)
	if err := fixture.speculate(t, base, fixture.windowSize); err != nil {
		t.Fatal(err)
	}
	var request BuildRequest
	select {
	case request = <-requests:
	case <-time.After(2 * time.Second):
		t.Fatal("the speculative build never started")
	}

	rate := fixture.update.TargetRate
	if request.ExternalProcessUntil.Sub(request.PaceStartedAt) != rate {
		t.Fatalf("external processing budget = %v, want one target rate %v",
			request.ExternalProcessUntil.Sub(request.PaceStartedAt), rate)
	}
	if !request.ExternalWaitUntil.Equal(request.PaceStartedAt) {
		t.Fatalf("external wait = %v, want the estimate %v so the build never idles",
			request.ExternalWaitUntil, request.PaceStartedAt)
	}
	if !request.ExternalProcessUntil.After(request.ExternalWaitUntil) {
		t.Fatal("the processing budget did not outlive the wait; ready externals would be refused")
	}
}

// The bet's lifetime bounds how long it waits to be collected, not how long its
// build may run. A producer adopts the future as it is — context included — so
// a build cut off by the bet's short lifetime would surface as the slot's
// hard-deadline error, which the producer treats as terminal; one slow first
// slot would then cost the whole window. The build must run under the deadline
// the slot itself would have had.
func TestSpeculativeBuildOutlivesTheBetsLifetime(t *testing.T) {
	entered := make(chan struct{}, 1)
	release := make(chan struct{})
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(ctx context.Context, request BuildRequest) (*Candidate, error) {
		entered <- struct{}{}
		select {
		case <-release:
			return runtimeBuiltCandidate(request), nil
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	fixture := newSpeculationFixture(t, pipeline, nil)
	defer fixture.close(t)

	baseID, base := fixture.candidate(t, fixture.windowSize-1, 0x93)
	lifetime := 150 * time.Millisecond
	err := fixture.service.SpeculateWindow(context.Background(), SpeculativeWindowRequest{
		SessionID: fixture.session.ID,
		StartSlot: fixture.windowSize,
		Leader:    0,
		Base:      base,
		StartAt:   time.Now(),
		Deadline:  time.Now().Add(lifetime),
	})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(2 * time.Second):
		t.Fatal("the speculative build did not start")
	}
	managed, err := fixture.service.runningSession(fixture.session.ID)
	if err != nil {
		t.Fatal(err)
	}
	// Collect the bet before its lifetime runs out, the way a window opening on
	// time does, then let the build run well past that lifetime.
	future, _, taken := managed.speculation.takeMatching(fixture.windowSize, simplex.Parent(baseID))
	if !taken {
		t.Fatal("the bet was not collectable")
	}
	time.Sleep(2 * lifetime)
	select {
	case result := <-future.result:
		t.Fatalf("the adopted build ended on its own with %v; its context was bounded by the bet's lifetime", result.err)
	default:
	}
	if !future.hardDeadline.After(time.Now().Add(lifetime)) {
		t.Fatalf("adopted build hard deadline %v is the bet's lifetime, not the slot's", time.Until(future.hardDeadline))
	}
	close(release)
	select {
	case result := <-future.result:
		if result.err != nil {
			t.Fatalf("adopted build failed: %v", result.err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the adopted build did not finish")
	}
	future.stop()
}

// The other half: a bet nobody collects is still taken down when its lifetime
// runs out, even though its build's own deadline is far longer. Without this,
// a stalled session would leave every bet it placed collating to the slot's
// sixty-second deadline.
func TestSpeculationExpiresWhileParked(t *testing.T) {
	base := simplex.CandidateID{Slot: 3, Hash: [32]byte{0xc3}}
	slot := &speculationSlot{}
	spec, stopped := stoppedSpeculation(4, base)
	spec.expiry = time.Now().Add(50 * time.Millisecond)
	if !slot.install(spec) {
		t.Fatal("the bet was not parked")
	}
	awaitStopped(t, stopped, true)
	if _, _, pending := slot.pending(); pending {
		t.Fatal("an expired bet is still parked")
	}
	// A bet collected in time is not touched by its timer afterwards.
	spec, stopped = stoppedSpeculation(5, base)
	spec.expiry = time.Now().Add(50 * time.Millisecond)
	if !slot.install(spec) {
		t.Fatal("the second bet was not parked")
	}
	if _, _, taken := slot.takeMatching(5, simplex.Parent(base)); !taken {
		t.Fatal("the second bet was not collected")
	}
	time.Sleep(120 * time.Millisecond)
	awaitStopped(t, stopped, false)
}

// Adoption and build completion are independent goroutines. When adoption wins,
// the later offer must be forwarded, and a withdrawal must not overtake the
// producer installing the successor it names.
func TestSpeculativeHandoffForwardsLateOfferBeforeWithdrawal(t *testing.T) {
	handoff := &speculativeHandoff{}
	acceptEntered := make(chan struct{})
	releaseAccept := make(chan struct{})
	accepted := make(chan SuccessorOffer, 1)
	revoked := make(chan speculativeWithdrawal, 1)
	handoff.adopt(
		func(offer SuccessorOffer) {
			close(acceptEntered)
			<-releaseAccept
			accepted <- offer
		},
		func(token *successorToken, root [32]byte, outcome PipelineHandoffOutcome) {
			revoked <- speculativeWithdrawal{token: token, root: root, outcome: outcome}
		},
	)

	root := [32]byte{0xb4}
	token := &successorToken{}
	offer := SuccessorOffer{
		successorPayload: successorPayload{ID: runtimeTestBlockID(0, -1<<63, 9)},
		token:            token,
	}
	offer.ID.RootHash = root[:]
	parked := make(chan struct{})
	go func() {
		handoff.park(offer)
		close(parked)
	}()
	select {
	case <-acceptEntered:
	case <-time.After(time.Second):
		t.Fatal("offer parked after adoption was not forwarded to the producer")
	}

	handoff.withdraw(token, root, PipelineHandoffAbandonedSuperseded)
	select {
	case <-revoked:
		t.Fatal("withdrawal overtook the producer accepting the late offer")
	default:
	}
	close(releaseAccept)
	select {
	case <-parked:
	case <-time.After(time.Second):
		t.Fatal("late offer did not finish forwarding")
	}
	select {
	case got := <-accepted:
		if [32]byte(got.ID.RootHash) != root {
			t.Fatal("producer accepted a different late offer")
		}
	case <-time.After(time.Second):
		t.Fatal("producer did not accept the late offer")
	}
	select {
	case got := <-revoked:
		if got.token != token || got.root != root || got.outcome != PipelineHandoffAbandonedSuperseded {
			t.Fatalf("forwarded withdrawal = %+v", got)
		}
	case <-time.After(time.Second):
		t.Fatal("withdrawal was not forwarded after offer acceptance")
	}
}

// The boundary the bet could not cover until now. A pipelined build hands its
// successor straight to the producer; a speculative build finishes before its
// window opens, so at the instant it hands over there is no producer to hand to
// and the offer used to be dropped on the floor. Slot 0 into slot 1 was then the
// one unpipelined boundary in a window the bet had otherwise bought a head start
// for.
//
// The offer is parked instead, and the producer collects it together with the
// build. What that looks like from the pipeline's side is a slot 1 build carrying
// PreviousPending — started from its predecessor before that predecessor was
// committed — rather than one the producer opened on schedule.
func TestAdoptedSpeculativeFirstSlotHandsOffToTheSecond(t *testing.T) {
	var mu sync.Mutex
	pending := map[uint32]bool{}
	entered := make(chan uint32, 8)
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		mu.Lock()
		if _, seen := pending[request.Slot]; !seen {
			pending[request.Slot] = request.PreviousPending != nil
		}
		mu.Unlock()
		select {
		case entered <- request.Slot:
		default:
		}
		candidate := runtimeBuiltCandidate(request)
		// The stand-in for what a real build does on its block-BOC branch: hand
		// the finished predecessor over before anything is serialized. A
		// speculative build reaches this with no producer in existence, which is
		// the case the parked offer is for.
		runtimeHandOffSuccessor(request, candidate, nil, nil)

		return candidate, nil
	}
	pipeline.state = func(_ context.Context, request BuildRequest) (CandidateState, error) {
		block := cloneBlockID(request.Previous.Candidate.Block)

		return CandidateState{Block: block, NextSeqno: block.SeqNo + 1}, nil
	}
	emitted := make(chan CandidateArtifact, 8)
	fixture := newSpeculationFixture(t, pipeline, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	base, baseState := fixture.candidate(t, fixture.windowSize-1, 0x21)
	if err := fixture.speculate(t, baseState, fixture.windowSize); err != nil {
		t.Fatal(err)
	}
	select {
	case slot := <-entered:
		if slot != fixture.windowSize {
			t.Fatalf("speculative build slot = %d, want %d", slot, fixture.windowSize)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the speculative build never started")
	}

	fixture.openWindow(t, fixture.windowSize, base, baseState)
	first := runtimeAwaitArtifact(t, emitted)
	second := runtimeAwaitArtifact(t, emitted)
	if first.Candidate.ID.Slot != fixture.windowSize || second.Candidate.ID.Slot != fixture.windowSize+1 {
		t.Fatalf("emitted slots = %d, %d, want %d, %d",
			first.Candidate.ID.Slot, second.Candidate.ID.Slot, fixture.windowSize, fixture.windowSize+1)
	}

	mu.Lock()
	defer mu.Unlock()
	if !pending[fixture.windowSize+1] {
		t.Fatal("the slot after an adopted speculative build was opened on schedule; " +
			"the offer the bet parked was never collected")
	}
}

// newSessionStartFixture prepares and activates a session on which consensus
// has not opened a window yet: the state a session is in right after its
// activation, when the session-start bet is placed.
func newSessionStartFixture(
	t *testing.T,
	pipeline *runtimeTestPipeline,
	emit EmitCandidate,
) *speculationFixture {
	t.Helper()
	const windowSize = 2
	fixture := newRuntimeSelfFixture(t, pipeline, nil, nil, emit)
	session, update := fixture.session(83, windowSize, 0, time.Time{})
	update.HasCurrentWindow = false
	update.CurrentWindowObservedSlot = 0
	update.CurrentWindowStartAt = time.Time{}
	fixture.prepare(t, session, update)

	return &speculationFixture{
		runtimeFixture: fixture,
		session:        session,
		update:         update,
		windowSize:     windowSize,
	}
}

// openGenesisWindow is consensus opening window zero of a fresh session: its
// base is the session genesis, and there is no base state to hand over.
func (f *speculationFixture) openGenesisWindow(t *testing.T) {
	t.Helper()
	progress := ConsensusProgress{
		SessionID: f.session.ID,
		Window: simplex.Window{
			Base:         simplex.Genesis(),
			ObservedSlot: 0,
			StartSlot:    0,
			EndSlot:      f.windowSize,
			Leader:       0,
			ObservedAt:   time.Now(),
		},
		StartAt: time.Now(),
	}
	if err := f.service.ApplyConsensusProgress(context.Background(), progress); err != nil {
		t.Fatal(err)
	}
	if err := f.service.ActivateSelfWindow(
		context.Background(),
		f.selfRequest(f.session, 0, time.Now().Add(5*time.Second)),
	); err != nil {
		t.Fatal(err)
	}
}

func (f *speculationFixture) speculateSessionStart(t *testing.T) error {
	t.Helper()

	return f.service.SpeculateSessionStart(context.Background(), SpeculativeSessionStartRequest{
		SessionID: f.session.ID,
		Leader:    0,
		StartAt:   time.Now(),
		Deadline:  time.Now().Add(5 * time.Second),
	})
}

// Window zero of a fresh session has no candidate to bet on: its base is the
// session genesis. The bet is placed at activation, before consensus has
// opened anything, and the producer that opens window zero on the genesis
// adopts it — the first slot is collated exactly once, ahead of the window.
func TestSessionStartSpeculationIsAdoptedByWindowZero(t *testing.T) {
	probe := newSpeculationProbe()
	pipeline := &runtimeTestPipeline{}
	var stamped atomic.Bool
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		probe.note(request)
		if request.Slot == 0 && !request.sessionStartAt.IsZero() {
			stamped.Store(true)
		}

		return runtimeBuiltCandidate(request), nil
	}
	emitted := make(chan CandidateArtifact, 4)
	fixture := newSessionStartFixture(t, pipeline, func(_ context.Context, artifact CandidateArtifact) error {
		emitted <- artifact

		return nil
	})
	defer fixture.close(t)

	if err := fixture.speculateSessionStart(t); err != nil {
		t.Fatal(err)
	}
	select {
	case slot := <-probe.entered:
		if slot != 0 {
			t.Fatalf("session-start build slot = %d, want 0", slot)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("the session-start build never started")
	}

	fixture.openGenesisWindow(t)
	first := runtimeAwaitArtifact(t, emitted)
	if first.Candidate.ID.Slot != 0 {
		t.Fatalf("first emitted slot = %d, want 0", first.Candidate.ID.Slot)
	}
	slots, _ := probe.snapshot()
	builds := 0
	for _, slot := range slots {
		if slot == 0 {
			builds++
		}
	}
	if builds != 1 {
		t.Fatalf("slot 0 builds = %d, want exactly the session-start one: %v", builds, slots)
	}
	if !stamped.Load() {
		t.Fatal("the session-start build carried no header instant: the real acquisition would refuse it as outside the current window")
	}
}

// Once consensus has opened a window the ordinary producer owns slot zero,
// and a late session-start bet is declined without a build.
func TestSessionStartSpeculationIsDeclinedOnceAWindowIsOpen(t *testing.T) {
	probe := newSpeculationProbe()
	pipeline := &runtimeTestPipeline{}
	pipeline.build = func(_ context.Context, request BuildRequest) (*Candidate, error) {
		probe.note(request)

		return runtimeBuiltCandidate(request), nil
	}
	fixture := newSpeculationFixture(t, pipeline, func(context.Context, CandidateArtifact) error { return nil })
	defer fixture.close(t)

	if err := fixture.speculateSessionStart(t); err != nil {
		t.Fatal(err)
	}
	select {
	case slot := <-probe.entered:
		t.Fatalf("a session-start build for slot %d started although a window is open", slot)
	case <-time.After(300 * time.Millisecond):
	}
}

// The session-start bet is built before consensus has observed any window, so
// the window-relative header arithmetic has nothing to measure from: the
// header is stamped with the instant the bet was placed. Without this every
// such bet failed on the stand with "build slot is outside the current window"
// and window zero was collated only once the engine had started.
func TestSessionStartBetStampsItsHeaderWithoutAWindow(t *testing.T) {
	at := time.Unix(1_700_000_000, 250_000_000)
	acquisition := &LocalAcquisition{}
	request := BuildRequest{
		Slot:           0,
		Update:         SessionUpdate{TargetRate: 400 * time.Millisecond},
		sessionStartAt: at,
	}
	header, err := acquisition.requestHeader(request)
	if err != nil {
		t.Fatal(err)
	}
	if header.GenUtimeMS != uint64(at.UnixMilli()) || header.GenUtime != uint32(at.Unix()) {
		t.Fatalf("session-start header = %+v, want the bet's instant %d/%d", header, at.Unix(), at.UnixMilli())
	}
	// The same windowless request without the bet's instant is still refused.
	request.sessionStartAt = time.Time{}
	if _, err = acquisition.requestHeader(request); err == nil {
		t.Fatal("a windowless build without the session-start instant must be refused")
	}
}
