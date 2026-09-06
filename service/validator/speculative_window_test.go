package validator

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"math"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/ton"
)

const speculativeTestWindow = 4

// speculativeTestBackend is a backend positioned in the window before the one
// ourIndex leads, which is the only position a bet is placed from.
func speculativeTestBackend(ourIndex uint32, validators int) *LocalSessionBackend {
	return &LocalSessionBackend{
		self:           &localBackendTestCollator{},
		productionMode: collator.ProductionModeSelf,
		validator:      &ValidatorIdentity{Index: ourIndex},
		config: SessionConfig{
			SessionID:  [32]byte{0x31},
			Shard:      groups.ShardID{Workchain: 0, Shard: math.MinInt64},
			Protocol:   SessionProtocol{SlotsPerLeaderWindow: speculativeTestWindow},
			Validators: make([]groups.Validator, validators),
		},
	}
}

func speculativeTestView(currentWindowStart uint32, rate time.Duration) *localValidationView {
	return &localValidationView{
		update: collator.SessionUpdate{
			TargetRate:         rate,
			HasCurrentWindow:   true,
			CurrentWindowStart: currentWindowStart,
		},
	}
}

// The one moment a bet is worth placing: the last slot of the current window has
// just validated, and the window its notarization opens is ours.
func TestSpeculationIsPlacedOnTheLastSlotOfTheWindowBeforeOurs(t *testing.T) {
	backend := speculativeTestBackend(1, 4)
	now := time.Unix(1787464321, 0)

	bet, ok := backend.nextWindowBet(
		speculativeTestView(0, 400*time.Millisecond),
		speculativeTestWindow-1,
		time.Time{},
		now,
	)
	if !ok {
		t.Fatal("no bet was placed on the last slot of the window before ours")
	}
	if bet.startSlot != speculativeTestWindow {
		t.Fatalf("bet start slot = %d, want %d", bet.startSlot, speculativeTestWindow)
	}
	if !bet.startAt.Equal(now) {
		t.Fatalf("bet start = %v, want the present %v", bet.startAt, now)
	}
	if !bet.deadline.Equal(now.Add(3 * 400 * time.Millisecond)) {
		t.Fatalf("bet deadline = %v, want three target rates out", bet.deadline)
	}
}

// The tail of the window before ours bets too: when the committee skips that
// window's last slots, the window opens on an earlier candidate, and a bet
// placed only from the last slot never existed. Each tail slot's bet lives for
// the slots still to come plus the two rates the last slot's bet always had.
func TestSpeculationIsPlacedFromTheTailOfTheWindowBeforeOurs(t *testing.T) {
	backend := speculativeTestBackend(1, 4)
	rate := 400 * time.Millisecond
	now := time.Unix(1787464321, 0)
	for _, test := range []struct {
		slot     uint32
		deadline time.Duration
	}{
		{slot: speculativeTestWindow - 1, deadline: 3 * rate},
		{slot: speculativeTestWindow - 2, deadline: 4 * rate},
		{slot: speculativeTestWindow - 3, deadline: 5 * rate},
	} {
		bet, ok := backend.nextWindowBet(speculativeTestView(0, rate), test.slot, time.Time{}, now)
		if !ok {
			t.Fatalf("no bet was placed from slot %d", test.slot)
		}
		if bet.startSlot != speculativeTestWindow {
			t.Fatalf("slot %d bet on window %d, want %d", test.slot, bet.startSlot, speculativeTestWindow)
		}
		if !bet.deadline.Equal(now.Add(test.deadline)) {
			t.Fatalf("slot %d bet deadline = %v, want %s out", test.slot, bet.deadline, test.deadline)
		}
	}
}

// Every position a bet must NOT be placed from. Each is a case where the
// candidate just validated either cannot be the one whose notarization opens a
// window, or opens a window this node does not lead, or belongs to a session
// that cannot act on it.
func TestSpeculationIsRefusedOutsideItsOneMoment(t *testing.T) {
	rate := 400 * time.Millisecond
	for _, test := range []struct {
		name    string
		backend func() *LocalSessionBackend
		view    func() *localValidationView
		slot    uint32
	}{
		{
			// The window before ours is four slots here, and the last three of
			// them bet (speculationTailSlots); its first slot is the one that
			// does not.
			name:    "a slot before the tail of the window",
			backend: func() *LocalSessionBackend { return speculativeTestBackend(1, 4) },
			view:    func() *localValidationView { return speculativeTestView(0, rate) },
			slot:    speculativeTestWindow - 4,
		},
		{
			name:    "the next window belongs to another leader",
			backend: func() *LocalSessionBackend { return speculativeTestBackend(2, 4) },
			view:    func() *localValidationView { return speculativeTestView(0, rate) },
			slot:    speculativeTestWindow - 1,
		},
		{
			name:    "the session is not in the window before",
			backend: func() *LocalSessionBackend { return speculativeTestBackend(1, 4) },
			view:    func() *localValidationView { return speculativeTestView(speculativeTestWindow, rate) },
			slot:    speculativeTestWindow - 1,
		},
		{
			name:    "the session has no window at all",
			backend: func() *LocalSessionBackend { return speculativeTestBackend(1, 4) },
			view: func() *localValidationView {
				view := speculativeTestView(0, rate)
				view.update.HasCurrentWindow = false

				return view
			},
			slot: speculativeTestWindow - 1,
		},
		{
			name: "a delegated producer",
			backend: func() *LocalSessionBackend {
				backend := speculativeTestBackend(1, 4)
				backend.productionMode = collator.ProductionModeDelegated

				return backend
			},
			view: func() *localValidationView { return speculativeTestView(0, rate) },
			slot: speculativeTestWindow - 1,
		},
		{
			name: "an observer with no voting identity",
			backend: func() *LocalSessionBackend {
				backend := speculativeTestBackend(1, 4)
				backend.validator = nil

				return backend
			},
			view: func() *localValidationView { return speculativeTestView(0, rate) },
			slot: speculativeTestWindow - 1,
		},
		{
			name: "a masterchain session",
			backend: func() *LocalSessionBackend {
				backend := speculativeTestBackend(1, 4)
				backend.config.Shard = groups.ShardID{Workchain: -1, Shard: math.MinInt64}

				return backend
			},
			view: func() *localValidationView { return speculativeTestView(0, rate) },
			slot: speculativeTestWindow - 1,
		},
		{
			name:    "a session with no pacing",
			backend: func() *LocalSessionBackend { return speculativeTestBackend(1, 4) },
			view:    func() *localValidationView { return speculativeTestView(0, 0) },
			slot:    speculativeTestWindow - 1,
		},
		{
			// A window start that is not a multiple of the window size is a state
			// the session should never present — every path that sets one aligns
			// it. The rule this checks is about the slot, not about the session
			// that reported it: a bet is placed on the candidate whose
			// notarization STARTS a window, and nothing else makes it the base.
			name:    "a slot that does not start a window",
			backend: func() *LocalSessionBackend { return speculativeTestBackend(1, 4) },
			view:    func() *localValidationView { return speculativeTestView(1, rate) },
			slot:    speculativeTestWindow,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			if bet, ok := test.backend().nextWindowBet(
				test.view(),
				test.slot,
				time.Time{},
				time.Unix(1787464321, 0),
			); ok {
				t.Fatalf("a bet was placed from %s: %+v", test.name, bet)
			}
		})
	}
}

// The instant the block is stamped with is the one the observed window would
// compute, which is what keeps a speculative block inside the reference
// validator's monotonicity rules without relying on the clamp to rescue it.
func TestSpeculationStampsTheInstantTheObservedWindowWouldCompute(t *testing.T) {
	backend := speculativeTestBackend(1, 4)
	rate := 400 * time.Millisecond
	now := time.Unix(1787464321, 0)
	view := speculativeTestView(0, rate)

	for _, test := range []struct {
		name       string
		validAfter time.Time
		want       time.Time
	}{
		{
			name:       "a parent generated long ago starts the window now",
			validAfter: now.Add(-time.Hour),
			want:       now,
		},
		{
			name:       "a parent generated a moment ago starts it one rate later",
			validAfter: now.Add(-100 * time.Millisecond),
			want:       now.Add(300 * time.Millisecond),
		},
		{
			name:       "a parent in the future is clamped to one rate ahead",
			validAfter: now.Add(time.Hour),
			want:       now.Add(rate),
		},
		{
			name:       "an unknown generation time starts the window now",
			validAfter: time.Time{},
			want:       now,
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			bet, ok := backend.nextWindowBet(view, speculativeTestWindow-1, test.validAfter, now)
			if !ok {
				t.Fatal("no bet was placed")
			}
			if !bet.startAt.Equal(test.want) {
				t.Fatalf("bet start = %v, want %v", bet.startAt, test.want)
			}
		})
	}
}

// The bet is placed from the validation path and nowhere else. Its whole value
// is the head start between a candidate becoming valid and its certificate
// arriving, so a call site that drifted to any later event would leave the
// mechanism intact and silently worthless — which no behavioural test can see.
func TestSpeculationIsReachedFromCandidateValidation(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "local_session_backend.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	callers := map[string]int{}
	for _, declaration := range parsed.Decls {
		function, ok := declaration.(*ast.FuncDecl)
		if !ok {
			continue
		}
		ast.Inspect(function, func(node ast.Node) bool {
			call, ok := node.(*ast.CallExpr)
			if !ok {
				return true
			}
			selector, ok := call.Fun.(*ast.SelectorExpr)
			if !ok || selector.Sel.Name != "speculateNextWindow" {
				return true
			}
			callers[function.Name.Name]++

			return true
		})
	}
	if callers["ValidateCandidate"] != 1 || len(callers) != 1 {
		t.Fatalf("speculateNextWindow callers = %v, want exactly ValidateCandidate", callers)
	}
}

// Activating a session this node leads from slot zero places the session-start
// bet with the collator, timed from now and bounded by the first-block timeout
// plus two target rates; a session led by someone else places none.
func TestSessionStartBetIsPlacedAtActivationWhenWeLeadWindowZero(t *testing.T) {
	for _, tc := range []struct {
		name  string
		index uint32
		bets  int
	}{
		{name: "leader of window zero", index: 0, bets: 1},
		{name: "another validator", index: 1, bets: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			producer := &localBackendTestCollator{id: [32]byte{0x63}, sessionStartWake: make(chan struct{}, 1)}
			backend := speculativeTestBackend(tc.index, 4)
			backend.collator = producer
			backend.self = producer
			backend.state = SessionState{Params: simplex.DefaultParams()}
			backend.update = collator.SessionUpdate{SessionID: backend.config.SessionID, TargetRate: 400 * time.Millisecond}
			start := SessionStart{
				Genesis:        []ton.BlockIDExt{localBackendTestBlockID(0, math.MinInt64, 7, []byte{7}, nil)},
				MinMasterchain: localBackendTestBlockID(-1, math.MinInt64, 3, []byte{3}, nil),
			}
			before := time.Now()
			if err := backend.ActivateSession(context.Background(), start); err != nil {
				t.Fatal(err)
			}
			if tc.bets == 1 {
				select {
				case <-producer.sessionStartWake:
				case <-time.After(2 * time.Second):
					t.Fatal("no session-start bet reached the collator")
				}
			} else {
				time.Sleep(100 * time.Millisecond)
			}
			producer.mu.Lock()
			calls := append([]collator.SpeculativeSessionStartRequest(nil), producer.sessionStartCall...)
			producer.mu.Unlock()
			if len(calls) != tc.bets {
				t.Fatalf("session-start bets = %d, want %d", len(calls), tc.bets)
			}
			if tc.bets == 0 {
				return
			}
			bet := calls[0]
			if bet.SessionID != backend.config.SessionID || bet.Leader != tc.index {
				t.Fatalf("bet = %+v, want session %x led by %d", bet, backend.config.SessionID[:2], tc.index)
			}
			if bet.StartAt.Before(before) {
				t.Fatalf("bet start %v precedes the activation at %v", bet.StartAt, before)
			}
			want := simplex.DefaultParams().FirstBlockTimeout + 2*400*time.Millisecond
			if got := bet.Deadline.Sub(bet.StartAt); got != want {
				t.Fatalf("bet lifetime = %v, want the first-block timeout plus two target rates %v", got, want)
			}
		})
	}
}
