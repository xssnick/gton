package validator

import (
	"context"
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

func newObserverSpeculationRuntime(
	t *testing.T,
	offered chan<- sessionSpeculativeWindow,
) *sessionRuntime {
	t.Helper()

	config, _ := runtimeTestConfig(0x73, &runtimeTestJournal{})
	config.Protocol.SlotsPerLeaderWindow = 4
	config.StorageID.Protocol = config.Protocol
	codec, err := newCandidateCodec(config, CandidateLimits{
		MaxBlockBytes:        1 << 20,
		MaxCollatedDataBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}

	runtime := &sessionRuntime{
		config: config,
		codec:  codec,
		state:  SessionState{Params: simplex.DefaultParams()},
	}
	runtime.state.Params.TargetRate = 400 * time.Millisecond
	runtime.speculate = func(_ context.Context, window sessionSpeculativeWindow) error {
		offered <- window

		return nil
	}

	return runtime
}

func observerSpeculationState() *ChainState {
	return &ChainState{tips: []ChainTip{{}}}
}

// The bet is placed on exactly one candidate per window: the one whose
// certificate opens the next window, and therefore the base that window carries
// unless the network skips it. A bet on any earlier slot names a base consensus
// will not select, and the producer would drop it.
func TestObserverBetsOnlyOnTheLastSlotOfAWindow(t *testing.T) {
	offered := make(chan sessionSpeculativeWindow, 4)
	runtime := newObserverSpeculationRuntime(t, offered)

	for _, slot := range []uint32{4, 5, 6} {
		runtime.offerSpeculativeWindow(
			context.Background(),
			simplex.CandidateID{Slot: slot},
			ResolvedState{State: observerSpeculationState()},
		)
		select {
		case window := <-offered:
			t.Fatalf("slot %d placed a bet on window %d", slot, window.StartSlot)
		default:
		}
	}

	base := simplex.CandidateID{Slot: 7, Hash: [32]byte{0x77}}
	runtime.offerSpeculativeWindow(
		context.Background(),
		base,
		ResolvedState{State: observerSpeculationState()},
	)
	select {
	case window := <-offered:
		if window.StartSlot != 8 {
			t.Fatalf("bet window start = %d, want 8", window.StartSlot)
		}
		// Two validators are in the roster fixture's rotation, so the leader of
		// the window this bet is for is not the leader of the one it was placed
		// from. A collator that guessed the current window's leader would build
		// a block its own producer refuses to sign.
		if want := window.StartSlot / 4 % uint32(len(runtime.config.Validators)); window.Leader != want {
			t.Fatalf("bet leader = %d, want %d", window.Leader, want)
		}
		if window.Base != base {
			t.Fatalf("bet base = %+v, want the candidate it was placed from %+v", window.Base, base)
		}
	case <-time.After(time.Second):
		t.Fatal("the last slot of a window placed no bet")
	}
}

// The header instant a speculative first block is stamped with is the one the
// observed window would compute: the base's generation time plus one target
// rate, never earlier than now and never more than one rate ahead. Getting it
// from the wall clock instead would stamp a block the network then rejects for
// its generation time.
func TestObserverBetStampsTheInstantTheObservedWindowWouldCompute(t *testing.T) {
	offered := make(chan sessionSpeculativeWindow, 1)
	runtime := newObserverSpeculationRuntime(t, offered)
	rate := runtime.state.Params.TargetRate

	for _, test := range []struct {
		name     string
		genUtime time.Time
		want     func(now time.Time) time.Time
	}{
		{
			name:     "a base older than one rate stamps now",
			genUtime: time.Now().Add(-10 * rate),
			want:     func(now time.Time) time.Time { return now },
		},
		{
			name:     "a base far in the future is clamped to one rate ahead",
			genUtime: time.Now().Add(10 * rate),
			want:     func(now time.Time) time.Time { return now.Add(rate) },
		},
	} {
		t.Run(test.name, func(t *testing.T) {
			before := time.Now()
			runtime.offerSpeculativeWindow(
				context.Background(),
				simplex.CandidateID{Slot: 7},
				ResolvedState{State: observerSpeculationState(), GenUtime: test.genUtime},
			)
			after := time.Now()

			window := <-offered
			low, high := test.want(before), test.want(after)
			if window.StartAt.Before(low) || window.StartAt.After(high) {
				t.Fatalf("bet start = %v, want within [%v, %v]", window.StartAt, low, high)
			}
			if want := window.StartAt.Add(3 * rate); !window.Deadline.Equal(want) {
				t.Fatalf("bet deadline = %v, want %v", window.Deadline, want)
			}
		})
	}
}

// The observer's bet is reached from the candidate warm-up and nowhere else.
// Its whole value is the head start between a candidate arriving and its
// certificate — the same head start a validator gets from validation — so a
// call site that drifted to any later event (a notarization, a window) would
// leave the mechanism intact and worth nothing, which no behavioural test can
// see. This is the observer counterpart of
// TestSpeculationIsReachedFromCandidateValidation.
//
// The warm-up has two entry points — a received candidate bets, a candidate this
// node published does not, because the producer that built it already parked a
// successor for that window — so the bet is pinned to the shared core and
// neither entry point can grow one of its own.
func TestObserverSpeculationIsReachedFromTheCandidateWarmUp(t *testing.T) {
	fileSet := token.NewFileSet()
	parsed, err := parser.ParseFile(fileSet, "session_runtime.go", nil, 0)
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
			if !ok || selector.Sel.Name != "offerSpeculativeWindow" {
				return true
			}
			callers[function.Name.Name]++

			return true
		})
	}
	if callers["warmState"] != 1 || len(callers) != 1 {
		t.Fatalf("offerSpeculativeWindow callers = %v, want exactly warmState", callers)
	}
}
