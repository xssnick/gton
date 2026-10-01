package validator

import (
	"context"
	"fmt"
	"math"
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
		states: &stateResolver{candidates: &candidateResolver{}},
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

func TestObserverBetsFromTheSameWindowTailAsEmbedded(t *testing.T) {
	offered := make(chan sessionSpeculativeWindow, 1)
	runtime := newObserverSpeculationRuntime(t, offered)
	backend := speculativeTestBackend(0, len(runtime.config.Validators))
	rate := runtime.state.Params.TargetRate

	for _, slot := range []uint32{4, 5, 6, 7} {
		t.Run(fmt.Sprint(slot), func(t *testing.T) {
			embedded, eligible := backend.nextWindowBet(
				speculativeTestView(4, rate), slot, time.Time{}, time.Now(),
			)
			base := simplex.CandidateID{Slot: slot, Hash: [32]byte{byte(slot)}}
			runtime.offerSpeculativeWindow(context.Background(), base, ResolvedState{State: observerSpeculationState()})
			select {
			case window := <-offered:
				if !eligible {
					t.Fatal("observer offered a slot outside the embedded producer's tail")
				}
				if window.StartSlot != embedded.startSlot || window.StartSlot != 8 || window.Base != base {
					t.Fatalf("bet = %+v, want base %v and window 8", window, base)
				}
				if want := runtime.codec.schedule.ExpectedLeader(8); window.Leader != want {
					t.Fatalf("bet leader = %d, want %d", window.Leader, want)
				}
				if window.Deadline.Sub(window.StartAt) != embedded.deadline.Sub(embedded.startAt) {
					t.Fatalf("bet lifetime = %s, embedded = %s", window.Deadline.Sub(window.StartAt), embedded.deadline.Sub(embedded.startAt))
				}
			default:
				if eligible {
					t.Fatal("observer skipped an eligible tail candidate")
				}
			}
		})
	}
}

func TestObserverDoesNotBetAfterShutdownOrSlotOverflow(t *testing.T) {
	offered := make(chan sessionSpeculativeWindow, 1)
	runtime := newObserverSpeculationRuntime(t, offered)
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	runtime.offerSpeculativeWindow(ctx, simplex.CandidateID{Slot: 7}, ResolvedState{State: observerSpeculationState()})
	for _, slot := range []uint32{math.MaxUint32 - 2, math.MaxUint32 - 1, math.MaxUint32} {
		runtime.offerSpeculativeWindow(t.Context(), simplex.CandidateID{Slot: slot}, ResolvedState{State: observerSpeculationState()})
	}
	select {
	case window := <-offered:
		t.Fatalf("invalid speculative window offered: %+v", window)
	default:
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
