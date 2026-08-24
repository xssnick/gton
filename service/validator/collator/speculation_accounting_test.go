package collator

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// speculationAccountingObserver records the two series a bet must be visible
// on and invisible on. The rest of the interface is stubbed because
// CollationObserver is a bounded-enum interface.
type speculationAccountingObserver struct {
	mu       sync.Mutex
	handoffs map[PipelineHandoffOutcome]int
	builds   []CollationBuildObservation
	inflight int
}

func newSpeculationAccountingObserver() *speculationAccountingObserver {
	return &speculationAccountingObserver{handoffs: map[PipelineHandoffOutcome]int{}}
}

func (o *speculationAccountingObserver) ObservePipelineHandoff(_ MetricChain, outcome PipelineHandoffOutcome) {
	o.mu.Lock()
	o.handoffs[outcome]++
	o.mu.Unlock()
}

func (o *speculationAccountingObserver) ObserveCollationBuild(observation CollationBuildObservation) {
	o.mu.Lock()
	o.builds = append(o.builds, observation)
	o.mu.Unlock()
}

func (o *speculationAccountingObserver) AddCollationBuildInflight(_ MetricChain, delta int) {
	o.mu.Lock()
	o.inflight += delta
	o.mu.Unlock()
}

func (o *speculationAccountingObserver) counts() (map[PipelineHandoffOutcome]int, []CollationBuildObservation, int) {
	o.mu.Lock()
	defer o.mu.Unlock()
	out := make(map[PipelineHandoffOutcome]int, len(o.handoffs))
	for outcome, count := range o.handoffs {
		out[outcome] = count
	}

	return out, append([]CollationBuildObservation(nil), o.builds...), o.inflight
}

func (o *speculationAccountingObserver) ObservePipelineHandoffPickup(MetricChain, time.Duration)   {}
func (o *speculationAccountingObserver) ObservePipelineOverlap(MetricChain, time.Duration)         {}
func (o *speculationAccountingObserver) ObserveCollationStage(CollationStageObservation)           {}
func (o *speculationAccountingObserver) ObserveCollationCandidate(CandidateObservation)            {}
func (o *speculationAccountingObserver) ObserveCandidateProduction(CandidateProductionObservation) {}
func (o *speculationAccountingObserver) ObserveCollationRetry(MetricChain, ProductionRetryReason)  {}
func (o *speculationAccountingObserver) ObserveCollationAlarm(MetricChain, CollationAlarm)         {}
func (o *speculationAccountingObserver) AddCollationWindowInflight(MetricChain, int)               {}
func (o *speculationAccountingObserver) ObserveCollationWindow(WindowObservation)                  {}
func (o *speculationAccountingObserver) ObserveScheduleLateness(MetricChain, ScheduleEvent, time.Duration) {
}
func (o *speculationAccountingObserver) ObserveCollationDeadline(MetricChain, CollationDeadline, DeadlineAction) {
}

// TestEveryBetLeavesTheSlotAccountedExactlyOnce is the gate the regression it
// was written for slipped through: a speculation that stopped being adopted
// entirely showed 85 bets started, one missed and nothing else, because three
// of the four exit paths dropped a bet without a word. Each path below reports
// once, and the one that means the build could not serve is told apart from the
// one that means consensus went elsewhere.
func TestEveryBetLeavesTheSlotAccountedExactlyOnce(t *testing.T) {
	const startSlot = 8
	base := simplex.CandidateID{Slot: 7, Hash: [32]byte{0xb1}}
	other := simplex.CandidateID{Slot: 7, Hash: [32]byte{0xb2}}

	for _, tc := range []struct {
		name    string
		broken  bool
		exit    func(*speculationSlot)
		want    map[PipelineHandoffOutcome]int
		stopped bool
	}{
		{
			name: "adopted reports nothing here",
			exit: func(slot *speculationSlot) {
				if _, _, taken := slot.takeMatching(startSlot, simplex.Parent(base)); !taken {
					t.Fatal("the producer refused the bet placed for its own window and base")
				}
			},
			want: map[PipelineHandoffOutcome]int{},
		},
		{
			name: "refused for another window is a miss",
			exit: func(slot *speculationSlot) {
				if _, _, taken := slot.takeMatching(startSlot+4, simplex.Parent(base)); taken {
					t.Fatal("a bet was adopted for a window it was not placed for")
				}
			},
			want:    map[PipelineHandoffOutcome]int{PipelineHandoffSpeculativeMissed: 1},
			stopped: true,
		},
		{
			name:   "refused because the build cannot serve is a failure",
			broken: true,
			exit: func(slot *speculationSlot) {
				if _, _, taken := slot.takeMatching(startSlot, simplex.Parent(base)); taken {
					t.Fatal("a bet whose build failed was adopted")
				}
			},
			want:    map[PipelineHandoffOutcome]int{PipelineHandoffSpeculativeFailed: 1},
			stopped: true,
		},
		{
			name: "settled against another base is a miss",
			exit: func(slot *speculationSlot) {
				if !slot.dropOutdated(startSlot, simplex.Parent(other)) {
					t.Fatal("a bet survived a window that opened on another base")
				}
			},
			want:    map[PipelineHandoffOutcome]int{PipelineHandoffSpeculativeMissed: 1},
			stopped: true,
		},
		{
			name: "expiring while parked is a miss",
			exit: func(slot *speculationSlot) {
				slot.expire(slot.current)
			},
			want:    map[PipelineHandoffOutcome]int{PipelineHandoffSpeculativeMissed: 1},
			stopped: true,
		},
		{
			name: "closing the slot is a miss",
			exit: func(slot *speculationSlot) {
				slot.close()
			},
			want:    map[PipelineHandoffOutcome]int{PipelineHandoffSpeculativeMissed: 1},
			stopped: true,
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observer := newSpeculationAccountingObserver()
			spec, stopped := stoppedSpeculation(startSlot, base)
			spec.report = func(outcome PipelineHandoffOutcome) {
				observer.ObservePipelineHandoff(MetricChainShardchain, outcome)
			}
			if tc.broken {
				spec.future.result <- candidateBuildResult{err: errors.New("acquisition not ready")}
			}
			var slot speculationSlot
			if !slot.install(spec) {
				t.Fatal("install refused a bet on an empty slot")
			}

			tc.exit(&slot)

			got, _, _ := observer.counts()
			if len(got) != len(tc.want) {
				t.Fatalf("outcomes = %v, want %v", got, tc.want)
			}
			for outcome, count := range tc.want {
				if got[outcome] != count {
					t.Fatalf("outcome %v = %d, want %d (all: %v)", outcome, got[outcome], count, got)
				}
			}
			awaitStopped(t, stopped, tc.stopped)
		})
	}
}

// TestSpeculativeBuildStaysOffTheCollationSeries pins where a bet's failure is
// allowed to show up. A lost bet costs the collation nothing, so counting it as
// a collation build makes the error panel report failures the collator never
// had, and holding it in the inflight gauge shows a collation running while
// this node is not even in the leader order. The complement is checked in the
// same test, because an exclusion that swallowed real builds would be worse
// than the noise it removes.
func TestSpeculativeBuildStaysOffTheCollationSeries(t *testing.T) {
	shard := groups.ShardID{Workchain: 0, Shard: -1 << 63}
	failure := errors.New("acquisition not ready")

	for _, speculative := range []bool{true, false} {
		name := "speculative"
		if !speculative {
			name = "ordinary"
		}
		t.Run(name, func(t *testing.T) {
			observer := newSpeculationAccountingObserver()
			pipeline := &runtimeTestPipeline{}
			pipeline.build = func(context.Context, BuildRequest) (*Candidate, error) {
				return nil, failure
			}
			service := newObservedRuntimeService(t, pipeline, observer)

			request := BuildRequest{
				Session: ActivatedSession{Session: Session{Shard: shard}},
				Slot:    12,
			}
			if speculative {
				request.speculative = &speculativeBase{at: time.Now()}
			}
			future := service.startBuildFuture(context.Background(), request, time.Now().Add(time.Minute))
			select {
			case <-future.done:
			case <-time.After(5 * time.Second):
				t.Fatal("the build never finished")
			}

			_, builds, inflight := observer.counts()
			if speculative {
				if len(builds) != 0 {
					t.Fatalf("a bet was counted as a collation build: %+v", builds)
				}
				if inflight != 0 {
					t.Fatalf("a bet moved the collation inflight gauge to %d", inflight)
				}

				return
			}
			if len(builds) != 1 {
				t.Fatalf("an ordinary build was counted %d times, want once", len(builds))
			}
			if inflight != 0 {
				t.Fatalf("inflight gauge left at %d after the build finished", inflight)
			}
		})
	}
}

// newObservedRuntimeService is a started service whose only purpose is to run
// startBuildFuture against a recording observer. It needs no session: the
// build future reads the shard and slot out of the request it is given.
func newObservedRuntimeService(
	t *testing.T,
	pipeline *runtimeTestPipeline,
	observer CollationObserver,
) *Service {
	t.Helper()
	keys := newRuntimeTestKeys(t)
	service, err := NewService(ServiceOptions{
		ProductionMode: ProductionModeSelf,
		Storage:        newRuntimeMemoryStorage(),
		Pipeline:       pipeline,
		Keys:           keys,
		CollatorKeyID:  keys.id,
		Observer:       observer,
		Emit:           func(context.Context, CandidateArtifact) error { return nil },
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = service.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = service.Close(context.Background()) })

	return service
}
