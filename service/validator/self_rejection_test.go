package validator

import (
	"context"
	"errors"
	"fmt"
	"testing"

	"github.com/rs/zerolog"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

// selfRejectionObserver counts only what this test asks about. Every other
// method is present because ValidationObserver is a bounded-enum interface and
// a partial implementation would not compile.
type selfRejectionObserver struct {
	selfRejected map[collator.MetricChain]int
}

func (o *selfRejectionObserver) AddSelfRejectedCandidate(chain collator.MetricChain) {
	if o.selfRejected == nil {
		o.selfRejected = make(map[collator.MetricChain]int, 2)
	}
	o.selfRejected[chain]++
}

func (o *selfRejectionObserver) AddValidationInflight(ValidationContext, int)      {}
func (o *selfRejectionObserver) ObserveValidation(ValidationObservation)           {}
func (o *selfRejectionObserver) ObserveValidationStage(ValidationStageObservation) {}
func (o *selfRejectionObserver) ObserveValidationTask(ValidationTaskObservation)   {}
func (o *selfRejectionObserver) ObserveValidationCandidateSize(collator.MetricChain, int, int) {
}
func (o *selfRejectionObserver) AddCandidateCache(collator.MetricChain, CandidateCacheDelta) {}
func (o *selfRejectionObserver) AddCandidateRetentionCapped(collator.MetricChain)            {}
func (o *selfRejectionObserver) AddCandidatePersistFailure(collator.MetricChain)             {}
func (o *selfRejectionObserver) AddChainTipWaitBackstop(collator.MetricChain)                {}
func (o *selfRejectionObserver) ObserveSessionSpecRejection(SessionSpecRejectionObservation) {}
func (o *selfRejectionObserver) ObserveValidationCoreStage(collator.ValidationCoreStageObservation) {
}
func (o *selfRejectionObserver) ObserveLineageWalk(LineageWalkObservation) {}
func (o *selfRejectionObserver) RegisterConsensusSession(ConsensusSessionKey, ConsensusSessionSource) {
}
func (o *selfRejectionObserver) UnregisterConsensusSession(ConsensusSessionKey) {}

// TestSelfRejectionAlarmFiresOnlyForOurOwnCandidate pins both halves of the
// alarm ported from BlockValidator::try_notarize
// (cppnode/ton/validator/consensus/block-validator.cpp:112-117): it must fire
// when a rejected candidate's leader is the local index, and it must be
// unreachable otherwise.
//
// The second half is the one worth testing. candidate.Leader is pinned by
// verifyCandidate to schedule.ExpectedLeader(slot) and checked against that
// leader's signature before any of this runs, so a remote peer cannot forge our
// index — but the code must not add a second way in through a runtime that has
// no voting identity at all, which is every observer, delegated and standalone
// composition.
func TestSelfRejectionAlarmFiresOnlyForOurOwnCandidate(t *testing.T) {
	const localIndex = uint32(3)

	rejection := fmt.Errorf("%w: outbound queue entry remains without a dequeue descriptor", ErrCandidateRejected)
	logger := zerolog.Nop()

	for _, tc := range []struct {
		name      string
		validator *ValidatorIdentity
		leader    uint32
		want      int
	}{
		{"our own candidate", &ValidatorIdentity{Index: localIndex}, localIndex, 1},
		{"a peer's candidate", &ValidatorIdentity{Index: localIndex}, localIndex + 1, 0},
		{"a peer's candidate carrying index zero", &ValidatorIdentity{Index: localIndex}, 0, 0},
		// No voting identity at all: the delegated and standalone collator
		// compositions run here, and they have no local index to match.
		{"observer runtime", nil, localIndex, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			observer := &selfRejectionObserver{}
			runtime := &sessionRuntime{
				config: SessionConfig{
					Shard:    groups.ShardID{Workchain: 0, Shard: -0x8000000000000000},
					Identity: SessionIdentity{Validator: tc.validator},
				},
				metrics: observer,
				log:     &logger,
			}
			candidate := &simplex.Candidate{Leader: tc.leader}
			candidate.ID.Slot = 11

			runtime.reportSelfRejection(candidate, rejection)

			if got := observer.selfRejected[collator.MetricChainShardchain]; got != tc.want {
				t.Fatalf("self-rejection alarms = %d, want %d", got, tc.want)
			}
		})
	}

	// And the alarm must not swallow or alter the rejection it reports on.
	observer := &selfRejectionObserver{}
	runtime := &sessionRuntime{
		config: SessionConfig{
			Shard:    groups.ShardID{Workchain: -1, Shard: -0x8000000000000000},
			Identity: SessionIdentity{Validator: &ValidatorIdentity{Index: localIndex}},
		},
		metrics: observer,
		log:     &logger,
	}
	candidate := &simplex.Candidate{Leader: localIndex}
	runtime.reportSelfRejection(candidate, rejection)
	if !errors.Is(rejection, ErrCandidateRejected) {
		t.Fatal("the alarm must leave the rejection it reports on unchanged")
	}
	if got := observer.selfRejected[collator.MetricChainMasterchain]; got != 1 {
		t.Fatalf("masterchain self-rejection alarms = %d, want 1", got)
	}
}

// TestSessionRuntimeRaisesTheSelfRejectionAlarmWhereItValidates pins the alarm
// at its CALL SITE, which the test above cannot: it calls reportSelfRejection
// itself, so deleting the one line that invokes it from the validation path
// leaves the whole ./service/... suite green. An alarm nobody exercises is an
// alarm a later refactor deletes without noticing, and this one exists
// precisely because the fault it reports — a block this node produced and its
// own validator refuses — is otherwise invisible on this node.
//
// The route is the real one: a backend rejection travelling out of
// validateCandidate, on a candidate whose leader is the local index. The peer
// arm keeps the second half honest, because a call site that raised the alarm
// unconditionally would satisfy the first arm alone.
func TestSessionRuntimeRaisesTheSelfRejectionAlarmWhereItValidates(t *testing.T) {
	rejection := fmt.Errorf(
		"%w: outbound queue entry remains without a dequeue descriptor", ErrCandidateRejected)

	for _, tc := range []struct {
		name string
		// index is the local validator index. The fixture's artifact is
		// produced by leader 0, so an index of 0 makes the candidate ours.
		index uint32
		want  int
	}{
		{name: "our own candidate", index: 0, want: 1},
		{name: "a peer's candidate", index: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			backend := newRuntimeTestBackend()
			backend.validation = func(
				_ context.Context,
				_ *ChainState,
				_ *CandidateArtifact,
			) (CandidateValidation, error) {
				return CandidateValidation{}, rejection
			}
			runtime, privateKey := prepareRuntimeTest(
				t, 0x6c, newRuntimeTestStorage(), newRuntimeTestNetwork(), backend)
			defer runtime.Close()

			observer := &selfRejectionObserver{}
			runtime.metrics = observer
			runtime.config.Identity.Validator = &ValidatorIdentity{Index: tc.index}
			if err := runtime.states.start(context.Background(), runtimeTestStart()); err != nil {
				t.Fatal(err)
			}
			artifact := runtimeOrdinaryArtifact(t, runtime.config, privateKey, 0, simplex.Genesis())
			if artifact.Candidate.Leader != 0 {
				t.Fatalf("the fixture's candidate is led by %d, so the arms mean nothing",
					artifact.Candidate.Leader)
			}
			if err := runtime.candidates.stage(artifact, []byte{0x01}); err != nil {
				t.Fatal(err)
			}

			err := runtime.validateCandidate(context.Background(), &artifact.Candidate)
			if !errors.Is(err, ErrCandidateRejected) {
				t.Fatalf("validation error = %v, want the backend's rejection", err)
			}
			if got := observer.selfRejected[runtime.validationChain()]; got != tc.want {
				t.Fatalf("self-rejection alarms = %d, want %d", got, tc.want)
			}
		})
	}
}
