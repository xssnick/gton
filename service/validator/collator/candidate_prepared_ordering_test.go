package collator

import (
	"errors"
	"runtime"
	"testing"

	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// retainedHeapBytes is the live heap one value pins, measured by keeping it
// alive across a collection. TotalAlloc would answer a different question — how
// much the construction churned — and the question here is what a suspended
// validation task keeps while its inputs are unavailable.
func retainedHeapBytes(build func() any) (uint64, any) {
	runtime.GC()
	var before, after runtime.MemStats
	runtime.ReadMemStats(&before)
	value := build()
	runtime.GC()
	runtime.ReadMemStats(&after)
	runtime.KeepAlive(value)
	if after.HeapAlloc < before.HeapAlloc {
		return 0, value
	}

	return after.HeapAlloc - before.HeapAlloc, value
}

// The update-side walk is the most expensive thing candidate validation decides
// with, and its verdict is a pure function of the update cell — it cannot change
// with anything a lagging node is waiting for. So it must not run before the
// task has acquired the view it needs.
//
// Two orderings are pinned:
//
//   - A node that is behind waits at the masterchain view while retaining this
//     capsule. It must not pay the walk before that wait. The capsule's own
//     verdict field is the proof: it is written exactly where the walk runs and
//     nowhere else.
//   - An oversized candidate is refused by the size limits, and those run first
//     inside the config-bound stage, so the walk is not paid before ErrSizeLimit
//     either.
func TestValidationDefersTheUpdateWalkPastTheMasterView(t *testing.T) {
	req, _ := benchMainnetRequest(t, benchMainnetFiller)
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("collate the mainnet fixture: %v", err)
	}
	artifact := candidateValidationArtifact(candidate)

	prepared, err := prepareValidationCandidate(
		t.Context(), artifact, req.CreatedBy, false, []PreviousBlock{req.Previous},
	)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.verified.stateUpdate != nil {
		t.Fatal("the task walked the candidate state update before acquiring its master view")
	}
	if prepared.stateRoot != nil || prepared.candidate.State != nil {
		t.Fatal("the task applied the candidate state update before acquiring its master view")
	}

	// The size limits reject before the walk, on the very stage that owns both.
	oversized := *req.Masterchain.Config
	oversized.maxBlockBytes = uint32(len(candidate.BlockBOC) - 1)
	if err = prepared.bindConfig(t.Context(), &oversized); !errors.Is(err, ErrSizeLimit) {
		t.Fatalf("oversized candidate = %v, want ErrSizeLimit", err)
	}
	if prepared.verified.stateUpdate != nil {
		t.Fatal("an oversized candidate paid the update walk before it was refused for its size")
	}
	if prepared.stateRoot != nil || prepared.candidate.State != nil {
		t.Fatal("an oversized candidate applied its state update before it was refused for its size")
	}

	// And once the config accepts the candidate, the walk runs exactly once.
	if err = prepared.bindConfig(t.Context(), req.Masterchain.Config); err != nil {
		t.Fatalf("bind the candidate to its config: %v", err)
	}
	if prepared.verified.stateUpdate == nil {
		t.Fatal("the config-bound stage never decided the state update")
	}

	// What the suspended task does not retain before the dependency arrives.
	update := prepared.verified.block.StateUpdate
	plannedBytes, plannedCapsule := retainedHeapBytes(func() any {
		capsule, prepareErr := cell.PrepareMerkleUpdatePlanned(update)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		return capsule
	})
	plainBytes, _ := retainedHeapBytes(func() any {
		capsule, prepareErr := cell.PrepareMerkleUpdate(update)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		return capsule
	})
	if !plannedCapsule.(*cell.PreparedMerkleUpdate).Planned() {
		t.Fatal("the planned form recorded no plans")
	}
	t.Logf(
		"while waiting, not yet paid: verdict walk over a %d-byte block; "+
			"retained plans %d B (planned capsule) against %d B (verdict only)",
		len(candidate.BlockBOC), plannedBytes, plainBytes,
	)
}

// The apply plans are the expensive half of a decided update — one entry per
// update node, half a megabyte on a real mainnet block — and they are worth
// recording on exactly one path: the proof-backed shard run, where the successor
// this call builds stands on the candidate's own proof and the caller therefore
// has to apply the same update a second time, to the parent it holds. Everywhere
// else the update is applied once and the result handed back, so a plan would be
// built, retained for the length of the call and never replayed.
func TestApplyPlansAreRecordedOnlyWhereOneIsApplied(t *testing.T) {
	// Runs in the shared-fixture parallel batch: it reads the cached mainnet
	// workload and keeps every mutation on its own copy, holds no package-level
	// counter and derives nothing from wall-clock timing.
	t.Parallel()
	// (1) The masterchain, through the real entry point: its successor is always
	// carried back, so it never wants a plan.
	acquisition, request, _ := advSessionValidation(t)
	result, err := acquisition.ValidateCandidate(t.Context(), request)
	if err != nil {
		t.Fatalf("validate a masterchain candidate: %v", err)
	}
	if result.Successor.Prepared == nil {
		t.Fatal("the masterchain path decided no update at all")
	}
	if result.Successor.Prepared.Planned() {
		t.Fatal("the masterchain path recorded apply plans nothing applies")
	}

	// (2) A shard candidate without full collated data: same carry-back, same
	// answer, and this is the fixture whose plans are actually large.
	req, _ := benchMainnetRequest(t, benchMainnetFiller)
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("collate the mainnet fixture: %v", err)
	}
	plain, err := runLazyBudgetValidation(t, req, candidate, nil, nil, nil)
	if err != nil {
		t.Fatalf("validate a shard candidate without full collated data: %v", err)
	}
	if plain.substituted {
		t.Fatal("the fixture is proof-backed, so it measures the other path")
	}
	if plain.verified.stateUpdate.Planned() {
		t.Fatal("a shard candidate whose successor is carried back recorded apply plans")
	}

	// (3) And the one path that does apply a plan.
	provenReq := benchMainnetCollatedRequest(t, 1)
	proven, err := testBuilder().BuildShard(t.Context(), provenReq)
	if err != nil {
		t.Fatalf("collate the mainnet fixture with full collated data: %v", err)
	}
	assertBenchMainnetCollated(t, proven)
	backed, err := runLazyBudgetValidation(t, provenReq, proven, nil, NewSemanticVerifier(tvm.NewTVM()),
		func(verification *ShardVerificationRequest) {
			verification.Neighbors = collatedNeighborQueues(t, provenReq, proven)
			verification.stateProven = true
		})
	if err != nil {
		t.Fatalf("validate a proof-backed shard candidate: %v", err)
	}
	if !backed.substituted {
		t.Fatal("the full-collated fixture did not take the proof-backed path")
	}
	if !backed.verified.stateUpdate.Planned() {
		t.Fatal("the proof-backed path recorded no plans for the apply the caller still has to make")
	}

	// (4) The case that separates "carries full collated data" from "applies to a
	// proven predecessor": a candidate whose collated data IS full and whose
	// predecessors are NOT substituted. That is every masterchain candidate on a
	// network with the capability on — collated data carries no masterchain state
	// proof, so the masterchain keeps its resident predecessor and carries its
	// successor back — and the masterchain flag is the only input that decides it.
	master, err := prepareValidationCandidate(
		t.Context(),
		candidateValidationArtifact(proven),
		provenReq.CreatedBy,
		true,
		[]PreviousBlock{provenReq.Previous},
	)
	if err != nil {
		t.Fatalf("prepare a full-collated candidate on the masterchain path: %v", err)
	}
	if !master.verified.collated.full || master.substituted {
		t.Fatalf("fixture is not the discriminating shape: full=%v substituted=%v",
			master.verified.collated.full, master.substituted)
	}
	if err = master.bindConfig(t.Context(), provenReq.Masterchain.Config); err != nil {
		t.Fatalf("bind the full-collated candidate to its config: %v", err)
	}
	if master.verified.stateUpdate.Planned() {
		t.Fatal("a candidate that carries full collated data but applies to its resident parent recorded plans")
	}

	// What the carried-back paths stopped retaining, on the update of (2).
	update := plain.verified.block.StateUpdate
	plannedBytes, _ := retainedHeapBytes(func() any {
		capsule, prepareErr := cell.PrepareMerkleUpdatePlanned(update)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		return capsule
	})
	plainBytes, _ := retainedHeapBytes(func() any {
		capsule, prepareErr := cell.PrepareMerkleUpdate(update)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		return capsule
	})
	t.Logf("retained per validation call: %d B with plans, %d B without", plannedBytes, plainBytes)
}
