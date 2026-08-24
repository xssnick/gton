package collator

import (
	"bytes"
	"slices"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

// The broadcast payload is the third serialization of one candidate's cells,
// and on the mainnet fixture it costs more than the two component
// serializations put together. Where it starts therefore decides what delivery
// pays: encodeForBroadcast blocks in payloadFor until the capsule is ready, so
// every millisecond the capsule has not overlapped with something is a
// millisecond the block waits before it is on the wire.
//
// Both root sets are final before either component BOC exists — the block root
// since the block was assembled, the collated roots the moment buildCollatedRoots
// returns — so the capsule is started there. Started after the join instead, the
// only work left to overlap was finalization, signing, the marker fsync and the
// commit, and the collated serialization it could have run underneath had
// already been paid serially.
//
// This is the structural half of the pin: the runtime half is
// TestBroadcastCapsuleIsAheadWhenDeliveryAsksForIt below.
func TestBroadcastCapsuleStartsWithItsRootsNotWithTheBOCs(t *testing.T) {
	order := methodCallOrder(t, "build.go", "serializeCollatedData", "c", "cell", "simplex")
	roots := slices.Index(order, "buildCollatedRoots")
	launch := slices.Index(order, "PrepareCandidateAsync")
	serialize := slices.Index(order, "ToBOCWithOptionsErr")
	switch {
	case launch < 0:
		t.Fatalf("serializeCollatedData no longer starts the broadcast payload: %v", order)
	case roots < 0:
		t.Fatalf("serializeCollatedData no longer builds the collated roots: %v", order)
	case launch < roots:
		t.Fatalf("the broadcast payload is started before its collated roots exist: %v", order)
	case serialize < 0:
		t.Fatalf("serializeCollatedData no longer serializes the collated data: %v", order)
	case serialize < launch:
		t.Fatalf(
			"the broadcast payload is started after the collated data is serialized, "+
				"so it no longer overlaps it: %v",
			order,
		)
	}

	// And the far side of the same pin. finish() joins the two component
	// serializations before it does anything else; a capsule started there is a
	// capsule that overlaps neither of them, which is the whole defect.
	if joined := methodCallOrder(t, "build.go", "finish", "simplex"); slices.Contains(joined, "PrepareCandidateAsync") {
		t.Fatalf("finish() starts the broadcast payload after joining both serializations: %v", joined)
	}
}

// broadcastCandidate is the consensus identity of a locally built candidate: the
// four values the capsule is bound against before it is asked for a payload.
func broadcastCandidate(candidate *Candidate) simplex.Candidate {
	broadcast := simplex.Candidate{
		Block: ton.BlockIDExt{
			Workchain: candidate.ID.Workchain,
			Shard:     candidate.ID.Shard,
			SeqNo:     candidate.ID.SeqNo,
			RootHash:  candidate.ID.RootHash,
			FileHash:  candidate.ID.FileHash,
		},
		CollatedFileHash: candidate.CollatedFileHash,
	}
	broadcast.ID = broadcast.ComputeID(0)

	return broadcast
}

// The runtime half: what delivery still has to wait for, against a capsule that
// had no head start at all.
//
// The reference arm is the same payload built synchronously from the roots this
// candidate's own BOCs parse back to — the identical combined serialization plus
// the identical LZ4 — timed with the parse outside the timer. It is measured in
// the same process, on the same cells, and it is the honest zero-overlap
// baseline: a capsule started after both components are written has exactly that
// much left to do when delivery arrives.
//
// The comparison is between the fastest run of each arm rather than the mean.
// Timing noise here is one-sided — it only ever adds — so the minimum is the
// least contaminated estimate of both, and the reference arm gets the friendlier
// conditions of the two: it runs alone, while the capsule it is compared against
// shares its head start with the collated serialization it now overlaps. The
// test is conservative in that direction on purpose.
//
// Delivery must also be handed the same bytes it would have serialized itself.
// The hint that presizes the capsule changed with its start — it can no longer
// be read out of two finished BOC headers — so the payloads are compared, not
// just the timings.
func TestBroadcastCapsuleIsAheadWhenDeliveryAsksForIt(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 1)
	builder := testBuilder()

	const runs = 5
	waits := make([]time.Duration, 0, runs)
	alone := make([]time.Duration, 0, runs)
	for run := range runs {
		candidate, err := builder.BuildShard(t.Context(), req)
		if err != nil {
			t.Fatalf("collate the mainnet fixture with full collated data: %v", err)
		}
		if run == 0 {
			assertBenchMainnetCollated(t, candidate)
		}
		broadcast := broadcastCandidate(candidate)

		// Exactly what delivery does: encodeForBroadcast hands the capsule the
		// candidate it belongs to and blocks until the payload is there.
		asked := time.Now()
		delivered, err := simplex.SerializeCandidateForBroadcastPrepared(broadcast, candidate.prepared)
		if err != nil {
			t.Fatalf("serialize the candidate from its capsule: %v", err)
		}
		waits = append(waits, time.Since(asked))

		blockRoot, err := cell.FromBOC(candidate.BlockBOC)
		if err != nil {
			t.Fatal(err)
		}
		collatedRoots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
		if err != nil {
			t.Fatal(err)
		}
		started := time.Now()
		reference, err := simplex.PrepareCandidate(
			candidate.ID.SeqNo,
			blockRoot,
			collatedRoots,
			[32]byte(candidate.ID.FileHash),
			candidate.CollatedFileHash,
			simplex.PayloadCellHint(candidate.BlockBOC, candidate.CollatedData),
		)
		if err != nil {
			t.Fatalf("build the reference payload: %v", err)
		}
		alone = append(alone, time.Since(started))

		expected, err := simplex.SerializeCandidateForBroadcastPrepared(broadcast, reference)
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(delivered.Data, expected.Data) {
			t.Fatalf("run %d: the capsule delivered %d payload bytes, want the %d the BOCs serialize to",
				run, len(delivered.Data), len(expected.Data))
		}
	}

	wait, zeroOverlap := slices.Min(waits), slices.Min(alone)
	t.Logf("delivery waited %.3f ms for a capsule that a standing start costs %.3f ms "+
		"(best of %d; every wait %v, every standing start %v)",
		float64(wait.Microseconds())/1000, float64(zeroOverlap.Microseconds())/1000, runs, waits, alone)
	if wait >= zeroOverlap {
		t.Fatalf(
			"delivery waited %v, as much as building the payload from a standing start (%v): "+
				"the capsule made no progress before delivery asked for it",
			wait, zeroOverlap,
		)
	}
}
