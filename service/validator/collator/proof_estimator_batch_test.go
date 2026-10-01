package collator

import (
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestProofEstimatorBatchMatchesIndividualCharges(t *testing.T) {
	cells := proofEstimatorFixture(t)
	batch := newProofSizeEstimator(len(cells))
	reference := &referenceProofSizeEstimator{}

	for pass := range 4 {
		start := pass * 19 % len(cells)
		rotated := append(append(make([]*cell.Cell, 0, len(cells)), cells[start:]...), cells[:start]...)

		// Selection-only observations can arrive on either side of the read-set
		// callback. The batched path must preserve both orders and duplicate
		// handling exactly.
		for i := pass; i < len(rotated); i += 9 {
			batch.addExecutionRead(rotated[i])
			reference.addExecutionRead(rotated[i])
		}
		batch.addLoadedCells(rotated)
		for _, loaded := range rotated {
			reference.addLoadedCell(loaded)
		}
		for i := pass + 3; i < len(rotated); i += 11 {
			batch.addExecutionRead(rotated[i])
			reference.addExecutionRead(rotated[i])
		}
	}

	assertProofEstimatorsAgree(t, batch, reference)
}

func TestProofEstimatorBatchAndIndividualChargesAreConcurrent(t *testing.T) {
	cells := proofEstimatorFixture(t)
	reference := &referenceProofSizeEstimator{}
	for _, loaded := range cells {
		reference.addLoadedCell(loaded)
	}

	for attempt := range 8 {
		estimator := newProofSizeEstimator(len(cells))
		var wait sync.WaitGroup
		for worker := range 12 {
			wait.Go(func() {
				start := (worker*17 + attempt*23) % len(cells)
				rotated := append(append(make([]*cell.Cell, 0, len(cells)), cells[start:]...), cells[:start]...)
				if worker%3 == 0 {
					for _, loaded := range rotated {
						estimator.addLoadedCell(loaded)
					}
					return
				}
				estimator.addLoadedCells(rotated)
			})
		}
		wait.Wait()

		assertProofEstimatorsAgree(t, estimator, reference)
	}
}

func proofEstimatorBenchmarkCells(tb testing.TB, leaves int) []*cell.Cell {
	tb.Helper()

	leafCells := make([]*cell.Cell, 0, leaves)
	for i := range leaves {
		leafCells = append(leafCells, cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell())
	}
	parents := make([]*cell.Cell, 0, leaves/2)
	for i := 0; i < len(leafCells); i += 2 {
		parents = append(parents, cell.BeginCell().
			MustStoreUInt(uint64(i), 24).
			MustStoreRef(leafCells[i]).
			MustStoreRef(leafCells[i+1]).
			EndCell())
	}
	// Parents first makes the later leaf loads promote charged boundaries, the
	// same shape as a proof traversal descending from the root.
	return append(parents, leafCells...)
}

func BenchmarkProofSizeEstimatorLoadedCells(b *testing.B) {
	cells := proofEstimatorBenchmarkCells(b, 4096)

	for _, mode := range []string{"individual", "batch"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				estimator := newProofSizeEstimator(len(cells))
				if mode == "batch" {
					estimator.addLoadedCells(cells)
					continue
				}
				for _, loaded := range cells {
					estimator.addLoadedCell(loaded)
				}
			}
		})
	}
}

// BenchmarkClaimedPrefixRecordMany isolates the production call site. The base
// read set and estimator are populated outside the timer, so the timed region is
// exactly the cached processed-prefix cells being handed back after the state
// update, with the same duplicate ratio and resident table sizes as collation.
func BenchmarkClaimedPrefixRecordMany(b *testing.B) {
	fixture := collateToValidationClosure(b, benchMainnetCollatedRequest(b, 3))
	if !fixture.claimedPrefix.serves(fixture.oldOutQueue, fixture.shard, fixture.processedClaim) {
		b.Fatal("fixture does not retain a processed-prefix traversal")
	}
	base := append([]*cell.Cell(nil), fixture.usage.Cells()...)
	prefix := append([]*cell.Cell(nil), fixture.claimedPrefix.cells...)
	if len(prefix) == 0 {
		b.Fatal("fixture retained no processed-prefix cells")
	}

	for _, mode := range []string{"individual", "readset_batch", "full_batch"} {
		b.Run(mode, func(b *testing.B) {
			b.ReportAllocs()
			b.ReportMetric(float64(len(prefix)), "cells/op")
			b.ResetTimer()
			for b.Loop() {
				b.StopTimer()
				usage := cell.NewReadSetSized(fixture.usage.Source(), len(base)+len(prefix))
				estimator := newProofSizeEstimator(len(base) + len(prefix))
				usage.SetRecordCallback(estimator.addLoadedCell)
				if mode == "full_batch" {
					usage.SetRecordManyCallback(estimator.addLoadedCells)
				}
				usage.RecordMany(base)
				b.StartTimer()

				if mode != "individual" {
					usage.RecordMany(prefix)
					continue
				}
				for _, loaded := range prefix {
					usage.Record(loaded)
				}
			}
			b.ReportMetric(float64(len(prefix)), "cells/op")
		})
	}
}
