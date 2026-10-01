package blockstats

import (
	"sync"
	"testing"
)

func TestAccumulatorStartsEmptyAndResetsWithNewInstance(t *testing.T) {
	first := New()
	first.ObserveCollation(true, true)
	first.ObserveValidation(false, false)

	if got := first.BlockStats(); got.Collated.Master.OK != 1 || got.Validated.Shard.Error != 1 {
		t.Fatalf("first snapshot = %#v", got)
	}

	if got := New().BlockStats(); got != (Snapshot{}) {
		t.Fatalf("new accumulator snapshot = %#v, want zero counters", got)
	}
}

func TestAccumulatorIsSafeForConcurrentSnapshots(t *testing.T) {
	stats := New()
	const workers = 16
	const observationsPerWorker = 1_000

	var work sync.WaitGroup
	for worker := range workers {
		work.Go(func() {
			for range observationsPerWorker {
				stats.ObserveCollation(worker%2 == 0, true)
				stats.ObserveValidation(worker%2 != 0, false)
				_ = stats.BlockStats()
			}
		})
	}
	work.Wait()

	got := stats.BlockStats()
	if got.Collated.Master.OK+got.Collated.Shard.OK != workers*observationsPerWorker {
		t.Fatalf("collation total = %#v", got.Collated)
	}
	if got.Validated.Master.Error+got.Validated.Shard.Error != workers*observationsPerWorker {
		t.Fatalf("validation total = %#v", got.Validated)
	}
}
