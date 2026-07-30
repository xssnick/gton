package archive

import (
	"context"
	"testing"
	"time"
)

func TestImportedBlockPrepareSlotsReserveHotCapacity(t *testing.T) {
	if importedBlockPrepareHotReserve() == 0 {
		t.Skip("single prepare slot: no hot reserve on this machine")
	}
	if total := cap(importedBlockPrepareSharedSlots) + cap(importedBlockPrepareHotSlots); total != importedBlockPrepareParallelism() {
		t.Fatalf("prepare slots shared=%d hot=%d, want total %d",
			cap(importedBlockPrepareSharedSlots), cap(importedBlockPrepareHotSlots), importedBlockPrepareParallelism())
	}

	// Occupy every shared slot to model a full house of prefetch prepares.
	for i := 0; i < cap(importedBlockPrepareSharedSlots); i++ {
		importedBlockPrepareSharedSlots <- struct{}{}
	}
	defer func() {
		for i := 0; i < cap(importedBlockPrepareSharedSlots); i++ {
			<-importedBlockPrepareSharedSlots
		}
	}()

	hotCtx, cancelHot := context.WithCancel(WithHotImportPrepare(context.Background()))
	defer cancelHot()
	hot := &importedBlockPreparer{ctx: hotCtx, cancel: cancelHot, hot: hotImportPrepare(hotCtx)}
	if !hot.hot {
		t.Fatal("WithHotImportPrepare did not mark the preparer hot")
	}
	release, err := hot.acquireSlot()
	if err != nil {
		t.Fatalf("hot prepare could not take a reserved slot: %v", err)
	}
	release()

	prefetchCtx, cancelPrefetch := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelPrefetch()
	prefetch := &importedBlockPreparer{ctx: prefetchCtx, cancel: cancelPrefetch, hot: hotImportPrepare(prefetchCtx)}
	if _, err := prefetch.acquireSlot(); err == nil {
		t.Fatal("prefetch prepare took a hot-reserved slot")
	}
}
