package service

import (
	"encoding/binary"
	"fmt"
	"runtime"
	"sync"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func stageTestRecords(offset, count int) storage.StateCellRecords {
	records := make([]storage.EncodedCellRecord, count)
	for i := range records {
		id := offset + i
		binary.BigEndian.PutUint64(records[i].Hash[24:], uint64(id))
		// Payload must be a function of the hash, so re-staging the same cell
		// carries identical data and folding it deduplicates instead of
		// replacing.
		records[i].Data = []byte{0x01, byte(id), byte(id >> 8)}
	}
	return storage.NewStateCellRecords(records)
}

func stageTestHash(id int) cell.Hash {
	var hash cell.Hash
	binary.BigEndian.PutUint64(hash[24:], uint64(id))
	return hash
}

func stagedRecordData(t *testing.T, cache *stateCellEncodedCache, hash cell.Hash) []byte {
	t.Helper()

	chunks, _ := cache.appendRecordChunks(nil)
	// Later chunks shadow earlier ones, matching the newest-wins load order.
	var data []byte
	for _, chunk := range chunks {
		for _, record := range chunk {
			if record.Hash == hash {
				data = record.Data
			}
		}
	}
	return data
}

// TestStageRecordsWithoutPrewriterKeepsWindowState pins that skipping the
// prewrite request when no prewriter is attached changes nothing observable:
// the window still absorbs every record and the request stays empty.
func TestStageRecordsWithoutPrewriterKeepsWindowState(t *testing.T) {
	cache := newStateCellEncodedCache(16)

	request := cache.stageRecords(stageTestRecords(0, 500), nil)
	if request.cache != nil || request.prewriter != nil || !request.records.Empty() {
		t.Fatalf("request without prewriter carries %d records, want an empty request", request.records.Len())
	}
	if cache.len() != 500 {
		t.Fatalf("window records = %d, want 500", cache.len())
	}
	if cache.prewritePending != 0 {
		t.Fatalf("prewritePending = %d, want 0", cache.prewritePending)
	}
	if len(stagedRecordData(t, cache, stageTestHash(499))) == 0 {
		t.Fatal("window lost a staged record on the nil-prewriter path")
	}
}

// TestStageRecordsPublishesLayerWithoutRebuildingIndex pins the O(1) staging
// contract: the records land as one immutable layer, the base map is untouched
// until a fold, and the whole set is handed to the prewriter.
func TestStageRecordsPublishesLayerWithoutRebuildingIndex(t *testing.T) {
	cache := newStateCellEncodedCache(16)
	prewriter := &stateCellPrewriter{}

	records := stageTestRecords(0, 300)
	request := cache.stageRecords(records, prewriter)
	if request.records.Len() != records.Len() {
		t.Fatalf("prewrite = %d records, want the whole staged set of %d", request.records.Len(), records.Len())
	}
	if cache.prewritePending != 1 {
		t.Fatalf("prewritePending after stage = %d, want 1", cache.prewritePending)
	}
	if got := cache.stagedLayers(); got != 1 {
		t.Fatalf("staged layers = %d, want 1", got)
	}
	if got := len(cache.records); got != 0 {
		t.Fatalf("base records = %d, want 0 before a fold", got)
	}
	if cache.len() != 300 {
		t.Fatalf("window records = %d, want 300", cache.len())
	}
	if want := records.ByteSize(); cache.byteSize() != want {
		t.Fatalf("window bytes = %d, want %d", cache.byteSize(), want)
	}
	cache.completePrewrite(7, nil)

	if token, ok := cache.prewriteTarget(); !ok || token != 7 {
		t.Fatalf("prewrite target = (%d, %v), want (7, true)", token, ok)
	}
}

// TestStageRecordsResolvesNewestLayerFirst pins that a re-staged hash resolves
// to its newest encoding, which is what in-place replacement used to give.
func TestStageRecordsResolvesNewestLayerFirst(t *testing.T) {
	cache := newStateCellEncodedCache(16)
	hash := stageTestHash(1)

	cache.stageRecords(storage.NewStateCellRecords([]storage.EncodedCellRecord{{Hash: hash, Data: []byte{0x01}}}), nil)
	cache.stageRecords(storage.NewStateCellRecords([]storage.EncodedCellRecord{{Hash: hash, Data: []byte{0x02}}}), nil)

	if got := stagedRecordData(t, cache, hash); len(got) != 1 || got[0] != 0x02 {
		t.Fatalf("checkpoint chunk order resolved %x, want the newest encoding 02", got)
	}

	// Folding must preserve the same answer once the layers are gone.
	cache.foldLayers(2, 1<<20)
	if got := cache.stagedLayers(); got != 0 {
		t.Fatalf("staged layers after fold = %d, want 0", got)
	}
	if got := stagedRecordData(t, cache, hash); len(got) != 1 || got[0] != 0x02 {
		t.Fatalf("folded record = %x, want the newest encoding 02", got)
	}
}

// TestFoldLayersDeduplicatesAndKeepsByteAccounting pins the compaction path:
// re-staged identical records collapse into one base entry and the window's
// byte budget follows the folded state, not the staged duplicates.
func TestFoldLayersDeduplicatesAndKeepsByteAccounting(t *testing.T) {
	cache := newStateCellEncodedCache(16)
	records := stageTestRecords(0, 400)
	single := records.ByteSize()

	cache.stageRecords(records, nil)
	cache.stageRecords(stageTestRecords(0, 400), nil)
	if got := cache.byteSize(); got != 2*single {
		t.Fatalf("staged bytes = %d, want both layers counted (%d)", got, 2*single)
	}

	// A budget below one layer must leave folding owed rather than spinning.
	folded, owed := cache.foldLayers(2, 10)
	if folded != 0 || !owed {
		t.Fatalf("bounded fold = (%d, %v), want (0, true)", folded, owed)
	}
	if got := cache.stagedLayers(); got != 2 {
		t.Fatalf("staged layers after bounded fold = %d, want 2", got)
	}

	for {
		if _, owed = cache.foldLayers(2, 64); !owed {
			break
		}
	}
	if got := cache.stagedLayers(); got != 0 {
		t.Fatalf("staged layers after fold = %d, want 0", got)
	}
	if got := cache.len(); got != 400 {
		t.Fatalf("folded records = %d, want the deduplicated 400", got)
	}
	if got := cache.byteSize(); got != single {
		t.Fatalf("folded bytes = %d, want the deduplicated %d", got, single)
	}
	if len(stagedRecordData(t, cache, stageTestHash(399))) == 0 {
		t.Fatal("folding lost a staged record")
	}
}

// TestFoldLayersResumesPartiallyFoldedLayer pins that a layer folded in slices
// stays fully resolvable while it is half folded and does not lose or double
// count records when the fold resumes.
func TestFoldLayersResumesPartiallyFoldedLayer(t *testing.T) {
	cache := newStateCellEncodedCache(16)
	records := stageTestRecords(0, 200)
	cache.stageRecords(records, nil)

	if folded, owed := cache.foldLayers(1, 50); folded != 0 || !owed {
		t.Fatalf("first fold slice = (%d, %v), want (0, true)", folded, owed)
	}
	// Half folded: the layer still shadows its folded prefix, so every record
	// must resolve and the total must not double count.
	if got := cache.len(); got != 200+50 {
		t.Fatalf("half-folded record count = %d, want the base prefix plus the layer", got)
	}
	for _, id := range []int{0, 49, 50, 199} {
		if len(stagedRecordData(t, cache, stageTestHash(id))) == 0 {
			t.Fatalf("record %d is not resolvable while half folded", id)
		}
	}

	for {
		if _, owed := cache.foldLayers(1, 50); !owed {
			break
		}
	}
	if got := cache.len(); got != 200 {
		t.Fatalf("folded record count = %d, want 200", got)
	}
	if got := cache.byteSize(); got != records.ByteSize() {
		t.Fatalf("folded bytes = %d, want %d", got, records.ByteSize())
	}
}

// TestStagedLayersStayVisibleAcrossCheckpoint pins that a checkpoint taken
// while layers are unfolded still carries their records, and that the window
// keeps resolving them until the checkpoint completes.
func TestStagedLayersStayVisibleAcrossCheckpoint(t *testing.T) {
	window := newTestStateCellWindowCache(nil)
	root := cell.BeginCell().MustStoreUInt(0x4242, 16).EndCell()
	hash := root.HashKey()

	wait, err := window.stagePreparedRecords(mustPreparedReachableStateCells(t, root))
	if err != nil {
		t.Fatalf("stage prepared records: %v", err)
	}
	if err = wait(); err != nil {
		t.Fatalf("drain prewrite backpressure: %v", err)
	}
	if got := window.activeStagedLayers(); got != 1 {
		t.Fatalf("staged layers = %d, want 1", got)
	}

	checkpoint := window.beginCheckpoint()
	if !hasCellRecord(checkpoint.records(), hash) {
		t.Fatal("checkpoint does not carry the unfolded staged layer")
	}
	if _, err = window.loader()(hash); err != nil {
		t.Fatalf("load staged record through the window after checkpoint: %v", err)
	}

	checkpoint.complete()
	if _, err = window.loader()(hash); err == nil {
		t.Fatal("completed checkpoint left the staged layer resolvable")
	}
}

// TestCompactStagedLayersFoldsWindow pins the commit-side compaction: staging
// through the window and compacting leaves the records in the base map with no
// layers behind, which is the memory profile the single map used to have.
func TestCompactStagedLayersFoldsWindow(t *testing.T) {
	window := newTestStateCellWindowCache(nil)
	for i := 0; i < 4; i++ {
		if err := window.addPreparedRecords(stageTestRecords(i*100, 100)); err != nil {
			t.Fatalf("add prepared records: %v", err)
		}
	}

	if got := window.activeStagedLayers(); got != 0 {
		t.Fatalf("staged layers after commit-side staging = %d, want 0", got)
	}
	if got := window.active.len(); got != 400 {
		t.Fatalf("folded records = %d, want 400", got)
	}
	for _, id := range []int{0, 150, 399} {
		if len(stagedRecordData(t, window.active, stageTestHash(id))) == 0 {
			t.Fatalf("folded record %d is not in the window", id)
		}
	}
}

// TestLayeredWindowResolvesDependentApplyChain is the load-bearing L1 test: a
// chain of applied blocks whose states keep referencing a subtree staged by the
// first block. Every later block prunes that subtree, so applying it can only
// succeed by resolving it out of an older staged layer, and the base loader
// rejects everything. The chain is folded mid-way, so the second half exercises
// the same resolution after compaction moved the records into the base map.
func TestLayeredWindowResolvesDependentApplyChain(t *testing.T) {
	const blocks = 8

	stableLeaf := cell.BeginCell().MustStoreUInt(0xa1, 8).EndCell()
	stable := cell.BeginCell().MustStoreRef(stableLeaf).EndCell()
	states := make([]*cell.Cell, blocks+1)
	states[0] = cell.BeginCell().MustStoreUInt(0xb0, 8).EndCell()
	for i := 1; i <= blocks; i++ {
		change := cell.BeginCell().MustStoreUInt(uint64(0xc0+i), 8).EndCell()
		states[i] = cell.BeginCell().
			MustStoreUInt(uint64(0xb0+i), 8).
			MustStoreRef(stable).
			MustStoreRef(change).
			EndCell()
	}

	var baseLoads int
	window := newTestStateCellWindowCache(func(hash cell.Hash) (*cell.Cell, error) {
		baseLoads++
		return nil, fmt.Errorf("unexpected base load %x", hash[:])
	})

	previous := states[0]
	for i := 1; i <= blocks; i++ {
		// The first block introduces the stable subtree in full; every later
		// block prunes it, so its cells must come from block 1's layer.
		to := states[i]
		if i > 1 {
			to = mustProofBody(t, states[i], 1)
		}
		update := mustMerkleUpdateCell(t, mustProofBodyForApply(t, previous, i), to)
		prepared := mustPreparedStateUpdateCells(t, update)
		// The premise of the test: from the second block on, the carried
		// subtree is absent from the block's own cells, so the apply can only
		// get it from an earlier block's staged records.
		if got := prepared.Has(stable.HashKey()); got != (i == 1) {
			t.Fatalf("block %d prepared cells contain the stable subtree = %v, want %v", i, got, i == 1)
		}

		applied, err := window.applyMerkleUpdate([]*storage.BlockState{{Cell: previous}}, update, prepared)
		if err != nil {
			t.Fatalf("apply block %d through the layered window: %v", i, err)
		}
		if applied.NextRoot.HashKey() != states[i].HashKey() {
			t.Fatalf("block %d applied root = %x, want %x", i, applied.NextRoot.HashKey(), states[i].HashKey())
		}

		// The dependent must be able to walk into the carried-over subtree.
		loadedStable, err := applied.NextRoot.PeekRef(0)
		if err != nil {
			t.Fatalf("peek carried subtree at block %d: %v", i, err)
		}
		if loadedStable, err = loadedStable.Prewarm(); err != nil {
			t.Fatalf("materialize carried subtree at block %d: %v", i, err)
		}
		if loadedStable.HashKey() != stable.HashKey() {
			t.Fatalf("carried subtree at block %d = %x, want %x", i, loadedStable.HashKey(), stable.HashKey())
		}

		previous = applied.NextRoot
		if i == blocks/2 {
			// The commit-side fold: everything staged so far moves into the
			// base map while the chain keeps going.
			window.compactStagedLayers(0)
			if got := window.activeStagedLayers(); got != 0 {
				t.Fatalf("staged layers after mid-chain fold = %d, want 0", got)
			}
		}
	}

	if baseLoads != 0 {
		t.Fatalf("base loader calls = %d, want 0: the window must resolve the whole chain", baseLoads)
	}

	// After a full fold the same chain is still resolvable, and the checkpoint
	// carries every block's cells exactly once.
	window.compactStagedLayers(0)
	loaded, err := window.loader()(states[blocks].HashKey())
	if err != nil {
		t.Fatalf("reload head state after fold: %v", err)
	}
	if loaded.HashKey() != states[blocks].HashKey() {
		t.Fatalf("reloaded head = %x, want %x", loaded.HashKey(), states[blocks].HashKey())
	}

	records := window.beginCheckpoint().records()
	seen := map[cell.Hash]int{}
	for _, record := range records {
		seen[record.Hash]++
	}
	if seen[stable.HashKey()] != 1 {
		t.Fatalf("checkpoint carries the stable subtree %d times, want once", seen[stable.HashKey()])
	}
	for i := 1; i <= blocks; i++ {
		if seen[states[i].HashKey()] != 1 {
			t.Fatalf("checkpoint carries block %d state %d times, want once", i, seen[states[i].HashKey()])
		}
	}
}

// TestLayeredWindowConcurrentStageLoadAndFold runs the three roles that share
// the window — appliers staging layers, loaders resolving cells, and the commit
// goroutine folding — against each other, which is the arrangement the layer
// design has to be safe under. Run with -race.
func TestLayeredWindowConcurrentStageLoadAndFold(t *testing.T) {
	const (
		stagers        = 8
		blocksPerStage = 24
	)

	window := newTestStateCellWindowCache(nil)
	roots := make([][]*cell.Cell, stagers)
	for stager := range roots {
		roots[stager] = make([]*cell.Cell, blocksPerStage)
		for block := range roots[stager] {
			leaf := cell.BeginCell().MustStoreUInt(uint64(stager*blocksPerStage+block), 32).EndCell()
			roots[stager][block] = cell.BeginCell().MustStoreUInt(0xfe, 8).MustStoreRef(leaf).EndCell()
		}
	}

	staged := make(chan cell.Hash, stagers*blocksPerStage)
	done := make(chan struct{})
	var stageWG, readerWG sync.WaitGroup

	for stager := 0; stager < stagers; stager++ {
		stageWG.Add(1)
		go func() {
			defer stageWG.Done()
			for _, root := range roots[stager] {
				wait, err := window.stagePreparedRecords(mustPreparedReachableStateCells(t, root))
				if err != nil {
					t.Errorf("stage prepared records: %v", err)
					return
				}
				if err = wait(); err != nil {
					t.Errorf("drain prewrite backpressure: %v", err)
					return
				}
				staged <- root.HashKey()
			}
		}()
	}

	// Loaders: every hash handed over must stay resolvable no matter which
	// layer it is in, whether it is half folded, or already in the base map.
	for loader := 0; loader < 4; loader++ {
		readerWG.Add(1)
		go func() {
			defer readerWG.Done()
			load := window.loader()
			for {
				select {
				case hash := <-staged:
					loaded, err := load(hash)
					if err != nil {
						t.Errorf("load staged root %x: %v", hash[:], err)
						return
					}
					if loaded.HashKey() != hash {
						t.Errorf("loaded root %x, want %x", loaded.HashKey(), hash[:])
						return
					}
				case <-done:
					return
				}
			}
		}()
	}

	folder := make(chan struct{})
	go func() {
		defer close(folder)
		for {
			select {
			case <-done:
				return
			default:
			}
			window.compactStagedLayers(0)
			runtime.Gosched()
		}
	}()

	stageWG.Wait()
	close(done)
	readerWG.Wait()
	<-folder

	window.compactStagedLayers(0)
	if got := window.activeStagedLayers(); got != 0 {
		t.Fatalf("staged layers after the final fold = %d, want 0", got)
	}
	load := window.loader()
	for stager := range roots {
		for _, root := range roots[stager] {
			if _, err := load(root.HashKey()); err != nil {
				t.Fatalf("load staged root after concurrent folding: %v", err)
			}
		}
	}
}

// mustProofBodyForApply returns the previous state in the shape the next merkle
// update is built against: the first update starts from the plain state cell,
// later ones from the pruned body the previous update produced.
func mustProofBodyForApply(tb testing.TB, state *cell.Cell, block int) *cell.Cell {
	tb.Helper()

	if block <= 1 {
		return state
	}
	return mustProofBody(tb, state, 1)
}

// TestStagePreparedRecordsFoldsBeyondLayerLimit pins the applier-side valve:
// a pipeline that never commit-folds still cannot accumulate layers without
// bound.
func TestStagePreparedRecordsFoldsBeyondLayerLimit(t *testing.T) {
	window := newTestStateCellWindowCache(nil)

	for i := 0; i <= stateCellWindowMaxStagedLayers+4; i++ {
		wait, err := window.stagePreparedRecords(stageTestRecords(i*10, 10))
		if err != nil {
			t.Fatalf("stage prepared records: %v", err)
		}
		if err = wait(); err != nil {
			t.Fatalf("drain prewrite backpressure: %v", err)
		}
	}

	if got := window.activeStagedLayers(); got > stateCellWindowMaxStagedLayers {
		t.Fatalf("staged layers = %d, want at most %d", got, stateCellWindowMaxStagedLayers)
	}
	if len(stagedRecordData(t, window.active, stageTestHash(0))) == 0 {
		t.Fatal("the folded overflow layer lost its records")
	}
}
