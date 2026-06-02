package service

import (
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestShouldSwitchNextToArchiveByLagUsesSwitchThreshold(t *testing.T) {
	if shouldSwitchNextToArchiveByLag(nextToArchiveLagSeconds) {
		t.Fatal("archive catch-up should not start at the switch boundary")
	}

	if !shouldSwitchNextToArchiveByLag(nextToArchiveLagSeconds + 1) {
		t.Fatal("archive catch-up should start above the switch boundary")
	}
}

func TestShouldSwitchArchiveToNextUsesLiveTailLag(t *testing.T) {
	if !shouldSwitchArchiveToNextByLag(archiveToNextLagSeconds - 1) {
		t.Fatal("archive catch-up should switch below the live tail lag")
	}
	if shouldSwitchArchiveToNextByLag(archiveToNextLagSeconds) {
		t.Fatal("archive catch-up should continue at the live tail boundary")
	}
}

func TestArchiveCatchUpTargetByLagStartsAboveNextToArchiveThreshold(t *testing.T) {
	current := &storage.CurrentState{
		ShardClientSeqno: 1000,
		Masterchain: storage.BlockState{
			Block: ton.BlockIDExt{SeqNo: 1000},
		},
	}

	archiveTarget, ok := archiveCatchUpTargetByLag(current, nextToArchiveLagSeconds+1)
	if !ok {
		t.Fatal("archive catch-up should start above the next-to-archive threshold")
	}

	want := ^uint32(0)
	if archiveTarget.SeqNo != want {
		t.Fatalf("archive target seqno mismatch: got %d want %d", archiveTarget.SeqNo, want)
	}
}

func TestArchiveCatchUpTargetByLagDoesNotGuessTargetFromRemainingLag(t *testing.T) {
	current := &storage.CurrentState{
		ShardClientSeqno: 1000,
		Masterchain: storage.BlockState{
			Block: ton.BlockIDExt{SeqNo: 1000},
		},
	}

	remaining := int64(12_345)
	archiveTarget, ok := archiveCatchUpTargetByLag(current, archiveToNextLagSeconds+remaining)
	if !ok {
		t.Fatal("archive catch-up should start for large masterchain lag")
	}

	want := ^uint32(0)
	if archiveTarget.SeqNo != want {
		t.Fatalf("archive target seqno mismatch: got %d want %d", archiveTarget.SeqNo, want)
	}
}

func TestArchivePipelineSchedulingUsesPendingWindowLimit(t *testing.T) {
	runner := &archiveCatchUpRunner{
		service: &Service{archiveCatchUpPrefetchWindows: 2},
		target:  testMasterBlockID(1000),
	}
	planning := &storage.CurrentState{ShardClientSeqno: 100}

	if !runner.canScheduleArchiveWindow(planning, 0, runner.target.SeqNo) {
		t.Fatal("pipeline should schedule the first window immediately")
	}

	if runner.canScheduleArchiveWindow(planning, 2, runner.target.SeqNo) {
		t.Fatal("pipeline should stop scheduling at the pending window limit")
	}

	planning.ShardClientSeqno = runner.target.SeqNo
	if runner.canScheduleArchiveWindow(planning, 0, runner.target.SeqNo) {
		t.Fatal("pipeline should stop scheduling at target")
	}
}

func TestArchivePipelineSchedulingStopsNearLiveTail(t *testing.T) {
	runner := &archiveCatchUpRunner{
		service: &Service{archiveCatchUpPrefetchWindows: 2},
		target:  testMasterBlockID(1000),
	}
	nowUnix := time.Now().Unix()
	planning := &storage.CurrentState{
		ShardClientSeqno: 100,
		Masterchain: storage.BlockState{
			Parsed: &tlb.ShardStateUnsplit{GenUTime: uint32(nowUnix - archiveToNextLagSeconds + 1)},
		},
	}

	if runner.canScheduleArchiveWindow(planning, 0, runner.target.SeqNo) {
		t.Fatal("pipeline should stop scheduling archive windows below the live-tail lag")
	}
}

func TestArchivePipelinePendingWindowLimitUsesConfiguredPrefetch(t *testing.T) {
	runner := &archiveCatchUpRunner{service: &Service{archiveCatchUpPrefetchWindows: 2}}
	if got := runner.archivePendingWindowLimit(); got != 2 {
		t.Fatalf("pending window limit = %d, want 2", got)
	}
}

func TestArchivePipelinePendingWindowLimitUsesDefaultPrefetch(t *testing.T) {
	runner := &archiveCatchUpRunner{service: &Service{}}
	if got := runner.archivePendingWindowLimit(); got != DefaultArchiveCatchUpPrefetchWindows {
		t.Fatalf("pending window limit = %d, want %d", got, DefaultArchiveCatchUpPrefetchWindows)
	}
}

func TestArchivePipelineReadyWindowBacklog(t *testing.T) {
	runner := &archiveCatchUpRunner{service: &Service{}}

	if runner.archiveReadyWindowBacklog() != archiveReadyWindowBacklog {
		t.Fatalf("ready window backlog = %d, want %d", runner.archiveReadyWindowBacklog(), archiveReadyWindowBacklog)
	}

	if !runner.shouldEmitReadyArchiveWindow([]archivePendingWindow{testReadyArchiveWindow(nil)}) {
		t.Fatal("pipeline should emit the first ready window")
	}

	pending := make([]archivePendingWindow, archiveReadyWindowBacklog)
	for i := range pending {
		pending[i] = testReadyArchiveWindow(nil)
	}
	if !runner.shouldEmitReadyArchiveWindow(pending) {
		t.Fatal("pipeline should emit when ready backlog is filled")
	}
}

func TestArchivePipelineReadyWindowErrorEmitsImmediately(t *testing.T) {
	runner := &archiveCatchUpRunner{service: &Service{}}
	if !runner.shouldEmitReadyArchiveWindow([]archivePendingWindow{testReadyArchiveWindow(errors.New("boom"))}) {
		t.Fatal("pipeline should emit ready window errors immediately")
	}
}

func TestArchiveCheckpointBackpressureWaitsAtConfiguredWindowTarget(t *testing.T) {
	runner := &archiveCatchUpRunner{
		service:                &Service{archiveCatchUpCheckpointBlocks: 2000},
		checkpointDone:         make(chan archiveCheckpointResult),
		checkpointBlocksTarget: 2000,
		lastCheckpointSeqno:    1000,
		current:                &storage.CurrentState{ShardClientSeqno: 8999},
	}
	if runner.archiveCheckpointBackpressureBlocks() != 8000 {
		t.Fatalf("checkpoint backpressure blocks = %d, want 8000", runner.archiveCheckpointBackpressureBlocks())
	}
	if runner.shouldWaitArchiveCheckpointBackpressure() {
		t.Fatal("checkpoint backpressure should not wait below configured window target")
	}

	runner.current.ShardClientSeqno = 9000
	if !runner.shouldWaitArchiveCheckpointBackpressure() {
		t.Fatal("checkpoint backpressure should wait at configured window target")
	}
}

func TestArchiveShardPrefixesIncludeIntermediateShardChanges(t *testing.T) {
	shard := int64(-1 << 61)
	startBlocks := []ton.BlockIDExt{
		testBlockID(0, shard, 10),
	}
	stateBlocks := [][]ton.BlockIDExt{
		{testBlockID(0, shard, 11)},
		{testBlockID(0, shard, 10)},
	}

	got := archiveShardImportPlansForBlockStatesMatching(2, startBlocks, stateBlocks, nil)
	want := archive.ShardID{Workchain: 0, Shard: shard}
	if len(got) != 1 || got[0].shard != want {
		t.Fatalf("changed shard prefixes = %#v, want [%#v]", got, want)
	}
	if len(got[0].needed) != 1 || !got[0].needed[0].Equals(&stateBlocks[0][0]) {
		t.Fatalf("changed shard needed blocks = %#v, want [%s]", got[0].needed, storage.FormatBlockRef(stateBlocks[0][0]))
	}
}

func TestArchiveShardPrefixesMissingFromBlockStatesOnlyReturnsUncoveredChanges(t *testing.T) {
	shard := int64(-1 << 61)
	next := testBlockID(0, shard, 11)
	startBlocks := []ton.BlockIDExt{
		testBlockID(0, shard, 10),
	}
	stateBlocks := [][]ton.BlockIDExt{
		{next},
	}

	missing := archiveShardImportPlansMissingFromBlockStates(2, startBlocks, stateBlocks, nil)
	want := archive.ShardID{Workchain: 0, Shard: shard}
	if len(missing) != 1 || missing[0].shard != want {
		t.Fatalf("missing shard prefixes = %#v, want [%#v]", missing, want)
	}
	if len(missing[0].needed) != 1 || !missing[0].needed[0].Equals(&next) {
		t.Fatalf("missing shard needed blocks = %#v, want [%s]", missing[0].needed, storage.FormatBlockRef(next))
	}

	covered := archiveShardImportPlansMissingFromBlockStates(2, startBlocks, stateBlocks, map[storage.BlockRootHash]PreparedBlock{
		storage.BlockKey(next): {ID: next},
	})
	if len(covered) != 0 {
		t.Fatalf("covered shard prefixes = %#v, want none", covered)
	}
}

func TestValidateArchiveImportCoversPlanRequiresPlannedBlocks(t *testing.T) {
	shard := archive.ShardID{Workchain: 0, Shard: int64(-1 << 61)}
	block := testBlockID(0, shard.Shard, 11)
	plan := archiveShardImportPlan{shard: shard, needed: []ton.BlockIDExt{block}}

	err := validateArchiveImportCoversPlan(&archiveImportResult{
		blocks: map[storage.BlockRootHash]PreparedBlock{},
	}, plan)
	if err == nil {
		t.Fatal("coverage validation accepted an archive without the planned shard block")
	}

	err = validateArchiveImportCoversPlan(&archiveImportResult{
		blocks: map[storage.BlockRootHash]PreparedBlock{
			storage.BlockKey(block): {ID: block},
		},
	}, plan)
	if err != nil {
		t.Fatalf("coverage validation rejected covered archive: %v", err)
	}
}

func TestArchiveCheckpointBackpressureWaitsAtByteLimit(t *testing.T) {
	var first, second cell.Hash
	first[0] = 1
	second[0] = 2

	runner := &archiveCatchUpRunner{
		service:                &Service{archiveCatchUpCheckpointBlocks: 2000, checkpointBytes: 1024},
		checkpointDone:         make(chan archiveCheckpointResult),
		checkpointBlocksTarget: 2000,
		lastCheckpointSeqno:    1000,
		current:                &storage.CurrentState{ShardClientSeqno: 1001},
		stateCells:             newArchiveStateCellOverlay(nil),
	}
	runner.stateCells.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		first: make([]byte, 4095),
	}))
	if runner.shouldWaitArchiveCheckpointBackpressure() {
		t.Fatal("checkpoint backpressure should not wait below byte limit")
	}

	runner.stateCells.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		second: make([]byte, 1),
	}))
	if !runner.shouldWaitArchiveCheckpointBackpressure() {
		t.Fatal("checkpoint backpressure should wait at byte limit")
	}
}

func TestArchiveCheckpointPersistsAtByteTarget(t *testing.T) {
	service := &Service{
		archiveCatchUpCheckpointBlocks: 2000,
		archiveCatchUpCheckpointPeriod: time.Hour,
		checkpointBytes:                1024,
	}
	lastCheckpoint := time.Now()

	if service.shouldPersistArchiveCatchUpCheckpoint(1001, 10_000, 1000, lastCheckpoint, 2000, 1023) {
		t.Fatal("checkpoint should not persist below byte target")
	}
	if !service.shouldPersistArchiveCatchUpCheckpoint(1001, 10_000, 1000, lastCheckpoint, 2000, 1024) {
		t.Fatal("checkpoint should persist at byte target")
	}
}

func TestNextCheckpointUsesBlockAndByteTargets(t *testing.T) {
	var first, second cell.Hash
	first[0] = 1
	second[0] = 2

	runner := &nextSyncRunner{
		service:      &Service{nextBlockCheckpointBlocks: 10, checkpointBytes: 1024},
		stagedBlocks: 9,
		stateCells:   newStateCellWindowCache(nil),
	}
	runner.stateCells.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		first: make([]byte, 1023),
	}))
	if runner.shouldCheckpointStagedCurrent() {
		t.Fatal("next checkpoint should not start below block and byte targets")
	}

	runner.stateCells.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		second: make([]byte, 1),
	}))
	if !runner.shouldCheckpointStagedCurrent() {
		t.Fatal("next checkpoint should start at byte target")
	}
	if runner.shouldBackpressureStagedCurrent() {
		t.Fatal("next checkpoint should not backpressure below configured byte target")
	}

	runner.stagedBlocks = 40
	if !runner.shouldBackpressureStagedCurrent() {
		t.Fatal("next checkpoint should backpressure at configured block target")
	}
}

func TestArchiveCheckpointAdaptiveTargetUsesConfiguredBackpressureWindows(t *testing.T) {
	service := &Service{archiveCatchUpCheckpointBlocks: 2000}
	target := service.adaptArchiveCheckpointBlocks(4000, 2*time.Second)
	if target != 4000 {
		t.Fatalf("adaptive checkpoint target = %d, want 4000", target)
	}

	runner := &archiveCatchUpRunner{
		service:                service,
		checkpointBlocksTarget: target,
	}
	if runner.archiveCheckpointBackpressureBlocks() != 16000 {
		t.Fatalf("checkpoint backpressure blocks = %d, want 16000", runner.archiveCheckpointBackpressureBlocks())
	}
}

func TestArchiveAppliedStateUsesCheckpointArtifactPath(t *testing.T) {
	prev := testBlockID(0, topShard, 100)
	block := testBlockID(0, topShard, 101)
	state := &storage.BlockState{Block: block}
	meta := &storage.BlockMeta{
		ID:       block,
		PrevRefs: []ton.BlockIDExt{prev},
	}
	master := testMasterBlockID(50)
	setShardBlockMasterchainRef(meta, master)

	window := &shardClientArchiveWindow{}
	err := window.rememberAppliedArchiveState(state, PreparedBlock{
		ID:       block,
		BlockBOC: []byte{1, 2, 3},
		ProofBOC: []byte{4, 5, 6},
		Meta:     meta,
		IsLink:   true,
	}, 3)
	if err != nil {
		t.Fatal(err)
	}

	checkpoint := window.appliedStates.checkpoint()
	if len(checkpoint.entries) != 1 {
		t.Fatalf("checkpoint entries = %d, want 1", len(checkpoint.entries))
	}
	entry := checkpoint.entries[0]
	if entry.Artifact == nil {
		t.Fatal("archive applied state did not store checkpoint artifact")
	}
	if !entry.Artifact.ID.Equals(&block) {
		t.Fatalf("artifact block = %s, want %s", storage.FormatBlockRef(entry.Artifact.ID), storage.FormatBlockRef(block))
	}
	if entry.Artifact.ArchiveShardSplitDepth != 3 {
		t.Fatalf("artifact split depth = %d, want 3", entry.Artifact.ArchiveShardSplitDepth)
	}
	if entry.Artifact.Meta == nil || entry.Artifact.Meta.MasterchainRef == nil || !entry.Artifact.Meta.MasterchainRef.Equals(&master) {
		t.Fatalf("artifact master ref = %+v, want %s", entry.Artifact.Meta, storage.FormatBlockRef(master))
	}
	if len(entry.Links) != 1 || !entry.Links[0].Prev.Equals(&prev) || !entry.Links[0].Next.Equals(&block) {
		t.Fatalf("artifact links = %+v, want %s -> %s", entry.Links, storage.FormatBlockRef(prev), storage.FormatBlockRef(block))
	}
}

func testReadyArchiveWindow(err error) archivePendingWindow {
	done := make(chan error, 1)
	done <- err
	return archivePendingWindow{shards: &archiveWindowShardImportTask{done: done}}
}

func TestArchiveMasterBlockSequenceStopsBeforeNonStartKeyBlock(t *testing.T) {
	start := &storage.BlockState{Block: testMasterBlockID(10)}
	blocks := map[uint32]PreparedBlock{
		11: testArchiveMasterBlock(11, false),
		12: testArchiveMasterBlock(12, true),
	}

	sequence, err := archiveMasterBlockSequence(start, 20, 11, blocks)
	if err != nil {
		t.Fatal(err)
	}
	if len(sequence) != 1 || sequence[0].ID.SeqNo != 11 {
		t.Fatalf("sequence should stop before key block after start, got %v", sequence)
	}
}

func TestArchiveMasterBlockSequenceAllowsStartKeyBlock(t *testing.T) {
	start := &storage.BlockState{Block: testMasterBlockID(10)}
	blocks := map[uint32]PreparedBlock{
		11: testArchiveMasterBlock(11, true),
		12: testArchiveMasterBlock(12, false),
	}

	sequence, err := archiveMasterBlockSequence(start, 20, 11, blocks)
	if err != nil {
		t.Fatal(err)
	}
	if len(sequence) != 2 || sequence[0].ID.SeqNo != 11 || sequence[1].ID.SeqNo != 12 {
		t.Fatalf("sequence should include key block at window start, got %v", sequence)
	}
}

func testArchiveMasterBlock(seqno uint32, keyBlock bool) PreparedBlock {
	block := testMasterBlockID(seqno)
	meta := &storage.BlockMeta{ID: block}
	if keyBlock {
		meta.Mark(storage.BlockMetaIsKeyBlock)
	}
	return PreparedBlock{ID: block, Meta: meta}
}

func testMasterBlockID(seqno uint32) ton.BlockIDExt {
	return testBlockID(-1, topShard, seqno)
}
