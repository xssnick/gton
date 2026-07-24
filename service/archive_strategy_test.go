package service

import (
	"errors"
	"math/big"
	"testing"
	"time"

	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestArchiveCatchUpRetriesWhenPeerPoolIsEmpty(t *testing.T) {
	if !isArchiveCatchUpRetryError(p2p.ErrNoArchivePeers) {
		t.Fatal("empty archive peer pool must restart the pipeline without closing the archive session")
	}
}

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

func TestArchiveSyncUntilProgressGoalUsesCutoffTime(t *testing.T) {
	goal := archiveSyncUntilProgressGoal(1000, 700, 1200)
	if goal.kind != archiveProgressGoalSyncUntil {
		t.Fatalf("progress goal kind = %d, want sync_until", goal.kind)
	}
	if !goal.knownRemaining() {
		t.Fatal("sync_until progress should have known remaining seconds")
	}
	if goal.remainingSeconds != 300 {
		t.Fatalf("remaining seconds = %d, want 300", goal.remainingSeconds)
	}
}

func TestArchiveSyncUntilProgressGoalIgnoresFutureCutoff(t *testing.T) {
	goal := archiveSyncUntilProgressGoal(1300, 700, 1200)
	if goal.kind != archiveProgressGoalNone {
		t.Fatalf("progress goal kind = %d, want none", goal.kind)
	}
}

func TestArchiveSyncUntilProgressGoalKeepsCutoffWithoutBlockTime(t *testing.T) {
	goal := archiveSyncUntilProgressGoal(1000, 0, 1200)
	if goal.kind != archiveProgressGoalSyncUntil {
		t.Fatalf("progress goal kind = %d, want sync_until", goal.kind)
	}
	if goal.knownRemaining() {
		t.Fatal("sync_until progress with unknown block time should not have known remaining seconds")
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

func TestMonitorMinSplitDepthAllowsMissingWorkchainConfig(t *testing.T) {
	state := testMonitorMasterStateWithoutWorkchainConfig(t, testMasterBlockID(10), testMasterBlockID(1), true)

	depth, err := monitorMinSplitDepth(state, 0)
	if err != nil {
		t.Fatal(err)
	}
	if depth != 0 {
		t.Fatalf("split depth = %d, want 0", depth)
	}
}

func TestMonitorMinSplitDepthAllowsMissingWorkchain(t *testing.T) {
	state := testArchiveStrategyMasterStateWithWorkchains(t, cell.NewDict(32))

	depth, err := monitorMinSplitDepth(state, 0)
	if err != nil {
		t.Fatal(err)
	}
	if depth != 0 {
		t.Fatalf("split depth = %d, want 0", depth)
	}
}

func TestMonitorMinSplitDepthReadsWorkchainDescriptor(t *testing.T) {
	state := testMonitorMasterStateWithKeyBlock(t, testMasterBlockID(10), 7, testMasterBlockID(1), true)

	depth, err := monitorMinSplitDepth(state, 0)
	if err != nil {
		t.Fatal(err)
	}
	if depth != 7 {
		t.Fatalf("split depth = %d, want 7", depth)
	}
}

func TestArchivePipelineReadyWindowBacklog(t *testing.T) {
	runner := &archiveCatchUpRunner{
		service: &Service{archiveCatchUpPrefetchWindows: DefaultArchiveCatchUpPrefetchWindows},
	}

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
	runner := &archiveCatchUpRunner{
		service: &Service{archiveCatchUpPrefetchWindows: DefaultArchiveCatchUpPrefetchWindows},
	}
	if !runner.shouldEmitReadyArchiveWindow([]archivePendingWindow{testReadyArchiveWindow(errors.New("boom"))}) {
		t.Fatal("pipeline should emit ready window errors immediately")
	}
}

func TestArchiveCheckpointBackpressureWaitsAtConfiguredWindowTarget(t *testing.T) {
	runner := &archiveCatchUpRunner{
		service:                &Service{archiveCatchUpCheckpointBlocks: 2000, checkpointBytes: DefaultCheckpointBytes, syncBackpressureWindows: DefaultSyncBackpressureWindows},
		checkpointDone:         make(chan archiveCheckpointResult),
		checkpointBlocksTarget: 2000,
		lastCheckpointSeqno:    1000,
		current:                &storage.CurrentState{ShardClientSeqno: 8999},
		stateCells:             newTestStateCellWindowCache(nil),
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

func TestArchiveShardPrefixesIncludeWideShardInEveryCoveredPrefix(t *testing.T) {
	block := testBlockID(0, topShard, 11)

	got := archiveShardImportPlansForBlockStatesMatching(2, nil, [][]ton.BlockIDExt{{block}}, nil)
	if len(got) != 4 {
		t.Fatalf("wide shard prefixes = %#v, want 4 plans", got)
	}
	for i, plan := range got {
		wantShard := archiveShardIDForPrefixIndex(2, i)
		if plan.shard != wantShard {
			t.Fatalf("plan[%d] shard = %#v, want %#v", i, plan.shard, wantShard)
		}
		if len(plan.needed) != 1 || !plan.needed[0].Equals(&block) {
			t.Fatalf("plan[%d] needed = %#v, want [%s]", i, plan.needed, storage.FormatBlockRef(block))
		}
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

func TestArchiveShardPrefixesSkipZeroStateTargets(t *testing.T) {
	shard := int64(-1 << 61)
	zero := testBlockID(0, shard, 0)

	missing := archiveShardImportPlansMissingFromBlockStates(2, nil, [][]ton.BlockIDExt{{zero}}, nil)
	if len(missing) != 0 {
		t.Fatalf("zerostate shard prefixes = %#v, want none", missing)
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
		service:                &Service{archiveCatchUpCheckpointBlocks: 2000, checkpointBytes: 1024, syncBackpressureWindows: DefaultSyncBackpressureWindows},
		checkpointDone:         make(chan archiveCheckpointResult),
		checkpointBlocksTarget: 2000,
		lastCheckpointSeqno:    1000,
		current:                &storage.CurrentState{ShardClientSeqno: 1001},
		stateCells:             newTestStateCellWindowCache(nil),
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

func TestArchiveCheckpointBackpressureCountsArtifactBytes(t *testing.T) {
	first := &storage.BlockState{Block: testBlockID(0, topShard, 100)}
	second := &storage.BlockState{Block: testBlockID(0, topShard, 101)}

	runner := &archiveCatchUpRunner{
		service:                &Service{archiveCatchUpCheckpointBlocks: 2000, checkpointBytes: 1024, syncBackpressureWindows: DefaultSyncBackpressureWindows},
		checkpointDone:         make(chan archiveCheckpointResult),
		checkpointBlocksTarget: 2000,
		lastCheckpointSeqno:    1000,
		current:                &storage.CurrentState{ShardClientSeqno: 1001},
		stateCells:             newTestStateCellWindowCache(nil),
	}
	runner.checkpointStates.rememberWithArtifacts(first, &storage.ServedBlockFull{
		ID:    first.Block,
		Block: make([]byte, 4095),
		Meta:  &storage.BlockMeta{},
	}, nil)
	if got := runner.pendingArchiveCheckpointBytes(); got != 4095 {
		t.Fatalf("pending checkpoint bytes = %d, want 4095", got)
	}
	if runner.shouldWaitArchiveCheckpointBackpressure() {
		t.Fatal("checkpoint backpressure should not wait below artifact byte limit")
	}

	runner.checkpointStates.rememberWithArtifacts(second, &storage.ServedBlockFull{
		ID:    second.Block,
		Proof: []byte{1},
		Meta:  &storage.BlockMeta{},
	}, nil)
	if got := runner.pendingArchiveCheckpointBytes(); got != 4096 {
		t.Fatalf("pending checkpoint bytes = %d, want 4096", got)
	}
	if !runner.shouldWaitArchiveCheckpointBackpressure() {
		t.Fatal("checkpoint backpressure should wait at artifact byte limit")
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
		service:      &Service{nextBlockCheckpointBlocks: 10, checkpointBytes: 1024, syncBackpressureWindows: DefaultSyncBackpressureWindows},
		stagedBlocks: 9,
		stateCells:   newTestStateCellWindowCache(nil),
	}
	if err := runner.stateCells.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		first: make([]byte, 1023),
	})); err != nil {
		t.Fatalf("add first prepared records: %v", err)
	}
	if runner.shouldCheckpointStagedCurrent() {
		t.Fatal("next checkpoint should not start below block and byte targets")
	}

	if err := runner.stateCells.addPreparedRecords(testStateCellRecords(map[cell.Hash][]byte{
		second: make([]byte, 1),
	})); err != nil {
		t.Fatalf("add second prepared records: %v", err)
	}
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
	service := &Service{archiveCatchUpCheckpointBlocks: 2000, syncBackpressureWindows: DefaultSyncBackpressureWindows}
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
	window.rememberAppliedArchiveState(state, PreparedBlock{
		ID:       block,
		BlockBOC: []byte{1, 2, 3},
		ProofBOC: []byte{4, 5, 6},
		Meta:     meta,
		IsLink:   true,
	}, 3)

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
	if entry.Artifact.Meta == nil || entry.Artifact.Meta.MasterchainRefSeqno != master.SeqNo {
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

func testArchiveStrategyMasterStateWithWorkchains(t *testing.T, workchains *cell.Dictionary) *storage.BlockState {
	t.Helper()

	workchainsCell, err := tlb.ToCell(&tlb.WorkchainsConfig{Workchains: workchains})
	if err != nil {
		t.Fatalf("build workchains config: %v", err)
	}

	configParams := cell.NewDict(32)
	param := cell.BeginCell().MustStoreRef(workchainsCell).EndCell()
	if err = configParams.SetIntKey(big.NewInt(int64(tlb.ConfigParamWorkchains)), param); err != nil {
		t.Fatalf("store workchains config param: %v", err)
	}

	extra := cell.BeginCell().
		MustStoreUInt(0xcc26, 16).
		MustStoreDict(nil).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreRef(configParams.AsCell()).
		MustStoreRef(testMonitorMasterInfo(t, testMasterBlockID(1), true)).
		MustStoreCoins(0).
		MustStoreDict(nil).
		EndCell()

	return &storage.BlockState{
		Block: testMasterBlockID(10),
		Cell:  cell.BeginCell().EndCell(),
		Parsed: &tlb.ShardStateUnsplit{
			McStateExtra: extra,
		},
	}
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
