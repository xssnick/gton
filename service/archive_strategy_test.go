package service

import (
	"testing"

	"flexserver/service/p2p"
	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
)

func TestArchiveCatchUpTargetByKnownTargetTimeUsesLagHysteresis(t *testing.T) {
	current := &storage.CurrentState{
		Masterchain: storage.BlockState{Block: ton.BlockIDExt{SeqNo: 1000}},
	}
	target := ton.BlockIDExt{SeqNo: 5000}

	_, lagSeconds, ok := archiveCatchUpTargetByKnownTargetTime(current, target, 10, 10+nextToArchiveLagSeconds)
	if ok {
		t.Fatal("archive catch-up should not start at the switch boundary")
	}
	if lagSeconds != nextToArchiveLagSeconds {
		t.Fatalf("lag seconds mismatch: got %d want %d", lagSeconds, int64(nextToArchiveLagSeconds))
	}

	archiveTarget, lagSeconds, ok := archiveCatchUpTargetByKnownTargetTime(current, target, 10, 10+nextToArchiveLagSeconds+1)
	if !ok {
		t.Fatal("archive catch-up should start above the switch boundary")
	}
	if lagSeconds != nextToArchiveLagSeconds+1 {
		t.Fatalf("lag seconds mismatch: got %d want %d", lagSeconds, int64(nextToArchiveLagSeconds+1))
	}
	if !archiveTarget.Equals(&target) {
		t.Fatalf("archive target = %s, want %s", storage.FormatBlockRef(archiveTarget), storage.FormatBlockRef(target))
	}
}

func TestShouldStopArchiveCatchUpUsesLiveTailLag(t *testing.T) {
	if !shouldStopArchiveCatchUpByLag(archiveToNextLagSeconds - 1) {
		t.Fatal("archive catch-up should stop below the live tail lag")
	}
	if shouldStopArchiveCatchUpByLag(archiveToNextLagSeconds) {
		t.Fatal("archive catch-up should continue at the live tail boundary")
	}
}

func TestArchiveCatchUpTargetByTimeStartsForStaleMasterchain(t *testing.T) {
	current := &storage.CurrentState{
		ShardClientSeqno: 1000,
		Masterchain: storage.BlockState{
			Block:  ton.BlockIDExt{SeqNo: 1000},
			Parsed: &tlb.ShardStateUnsplit{GenUTime: 10},
		},
	}

	archiveTarget, lagSeconds, ok := archiveCatchUpTargetByTime(current, 10+nextToArchiveLagSeconds+1)
	if !ok {
		t.Fatal("archive catch-up should start for stale masterchain time")
	}
	if lagSeconds != nextToArchiveLagSeconds+1 {
		t.Fatalf("lag seconds mismatch: got %d want %d", lagSeconds, int64(nextToArchiveLagSeconds+1))
	}

	want := current.Masterchain.Block.SeqNo + uint32(nextToArchiveLagSeconds+1-archiveToNextLagSeconds)
	if archiveTarget.SeqNo != want {
		t.Fatalf("archive target seqno mismatch: got %d want %d", archiveTarget.SeqNo, want)
	}
}

func TestArchiveCatchUpTargetByBlockTimeStartsWithoutParsedState(t *testing.T) {
	current := &storage.CurrentState{
		ShardClientSeqno: 1000,
		Masterchain: storage.BlockState{
			Block: ton.BlockIDExt{SeqNo: 1000},
		},
	}

	archiveTarget, lagSeconds, ok := archiveCatchUpTargetByBlockTime(current, 10, 10+nextToArchiveLagSeconds+1)
	if !ok {
		t.Fatal("archive catch-up should use explicit block time when parsed state is not loaded")
	}
	if lagSeconds != nextToArchiveLagSeconds+1 {
		t.Fatalf("lag seconds mismatch: got %d want %d", lagSeconds, int64(nextToArchiveLagSeconds+1))
	}

	want := current.Masterchain.Block.SeqNo + uint32(nextToArchiveLagSeconds+1-archiveToNextLagSeconds)
	if archiveTarget.SeqNo != want {
		t.Fatalf("archive target seqno mismatch: got %d want %d", archiveTarget.SeqNo, want)
	}
}

func TestArchiveCatchUpTargetByTimeCapsLookahead(t *testing.T) {
	current := &storage.CurrentState{
		ShardClientSeqno: 1000,
		Masterchain: storage.BlockState{
			Block:  ton.BlockIDExt{SeqNo: 1000},
			Parsed: &tlb.ShardStateUnsplit{GenUTime: 10},
		},
	}

	archiveTarget, _, ok := archiveCatchUpTargetByTime(current, 10+archiveToNextLagSeconds+int64(archiveTimeLagLookaheadBlocks*10))
	if !ok {
		t.Fatal("archive catch-up should start for large stale masterchain time")
	}

	want := current.Masterchain.Block.SeqNo + archiveTimeLagLookaheadBlocks
	if archiveTarget.SeqNo != want {
		t.Fatalf("archive target seqno mismatch: got %d want %d", archiveTarget.SeqNo, want)
	}
}

func TestFilteredArchiveImportKeepsOnlyAppliedBlocks(t *testing.T) {
	master1 := testMasterBlockID(11)
	master2 := testMasterBlockID(12)
	future := testMasterBlockID(13)
	shard := ton.BlockIDExt{Workchain: 0, Shard: topShard, SeqNo: 101}

	src := &storage.ServedArchiveImport{
		FullBlocks: []*storage.ServedBlockFull{
			testStoredArchiveFull(master1),
			testStoredArchiveFull(master2),
			testStoredArchiveFull(future),
			testStoredArchiveFull(shard),
		},
		Links: []storage.ServedBlockLink{
			{Prev: master1, Next: master2},
			{Prev: master2, Next: future},
		},
	}
	allowed := map[string]struct{}{
		storage.BlockKey(master1): {},
		storage.BlockKey(master2): {},
		storage.BlockKey(shard):   {},
	}

	dst := &storage.ServedArchiveImport{}
	appendFilteredArchiveImport(dst, src, allowed, map[string]struct{}{}, map[string]struct{}{})

	if len(dst.FullBlocks) != 3 {
		t.Fatalf("stored full blocks = %d, want 3", len(dst.FullBlocks))
	}
	if storage.BlockKey(dst.FullBlocks[0].ID) != storage.BlockKey(master1) ||
		storage.BlockKey(dst.FullBlocks[1].ID) != storage.BlockKey(master2) ||
		storage.BlockKey(dst.FullBlocks[2].ID) != storage.BlockKey(shard) {
		t.Fatalf("unexpected stored full block order: %v", dst.FullBlocks)
	}
	if len(dst.Links) != 1 || !dst.Links[0].Prev.Equals(&master1) || !dst.Links[0].Next.Equals(&master2) {
		t.Fatalf("stored links = %+v, want only master1->master2", dst.Links)
	}
}

func TestArchiveMasterBlockSequenceStopsBeforeNonStartKeyBlock(t *testing.T) {
	start := &storage.BlockState{Block: testMasterBlockID(10)}
	blocks := map[uint32]p2p.DownloadedBlock{
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
	blocks := map[uint32]p2p.DownloadedBlock{
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

func testArchiveMasterBlock(seqno uint32, keyBlock bool) p2p.DownloadedBlock {
	block := testMasterBlockID(seqno)
	meta := &storage.BlockMeta{ID: block}
	if keyBlock {
		meta.Mark(storage.BlockMetaIsKeyBlock)
	}
	return p2p.DownloadedBlock{ID: block, Meta: meta}
}

func testMasterBlockID(seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{Workchain: -1, Shard: topShard, SeqNo: seqno}
}

func testStoredArchiveFull(block ton.BlockIDExt) *storage.ServedBlockFull {
	return &storage.ServedBlockFull{
		ID:       block,
		BlockRef: &storage.ArtifactRef{Path: "archive.pack", Offset: int64(block.SeqNo), Size: 10},
		ProofRef: &storage.ArtifactRef{Path: "archive.pack", Offset: int64(block.SeqNo) + 10, Size: 5},
		Meta:     &storage.BlockMeta{ID: block},
	}
}
