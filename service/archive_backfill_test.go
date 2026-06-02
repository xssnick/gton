package service

import (
	"context"
	"errors"
	"testing"

	archivepkg "github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestArchiveBackfillRetentionZeroArchiveTTLTargetsZeroblock(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	store := testArchiveBackfillStore{
		active: storage.CellGenerationInfo{OriginPersistentState: origin},
		meta:   &storage.BlockMeta{GenUTime: 10_000},
	}
	svc := &Service{archiveTTL: 0}

	retention, err := svc.archiveBackfillRetention(context.Background(), store)
	if err != nil {
		t.Fatalf("archive backfill retention: %v", err)
	}

	if retention.gcCutoff != 0 {
		t.Fatalf("gc cutoff = %d, want 0", retention.gcCutoff)
	}
	if retention.target != 0 {
		t.Fatalf("target = %d, want 0", retention.target)
	}
}

func TestArchiveBackfillRetentionDisabledByFlag(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	store := testArchiveBackfillStore{
		active: storage.CellGenerationInfo{OriginPersistentState: origin},
		meta:   &storage.BlockMeta{GenUTime: 10_000},
	}
	svc := &Service{archiveTTL: 0, disableArchiveBackfill: true}

	_, err := svc.archiveBackfillRetention(context.Background(), store)
	if !errors.Is(err, errArchiveBackfillDisabled) {
		t.Fatalf("archive backfill retention error = %v, want disabled", err)
	}
}

func TestArchiveBackfillProgressTargetZeroContinuesUntilZeroblock(t *testing.T) {
	origin := testBlockID(-1, topShard, 100)
	retention := archiveBackfillRetention{
		origin:      origin,
		originUTime: 10_000,
		target:      0,
	}
	store := testArchiveBackfillStore{
		progress: storage.ArchiveBackfillProgress{
			OriginPersistentState: origin,
			VerifiedFloorSeqno:    1,
			VerifiedFloorUTime:    10,
		},
	}

	progress, err := (&Service{}).archiveBackfillProgress(context.Background(), store, retention)
	if err != nil {
		t.Fatalf("archive backfill progress: %v", err)
	}
	if progress.VerifiedFloorSeqno != 1 {
		t.Fatalf("verified floor seqno = %d, want 1", progress.VerifiedFloorSeqno)
	}

	store.progress.VerifiedFloorSeqno = 0
	_, err = (&Service{}).archiveBackfillProgress(context.Background(), store, retention)
	if !errors.Is(err, errArchiveBackfillComplete) {
		t.Fatalf("archive backfill progress error = %v, want complete", err)
	}
}

func TestArchiveBackfillProgressCarriesFloorAcrossAdvancedOrigin(t *testing.T) {
	oldOrigin := testBlockID(-1, topShard, 100)
	newOrigin := testBlockID(-1, topShard, 200)
	retention := archiveBackfillRetention{
		origin:      newOrigin,
		originUTime: 20_000,
		target:      6_000,
	}
	store := testArchiveBackfillStore{
		progress: storage.ArchiveBackfillProgress{
			OriginPersistentState: oldOrigin,
			OriginGenUTime:        10_000,
			VerifiedFloorSeqno:    70,
			VerifiedFloorUTime:    7_000,
		},
	}

	progress, err := (&Service{}).archiveBackfillProgress(context.Background(), store, retention)
	if err != nil {
		t.Fatalf("archive backfill progress: %v", err)
	}
	if !progress.OriginPersistentState.Equals(&newOrigin) {
		t.Fatalf("progress origin = %s, want %s", storage.FormatBlockRef(progress.OriginPersistentState), storage.FormatBlockRef(newOrigin))
	}
	if progress.VerifiedFloorSeqno != 70 || progress.VerifiedFloorUTime != 7_000 {
		t.Fatalf("verified floor = (%d,%d), want (70,7000)", progress.VerifiedFloorSeqno, progress.VerifiedFloorUTime)
	}
}

func TestArchiveBackfillProgressDoesNotRestartWhenCarriedFloorIsComplete(t *testing.T) {
	oldOrigin := testBlockID(-1, topShard, 100)
	newOrigin := testBlockID(-1, topShard, 200)
	retention := archiveBackfillRetention{
		origin:      newOrigin,
		originUTime: 20_000,
		target:      8_000,
	}
	store := testArchiveBackfillStore{
		progress: storage.ArchiveBackfillProgress{
			OriginPersistentState: oldOrigin,
			OriginGenUTime:        10_000,
			VerifiedFloorSeqno:    70,
			VerifiedFloorUTime:    7_000,
		},
	}

	_, err := (&Service{}).archiveBackfillProgress(context.Background(), store, retention)
	if !errors.Is(err, errArchiveBackfillComplete) {
		t.Fatalf("archive backfill progress error = %v, want complete", err)
	}
}

func TestArchiveBackfillProgressResetsIncompatibleOrigin(t *testing.T) {
	oldOrigin := testBlockID(-1, topShard, 300)
	newOrigin := testBlockID(-1, topShard, 200)
	retention := archiveBackfillRetention{
		origin:      newOrigin,
		originUTime: 20_000,
		target:      6_000,
	}
	store := testArchiveBackfillStore{
		progress: storage.ArchiveBackfillProgress{
			OriginPersistentState: oldOrigin,
			OriginGenUTime:        30_000,
			VerifiedFloorSeqno:    250,
			VerifiedFloorUTime:    25_000,
		},
	}

	progress, err := (&Service{}).archiveBackfillProgress(context.Background(), store, retention)
	if err != nil {
		t.Fatalf("archive backfill progress: %v", err)
	}
	if !progress.OriginPersistentState.Equals(&newOrigin) {
		t.Fatalf("progress origin = %s, want %s", storage.FormatBlockRef(progress.OriginPersistentState), storage.FormatBlockRef(newOrigin))
	}
	if progress.VerifiedFloorSeqno != newOrigin.SeqNo || progress.VerifiedFloorUTime != retention.originUTime {
		t.Fatalf("verified floor = (%d,%d), want (%d,%d)", progress.VerifiedFloorSeqno, progress.VerifiedFloorUTime, newOrigin.SeqNo, retention.originUTime)
	}
}

func TestArchiveBackfillStoredShardTargetsBuildsRefsAndChangedTargets(t *testing.T) {
	oldMaster := testBlockID(-1, topShard, 10)
	newMaster := testBlockID(-1, topShard, 11)
	oldShard := testBlockID(0, topShard, 100)
	newShard := testBlockID(0, topShard, 101)

	targets, refs, err := archiveBackfillStoredShardTargets([]archiveBackfillStoredMasterBlock{
		{ID: newMaster, Shards: []ton.BlockIDExt{newShard}},
		{ID: oldMaster, Shards: []ton.BlockIDExt{oldShard}},
	})
	if err != nil {
		t.Fatalf("stored shard targets: %v", err)
	}
	if len(targets) != 1 || !targets[0].Equals(&newShard) {
		t.Fatalf("targets = %+v, want %s", targets, storage.FormatBlockRef(newShard))
	}
	if got := refs[storage.BlockKey(oldShard)]; !got.Equals(&oldMaster) {
		t.Fatalf("old shard master ref = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(oldMaster))
	}
	if got := refs[storage.BlockKey(newShard)]; !got.Equals(&newMaster) {
		t.Fatalf("new shard master ref = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(newMaster))
	}
}

func TestArchiveBackfillStoredWindowSplitDepthUsesShardDescriptions(t *testing.T) {
	deepShard := int64(-1 << 61)

	got, err := archiveBackfillStoredWindowSplitDepth(0, []archiveBackfillStoredMasterBlock{
		{Shards: []ton.BlockIDExt{testBlockID(0, deepShard, 100)}},
	})
	if err != nil {
		t.Fatalf("stored window split depth: %v", err)
	}
	if got != 2 {
		t.Fatalf("split depth = %d, want 2", got)
	}
}

func TestArchiveBackfillShardPlansUseStoredWindowSplitDepth(t *testing.T) {
	deepShard := int64(-1 << 61)
	target := testBlockID(0, deepShard, 67371646)
	runner := &archiveBackfillRunner{
		store: testArchiveBackfillStore{metaErr: storage.ErrNotFound},
	}

	plans, err := runner.missingArchiveBackfillShardPlans(context.Background(), 2, []ton.BlockIDExt{target})
	if err != nil {
		t.Fatalf("missing shard plans: %v", err)
	}
	if len(plans) != 1 {
		t.Fatalf("plans = %d, want 1", len(plans))
	}
	if plans[0].shard.Workchain != 0 || plans[0].shard.Shard != deepShard {
		t.Fatalf("plan shard = %s, want wc=0 shard=%016x", plans[0].shard.String(), uint64(deepShard))
	}
	if plans[0].splitDepth != 2 {
		t.Fatalf("plan split depth = %d, want 2", plans[0].splitDepth)
	}
	if len(plans[0].needed) != 1 || !plans[0].needed[0].Equals(&target) {
		t.Fatalf("plan needed = %+v, want %s", plans[0].needed, storage.FormatBlockRef(target))
	}
}

func TestArchiveBackfillShardTargetStoredSelfHealsMissingMasterRef(t *testing.T) {
	target := testBlockID(0, topShard, 100)
	runner := &archiveBackfillRunner{
		store: testArchiveBackfillStore{
			meta: &storage.BlockMeta{
				Flags: storage.BlockMetaHasServedFull | storage.BlockMetaHasBlockData,
			},
		},
	}

	stored, err := runner.archiveBackfillShardTargetStored(context.Background(), target)
	if err != nil {
		t.Fatalf("target stored: %v", err)
	}
	if stored {
		t.Fatal("stored target with missing master ref was not treated as missing")
	}
}

func TestArchiveBackfillShardTargetStoredAcceptsMasterRef(t *testing.T) {
	target := testBlockID(0, topShard, 100)
	master := testBlockID(-1, topShard, 10)
	runner := &archiveBackfillRunner{
		store: testArchiveBackfillStore{
			meta: &storage.BlockMeta{
				Flags:          storage.BlockMetaHasServedFull | storage.BlockMetaHasBlockData,
				MasterchainRef: &master,
			},
		},
	}

	stored, err := runner.archiveBackfillShardTargetStored(context.Background(), target)
	if err != nil {
		t.Fatalf("target stored: %v", err)
	}
	if !stored {
		t.Fatal("stored target with master ref was treated as missing")
	}
}

func TestArchiveBackfillMasterStorageSkipsShardEntriesWithoutMasterRef(t *testing.T) {
	master := testBlockID(-1, topShard, 100)
	shard := testBlockID(0, topShard, 200)
	imported := &archivepkg.Imported{
		ServedArchiveImport: storage.ServedArchiveImport{
			FullBlocks: []*storage.ServedBlockFull{
				{ID: master, Block: []byte{0x01}, Proof: []byte{0x02}},
				{ID: shard, Block: []byte{0x03}, Proof: []byte{0x04}},
			},
		},
		PreparedBlocks: map[storage.BlockRootHash]archivepkg.PreparedBlock{
			storage.BlockKey(master): {Meta: &storage.BlockMeta{ID: master}},
			storage.BlockKey(shard):  {Meta: &storage.BlockMeta{ID: shard}},
		},
	}

	stored, err := servedArchiveImportFromImported(imported, 0, nil, archiveBackfillIncludeBlocks([]PreparedBlock{{ID: master}}))
	if err != nil {
		t.Fatalf("master storage import: %v", err)
	}
	if len(stored.FullBlocks) != 1 {
		t.Fatalf("stored full blocks = %d, want 1", len(stored.FullBlocks))
	}
	if !stored.FullBlocks[0].ID.Equals(&master) {
		t.Fatalf("stored block = %s, want %s", storage.FormatBlockRef(stored.FullBlocks[0].ID), storage.FormatBlockRef(master))
	}
}

func TestArchiveBackfillShardImportMasterRefsPropagatesSameShardPrevChain(t *testing.T) {
	master := testBlockID(-1, topShard, 10)
	prev := testBlockID(0, topShard, 100)
	target := testBlockID(0, topShard, 101)
	extra := testBlockID(0, int64(0x4000000000000000), 50)

	refs, include, err := archiveBackfillShardImportMasterRefs(map[storage.BlockRootHash]PreparedBlock{
		storage.BlockKey(target): {
			ID:   target,
			Meta: &storage.BlockMeta{ID: target, PrevRefs: []ton.BlockIDExt{prev}},
		},
		storage.BlockKey(prev): {
			ID:   prev,
			Meta: &storage.BlockMeta{ID: prev, PrevRefs: []ton.BlockIDExt{testBlockID(0, topShard, 99)}},
		},
		storage.BlockKey(extra): {
			ID:   extra,
			Meta: &storage.BlockMeta{ID: extra},
		},
	}, []ton.BlockIDExt{target}, map[storage.BlockRootHash]ton.BlockIDExt{
		storage.BlockKey(target): master,
	})
	if err != nil {
		t.Fatalf("shard import master refs: %v", err)
	}
	for _, block := range []ton.BlockIDExt{target, prev} {
		got := refs[storage.BlockKey(block)]
		if !got.Equals(&master) {
			t.Fatalf("master ref for %s = %s, want %s", storage.FormatBlockRef(block), storage.FormatBlockRef(got), storage.FormatBlockRef(master))
		}
		if !include[storage.BlockKey(block)] {
			t.Fatalf("block %s was not included", storage.FormatBlockRef(block))
		}
	}
	if include[storage.BlockKey(extra)] {
		t.Fatalf("unneeded block %s was included", storage.FormatBlockRef(extra))
	}
}

func TestArchiveBackfillShardImportMasterRefsKeepsEarliestMasterForVisitedPrev(t *testing.T) {
	earlierMaster := testBlockID(-1, topShard, 62070015)
	laterMaster := testBlockID(-1, topShard, 62070016)
	prev := testBlockID(0, int64(-3<<61), 67372599)
	child := testBlockID(0, int64(-3<<61), 67372600)

	refs, include, err := archiveBackfillShardImportMasterRefs(map[storage.BlockRootHash]PreparedBlock{
		storage.BlockKey(child): {
			ID:   child,
			Meta: &storage.BlockMeta{ID: child, PrevRefs: []ton.BlockIDExt{prev}},
		},
		storage.BlockKey(prev): {
			ID:   prev,
			Meta: &storage.BlockMeta{ID: prev},
		},
	}, []ton.BlockIDExt{child, prev}, map[storage.BlockRootHash]ton.BlockIDExt{
		storage.BlockKey(child): laterMaster,
		storage.BlockKey(prev):  earlierMaster,
	})
	if err != nil {
		t.Fatalf("shard import master refs: %v", err)
	}
	if got := refs[storage.BlockKey(prev)]; !got.Equals(&earlierMaster) {
		t.Fatalf("prev master ref = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(earlierMaster))
	}
	if got := refs[storage.BlockKey(child)]; !got.Equals(&laterMaster) {
		t.Fatalf("child master ref = %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(laterMaster))
	}
	if !include[storage.BlockKey(prev)] || !include[storage.BlockKey(child)] {
		t.Fatalf("expected both visited blocks to be included")
	}
}

func TestArchiveBackfillShardImportMasterRefsRequiresTargetMaster(t *testing.T) {
	target := testBlockID(0, topShard, 101)

	_, _, err := archiveBackfillShardImportMasterRefs(map[storage.BlockRootHash]PreparedBlock{
		storage.BlockKey(target): {
			ID:   target,
			Meta: &storage.BlockMeta{ID: target},
		},
	}, []ton.BlockIDExt{target}, nil)
	if err == nil {
		t.Fatal("shard import master refs accepted target without master ref")
	}
}

func TestValidateArchiveBackfillShardImportIgnoresUnselectedBlocks(t *testing.T) {
	unselected := testBlockID(0, int64(0x2000000000000000), 67370774)

	err := validateArchiveBackfillShardImport(&archiveImportResult{
		blocks: map[storage.BlockRootHash]PreparedBlock{
			storage.BlockKey(unselected): {
				ID:     unselected,
				IsLink: false,
			},
		},
	}, map[storage.BlockRootHash]bool{})
	if err != nil {
		t.Fatalf("validate shard import: %v", err)
	}
}

type testArchiveBackfillStore struct {
	active   storage.CellGenerationInfo
	meta     *storage.BlockMeta
	metaErr  error
	progress storage.ArchiveBackfillProgress
}

func (s testArchiveBackfillStore) ActiveCellGeneration(context.Context) (storage.CellGenerationInfo, error) {
	return s.active, nil
}

func (s testArchiveBackfillStore) BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error) {
	if s.metaErr != nil {
		return nil, s.metaErr
	}
	return s.meta, nil
}

func (s testArchiveBackfillStore) LookupBlockBySeqNo(context.Context, storage.BlockHistoryKey, uint32) (ton.BlockIDExt, error) {
	return ton.BlockIDExt{}, errors.New("unexpected block seqno lookup")
}

func (s testArchiveBackfillStore) BlockData(context.Context, ton.BlockIDExt) ([]byte, error) {
	return nil, errors.New("unexpected block data load")
}

func (s testArchiveBackfillStore) ArchiveBackfillProgress(context.Context) (storage.ArchiveBackfillProgress, error) {
	return s.progress, nil
}

func (s testArchiveBackfillStore) SaveArchiveBackfillProgress(context.Context, storage.ArchiveBackfillProgress) error {
	return nil
}

func (s testArchiveBackfillStore) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	return nil, errors.New("unexpected state cell load")
}
