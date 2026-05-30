package service

import (
	"context"
	"errors"
	"testing"

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

type testArchiveBackfillStore struct {
	active   storage.CellGenerationInfo
	meta     *storage.BlockMeta
	progress storage.ArchiveBackfillProgress
}

func (s testArchiveBackfillStore) ActiveCellGeneration(context.Context) (storage.CellGenerationInfo, error) {
	return s.active, nil
}

func (s testArchiveBackfillStore) BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error) {
	return s.meta, nil
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
