package pebblestore

import (
	"context"
	"testing"

	"github.com/xssnick/gton/service/storage"
)

func TestArchiveBackfillProgressRoundTrip(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	origin := testArchivePruneBlock(1000, 0x10)
	want := storage.ArchiveBackfillProgress{
		OriginPersistentState: origin,
		OriginGenUTime:        10_000,
		GCCutoffUnix:          1000,
		TargetUnix:            2800,
		VerifiedFloorSeqno:    700,
		VerifiedFloorUTime:    7500,
		UpdatedAtUnix:         42,
	}
	if err = store.SaveArchiveBackfillProgress(context.Background(), want); err != nil {
		t.Fatalf("save progress: %v", err)
	}

	got, err := store.ArchiveBackfillProgress(context.Background())
	if err != nil {
		t.Fatalf("load progress: %v", err)
	}
	if !got.OriginPersistentState.Equals(&want.OriginPersistentState) ||
		got.OriginGenUTime != want.OriginGenUTime ||
		got.GCCutoffUnix != want.GCCutoffUnix ||
		got.TargetUnix != want.TargetUnix ||
		got.VerifiedFloorSeqno != want.VerifiedFloorSeqno ||
		got.VerifiedFloorUTime != want.VerifiedFloorUTime ||
		got.UpdatedAtUnix != want.UpdatedAtUnix {
		t.Fatalf("progress = %+v, want %+v", got, want)
	}
}
