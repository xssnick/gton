package pebblestore

import (
	"context"
	"testing"
)

func TestDBStatusIncludesMetaDB(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer store.Close()

	status, err := store.DBStatus(context.Background())
	if err != nil {
		t.Fatalf("db status: %v", err)
	}
	if status.Meta == nil {
		t.Fatalf("meta db status is nil")
	}
	if len(status.CellGenerations) != 1 {
		t.Fatalf("cell generations = %d, want 1", len(status.CellGenerations))
	}
}
