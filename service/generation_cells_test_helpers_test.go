package service

import (
	"context"
	"testing"

	"github.com/xssnick/gton/service/storage"
)

type testActiveCellStore interface {
	ActiveCells() (storage.CellGeneration, error)
}

func activeTestCellGeneration(tb testing.TB, store testActiveCellStore) storage.CellGeneration {
	tb.Helper()

	cells, err := store.ActiveCells()
	if err != nil {
		tb.Fatalf("select active cell generation: %v", err)
	}
	return cells
}

func saveActiveTestCells(store testActiveCellStore, records []*storage.CellRecord) error {
	cells, err := store.ActiveCells()
	if err != nil {
		return err
	}

	return cells.Save(context.Background(), records)
}
