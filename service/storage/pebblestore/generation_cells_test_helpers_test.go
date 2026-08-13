package pebblestore

import (
	"context"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func saveActiveTestCells(store *Store, records []*storage.CellRecord) error {
	cells, err := store.ActiveCells()
	if err != nil {
		return err
	}

	return cells.Save(context.Background(), records)
}

func saveActiveTestStateCellTree(ctx context.Context, store *Store, request stateCellTreeSave) (stateCellSaveStats, error) {
	generation, err := store.activeCellGenerationID()
	if err != nil {
		return stateCellSaveStats{}, err
	}
	request.cellGeneration = generation

	return store.saveStateCellTree(ctx, request)
}

func loadActiveTestLargeBOCMeta(store *Store, ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCMetaRecord) ([]cell.LargeBOCMetaRecord, error) {
	cells, err := store.ActiveCells()
	if err != nil {
		return dst, err
	}

	return cells.LoadLargeBOCMeta(ctx, hashes, dst)
}

func loadActiveTestLargeBOCPayload(store *Store, ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCPayloadRecord) ([]cell.LargeBOCPayloadRecord, error) {
	cells, err := store.ActiveCells()
	if err != nil {
		return dst, err
	}

	return cells.LoadLargeBOCPayload(ctx, hashes, dst)
}

func loadActiveTestLargeBOCCells(store *Store, ctx context.Context, hashes []cell.Hash, dst []cell.LargeBOCRecord) ([]cell.LargeBOCRecord, error) {
	cells, err := store.ActiveCells()
	if err != nil {
		return dst, err
	}

	return cells.LoadLargeBOCCells(ctx, hashes, dst)
}
