package pebblestore

import (
	"context"
	"fmt"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type generationCells struct {
	store      *Store
	generation uint64
	// One recursive loader belongs to the selected generation handle. Reusing it
	// keeps top-level cache misses from allocating another self-referential loader.
	loader cell.LazyCellLoader
}

var _ storage.CellGeneration = generationCells{}

func (s *Store) ActiveCells() (storage.CellGeneration, error) {
	generation, err := s.activeCellGenerationID()
	if err != nil {
		return nil, err
	}

	return s.newGenerationCells(generation), nil
}

func (s *Store) Cells(generation uint64) (storage.CellGeneration, error) {
	if generation == 0 {
		return nil, fmt.Errorf("cell generation is zero")
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.closed {
		return nil, errPebbleClosed
	}
	if s.cellGenerations[generation] == nil {
		return nil, fmt.Errorf("%w: %d", errCellGenerationNotOpen, generation)
	}

	return s.newGenerationCells(generation), nil
}

func (s *Store) newGenerationCells(generation uint64) generationCells {
	return generationCells{
		store:      s,
		generation: generation,
		loader:     s.newLazyCellLoaderForGeneration(generation),
	}
}

func (c generationCells) ID() uint64 {
	return c.generation
}

func (c generationCells) Save(ctx context.Context, records []*storage.CellRecord) error {
	return c.store.saveCellsInGeneration(ctx, c.generation, records)
}

func (c generationCells) SaveEncoded(ctx context.Context, records []storage.EncodedCellRecord, sync bool) error {
	return c.store.saveEncodedCellsInGeneration(ctx, c.generation, records, sync)
}

func (c generationCells) Record(ctx context.Context, hash []byte) (*storage.CellRecord, error) {
	return c.store.cellRecordInGeneration(ctx, c.generation, hash)
}

func (c generationCells) Load(ctx context.Context, hash []byte) (*cell.Cell, error) {
	return c.store.loadLazyCellFromGeneration(ctx, c.generation, hash, c.loader)
}

func (c generationCells) Loader() cell.LazyCellLoader {
	return c.loader
}

func (c generationCells) LoadLargeBOCMeta(
	ctx context.Context,
	hashes []cell.Hash,
	dst []cell.LargeBOCMetaRecord,
) ([]cell.LargeBOCMetaRecord, error) {
	return c.store.largeBOCLoadMetaInGeneration(ctx, c.generation, hashes, dst)
}

func (c generationCells) LoadLargeBOCPayload(
	ctx context.Context,
	hashes []cell.Hash,
	dst []cell.LargeBOCPayloadRecord,
) ([]cell.LargeBOCPayloadRecord, error) {
	return c.store.largeBOCLoadPayloadInGeneration(ctx, c.generation, hashes, dst)
}

func (c generationCells) LoadLargeBOCCells(
	ctx context.Context,
	hashes []cell.Hash,
	dst []cell.LargeBOCRecord,
) ([]cell.LargeBOCRecord, error) {
	return c.store.largeBOCLoadCellsInGeneration(ctx, c.generation, hashes, dst)
}

func (c generationCells) ImportStateCellTree(
	ctx context.Context,
	block ton.BlockIDExt,
	root *cell.Cell,
	totalCells uint64,
) (*cell.Cell, error) {
	rootHash, err := c.store.persistStateCellTreeInGeneration(ctx, c.generation, block, root, totalCells)
	if err != nil {
		return nil, err
	}

	return c.Load(ctx, rootHash[:])
}

func (c generationCells) ImportStateBOCView(
	ctx context.Context,
	block ton.BlockIDExt,
	view *cell.BOCView,
) (*cell.Cell, error) {
	rootHash, err := c.store.persistStateBOCViewInGeneration(ctx, c.generation, block, view)
	if err != nil {
		return nil, err
	}

	return c.Load(ctx, rootHash[:])
}
