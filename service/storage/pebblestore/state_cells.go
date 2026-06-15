package pebblestore

import (
	"bytes"
	"context"
	"fmt"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *Store) ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (*cell.Cell, error) {
	generation, err := s.activeCellGenerationID()
	if err != nil {
		return nil, err
	}
	return s.importStateCellTreeInGeneration(ctx, generation, block, root, totalCells)
}

func (s *Store) ImportStateBOCView(ctx context.Context, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error) {
	generation, err := s.activeCellGenerationID()
	if err != nil {
		return nil, err
	}
	return s.importStateBOCViewInGeneration(ctx, generation, block, view)
}

func (s *Store) ImportStateCellTreeInGeneration(ctx context.Context, generation uint64, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (*cell.Cell, error) {
	if generation == 0 {
		return nil, fmt.Errorf("cell generation is zero")
	}
	return s.importStateCellTreeInGeneration(ctx, generation, block, root, totalCells)
}

func (s *Store) ImportStateBOCViewInGeneration(ctx context.Context, generation uint64, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error) {
	if generation == 0 {
		return nil, fmt.Errorf("cell generation is zero")
	}
	return s.importStateBOCViewInGeneration(ctx, generation, block, view)
}

func (s *Store) TrustImportedStateCellHashes() bool {
	return false
}

func (s *Store) ReuseImportedSplitStatePartCells() bool {
	return true
}

func (s *Store) SaveStateCellRecords(ctx context.Context, records storage.StateCellRecords) error {
	generation, err := s.activeCellGenerationID()
	if err != nil {
		return err
	}
	_, err = s.saveCellRecordSet(ctx, records, false, generation, false)
	return err
}

func (s *Store) FlushStateCells(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	generation, err := s.activeCellGenerationID()
	if err != nil {
		return err
	}
	return s.flushCellDBs(generation)
}

func (s *Store) importStateCellTreeInGeneration(ctx context.Context, generation uint64, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (*cell.Cell, error) {
	rootCellHash := root.HashKey()
	if _, err := s.saveStateCellTree(ctx, stateCellTreeSave{
		block:          block,
		root:           root,
		totalCells:     totalCells,
		cellGeneration: generation,
	}); err != nil {
		return nil, err
	}
	if err := s.flushCellDBs(generation); err != nil {
		return nil, fmt.Errorf("flush generation %d state cells before returning lazy root: %w", generation, err)
	}

	lazyRoot, err := s.loadLazyCellFromGeneration(ctx, generation, rootCellHash[:])
	if err != nil {
		return nil, fmt.Errorf("load persisted lazy state root: %w", err)
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Uint64("cell_generation", generation).
		Uint64("cells", totalCells).
		Msg("state cell tree imported and switched to lazy celldb root")
	return lazyRoot, nil
}

func (s *Store) importStateBOCViewInGeneration(ctx context.Context, generation uint64, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error) {
	roots := view.Roots()
	if len(roots) != 1 {
		return nil, fmt.Errorf("state boc should contain exactly one root, got %d", len(roots))
	}

	rootCell, _, err := view.ReadCell(roots[0], nil)
	if err != nil {
		return nil, fmt.Errorf("load state boc root cell: %w", err)
	}
	if rootCell.D1&0b1000 != 0 && len(rootCell.Body) > 0 && cell.Type(rootCell.Body[0]) == cell.PrunedCellType {
		return nil, fmt.Errorf("state cell tree root is pruned")
	}

	rootCellHash := rootCell.Meta.Hash
	if err = s.saveStateBOCView(ctx, generation, block, view); err != nil {
		return nil, err
	}
	if err = s.flushCellDBs(generation); err != nil {
		return nil, fmt.Errorf("flush generation %d boc state cells before returning lazy root: %w", generation, err)
	}

	lazyRoot, err := s.loadLazyCellFromGeneration(ctx, generation, rootCellHash[:])
	if err != nil {
		return nil, fmt.Errorf("load persisted lazy state root: %w", err)
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(block)).
		Uint64("cell_generation", generation).
		Uint64("cells", uint64(view.Cells())).
		Msg("boc state cells imported and switched to lazy celldb root")
	return lazyRoot, nil
}

func (s *Store) LoadStateCellTree(ctx context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error) {
	state, err := s.blockStateMeta(ctx, block)
	if err != nil {
		return nil, err
	}

	if len(rootHash) > 0 && !bytes.Equal(state.StateRootHash, rootHash) {
		return nil, storage.ErrNotFound
	}

	root, err := s.loadLazyCellFromGeneration(ctx, 0, state.StateRootHash)
	if err != nil {
		return nil, err
	}
	hash := root.HashKey(0)
	if !bytes.Equal(hash[:], state.StateRootHash) {
		return nil, storage.ErrNotFound
	}
	return root, nil
}

func (s *Store) replaceBlockStateWithLazyRoot(state *storage.BlockState, saved storage.BlockState, parsed *storage.BlockState, root *cell.Cell) error {
	var err error
	if root == nil {
		root, err = s.loadLazyCellFromGeneration(context.Background(), saved.CellGeneration, saved.StateRootHash)
		if err != nil {
			return fmt.Errorf("load persisted lazy state root: %w", err)
		}
	}

	state.Block = saved.Block
	state.StateRootHash = bytes.Clone(saved.StateRootHash)
	state.StateFileHash = bytes.Clone(saved.StateFileHash)
	if saved.MasterchainRef == nil {
		state.MasterchainRef = nil
	} else {
		ref := *saved.MasterchainRef
		state.MasterchainRef = &ref
	}
	state.CellGeneration = saved.CellGeneration
	state.Cell = root
	state.Parsed = saved.Parsed
	if parsed != nil {
		state.Parsed = parsed.Parsed
	}

	s.log.Debug().
		Str("block", storage.FormatBlockRef(saved.Block)).
		Msg("block state switched to lazy celldb root")
	return nil
}

func (s *Store) SaveCells(records []*storage.CellRecord) error {
	return s.SaveCellsInGeneration(context.Background(), 0, records)
}

func (s *Store) SaveCellsInGeneration(ctx context.Context, generation uint64, records []*storage.CellRecord) error {
	encoded := make([]storage.EncodedCellRecord, 0, len(records))
	for _, record := range records {
		if len(record.Hash) != 32 {
			return fmt.Errorf("cell record hash size mismatch: %d", len(record.Hash))
		}

		var hash cell.Hash
		copy(hash[:], record.Hash)
		encoded = append(encoded, storage.EncodedCellRecord{
			Hash: hash,
			Data: encodeCellRecord(record),
		})
	}

	_, err := s.saveCellRecordBatch(ctx, encoded, true, generation, true)
	return err
}

func (s *Store) SaveEncodedCellsInGeneration(ctx context.Context, generation uint64, records []storage.EncodedCellRecord, sync bool) error {
	_, err := s.saveCellRecordBatch(ctx, records, sync, generation, true)
	return err
}

type cellRecordBatchStats struct {
	written int
	skipped int
	bytes   int64
}

func (s *Store) savePreparedStateCellRecords(ctx context.Context, records storage.StateCellRecords, generation uint64) (stateCellSaveStats, error) {
	stats, err := s.saveCellRecordSet(ctx, records, false, generation, false)
	if err != nil {
		return stateCellSaveStats{}, err
	}
	return stateCellSaveStats{applied: stats.written > 0}, nil
}

func (s *Store) saveCellRecordBatch(ctx context.Context, records []storage.EncodedCellRecord, sync bool, generation uint64, dedupe bool) (cellRecordBatchStats, error) {
	return s.saveCellRecordSet(ctx, storage.NewStateCellRecords(records), sync, generation, dedupe)
}

func (s *Store) saveCellRecordSet(ctx context.Context, records storage.StateCellRecords, sync bool, generation uint64, dedupe bool) (cellRecordBatchStats, error) {
	var stats cellRecordBatchStats
	if records.Empty() {
		return stats, nil
	}
	if err := s.ensureWritable(); err != nil {
		return stats, err
	}

	cells, err := s.acquireCellStore(ctx, generation)
	if err != nil {
		return stats, err
	}
	defer cells.release()

	writer := cells.newBatchWriter()
	defer writer.close()

	var written map[cell.Hash]struct{}
	if dedupe {
		written = make(map[cell.Hash]struct{}, records.Len())
	}
	i := 0
	if err := records.ForEach(func(record storage.EncodedCellRecord) error {
		if i&0x3fff == 0 {
			select {
			case <-ctx.Done():
				return ctx.Err()
			default:
			}
		}
		i++

		if len(record.Data) == 0 {
			return fmt.Errorf("encoded cell record is empty")
		}

		if dedupe {
			if _, ok := written[record.Hash]; ok {
				stats.skipped++
				return nil
			}
			written[record.Hash] = struct{}{}
		}

		if err := writer.set(record.Hash[:], record.Data); err != nil {
			return err
		}
		stats.written++
		stats.bytes += int64(len(record.Data))

		if writer.bytesInBatch >= stateCellImportBatchTargetBytes {
			if _, err := writer.flush(); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return stats, err
	}

	if _, err := writer.flush(); err != nil {
		return stats, err
	}
	if sync {
		if err := cells.flush(); err != nil {
			return stats, err
		}
	}
	return stats, nil
}

func (s *Store) CellRecord(ctx context.Context, hash []byte) (*storage.CellRecord, error) {
	return s.CellRecordInGeneration(ctx, 0, hash)
}

func (s *Store) CellRecordInGeneration(ctx context.Context, generation uint64, hash []byte) (*storage.CellRecord, error) {
	raw, err := s.getCellCopyFromGeneration(ctx, generation, hash)
	if err != nil {
		return nil, err
	}
	record, err := decodeCellRecord(hash, raw)
	if err != nil {
		return nil, err
	}
	return record, nil
}

func (s *Store) LoadCell(ctx context.Context, hash []byte) (*cell.Cell, error) {
	return s.loadLazyCell(ctx, hash)
}

func (s *Store) LazyCellLoader() cell.LazyCellLoader {
	return s.lazyCellLoaderForGeneration(0)
}

func (s *Store) LazyCellLoaderInGeneration(generation uint64) cell.LazyCellLoader {
	return s.lazyCellLoaderForGeneration(generation)
}

func (s *Store) lazyCellLoaderForGeneration(generation uint64) cell.LazyCellLoader {
	return func(hash cell.Hash) (*cell.Cell, error) {
		loaded, err := s.loadLazyCellFromGeneration(context.Background(), generation, hash[:])
		if err != nil {
			return nil, fmt.Errorf("load lazy cell %x: %w", hash[:], err)
		}
		return loaded, nil
	}
}

func (s *Store) loadLazyCell(ctx context.Context, hash []byte) (*cell.Cell, error) {
	return s.loadLazyCellFromGeneration(ctx, 0, hash)
}

func (s *Store) loadLazyCellFromGeneration(ctx context.Context, generation uint64, hash []byte) (*cell.Cell, error) {
	requestedGeneration := generation
	if generation == 0 {
		var err error
		generation, err = s.activeCellGenerationID()
		if err != nil {
			return nil, err
		}
	}

	cacheGeneration := generation
	loaderGeneration := generation
	if requestedGeneration == 0 {
		cacheGeneration = 0
		loaderGeneration = 0
	}
	if loaded, ok := s.cellCache.get(cacheGeneration, hash); ok {
		return loaded, nil
	}

	raw, err := s.getCellCopyFromGeneration(ctx, generation, hash)
	if err != nil {
		return nil, err
	}
	loaded, err := storage.LazyCellRecord(storage.DecodeCellRecordTrusted(hash, raw), s.lazyCellLoaderForGeneration(loaderGeneration))
	if err != nil {
		return nil, fmt.Errorf("create lazy cell %x: %w", hash, err)
	}
	s.cellCache.set(cacheGeneration, hash, loaded)
	return loaded, nil
}
