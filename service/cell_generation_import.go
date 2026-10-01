package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"
	sharddomain "github.com/xssnick/gton/service/shard"
	state2 "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type generationStateCellImporter struct {
	cells storage.CellGeneration
}

func (i generationStateCellImporter) ImportStateCellTree(ctx context.Context, block ton.BlockIDExt, root *cell.Cell, totalCells uint64) (*cell.Cell, error) {
	return i.cells.ImportStateCellTree(ctx, block, root, totalCells)
}

func (i generationStateCellImporter) ImportStateBOCView(ctx context.Context, block ton.BlockIDExt, view *cell.BOCView) (*cell.Cell, error) {
	return i.cells.ImportStateBOCView(ctx, block, view)
}

func (i generationStateCellImporter) LoadStateCellTree(context.Context, ton.BlockIDExt, []byte) (*cell.Cell, error) {
	return nil, storage.ErrNotFound
}

func (i generationStateCellImporter) TrustImportedStateCellHashes() bool {
	return true
}

func (i generationStateCellImporter) ReuseImportedSplitStatePartCells() bool {
	return false
}

func (s *StateLifecycle) importSerializedPersistentCurrent(ctx context.Context, store cellGenerationStore, generation uint64, master ton.BlockIDExt, resume *cellGenerationCandidate) (*cellGenerationCandidate, error) {
	generationCells, err := store.Cells(generation)
	if err != nil {
		return nil, fmt.Errorf("select candidate cell generation %d: %w", generation, err)
	}

	importer := generationStateCellImporter{cells: generationCells}
	var current *storage.CurrentState
	var importedMaster *storage.BlockState
	if resume != nil && resume.current != nil && resume.current.Masterchain.Block.Equals(&master) {
		current = resume.current
		importedMaster = storage.CloneBlockState(&current.Masterchain)
		var err error
		importedMaster, err = parsedCellGenerationProgressState(importedMaster)
		if err != nil {
			return nil, fmt.Errorf("parse resumed masterchain persistent state %s: %w", storage.FormatBlockRef(master), err)
		}
		current.Masterchain = *storage.CloneBlockState(importedMaster)
	} else {
		masterState, err := s.persistentImportStateMetadata(ctx, store, master)
		if err != nil {
			return nil, fmt.Errorf("load serialized masterchain state %s: %w", storage.FormatBlockRef(master), err)
		}

		importedMaster, err = s.importSerializedPersistentBlockState(ctx, store, importer, masterState.Block, master, 0, masterState.StateRootHash)
		if err != nil {
			return nil, fmt.Errorf("import serialized masterchain persistent state: %w", err)
		}

		current = &storage.CurrentState{
			SyncedAt:         time.Now(),
			ShardClientSeqno: importedMaster.Block.SeqNo,
			Masterchain:      *storage.CloneBlockState(importedMaster),
			Shards:           map[storage.ShardKey]storage.BlockState{},
		}
		if err := store.SaveCellGenerationMigrationProgress(ctx, generation, current); err != nil {
			return nil, fmt.Errorf("save imported masterchain persistent state progress: %w", err)
		}
	}

	shards, err := state2.ShardBlocksFromMasterState(importedMaster)
	if err != nil {
		return nil, fmt.Errorf("load shard list from imported persistent state %s: %w", storage.FormatBlockRef(master), err)
	}

	splitDepths, err := state2.PersistentStateSplitDepths(importedMaster, shards)
	if err != nil {
		return nil, fmt.Errorf("load persistent state split depths for imported persistent state %s: %w", storage.FormatBlockRef(master), err)
	}
	if current.Shards == nil {
		current.Shards = make(map[storage.ShardKey]storage.BlockState, len(shards))
	}

	for _, shard := range shards {
		key := storage.ShardKeyFromBlock(shard)
		if state, ok := current.Shards[key]; ok && state.Block.Equals(&shard) {
			continue
		}

		canonicalShard, err := s.persistentImportStateMetadata(ctx, store, shard)
		if err != nil {
			return nil, fmt.Errorf("load serialized shard state metadata %s: %w", storage.FormatBlockRef(shard), err)
		}
		importedShard, err := s.importSerializedPersistentBlockState(ctx, store, importer, shard, master, splitDepths[shard.Workchain], canonicalShard.StateRootHash)
		if err != nil {
			return nil, fmt.Errorf("import serialized shard persistent state %s: %w", storage.FormatBlockRef(shard), err)
		}
		setShardStateMasterchainRef(importedShard, master)
		current.Shards[key] = *storage.CloneBlockState(importedShard)
		if err := store.SaveCellGenerationMigrationProgress(ctx, generation, current); err != nil {
			return nil, fmt.Errorf("save imported shard persistent state progress: %w", err)
		}
	}
	current.SyncedAt = time.Now()

	s.log.Info().
		Uint64("cell_generation", generation).
		Str("masterchain", storage.FormatBlockRef(current.Masterchain.Block)).
		Int("shards", len(current.Shards)).
		Msg("imported persistent state into next celldb generation")

	loader := generationCells.Loader()
	candidate := &cellGenerationCandidate{
		generation:      generation,
		generationCells: generationCells,
		loader:          loader,
		current:         current,
		cells:           newStateCellWindowCache(loader, &s.status.lazyCellLoads),
	}
	if err := store.SaveCellGenerationMigrationProgress(ctx, generation, current); err != nil {
		return nil, fmt.Errorf("save initial cell generation migration progress: %w", err)
	}
	return candidate, nil
}

func (s *StateLifecycle) cellGenerationPersistentImportComplete(candidate *cellGenerationCandidate) (bool, error) {
	master, err := parsedCellGenerationProgressState(&candidate.current.Masterchain)
	if err != nil {
		return false, fmt.Errorf("parse imported masterchain state %s: %w", storage.FormatBlockRef(candidate.current.Masterchain.Block), err)
	}

	shards, err := state2.ShardBlocksFromMasterState(master)
	if err != nil {
		return false, err
	}
	for _, shard := range shards {
		state, ok := candidate.current.Shards[storage.ShardKeyFromBlock(shard)]
		if !ok || !state.Block.Equals(&shard) {
			return false, nil
		}
	}
	return true, nil
}

func parsedCellGenerationProgressState(state *storage.BlockState) (*storage.BlockState, error) {
	if state.Parsed != nil {
		return storage.CloneBlockState(state), nil
	}
	if state.Cell == nil {
		return nil, fmt.Errorf("state cell is missing")
	}

	parsed, err := storage.ParseStateProof(&state.Block, state.Cell, nil, state.StateRootHash, nil)
	if err != nil {
		return nil, err
	}
	parsed.StateFileHash = bytes.Clone(state.StateFileHash)
	if state.MasterchainRef != nil {
		ref := *state.MasterchainRef
		parsed.MasterchainRef = &ref
	}
	return parsed, nil
}

func (s *StateLifecycle) loadCellGenerationMigrationProgress(ctx context.Context, store cellGenerationStore, generation uint64) (*cellGenerationCandidate, error) {
	generationCells, err := store.Cells(generation)
	if err != nil {
		return nil, err
	}

	current, err := store.CellGenerationMigrationProgress(ctx, generation)
	if err != nil {
		return nil, err
	}
	loader := generationCells.Loader()
	if err = s.loadCellGenerationMigrationProgressCells(ctx, loader, current); err != nil {
		if errors.Is(err, storage.ErrNotFound) {
			// Progress is only useful when every recorded current-state root is
			// present in the candidate generation. If the process stopped before
			// the progress sync became durable, rebuilding from the origin state is
			// slower but preserves correctness.
			return nil, storage.ErrNotFound
		}
		return nil, err
	}

	return &cellGenerationCandidate{
		generation:      generation,
		generationCells: generationCells,
		loader:          loader,
		current:         current,
		cells:           newStateCellWindowCache(loader, &s.status.lazyCellLoads),
	}, nil
}

func (s *StateLifecycle) loadCellGenerationMigrationProgressCells(ctx context.Context, loader cell.LazyCellLoader, current *storage.CurrentState) error {
	masterRoot, err := s.loadCellGenerationMigrationProgressBlock(ctx, loader, current.Masterchain)
	if err != nil {
		return fmt.Errorf("load migration progress masterchain state %s: %w", storage.FormatBlockRef(current.Masterchain.Block), err)
	}
	current.Masterchain.Cell = masterRoot

	for _, key := range storage.SortedShardKeys(current.Shards) {
		shard := current.Shards[key]
		root, err := s.loadCellGenerationMigrationProgressBlock(ctx, loader, shard)
		if err != nil {
			return fmt.Errorf("load migration progress shard state %s: %w", storage.FormatBlockRef(shard.Block), err)
		}
		shard.Cell = root
		current.Shards[key] = shard
	}
	return nil
}

func (s *StateLifecycle) loadCellGenerationMigrationProgressBlock(ctx context.Context, loader cell.LazyCellLoader, state storage.BlockState) (*cell.Cell, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}
	if len(state.StateRootHash) != 32 {
		return nil, fmt.Errorf("state root hash size is %d", len(state.StateRootHash))
	}

	var hash cell.Hash
	copy(hash[:], state.StateRootHash)
	root, err := loader(hash)
	if err != nil {
		return nil, err
	}
	rootHash := root.HashKeyAt(0)
	if !bytes.Equal(rootHash[:], state.StateRootHash) {
		return nil, fmt.Errorf("state root hash mismatch")
	}
	return root, nil
}

func (s *StateLifecycle) importSerializedPersistentBlockState(
	ctx context.Context,
	store persistentStateArtifactStore,
	importer generationStateCellImporter,
	block ton.BlockIDExt,
	master ton.BlockIDExt,
	splitDepth uint32,
	stateRootHash []byte,
) (*storage.BlockState, error) {
	artifact, err := s.artifactLoader.loadPersistentStateArtifact(
		ctx,
		store,
		block,
		master,
		splitDepth,
		stateRootHash,
	)
	if err != nil {
		return nil, err
	}
	state, err := artifact.ImportCells(ctx, importer)
	if err != nil {
		return nil, err
	}
	return state, nil
}

func (s *StateLifecycle) persistentImportStateMetadata(ctx context.Context, store cellGenerationStore, block ton.BlockIDExt) (*storage.BlockState, error) {
	meta, err := store.BlockMeta(ctx, block)
	if err != nil {
		return nil, err
	}
	if len(meta.StateRootHash) != 32 {
		return nil, fmt.Errorf("state root hash has size %d", len(meta.StateRootHash))
	}

	state := &storage.BlockState{
		Block:         block,
		StateRootHash: bytes.Clone(meta.StateRootHash),
		StateFileHash: bytes.Clone(meta.StateFileHash),
	}
	return state, nil
}

func (s *SyncCoordinator) loadPersistentStateArtifact(ctx context.Context, store persistentStateArtifactStore, block ton.BlockIDExt, master ton.BlockIDExt, splitDepth uint32, stateRootHash []byte) (storage.DownloadedState, error) {
	prefixLen, err := sharddomain.PrefixLength(block.Shard)
	if err != nil {
		return nil, fmt.Errorf("invalid block shard %016x: %w", uint64(block.Shard), err)
	}
	if block.Workchain == -1 || splitDepth <= prefixLen {
		file, err := store.PersistentStateFile(ctx, block, master, 0)
		if err != nil {
			if errors.Is(err, storage.ErrNotFound) {
				return nil, fmt.Errorf("%w: %s", errCellGenerationPersistentMissing, storage.FormatBlockRef(block))
			}
			return nil, err
		}
		return p2p.NewPersistentStateSnapshotArtifactFromFile(s.node, file)
	}

	return p2p.NewSplitPersistentStateSnapshotArtifactFromStoredFiles(ctx, s.node, block, master, splitDepth, stateRootHash, func(effectiveShard int64) (*storage.PersistentStateFile, error) {
		file, err := store.PersistentStateFile(ctx, block, master, effectiveShard)
		if err != nil && errors.Is(err, storage.ErrNotFound) {
			return nil, fmt.Errorf("%w: %s effective_shard=%016x", errCellGenerationPersistentMissing, storage.FormatBlockRef(block), uint64(effectiveShard))
		}
		return file, err
	})
}
