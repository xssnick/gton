package liveview

import (
	"bytes"
	"fmt"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type nonfinalParsedStateUpdate struct {
	root   *cell.Cell
	meta   *storage.BlockMeta
	update *cell.Cell
}

func nonfinalStateFromSnapshot(block storage.LiveBlockArtifacts, loader cell.LazyCellLoader) (*storage.BlockState, *storage.BlockMeta, storage.StateCellRecords, error) {
	state := storage.CloneBlockState(block.State)
	if state.Cell == nil {
		return nil, nil, storage.StateCellRecords{}, storage.ErrNotFound
	}
	records, err := nonfinalSnapshotStateRecords(state.Cell)
	if err != nil {
		return nil, nil, storage.StateCellRecords{}, err
	}

	rootHash := state.Cell.GetMetadata().Hash
	lazy, err := nonfinalLoadStateRoot(rootHash, records, loader)
	if err != nil {
		return nil, nil, storage.StateCellRecords{}, err
	}
	state.Cell = lazy
	return state, block.Meta.Clone(), records, nil
}

// nonfinalParseStateUpdate extracts and validates the block's Merkle state
// update. updateTrusted marks a publisher-provided StateUpdate as coming from
// a signed, hash-anchored block whose update needs no standalone validation;
// candidate-origin updates are unsigned, so they are always validated here.
func nonfinalParseStateUpdate(block storage.LiveBlockArtifacts, updateTrusted bool) (nonfinalParsedStateUpdate, error) {
	root := block.Root
	if root == nil && len(block.BlockData) > 0 {
		parsed, err := ParseTrustedBlockBOC(block.Block, block.BlockData)
		if err != nil {
			return nonfinalParsedStateUpdate{}, err
		}
		root = parsed
	}
	if root == nil {
		return nonfinalParsedStateUpdate{meta: block.Meta.Clone()}, nil
	}
	normalized, err := normalizeLiveBlockRoot(block.Block, root)
	if err != nil {
		return nonfinalParsedStateUpdate{}, err
	}
	root = normalized

	meta := block.Meta
	stateUpdate := block.StateUpdate
	updateValidated := stateUpdate != nil && updateTrusted
	if meta == nil {
		parsed, err := storage.ParseVerifiedBlockCell(block.Block, root)
		if err != nil {
			return nonfinalParsedStateUpdate{}, err
		}
		meta, err = storage.BuildBlockMetaFromParsedBlock(block.Block, parsed)
		if err != nil {
			return nonfinalParsedStateUpdate{}, err
		}
		if stateUpdate == nil {
			stateUpdate = parsed.StateUpdate
		}
	}
	if meta == nil || len(meta.StateRootHash) != 32 {
		return nonfinalParsedStateUpdate{root: root, meta: meta.Clone()}, nil
	}

	if stateUpdate == nil {
		parsed, err := storage.ParseVerifiedBlockCell(block.Block, root)
		if err != nil {
			return nonfinalParsedStateUpdate{}, err
		}
		stateUpdate = parsed.StateUpdate
	}
	if stateUpdate == nil {
		return nonfinalParsedStateUpdate{root: root, meta: meta.Clone()}, nil
	}
	if !updateValidated {
		if err = cell.ValidateMerkleUpdate(stateUpdate); err != nil {
			return nonfinalParsedStateUpdate{}, fmt.Errorf("validate non-final state update: %w", err)
		}
	}

	return nonfinalParsedStateUpdate{
		root:   root,
		meta:   meta.Clone(),
		update: stateUpdate,
	}, nil
}

func nonfinalStateFromParsedUpdate(block storage.LiveBlockArtifacts, parsed nonfinalParsedStateUpdate, loader cell.LazyCellLoader) (*storage.BlockState, storage.StateCellRecords, error) {
	if parsed.update == nil {
		return nil, storage.StateCellRecords{}, nil
	}

	updateTo, err := parsed.update.PeekRef(1)
	if err != nil {
		return nil, storage.StateCellRecords{}, fmt.Errorf("load non-final state update target: %w", err)
	}
	stateRoot := updateTo.Virtualize(0)
	stateRootHash := stateRoot.GetMetadata().Hash
	if parsed.meta != nil && len(parsed.meta.StateRootHash) == 32 && !bytes.Equal(parsed.meta.StateRootHash, stateRootHash[:]) {
		return nil, storage.StateCellRecords{}, fmt.Errorf("non-final state root hash mismatch: meta=%x root=%x", parsed.meta.StateRootHash, stateRootHash[:])
	}

	records, err := storage.PrepareStateUpdateCells(parsed.update)
	if err != nil {
		return nil, storage.StateCellRecords{}, err
	}
	if !records.Has(stateRootHash) {
		return nil, storage.StateCellRecords{}, fmt.Errorf("prepared non-final state update cells do not contain destination root %x", stateRootHash[:])
	}

	stateRoot, err = nonfinalLoadStateRoot(stateRootHash, records, loader)
	if err != nil {
		return nil, storage.StateCellRecords{}, err
	}

	return &storage.BlockState{
		Block:         block.Block,
		StateRootHash: append([]byte(nil), stateRootHash[:]...),
		Cell:          stateRoot,
	}, records, nil
}

func nonfinalStateUpdateApplies(previous []*storage.BlockState, update *cell.Cell) error {
	currentRoot, err := nonfinalPreviousStateRoot(previous)
	if err != nil {
		return err
	}
	if err = cell.MayApplyMerkleUpdate(currentRoot, update); err != nil {
		return fmt.Errorf("non-final state update does not match previous state: %w", err)
	}
	return nil
}

func nonfinalPreviousStateRoot(previous []*storage.BlockState) (*cell.Cell, error) {
	switch len(previous) {
	case 1:
		return nonfinalStateRoot(previous[0])
	case 2:
		left, err := nonfinalStateRoot(previous[0])
		if err != nil {
			return nil, fmt.Errorf("load left merge state root: %w", err)
		}
		right, err := nonfinalStateRoot(previous[1])
		if err != nil {
			return nil, fmt.Errorf("load right merge state root: %w", err)
		}

		return cell.BeginCell().
			MustStoreUInt(0x5f327da5, 32).
			MustStoreRef(left).
			MustStoreRef(right).
			EndCell(), nil
	default:
		return nil, fmt.Errorf("unsupported previous state count %d", len(previous))
	}
}

func nonfinalStateRoot(state *storage.BlockState) (*cell.Cell, error) {
	if state == nil || state.Cell == nil {
		return nil, fmt.Errorf("previous state cell is missing")
	}

	root := state.Cell.Virtualize(0)
	if len(state.StateRootHash) > 0 {
		hash := root.HashKey(0)
		if !bytes.Equal(hash[:], state.StateRootHash) {
			return nil, fmt.Errorf("previous state root hash mismatch for %s: got=%x want=%x", storage.FormatBlockRef(state.Block), hash[:], state.StateRootHash)
		}
	}
	return root, nil
}

func nonfinalRecordOverlayLoader(records storage.StateCellRecords, base cell.LazyCellLoader) cell.LazyCellLoader {
	var loader cell.LazyCellLoader
	loader = func(hash cell.Hash) (*cell.Cell, error) {
		if data := records.Data(hash); len(data) > 0 {
			return nonfinalLazyCellRecord(hash, data, loader)
		}
		if base == nil {
			return nil, cell.ErrLazyRefNotFound
		}
		return base(hash)
	}
	return loader
}

func nonfinalLoadStateRoot(hash cell.Hash, records storage.StateCellRecords, base cell.LazyCellLoader) (*cell.Cell, error) {
	return nonfinalRecordOverlayLoader(records, base)(hash)
}

func nonfinalLazyCellRecord(hash cell.Hash, encoded []byte, loader cell.LazyCellLoader) (*cell.Cell, error) {
	return storage.LazyCellRecord(storage.DecodeCellRecordTrusted(hash[:], encoded), loader)
}

func nonfinalSnapshotStateRecords(root *cell.Cell) (storage.StateCellRecords, error) {
	if root == nil {
		return storage.StateCellRecords{}, nil
	}

	records := make([]storage.EncodedCellRecord, 0)
	seen := map[cell.Hash]struct{}{}
	stack := []*cell.Cell{root}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current == nil {
			continue
		}
		if current.IsLazy() {
			loader, err := current.BeginParse()
			if err != nil {
				return storage.StateCellRecords{}, fmt.Errorf("load non-final snapshot cell %x: %w", current.Hash(), err)
			}
			current = loader.BaseCell()
		}
		if current.GetType() == cell.PrunedCellType {
			continue
		}

		meta := current.GetMetadata()
		if _, ok := seen[meta.Hash]; ok {
			continue
		}
		record, err := storage.PrepareEncodedCellRecordFromCellMetadata(current, meta)
		if err != nil {
			return storage.StateCellRecords{}, fmt.Errorf("build non-final snapshot cell record %x: %w", meta.Hash[:], err)
		}
		seen[meta.Hash] = struct{}{}
		records = append(records, record)

		for i, ref := range meta.Refs {
			if ref.Lazy {
				continue
			}
			refCell, err := current.PeekRef(i)
			if err != nil {
				return storage.StateCellRecords{}, fmt.Errorf("load non-final snapshot ref %d from %x: %w", i, meta.Hash[:], err)
			}
			stack = append(stack, refCell)
		}
	}
	return storage.NewStateCellRecords(records), nil
}
