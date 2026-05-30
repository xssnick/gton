package liteserver

import (
	"fmt"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type lazyCellLoaderStore interface {
	LazyCellLoader() cell.LazyCellLoader
}

func nonfinalStateFromUpdate(block storage.LiveBlockArtifacts, loader cell.LazyCellLoader) (*storage.BlockState, *storage.BlockMeta, map[cell.Hash]*cell.Cell, error) {
	if block.State != nil {
		state := storage.CloneBlockState(block.State)
		cells, err := nonfinalStateCells(state.Cell)
		if err != nil {
			return nil, nil, nil, err
		}
		lazy, err := nonfinalLazyCellRoot(state.Cell, loader)
		if err != nil {
			return nil, nil, nil, err
		}
		state.Cell = lazy
		return state, block.Meta.Clone(), cells, nil
	}

	root := block.Root
	if root == nil && len(block.BlockData) > 0 {
		parsed, err := parseTrustedBlockBOC(block.Block, block.BlockData)
		if err != nil {
			return nil, nil, nil, err
		}
		root = parsed
	}
	if root == nil {
		return nil, block.Meta.Clone(), nil, nil
	}
	normalized, err := normalizeLiveBlockRoot(block.Block, root)
	if err != nil {
		return nil, nil, nil, err
	}
	root = normalized

	meta := block.Meta
	if meta == nil {
		var err error
		meta, err = storage.BuildBlockMetaFromBlockCell(block.Block, root)
		if err != nil {
			return nil, nil, nil, err
		}
	}
	if meta == nil || len(meta.StateRootHash) != 32 {
		return nil, meta.Clone(), nil, nil
	}

	parsed, err := storage.ParseVerifiedBlockCell(block.Block, root)
	if err != nil {
		return nil, nil, nil, err
	}
	if parsed.StateUpdate == nil {
		return nil, meta.Clone(), nil, nil
	}
	if err = cell.ValidateMerkleUpdate(parsed.StateUpdate); err != nil {
		return nil, nil, nil, fmt.Errorf("validate non-final state update: %w", err)
	}

	stateRoot, err := parsed.StateUpdate.PeekRef(1)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("load non-final state update target: %w", err)
	}
	cells, err := nonfinalStateCells(stateRoot)
	if err != nil {
		return nil, nil, nil, err
	}
	stateRoot, err = nonfinalLazyCellRoot(stateRoot, loader)
	if err != nil {
		return nil, nil, nil, err
	}

	return &storage.BlockState{
		Block:         block.Block,
		StateRootHash: meta.StateRootHash,
		Cell:          stateRoot,
	}, meta.Clone(), cells, nil
}

func nonfinalLazyCellRoot(root *cell.Cell, loader cell.LazyCellLoader) (*cell.Cell, error) {
	if root == nil {
		return nil, nil
	}
	if root.RefsNum() == 0 {
		return root, nil
	}
	return nonfinalLazyCell(root, loader)
}

func nonfinalStateCells(root *cell.Cell) (map[cell.Hash]*cell.Cell, error) {
	if root == nil {
		return nil, nil
	}

	cells := map[cell.Hash]*cell.Cell{}
	if err := rememberNonfinalCells(cells, root); err != nil {
		return nil, err
	}
	return cells, nil
}

func rememberNonfinalCells(cells map[cell.Hash]*cell.Cell, root *cell.Cell) error {
	if root.GetType() == cell.PrunedCellType {
		return nil
	}

	hash := root.HashKey()
	if _, ok := cells[hash]; ok {
		return nil
	}
	cells[hash] = root

	for i := 0; i < int(root.RefsNum()); i++ {
		ref, err := root.PeekRef(i)
		if err != nil {
			return fmt.Errorf("load non-final state cell ref %d: %w", i, err)
		}
		if err := rememberNonfinalCells(cells, ref); err != nil {
			return err
		}
	}
	return nil
}

func nonfinalLazyCell(root *cell.Cell, loader cell.LazyCellLoader) (*cell.Cell, error) {
	meta := root.GetMetadata()
	refs := make([]cell.LazyRef, len(meta.Refs))
	for i, ref := range meta.Refs {
		refs[i] = cell.LazyRef{
			LevelMask: ref.LevelMask,
			Hashes:    flattenCellHashes(ref.Hashes),
			Depths:    append([]uint16(nil), ref.Depths...),
		}
	}

	lazy, err := cell.CreateWithLazyRefsUnsafe(
		nonfinalCellDescriptors(root),
		nonfinalSerializedCellData(root),
		flattenCellHashes(meta.Hashes),
		append([]uint16(nil), meta.Depths...),
		refs,
		loader,
	)
	if err != nil {
		return nil, err
	}
	return lazy, nil
}

func nonfinalCellDescriptors(root *cell.Cell) uint16 {
	d1 := byte(root.RefsNum())
	if root.IsSpecial() {
		d1 |= 8
	}
	d1 |= root.LevelMask().Mask * 32

	ln := (root.BitsSize() / 8) * 2
	if root.BitsSize()%8 != 0 {
		ln++
	}
	return uint16(d1)<<8 | uint16(byte(ln))
}

func nonfinalSerializedCellData(root *cell.Cell) []byte {
	data := make([]byte, root.SerializedBOCBodySize())
	root.SerializeBOCBodyTo(data)
	return data
}

func flattenCellHashes(hashes []cell.Hash) []byte {
	if len(hashes) == 0 {
		return nil
	}

	out := make([]byte, 0, len(hashes)*32)
	for _, hash := range hashes {
		out = append(out, hash[:]...)
	}
	return out
}
