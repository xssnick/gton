package storage

import "github.com/xssnick/tonutils-go/tvm/cell"

// stateCellRecordView keeps raw, untraced projections on the traversal stack
// instead of allocating a Cell wrapper for every edge of a Merkle update.
// level is the effective level, not the highest bit of the projected mask:
// sparse masks can have a lower visible level without changing child views.
type stateCellRecordView struct {
	cell      *cell.Cell
	level     uint8
	projected bool
}

func (v stateCellRecordView) virtualize(level uint8) stateCellRecordView {
	if v.levelMask().GetLevel() <= int(level) {
		return v
	}

	// Existing library views, lazy loaders and traces carry semantics beyond
	// the level projection. Let the library preserve that metadata.
	if v.cell.IsVirtualized() || v.cell.IsLazy() || v.cell.Trace() != nil {
		return stateCellRecordView{cell: v.cell.Virtualize(level)}
	}

	v.level = level
	v.projected = true
	return v
}

func (v stateCellRecordView) levelMask() cell.LevelMask {
	mask := v.cell.LevelMask()
	if v.projected {
		return mask.Apply(int(v.level))
	}
	return mask
}

func (v stateCellRecordView) effectiveLevel() int {
	if v.projected {
		return int(v.level)
	}

	return v.cell.EffectiveLevel()
}

func (v stateCellRecordView) hash() cell.Hash {
	level := -1
	if v.projected {
		level = int(v.level)
	}

	return v.cell.HashKeyAt(level)
}

// hashAt and depth take the nonnegative levels visited by the record encoder.
func (v stateCellRecordView) hashAt(level int) cell.Hash {
	if v.projected {
		level = min(level, int(v.level))
	}

	return v.cell.HashKeyAt(level)
}

func (v stateCellRecordView) depth(level int) uint16 {
	if v.projected {
		level = min(level, int(v.level))
	}

	return v.cell.Depth(level)
}

func (v stateCellRecordView) childLevel() uint8 {
	level := v.cell.Level()
	if v.projected {
		level = int(v.level)
	}
	switch v.cell.GetType() {
	case cell.MerkleProofCellType, cell.MerkleUpdateCellType:
		level++
	}

	return uint8(level)
}

func (v stateCellRecordView) ref(i int, childLevel uint8) (stateCellRecordView, error) {
	ref, err := v.cell.PeekRef(i)
	if err != nil {
		return stateCellRecordView{}, err
	}

	view := stateCellRecordView{cell: ref}
	if v.projected {
		view = view.virtualize(childLevel)
	}

	return view, nil
}

func (v stateCellRecordView) logicalRef(ref stateCellRecordView, childLevel uint8) stateCellRecordView {
	if v.projected || v.cell.IsVirtualized() {
		return ref
	}

	// A raw parent is a view at its OWN level, not at zero. Inside a nested
	// Merkle proof, projecting its children to zero would discard level-1
	// hashes/depths and make the persisted record fail lazy-load validation.
	return ref.virtualize(childLevel)
}

func (v stateCellRecordView) materialize() *cell.Cell {
	if v.projected {
		return v.cell.Virtualize(v.level)
	}

	return v.cell
}
