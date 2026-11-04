package collator

import (
	"fmt"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// collatedProofTree unifies the selected cells of predecessor, neighbor and
// storage proofs by their virtual hash. The GV16 wire format shares that union
// across all roots and places its pruned boundaries at level 3.
type collatedProofTree struct {
	loaded map[cell.Hash]*cell.Cell
	built  map[cell.Hash]*cell.Cell
	seen   map[collatedProofVisit]struct{}
}

type collatedProofVisit struct {
	hash  cell.Hash
	level uint8
}

// encodeCollatedProofTrees converts the internal proof-provider format to the
// GV16 wire format. Block proofs keep their MerkleProof wrapper: they bind a
// block id to its state hash and are not declarations of state roots.
func encodeCollatedProofTrees(roots []*cell.Cell) ([]*cell.Cell, error) {
	tree := collatedProofTree{
		loaded: make(map[cell.Hash]*cell.Cell),
		built:  make(map[cell.Hash]*cell.Cell),
		seen:   make(map[collatedProofVisit]struct{}),
	}
	encoded := make([]*cell.Cell, 0, len(roots)*2+1)
	var proofRoots []*cell.Cell
	for _, root := range roots {
		proof := root
		tag := collatedRootStateTag
		if !root.IsSpecial() {
			loader, err := root.BeginParse()
			if err != nil {
				return nil, err
			}
			magic, err := loader.LoadUInt(32)
			if err != nil || magic != accountStorageDictProofTag {
				encoded = append(encoded, root)
				continue
			}
			proof, err = loader.LoadRefCell()
			if err != nil {
				return nil, err
			}
			tag = collatedRootStorageTag
		}
		virtual, err := unwrapCollatedProof(proof)
		if err != nil {
			return nil, fmt.Errorf("decode collated proof for GV16: %w", err)
		}
		if tag == collatedRootStateTag {
			loader, err := virtual.BeginParse()
			if err != nil {
				return nil, err
			}
			magic, err := loader.LoadUInt(32)
			if err == nil && magic == blockTag {
				encoded = append(encoded, root)
				continue
			}
		}
		body, err := proof.PeekRef(0)
		if err != nil {
			return nil, err
		}
		if err = tree.collect(body, 0); err != nil {
			return nil, err
		}
		proofRoots = append(proofRoots, virtual)
		encoded = append(encoded, cell.BeginCell().MustStoreUInt(tag, 32).MustStoreSlice(virtual.Hash(), 256).EndCell())
	}
	encoded = append(encoded, cell.BeginCell().MustStoreUInt(collatedSeparatorTag, 32).EndCell())
	for _, root := range proofRoots {
		proof, err := tree.build(root)
		if err != nil {
			return nil, fmt.Errorf("build GV16 collated proof: %w", err)
		}
		encoded = append(encoded, proof)
	}

	return encoded, nil
}

func (t *collatedProofTree) collect(root *cell.Cell, level uint8) error {
	visit := collatedProofVisit{hash: root.HashKey(), level: level}
	if _, seen := t.seen[visit]; seen {
		return nil
	}
	t.seen[visit] = struct{}{}
	if root.GetType() == cell.PrunedCellType && root.Level() > int(level) {
		return nil
	}
	virtual := root.Virtualize(level)
	t.loaded[virtual.HashKey()] = virtual
	childLevel := level
	if root.GetType() == cell.MerkleProofCellType || root.GetType() == cell.MerkleUpdateCellType {
		childLevel++
	}
	for i := uint(0); i < root.RefsNum(); i++ {
		child, err := root.PeekRef(int(i))
		if err != nil {
			return err
		}
		if err = t.collect(child, childLevel); err != nil {
			return err
		}
	}

	return nil
}

func (t *collatedProofTree) build(source *cell.Cell) (*cell.Cell, error) {
	hash := source.HashKey()
	if built := t.built[hash]; built != nil {
		return built, nil
	}
	loaded := t.loaded[hash]
	if loaded == nil {
		pruned, err := cell.CreatePrunedBranch(source, 3, source.Level())
		if err != nil {
			return nil, err
		}
		t.built[hash] = pruned
		return pruned, nil
	}
	var refs [4]*cell.Cell
	for i := uint(0); i < loaded.RefsNum(); i++ {
		ref, err := loaded.PeekRef(int(i))
		if err != nil {
			return nil, err
		}
		refs[i], err = t.build(ref)
		if err != nil {
			return nil, err
		}
	}
	built, err := loaded.RebuildWithRefs(refs[:loaded.RefsNum()])
	if err != nil {
		return nil, err
	}
	t.built[hash] = built

	return built, nil
}
