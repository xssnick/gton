package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type stateUpdateApplier interface {
	applyBlockStateUpdate(previous []*tnstore.BlockState, block PreparedBlock) (*cell.Cell, error)
}

func downloadedBlockRoot(downloaded p2p.DownloadedBlock) (*cell.Cell, error) {
	root := downloaded.Block
	if root == nil {
		return nil, fmt.Errorf("downloaded block %s is missing parsed cell", downloaded.BlockRef())
	}

	if !downloaded.IsLink || root.GetType() != cell.MerkleProofCellType {
		return root, nil
	}

	unwrapped, err := cell.UnwrapProof(root, downloaded.ID.RootHash)
	if err != nil {
		return nil, fmt.Errorf("unwrap merkle proof link for %s: %w", downloaded.BlockRef(), err)
	}
	return unwrapped, nil
}

func prepareBlockData(kind string, id ton.BlockIDExt, data []byte) (VerifiedBlock, error) {
	root, err := cell.FromBOC(data)
	if err != nil {
		return VerifiedBlock{}, fmt.Errorf("parse %s boc %s: %w", kind, tnstore.FormatBlockRef(id), err)
	}
	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], id.RootHash) {
		return VerifiedBlock{}, fmt.Errorf("%s root hash mismatch for %s", kind, tnstore.FormatBlockRef(id))
	}

	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], id.FileHash) {
		return VerifiedBlock{}, fmt.Errorf("%s file hash mismatch for %s", kind, tnstore.FormatBlockRef(id))
	}

	block, err := tnstore.ParseVerifiedBlockCell(id, root)
	if err != nil {
		return VerifiedBlock{}, err
	}
	if block.StateUpdate == nil {
		return VerifiedBlock{}, fmt.Errorf("%s block %s has no state update", kind, tnstore.FormatBlockRef(id))
	}
	meta, err := tnstore.BuildBlockMetaFromParsedBlock(id, block)
	if err != nil {
		return VerifiedBlock{}, fmt.Errorf("build %s block meta %s: %w", kind, tnstore.FormatBlockRef(id), err)
	}

	return VerifiedBlock{
		ID:          id,
		Kind:        kind,
		BlockRoot:   root,
		BlockBOC:    data,
		Meta:        meta,
		StateUpdate: block.StateUpdate,
	}, nil
}

func prepareBlockDataForApply(kind string, id ton.BlockIDExt, data []byte) (PreparedBlock, error) {
	started := time.Now()
	verified, err := prepareBlockData(kind, id, data)
	if err != nil {
		return PreparedBlock{}, err
	}
	prepared, err := prepareVerifiedBlockForApply(verified)
	if err != nil {
		return PreparedBlock{}, err
	}
	prepared.PrepareElapsed = time.Since(started)
	return prepared, nil
}

func loadStoredBlockForApply(ctx context.Context, store tnstore.Storage, id ton.BlockIDExt, persistMeta bool) (PreparedBlock, error) {
	full, err := store.BlockFull(ctx, id)
	if err != nil {
		return PreparedBlock{}, err
	}

	downloaded, err := prepareBlockDataForApply("stored block", id, full.Block)
	if err != nil {
		return PreparedBlock{}, err
	}
	downloaded.ProofBOC = full.Proof
	downloaded.IsLink = full.IsLink
	if full.Meta != nil {
		downloaded.Meta = tnstore.MergeBlockMeta(downloaded.Meta, full.Meta)
		downloaded.Meta.ID = id
	}
	if persistMeta {
		if err = store.SaveBlockMeta(blockMetaWithoutArtifactFlags(downloaded.Meta)); err != nil {
			return PreparedBlock{}, fmt.Errorf("persist stored block meta %s: %w", tnstore.FormatBlockRef(id), err)
		}
	}
	return downloaded, nil
}

func blockMetaWithoutArtifactFlags(meta *tnstore.BlockMeta) *tnstore.BlockMeta {
	cloned := meta.Clone()
	cloned.Flags &^= tnstore.BlockMetaHasServedFull |
		tnstore.BlockMetaServedFullIsLink |
		tnstore.BlockMetaHasBlockData |
		tnstore.BlockMetaHasProofBlock |
		tnstore.BlockMetaHasProofBlockLink |
		tnstore.BlockMetaHasProofKeyBlock |
		tnstore.BlockMetaHasProofKeyBlockLink
	return cloned
}

// ApplyBlock validates that downloaded contains a state transition from current
// and returns the next state embedded into block.state_update.
//
// TON blocks carry MERKLE_UPDATE, so the next state is reconstructed by
// applying block.state_update to the current state tree.
func ApplyBlock(current *tnstore.BlockState, block PreparedBlock) (*tnstore.BlockState, error) {
	return ApplyBlockWithPreviousStates([]*tnstore.BlockState{current}, block)
}

func ApplyBlockWithPreviousStates(previous []*tnstore.BlockState, block PreparedBlock) (*tnstore.BlockState, error) {
	return applyBlockWithPreviousStates(previous, block, nil)
}

func applyBlockWithPreviousStates(previous []*tnstore.BlockState, block PreparedBlock, applier stateUpdateApplier) (*tnstore.BlockState, error) {
	stateUpdate := block.StateUpdate
	if stateUpdate == nil {
		return nil, fmt.Errorf("prepared block %s has no state update", block.BlockRef())
	}

	var nextRoot *cell.Cell
	var err error
	if applier != nil {
		nextRoot, err = applier.applyBlockStateUpdate(previous, block)
	} else {
		var currentRoot *cell.Cell
		currentRoot, err = previousStateRoot(previous)
		if err == nil {
			nextRoot, _, err = cell.ApplyMerkleUpdate(currentRoot, stateUpdate)
		}
	}
	if err != nil {
		return nil, fmt.Errorf("apply state update for %s: %w", tnstore.FormatBlockRef(block.ID), err)
	}

	next, err := tnstore.ParseStateProof(&block.ID, nextRoot, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("parse next state from %s: %w", tnstore.FormatBlockRef(block.ID), err)
	}
	return next, nil
}

func previousStateRoot(previous []*tnstore.BlockState) (*cell.Cell, error) {
	switch len(previous) {
	case 1:
		current := previous[0]
		if current.Cell == nil {
			return nil, errors.New("current state cell is missing")
		}
		return current.Cell.Virtualize(0), nil
	case 2:
		left := previous[0]
		right := previous[1]
		if left.Cell == nil || right.Cell == nil {
			return nil, errors.New("merge previous state cell is missing")
		}
		return cell.BeginCell().
			MustStoreUInt(0x5f327da5, 32).
			MustStoreRef(left.Cell.Virtualize(0)).
			MustStoreRef(right.Cell.Virtualize(0)).
			EndCell(), nil
	default:
		return nil, fmt.Errorf("unsupported previous state count %d", len(previous))
	}
}
