package service

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/p2p"
	tnstore "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type stateUpdateApplier interface {
	applyBlockStateUpdate(previous []*tnstore.BlockState, block PreparedBlock) (stateUpdateApplyResult, error)
}

type stateUpdateApplyResult struct {
	PreviousRoot *cell.Cell
	NextRoot     *cell.Cell
}

type appliedBlockState struct {
	PreviousRoot *cell.Cell
	Next         *tnstore.BlockState
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

func prepareStoredBlockForApply(id ton.BlockIDExt, full *tnstore.ServedBlockFull) (PreparedBlock, error) {
	started := time.Now()
	data := full.Block
	root, err := cell.FromBOC(data)
	if err != nil {
		return PreparedBlock{}, fmt.Errorf("parse stored block boc %s: %w", tnstore.FormatBlockRef(id), err)
	}
	// Keep the apply-time root invariant used by the reference node. The file
	// hash and block metadata were already verified before this artifact became
	// servable, so repeating those checks on every stored apply is unnecessary.
	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], id.RootHash) {
		return PreparedBlock{}, fmt.Errorf("stored block root hash mismatch for %s", tnstore.FormatBlockRef(id))
	}

	block, err := tnstore.ParseVerifiedBlockCell(id, root)
	if err != nil {
		return PreparedBlock{}, err
	}
	if block.StateUpdate == nil {
		return PreparedBlock{}, fmt.Errorf("stored block %s has no state update", tnstore.FormatBlockRef(id))
	}
	if full.Meta == nil {
		return PreparedBlock{}, fmt.Errorf("stored block %s has no metadata", tnstore.FormatBlockRef(id))
	}

	verified := VerifiedBlock{
		ID:          id,
		Kind:        "stored block",
		BlockRoot:   root,
		BlockBOC:    data,
		ProofBOC:    full.Proof,
		Meta:        full.Meta,
		StateUpdate: block.StateUpdate,
		IsLink:      full.IsLink,
	}
	prepared, err := prepareVerifiedBlockForApply(verified)
	if err != nil {
		return PreparedBlock{}, err
	}
	prepared.PrepareElapsed = time.Since(started)
	return prepared, nil
}

func loadStoredBlockForApply(ctx context.Context, store tnstore.Storage, id ton.BlockIDExt) (PreparedBlock, error) {
	full, err := store.BlockFull(ctx, id)
	if err != nil {
		return PreparedBlock{}, err
	}
	return prepareStoredBlockForApply(id, full)
}

func applyBlockWithPreviousStates(previous []*tnstore.BlockState, block PreparedBlock, applier stateUpdateApplier) (appliedBlockState, error) {
	stateUpdate := block.StateUpdate
	if stateUpdate == nil {
		return appliedBlockState{}, fmt.Errorf("prepared block %s has no state update", block.BlockRef())
	}

	var result stateUpdateApplyResult
	var err error
	if applier != nil {
		result, err = applier.applyBlockStateUpdate(previous, block)
	} else {
		result.PreviousRoot, err = previousStateRoot(previous)
		if err == nil {
			result.NextRoot, _, err = cell.ApplyMerkleUpdate(result.PreviousRoot, stateUpdate)
		}
	}
	if err != nil {
		return appliedBlockState{}, fmt.Errorf("apply state update for %s: %w", tnstore.FormatBlockRef(block.ID), err)
	}

	next, err := tnstore.ParseStateProof(&block.ID, result.NextRoot, nil, nil, nil)
	if err != nil {
		return appliedBlockState{}, fmt.Errorf("parse next state from %s: %w", tnstore.FormatBlockRef(block.ID), err)
	}
	return appliedBlockState{
		PreviousRoot: result.PreviousRoot,
		Next:         next,
	}, nil
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
