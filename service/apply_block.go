package service

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"time"

	"flexserver/service/p2p"
	tnstore "flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func prepareDownloadedBlock(downloaded p2p.DownloadedBlock) (p2p.DownloadedBlock, error) {
	if downloaded.Parsed == nil {
		root, err := downloadedBlockRoot(downloaded)
		if err != nil {
			return p2p.DownloadedBlock{}, err
		}

		block, err := tnstore.ParseVerifiedBlockCell(downloaded.ID, root)
		if err != nil {
			return p2p.DownloadedBlock{}, err
		}
		downloaded.Parsed = block
	}
	if err := tnstore.VerifyBlockIdentity(downloaded.ID, downloaded.Parsed); err != nil {
		return p2p.DownloadedBlock{}, err
	}
	if downloaded.Parsed.StateUpdate == nil {
		return p2p.DownloadedBlock{}, fmt.Errorf("block %s does not contain state update", tnstore.FormatBlockRef(downloaded.ID))
	}
	if downloaded.Meta != nil {
		return downloaded, nil
	}

	meta, err := tnstore.BuildBlockMetaFromParsedBlock(downloaded.ID, downloaded.Parsed)
	if err != nil {
		return p2p.DownloadedBlock{}, fmt.Errorf("build block meta %s: %w", downloaded.BlockRef(), err)
	}

	downloaded.Meta = meta
	return downloaded, nil
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

func prepareBlockDataForApply(kind string, id ton.BlockIDExt, data []byte) (p2p.DownloadedBlock, error) {
	root, err := cell.FromBOC(data)
	if err != nil {
		return p2p.DownloadedBlock{}, fmt.Errorf("parse %s boc %s: %w", kind, tnstore.FormatBlockRef(id), err)
	}
	rootHash := root.HashKey()
	if !bytes.Equal(rootHash[:], id.RootHash) {
		return p2p.DownloadedBlock{}, fmt.Errorf("%s root hash mismatch for %s", kind, tnstore.FormatBlockRef(id))
	}

	sum := sha256.Sum256(data)
	if !bytes.Equal(sum[:], id.FileHash) {
		return p2p.DownloadedBlock{}, fmt.Errorf("%s file hash mismatch for %s", kind, tnstore.FormatBlockRef(id))
	}

	downloaded := p2p.DownloadedBlock{
		ID:               id,
		Kind:             kind,
		Block:            root,
		BlockBOC:         data,
		VerifiedRootHash: true,
		VerifiedFileHash: true,
	}
	return prepareDownloadedBlock(downloaded)
}

func loadStoredBlockForApply(ctx context.Context, store tnstore.Storage, id ton.BlockIDExt, persistMeta bool) (p2p.DownloadedBlock, error) {
	data, err := store.BlockData(ctx, id)
	if err != nil {
		return p2p.DownloadedBlock{}, err
	}

	downloaded, err := prepareBlockDataForApply("stored block", id, data)
	if err != nil {
		return p2p.DownloadedBlock{}, err
	}
	if persistMeta {
		if err = store.SaveBlockMeta(downloaded.Meta); err != nil {
			return p2p.DownloadedBlock{}, fmt.Errorf("persist stored block meta %s: %w", tnstore.FormatBlockRef(id), err)
		}
	}
	return downloaded, nil
}

// ApplyBlock validates that downloaded contains a state transition from current
// and returns the next state embedded into block.state_update.
//
// TON blocks carry MERKLE_UPDATE, so the next state is reconstructed by
// applying block.state_update to the current state tree.
func ApplyBlock(current *tnstore.BlockState, downloaded p2p.DownloadedBlock) (*tnstore.BlockState, error) {
	return ApplyBlockWithPreviousStates([]*tnstore.BlockState{current}, downloaded)
}

func ApplyBlockWithPreviousStates(previous []*tnstore.BlockState, downloaded p2p.DownloadedBlock) (*tnstore.BlockState, error) {
	downloaded, err := prepareDownloadedBlock(downloaded)
	if err != nil {
		return nil, err
	}

	currentRoot, err := previousStateRoot(previous)
	if err != nil {
		return nil, err
	}

	nextRoot, reusedState, err := cell.ApplyMerkleUpdate(currentRoot, downloaded.Parsed.StateUpdate)
	if err != nil {
		return nil, fmt.Errorf("apply state update for %s: %w", tnstore.FormatBlockRef(downloaded.ID), err)
	}
	next, err := tnstore.ParseStateProof(&downloaded.ID, nextRoot, nil, nil, nil)
	if err != nil {
		return nil, fmt.Errorf("parse next state from %s: %w", tnstore.FormatBlockRef(downloaded.ID), err)
	}
	next.DownloadedAt = time.Now()
	next.ReusedStateCells = reusedState.Cells
	next.ReusedStateRefs = reusedState.Refs
	return next, nil
}

func previousStateRoot(previous []*tnstore.BlockState) (*cell.Cell, error) {
	switch len(previous) {
	case 1:
		current := previous[0]
		if current == nil {
			return nil, errors.New("current state is nil")
		}
		if current.Cell == nil {
			return nil, errors.New("current state cell is missing")
		}
		return current.Cell.Virtualize(0), nil
	case 2:
		left := previous[0]
		right := previous[1]
		if left == nil || right == nil {
			return nil, errors.New("merge previous state is nil")
		}
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
