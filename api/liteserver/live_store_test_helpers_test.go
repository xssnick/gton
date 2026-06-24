package liteserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func (s *LiveStore) publishLiveBlockData(block ton.BlockIDExt, root *cell.Cell, data []byte, artifactFlushed bool) error {
	if root == nil {
		if len(data) == 0 {
			return errors.New("live block has no cell tree or BOC")
		}

		parsed, err := parseTrustedBlockBOC(block, data)
		if err != nil {
			return err
		}
		root = parsed
	}

	root, err := normalizeLiveBlockRoot(block, root)
	if err != nil {
		return err
	}
	meta, _ := storage.BuildBlockMetaFromBlockCell(block, root)

	return s.PublishLiveBlockArtifacts(storage.LiveBlockArtifacts{
		Block:           block,
		Root:            root,
		BlockData:       data,
		Meta:            meta,
		ArtifactFlushed: artifactFlushed,
	})
}

func (s *Server) loadStateRootWithBlockRoot(ctx context.Context, id ton.BlockIDExt) (*cell.Cell, *cell.Cell, error) {
	stateRootHash, err := s.loadStateRootHash(ctx, id)
	if err != nil {
		return nil, nil, err
	}

	blockRoot, err := s.loadBlockRoot(ctx, id)
	if err != nil {
		return nil, nil, fmt.Errorf("load block root for state %s: %w", storage.FormatBlockRef(id), err)
	}

	blockStateRootHash, err := stateRootHashFromBlock(id, blockRoot)
	if err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(blockStateRootHash, stateRootHash) {
		return nil, nil, fmt.Errorf("state root hash mismatch for %s: meta=%x block=%x", storage.FormatBlockRef(id), stateRootHash, blockStateRootHash)
	}

	root, err := s.store.LoadStateCellTree(ctx, id, stateRootHash)
	if err != nil {
		return nil, nil, fmt.Errorf("load state root %x for %s: %w", stateRootHash, storage.FormatBlockRef(id), err)
	}
	return root, blockRoot, nil
}
