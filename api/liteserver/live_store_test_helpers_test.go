package liteserver

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type LiveStore struct {
	*liveview.Store
}

type LiveStoreOptions = liveview.Options
type CurrentAccountBlockIDs = liveview.CurrentAccountBlockIDs
type liveBlockFragments = liveview.BlockView

var (
	errWaitMasterchainTimeout = liveview.ErrWaitMasterchainTimeout
	errWaitMasterchainTooFar  = liveview.ErrWaitMasterchainTooFar
)

func NewLiveStore(store liveview.Backing, opts ...LiveStoreOptions) *LiveStore {
	return &LiveStore{Store: liveview.New(store, opts...)}
}

func buildLiveBlockFragments(block ton.BlockIDExt, blockRoot *cell.Cell, stateRoot *cell.Cell) (*liveview.BlockView, error) {
	return liveview.NewBlockView(block, blockRoot, stateRoot)
}

func parseTrustedBlockBOC(id ton.BlockIDExt, data []byte) (*cell.Cell, error) {
	return liveview.ParseTrustedBlockBOC(id, data)
}

func currentAccountBlocksFromState(current *storage.CurrentState, workchain int32, account []byte) (CurrentAccountBlockIDs, error) {
	if current == nil {
		return CurrentAccountBlockIDs{}, storage.ErrNotFound
	}

	master := current.Masterchain.Block
	if workchain == masterchainID {
		return CurrentAccountBlockIDs{Master: master, Account: master}, nil
	}
	if len(account) != 32 {
		return CurrentAccountBlockIDs{}, storage.ErrNotFound
	}

	candidates := storage.AccountShardCandidates(workchain, account)
	for i := len(candidates) - 1; i >= 0; i-- {
		key := storage.ShardKey{Workchain: workchain, Shard: candidates[i]}
		if shard, ok := current.Shards[key]; ok {
			return CurrentAccountBlockIDs{Master: master, Account: shard.Block}, nil
		}
	}
	return CurrentAccountBlockIDs{}, storage.ErrNotFound
}

func (s *LiveStore) publishLiveBlockData(block ton.BlockIDExt, root *cell.Cell, data []byte, artifactFlushed bool) error {
	if root == nil {
		if len(data) == 0 {
			return errors.New("live block has no cell tree or BOC")
		}

		parsed, err := cell.FromBOC(data)
		if err != nil {
			return err
		}
		root = parsed
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
