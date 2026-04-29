package memstore

import (
	"bytes"
	"context"
	"flexserver/service/storage"
	"fmt"
	"sync"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type StateStore struct {
	mx       sync.RWMutex
	current  *storage.CurrentState
	progress *storage.CurrentState
	blocks   map[string]*storage.BlockState
	trees    map[string]stateCellTree
}

type stateCellTree struct {
	root  *cell.Cell
	cells uint64
}

func NewStateStore() *StateStore {
	return &StateStore{
		blocks: map[string]*storage.BlockState{},
		trees:  map[string]stateCellTree{},
	}
}

func (s *StateStore) SaveCurrentState(_ context.Context, state *storage.CurrentState) error {
	if state == nil {
		return fmt.Errorf("current state is nil")
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	cloned := storage.CloneCurrentState(state)
	s.current = cloned
	s.blocks[storage.BlockKey(cloned.Masterchain.Block)] = storage.CloneBlockState(&cloned.Masterchain)
	for _, shard := range cloned.Shards {
		s.blocks[storage.BlockKey(shard.Block)] = storage.CloneBlockState(&shard)
	}
	return nil
}

func (s *StateStore) SaveBlockStateAndCurrentState(_ context.Context, block *storage.BlockState, current *storage.CurrentState) error {
	if block == nil {
		return s.SaveBlockStatesAndCurrentState(context.Background(), nil, current)
	}
	return s.SaveBlockStatesAndCurrentState(context.Background(), []*storage.BlockState{block}, current)
}

func (s *StateStore) SaveBlockStatesAndCurrentState(_ context.Context, blocks []*storage.BlockState, current *storage.CurrentState) error {
	if current == nil {
		return fmt.Errorf("current state is nil")
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	for _, block := range blocks {
		if block == nil {
			continue
		}
		clonedBlock := storage.CloneBlockState(block)
		fillBlockStateHashes(clonedBlock)
		s.blocks[storage.BlockKey(clonedBlock.Block)] = clonedBlock
	}

	cloned := storage.CloneCurrentState(current)
	s.current = cloned
	s.blocks[storage.BlockKey(cloned.Masterchain.Block)] = storage.CloneBlockState(&cloned.Masterchain)
	for _, shard := range cloned.Shards {
		s.blocks[storage.BlockKey(shard.Block)] = storage.CloneBlockState(&shard)
	}
	return nil
}

func (s *StateStore) CurrentState(_ context.Context) (*storage.CurrentState, error) {
	s.mx.RLock()
	defer s.mx.RUnlock()

	if s.current == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneCurrentState(s.current), nil
}

func (s *StateStore) SaveStateSyncProgress(_ context.Context, state *storage.CurrentState) error {
	if state == nil {
		return fmt.Errorf("state sync progress is nil")
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	s.progress = storage.CloneCurrentState(state)
	return nil
}

func (s *StateStore) StateSyncProgress(_ context.Context) (*storage.CurrentState, error) {
	s.mx.RLock()
	defer s.mx.RUnlock()

	if s.progress == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneCurrentState(s.progress), nil
}

func (s *StateStore) ClearStateSyncProgress(_ context.Context) error {
	s.mx.Lock()
	defer s.mx.Unlock()

	s.progress = nil
	return nil
}

func (s *StateStore) ImportStateCellTree(_ context.Context, block ton.BlockIDExt, root *cell.Cell, _ []cell.Cell, totalCells uint64) (*cell.Cell, error) {
	if root == nil {
		return nil, fmt.Errorf("state cell tree root is nil")
	}
	s.mx.Lock()
	s.trees[storage.BlockKey(block)] = stateCellTree{
		root:  root,
		cells: totalCells,
	}
	s.mx.Unlock()
	return root, nil
}

func (s *StateStore) ImportStateCellTrees(ctx context.Context, trees []storage.StateCellTreeImport) ([]*cell.Cell, error) {
	roots := make([]*cell.Cell, len(trees))
	for i, tree := range trees {
		root, err := s.ImportStateCellTree(ctx, tree.Block, tree.Root, tree.ParsedCells, tree.TotalCells)
		if err != nil {
			return nil, err
		}
		roots[i] = root
	}
	return roots, nil
}

func (s *StateStore) LoadStateCellTree(_ context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, uint64, error) {
	s.mx.RLock()
	tree, ok := s.trees[storage.BlockKey(block)]
	s.mx.RUnlock()
	if !ok || tree.root == nil {
		return nil, 0, storage.ErrNotFound
	}
	if len(rootHash) > 0 {
		hash := tree.root.HashKey(0)
		if !bytes.Equal(hash[:], rootHash) {
			return nil, 0, storage.ErrNotFound
		}
	}
	return tree.root, tree.cells, nil
}

func (s *StateStore) SaveBlockState(_ context.Context, state *storage.BlockState) error {
	if state == nil {
		return fmt.Errorf("block state is nil")
	}

	s.mx.Lock()
	defer s.mx.Unlock()
	cloned := storage.CloneBlockState(state)
	fillBlockStateHashes(cloned)
	s.blocks[storage.BlockKey(state.Block)] = cloned
	return nil
}

func fillBlockStateHashes(state *storage.BlockState) {
	if len(state.StateRootHash) == 0 && state.Cell != nil {
		hash := state.Cell.HashKey(0)
		state.StateRootHash = hash[:]
	}
	if len(state.StateCellHash) == 0 && state.Cell != nil {
		hash := state.Cell.HashKey()
		state.StateCellHash = hash[:]
	}
}

func (s *StateStore) BlockState(_ context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	s.mx.RLock()
	defer s.mx.RUnlock()

	state := s.blocks[storage.BlockKey(block)]
	if state == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneBlockState(state), nil
}
