package state

import (
	"bytes"
	"context"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type testStateStore struct {
	mx       sync.RWMutex
	current  *storage.CurrentState
	progress *storage.CurrentState
	blocks   map[storage.BlockRootHash]*storage.BlockState
	trees    map[storage.BlockRootHash]testStateCellTree
}

type testStateCellTree struct {
	root *cell.Cell
}

func newTestStateStore() *testStateStore {
	return &testStateStore{
		blocks: map[storage.BlockRootHash]*storage.BlockState{},
		trees:  map[storage.BlockRootHash]testStateCellTree{},
	}
}

func (s *testStateStore) SaveCurrentState(_ context.Context, state *storage.CurrentState) error {
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

func (s *testStateStore) SaveStateCellRecords(_ context.Context, _ storage.StateCellRecords) error {
	return nil
}

func (s *testStateStore) FlushStateCells(_ context.Context) error {
	return nil
}

func (s *testStateStore) SaveStateCheckpoint(_ context.Context, blocks []*storage.BlockState, current *storage.CurrentState) error {
	entries := make([]storage.StateCheckpointBlock, 0, len(blocks))
	for _, block := range blocks {
		if block == nil {
			continue
		}
		entries = append(entries, storage.StateCheckpointBlock{State: block})
	}
	_, err := s.SaveStateCheckpointEntries(context.Background(), entries, storage.StateCellRecords{}, current)
	return err
}

func (s *testStateStore) SaveStateCheckpointEntries(_ context.Context, blocks []storage.StateCheckpointBlock, _ storage.StateCellRecords, current *storage.CurrentState) (storage.StateCheckpointTiming, error) {
	if current == nil {
		return storage.StateCheckpointTiming{}, fmt.Errorf("current state is nil")
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	for _, entry := range blocks {
		if entry.State == nil {
			continue
		}
		clonedBlock := storage.CloneBlockState(entry.State)
		fillTestBlockStateHashes(clonedBlock)
		s.blocks[storage.BlockKey(clonedBlock.Block)] = clonedBlock
	}

	cloned := storage.CloneCurrentState(current)
	s.current = cloned
	s.blocks[storage.BlockKey(cloned.Masterchain.Block)] = storage.CloneBlockState(&cloned.Masterchain)
	for _, shard := range cloned.Shards {
		s.blocks[storage.BlockKey(shard.Block)] = storage.CloneBlockState(&shard)
	}
	return storage.StateCheckpointTiming{}, nil
}

func (s *testStateStore) CurrentState(_ context.Context) (*storage.CurrentState, error) {
	s.mx.RLock()
	defer s.mx.RUnlock()

	if s.current == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneCurrentState(s.current), nil
}

func (s *testStateStore) SaveStateSyncProgress(_ context.Context, state *storage.CurrentState) error {
	if state == nil {
		return fmt.Errorf("state sync progress is nil")
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	s.progress = storage.CloneCurrentState(state)
	return nil
}

func (s *testStateStore) StateSyncProgress(_ context.Context) (*storage.CurrentState, error) {
	s.mx.RLock()
	defer s.mx.RUnlock()

	if s.progress == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneCurrentState(s.progress), nil
}

func (s *testStateStore) ClearStateSyncProgress(_ context.Context) error {
	s.mx.Lock()
	defer s.mx.Unlock()

	s.progress = nil
	return nil
}

func (s *testStateStore) ImportStateCellTree(_ context.Context, block ton.BlockIDExt, root *cell.Cell, _ uint64) (*cell.Cell, error) {
	if root == nil {
		return nil, fmt.Errorf("state cell tree root is nil")
	}
	s.mx.Lock()
	s.trees[storage.BlockKey(block)] = testStateCellTree{
		root: root,
	}
	s.mx.Unlock()
	return root, nil
}

func (s *testStateStore) ImportStateBOCView(context.Context, ton.BlockIDExt, *cell.BOCView) (*cell.Cell, error) {
	return nil, fmt.Errorf("state boc view import is not supported by test store")
}

func (s *testStateStore) TrustImportedStateCellHashes() bool {
	return false
}

func (s *testStateStore) ReuseImportedSplitStatePartCells() bool {
	return true
}

func (s *testStateStore) LoadStateCellTree(_ context.Context, block ton.BlockIDExt, rootHash []byte) (*cell.Cell, error) {
	s.mx.RLock()
	tree, ok := s.trees[storage.BlockKey(block)]
	s.mx.RUnlock()
	if !ok || tree.root == nil {
		return nil, storage.ErrNotFound
	}
	if len(rootHash) > 0 {
		hash := tree.root.HashKey(0)
		if !bytes.Equal(hash[:], rootHash) {
			return nil, storage.ErrNotFound
		}
	}
	return tree.root, nil
}

func (s *testStateStore) SaveBlockState(_ context.Context, state *storage.BlockState) error {
	if state == nil {
		return fmt.Errorf("block state is nil")
	}

	s.mx.Lock()
	defer s.mx.Unlock()

	cloned := storage.CloneBlockState(state)
	fillTestBlockStateHashes(cloned)
	s.blocks[storage.BlockKey(state.Block)] = cloned
	return nil
}

func (s *testStateStore) BlockState(_ context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	s.mx.RLock()
	defer s.mx.RUnlock()

	state := s.blocks[storage.BlockKey(block)]
	if state == nil {
		return nil, storage.ErrNotFound
	}
	return storage.CloneBlockState(state), nil
}

func fillTestBlockStateHashes(state *storage.BlockState) {
	if len(state.StateRootHash) == 0 && state.Cell != nil {
		hash := state.Cell.HashKey(0)
		state.StateRootHash = hash[:]
	}
}
