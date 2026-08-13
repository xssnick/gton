package service

import (
	"bytes"
	"context"
	"testing"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCellGenerationSwitchPublishesCurrentStateWithActiveLoader(t *testing.T) {
	const (
		candidateGeneration = uint64(2)
		nextGeneration      = uint64(3)
	)

	masterLeaf := cell.BeginCell().MustStoreUInt(0xa1, 8).EndCell()
	masterRoot := cell.BeginCell().MustStoreUInt(0x11, 8).MustStoreRef(masterLeaf).EndCell()
	shardLeaf := cell.BeginCell().MustStoreUInt(0xb2, 8).EndCell()
	shardRoot := cell.BeginCell().MustStoreUInt(0x22, 8).MustStoreRef(shardLeaf).EndCell()
	records := testCellGenerationRecords(t, masterRoot, masterLeaf, shardRoot, shardLeaf)

	masterBlock := testBlockID(-1, topShard, 100)
	shardBlock := testBlockID(0, topShard, 200)
	latest := &storage.CurrentState{
		ShardClientSeqno: masterBlock.SeqNo,
		Masterchain: storage.BlockState{
			Block:         masterBlock,
			StateRootHash: testCellGenerationHash(masterRoot),
		},
		Shards: map[storage.ShardKey]storage.BlockState{
			storage.ShardKeyFromBlock(shardBlock): {
				Block:         shardBlock,
				StateRootHash: testCellGenerationHash(shardRoot),
			},
		},
	}
	store := newActiveLoaderSwitchStore(latest, map[uint64]map[cell.Hash]*storage.CellRecord{
		1:                   records,
		candidateGeneration: records,
		nextGeneration:      records,
	})

	candidateLoader := store.loaderForGeneration(candidateGeneration)
	candidateMaster, err := candidateLoader(masterRoot.HashKey())
	if err != nil {
		t.Fatalf("load candidate master root: %v", err)
	}
	candidateShard, err := candidateLoader(shardRoot.HashKey())
	if err != nil {
		t.Fatalf("load candidate shard root: %v", err)
	}
	candidate := &cellGenerationCandidate{
		generation: candidateGeneration,
		current:    storage.CloneCurrentState(latest),
	}
	candidate.current.Masterchain.Cell = candidateMaster
	candidateShardState := candidate.current.Shards[storage.ShardKeyFromBlock(shardBlock)]
	candidateShardState.Cell = candidateShard
	candidate.current.Shards[storage.ShardKeyFromBlock(shardBlock)] = candidateShardState

	transitions := &captureCellGenerationSwitchTransitions{}
	lifecycle := &StateLifecycle{
		log:                   zerolog.Nop(),
		checkpointTransitions: transitions,
	}
	oldGeneration, err := lifecycle.trySwitchCellGenerationCandidate(
		context.Background(),
		store,
		candidate,
		testBlockID(-1, topShard, 50),
	)
	if err != nil {
		t.Fatalf("switch candidate generation: %v", err)
	}
	if oldGeneration != 1 {
		t.Fatalf("old generation = %d, want 1", oldGeneration)
	}
	if transitions.current == nil {
		t.Fatal("switched current state was not published")
	}
	publishedShard := transitions.current.Shards[storage.ShardKeyFromBlock(shardBlock)]
	if err = store.CleanupCellGeneration(context.Background(), oldGeneration); err != nil {
		t.Fatalf("cleanup old generation: %v", err)
	}

	if _, err = store.SwitchCellGeneration(
		context.Background(),
		nextGeneration,
		ton.BlockIDExt{},
		masterBlock,
		currentStateWithoutCells(transitions.current),
	); err != nil {
		t.Fatalf("switch next generation: %v", err)
	}
	if err = store.CleanupCellGeneration(context.Background(), candidateGeneration); err != nil {
		t.Fatalf("cleanup candidate generation: %v", err)
	}

	assertPublishedCellLoadsFromActiveGeneration(t, transitions.current.Masterchain.Cell, 0xa1)
	publishedShard = transitions.current.Shards[storage.ShardKeyFromBlock(shardBlock)]
	assertPublishedCellLoadsFromActiveGeneration(t, publishedShard.Cell, 0xb2)
}

type activeLoaderSwitchStore struct {
	*testCellGenerationMigrationStore

	active       uint64
	records      map[uint64]map[cell.Hash]*storage.CellRecord
	activeLoader cell.LazyCellLoader
}

func newActiveLoaderSwitchStore(current *storage.CurrentState, records map[uint64]map[cell.Hash]*storage.CellRecord) *activeLoaderSwitchStore {
	store := &activeLoaderSwitchStore{
		testCellGenerationMigrationStore: &testCellGenerationMigrationStore{current: current},
		active:                           1,
		records:                          records,
	}
	store.activeLoader = store.loadActiveCell
	return store
}

func (s *activeLoaderSwitchStore) ActiveCells() (storage.CellGeneration, error) {
	return activeLoaderTestCellGeneration{
		testCellGeneration: testCellGeneration{generation: s.active, store: s.testCellGenerationMigrationStore},
		loader:             s.activeLoader,
	}, nil
}

func (s *activeLoaderSwitchStore) Cells(generation uint64) (storage.CellGeneration, error) {
	return activeLoaderTestCellGeneration{
		testCellGeneration: testCellGeneration{generation: generation, store: s.testCellGenerationMigrationStore},
		loader:             s.loaderForGeneration(generation),
	}, nil
}

func (s *activeLoaderSwitchStore) LazyCellLoader() cell.LazyCellLoader {
	return s.activeLoader
}

func (s *activeLoaderSwitchStore) ActiveCellGeneration(context.Context) (storage.CellGenerationInfo, error) {
	return storage.CellGenerationInfo{ID: s.active}, nil
}

func (s *activeLoaderSwitchStore) BlockState(ctx context.Context, block ton.BlockIDExt) (*storage.BlockState, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var state storage.BlockState
	switch {
	case block.Equals(&s.current.Masterchain.Block):
		state = s.current.Masterchain
	default:
		for _, shard := range s.current.Shards {
			if block.Equals(&shard.Block) {
				state = shard
				break
			}
		}
	}
	if len(state.StateRootHash) == 0 {
		return nil, storage.ErrNotFound
	}

	var hash cell.Hash
	copy(hash[:], state.StateRootHash)
	root, err := s.activeLoader(hash)
	if err != nil {
		return nil, err
	}
	state.Cell = root
	return &state, nil
}

func (s *activeLoaderSwitchStore) SwitchCellGeneration(_ context.Context, generation uint64, _ ton.BlockIDExt, _ ton.BlockIDExt, _ *storage.CurrentState) (uint64, error) {
	previous := s.active
	s.active = generation
	return previous, nil
}

func (s *activeLoaderSwitchStore) CleanupCellGeneration(_ context.Context, generation uint64) error {
	delete(s.records, generation)
	return nil
}

func (s *activeLoaderSwitchStore) loadActiveCell(hash cell.Hash) (*cell.Cell, error) {
	return s.loadCell(s.active, hash, s.activeLoader)
}

func (s *activeLoaderSwitchStore) loaderForGeneration(generation uint64) cell.LazyCellLoader {
	var loader cell.LazyCellLoader
	loader = func(hash cell.Hash) (*cell.Cell, error) {
		return s.loadCell(generation, hash, loader)
	}
	return loader
}

func (s *activeLoaderSwitchStore) loadCell(generation uint64, hash cell.Hash, loader cell.LazyCellLoader) (*cell.Cell, error) {
	record := s.records[generation][hash]
	if record == nil {
		return nil, storage.ErrNotFound
	}
	return storage.LazyCellRecord(record, loader)
}

type activeLoaderTestCellGeneration struct {
	testCellGeneration
	loader cell.LazyCellLoader
}

func (c activeLoaderTestCellGeneration) Load(_ context.Context, hash []byte) (*cell.Cell, error) {
	var key cell.Hash
	copy(key[:], hash)
	return c.loader(key)
}

func (c activeLoaderTestCellGeneration) Loader() cell.LazyCellLoader {
	return c.loader
}

type captureCellGenerationSwitchTransitions struct {
	current *storage.CurrentState
}

func (c *captureCellGenerationSwitchTransitions) publishCommittedCurrentState(current *storage.CurrentState) {
	c.current = storage.CloneCurrentState(current)
}

func (*captureCellGenerationSwitchTransitions) broadcastCurrentStateWake() {}

func testCellGenerationRecords(t testing.TB, cells ...*cell.Cell) map[cell.Hash]*storage.CellRecord {
	t.Helper()

	records := make(map[cell.Hash]*storage.CellRecord, len(cells))
	for _, cl := range cells {
		record, err := storage.CellRecordFromCell(cl)
		if err != nil {
			t.Fatalf("build cell record: %v", err)
		}
		records[cl.HashKey()] = record
	}
	return records
}

func testCellGenerationHash(cl *cell.Cell) []byte {
	hash := cl.HashKey()
	return bytes.Clone(hash[:])
}

func assertPublishedCellLoadsFromActiveGeneration(t testing.TB, root *cell.Cell, want uint64) {
	t.Helper()

	if root == nil {
		t.Fatal("published root is nil")
	}
	child, err := root.PeekRef(0)
	if err != nil {
		t.Fatalf("peek published child: %v", err)
	}
	loader, err := child.BeginParse()
	if err != nil {
		t.Fatalf("materialize published child after next generation switch: %v", err)
	}
	got, err := loader.LoadUInt(8)
	if err != nil {
		t.Fatalf("load published child payload: %v", err)
	}
	if got != want {
		t.Fatalf("published child payload = %#x, want %#x", got, want)
	}
}
