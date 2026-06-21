package p2p

import (
	"context"
	"fmt"
	"os"
	"sync"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

type testPeerStore struct {
	mu sync.RWMutex

	blocks       map[storage.BlockRootHash]*storage.ServedBlockFull
	nextBlocks   map[storage.BlockRootHash]storage.BlockRootHash
	blockData    map[storage.BlockRootHash][]byte
	proofs       map[testPeerProofKey][]byte
	zeroStates   map[storage.BlockRootHash][]byte
	stateFiles   map[testPeerPersistentStateKey][]byte
	archiveInfos map[string]int64
	archiveFiles map[int64][]byte
}

func newTestPeerStore() *testPeerStore {
	return &testPeerStore{
		blocks:       map[storage.BlockRootHash]*storage.ServedBlockFull{},
		nextBlocks:   map[storage.BlockRootHash]storage.BlockRootHash{},
		blockData:    map[storage.BlockRootHash][]byte{},
		proofs:       map[testPeerProofKey][]byte{},
		zeroStates:   map[storage.BlockRootHash][]byte{},
		stateFiles:   map[testPeerPersistentStateKey][]byte{},
		archiveInfos: map[string]int64{},
		archiveFiles: map[int64][]byte{},
	}
}

func (s *testPeerStore) SaveBlockFull(block *storage.ServedBlockFull) error {
	if block == nil {
		return fmt.Errorf("served block is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.saveBlockFullLocked(block)
	return nil
}

func (s *testPeerStore) saveBlockFullLocked(block *storage.ServedBlockFull) {
	cloned := block.Clone()
	s.blocks[storage.BlockKey(block.ID)] = cloned
	if len(cloned.Block) > 0 {
		s.blockData[storage.BlockKey(cloned.ID)] = append([]byte(nil), cloned.Block...)
	}
	if len(cloned.Proof) > 0 {
		isKeyBlock := false
		if cloned.Meta != nil {
			isKeyBlock = cloned.Meta.Has(storage.BlockMetaIsKeyBlock)
		}
		kinds := storage.StoredProofKindsForServedBlock(cloned.ID, cloned.IsLink, isKeyBlock)
		for _, kind := range kinds {
			s.proofs[s.proofKey(kind, cloned.ID)] = append([]byte(nil), cloned.Proof...)
		}
	}
}

func (s *testPeerStore) LinkNextBlock(prev ton.BlockIDExt, next ton.BlockIDExt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextBlocks[storage.BlockKey(prev)] = storage.BlockKey(next)
	return nil
}

func (s *testPeerStore) SaveBlockProof(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte, _ *storage.ArtifactRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.proofs[s.proofKey(kind, block)] = append([]byte(nil), data...)
	return nil
}

func (s *testPeerStore) SaveZeroState(block ton.BlockIDExt, data []byte, _ *storage.ArtifactRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.zeroStates[storage.BlockKey(block)] = append([]byte(nil), data...)
	return nil
}

func (s *testPeerStore) SavePersistentStateFile(file *storage.PersistentStateFile) error {
	if file == nil || file.Ref == nil {
		return fmt.Errorf("persistent state file is nil")
	}
	data, err := os.ReadFile(file.Ref.Path)
	if err != nil {
		return err
	}
	if file.Ref.Offset < 0 || file.Ref.Size < 0 || file.Ref.Offset+file.Ref.Size > int64(len(data)) {
		return fmt.Errorf("invalid persistent state file ref")
	}
	data = data[file.Ref.Offset : file.Ref.Offset+file.Ref.Size]

	s.mu.Lock()
	defer s.mu.Unlock()
	s.stateFiles[s.persistentStateKey(file.Block, file.MasterchainBlock, file.EffectiveShard)] = append([]byte(nil), data...)
	return nil
}

func (s *testPeerStore) BlockFull(_ context.Context, block ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value := s.blocks[storage.BlockKey(block)]
	if value == nil {
		return nil, storage.ErrNotFound
	}
	return value.Clone(), nil
}

func (s *testPeerStore) NextBlockFull(_ context.Context, prev ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nextKey, ok := s.nextBlocks[storage.BlockKey(prev)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	value := s.blocks[nextKey]
	if value == nil {
		return nil, storage.ErrNotFound
	}
	return value.Clone(), nil
}

func (s *testPeerStore) BlockData(_ context.Context, block ton.BlockIDExt) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.blockData[storage.BlockKey(block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), value...), nil
}

func (s *testPeerStore) BlockProof(_ context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.proofs[s.proofKey(kind, block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), value...), nil
}

func (s *testPeerStore) ZeroState(_ context.Context, block ton.BlockIDExt) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.zeroStates[storage.BlockKey(block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), value...), nil
}

func (s *testPeerStore) PersistentStateSize(_ context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.stateFiles[s.persistentStateKey(block, masterchainBlock, effectiveShard)]
	if !ok {
		return 0, storage.ErrNotFound
	}
	return int64(len(value)), nil
}

func (s *testPeerStore) PersistentStateSlice(_ context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64, offset int64, maxSize int64) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.stateFiles[s.persistentStateKey(block, masterchainBlock, effectiveShard)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	if offset < 0 || maxSize < 0 {
		return nil, fmt.Errorf("invalid persistent state range offset=%d max_size=%d", offset, maxSize)
	}
	if maxSize == 0 {
		return []byte{}, nil
	}
	if offset >= int64(len(value)) {
		return nil, nil
	}
	value = value[offset:]
	if len(value) > int(maxSize) {
		value = value[:maxSize]
	}
	return append([]byte(nil), value...), nil
}

func (s *testPeerStore) ArchiveInfo(_ context.Context, masterchainSeqno int32, workchain int32, shard int64) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.archiveInfos[testArchiveInfoKey(masterchainSeqno, workchain, shard)]
	if !ok {
		return 0, storage.ErrNotFound
	}
	return value, nil
}

func (s *testPeerStore) ArchiveSlice(_ context.Context, archiveID, offset int64, maxSize int32) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	value, ok := s.archiveFiles[archiveID]
	if !ok {
		return nil, storage.ErrNotFound
	}
	if offset < 0 || maxSize < 0 {
		return nil, fmt.Errorf("invalid archive range offset=%d max_size=%d", offset, maxSize)
	}
	if maxSize == 0 {
		return []byte{}, nil
	}
	if offset >= int64(len(value)) {
		return nil, nil
	}
	value = value[offset:]
	if len(value) > int(maxSize) {
		value = value[:maxSize]
	}
	return append([]byte(nil), value...), nil
}

type testPeerProofKey struct {
	kind  storage.ServedProofKind
	block storage.BlockRootHash
}

func (s *testPeerStore) proofKey(kind storage.ServedProofKind, block ton.BlockIDExt) testPeerProofKey {
	return testPeerProofKey{kind: kind, block: storage.BlockKey(block)}
}

type testPeerPersistentStateKey struct {
	block            storage.BlockRootHash
	masterchainBlock storage.BlockRootHash
	effectiveShard   int64
}

func (s *testPeerStore) persistentStateKey(block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) testPeerPersistentStateKey {
	return testPeerPersistentStateKey{
		block:            storage.BlockKey(block),
		masterchainBlock: storage.BlockKey(masterchainBlock),
		effectiveShard:   effectiveShard,
	}
}

func testArchiveInfoKey(masterchainSeqno int32, workchain int32, shard int64) string {
	return fmt.Sprintf("%d:%d:%016x", masterchainSeqno, workchain, uint64(shard))
}
