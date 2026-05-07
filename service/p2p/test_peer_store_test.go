package p2p

import (
	"context"
	"fmt"
	"os"
	"sync"

	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const testArchivePackageMasterchainBlocks = 100

type testPeerStore struct {
	mu sync.RWMutex

	blocks       map[string]*storage.ServedBlockFull
	nextBlocks   map[string]string
	blockData    map[string][]byte
	proofs       map[string][]byte
	zeroStates   map[string][]byte
	stateFiles   map[string][]byte
	archiveInfos map[string]int64
	archiveFiles map[int64][]byte
}

func newTestPeerStore() *testPeerStore {
	return &testPeerStore{
		blocks:       map[string]*storage.ServedBlockFull{},
		nextBlocks:   map[string]string{},
		blockData:    map[string][]byte{},
		proofs:       map[string][]byte{},
		zeroStates:   map[string][]byte{},
		stateFiles:   map[string][]byte{},
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
		kinds := storage.StoredProofKinds(cloned.Block, cloned.IsLink)
		if cloned.Meta != nil {
			kinds = storage.StoredProofKindsForBlock(cloned.IsLink, cloned.Meta.Has(storage.BlockMetaIsKeyBlock))
		}
		for _, kind := range kinds {
			s.proofs[s.proofKey(kind, cloned.ID)] = append([]byte(nil), cloned.Proof...)
		}
	}
}

func (s *testPeerStore) SaveArchiveImport(imported *storage.ServedArchiveImport) error {
	if imported == nil {
		return fmt.Errorf("served archive import is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	for _, full := range imported.FullBlocks {
		s.saveBlockFullLocked(full)
	}
	for _, block := range imported.BlockData {
		s.blockData[storage.BlockKey(block.ID)] = append([]byte(nil), block.Data...)
	}
	for _, proof := range imported.Proofs {
		s.proofs[s.proofKey(proof.Kind, proof.ID)] = append([]byte(nil), proof.Data...)
	}
	for _, link := range imported.Links {
		s.nextBlocks[storage.BlockKey(link.Prev)] = storage.BlockKey(link.Next)
	}
	return nil
}

func (s *testPeerStore) LinkNextBlock(prev ton.BlockIDExt, next ton.BlockIDExt) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextBlocks[storage.BlockKey(prev)] = storage.BlockKey(next)
	return nil
}

func (s *testPeerStore) SaveBlockData(block ton.BlockIDExt, data []byte, _ *storage.ArtifactRef) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockData[storage.BlockKey(block)] = append([]byte(nil), data...)
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

func (s *testPeerStore) SaveArchiveFile(masterchainSeqno int32, workchain int32, shard int64, archiveID int64, path string) (string, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	s.archiveInfos[testArchiveInfoKey(masterchainSeqno, workchain, shard)] = archiveID
	s.archiveFiles[archiveID] = data
	return path, nil
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
		rounded := testRoundArchiveMasterchainSeqno(masterchainSeqno)
		if rounded != masterchainSeqno {
			value, ok = s.archiveInfos[testArchiveInfoKey(rounded, workchain, shard)]
		}
	}
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

func (s *testPeerStore) proofKey(kind storage.ServedProofKind, block ton.BlockIDExt) string {
	return string(kind) + ":" + storage.BlockKey(block)
}

func (s *testPeerStore) persistentStateKey(block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) string {
	return storage.BlockKey(block) + ":" + storage.BlockKey(masterchainBlock) + ":" + fmt.Sprintf("%016x", uint64(effectiveShard))
}

func testArchiveInfoKey(masterchainSeqno int32, workchain int32, shard int64) string {
	return fmt.Sprintf("%d:%d:%016x", masterchainSeqno, workchain, uint64(shard))
}

func testRoundArchiveMasterchainSeqno(seqno int32) int32 {
	if seqno <= 0 {
		return seqno
	}
	return seqno - seqno%testArchivePackageMasterchainBlocks
}
