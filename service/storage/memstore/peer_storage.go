package memstore

import (
	"context"
	"flexserver/service/storage"
	"fmt"
	"sync"

	"github.com/xssnick/tonutils-go/ton"
)

type PeerStore struct {
	mu sync.RWMutex

	blocks        map[string]*storage.ServedBlockFull
	nextBlocks    map[string]string
	blockData     map[string][]byte
	proofs        map[string][]byte
	archiveInfos  map[int32]int64
	archiveSlices map[string][]byte
}

func NewPeerStore() *PeerStore {
	return &PeerStore{
		blocks:        map[string]*storage.ServedBlockFull{},
		nextBlocks:    map[string]string{},
		blockData:     map[string][]byte{},
		proofs:        map[string][]byte{},
		archiveInfos:  map[int32]int64{},
		archiveSlices: map[string][]byte{},
	}
}

func (s *PeerStore) SaveBlockFull(block *storage.ServedBlockFull) error {
	if block == nil {
		return fmt.Errorf("served block is nil")
	}

	s.mu.Lock()
	defer s.mu.Unlock()

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
	return nil
}

func (s *PeerStore) LinkNextBlock(prev ton.BlockIDExt, next ton.BlockIDExt) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.nextBlocks[storage.BlockKey(prev)] = storage.BlockKey(next)
}

func (s *PeerStore) SaveBlockData(block ton.BlockIDExt, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.blockData[storage.BlockKey(block)] = append([]byte(nil), data...)
}

func (s *PeerStore) SaveBlockProof(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.proofs[s.proofKey(kind, block)] = append([]byte(nil), data...)
}

func (s *PeerStore) SaveArchiveInfo(masterchainSeqno int32, archiveID int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.archiveInfos[masterchainSeqno] = archiveID
}

func (s *PeerStore) SaveArchiveSlice(archiveID, offset int64, data []byte) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.archiveSlices[archiveSliceKey(archiveID, offset)] = append([]byte(nil), data...)
}

func (s *PeerStore) BlockFull(_ context.Context, block ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.blocks[storage.BlockKey(block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return value.Clone(), nil
}

func (s *PeerStore) NextBlockFull(_ context.Context, prev ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	nextKey, ok := s.nextBlocks[storage.BlockKey(prev)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	value, ok := s.blocks[nextKey]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return value.Clone(), nil
}

func (s *PeerStore) BlockData(_ context.Context, block ton.BlockIDExt) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.blockData[storage.BlockKey(block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), value...), nil
}

func (s *PeerStore) BlockProof(_ context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.proofs[s.proofKey(kind, block)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), value...), nil
}

func (s *PeerStore) ArchiveInfo(_ context.Context, masterchainSeqno int32) (int64, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.archiveInfos[masterchainSeqno]
	if !ok {
		return 0, storage.ErrNotFound
	}
	return value, nil
}

func (s *PeerStore) ArchiveSlice(_ context.Context, archiveID, offset int64, _ int32) ([]byte, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	value, ok := s.archiveSlices[archiveSliceKey(archiveID, offset)]
	if !ok {
		return nil, storage.ErrNotFound
	}
	return append([]byte(nil), value...), nil
}

func (s *PeerStore) proofKey(kind storage.ServedProofKind, block ton.BlockIDExt) string {
	return string(kind) + ":" + storage.BlockKey(block)
}

func archiveSliceKey(archiveID, offset int64) string {
	return fmt.Sprintf("%d:%d", archiveID, offset)
}
