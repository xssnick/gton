package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"flexserver/service/archive/packfile"
	"flexserver/service/storage"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"
	"time"

	"github.com/cockroachdb/pebble"
	"github.com/xssnick/tonutils-go/ton"
)

func (s *Store) SaveBlockFull(block *storage.ServedBlockFull) error {
	if block == nil {
		return fmt.Errorf("served block is nil")
	}

	meta, proofKinds := servedBlockFullMeta(block)
	blockRef, proofRefs, err := s.servedBlockFullArtifactRefs(block, proofKinds)
	if err != nil {
		return err
	}
	return s.withHotBatch(func(batch *pebble.Batch) error {
		if err := s.setServedBlockFullArtifactRefs(batch, block, proofKinds, blockRef, proofRefs); err != nil {
			return err
		}
		return s.setMergedBlockMeta(batch, meta)
	})
}

func servedBlockFullMeta(block *storage.ServedBlockFull) (*storage.BlockMeta, []storage.ServedProofKind) {
	meta := &storage.BlockMeta{
		ID:        block.ID,
		Flags:     blockMetaServedFlags(block.IsLink),
		UpdatedAt: time.Now(),
	}
	if block.Meta != nil {
		meta = storage.MergeBlockMeta(meta, block.Meta)
		meta.ID = block.ID
	}
	if len(block.Block) > 0 || block.BlockRef != nil {
		meta.Mark(storage.BlockMetaHasBlockData)
		if block.Meta == nil && len(block.Block) > 0 {
			meta = storage.MergeBlockMetaFromBlockData(meta, block.ID, block.Block)
		}
	}

	var proofKinds []storage.ServedProofKind
	if len(block.Proof) > 0 || block.ProofRef != nil {
		proofKinds = storage.StoredProofKindsForBlock(block.IsLink, meta.Has(storage.BlockMetaIsKeyBlock))
		for _, kind := range proofKinds {
			meta.Mark(storage.BlockMetaFlagForProof(kind))
		}
	}
	return meta, proofKinds
}

func (s *Store) SaveArchiveImport(imported *storage.ServedArchiveImport) error {
	if imported == nil {
		return fmt.Errorf("served archive import is nil")
	}

	type fullWrite struct {
		block      *storage.ServedBlockFull
		proofKinds []storage.ServedProofKind
		blockRef   *storage.ArtifactRef
		proofRefs  map[storage.ServedProofKind]*storage.ArtifactRef
	}
	type blockDataWrite struct {
		block ton.BlockIDExt
		ref   *storage.ArtifactRef
	}
	type proofWrite struct {
		kind  storage.ServedProofKind
		block ton.BlockIDExt
		ref   *storage.ArtifactRef
	}

	metas := make(map[string]*storage.BlockMeta, len(imported.FullBlocks)+len(imported.BlockData)+len(imported.Proofs))
	fullWrites := make([]fullWrite, 0, len(imported.FullBlocks))
	for _, full := range imported.FullBlocks {
		meta, proofKinds := servedBlockFullMeta(full)
		blockRef, proofRefs, err := s.servedBlockFullArtifactRefs(full, proofKinds)
		if err != nil {
			return err
		}
		mergeArchiveImportMeta(metas, meta)
		fullWrites = append(fullWrites, fullWrite{
			block:      full,
			proofKinds: proofKinds,
			blockRef:   blockRef,
			proofRefs:  proofRefs,
		})
	}

	blockDataWrites := make([]blockDataWrite, 0, len(imported.BlockData))
	for _, block := range imported.BlockData {
		if len(block.Data) == 0 && block.Ref == nil {
			continue
		}
		ref := block.Ref
		if ref == nil {
			var err error
			ref, err = s.appendArtifactEntry(packfile.KindBlock, block.ID, block.Data)
			if err != nil {
				return err
			}
		}
		meta := &storage.BlockMeta{
			ID:        block.ID,
			Flags:     storage.BlockMetaHasBlockData,
			UpdatedAt: time.Now(),
		}
		if len(block.Data) > 0 {
			meta = storage.MergeBlockMetaFromBlockData(meta, block.ID, block.Data)
		}
		mergeArchiveImportMeta(metas, meta)
		blockDataWrites = append(blockDataWrites, blockDataWrite{block: block.ID, ref: ref})
	}

	proofWrites := make([]proofWrite, 0, len(imported.Proofs))
	for _, proof := range imported.Proofs {
		if len(proof.Data) == 0 && proof.Ref == nil {
			continue
		}
		ref := proof.Ref
		if ref == nil {
			var err error
			ref, err = s.appendProofEntry(proof.Kind, proof.ID, proof.Data)
			if err != nil {
				return err
			}
		} else if isKeyProofKind(proof.Kind) {
			var err error
			ref, err = s.copyProofRefToKeyPack(proof.Kind, proof.ID, ref)
			if err != nil {
				return err
			}
		}
		mergeArchiveImportMeta(metas, &storage.BlockMeta{
			ID:        proof.ID,
			Flags:     blockMetaFlagsForProofKind(proof.Kind),
			UpdatedAt: time.Now(),
		})
		proofWrites = append(proofWrites, proofWrite{kind: proof.Kind, block: proof.ID, ref: ref})
	}

	metaKeys := make([]string, 0, len(metas))
	for key := range metas {
		metaKeys = append(metaKeys, key)
	}
	sort.Strings(metaKeys)

	return s.withHotBatch(func(batch *pebble.Batch) error {
		for _, write := range fullWrites {
			if err := s.setServedBlockFullArtifactRefs(batch, write.block, write.proofKinds, write.blockRef, write.proofRefs); err != nil {
				return err
			}
		}
		for _, write := range blockDataWrites {
			if err := s.setHotUnique(batch, hotKeyBlockDataRef(write.block), encodeArtifactRef(write.ref)); err != nil {
				return err
			}
		}
		for _, write := range proofWrites {
			if err := s.setHotUnique(batch, hotKeyStoredProofRef(write.kind, write.block), encodeArtifactRef(write.ref)); err != nil {
				return err
			}
		}
		for _, link := range imported.Links {
			if err := s.setHotUnique(batch, hotKeyNextBlock(link.Prev), encodeBlockID(link.Next)); err != nil {
				return err
			}
		}
		for _, key := range metaKeys {
			if err := s.setMergedBlockMeta(batch, metas[key]); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) servedBlockFullArtifactRefs(block *storage.ServedBlockFull, proofKinds []storage.ServedProofKind) (*storage.ArtifactRef, map[storage.ServedProofKind]*storage.ArtifactRef, error) {
	if len(block.Block) == 0 && len(block.Proof) == 0 && block.BlockRef == nil && block.ProofRef == nil {
		return nil, nil, nil
	}

	blockRef := block.BlockRef
	if len(block.Block) > 0 && blockRef == nil {
		ref, err := s.appendArtifactEntry(packfile.KindBlock, block.ID, block.Block)
		if err != nil {
			return nil, nil, err
		}
		blockRef = ref
	}

	proofRefs := make(map[storage.ServedProofKind]*storage.ArtifactRef, len(proofKinds))
	if len(proofKinds) > 0 {
		if len(block.Proof) > 0 {
			for _, kind := range proofKinds {
				ref, err := s.appendProofEntry(kind, block.ID, block.Proof)
				if err != nil {
					return nil, nil, err
				}
				proofRefs[kind] = ref
			}
		} else if block.ProofRef != nil {
			for _, kind := range proofKinds {
				ref := block.ProofRef
				if isKeyProofKind(kind) {
					var err error
					ref, err = s.copyProofRefToKeyPack(kind, block.ID, block.ProofRef)
					if err != nil {
						return nil, nil, err
					}
				}
				proofRefs[kind] = ref
			}
		}
	}

	return blockRef, proofRefs, nil
}

func (s *Store) setServedBlockFullArtifactRefs(batch *pebble.Batch, block *storage.ServedBlockFull, proofKinds []storage.ServedProofKind, blockRef *storage.ArtifactRef, proofRefs map[storage.ServedProofKind]*storage.ArtifactRef) error {
	if blockRef != nil {
		if err := s.setHotUnique(batch, hotKeyBlockDataRef(block.ID), encodeArtifactRef(blockRef)); err != nil {
			return err
		}
	}
	for _, kind := range proofKinds {
		ref := proofRefs[kind]
		if ref == nil {
			continue
		}
		if err := s.setHotUnique(batch, hotKeyStoredProofRef(kind, block.ID), encodeArtifactRef(ref)); err != nil {
			return err
		}
	}
	return nil
}

func mergeArchiveImportMeta(metas map[string]*storage.BlockMeta, meta *storage.BlockMeta) {
	key := storage.BlockKey(meta.ID)
	merged := storage.MergeBlockMeta(metas[key], meta)
	merged.ID = meta.ID
	metas[key] = merged
}

func (s *Store) LinkNextBlock(prev ton.BlockIDExt, next ton.BlockIDExt) error {
	return s.withHotBatch(func(batch *pebble.Batch) error {
		key := hotKeyNextBlock(prev)
		val := encodeBlockID(next)
		return s.setHotUnique(batch, key, val)
	})
}

func (s *Store) SaveBlockData(block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error {
	return s.persistBlockData(block, data, ref)
}

func (s *Store) persistBlockData(block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error {
	if len(data) == 0 {
		return nil
	}
	if ref == nil {
		var err error
		ref, err = s.appendArtifactEntry(packfile.KindBlock, block, data)
		if err != nil {
			return err
		}
	}
	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		return s.setHotUnique(batch, hotKeyBlockDataRef(block), encodeArtifactRef(ref))
	}); err != nil {
		return err
	}

	meta := &storage.BlockMeta{
		ID:        block,
		Flags:     storage.BlockMetaHasBlockData,
		UpdatedAt: time.Now(),
	}
	meta = storage.MergeBlockMetaFromBlockData(meta, block, data)
	return s.mergeAndStoreBlockMeta(meta)
}

func (s *Store) SaveBlockProof(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error {
	return s.persistBlockProof(kind, block, data, ref)
}

func (s *Store) persistBlockProof(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error {
	if len(data) == 0 && ref == nil {
		return nil
	}
	if ref == nil {
		var err error
		ref, err = s.appendProofEntry(kind, block, data)
		if err != nil {
			return err
		}
	} else if isKeyProofKind(kind) {
		var err error
		ref, err = s.copyProofRefToKeyPack(kind, block, ref)
		if err != nil {
			return err
		}
	}
	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		return s.setHotUnique(batch, hotKeyStoredProofRef(kind, block), encodeArtifactRef(ref))
	}); err != nil {
		return err
	}
	return s.mergeAndStoreBlockMeta(&storage.BlockMeta{
		ID:        block,
		Flags:     blockMetaFlagsForProofKind(kind),
		UpdatedAt: time.Now(),
	})
}

func (s *Store) SaveZeroState(block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error {
	return s.persistZeroState(block, data, ref)
}

func (s *Store) persistZeroState(block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error {
	if len(data) == 0 {
		return nil
	}
	if ref == nil {
		var err error
		ref, err = s.appendArtifactEntry(packfile.KindZeroState, block, data)
		if err != nil {
			return err
		}
	}
	return s.withHotBatch(func(batch *pebble.Batch) error {
		return s.setHotUnique(batch, hotKeyZeroStateRef(block), encodeArtifactRef(ref))
	})
}

func (s *Store) SavePersistentStateFile(file *storage.PersistentStateFile) error {
	if file == nil {
		return fmt.Errorf("persistent state file is nil")
	}
	if file.Ref == nil {
		return fmt.Errorf("persistent state file ref is nil")
	}
	if file.Ref.Size <= 0 {
		return fmt.Errorf("persistent state file size is invalid")
	}
	if file.Ref.Offset < 0 {
		return fmt.Errorf("persistent state file offset is invalid")
	}

	stored := *file
	stored.Ref = file.Ref.Clone()
	stored.Ref.Path = s.relativeArtifactPath(stored.Ref.Path)

	meta := &storage.BlockMeta{
		ID:            stored.Block,
		Flags:         storage.BlockMetaHasStateSnapshot,
		StateRootHash: bytes.Clone(stored.StateRootHash),
		StateFileHash: bytes.Clone(stored.FileHash),
		UpdatedAt:     time.Now(),
	}
	record := encodePersistentStateFileRecord(&stored)
	key := hotKeyPersistentStateFile(stored.Block, stored.MasterchainBlock, stored.EffectiveShard)

	return s.withHotBatch(func(batch *pebble.Batch) error {
		if err := s.setHotMaybeReplace(batch, key, record); err != nil {
			return err
		}
		if stored.EffectiveShard == 0 {
			return s.setMergedBlockMeta(batch, meta)
		}
		return nil
	})
}

func (s *Store) SaveArchiveFile(masterchainSeqno int32, workchain int32, shard int64, archiveID int64, path string) (string, error) {
	storedPath := s.archivePackPath(archiveID)
	s.artifactMu.Lock()
	stored, err := storeArchivePack(path, storedPath)
	if err != nil {
		s.artifactMu.Unlock()
		return "", err
	}
	if stored {
		s.pendingArchiveSync[storedPath] = struct{}{}
	}
	stat, err := os.Stat(storedPath)
	s.artifactMu.Unlock()
	if err != nil {
		return "", err
	}
	ref := &storage.ArtifactRef{
		Path: s.relativeArtifactPath(storedPath),
		Size: stat.Size(),
	}
	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		if err := s.setHotMaybeReplace(batch, hotKeyArchiveInfo(masterchainSeqno, workchain, shard), encodeInt64(archiveID)); err != nil {
			return err
		}
		return s.setHotMaybeReplace(batch, hotKeyArchiveFile(archiveID), encodeArtifactRef(ref))
	}); err != nil {
		return "", err
	}
	return storedPath, nil
}

func (s *Store) syncPendingArtifactFiles() error {
	if err := s.syncPendingArchiveFiles(); err != nil {
		return err
	}
	if err := s.syncPendingLooseFiles(); err != nil {
		return err
	}
	return s.syncPendingKeyProofFiles()
}

func (s *Store) syncPendingLooseFiles() error {
	return s.syncPendingPackFiles(s.pendingLooseSync, "loose")
}

func (s *Store) syncPendingArchiveFiles() error {
	return s.syncPendingPackFiles(s.pendingArchiveSync, "archive")
}

func (s *Store) syncPendingKeyProofFiles() error {
	return s.syncPendingPackFiles(s.pendingKeyProofSync, "key proof")
}

func (s *Store) syncPendingPackFiles(pending map[string]struct{}, label string) error {
	s.artifactMu.Lock()
	if len(pending) == 0 {
		s.artifactMu.Unlock()
		return nil
	}

	paths := make([]string, 0, len(pending))
	for path := range pending {
		paths = append(paths, path)
	}
	s.artifactMu.Unlock()

	sort.Strings(paths)
	dirs := make(map[string]struct{}, len(paths))
	for _, path := range paths {
		if err := syncFile(path); err != nil {
			return fmt.Errorf("sync %s pack %s: %w", label, path, err)
		}
		dirs[filepath.Dir(path)] = struct{}{}
	}

	dirPaths := make([]string, 0, len(dirs))
	for dir := range dirs {
		dirPaths = append(dirPaths, dir)
	}
	sort.Strings(dirPaths)
	for _, dir := range dirPaths {
		if err := syncDir(dir); err != nil {
			return fmt.Errorf("sync %s pack dir %s: %w", label, dir, err)
		}
	}

	s.artifactMu.Lock()
	for _, path := range paths {
		delete(pending, path)
	}
	s.artifactMu.Unlock()
	return nil
}

func syncFile(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	return file.Sync()
}

func syncDir(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()
	if err = file.Sync(); err != nil && !errors.Is(err, syscall.EINVAL) {
		return err
	}
	return nil
}

func (s *Store) BlockFull(ctx context.Context, block ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	meta, err := s.BlockMeta(ctx, block)
	if err != nil {
		return nil, err
	}
	if !meta.Has(storage.BlockMetaHasServedFull) {
		return nil, storage.ErrNotFound
	}

	data, err := s.BlockData(ctx, block)
	if err != nil {
		return nil, err
	}

	var proof []byte
	for _, kind := range storage.ProofCandidates(meta) {
		proof, err = s.BlockProof(ctx, kind, block)
		if err == nil {
			break
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	if err != nil {
		return nil, err
	}

	return &storage.ServedBlockFull{
		ID:     block,
		Proof:  proof,
		Block:  data,
		IsLink: meta.Has(storage.BlockMetaServedFullIsLink),
	}, nil
}

func (s *Store) NextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	nextRaw, err := s.getHotCopy(ctx, hotKeyNextBlock(prev))
	if err != nil {
		return nil, err
	}
	next, err := decodeBlockID(nextRaw)
	if err != nil {
		return nil, err
	}
	return s.BlockFull(ctx, next)
}

func (s *Store) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	return s.readArtifact(ctx, hotKeyBlockDataRef(block))
}

func (s *Store) BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	data, err := s.readArtifact(ctx, hotKeyStoredProofRef(kind, block))
	if err != nil && isKeyProofKind(kind) && isMissingProofArtifact(err) {
		return nil, storage.ErrNotFound
	}
	return data, err
}

func (s *Store) ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	return s.readArtifact(ctx, hotKeyZeroStateRef(block))
}

func (s *Store) PersistentStateSize(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (int64, error) {
	record, err := s.persistentStateFileRecord(ctx, block, masterchainBlock, effectiveShard)
	if err != nil {
		return 0, err
	}
	stat, err := os.Stat(s.artifactPath(record.ref.Path))
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, storage.ErrNotFound
		}
		return 0, err
	}
	if stat.Size() < record.ref.Offset+record.ref.Size {
		return 0, storage.ErrNotFound
	}
	return record.ref.Size, nil
}

func (s *Store) PersistentStateSlice(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64, offset int64, maxSize int64) ([]byte, error) {
	record, err := s.persistentStateFileRecord(ctx, block, masterchainBlock, effectiveShard)
	if err != nil {
		return nil, err
	}
	if offset < 0 || maxSize < 0 {
		return nil, fmt.Errorf("invalid persistent state range offset=%d max_size=%d", offset, maxSize)
	}
	if maxSize == 0 {
		return []byte{}, nil
	}
	if offset >= record.ref.Size {
		return nil, nil
	}
	size := record.ref.Size - offset
	if size > maxSize {
		size = maxSize
	}
	return packfile.ReadRange(s.artifactPath(record.ref.Path), record.ref.Offset+offset, size)
}

func (s *Store) persistentStateFileRecord(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (*persistentStateFileRecord, error) {
	raw, err := s.getHotCopy(ctx, hotKeyPersistentStateFile(block, masterchainBlock, effectiveShard))
	if err != nil {
		return nil, err
	}
	return decodePersistentStateFileRecord(raw)
}

func (s *Store) ArchiveInfo(ctx context.Context, masterchainSeqno int32, workchain int32, shard int64) (int64, error) {
	raw, err := s.getHotCopy(ctx, hotKeyArchiveInfo(masterchainSeqno, workchain, shard))
	if errors.Is(err, storage.ErrNotFound) {
		rounded := roundArchiveMasterchainSeqno(masterchainSeqno)
		if rounded != masterchainSeqno {
			raw, err = s.getHotCopy(ctx, hotKeyArchiveInfo(rounded, workchain, shard))
		}
	}
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid archive info payload")
	}
	return int64(binary.BigEndian.Uint64(raw)), nil
}

func roundArchiveMasterchainSeqno(seqno int32) int32 {
	if seqno <= 0 {
		return seqno
	}
	return seqno - seqno%archivePackageMasterchainBlocks
}

func (s *Store) ArchiveSlice(ctx context.Context, archiveID, offset int64, maxSize int32) ([]byte, error) {
	raw, err := s.getHotCopy(ctx, hotKeyArchiveFile(archiveID))
	if err != nil {
		return nil, err
	}
	ref, err := decodeArtifactRef(raw)
	if err != nil {
		return nil, err
	}
	if offset < 0 || maxSize < 0 {
		return nil, fmt.Errorf("invalid archive range offset=%d max_size=%d", offset, maxSize)
	}
	if maxSize == 0 {
		return []byte{}, nil
	}
	if offset >= ref.Size {
		return nil, nil
	}
	size := ref.Size - offset
	if size > int64(maxSize) {
		size = int64(maxSize)
	}
	return packfile.ReadRange(s.artifactPath(ref.Path), offset, size)
}

func (s *Store) appendArtifactEntry(kind string, block ton.BlockIDExt, data []byte) (*storage.ArtifactRef, error) {
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()

	path := s.loosePackPath()
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	ptr, err := packfile.Append(file, packfile.EntryName(kind, block), data, false)
	if err != nil {
		return nil, err
	}
	s.pendingLooseSync[path] = struct{}{}
	return &storage.ArtifactRef{
		Path:   s.relativeArtifactPath(path),
		Offset: ptr.Offset,
		Size:   ptr.Size,
	}, nil
}

func (s *Store) appendProofEntry(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte) (*storage.ArtifactRef, error) {
	if isKeyProofKind(kind) {
		return s.appendKeyBlockProofEntry(kind, block, data)
	}
	return s.appendArtifactEntry(packEntryKindForProofKind(kind), block, data)
}

func (s *Store) appendKeyBlockProofEntry(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte) (*storage.ArtifactRef, error) {
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()

	path := s.keyBlockProofPackPath(block.SeqNo)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	ptr, err := packfile.Append(file, packfile.EntryName(packEntryKindForProofKind(kind), block), data, false)
	if err != nil {
		return nil, err
	}
	s.pendingKeyProofSync[path] = struct{}{}
	return &storage.ArtifactRef{
		Path:   s.relativeArtifactPath(path),
		Offset: ptr.Offset,
		Size:   ptr.Size,
	}, nil
}

func (s *Store) copyProofRefToKeyPack(kind storage.ServedProofKind, block ton.BlockIDExt, ref *storage.ArtifactRef) (*storage.ArtifactRef, error) {
	data, err := s.readArtifactRef(ref)
	if err != nil {
		return nil, err
	}
	return s.appendKeyBlockProofEntry(kind, block, data)
}

func (s *Store) readArtifactRef(ref *storage.ArtifactRef) ([]byte, error) {
	if ref == nil {
		return nil, storage.ErrNotFound
	}
	data, err := packfile.ReadRange(s.artifactPath(ref.Path), ref.Offset, ref.Size)
	if err != nil {
		return nil, err
	}
	if int64(len(data)) != ref.Size {
		return nil, fmt.Errorf("artifact %s range offset=%d size=%d is truncated", ref.Path, ref.Offset, ref.Size)
	}
	return data, nil
}

func (s *Store) readArtifact(ctx context.Context, key []byte) ([]byte, error) {
	raw, err := s.getHotCopy(ctx, key)
	if err != nil {
		return nil, err
	}
	ref, err := decodeArtifactRef(raw)
	if err != nil {
		return nil, err
	}
	return s.readArtifactRef(ref)
}

func (s *Store) artifactPath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(s.dir, path)
}

func (s *Store) relativeArtifactPath(path string) string {
	rel, err := filepath.Rel(s.dir, path)
	if err != nil {
		return path
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) {
		return path
	}
	return rel
}

func (s *Store) loosePackPath() string {
	return filepath.Join(s.dir, "packs", "loose", "blocks.pack")
}

func (s *Store) archivePackPath(archiveID int64) string {
	return filepath.Join(s.dir, "packs", "archive", fmt.Sprintf("%d.pack", archiveID))
}

func (s *Store) keyBlockProofPackPath(seqno uint32) string {
	packageID := seqno - seqno%keyArchiveMasterchainBlocks
	group := packageID / 1000000
	return filepath.Join(
		s.dir,
		"packs",
		"key",
		fmt.Sprintf("key%03d", group),
		fmt.Sprintf("key.archive.%06d.pack", packageID),
	)
}

func isKeyProofKind(kind storage.ServedProofKind) bool {
	return kind == storage.ServedProofKeyBlock || kind == storage.ServedProofKeyBlockLink
}

func blockMetaFlagsForProofKind(kind storage.ServedProofKind) storage.BlockMetaFlags {
	flags := storage.BlockMetaFlagForProof(kind)
	if isKeyProofKind(kind) {
		flags |= storage.BlockMetaIsKeyBlock
	}
	return flags
}

func isMissingProofArtifact(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) || strings.Contains(err.Error(), "is truncated")
}

func packEntryKindForProofKind(kind storage.ServedProofKind) string {
	switch kind {
	case storage.ServedProofBlockLink, storage.ServedProofKeyBlockLink:
		return packfile.KindProofLink
	default:
		return packfile.KindProof
	}
}

func storeArchivePack(src string, dst string) (bool, error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return false, err
	}
	if filepath.Clean(src) == filepath.Clean(dst) {
		return false, validateArchivePack(dst)
	}

	if err := validateArchivePack(src); err != nil {
		return false, err
	}
	if err := os.Rename(src, dst); err == nil {
		return true, validateArchivePack(dst)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dst), filepath.Base(dst)+".*.tmp")
	if err != nil {
		return false, err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err = tmp.Close(); err != nil {
		return false, err
	}

	if err = copyFileSync(src, tmpPath); err != nil {
		return false, err
	}
	if err = validateArchivePack(tmpPath); err != nil {
		return false, err
	}
	if err = os.Rename(tmpPath, dst); err != nil {
		return false, err
	}
	return true, os.Remove(src)
}

func validateArchivePack(path string) error {
	file, err := os.Open(path)
	if err != nil {
		return err
	}
	defer func() { _ = file.Close() }()

	var magic [packfile.HeaderSize]byte
	if _, err = io.ReadFull(file, magic[:]); err != nil {
		return fmt.Errorf("read archive package magic: %w", err)
	}
	if binary.LittleEndian.Uint32(magic[:]) != packfile.PackageMagic {
		return fmt.Errorf("archive package magic mismatch")
	}
	return nil
}

func copyFileSync(src string, dst string) error {
	in, err := os.Open(src)
	if err != nil {
		return err
	}
	defer func() { _ = in.Close() }()

	out, err := os.OpenFile(dst, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	defer func() { _ = out.Close() }()

	if _, err = io.Copy(out, in); err != nil {
		return err
	}
	return out.Sync()
}
