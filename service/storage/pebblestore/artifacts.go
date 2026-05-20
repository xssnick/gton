package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/bits"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"syscall"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

const topShard = int64(-1 << 63)

type archivePackRegistration struct {
	valid            bool
	archiveID        int64
	path             string
	size             int64
	baseSeq          uint32
	startSeq         uint32
	workchain        int32
	shard            int64
	firstMasterSeq   uint32
	firstMasterUTime uint32
	firstMasterLT    uint64
}

type archivePackageMeta struct {
	archiveID        int64
	baseSeq          uint32
	startSeq         uint32
	workchain        int32
	shard            int64
	path             string
	size             int64
	firstMasterSeq   uint32
	firstMasterUTime uint32
	firstMasterLT    uint64
}

type artifactAppendResult struct {
	ref          *storage.ArtifactRef
	registration archivePackRegistration
}

type pendingPackSync struct {
	path string
	seq  uint64
	size int64
}

func (s *Store) SaveBlockFull(block *storage.ServedBlockFull) error {
	if block == nil {
		return fmt.Errorf("served block is nil")
	}

	meta, proofKinds := servedBlockFullMeta(block)
	blockRef, proofRefs, registrations, err := s.servedBlockFullArtifactRefs(block, meta, proofKinds)
	if err != nil {
		return err
	}
	return s.withHotBatch(func(batch *pebble.Batch) error {
		if err := s.registerArchivePacks(batch, registrations); err != nil {
			return err
		}
		if err := s.setServedBlockFullArtifactRefs(batch, block, proofKinds, blockRef, proofRefs); err != nil {
			return err
		}
		return s.setMergedBlockMeta(batch, meta)
	})
}

func servedBlockFullMeta(block *storage.ServedBlockFull) (*storage.BlockMeta, []storage.ServedProofKind) {
	meta := &storage.BlockMeta{
		ID:    block.ID,
		Flags: blockMetaServedFlags(block.IsLink),
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
		block         *storage.ServedBlockFull
		proofKinds    []storage.ServedProofKind
		blockRef      *storage.ArtifactRef
		proofRefs     map[storage.ServedProofKind]*storage.ArtifactRef
		registrations []archivePackRegistration
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
		blockRef, proofRefs, registrations, err := s.servedBlockFullArtifactRefs(full, meta, proofKinds)
		if err != nil {
			return err
		}
		mergeArchiveImportMeta(metas, meta)
		fullWrites = append(fullWrites, fullWrite{
			block:         full,
			proofKinds:    proofKinds,
			blockRef:      blockRef,
			proofRefs:     proofRefs,
			registrations: registrations,
		})
	}

	blockDataWrites := make([]blockDataWrite, 0, len(imported.BlockData))
	for _, block := range imported.BlockData {
		if len(block.Data) == 0 && block.Ref == nil {
			continue
		}
		ref := block.Ref
		var registrations []archivePackRegistration
		if ref == nil {
			meta := &storage.BlockMeta{
				ID:    block.ID,
				Flags: storage.BlockMetaHasBlockData,
			}
			meta = storage.MergeBlockMetaFromBlockData(meta, block.ID, block.Data)
			reusable, err := s.reusableArtifactRef(hotKeyBlockDataRef(block.ID))
			if err == nil {
				ref = reusable
			} else {
				if !errors.Is(err, storage.ErrNotFound) {
					return err
				}
				appended, err := s.appendArchiveEntry(packfile.KindBlock, block.ID, meta, 0, block.Data)
				if err != nil {
					return err
				}
				ref = appended.ref
				registrations = append(registrations, appended.registration)
			}
		}
		meta := &storage.BlockMeta{
			ID:    block.ID,
			Flags: storage.BlockMetaHasBlockData,
		}
		if len(block.Data) > 0 {
			meta = storage.MergeBlockMetaFromBlockData(meta, block.ID, block.Data)
		}
		mergeArchiveImportMeta(metas, meta)
		blockDataWrites = append(blockDataWrites, blockDataWrite{block: block.ID, ref: ref})
		if len(registrations) > 0 {
			fullWrites = append(fullWrites, fullWrite{registrations: registrations})
		}
	}

	proofWrites := make([]proofWrite, 0, len(imported.Proofs))
	for _, proof := range imported.Proofs {
		if len(proof.Data) == 0 && proof.Ref == nil {
			continue
		}
		ref := proof.Ref
		var registrations []archivePackRegistration
		if ref == nil {
			meta := &storage.BlockMeta{
				ID:    proof.ID,
				Flags: blockMetaFlagsForProofKind(proof.Kind),
			}
			reusable, err := s.reusableArtifactRef(hotKeyStoredProofRef(proof.Kind, proof.ID))
			if err == nil {
				ref = reusable
			} else {
				if !errors.Is(err, storage.ErrNotFound) {
					return err
				}
				appended, err := s.appendProofEntry(proof.Kind, proof.ID, meta, 0, proof.Data)
				if err != nil {
					return err
				}
				ref = appended.ref
				if appended.registration.valid {
					registrations = append(registrations, appended.registration)
				}
			}
		} else if isKeyProofKind(proof.Kind) {
			var err error
			ref, err = s.copyProofRefToKeyPack(proof.Kind, proof.ID, ref)
			if err != nil {
				return err
			}
		}
		mergeArchiveImportMeta(metas, &storage.BlockMeta{
			ID:    proof.ID,
			Flags: blockMetaFlagsForProofKind(proof.Kind),
		})
		proofWrites = append(proofWrites, proofWrite{kind: proof.Kind, block: proof.ID, ref: ref})
		if len(registrations) > 0 {
			fullWrites = append(fullWrites, fullWrite{registrations: registrations})
		}
	}

	metaKeys := make([]string, 0, len(metas))
	for key := range metas {
		metaKeys = append(metaKeys, key)
	}
	sort.Strings(metaKeys)

	return s.withHotBatch(func(batch *pebble.Batch) error {
		for _, write := range fullWrites {
			if err := s.registerArchivePacks(batch, write.registrations); err != nil {
				return err
			}
			if write.block == nil {
				continue
			}
			if err := s.setServedBlockFullArtifactRefs(batch, write.block, write.proofKinds, write.blockRef, write.proofRefs); err != nil {
				return err
			}
		}
		for _, write := range blockDataWrites {
			if err := batch.Set(hotKeyBlockDataRef(write.block), encodeArtifactRef(write.ref), pebble.NoSync); err != nil {
				return err
			}
		}
		for _, write := range proofWrites {
			if err := batch.Set(hotKeyStoredProofRef(write.kind, write.block), encodeArtifactRef(write.ref), pebble.NoSync); err != nil {
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

func (s *Store) servedBlockFullArtifactRefs(block *storage.ServedBlockFull, meta *storage.BlockMeta, proofKinds []storage.ServedProofKind) (*storage.ArtifactRef, map[storage.ServedProofKind]*storage.ArtifactRef, []archivePackRegistration, error) {
	if len(block.Block) == 0 && len(block.Proof) == 0 && block.BlockRef == nil && block.ProofRef == nil {
		return nil, nil, nil, nil
	}

	var registrations []archivePackRegistration
	blockRef := block.BlockRef
	if len(block.Block) > 0 && blockRef == nil {
		ref, err := s.reusableArtifactRef(hotKeyBlockDataRef(block.ID))
		if err == nil {
			blockRef = ref
		} else {
			if !errors.Is(err, storage.ErrNotFound) {
				return nil, nil, nil, err
			}
			appended, err := s.appendArchiveEntry(packfile.KindBlock, block.ID, meta, block.ArchiveShardSplitDepth, block.Block)
			if err != nil {
				return nil, nil, nil, err
			}
			blockRef = appended.ref
			registrations = append(registrations, appended.registration)
		}
	}

	proofRefs := make(map[storage.ServedProofKind]*storage.ArtifactRef, len(proofKinds))
	if len(proofKinds) > 0 {
		if len(block.Proof) > 0 {
			for _, kind := range proofKinds {
				ref, err := s.reusableArtifactRef(hotKeyStoredProofRef(kind, block.ID))
				if err == nil {
					proofRefs[kind] = ref
					continue
				}
				if !errors.Is(err, storage.ErrNotFound) {
					return nil, nil, nil, err
				}

				appended, err := s.appendProofEntry(kind, block.ID, meta, block.ArchiveShardSplitDepth, block.Proof)
				if err != nil {
					return nil, nil, nil, err
				}
				proofRefs[kind] = appended.ref
				if appended.registration.valid {
					registrations = append(registrations, appended.registration)
				}
			}
		} else if block.ProofRef != nil {
			for _, kind := range proofKinds {
				ref := block.ProofRef
				if isKeyProofKind(kind) {
					var err error
					ref, err = s.copyProofRefToKeyPack(kind, block.ID, block.ProofRef)
					if err != nil {
						return nil, nil, nil, err
					}
				}
				proofRefs[kind] = ref
			}
		}
	}

	return blockRef, proofRefs, registrations, nil
}

func (s *Store) setServedBlockFullArtifactRefs(batch *pebble.Batch, block *storage.ServedBlockFull, proofKinds []storage.ServedProofKind, blockRef *storage.ArtifactRef, proofRefs map[storage.ServedProofKind]*storage.ArtifactRef) error {
	if blockRef != nil {
		if err := batch.Set(hotKeyBlockDataRef(block.ID), encodeArtifactRef(blockRef), pebble.NoSync); err != nil {
			return err
		}
	}
	for _, kind := range proofKinds {
		ref := proofRefs[kind]
		if ref == nil {
			continue
		}
		if err := batch.Set(hotKeyStoredProofRef(kind, block.ID), encodeArtifactRef(ref), pebble.NoSync); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) reusableArtifactRef(key []byte) (*storage.ArtifactRef, error) {
	raw, err := s.getHotCopy(context.Background(), key)
	if err != nil {
		return nil, err
	}
	ref, err := decodeArtifactRef(raw)
	if err != nil {
		return nil, err
	}
	if err = s.checkArtifactRef(ref); err != nil {
		return nil, err
	}
	return ref, nil
}

func (s *Store) checkArtifactRef(ref *storage.ArtifactRef) error {
	if ref == nil {
		return storage.ErrNotFound
	}
	if ref.Offset < 0 || ref.Size < 0 {
		return storage.ErrNotFound
	}
	const maxInt64 = int64(^uint64(0) >> 1)
	if ref.Offset > maxInt64-ref.Size {
		return storage.ErrNotFound
	}

	stat, err := os.Stat(s.artifactPath(ref.Path))
	if err != nil {
		if isMissingArtifactError(err) {
			return storage.ErrNotFound
		}
		return err
	}
	if stat.Size() < ref.Offset+ref.Size {
		return storage.ErrNotFound
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
	if len(data) == 0 {
		return nil
	}
	meta := &storage.BlockMeta{
		ID:    block,
		Flags: storage.BlockMetaHasBlockData,
	}
	meta = storage.MergeBlockMetaFromBlockData(meta, block, data)
	var registrations []archivePackRegistration
	if ref == nil {
		appended, err := s.appendArchiveEntry(packfile.KindBlock, block, meta, 0, data)
		if err != nil {
			return err
		}
		ref = appended.ref
		registrations = append(registrations, appended.registration)
	}
	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		if err := s.registerArchivePacks(batch, registrations); err != nil {
			return err
		}
		return batch.Set(hotKeyBlockDataRef(block), encodeArtifactRef(ref), pebble.NoSync)
	}); err != nil {
		return err
	}

	return s.mergeAndStoreBlockMeta(meta)
}

func (s *Store) SaveBlockProof(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error {
	if len(data) == 0 && ref == nil {
		return nil
	}
	meta := &storage.BlockMeta{
		ID:    block,
		Flags: blockMetaFlagsForProofKind(kind),
	}
	var registrations []archivePackRegistration
	if ref == nil {
		appended, err := s.appendProofEntry(kind, block, meta, 0, data)
		if err != nil {
			return err
		}
		ref = appended.ref
		if appended.registration.valid {
			registrations = append(registrations, appended.registration)
		}
	} else if isKeyProofKind(kind) {
		var err error
		ref, err = s.copyProofRefToKeyPack(kind, block, ref)
		if err != nil {
			return err
		}
	}
	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		if err := s.registerArchivePacks(batch, registrations); err != nil {
			return err
		}
		return batch.Set(hotKeyStoredProofRef(kind, block), encodeArtifactRef(ref), pebble.NoSync)
	}); err != nil {
		return err
	}
	return s.mergeAndStoreBlockMeta(&storage.BlockMeta{
		ID:    block,
		Flags: blockMetaFlagsForProofKind(kind),
	})
}

func (s *Store) SaveZeroState(block ton.BlockIDExt, data []byte, ref *storage.ArtifactRef) error {
	if len(data) == 0 {
		return nil
	}
	if ref == nil {
		var err error
		ref, err = s.writeStateArtifactFile(block, data)
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
	}
	record := encodePersistentStateFileRecord(&stored)
	key := hotKeyPersistentStateFile(stored.Block, stored.MasterchainBlock, stored.EffectiveShard)

	return s.withHotBatch(func(batch *pebble.Batch) error {
		if err := batch.Set(key, record, pebble.NoSync); err != nil {
			return err
		}
		if stored.EffectiveShard == 0 {
			return s.setMergedBlockMeta(batch, meta)
		}
		return nil
	})
}

func (s *Store) DeletePersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) error {
	record, err := s.persistentStateFileRecord(ctx, block, masterchainBlock, effectiveShard)
	if err != nil {
		return err
	}

	key := hotKeyPersistentStateFile(block, masterchainBlock, effectiveShard)
	if err = s.withHotBatch(func(batch *pebble.Batch) error {
		if err := batch.Delete(key, pebble.NoSync); err != nil {
			return err
		}
		if effectiveShard == 0 {
			return s.clearPersistentStateSnapshotMeta(batch, block)
		}
		return nil
	}); err != nil {
		return err
	}

	if record.ref.Offset != 0 {
		return nil
	}
	if err = os.Remove(s.artifactPath(record.ref.Path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	return nil
}

func (s *Store) clearPersistentStateSnapshotMeta(batch *pebble.Batch, block ton.BlockIDExt) error {
	key := hotKeyBlockMeta(block)
	raw, err := pebbleReaderGetCopy(s.hot, key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}

	meta, err := decodeBlockMeta(block, raw)
	if err != nil {
		return err
	}
	meta.Flags &^= storage.BlockMetaHasStateSnapshot
	meta.StateFileHash = nil
	return batch.Set(key, encodeBlockMeta(meta), pebble.NoSync)
}

func (s *Store) SaveArchiveFile(masterchainSeqno int32, workchain int32, shard int64, archiveID int64, path string) (storage.SavedArchiveFile, error) {
	baseSeqno := uint32(archiveID)
	masterSeqno := uint32(masterchainSeqno)
	if masterSeqno < baseSeqno {
		return storage.SavedArchiveFile{}, fmt.Errorf("archive masterchain seqno %d is before package base %d", masterchainSeqno, baseSeqno)
	}
	sliceSeqno := archiveSliceSeqno(baseSeqno, masterSeqno)
	shardPrefix := archivePackShardPrefix(workchain, shard, 0)
	storedPath := s.archivePackPath(sliceSeqno, workchain, shardPrefix)
	localArchiveID := archivePackID(baseSeqno, sliceSeqno, workchain, shardPrefix)

	s.artifactMu.Lock()
	if filepath.Clean(path) != filepath.Clean(storedPath) {
		if err := s.ensureCleanPackTail(storedPath, s.pendingArchiveSync, s.dirtyArchivePacks); err != nil {
			s.artifactMu.Unlock()
			return storage.SavedArchiveFile{}, err
		}
	}
	storeResult, err := storeArchivePack(path, storedPath)
	if err != nil {
		s.markDirtyPackTail(s.dirtyArchivePacks, storedPath)
		s.artifactMu.Unlock()
		return storage.SavedArchiveFile{}, err
	}
	stat, err := os.Stat(storedPath)
	if err == nil && storeResult.stored {
		s.markPendingPackSync(s.pendingArchiveSync, storedPath)
	}
	s.artifactMu.Unlock()
	if err != nil {
		return storage.SavedArchiveFile{}, err
	}
	ref := &storage.ArtifactRef{
		Path: s.relativeArtifactPath(storedPath),
		Size: stat.Size(),
	}
	firstMasterUTime, firstMasterLT := s.archivePackageFirstMasterFromStoredMeta(uint32(masterchainSeqno))
	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		reg := archivePackRegistration{
			valid:            true,
			archiveID:        localArchiveID,
			path:             ref.Path,
			size:             ref.Size,
			baseSeq:          baseSeqno,
			startSeq:         sliceSeqno,
			workchain:        workchain,
			shard:            shardPrefix,
			firstMasterSeq:   uint32(masterchainSeqno),
			firstMasterUTime: firstMasterUTime,
			firstMasterLT:    firstMasterLT,
		}
		if err := batch.Set(hotKeyArchivePackageStart(baseSeqno), []byte{1}, pebble.NoSync); err != nil {
			return err
		}
		return s.registerArchivePacks(batch, []archivePackRegistration{reg})
	}); err != nil {
		return storage.SavedArchiveFile{}, err
	}
	return storage.SavedArchiveFile{
		Path:           storedPath,
		ReusedExisting: storeResult.reusedExisting,
	}, nil
}

func (s *Store) archivePackageFirstMasterFromStoredMeta(masterchainSeqno uint32) (uint32, uint64) {
	block, err := s.LookupBlockBySeqNo(context.Background(), storage.BlockHistoryKey{Workchain: -1, Shard: topShard}, masterchainSeqno)
	if err != nil {
		return 0, 0
	}
	meta, err := s.BlockMeta(context.Background(), block)
	if err != nil || meta == nil {
		return 0, 0
	}
	lt := meta.StartLT
	if lt == 0 {
		lt = meta.EndLT
	}
	return meta.GenUTime, lt
}

func (s *Store) syncPendingArtifactFiles() error {
	if err := s.syncPendingArchiveFiles(); err != nil {
		return err
	}
	return s.syncPendingKeyProofFiles()
}

func (s *Store) syncPendingArchiveFiles() error {
	return s.syncPendingPackFiles(s.pendingArchiveSync, "archive")
}

func (s *Store) syncPendingKeyProofFiles() error {
	return s.syncPendingPackFiles(s.pendingKeyProofSync, "key proof")
}

func (s *Store) syncPendingPackFiles(pending map[string]uint64, label string) error {
	s.artifactMu.Lock()
	if len(pending) == 0 {
		s.artifactMu.Unlock()
		return nil
	}

	items := make([]pendingPackSync, 0, len(pending))
	for path, seq := range pending {
		stat, err := os.Stat(path)
		if err != nil {
			s.artifactMu.Unlock()
			return fmt.Errorf("stat %s pending pack %s: %w", label, path, err)
		}
		items = append(items, pendingPackSync{
			path: path,
			seq:  seq,
			size: stat.Size(),
		})
	}
	s.artifactMu.Unlock()

	sort.Slice(items, func(i, j int) bool {
		return items[i].path < items[j].path
	})

	dirs := make(map[string]struct{}, len(items))
	for _, item := range items {
		if err := syncFile(item.path); err != nil {
			return fmt.Errorf("sync %s pack %s: %w", label, item.path, err)
		}
		dirs[filepath.Dir(item.path)] = struct{}{}
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
	if err := s.commitSyncedPackSizes(items); err != nil {
		return err
	}

	s.artifactMu.Lock()
	for _, item := range items {
		if pending[item.path] == item.seq {
			delete(pending, item.path)
		}
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
	return s.readArtifact(ctx, hotKeyStoredProofRef(kind, block))
}

func (s *Store) ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	return s.readArtifact(ctx, hotKeyZeroStateRef(block))
}

func (s *Store) StoredZeroStateBlocks(ctx context.Context) ([]ton.BlockIDExt, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return nil, err
	}
	defer s.releaseHotDB()

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: bytes.Clone(hotPrefixZeroStateRef),
		UpperBound: appendPrefixUpperBound(hotPrefixZeroStateRef),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var blocks []ton.BlockIDExt
	for iter.First(); iter.Valid(); iter.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		key := iter.Key()
		if len(key) != len(hotPrefixZeroStateRef)+80 || !bytes.HasPrefix(key, hotPrefixZeroStateRef) {
			return nil, fmt.Errorf("invalid zerostate key size %d", len(key))
		}
		block, err := decodeBlockID(key[len(hotPrefixZeroStateRef):])
		if err != nil {
			return nil, fmt.Errorf("decode zerostate key: %w", err)
		}
		blocks = append(blocks, block)
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}
	if len(blocks) == 0 {
		return nil, storage.ErrNotFound
	}
	return blocks, nil
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

func (s *Store) PersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (*storage.PersistentStateFile, error) {
	record, err := s.persistentStateFileRecord(ctx, block, masterchainBlock, effectiveShard)
	if err != nil {
		return nil, err
	}
	ref := record.ref.Clone()
	ref.Path = s.artifactPath(ref.Path)
	return &storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: masterchainBlock,
		EffectiveShard:   effectiveShard,
		Ref:              ref,
		FileHash:         bytes.Clone(record.fileHash),
		StateRootHash:    bytes.Clone(record.stateRootHash),
	}, nil
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
	return s.readArtifactRange(record.ref.Path, record.ref.Offset+offset, size, record.ref.Offset+record.ref.Size)
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
	if errors.Is(err, storage.ErrNotFound) && workchain != -1 {
		raw, err = s.archiveInfoForSplitShardPrefix(ctx, masterchainSeqno, workchain, shard)
	}
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid archive info payload")
	}
	return int64(binary.BigEndian.Uint64(raw)), nil
}

func (s *Store) archiveInfoForSplitShardPrefix(ctx context.Context, masterchainSeqno int32, workchain int32, shard int64) ([]byte, error) {
	for depth := uint32(1); depth <= 60; depth++ {
		prefix := archivePackShardPrefix(workchain, shard, depth)
		if prefix == shard {
			continue
		}
		raw, err := s.getHotCopy(ctx, hotKeyArchiveInfo(masterchainSeqno, workchain, prefix))
		if err == nil {
			return raw, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}
	return nil, storage.ErrNotFound
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
	return s.readArtifactRange(ref.Path, offset, size, ref.Size)
}

func (s *Store) appendArchiveEntry(kind string, block ton.BlockIDExt, meta *storage.BlockMeta, splitDepth uint32, data []byte) (artifactAppendResult, error) {
	location, err := s.archiveEntryLocation(block, meta, splitDepth)
	if err != nil {
		return artifactAppendResult{}, err
	}
	firstMasterSeq, firstMasterUTime, firstMasterLT, err := archiveEntryFirstMaster(block, meta)
	if err != nil {
		return artifactAppendResult{}, err
	}

	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()

	path := s.archivePackPath(location.sliceSeqno, location.workchain, location.shard)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return artifactAppendResult{}, err
	}
	if err := s.ensureCleanPackTail(path, s.pendingArchiveSync, s.dirtyArchivePacks); err != nil {
		return artifactAppendResult{}, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return artifactAppendResult{}, err
	}
	defer func() { _ = file.Close() }()

	ptr, err := packfile.Append(file, packfile.EntryName(kind, block), data, false)
	if err != nil {
		s.markDirtyPackTail(s.dirtyArchivePacks, path)
		return artifactAppendResult{}, err
	}
	stat, err := file.Stat()
	if err != nil {
		return artifactAppendResult{}, err
	}
	s.markPendingPackSync(s.pendingArchiveSync, path)

	ref := &storage.ArtifactRef{
		Path:   s.relativeArtifactPath(path),
		Offset: ptr.Offset,
		Size:   ptr.Size,
	}
	return artifactAppendResult{
		ref: ref,
		registration: archivePackRegistration{
			valid:            true,
			archiveID:        location.archiveID,
			path:             ref.Path,
			size:             stat.Size(),
			baseSeq:          location.baseSeqno,
			startSeq:         location.sliceSeqno,
			workchain:        location.workchain,
			shard:            location.shard,
			firstMasterSeq:   firstMasterSeq,
			firstMasterUTime: firstMasterUTime,
			firstMasterLT:    firstMasterLT,
		},
	}, nil
}

func (s *Store) appendProofEntry(kind storage.ServedProofKind, block ton.BlockIDExt, meta *storage.BlockMeta, splitDepth uint32, data []byte) (artifactAppendResult, error) {
	if isKeyProofKind(kind) {
		ref, err := s.appendKeyBlockProofEntry(kind, block, data)
		if err != nil {
			return artifactAppendResult{}, err
		}
		return artifactAppendResult{ref: ref}, nil
	}
	return s.appendArchiveEntry(packEntryKindForProofKind(kind), block, meta, splitDepth, data)
}

func (s *Store) appendKeyBlockProofEntry(kind storage.ServedProofKind, block ton.BlockIDExt, data []byte) (*storage.ArtifactRef, error) {
	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()

	path := s.keyBlockProofPackPath(block.SeqNo)
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return nil, err
	}
	if err := s.ensureCleanPackTail(path, s.pendingKeyProofSync, s.dirtyKeyProofPacks); err != nil {
		return nil, err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o644)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	ptr, err := packfile.Append(file, packfile.EntryName(packEntryKindForProofKind(kind), block), data, false)
	if err != nil {
		s.markDirtyPackTail(s.dirtyKeyProofPacks, path)
		return nil, err
	}
	s.markPendingPackSync(s.pendingKeyProofSync, path)
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
	return s.readArtifactRange(ref.Path, ref.Offset, ref.Size, ref.Offset+ref.Size)
}

func (s *Store) readArtifactRange(path string, offset int64, size int64, minFileSize int64) ([]byte, error) {
	if offset < 0 || size < 0 || minFileSize < 0 {
		return nil, fmt.Errorf("invalid artifact range offset=%d size=%d min_file_size=%d", offset, size, minFileSize)
	}

	file, err := os.Open(s.artifactPath(path))
	if err != nil {
		if isMissingArtifactError(err) {
			return nil, storage.ErrNotFound
		}
		return nil, err
	}
	defer func() { _ = file.Close() }()

	stat, err := file.Stat()
	if err != nil {
		return nil, err
	}
	if stat.Size() < minFileSize || offset+size > stat.Size() {
		return nil, storage.ErrNotFound
	}

	data := make([]byte, size)
	if _, err = file.ReadAt(data, offset); err != nil {
		if isMissingArtifactError(err) {
			return nil, storage.ErrNotFound
		}
		return nil, err
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

func (s *Store) archivePackPath(seqno uint32, workchain int32, shard int64) string {
	group := seqno / 100000
	name := fmt.Sprintf("archive.%05d", seqno)
	if workchain != -1 || shard != topShard {
		name = fmt.Sprintf("%s.%d_%016x", name, workchain, uint64(shard))
	}
	return filepath.Join(
		s.dir,
		"archive",
		"packages",
		fmt.Sprintf("arch%04d", group),
		name+".pack",
	)
}

func (s *Store) keyBlockProofPackPath(seqno uint32) string {
	packageID := seqno - seqno%keyArchiveMasterchainBlocks
	group := packageID / 1000000
	return filepath.Join(
		s.dir,
		"archive",
		"packages",
		fmt.Sprintf("key%03d", group),
		fmt.Sprintf("key.archive.%06d.pack", packageID),
	)
}

type archiveEntryLocation struct {
	archiveID  int64
	baseSeqno  uint32
	sliceSeqno uint32
	workchain  int32
	shard      int64
}

func (s *Store) archiveEntryLocation(block ton.BlockIDExt, meta *storage.BlockMeta, splitDepth uint32) (archiveEntryLocation, error) {
	masterSeqno, err := archiveEntryMasterSeqno(block, meta)
	if err != nil {
		return archiveEntryLocation{}, err
	}

	isKey := block.Workchain == -1 && block.Shard == topShard && meta != nil && meta.Has(storage.BlockMetaIsKeyBlock)
	baseSeqno, err := s.archivePackageBaseSeqno(masterSeqno, isKey)
	if err != nil {
		return archiveEntryLocation{}, err
	}
	sliceSeqno := archiveSliceSeqno(baseSeqno, masterSeqno)
	workchain, shard := archiveEntryShardPrefix(block, splitDepth)

	return archiveEntryLocation{
		archiveID:  archivePackID(baseSeqno, sliceSeqno, workchain, shard),
		baseSeqno:  baseSeqno,
		sliceSeqno: sliceSeqno,
		workchain:  workchain,
		shard:      shard,
	}, nil
}

func archiveEntryMasterSeqno(block ton.BlockIDExt, meta *storage.BlockMeta) (uint32, error) {
	if block.Workchain == -1 && block.Shard == topShard {
		return block.SeqNo, nil
	}
	if meta == nil || meta.MasterchainRef == nil {
		return 0, fmt.Errorf("masterchain reference is required for archive shard block %s", storage.FormatBlockRef(block))
	}
	return meta.MasterchainRef.SeqNo, nil
}

func archiveEntryFirstMaster(block ton.BlockIDExt, meta *storage.BlockMeta) (uint32, uint32, uint64, error) {
	seqno, err := archiveEntryMasterSeqno(block, meta)
	if err != nil {
		return 0, 0, 0, err
	}
	var utime uint32
	var lt uint64
	if meta != nil {
		utime = meta.GenUTime
		lt = meta.StartLT
		if lt == 0 {
			lt = meta.EndLT
		}
	}
	return seqno, utime, lt, nil
}

func archiveEntryShardPrefix(block ton.BlockIDExt, splitDepth uint32) (int32, int64) {
	if block.Workchain == -1 && block.Shard == topShard {
		return -1, topShard
	}
	return block.Workchain, archivePackShardPrefix(block.Workchain, block.Shard, splitDepth)
}

func archivePackShardPrefix(workchain int32, shard int64, splitDepth uint32) int64 {
	if workchain == -1 && shard == topShard {
		return topShard
	}
	if splitDepth == 0 {
		return shard
	}
	if splitDepth >= 63 {
		return int64(uint64(shard) | 1)
	}

	value := uint64(shard) | 1
	mask := ^uint64(0) << (64 - splitDepth)
	return int64((value & mask) | (uint64(1) << (63 - splitDepth)))
}

func archiveSliceSeqno(baseSeqno uint32, masterSeqno uint32) uint32 {
	if masterSeqno <= baseSeqno {
		return baseSeqno
	}
	return baseSeqno + ((masterSeqno-baseSeqno)/archiveSliceMasterchainBlocks)*archiveSliceMasterchainBlocks
}

func archivePackID(baseSeqno uint32, sliceSeqno uint32, workchain int32, shard int64) int64 {
	idx := archivePackIndex(baseSeqno, sliceSeqno, workchain, shard)
	return int64(uint64(idx)<<32 | uint64(baseSeqno))
}

func archivePackIndex(baseSeqno uint32, sliceSeqno uint32, workchain int32, shard int64) uint32 {
	sliceIndex := uint32(0)
	if sliceSeqno > baseSeqno {
		sliceIndex = (sliceSeqno - baseSeqno) / archiveSliceMasterchainBlocks
	}
	idx := sliceIndex * archivePackIndexSliceStride
	if workchain == -1 && shard == topShard {
		return idx
	}
	return idx + 1 + archiveShardIndex(workchain, shard)
}

const (
	archivePackIndexSliceStride     = 1 << 20
	archivePackIndexMaxShardDepth   = 12
	archivePackIndexWorkchainStride = 1 << (archivePackIndexMaxShardDepth + 1)
)

func archiveShardIndex(workchain int32, shard int64) uint32 {
	workchainOffset := uint32(workchain+1) * archivePackIndexWorkchainStride
	depth := archiveShardPrefixLength(shard)
	if depth <= 0 {
		return workchainOffset
	}
	if depth > archivePackIndexMaxShardDepth {
		depth = archivePackIndexMaxShardDepth
	}

	prefix := uint64(shard) >> (64 - depth)
	shardOffset := (uint32(1) << uint(depth)) - 1 + uint32(prefix)
	return workchainOffset + shardOffset
}

func archiveShardPrefixLength(shard int64) int {
	value := uint64(shard)
	if value == 0 {
		return 0
	}
	return 63 - bits.TrailingZeros64(value)
}

func (s *Store) archivePackageBaseSeqno(masterSeqno uint32, isKey bool) (uint32, error) {
	if isKey {
		return masterSeqno, nil
	}

	rounded := masterSeqno - masterSeqno%archivePackageMasterchainBlocks
	latest, err := s.latestArchivePackageStart(masterSeqno)
	if errors.Is(err, storage.ErrNotFound) {
		return rounded, nil
	}
	if err != nil {
		return 0, err
	}
	if latest > rounded {
		return latest, nil
	}
	return rounded, nil
}

func (s *Store) latestArchivePackageStart(masterSeqno uint32) (uint32, error) {
	db, err := s.acquireHotDB(context.Background())
	if err != nil {
		return 0, err
	}
	defer s.releaseHotDB()

	upper := hotKeyArchivePackageStart(masterSeqno + 1)
	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: hotKeyArchivePackageStartPrefix(),
		UpperBound: upper,
	})
	if err != nil {
		return 0, err
	}
	defer func() { _ = iter.Close() }()

	if !iter.Last() {
		if err = iter.Error(); err != nil {
			return 0, err
		}
		return 0, storage.ErrNotFound
	}
	key := iter.Key()
	if len(key) != len(hotPrefixPackStart)+4 || !bytes.HasPrefix(key, hotPrefixPackStart) {
		return 0, storage.ErrNotFound
	}
	return binary.BigEndian.Uint32(key[len(hotPrefixPackStart):]), nil
}

func (s *Store) registerArchivePacks(batch *pebble.Batch, registrations []archivePackRegistration) error {
	for _, reg := range registrations {
		if !reg.valid {
			continue
		}
		if err := s.setArchiveFileRef(batch, reg.archiveID, &storage.ArtifactRef{
			Path: reg.path,
			Size: reg.size,
		}); err != nil {
			return err
		}
		if err := batch.Set(hotKeyArchivePackageStart(reg.baseSeq), []byte{1}, pebble.NoSync); err != nil {
			return err
		}
		if err := s.setArchivePackageMeta(batch, reg); err != nil {
			return err
		}
		if err := s.registerArchiveInfoRange(batch, reg.startSeq, reg.workchain, reg.shard, reg.archiveID); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) setArchivePackageMeta(batch *pebble.Batch, reg archivePackRegistration) error {
	meta := archivePackageMeta{
		archiveID:        reg.archiveID,
		baseSeq:          reg.baseSeq,
		startSeq:         reg.startSeq,
		workchain:        reg.workchain,
		shard:            reg.shard,
		path:             reg.path,
		size:             reg.size,
		firstMasterSeq:   reg.firstMasterSeq,
		firstMasterUTime: reg.firstMasterUTime,
		firstMasterLT:    reg.firstMasterLT,
	}

	key := hotKeyArchivePackage(reg.archiveID)
	old, err := pebbleReaderGetCopy(s.hot, key)
	if err == nil {
		current, err := decodeArchivePackageMeta(old)
		if err != nil {
			return err
		}
		if current.path != meta.path {
			return fmt.Errorf("archive package path mismatch archive_id=%d old=%s new=%s", reg.archiveID, current.path, meta.path)
		}
		if current.baseSeq != meta.baseSeq || current.startSeq != meta.startSeq || current.workchain != meta.workchain || current.shard != meta.shard {
			return fmt.Errorf("archive package descriptor mismatch archive_id=%d", reg.archiveID)
		}
		if current.size > meta.size {
			meta.size = current.size
		}
		if current.firstMasterSeq != 0 && (meta.firstMasterSeq == 0 || current.firstMasterSeq < meta.firstMasterSeq) {
			meta.firstMasterSeq = current.firstMasterSeq
			meta.firstMasterUTime = current.firstMasterUTime
			meta.firstMasterLT = current.firstMasterLT
		} else if current.firstMasterSeq == meta.firstMasterSeq {
			if meta.firstMasterUTime == 0 {
				meta.firstMasterUTime = current.firstMasterUTime
			}
			if meta.firstMasterLT == 0 {
				meta.firstMasterLT = current.firstMasterLT
			}
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return batch.Set(key, encodeArchivePackageMeta(meta), pebble.NoSync)
}

func (s *Store) setArchiveFileRef(batch *pebble.Batch, archiveID int64, ref *storage.ArtifactRef) error {
	key := hotKeyArchiveFile(archiveID)
	old, err := pebbleReaderGetCopy(s.hot, key)
	if err == nil {
		current, err := decodeArtifactRef(old)
		if err != nil {
			return err
		}
		if current.Path != ref.Path {
			return fmt.Errorf("archive file ref path mismatch archive_id=%d old=%s new=%s", archiveID, current.Path, ref.Path)
		}
		if current.Size >= ref.Size {
			return nil
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return batch.Set(key, encodeArtifactRef(ref), pebble.NoSync)
}

func (s *Store) registerArchiveInfoRange(batch *pebble.Batch, startSeq uint32, workchain int32, shard int64, archiveID int64) error {
	for i := uint32(0); i < archiveSliceMasterchainBlocks; i++ {
		seqno := startSeq + i
		if err := batch.Set(hotKeyArchiveInfo(int32(seqno), workchain, shard), encodeInt64(archiveID), pebble.NoSync); err != nil {
			return err
		}
	}
	return nil
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

func packEntryKindForProofKind(kind storage.ServedProofKind) string {
	switch kind {
	case storage.ServedProofBlockLink, storage.ServedProofKeyBlockLink:
		return packfile.KindProofLink
	default:
		return packfile.KindProof
	}
}

func (s *Store) writeStateArtifactFile(block ton.BlockIDExt, data []byte) (*storage.ArtifactRef, error) {
	name, err := storage.ZeroStateFileName(block)
	if err != nil {
		return nil, err
	}
	dir := s.StateFilesDir()
	if err = os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	tmpDir := filepath.Join(s.dir, "archive", "tmp")
	if err = os.MkdirAll(tmpDir, 0o755); err != nil {
		return nil, err
	}
	tmp, err := os.CreateTemp(tmpDir, name+".*.tmp")
	if err != nil {
		return nil, err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()

	if _, err = tmp.Write(data); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err = tmp.Sync(); err != nil {
		_ = tmp.Close()
		return nil, err
	}
	if err = tmp.Close(); err != nil {
		return nil, err
	}

	finalPath := filepath.Join(dir, name)
	if err = os.Rename(tmpPath, finalPath); err != nil {
		return nil, err
	}
	if err = syncDir(dir); err != nil {
		return nil, err
	}
	return &storage.ArtifactRef{
		Path: s.relativeArtifactPath(finalPath),
		Size: int64(len(data)),
	}, nil
}

func (s *Store) markPendingPackSync(pending map[string]uint64, path string) {
	s.artifactSyncSeq++
	pending[path] = s.artifactSyncSeq
}

func (s *Store) commitSyncedPackSizes(items []pendingPackSync) error {
	if len(items) == 0 {
		return nil
	}

	return s.withHotBatch(func(batch *pebble.Batch) error {
		for _, item := range items {
			path := s.relativeArtifactPath(item.path)
			if err := s.setPackCommittedSize(batch, path, item.size); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) setPackCommittedSize(batch *pebble.Batch, path string, size int64) error {
	key := hotKeyPackCommitted(path)
	old, err := pebbleReaderGetCopy(s.hot, key)
	if err == nil {
		if len(old) != 8 {
			return fmt.Errorf("invalid committed pack size record")
		}
		if int64(binary.BigEndian.Uint64(old)) >= size {
			return nil
		}
	} else if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return batch.Set(key, encodeInt64(size), pebble.NoSync)
}

func (s *Store) packCommittedSize(path string) (int64, error) {
	raw, err := pebbleReaderGetCopy(s.hot, hotKeyPackCommitted(path))
	if errors.Is(err, storage.ErrNotFound) {
		return 0, nil
	}
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid committed pack size record")
	}
	return int64(binary.BigEndian.Uint64(raw)), nil
}

func (s *Store) ensureCleanPackTail(path string, pending map[string]uint64, dirty map[string]struct{}) error {
	if _, ok := pending[path]; ok {
		return nil
	}
	if _, ok := dirty[path]; !ok {
		return nil
	}

	if err := s.truncateUncommittedPackTail(path); err != nil {
		return err
	}
	delete(dirty, path)
	return nil
}

func (s *Store) truncateUncommittedPackTail(path string) error {
	if path == "" {
		return nil
	}

	stat, err := os.Stat(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}

	committedSize, err := s.packCommittedSize(s.relativeArtifactPath(path))
	if err != nil {
		return err
	}
	if stat.Size() <= committedSize {
		return nil
	}

	if err := os.Truncate(path, committedSize); err != nil {
		return fmt.Errorf("truncate uncommitted pack %s from %d to %d: %w", path, stat.Size(), committedSize, err)
	}
	return nil
}

func (s *Store) markDirtyPackTail(dirty map[string]struct{}, path string) {
	if path == "" {
		return
	}
	dirty[path] = struct{}{}
}

func isMissingArtifactError(err error) bool {
	return errors.Is(err, os.ErrNotExist) || errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF)
}

func (s *Store) reconcileCommittedArtifactFiles() error {
	committed := map[string]int64{}
	iter, err := s.hot.NewIter(&pebble.IterOptions{
		LowerBound: hotKeyPackCommittedPrefix(),
		UpperBound: appendPrefixUpperBound(hotPrefixPackCommitted),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	for iter.First(); iter.Valid(); iter.Next() {
		key := iter.Key()
		value := iter.Value()
		if len(value) != 8 {
			return fmt.Errorf("invalid committed pack size record")
		}
		relPath := string(key[len(hotPrefixPackCommitted):])
		size := int64(binary.BigEndian.Uint64(value))
		committed[relPath] = size
		path := s.artifactPath(relPath)
		stat, err := os.Stat(path)
		if err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return err
		}
		if stat.Size() > size {
			if err = os.Truncate(path, size); err != nil {
				return fmt.Errorf("truncate committed pack %s to %d: %w", path, size, err)
			}
		}
	}
	if err = iter.Error(); err != nil {
		return err
	}
	return s.removeUncommittedPackFiles(committed)
}

func (s *Store) removeUncommittedPackFiles(committed map[string]int64) error {
	root := filepath.Join(s.dir, "archive", "packages")
	return filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".pack") {
			return nil
		}
		rel := s.relativeArtifactPath(path)
		if _, ok := committed[rel]; ok {
			return nil
		}
		return os.Remove(path)
	})
}

func appendPrefixUpperBound(prefix []byte) []byte {
	upper := bytes.Clone(prefix)
	for i := len(upper) - 1; i >= 0; i-- {
		if upper[i] != 0xff {
			upper[i]++
			return upper[:i+1]
		}
	}
	return nil
}

type archivePackStoreResult struct {
	stored         bool
	reusedExisting bool
}

func storeArchivePack(src string, dst string) (archivePackStoreResult, error) {
	if err := os.MkdirAll(filepath.Dir(dst), 0o755); err != nil {
		return archivePackStoreResult{}, err
	}
	if filepath.Clean(src) == filepath.Clean(dst) {
		return archivePackStoreResult{}, validateArchivePack(dst)
	}

	if err := validateArchivePack(src); err != nil {
		return archivePackStoreResult{}, err
	}
	if _, err := os.Stat(dst); err == nil {
		merged, err := mergeArchivePack(src, dst)
		if err != nil {
			return archivePackStoreResult{}, err
		}
		return archivePackStoreResult{stored: merged, reusedExisting: true}, nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return archivePackStoreResult{}, err
	}
	if err := os.Rename(src, dst); err == nil {
		return archivePackStoreResult{stored: true}, validateArchivePack(dst)
	}

	tmp, err := os.CreateTemp(filepath.Dir(dst), filepath.Base(dst)+".*.tmp")
	if err != nil {
		return archivePackStoreResult{}, err
	}
	tmpPath := tmp.Name()
	defer func() { _ = os.Remove(tmpPath) }()
	if err = tmp.Close(); err != nil {
		return archivePackStoreResult{}, err
	}

	if err = copyFileSync(src, tmpPath); err != nil {
		return archivePackStoreResult{}, err
	}
	if err = validateArchivePack(tmpPath); err != nil {
		return archivePackStoreResult{}, err
	}
	if err = os.Rename(tmpPath, dst); err != nil {
		return archivePackStoreResult{}, err
	}
	if err = os.Remove(src); err != nil && !errors.Is(err, os.ErrNotExist) {
		return archivePackStoreResult{}, err
	}
	return archivePackStoreResult{stored: true}, nil
}

func mergeArchivePack(src string, dst string) (bool, error) {
	if err := validateArchivePack(dst); err != nil {
		return false, err
	}

	existing, err := archivePackEntryNames(dst)
	if err != nil {
		return false, err
	}

	out, err := os.OpenFile(dst, os.O_RDWR, 0o644)
	if err != nil {
		return false, err
	}
	defer func() { _ = out.Close() }()
	stat, err := out.Stat()
	if err != nil {
		return false, err
	}
	originalSize := stat.Size()

	in, err := os.Open(src)
	if err != nil {
		return false, err
	}
	defer func() { _ = in.Close() }()

	merged := false
	if err = packfile.Read(context.Background(), in, func(entry packfile.Entry) error {
		if _, ok := existing[entry.Name]; ok {
			return nil
		}
		if _, err := packfile.Append(out, entry.Name, entry.Data, false); err != nil {
			return err
		}
		existing[entry.Name] = struct{}{}
		merged = true
		return nil
	}); err != nil {
		return false, err
	}
	if merged {
		if err = out.Sync(); err != nil {
			if truncateErr := out.Truncate(originalSize); truncateErr != nil {
				return false, errors.Join(err, fmt.Errorf("rollback archive pack merge %s to %d: %w", dst, originalSize, truncateErr))
			}
			return false, err
		}
	}
	if err = os.Remove(src); err != nil && !errors.Is(err, os.ErrNotExist) {
		return false, err
	}
	return merged, nil
}

func archivePackEntryNames(path string) (map[string]struct{}, error) {
	file, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	defer func() { _ = file.Close() }()

	names := map[string]struct{}{}
	if err = packfile.Read(context.Background(), file, func(entry packfile.Entry) error {
		names[entry.Name] = struct{}{}
		return nil
	}); err != nil {
		return nil, err
	}
	return names, nil
}

func validateArchivePack(path string) error {
	return packfile.Validate(path)
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
