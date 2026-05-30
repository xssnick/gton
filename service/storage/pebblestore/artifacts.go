package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/archive/packfile"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func (s *Store) SaveBlockFull(block *storage.ServedBlockFull) error {
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

	metas := make(map[storage.BlockRootHash]*storage.BlockMeta, len(imported.FullBlocks)+len(imported.BlockData)+len(imported.Proofs))
	fullWrites := make([]fullWrite, 0, len(imported.FullBlocks))
	for _, full := range imported.FullBlocks {
		meta, proofKinds := servedBlockFullMeta(full)
		blockRef, proofRefs, registrations, err := s.servedBlockFullArtifactRefs(full, meta, proofKinds)
		if err != nil {
			return err
		}
		if blockRef != nil {
			registration, err := s.archivePackRegistrationFromRef(full.ID, meta, full.ArchiveShardSplitDepth, blockRef)
			if err != nil {
				return err
			}
			if registration.valid {
				registrations = append(registrations, registration)
			}
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

	metaKeys := make([]storage.BlockRootHash, 0, len(metas))
	for key := range metas {
		metaKeys = append(metaKeys, key)
	}
	sort.Slice(metaKeys, func(i, j int) bool {
		return bytes.Compare(metaKeys[i][:], metaKeys[j][:]) < 0
	})

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
			if err := s.setNextBlockLink(batch, link.Prev, link.Next); err != nil {
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

func mergeArchiveImportMeta(metas map[storage.BlockRootHash]*storage.BlockMeta, meta *storage.BlockMeta) {
	key := storage.BlockKey(meta.ID)
	merged := storage.MergeBlockMeta(metas[key], meta)
	merged.ID = meta.ID
	metas[key] = merged
}

func (s *Store) archivePackRegistrationFromRef(block ton.BlockIDExt, meta *storage.BlockMeta, splitDepth uint32, ref *storage.ArtifactRef) (archivePackRegistration, error) {
	if ref == nil || ref.Path == "" {
		return archivePackRegistration{}, nil
	}
	if block.Workchain != -1 || block.Shard != topShard {
		return archivePackRegistration{}, nil
	}

	location, err := s.archiveEntryLocation(block, meta, splitDepth)
	if err != nil {
		return archivePackRegistration{}, err
	}
	firstMasterSeq, firstMasterUTime, firstMasterLT, err := archiveEntryFirstMaster(block, meta)
	if err != nil {
		return archivePackRegistration{}, err
	}

	path := ref.Path
	if filepath.IsAbs(path) {
		path = s.relativeArtifactPath(path)
	}
	stat, err := os.Stat(s.artifactPath(path))
	if err != nil {
		return archivePackRegistration{}, err
	}
	return archivePackRegistration{
		valid:            true,
		archiveID:        location.archiveID,
		path:             path,
		size:             stat.Size(),
		baseSeq:          location.baseSeqno,
		startSeq:         location.sliceSeqno,
		workchain:        location.workchain,
		shard:            location.shard,
		firstMasterSeq:   firstMasterSeq,
		firstMasterUTime: firstMasterUTime,
		firstMasterLT:    firstMasterLT,
	}, nil
}

func (s *Store) LinkNextBlock(prev ton.BlockIDExt, next ton.BlockIDExt) error {
	return s.withHotBatch(func(batch *pebble.Batch) error {
		return s.setNextBlockLink(batch, prev, next)
	})
}

func (s *Store) setNextBlockLink(batch *pebble.Batch, prev ton.BlockIDExt, next ton.BlockIDExt) error {
	key := hotKeyNextBlock(prev)
	value := encodeBlockID(next)
	current, closer, err := pebbleReaderGet(s.hot, key)
	if err == nil {
		defer func() { _ = closer.Close() }()
		if bytes.Equal(current, value) {
			return nil
		}

		existing, err := decodeBlockID(current)
		if err != nil {
			return err
		}
		if isShardSplitNextLinkConflict(prev, existing, next) {
			s.log.Debug().
				Str("prev", storage.FormatBlockRef(prev)).
				Str("existing_next", storage.FormatBlockRef(existing)).
				Str("next", storage.FormatBlockRef(next)).
				Msg("keeping existing shard split next block link")
			return nil
		}

		return fmt.Errorf("next block link for %s already points to %s, cannot set %s", storage.FormatBlockRef(prev), storage.FormatBlockRef(existing), storage.FormatBlockRef(next))
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return batch.Set(key, value, pebble.NoSync)
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
	raw, closer, err := pebbleReaderGet(s.hot, key)
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	defer func() { _ = closer.Close() }()

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
	return s.readArtifactRange(ctx, record.ref.Path, record.ref.Offset+offset, size, record.ref.Offset+record.ref.Size)
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
	return s.readArtifactRange(ctx, ref.Path, offset, size, ref.Size)
}

func (s *Store) readArtifactRef(ctx context.Context, ref *storage.ArtifactRef) ([]byte, error) {
	if ref == nil {
		return nil, storage.ErrNotFound
	}
	return s.readArtifactRange(ctx, ref.Path, ref.Offset, ref.Size, ref.Offset+ref.Size)
}

func (s *Store) readArtifactRange(ctx context.Context, path string, offset int64, size int64, minFileSize int64) ([]byte, error) {
	if offset < 0 || size < 0 || minFileSize < 0 || artifactRangeOverflow(offset, size) {
		return nil, fmt.Errorf("invalid artifact range offset=%d size=%d min_file_size=%d", offset, size, minFileSize)
	}

	return s.readArtifactFileRange(ctx, s.artifactPath(path), offset, size, minFileSize)
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
	return s.readArtifactRef(ctx, ref)
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
