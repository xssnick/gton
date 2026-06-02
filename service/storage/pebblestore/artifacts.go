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

type checkpointArtifactWrite struct {
	block           *storage.ServedBlockFull
	proofKinds      []storage.ServedProofKind
	blockRef        *storage.ArtifactRef
	blockRefIndex   int
	proofRefs       map[storage.ServedProofKind]*storage.ArtifactRef
	proofRefIndexes map[storage.ServedProofKind]int
	meta            *storage.BlockMeta
}

func (s *Store) prepareCheckpointArtifactWrites(entries []storage.StateCheckpointBlock) ([]checkpointArtifactWrite, []archivePackRegistration, []storage.ServedBlockLink, error) {
	const noQueuedRef = -1

	writes := make([]checkpointArtifactWrite, 0, len(entries))
	registrations := make([]archivePackRegistration, 0, len(entries))
	appendRequests := make([]archiveAppendRequest, 0, len(entries)*2)
	var links []storage.ServedBlockLink
	queueArchiveAppend := func(kind string, block ton.BlockIDExt, meta *storage.BlockMeta, splitDepth uint32, data []byte) int {
		idx := len(appendRequests)
		appendRequests = append(appendRequests, archiveAppendRequest{
			kind:       kind,
			block:      block,
			meta:       meta,
			splitDepth: splitDepth,
			data:       data,
		})
		return idx
	}

	for _, entry := range entries {
		if entry.Artifact == nil {
			continue
		}

		meta, proofKinds := servedBlockFullMeta(entry.Artifact)
		var blockRef *storage.ArtifactRef
		blockRefIndex := noQueuedRef
		if len(entry.Artifact.Block) > 0 {
			blockRefIndex = queueArchiveAppend(packfile.KindBlock, entry.Artifact.ID, meta, entry.Artifact.ArchiveShardSplitDepth, entry.Artifact.Block)
		}

		proofRefs := make(map[storage.ServedProofKind]*storage.ArtifactRef, len(proofKinds))
		proofRefIndexes := make(map[storage.ServedProofKind]int, len(proofKinds))
		for _, kind := range proofKinds {
			if len(entry.Artifact.Proof) == 0 {
				continue
			}
			if isKeyProofKind(kind) {
				ref, err := s.appendKeyBlockProofEntry(kind, entry.Artifact.ID, entry.Artifact.Proof)
				if err != nil {
					return nil, nil, nil, err
				}
				proofRefs[kind] = ref
				continue
			}
			proofRefIndexes[kind] = queueArchiveAppend(packEntryKindForProofKind(kind), entry.Artifact.ID, meta, entry.Artifact.ArchiveShardSplitDepth, entry.Artifact.Proof)
		}

		links = append(links, entry.Links...)
		writes = append(writes, checkpointArtifactWrite{
			block:           entry.Artifact,
			proofKinds:      proofKinds,
			blockRef:        blockRef,
			blockRefIndex:   blockRefIndex,
			proofRefs:       proofRefs,
			proofRefIndexes: proofRefIndexes,
			meta:            meta,
		})
	}

	appendedRefs, appendRegistrations, err := s.appendArchiveEntries(appendRequests)
	if err != nil {
		return nil, nil, nil, err
	}
	registrations = append(registrations, appendRegistrations...)
	for idx := range writes {
		write := &writes[idx]
		if write.blockRefIndex != noQueuedRef {
			write.blockRef = appendedRefs[write.blockRefIndex]
		}
		for kind, refIndex := range write.proofRefIndexes {
			write.proofRefs[kind] = appendedRefs[refIndex]
		}
	}

	return writes, registrations, links, nil
}

func (s *Store) setCheckpointArtifactWrites(batch *pebble.Batch, writes []checkpointArtifactWrite, registrations []archivePackRegistration, links []storage.ServedBlockLink) error {
	if err := s.registerArchivePacks(batch, registrations); err != nil {
		return err
	}
	for _, write := range writes {
		if err := s.setServedBlockFullArtifactRefs(batch, write.block, write.proofKinds, write.blockRef, write.proofRefs); err != nil {
			return err
		}
	}

	pendingLinks := make(map[storage.BlockRootHash]ton.BlockIDExt, len(links))
	for _, link := range links {
		if err := s.setNextBlockLinkWithPending(batch, pendingLinks, link.Prev, link.Next); err != nil {
			return err
		}
	}
	return nil
}

func servedBlockFullMeta(block *storage.ServedBlockFull) (*storage.BlockMeta, []storage.ServedProofKind) {
	isLink := storage.ServedBlockProofIsLink(block.ID, block.IsLink)
	meta := &storage.BlockMeta{
		ID:    block.ID,
		Flags: blockMetaServedFlags(isLink),
	}
	if block.Meta != nil {
		meta = storage.MergeBlockMeta(meta, block.Meta)
		meta.ID = block.ID
	}
	if len(block.Block) > 0 {
		meta.Mark(storage.BlockMetaHasBlockData)
		if block.Meta == nil && len(block.Block) > 0 {
			meta = storage.MergeBlockMetaFromBlockData(meta, block.ID, block.Block)
		}
	}

	var proofKinds []storage.ServedProofKind
	if len(block.Proof) > 0 {
		proofKinds = storage.StoredProofKindsForServedBlock(block.ID, block.IsLink, meta.Has(storage.BlockMetaIsKeyBlock))
		for _, kind := range proofKinds {
			meta.Mark(storage.BlockMetaFlagForProof(kind))
		}
	}
	return meta, proofKinds
}

func (s *Store) SaveArchiveImport(imported *storage.ServedArchiveImport) error {
	type fullWrite struct {
		block           *storage.ServedBlockFull
		proofKinds      []storage.ServedProofKind
		blockRef        *storage.ArtifactRef
		blockRefIndex   int
		proofRefs       map[storage.ServedProofKind]*storage.ArtifactRef
		proofRefIndexes map[storage.ServedProofKind]int
	}
	const noQueuedRef = -1

	s.artifactPublishMu.Lock()
	artifactCommitted := false
	defer func() {
		if !artifactCommitted {
			s.abandonPendingArtifactPacks()
		}
		s.artifactPublishMu.Unlock()
	}()

	metas := make(map[storage.BlockRootHash]*storage.BlockMeta, len(imported.FullBlocks))
	registrations := make([]archivePackRegistration, 0, len(imported.FullBlocks))
	appendRequests := make([]archiveAppendRequest, 0, len(imported.FullBlocks)*2)
	queueArchiveAppend := func(kind string, block ton.BlockIDExt, meta *storage.BlockMeta, splitDepth uint32, data []byte) int {
		idx := len(appendRequests)
		appendRequests = append(appendRequests, archiveAppendRequest{
			kind:       kind,
			block:      block,
			meta:       meta,
			splitDepth: splitDepth,
			data:       data,
		})
		return idx
	}

	fullWrites := make([]fullWrite, 0, len(imported.FullBlocks))
	for _, full := range imported.FullBlocks {
		meta, proofKinds := servedBlockFullMeta(full)
		var blockRef *storage.ArtifactRef
		blockRefIndex := noQueuedRef
		if len(full.Block) > 0 {
			reusable, err := s.reusableArtifactRef(hotKeyBlockDataRef(full.ID))
			if err == nil {
				blockRef = reusable
			} else {
				if !errors.Is(err, storage.ErrNotFound) {
					return err
				}
				blockRefIndex = queueArchiveAppend(packfile.KindBlock, full.ID, meta, full.ArchiveShardSplitDepth, full.Block)
			}
		}

		proofRefs := make(map[storage.ServedProofKind]*storage.ArtifactRef, len(proofKinds))
		proofRefIndexes := make(map[storage.ServedProofKind]int, len(proofKinds))
		for _, kind := range proofKinds {
			reusable, err := s.reusableArtifactRef(hotKeyStoredProofRef(kind, full.ID))
			if err == nil {
				proofRefs[kind] = reusable
				continue
			}
			if !errors.Is(err, storage.ErrNotFound) {
				return err
			}

			if len(full.Proof) == 0 {
				continue
			}
			if isKeyProofKind(kind) {
				ref, err := s.appendKeyBlockProofEntry(kind, full.ID, full.Proof)
				if err != nil {
					return err
				}
				proofRefs[kind] = ref
				continue
			}
			proofRefIndexes[kind] = queueArchiveAppend(packEntryKindForProofKind(kind), full.ID, meta, full.ArchiveShardSplitDepth, full.Proof)
		}

		mergeArchiveImportMeta(metas, meta)
		fullWrites = append(fullWrites, fullWrite{
			block:           full,
			proofKinds:      proofKinds,
			blockRef:        blockRef,
			blockRefIndex:   blockRefIndex,
			proofRefs:       proofRefs,
			proofRefIndexes: proofRefIndexes,
		})
	}

	metaKeys := make([]storage.BlockRootHash, 0, len(metas))
	for key := range metas {
		metaKeys = append(metaKeys, key)
	}
	sort.Slice(metaKeys, func(i, j int) bool {
		return bytes.Compare(metaKeys[i][:], metaKeys[j][:]) < 0
	})

	appendedRefs, appendRegistrations, err := s.appendArchiveEntries(appendRequests)
	if err != nil {
		return err
	}
	registrations = append(registrations, appendRegistrations...)
	for idx := range fullWrites {
		write := &fullWrites[idx]
		if write.blockRefIndex != noQueuedRef {
			write.blockRef = appendedRefs[write.blockRefIndex]
		}
		for kind, refIndex := range write.proofRefIndexes {
			write.proofRefs[kind] = appendedRefs[refIndex]
		}
	}

	syncedPacks, err := s.syncPendingArtifactPacks()
	if err != nil {
		return err
	}

	if err := s.withHotBatchOptions(pebble.Sync, func(batch *pebble.Batch) error {
		if err := s.registerArchivePacks(batch, registrations); err != nil {
			return err
		}
		for _, write := range fullWrites {
			if err := s.setServedBlockFullArtifactRefs(batch, write.block, write.proofKinds, write.blockRef, write.proofRefs); err != nil {
				return err
			}
		}
		pendingLinks := make(map[storage.BlockRootHash]ton.BlockIDExt, len(imported.Links))
		for _, link := range imported.Links {
			if err := s.setNextBlockLinkWithPending(batch, pendingLinks, link.Prev, link.Next); err != nil {
				return err
			}
		}
		for _, key := range metaKeys {
			if err := s.setMergedBlockMeta(batch, metas[key]); err != nil {
				return err
			}
		}
		return nil
	}); err != nil {
		return err
	}
	s.clearSyncedArtifactPacks(syncedPacks)
	artifactCommitted = true
	return nil
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

func (s *Store) setNextBlockLinkWithPending(batch *pebble.Batch, pending map[storage.BlockRootHash]ton.BlockIDExt, prev ton.BlockIDExt, next ton.BlockIDExt) error {
	key := hotKeyNextBlock(prev)
	value := encodeBlockID(next)
	pendingKey := storage.BlockKey(prev)
	if existing, ok := pending[pendingKey]; ok {
		if existing.Equals(&next) {
			return nil
		}
		if selected, ok := selectShardSplitNextLink(prev, existing, next); ok {
			if existing.Equals(&selected) {
				s.log.Debug().
					Str("prev", storage.FormatBlockRef(prev)).
					Str("existing_next", storage.FormatBlockRef(existing)).
					Str("ignored_next", storage.FormatBlockRef(next)).
					Msg("keeping selected shard split next block link")
				return nil
			}
			s.log.Debug().
				Str("prev", storage.FormatBlockRef(prev)).
				Str("existing_next", storage.FormatBlockRef(existing)).
				Str("selected_next", storage.FormatBlockRef(selected)).
				Msg("updating shard split next block link to selected child")
			pending[pendingKey] = selected
			return batch.Set(key, encodeBlockID(selected), pebble.NoSync)
		}
		return fmt.Errorf("next block link for %s already points to %s, cannot set %s", storage.FormatBlockRef(prev), storage.FormatBlockRef(existing), storage.FormatBlockRef(next))
	}

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
		if selected, ok := selectShardSplitNextLink(prev, existing, next); ok {
			if existing.Equals(&selected) {
				s.log.Debug().
					Str("prev", storage.FormatBlockRef(prev)).
					Str("existing_next", storage.FormatBlockRef(existing)).
					Str("ignored_next", storage.FormatBlockRef(next)).
					Msg("keeping selected shard split next block link")
				return nil
			}
			s.log.Debug().
				Str("prev", storage.FormatBlockRef(prev)).
				Str("existing_next", storage.FormatBlockRef(existing)).
				Str("selected_next", storage.FormatBlockRef(selected)).
				Msg("updating shard split next block link to selected child")
			if pending != nil {
				pending[pendingKey] = selected
			}
			return batch.Set(key, encodeBlockID(selected), pebble.NoSync)
		}

		return fmt.Errorf("next block link for %s already points to %s, cannot set %s", storage.FormatBlockRef(prev), storage.FormatBlockRef(existing), storage.FormatBlockRef(next))
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	if pending != nil {
		pending[pendingKey] = next
	}
	return batch.Set(key, value, pebble.NoSync)
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
		Meta:   meta.Clone(),
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
