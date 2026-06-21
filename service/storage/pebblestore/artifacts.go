package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"
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

		meta, proofKinds, err := servedBlockFullMeta(entry.Artifact)
		if err != nil {
			return nil, nil, nil, err
		}
		if entry.State != nil {
			if !entry.State.Block.Equals(&entry.Artifact.ID) {
				return nil, nil, nil, fmt.Errorf(
					"checkpoint artifact %s state block mismatch %s",
					storage.FormatBlockRef(entry.Artifact.ID),
					storage.FormatBlockRef(entry.State.Block),
				)
			}
			meta = storage.MergeBlockMeta(meta, storage.BuildBlockMetaFromState(*entry.State))
		}
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

func (s *Store) setCheckpointArtifactWrites(batch *pebble.Batch, writes []checkpointArtifactWrite, registrations []archivePackRegistration, links []storage.ServedBlockLink, metas map[storage.BlockRootHash]*storage.BlockMeta) error {
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
		if err := s.mergeNextBlockLinkWithPendingMeta(metas, pendingLinks, link.Prev, link.Next); err != nil {
			return err
		}
	}
	return nil
}

func servedBlockFullMeta(block *storage.ServedBlockFull) (*storage.BlockMeta, []storage.ServedProofKind, error) {
	if block == nil {
		return nil, nil, fmt.Errorf("served block is missing")
	}
	if len(block.Block) == 0 && len(block.Proof) == 0 {
		return nil, nil, fmt.Errorf("served block %s has no block data or proof", storage.FormatBlockRef(block.ID))
	}

	isLink := storage.ServedBlockProofIsLink(block.ID, block.IsLink)
	meta := &storage.BlockMeta{
		ID: block.ID,
	}
	if block.Meta != nil {
		if len(block.Meta.NextRefs) > 0 {
			return nil, nil, fmt.Errorf("served block meta %s cannot set next refs directly", storage.FormatBlockRef(block.ID))
		}
		provided := block.Meta.Clone()
		provided.Flags &^= servedBlockPayloadMetaFlags()
		meta = storage.MergeBlockMeta(meta, provided)
		meta.ID = block.ID
	}
	if len(block.Block) > 0 {
		meta.Mark(storage.BlockMetaHasBlockData)
		if block.Meta == nil && len(block.Block) > 0 {
			merged, err := storage.MergeBlockMetaFromBlockData(meta, block.ID, block.Block)
			if err != nil {
				return nil, nil, fmt.Errorf("build block meta from %s: %w", storage.FormatBlockRef(block.ID), err)
			}
			meta = merged
		}
	}

	var proofKinds []storage.ServedProofKind
	if len(block.Proof) > 0 {
		proofKinds = storage.StoredProofKindsForServedBlock(block.ID, block.IsLink, meta.Has(storage.BlockMetaIsKeyBlock))
		for _, kind := range proofKinds {
			meta.Mark(storage.BlockMetaFlagForProof(kind))
		}
	}
	if len(block.Block) > 0 && len(proofKinds) > 0 {
		meta.Mark(blockMetaServedFlags(isLink))
	}
	if !blockMetaHasStoredMetadata(meta) {
		return nil, nil, fmt.Errorf("served block %s has no stored metadata", storage.FormatBlockRef(block.ID))
	}
	return meta, proofKinds, nil
}

func servedBlockPayloadMetaFlags() storage.BlockMetaFlags {
	return storage.BlockMetaHasServedFull |
		storage.BlockMetaServedFullIsLink |
		storage.BlockMetaHasBlockData |
		storage.BlockMetaHasProofBlock |
		storage.BlockMetaHasProofBlockLink |
		storage.BlockMetaHasProofKeyBlock |
		storage.BlockMetaHasProofKeyBlockLink
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

func (s *Store) artifactAvailable(ctx context.Context, key []byte) error {
	raw, err := s.getHotCopy(ctx, key)
	if err != nil {
		return err
	}
	ref, err := decodeArtifactRef(raw)
	if err != nil {
		return err
	}
	return s.checkArtifactRef(ref)
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

	path, err := s.artifactRefPath(context.Background(), ref)
	if err != nil {
		return err
	}
	stat, err := os.Stat(s.artifactPath(path))
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

func mergePendingBlockMeta(metas map[storage.BlockRootHash]*storage.BlockMeta, meta *storage.BlockMeta) {
	key := storage.BlockKey(meta.ID)
	merged := storage.MergeBlockMeta(metas[key], meta)
	merged.ID = meta.ID
	metas[key] = merged
}

func (s *Store) mergeNextBlockLinkWithPendingMeta(metas map[storage.BlockRootHash]*storage.BlockMeta, pending map[storage.BlockRootHash]ton.BlockIDExt, prev ton.BlockIDExt, next ton.BlockIDExt) error {
	pendingKey := storage.BlockKey(prev)
	if meta := metas[pendingKey]; meta != nil {
		if !blockMetaHasServedFull(meta) {
			return nextBlockLinkMissingPreviousError(prev, next)
		}
		if err := s.requireNextBlockLinkTargetMaterialized(metas, prev, next); err != nil {
			return err
		}
		if existing, ok := pending[pendingKey]; ok {
			return s.mergeSelectedNextBlockLink(metas, pending, prev, existing, next)
		}
		if len(meta.NextRefs) > 0 {
			return s.mergeSelectedNextBlockLink(metas, pending, prev, meta.NextRefs[0], next)
		}
		recordPendingNextBlockLink(metas, pending, prev, next)
		return nil
	}

	current, closer, err := pebbleReaderGet(s.hot, hotKeyBlockMeta(prev))
	if err == nil {
		defer func() { _ = closer.Close() }()
		meta, err := decodeBlockMeta(prev, current)
		if err != nil {
			return err
		}
		if !blockMetaHasServedFull(meta) {
			return nextBlockLinkMissingPreviousError(prev, next)
		}
		if err := s.requireNextBlockLinkTargetMaterialized(metas, prev, next); err != nil {
			return err
		}
		if len(meta.NextRefs) > 0 {
			return s.mergeSelectedNextBlockLink(metas, pending, prev, meta.NextRefs[0], next)
		}
		recordPendingNextBlockLink(metas, pending, prev, next)
		return nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return nextBlockLinkMissingPreviousError(prev, next)
}

func blockMetaHasServedFull(meta *storage.BlockMeta) bool {
	return meta != nil && meta.Has(storage.BlockMetaHasServedFull)
}

func nextBlockLinkMissingPreviousError(prev ton.BlockIDExt, next ton.BlockIDExt) error {
	return fmt.Errorf("next block link %s -> %s requires materialized previous block: %w", storage.FormatBlockRef(prev), storage.FormatBlockRef(next), storage.ErrNotFound)
}

func (s *Store) requireNextBlockLinkTargetMaterialized(metas map[storage.BlockRootHash]*storage.BlockMeta, prev ton.BlockIDExt, next ton.BlockIDExt) error {
	key := storage.BlockKey(next)
	if meta := metas[key]; meta != nil {
		if blockMetaHasServedFull(meta) {
			return nil
		}
		return nextBlockLinkMissingTargetError(prev, next)
	}

	current, closer, err := pebbleReaderGet(s.hot, hotKeyBlockMeta(next))
	if err == nil {
		defer func() { _ = closer.Close() }()
		meta, err := decodeBlockMeta(next, current)
		if err != nil {
			return err
		}
		if blockMetaHasServedFull(meta) {
			return nil
		}
		return nextBlockLinkMissingTargetError(prev, next)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return nextBlockLinkMissingTargetError(prev, next)
}

func nextBlockLinkMissingTargetError(prev ton.BlockIDExt, next ton.BlockIDExt) error {
	return fmt.Errorf("next block link %s -> %s requires materialized next block: %w", storage.FormatBlockRef(prev), storage.FormatBlockRef(next), storage.ErrNotFound)
}

func (s *Store) mergeSelectedNextBlockLink(metas map[storage.BlockRootHash]*storage.BlockMeta, pending map[storage.BlockRootHash]ton.BlockIDExt, prev ton.BlockIDExt, existing ton.BlockIDExt, next ton.BlockIDExt) error {
	if existing.Equals(&next) {
		if pending != nil {
			pending[storage.BlockKey(prev)] = next
		}
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
		recordPendingNextBlockLink(metas, pending, prev, selected)
		return nil
	}

	return fmt.Errorf("next block link for %s already points to %s, cannot set %s", storage.FormatBlockRef(prev), storage.FormatBlockRef(existing), storage.FormatBlockRef(next))
}

func recordPendingNextBlockLink(metas map[storage.BlockRootHash]*storage.BlockMeta, pending map[storage.BlockRootHash]ton.BlockIDExt, prev ton.BlockIDExt, next ton.BlockIDExt) {
	key := storage.BlockKey(prev)
	if pending != nil {
		pending[key] = next
	}
	// Pebble batch reads do not see earlier batch.Set calls. Keep every same-batch
	// block-handle update in this pending meta map first, then write each meta once
	// at commit time, otherwise a later metadata update can overwrite a staged
	// NextRefs update for the same previous block.
	mergePendingBlockMeta(metas, &storage.BlockMeta{ID: prev, NextRefs: []ton.BlockIDExt{next}})
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
	} else if err := s.validateZeroStateArtifactPath(block, ref.Path); err != nil {
		return err
	}
	if ref.Size <= 0 {
		return fmt.Errorf("zerostate file size is invalid")
	}
	if ref.Offset != 0 {
		return fmt.Errorf("zerostate file offset must be zero")
	}
	return s.withHotBatch(func(batch *pebble.Batch) error {
		return s.setHotUnique(batch, hotKeyZeroStateRef(block), encodeStateFileRef(ref.Size))
	})
}

func (s *Store) SavePersistentStateFile(file *storage.PersistentStateFile) error {
	if file.Ref.Size <= 0 {
		return fmt.Errorf("persistent state file size is invalid")
	}
	if file.Ref.Offset < 0 {
		return fmt.Errorf("persistent state file offset is invalid")
	}
	if file.Ref.Offset != 0 {
		return fmt.Errorf("persistent state file offset must be zero")
	}
	if err := s.validatePersistentStateArtifactPath(file.Block, file.MasterchainBlock, file.EffectiveShard, file.Ref.Path); err != nil {
		return err
	}

	stored := *file
	stored.Ref = file.Ref.Clone()
	stored.Ref.Path = ""

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
	_, err := s.persistentStateFileRecord(ctx, block, masterchainBlock, effectiveShard)
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

	path, err := s.persistentStateArtifactPath(block, masterchainBlock, effectiveShard)
	if err != nil {
		return err
	}
	if err = os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
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
	existing := meta.Clone()
	meta.Flags &^= storage.BlockMetaHasStateSnapshot
	meta.StateFileHash = nil
	if !blockMetaIndexedInHistory(meta) {
		if err := deleteBlockMetaHistoryIndexes(batch, existing); err != nil {
			return err
		}
	}
	if !blockMetaHasStoredMetadata(meta) {
		return batch.Delete(key, pebble.NoSync)
	}
	if err = batch.Set(key, encodeBlockMeta(meta), pebble.NoSync); err != nil {
		return err
	}
	return setBlockMetaHistoryIndexes(batch, existing, meta)
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

func (s *Store) BlockFullAvailable(ctx context.Context, block ton.BlockIDExt) error {
	meta, err := s.BlockMeta(ctx, block)
	if err != nil {
		return err
	}
	if !meta.Has(storage.BlockMetaHasServedFull) {
		return storage.ErrNotFound
	}
	if err = s.artifactAvailable(ctx, hotKeyBlockDataRef(block)); err != nil {
		return err
	}

	for _, kind := range storage.ProofCandidates(meta) {
		if !meta.HasProof(kind) {
			continue
		}
		err = s.artifactAvailable(ctx, hotKeyStoredProofRef(kind, block))
		if err == nil {
			return nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return err
		}
	}
	return storage.ErrNotFound
}

func (s *Store) NextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*storage.ServedBlockFull, error) {
	meta, err := s.BlockMeta(ctx, prev)
	if err != nil {
		return nil, err
	}
	if len(meta.NextRefs) == 0 {
		return nil, storage.ErrNotFound
	}
	return s.BlockFull(ctx, meta.NextRefs[0])
}

func (s *Store) BlockData(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	return s.readArtifact(ctx, hotKeyBlockDataRef(block))
}

func (s *Store) BlockProof(ctx context.Context, kind storage.ServedProofKind, block ton.BlockIDExt) ([]byte, error) {
	return s.readArtifact(ctx, hotKeyStoredProofRef(kind, block))
}

func (s *Store) ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	raw, err := s.getHotCopy(ctx, hotKeyZeroStateRef(block))
	if err != nil {
		return nil, err
	}
	size, err := decodeStateFileRef(raw)
	if err != nil {
		return nil, err
	}
	path, err := s.zeroStateArtifactPath(block)
	if err != nil {
		return nil, err
	}
	return s.readArtifactFileRange(ctx, path, 0, size, size)
}

func (s *Store) StoredZeroStateBlocks(ctx context.Context) ([]ton.BlockIDExt, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return nil, err
	}
	defer s.releaseHotDB()

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: bytes.Clone(hotPrefixZeroStateRef),
		UpperBound: prefixUpperBound(hotPrefixZeroStateRef),
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
	path, err := s.persistentStateArtifactPath(block, masterchainBlock, effectiveShard)
	if err != nil {
		return 0, err
	}
	stat, err := os.Stat(path)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return 0, storage.ErrNotFound
		}
		return 0, err
	}
	if stat.Size() < record.size {
		return 0, storage.ErrNotFound
	}
	return record.size, nil
}

func (s *Store) PersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (*storage.PersistentStateFile, error) {
	record, err := s.persistentStateFileRecord(ctx, block, masterchainBlock, effectiveShard)
	if err != nil {
		return nil, err
	}
	path, err := s.persistentStateArtifactPath(block, masterchainBlock, effectiveShard)
	if err != nil {
		return nil, err
	}
	return &storage.PersistentStateFile{
		Block:            block,
		MasterchainBlock: masterchainBlock,
		EffectiveShard:   effectiveShard,
		Ref: &storage.ArtifactRef{
			Path: path,
			Size: record.size,
		},
		FileHash:      bytes.Clone(record.fileHash),
		StateRootHash: bytes.Clone(record.stateRootHash),
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
	if offset >= record.size {
		return nil, nil
	}
	size := record.size - offset
	if size > maxSize {
		size = maxSize
	}
	path, err := s.persistentStateArtifactPath(block, masterchainBlock, effectiveShard)
	if err != nil {
		return nil, err
	}
	return s.readArtifactFileRange(ctx, path, offset, size, record.size)
}

func (s *Store) persistentStateFileRecord(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (*persistentStateFileRecord, error) {
	raw, err := s.getHotCopy(ctx, hotKeyPersistentStateFile(block, masterchainBlock, effectiveShard))
	if err != nil {
		return nil, err
	}
	return decodePersistentStateFileRecord(raw)
}

func (s *Store) ArchiveInfo(ctx context.Context, masterchainSeqno int32, workchain int32, shard int64) (int64, error) {
	if masterchainSeqno < 0 {
		return 0, storage.ErrNotFound
	}

	baseSeqno, err := s.archivePackageBaseSeqno(uint32(masterchainSeqno), false)
	if err != nil {
		return 0, err
	}
	startSeqno := archiveSliceSeqno(baseSeqno, uint32(masterchainSeqno))

	raw, err := s.getHotCopy(ctx, hotKeyArchiveInfo(baseSeqno, startSeqno, workchain, shard))
	if errors.Is(err, storage.ErrNotFound) && workchain != -1 {
		raw, err = s.archiveInfoForSplitShardPrefix(ctx, baseSeqno, startSeqno, workchain, shard)
	}
	if err != nil {
		return 0, err
	}
	if len(raw) != 8 {
		return 0, fmt.Errorf("invalid archive info payload")
	}
	return int64(binary.BigEndian.Uint64(raw)), nil
}

func (s *Store) archiveInfoForSplitShardPrefix(ctx context.Context, baseSeqno uint32, startSeqno uint32, workchain int32, shard int64) ([]byte, error) {
	for depth := uint32(1); depth <= 60; depth++ {
		prefix := archivePackShardPrefix(workchain, shard, depth)
		if prefix == shard {
			continue
		}
		raw, err := s.getHotCopy(ctx, hotKeyArchiveInfo(baseSeqno, startSeqno, workchain, prefix))
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
	meta, err := s.archivePackageMetaByID(ctx, archiveID)
	if err != nil {
		return nil, err
	}
	if offset < 0 || maxSize < 0 {
		return nil, fmt.Errorf("invalid archive range offset=%d max_size=%d", offset, maxSize)
	}
	if maxSize == 0 {
		return []byte{}, nil
	}
	if offset >= meta.size {
		return nil, nil
	}
	size := meta.size - offset
	if size > int64(maxSize) {
		size = int64(maxSize)
	}
	return s.readArtifactRange(ctx, meta.path, offset, size, meta.size)
}

func (s *Store) readArtifactRef(ctx context.Context, ref *storage.ArtifactRef) ([]byte, error) {
	if ref == nil {
		return nil, storage.ErrNotFound
	}
	path, err := s.artifactRefPath(ctx, ref)
	if err != nil {
		return nil, err
	}
	return s.readArtifactRange(ctx, path, ref.Offset, ref.Size, ref.Offset+ref.Size)
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

func (s *Store) artifactRefPath(ctx context.Context, ref *storage.ArtifactRef) (string, error) {
	if ref.ArchivePackage {
		if ref.ArchivePackageID < 0 {
			return s.keyBlockProofPackPath(uint32(keyProofPackageIDFromRef(ref.ArchivePackageID))), nil
		}
		meta, err := s.archivePackageMetaByID(ctx, ref.ArchivePackageID)
		if err != nil {
			return "", err
		}
		if meta.path == "" {
			return "", storage.ErrNotFound
		}
		return meta.path, nil
	}
	return "", storage.ErrNotFound
}

func (s *Store) zeroStateArtifactPath(block ton.BlockIDExt) (string, error) {
	name, err := storage.ZeroStateFileName(block)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.StateFilesDir(), name), nil
}

func (s *Store) persistentStateArtifactPath(block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64) (string, error) {
	name, err := storage.PersistentStateFileName(block, master, effectiveShard)
	if err != nil {
		return "", err
	}
	return filepath.Join(s.StateFilesDir(), name), nil
}

func (s *Store) validateZeroStateArtifactPath(block ton.BlockIDExt, path string) error {
	expected, err := s.zeroStateArtifactPath(block)
	if err != nil {
		return err
	}
	return validateArtifactCanonicalPath(s.artifactPath(path), expected)
}

func (s *Store) validatePersistentStateArtifactPath(block ton.BlockIDExt, master ton.BlockIDExt, effectiveShard int64, path string) error {
	expected, err := s.persistentStateArtifactPath(block, master, effectiveShard)
	if err != nil {
		return err
	}
	return validateArtifactCanonicalPath(s.artifactPath(path), expected)
}

func validateArtifactCanonicalPath(path string, expected string) error {
	absPath, err := filepath.Abs(path)
	if err != nil {
		return err
	}
	absExpected, err := filepath.Abs(expected)
	if err != nil {
		return err
	}
	if absPath != absExpected {
		return fmt.Errorf("artifact path %s does not match canonical path %s", absPath, absExpected)
	}
	return nil
}

func (s *Store) artifactPath(path string) string {
	if filepath.IsAbs(path) {
		return path
	}
	return filepath.Join(s.dir, path)
}

func (s *Store) relativeArtifactPath(path string) (string, error) {
	root, err := filepath.Abs(s.dir)
	if err != nil {
		return "", err
	}
	absPath, err := filepath.Abs(path)
	if err != nil {
		return "", err
	}
	rel, err := filepath.Rel(root, absPath)
	if err != nil {
		return "", err
	}
	if rel == ".." || strings.HasPrefix(rel, ".."+string(os.PathSeparator)) || filepath.IsAbs(rel) {
		return "", fmt.Errorf("artifact path %s is outside store dir %s", absPath, root)
	}
	return rel, nil
}
