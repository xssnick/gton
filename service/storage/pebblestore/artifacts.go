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

// prepareCheckpointArtifactWrites resolves the artifact writes for checkpoint
// entries: results streamed ahead by PrewriteCheckpointArtifacts are consumed
// as-is, everything else is appended to the pack files inline. Callers must
// hold artifactPublishMu.
func (s *Store) prepareCheckpointArtifactWrites(entries []storage.StateCheckpointBlock) ([]checkpointArtifactWrite, []archivePackRegistration, []storage.ServedBlockLink, error) {
	writes := make([]checkpointArtifactWrite, 0, len(entries))
	var links []storage.ServedBlockLink
	var missEntries []storage.StateCheckpointBlock
	var missIndexes []int
	for _, entry := range entries {
		if entry.Artifact == nil {
			continue
		}
		links = append(links, entry.Links...)
		write, err := s.takePrewrittenArtifactWrite(entry)
		if err == nil {
			writes = append(writes, write)
			continue
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, nil, nil, err
		}
		missIndexes = append(missIndexes, len(writes))
		writes = append(writes, checkpointArtifactWrite{})
		missEntries = append(missEntries, entry)
	}

	var missRegistrations []archivePackRegistration
	if len(missEntries) > 0 {
		missWrites, built, err := s.buildCheckpointArtifactWrites(missEntries)
		if err != nil {
			return nil, nil, nil, err
		}
		for i := range missWrites {
			writes[missIndexes[i]] = missWrites[i]
		}
		missRegistrations = built
	}
	// Drain only after the miss-path build: the build seeds its pack id claims
	// from the staged registrations, and draining first would let it hand their
	// indexes out again.
	registrations := append(s.drainPrewrittenPackRegistrations(), missRegistrations...)
	return writes, registrations, links, nil
}

// buildCheckpointArtifactWrites appends the artifact payloads for entries into
// the archive pack files and returns the produced writes (one per entry with
// an artifact, in entry order) and pack registrations. Callers must hold
// artifactPublishMu.
func (s *Store) buildCheckpointArtifactWrites(entries []storage.StateCheckpointBlock) ([]checkpointArtifactWrite, []archivePackRegistration, error) {
	const noQueuedRef = -1

	writes := make([]checkpointArtifactWrite, 0, len(entries))
	registrations := make([]archivePackRegistration, 0, len(entries))
	appendRequests := make([]archiveAppendRequest, 0, len(entries)*2)
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
			return nil, nil, err
		}
		if entry.State != nil {
			if !entry.State.Block.Equals(&entry.Artifact.ID) {
				return nil, nil, fmt.Errorf(
					"checkpoint artifact %s state block mismatch %s",
					storage.FormatBlockRef(entry.Artifact.ID),
					storage.FormatBlockRef(entry.State.Block),
				)
			}
			stateMeta, err := storage.BuildBlockMetaFromState(*entry.State)
			if err != nil {
				return nil, nil, err
			}
			meta = storage.MergeBlockMeta(meta, stateMeta)
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
					return nil, nil, err
				}
				proofRefs[kind] = ref
				continue
			}
			proofRefIndexes[kind] = queueArchiveAppend(packEntryKindForProofKind(kind), entry.Artifact.ID, meta, entry.Artifact.ArchiveShardSplitDepth, entry.Artifact.Proof)
		}

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
		return nil, nil, err
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

	return writes, registrations, nil
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
	if len(block.Block) == 0 && len(block.Proof) == 0 {
		return nil, nil, fmt.Errorf("served block %s has no block data or proof", storage.FormatBlockRef(block.ID))
	}
	if err := storage.ValidateBlockIDHashes(block.ID); err != nil {
		return nil, nil, err
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
			return fmt.Errorf(
				"proof ref invariant violated: kind=%q block=%s",
				kind,
				storage.FormatBlockRef(block.ID),
			)
		}
		if err := batch.Set(hotKeyStoredProofRef(kind, block.ID), encodeArtifactRef(ref), pebble.NoSync); err != nil {
			return err
		}
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
	if err := storage.ValidateBlockIDHashes(prev); err != nil {
		return err
	}
	if err := storage.ValidateBlockIDHashes(next); err != nil {
		return err
	}

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
		// Later links read metas before Pebble, so preserve the durable fields in
		// the staged view before adding a next-link delta.
		mergePendingBlockMeta(metas, meta)
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
		return fmt.Errorf("zerostate data is empty")
	}
	if err := storage.ValidateBlockIDHashes(block); err != nil {
		return err
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
	if err := storage.ValidateBlockIDHashes(file.Block); err != nil {
		return err
	}
	if err := storage.ValidateBlockIDHashes(file.MasterchainBlock); err != nil {
		return err
	}
	if err := validateFixedHash("persistent state file hash", file.FileHash); err != nil {
		return err
	}
	if err := validateFixedHash("persistent state root hash", file.StateRootHash); err != nil {
		return err
	}
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

	previousSize := int64(0)
	previous, err := s.persistentStateFileRecord(context.Background(), file.Block, file.MasterchainBlock, file.EffectiveShard)
	if err == nil {
		previousSize = previous.size
	} else if !errors.Is(err, storage.ErrNotFound) {
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

	if err := s.withHotBatch(func(batch *pebble.Batch) error {
		if err := batch.Set(key, record, pebble.NoSync); err != nil {
			return err
		}
		if stored.EffectiveShard == 0 {
			return s.setPersistentStateSnapshotMeta(batch, meta)
		}
		return nil
	}); err != nil {
		return err
	}

	s.observePersistentStateBytes(stored.Ref.Size - previousSize)
	return nil
}

func (s *Store) setPersistentStateSnapshotMeta(batch *pebble.Batch, meta *storage.BlockMeta) error {
	raw, closer, err := pebbleReaderGet(s.hot, hotKeyBlockMeta(meta.ID))
	if errors.Is(err, storage.ErrNotFound) {
		return s.setMergedBlockMeta(batch, meta)
	}
	if err != nil {
		return err
	}
	defer func() { _ = closer.Close() }()

	existing, err := decodeBlockMeta(meta.ID, raw)
	if err != nil {
		return err
	}
	if isArchivePrunedSnapshotMeta(existing) {
		if len(existing.StateRootHash) != 0 && !bytes.Equal(existing.StateRootHash, meta.StateRootHash) {
			return fmt.Errorf("persistent state root hash conflicts with archive-pruned block meta %s", storage.FormatBlockRef(meta.ID))
		}
		if len(existing.StateRootHash) == 0 {
			existing.StateRootHash = bytes.Clone(meta.StateRootHash)
			return batch.Set(hotKeyBlockMeta(meta.ID), encodeBlockMeta(existing), pebble.NoSync)
		}

		// The persistent-state record owns the file hash. Preserve the compact
		// marker so this update cannot recreate archive indexes.
		return nil
	}
	return s.setMergedBlockMeta(batch, meta)
}

func (s *Store) DeletePersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) error {
	deleted, err := s.unlinkPersistentStateFile(ctx, block, masterchainBlock, effectiveShard)
	if err != nil {
		return err
	}
	if err = syncPersistentStateDeleteDirs([]persistentStateFileDelete{deleted}); err != nil {
		return err
	}
	return s.deletePersistentStateFileRecords([]persistentStateFileDelete{deleted})
}

type persistentStateFileDelete struct {
	block            ton.BlockIDExt
	masterchainBlock ton.BlockIDExt
	effectiveShard   int64
	key              []byte
	dir              string
	diskFileRemoved  bool
	diskFileBytes    uint64
}

func (s *Store) unlinkPersistentStateFile(ctx context.Context, block ton.BlockIDExt, masterchainBlock ton.BlockIDExt, effectiveShard int64) (persistentStateFileDelete, error) {
	if err := s.ensureWritable(); err != nil {
		return persistentStateFileDelete{}, err
	}

	_, err := s.persistentStateFileRecord(ctx, block, masterchainBlock, effectiveShard)
	if err != nil {
		return persistentStateFileDelete{}, err
	}

	key := hotKeyPersistentStateFile(block, masterchainBlock, effectiveShard)
	path, err := s.persistentStateArtifactPath(block, masterchainBlock, effectiveShard)
	if err != nil {
		return persistentStateFileDelete{}, err
	}

	removedBytes := int64(0)
	diskFileExisted := false
	stat, err := os.Stat(path)
	if err == nil {
		diskFileExisted = true
		if stat.Size() > 0 {
			removedBytes = stat.Size()
		}
	} else if err != nil && !errors.Is(err, os.ErrNotExist) {
		return persistentStateFileDelete{}, err
	}

	removeErr := os.Remove(path)
	if removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		return persistentStateFileDelete{}, removeErr
	}
	if diskFileExisted {
		s.observePersistentStateBytes(-removedBytes)
	}
	return persistentStateFileDelete{
		block:            block,
		masterchainBlock: masterchainBlock,
		effectiveShard:   effectiveShard,
		key:              key,
		dir:              filepath.Dir(path),
		diskFileRemoved:  diskFileExisted,
		diskFileBytes:    uint64(removedBytes),
	}, nil
}

func syncPersistentStateDeleteDirs(files []persistentStateFileDelete) error {
	dirs := make(map[string]struct{}, len(files))
	for _, file := range files {
		dirs[file.dir] = struct{}{}
	}
	for dir := range dirs {
		if err := storage.SyncDir(dir); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
	}
	return nil
}

func (s *Store) deletePersistentStateFileRecords(files []persistentStateFileDelete) error {
	if len(files) == 0 {
		return nil
	}

	// Publish the metadata removal only after the directory entry is durable.
	// A crash may then leave a retryable record for an absent file, but never an
	// untracked persistent-state file that pruning cannot discover. NoSync is
	// sufficient because any later durability point is ordered after syncDir.
	return s.withHotBatch(func(batch *pebble.Batch) error {
		deletedKeys := make(map[string]struct{}, len(files))
		for _, file := range files {
			deletedKeys[string(file.key)] = struct{}{}
		}
		retained, err := archivePrunePersistentStateRetentions(context.Background(), s.hot, deletedKeys)
		if err != nil {
			return err
		}

		for _, file := range files {
			if err := batch.Delete(file.key, pebble.NoSync); err != nil {
				return err
			}
			if file.effectiveShard == 0 {
				retention, retainArchiveMarker := retained[storage.BlockKey(file.block)]
				if err := s.clearPersistentStateSnapshotMeta(batch, file.block, retention, retainArchiveMarker); err != nil {
					return err
				}
			}
		}

		checked := make(map[storage.BlockRootHash]struct{}, 2*len(files))
		for _, file := range files {
			for _, block := range []ton.BlockIDExt{file.block, file.masterchainBlock} {
				key := storage.BlockKey(block)
				if _, ok := checked[key]; ok {
					continue
				}
				checked[key] = struct{}{}
				if _, ok := retained[key]; ok {
					continue
				}
				if err = s.deleteArchivePrunedSnapshotMeta(batch, block); err != nil {
					return err
				}
			}
		}
		return nil
	})
}

func (s *Store) deleteArchivePrunedSnapshotMeta(batch *pebble.Batch, block ton.BlockIDExt) error {
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
	if !isArchivePrunedSnapshotMeta(meta) {
		return nil
	}
	return batch.Delete(key, pebble.NoSync)
}

func (s *Store) clearPersistentStateSnapshotMeta(
	batch *pebble.Batch,
	block ton.BlockIDExt,
	retention archivePrunePersistentStateRetention,
	retainArchiveMarker bool,
) error {
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
	if isArchivePrunedSnapshotMeta(meta) {
		if !retainArchiveMarker {
			return batch.Delete(key, pebble.NoSync)
		}
		if bytes.Equal(meta.StateRootHash, retention.stateRootHash) {
			return nil
		}
		meta.StateRootHash = bytes.Clone(retention.stateRootHash)
		return batch.Set(key, encodeBlockMeta(meta), pebble.NoSync)
	}
	existing := meta.Clone()
	if len(retention.unsplitFileHash) != 0 {
		meta.Mark(storage.BlockMetaHasStateSnapshot)
		meta.StateRootHash = bytes.Clone(retention.stateRootHash)
		meta.StateFileHash = bytes.Clone(retention.unsplitFileHash)
		if err = batch.Set(key, encodeBlockMeta(meta), pebble.NoSync); err != nil {
			return err
		}
		return setBlockMetaHistoryIndexes(batch, existing, meta)
	}
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

func (s *Store) ZeroStateSize(ctx context.Context, block ton.BlockIDExt) (int64, error) {
	raw, err := s.getHotCopy(ctx, hotKeyZeroStateRef(block))
	if err != nil {
		return 0, err
	}
	size, err := decodeStateFileRef(raw)
	if err != nil {
		return 0, err
	}
	if size <= 0 {
		return 0, fmt.Errorf("zerostate file ref size is invalid")
	}

	return size, nil
}

func (s *Store) ZeroState(ctx context.Context, block ton.BlockIDExt) ([]byte, error) {
	size, err := s.ZeroStateSize(ctx, block)
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
		prefix, err := archivePackShardPrefix(workchain, shard, depth)
		if err != nil {
			return nil, err
		}
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
