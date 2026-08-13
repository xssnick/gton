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

	"github.com/xssnick/gton/service/storage"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/tonutils-go/ton"
)

const archivePruneMetadataBatchKeys = 10000

type archivePruneBlockDelete struct {
	block                ton.BlockIDExt
	meta                 *storage.BlockMeta
	retainPersistentMeta bool
	stateRootHash        []byte
}

type archivePrunePersistentStateRetention struct {
	stateRootHash   []byte
	unsplitFileHash []byte
}

type archivePruneIteratorReader interface {
	NewIter(*pebble.IterOptions) (*pebble.Iterator, error)
}

func (s *Store) PruneArchivePackages(ctx context.Context, cutoffUnix uint32, maxStartGroups int) (storage.ArchivePruneStats, error) {
	stats := storage.ArchivePruneStats{CutoffUnix: cutoffUnix}
	if cutoffUnix == 0 {
		return stats, nil
	}

	packages, err := s.archivePackageSnapshot(ctx)
	if err != nil {
		return stats, err
	}
	stats.ScannedPackages = len(packages)

	deleteBeforeSeqno, boundarySeqno := archivePackagePruneBoundary(packages, cutoffUnix, maxStartGroups)
	if deleteBeforeSeqno == 0 {
		stats.RetainedBoundarySeqno = boundarySeqno
		return stats, nil
	}
	stats.DeletedBeforeSeqno = deleteBeforeSeqno
	stats.RetainedBoundarySeqno = boundarySeqno

	deletedMeta, deletedKeys, err := s.deleteArchivedBlockMetadata(ctx, deleteBeforeSeqno, cutoffUnix)
	if err != nil {
		return stats, err
	}
	stats.DeletedBlockMeta = deletedMeta
	stats.DeletedMetadataKeys = deletedKeys

	s.artifactMu.Lock()
	paths, deletedPackages, deletedKeys, err := s.deleteArchivePackageRecords(ctx, packages, deleteBeforeSeqno)
	if err != nil {
		s.artifactMu.Unlock()
		return stats, err
	}
	stats.DeletedPackages = deletedPackages
	stats.DeletedMetadataKeys += deletedKeys

	deletedFiles, deletedBytes, err := s.removeArchivePackageFilesLocked(paths)
	s.artifactMu.Unlock()
	if err != nil {
		stats.DeletedPackageFiles = deletedFiles
		stats.DeletedPackageBytes = deletedBytes
		return stats, err
	}
	stats.DeletedPackageFiles = deletedFiles
	stats.DeletedPackageBytes = deletedBytes
	return stats, nil
}

func (s *Store) archivePackageSnapshot(ctx context.Context) ([]archivePackageMeta, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return nil, err
	}
	defer s.releaseHotDB()

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: hotKeyArchivePackagePrefix(),
		UpperBound: prefixUpperBound(hotPrefixArchivePackage),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var packages []archivePackageMeta
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		meta, err := decodeArchivePackageMeta(iter.Value())
		if err != nil {
			return nil, err
		}
		packages = append(packages, meta)
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}
	return packages, nil
}

func archivePackagePruneBoundary(packages []archivePackageMeta, cutoffUnix uint32, maxStartGroups int) (uint32, uint32) {
	starts := map[uint32]struct{}{}
	var boundary uint32
	for _, pkg := range packages {
		if pkg.firstMasterUTime == 0 || pkg.firstMasterUTime >= cutoffUnix {
			continue
		}
		starts[pkg.startSeq] = struct{}{}
		if pkg.startSeq > boundary {
			boundary = pkg.startSeq
		}
	}
	if len(starts) == 0 {
		return 0, 0
	}

	deleteStarts := make([]uint32, 0, len(starts))
	for start := range starts {
		if start < boundary {
			deleteStarts = append(deleteStarts, start)
		}
	}
	if len(deleteStarts) == 0 {
		return 0, boundary
	}
	sort.Slice(deleteStarts, func(i, j int) bool {
		return deleteStarts[i] < deleteStarts[j]
	})
	if maxStartGroups > 0 && len(deleteStarts) > maxStartGroups {
		deleteStarts = deleteStarts[:maxStartGroups]
	}

	last := deleteStarts[len(deleteStarts)-1]
	if last > ^uint32(0)-archiveSliceMasterchainBlocks {
		return ^uint32(0), boundary
	}
	return last + archiveSliceMasterchainBlocks, boundary
}

func (s *Store) deleteArchivedBlockMetadata(ctx context.Context, beforeSeqno uint32, cutoffUnix uint32) (int, int, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return 0, 0, err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	snap := db.NewSnapshot()
	defer func() { _ = snap.Close() }()
	// Snapshot flags also describe cell checkpoints. Actual persistent-state
	// file records are the ownership source for metadata retained past archive TTL.
	retainedPersistentStates, err := archivePrunePersistentStateRetentions(ctx, snap, nil)
	if err != nil {
		return 0, 0, err
	}

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: bytes.Clone(hotPrefixBlockMeta),
		UpperBound: prefixUpperBound(hotPrefixBlockMeta),
	})
	if err != nil {
		return 0, 0, err
	}
	defer func() { _ = iter.Close() }()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	var pending []archivePruneBlockDelete
	deletedMeta := 0
	deletedKeys := 0
	flush := func() error {
		if len(pending) == 0 {
			return nil
		}
		keys, err := s.deleteArchivedBlockMetadataBatch(batch, pending)
		if err != nil {
			return err
		}
		if err = batch.Commit(pebble.NoSync); err != nil {
			return err
		}
		if err = batch.Close(); err != nil {
			return err
		}
		batch = db.NewBatch()
		deletedMeta += len(pending)
		deletedKeys += keys
		pending = pending[:0]
		return nil
	}

	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return deletedMeta, deletedKeys, ctx.Err()
		default:
		}

		key := iter.Key()
		if len(key) != len(hotPrefixBlockMeta)+80 {
			return deletedMeta, deletedKeys, fmt.Errorf("invalid block meta key size %d", len(key))
		}
		block, err := decodeBlockID(key[len(hotPrefixBlockMeta):])
		if err != nil {
			return deletedMeta, deletedKeys, err
		}
		meta, err := decodeBlockMeta(block, iter.Value())
		if err != nil {
			return deletedMeta, deletedKeys, err
		}
		retained, retainPersistentMeta := retainedPersistentStates[storage.BlockKey(block)]
		if isArchivePrunedSnapshotMeta(meta) {
			if retainPersistentMeta && bytes.Equal(meta.StateRootHash, retained.stateRootHash) {
				continue
			}
		} else if !archivePruneDeleteBlockMeta(block, meta, beforeSeqno, cutoffUnix) {
			continue
		}
		pending = append(pending, archivePruneBlockDelete{
			block:                block,
			meta:                 meta,
			retainPersistentMeta: retainPersistentMeta,
			stateRootHash:        retained.stateRootHash,
		})
		if len(pending) >= archivePruneMetadataBatchKeys {
			if err = flush(); err != nil {
				return deletedMeta, deletedKeys, err
			}
		}
	}
	if err = iter.Error(); err != nil {
		return deletedMeta, deletedKeys, err
	}
	if err = flush(); err != nil {
		return deletedMeta, deletedKeys, err
	}
	return deletedMeta, deletedKeys, nil
}

func archivePrunePersistentStateRetentions(ctx context.Context, reader archivePruneIteratorReader, excludedKeys map[string]struct{}) (map[storage.BlockRootHash]archivePrunePersistentStateRetention, error) {
	iter, err := reader.NewIter(&pebble.IterOptions{
		LowerBound: bytes.Clone(hotPrefixStateFileRef),
		UpperBound: prefixUpperBound(hotPrefixStateFileRef),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	retained := map[storage.BlockRootHash]archivePrunePersistentStateRetention{}
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}
		if _, excluded := excludedKeys[string(iter.Key())]; excluded {
			continue
		}

		block, master, effectiveShard, err := decodePersistentStateFileKey(iter.Key())
		if err != nil {
			return nil, err
		}
		record, err := decodePersistentStateFileRecord(iter.Value())
		if err != nil {
			return nil, err
		}
		if err = validateFixedHash("persistent state root hash", record.stateRootHash); err != nil {
			return nil, fmt.Errorf("decode persistent state %s retention: %w", storage.FormatBlockRef(block), err)
		}
		var unsplitFileHash []byte
		if effectiveShard == 0 {
			if err = validateFixedHash("persistent state file hash", record.fileHash); err != nil {
				return nil, fmt.Errorf("decode persistent state %s retention: %w", storage.FormatBlockRef(block), err)
			}
			unsplitFileHash = record.fileHash
		}
		if err = addArchivePrunePersistentStateRetention(retained, block, record.stateRootHash, unsplitFileHash); err != nil {
			return nil, err
		}
		if err = addArchivePrunePersistentStateRetention(retained, master, nil, nil); err != nil {
			return nil, err
		}
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}
	return retained, nil
}

func addArchivePrunePersistentStateRetention(
	retained map[storage.BlockRootHash]archivePrunePersistentStateRetention,
	block ton.BlockIDExt,
	stateRootHash []byte,
	unsplitFileHash []byte,
) error {
	key := storage.BlockKey(block)
	existing := retained[key]
	if len(existing.stateRootHash) == 0 {
		existing.stateRootHash = bytes.Clone(stateRootHash)
	} else if len(stateRootHash) != 0 && !bytes.Equal(existing.stateRootHash, stateRootHash) {
		return fmt.Errorf("persistent state %s has conflicting state root hashes", storage.FormatBlockRef(block))
	}
	if len(existing.unsplitFileHash) == 0 {
		existing.unsplitFileHash = bytes.Clone(unsplitFileHash)
	}
	retained[key] = existing
	return nil
}

func (s *Store) deleteArchivedBlockMetadataBatch(batch *pebble.Batch, blocks []archivePruneBlockDelete) (int, error) {
	deleted := 0
	for _, item := range blocks {
		block := item.block
		meta := item.meta
		ref := storage.BlockSeqRefFromBlock(block)
		keys := [][]byte{
			hotKeyBlockSeqIndex(ref),
			hotKeyBlockDataRef(block),
			hotKeyProofRef(storage.ServedProofBlock, block),
			hotKeyProofRef(storage.ServedProofBlockLink, block),
		}
		if item.retainPersistentMeta {
			retained := archivePrunedSnapshotMeta(block, meta, item.stateRootHash)
			if err := batch.Set(hotKeyBlockMeta(block), encodeBlockMeta(retained), pebble.NoSync); err != nil {
				return deleted, err
			}
		} else {
			keys = append(keys, hotKeyBlockMeta(block))
		}
		if meta.EndLT != 0 {
			keys = append(keys, hotKeyBlockLTIndex(ref, meta.EndLT))
		}
		if meta.GenUTime != 0 {
			keys = append(keys, hotKeyBlockUTimeIndex(ref, meta.GenUTime))
		}
		if isMasterchainBlock(block) && meta.Has(storage.BlockMetaIsKeyBlock) {
			keys = append(keys, hotKeyKeyBlockSeqIndex(block.SeqNo))
		}
		for _, key := range keys {
			if err := batch.Delete(key, pebble.NoSync); err != nil {
				return deleted, err
			}
			deleted++
		}
	}
	return deleted, nil
}

func archivePrunedSnapshotMeta(block ton.BlockIDExt, meta *storage.BlockMeta, stateRootHash []byte) *storage.BlockMeta {
	// Persistent-state file records own the file hash and payload. Archive GC only
	// keeps the timestamp and state root required by persistent-state consumers,
	// without retaining archive payload flags, references, or history indexes.
	return &storage.BlockMeta{
		ID:            block,
		Flags:         storage.BlockMetaHasStateSnapshot,
		GenUTime:      meta.GenUTime,
		StateRootHash: bytes.Clone(stateRootHash),
	}
}

func isArchivePrunedSnapshotMeta(meta *storage.BlockMeta) bool {
	return meta.Flags == storage.BlockMetaHasStateSnapshot &&
		meta.StartLT == 0 &&
		meta.EndLT == 0 &&
		len(meta.StateFileHash) == 0 &&
		meta.MasterchainRefSeqno == 0 &&
		len(meta.PrevRefs) == 0 &&
		len(meta.NextRefs) == 0
}

func archivePruneDeleteBlockMeta(block ton.BlockIDExt, meta *storage.BlockMeta, beforeSeqno uint32, cutoffUnix uint32) bool {
	if block.Workchain == -1 && block.Shard == topShard {
		return block.SeqNo < beforeSeqno
	}
	if meta != nil && meta.MasterchainRefKnown() {
		return meta.MasterchainRefSeqno < beforeSeqno
	}
	if meta != nil && meta.GenUTime != 0 && cutoffUnix != 0 {
		return meta.GenUTime < cutoffUnix
	}
	return false
}

func (s *Store) deleteArchivePackageRecords(ctx context.Context, packages []archivePackageMeta, beforeSeqno uint32) ([]string, int, int, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return nil, 0, 0, err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	deletePackage := map[int64]archivePackageMeta{}
	pathSet := map[string]struct{}{}
	for _, pkg := range packages {
		if pkg.startSeq >= beforeSeqno {
			continue
		}
		if pkg.path != "" {
			if _, ok := s.pendingArchiveSync[s.artifactPath(pkg.path)]; ok {
				continue
			}
			pathSet[pkg.path] = struct{}{}
		}
		deletePackage[pkg.archiveID] = pkg
	}
	if len(deletePackage) == 0 {
		return nil, 0, 0, nil
	}

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()

	deletedKeys := 0
	for _, pkg := range deletePackage {
		if err := batch.Delete(hotKeyArchivePackage(pkg.archiveID), pebble.NoSync); err != nil {
			return nil, 0, deletedKeys, err
		}
		deletedKeys++
	}
	for path := range pathSet {
		if err := s.setPackDeletePending(batch, path); err != nil {
			return nil, 0, deletedKeys, err
		}
	}

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: bytes.Clone(hotPrefixArchiveInfo),
		UpperBound: prefixUpperBound(hotPrefixArchiveInfo),
	})
	if err != nil {
		return nil, 0, deletedKeys, err
	}
	defer func() { _ = iter.Close() }()

	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return nil, 0, deletedKeys, ctx.Err()
		default:
		}
		key := iter.Key()
		if len(key) != len(hotPrefixArchiveInfo)+4+4+4+8 {
			return nil, 0, deletedKeys, fmt.Errorf("invalid archive info key size %d", len(key))
		}
		startSeqno := binary.BigEndian.Uint32(key[len(hotPrefixArchiveInfo)+4 : len(hotPrefixArchiveInfo)+8])
		if startSeqno >= beforeSeqno {
			continue
		}
		raw := iter.Value()
		if len(raw) != 8 {
			return nil, 0, deletedKeys, fmt.Errorf("invalid archive info payload")
		}
		archiveID := int64(binary.BigEndian.Uint64(raw))
		if _, ok := deletePackage[archiveID]; !ok {
			continue
		}
		if err := batch.Delete(bytes.Clone(key), pebble.NoSync); err != nil {
			return nil, 0, deletedKeys, err
		}
		deletedKeys++
	}
	if err = iter.Error(); err != nil {
		return nil, 0, deletedKeys, err
	}

	if err = batch.Commit(pebble.Sync); err != nil {
		return nil, 0, deletedKeys, err
	}
	s.deleteArchivePackageCache(deletePackage)

	paths := make([]string, 0, len(pathSet))
	for path := range pathSet {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	return paths, len(deletePackage), deletedKeys, nil
}

func (s *Store) removeArchivePackageFilesLocked(paths []string) (int, uint64, error) {
	if len(paths) == 0 {
		return 0, 0, nil
	}

	deleted, deletedBytes, err := s.removeArchivePackageFilesFromDiskLocked(paths)
	if err != nil {
		return deleted, deletedBytes, err
	}

	if err := s.clearPackDeletePending(paths); err != nil {
		return deleted, deletedBytes, err
	}
	return deleted, deletedBytes, nil
}

func (s *Store) removeArchivePackageFilesFromDiskLocked(paths []string) (int, uint64, error) {
	deleted := 0
	deletedBytes := uint64(0)
	dirs := map[string]struct{}{}
	for _, relPath := range paths {
		relPath, err := s.packJournalPath(relPath)
		if err != nil {
			return deleted, deletedBytes, err
		}

		path := s.artifactPath(relPath)
		delete(s.pendingArchiveSync, path)

		stat, err := os.Stat(path)
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return deleted, deletedBytes, fmt.Errorf("stat archive package %s: %w", path, err)
		}

		if err := os.Remove(path); err != nil {
			if errors.Is(err, os.ErrNotExist) {
				continue
			}
			return deleted, deletedBytes, fmt.Errorf("remove archive package %s: %w", path, err)
		}
		deleted++
		if stat.Size() > 0 {
			deletedBytes += uint64(stat.Size())
			s.observeArchivePackageBytes(-stat.Size())
		}
		dirs[filepath.Dir(path)] = struct{}{}
	}

	dirPaths := make([]string, 0, len(dirs))
	for dir := range dirs {
		dirPaths = append(dirPaths, dir)
	}
	sort.Strings(dirPaths)
	for _, dir := range dirPaths {
		if err := storage.SyncDir(dir); err != nil {
			return deleted, deletedBytes, fmt.Errorf("sync archive package dir %s: %w", dir, err)
		}
	}
	return deleted, deletedBytes, nil
}
