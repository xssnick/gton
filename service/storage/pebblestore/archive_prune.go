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
	block ton.BlockIDExt
	meta  *storage.BlockMeta
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

	paths, deletedPackages, deletedKeys, err := s.deleteArchivePackageRecords(ctx, packages, deleteBeforeSeqno)
	if err != nil {
		return stats, err
	}
	stats.DeletedPackages = deletedPackages
	stats.DeletedMetadataKeys += deletedKeys

	deletedFiles, deletedBytes, err := s.removeArchivePackageFiles(paths)
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
		UpperBound: appendPrefixUpperBound(hotPrefixArchivePackage),
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

	iter, err := snap.NewIter(&pebble.IterOptions{
		LowerBound: bytes.Clone(hotPrefixBlockMeta),
		UpperBound: appendPrefixUpperBound(hotPrefixBlockMeta),
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
		if !archivePruneDeleteBlockMeta(block, meta, beforeSeqno, cutoffUnix) {
			continue
		}
		pending = append(pending, archivePruneBlockDelete{block: block, meta: meta})
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

func (s *Store) deleteArchivedBlockMetadataBatch(batch *pebble.Batch, blocks []archivePruneBlockDelete) (int, error) {
	deleted := 0
	for _, item := range blocks {
		block := item.block
		meta := item.meta
		keys := [][]byte{
			hotKeyBlockMeta(block),
			hotKeyBlockSeqIndex(storage.BlockHistoryKey{Workchain: block.Workchain, Shard: block.Shard}, block.SeqNo),
			hotKeyBlockDataRef(block),
			hotKeyProofRef(storage.ServedProofBlock, block),
			hotKeyProofRef(storage.ServedProofBlockLink, block),
			hotKeyKeyProofRef(storage.ServedProofKeyBlock, block),
			hotKeyKeyProofRef(storage.ServedProofKeyBlockLink, block),
		}
		if meta.EndLT != 0 {
			keys = append(keys, hotKeyBlockLTIndex(meta))
		}
		if meta.GenUTime != 0 {
			keys = append(keys, hotKeyBlockUTimeIndex(meta))
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

	deletePackage := map[int64]archivePackageMeta{}
	pathSet := map[string]struct{}{}
	for _, pkg := range packages {
		if pkg.startSeq >= beforeSeqno {
			continue
		}
		deletePackage[pkg.archiveID] = pkg
		if pkg.path != "" {
			pathSet[pkg.path] = struct{}{}
		}
	}
	if len(deletePackage) == 0 {
		return nil, 0, 0, nil
	}

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

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
		UpperBound: appendPrefixUpperBound(hotPrefixArchiveInfo),
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

func (s *Store) removeArchivePackageFiles(paths []string) (int, uint64, error) {
	if len(paths) == 0 {
		return 0, 0, nil
	}

	s.artifactMu.Lock()
	defer s.artifactMu.Unlock()

	deleted := 0
	deletedBytes := uint64(0)
	dirs := map[string]struct{}{}
	for _, relPath := range paths {
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
			return deleted, deletedBytes, fmt.Errorf("sync archive package dir %s: %w", dir, err)
		}
	}
	if err := s.clearPackDeletePending(paths); err != nil {
		return deleted, deletedBytes, err
	}
	return deleted, deletedBytes, nil
}
