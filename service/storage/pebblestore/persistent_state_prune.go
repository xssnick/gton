package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/bits"
	"sort"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

type persistentStatePruneFile struct {
	block          ton.BlockIDExt
	master         ton.BlockIDExt
	effectiveShard int64
	size           int64
}

func (s *Store) PruneExpiredPersistentStateFiles(ctx context.Context, nowUnix uint64, keepRecentGroups int, maxFiles int) (storage.PersistentStatePruneStats, error) {
	stats := storage.PersistentStatePruneStats{NowUnix: nowUnix}

	files, err := s.persistentStateFileSnapshot(ctx)
	if err != nil {
		return stats, err
	}
	stats.ScannedFiles = len(files)
	if len(files) == 0 {
		return stats, nil
	}

	sortPersistentStatePruneFiles(files)
	retained := persistentStatePruneRetainedGroups(files, keepRecentGroups)
	pendingMigrationSeqno, pendingMigrationErr := s.pendingCellGenerationMigrationOriginSeqno()
	if pendingMigrationErr != nil && !errors.Is(pendingMigrationErr, storage.ErrNotFound) {
		return stats, pendingMigrationErr
	}
	hasPendingMigration := pendingMigrationErr == nil
	stats.RetainedRecentGroups = len(retained)
	stats.OldestRetainedMasterSeqno = oldestRetainedPersistentStateSeqno(retained)

	deletes := make([]persistentStatePruneFile, 0)
	for _, file := range files {
		select {
		case <-ctx.Done():
			return stats, ctx.Err()
		default:
		}

		if _, ok := retained[file.master.SeqNo]; ok {
			continue
		}
		if hasPendingMigration && file.master.SeqNo == pendingMigrationSeqno {
			continue
		}

		expired, err := s.persistentStateFileExpired(ctx, file.master, nowUnix)
		if err != nil {
			return stats, err
		}
		if !expired {
			continue
		}

		deletes = append(deletes, file)
		if maxFiles > 0 && len(deletes) >= maxFiles {
			break
		}
	}

	for _, file := range deletes {
		trackDeletedPersistentStateMasterSeqno(&stats, file.master.SeqNo)
	}
	if err = s.deletePersistentStatePruneFiles(ctx, &stats, deletes); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *Store) pendingCellGenerationMigrationOriginSeqno() (uint32, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.pendingCellMigration == nil {
		return 0, storage.ErrNotFound
	}
	return s.pendingCellMigration.origin.SeqNo, nil
}

func (s *Store) PrunePreviousPersistentStateFiles(ctx context.Context, beforeMasterSeqno uint32) (storage.PersistentStatePruneStats, error) {
	stats := storage.PersistentStatePruneStats{}
	if beforeMasterSeqno == 0 {
		return stats, nil
	}

	files, err := s.persistentStateFileSnapshot(ctx)
	if err != nil {
		return stats, err
	}
	stats.ScannedFiles = len(files)
	if len(files) == 0 {
		return stats, nil
	}

	sortPersistentStatePruneFiles(files)
	pendingMigrationSeqno, pendingMigrationErr := s.pendingCellGenerationMigrationOriginSeqno()
	if pendingMigrationErr != nil && !errors.Is(pendingMigrationErr, storage.ErrNotFound) {
		return stats, pendingMigrationErr
	}
	hasPendingMigration := pendingMigrationErr == nil
	var previousSeqno uint32
	for _, file := range files {
		if file.master.SeqNo >= beforeMasterSeqno {
			continue
		}
		if hasPendingMigration && file.master.SeqNo == pendingMigrationSeqno {
			continue
		}
		if file.master.SeqNo > previousSeqno {
			previousSeqno = file.master.SeqNo
		}
	}
	if previousSeqno == 0 {
		return stats, nil
	}
	stats.DeletedMasterSeqno = previousSeqno

	deletes := make([]persistentStatePruneFile, 0)
	for _, file := range files {
		if file.master.SeqNo == previousSeqno {
			deletes = append(deletes, file)
		}
	}
	if err = s.deletePersistentStatePruneFiles(ctx, &stats, deletes); err != nil {
		return stats, err
	}
	return stats, nil
}

func (s *Store) deletePersistentStatePruneFiles(ctx context.Context, stats *storage.PersistentStatePruneStats, files []persistentStatePruneFile) error {
	deleted := make([]persistentStateFileDelete, 0, len(files))
	for _, file := range files {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		entry, err := s.unlinkPersistentStateFile(ctx, file.block, file.master, file.effectiveShard)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return err
		}
		deleted = append(deleted, entry)
	}

	// All persistent-state artifacts share a directory in normal operation, so
	// a prune run needs one directory sync rather than one per removed file.
	// Metadata stays intact until every unlink is durable; a failure before that
	// point therefore remains retryable on the next prune pass.
	if err := syncPersistentStateDeleteDirs(deleted); err != nil {
		return err
	}
	if err := s.deletePersistentStateFileRecords(deleted); err != nil {
		return err
	}

	for _, entry := range deleted {
		stats.DeletedFileRecords++
		if entry.diskFileRemoved {
			stats.DeletedDiskFiles++
			stats.DeletedDiskBytes += entry.diskFileBytes
		}
	}
	return nil
}

func trackDeletedPersistentStateMasterSeqno(stats *storage.PersistentStatePruneStats, seqno uint32) {
	if stats.DeletedMasterSeqno == 0 || seqno > stats.DeletedMasterSeqno {
		stats.DeletedMasterSeqno = seqno
	}
}

func (s *Store) persistentStateFileSnapshot(ctx context.Context) ([]persistentStatePruneFile, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return nil, err
	}
	defer s.releaseHotDB()

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: bytes.Clone(hotPrefixStateFileRef),
		UpperBound: prefixUpperBound(hotPrefixStateFileRef),
	})
	if err != nil {
		return nil, err
	}
	defer func() { _ = iter.Close() }()

	var files []persistentStatePruneFile
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		default:
		}

		block, master, effectiveShard, err := decodePersistentStateFileKey(iter.Key())
		if err != nil {
			return nil, err
		}
		record, err := decodePersistentStateFileRecord(iter.Value())
		if err != nil {
			return nil, err
		}

		files = append(files, persistentStatePruneFile{
			block:          block,
			master:         master,
			effectiveShard: effectiveShard,
			size:           record.size,
		})
	}
	if err = iter.Error(); err != nil {
		return nil, err
	}
	return files, nil
}

func decodePersistentStateFileKey(key []byte) (ton.BlockIDExt, ton.BlockIDExt, int64, error) {
	if len(key) != len(hotPrefixStateFileRef)+80+80+8 {
		return ton.BlockIDExt{}, ton.BlockIDExt{}, 0, fmt.Errorf("invalid persistent state file key size %d", len(key))
	}

	pos := len(hotPrefixStateFileRef)
	block, err := decodeBlockID(key[pos : pos+80])
	if err != nil {
		return ton.BlockIDExt{}, ton.BlockIDExt{}, 0, err
	}

	pos += 80
	master, err := decodeBlockID(key[pos : pos+80])
	if err != nil {
		return ton.BlockIDExt{}, ton.BlockIDExt{}, 0, err
	}

	pos += 80
	effectiveShard := int64(binary.BigEndian.Uint64(key[pos : pos+8]))
	return block, master, effectiveShard, nil
}

func sortPersistentStatePruneFiles(files []persistentStatePruneFile) {
	sort.Slice(files, func(i, j int) bool {
		a := files[i]
		b := files[j]
		if a.master.SeqNo != b.master.SeqNo {
			return a.master.SeqNo < b.master.SeqNo
		}
		if a.block.Workchain != b.block.Workchain {
			return a.block.Workchain < b.block.Workchain
		}
		if a.block.Shard != b.block.Shard {
			return a.block.Shard < b.block.Shard
		}
		if a.block.SeqNo != b.block.SeqNo {
			return a.block.SeqNo < b.block.SeqNo
		}
		return a.effectiveShard < b.effectiveShard
	})
}

func persistentStatePruneRetainedGroups(files []persistentStatePruneFile, keepRecentGroups int) map[uint32]struct{} {
	if keepRecentGroups <= 0 || len(files) == 0 {
		return nil
	}

	groups := make([]uint32, 0)
	seen := map[uint32]struct{}{}
	for _, file := range files {
		seqno := file.master.SeqNo
		if _, ok := seen[seqno]; ok {
			continue
		}
		seen[seqno] = struct{}{}
		groups = append(groups, seqno)
	}
	sort.Slice(groups, func(i, j int) bool {
		return groups[i] < groups[j]
	})

	if len(groups) > keepRecentGroups {
		groups = groups[len(groups)-keepRecentGroups:]
	}

	retained := make(map[uint32]struct{}, len(groups))
	for _, seqno := range groups {
		retained[seqno] = struct{}{}
	}
	return retained
}

func oldestRetainedPersistentStateSeqno(retained map[uint32]struct{}) uint32 {
	var oldest uint32
	set := false
	for seqno := range retained {
		if !set || seqno < oldest {
			oldest = seqno
			set = true
		}
	}
	return oldest
}

func (s *Store) persistentStateFileExpired(ctx context.Context, master ton.BlockIDExt, nowUnix uint64) (bool, error) {
	meta, err := s.BlockMeta(ctx, master)
	if errors.Is(err, storage.ErrNotFound) {
		endTime, descErr := s.persistentStateDescriptionEndTime(ctx, master)
		if errors.Is(descErr, storage.ErrNotFound) {
			return true, nil
		}
		if descErr != nil {
			return false, fmt.Errorf("load persistent state description %s: %w", storage.FormatBlockRef(master), descErr)
		}

		// Archive GC in older releases could remove block metadata before the
		// persistent-state TTL. Automatic snapshots also persist the exact TTL
		// in their description, so preserve those files across an upgrade.
		return endTime < nowUnix, nil
	}
	if err != nil {
		return false, fmt.Errorf("load persistent state master block meta %s: %w", storage.FormatBlockRef(master), err)
	}
	if meta.GenUTime == 0 {
		return true, nil
	}
	return persistentStateFileTTL(meta.GenUTime) < nowUnix, nil
}

func (s *Store) persistentStateDescriptionEndTime(ctx context.Context, master ton.BlockIDExt) (uint64, error) {
	raw, err := s.getHotCopy(ctx, hotKeyPersistentStateDescription(master.SeqNo))
	if err != nil {
		return 0, err
	}
	desc, err := decodePersistentStateDescription(raw)
	if err != nil {
		return 0, err
	}
	if !desc.MasterchainBlock.Equals(&master) {
		return 0, storage.ErrNotFound
	}
	return desc.EndTime, nil
}

func persistentStateFileTTL(ts uint32) uint64 {
	x := uint64(ts) / (1 << 17)
	if x == 0 {
		return uint64(ts)
	}

	return uint64(ts) + ((uint64(1) << 18) << bits.TrailingZeros64(x))
}
