package service

import (
	"context"
	"time"
)

const (
	DefaultPersistentStateKeepRecent = 2
	PersistentStateKeepAll           = -1
	persistentStateGCInterval        = 30 * time.Minute
	persistentStateGCRetryDelay      = 5 * time.Minute
	persistentStateGCMaxFilesPerRun  = 256
)

func (s *Service) runPersistentStateGCOnce(ctx context.Context) (bool, error) {
	if s.persistentStateKeepRecent == PersistentStateKeepAll {
		s.log.Debug().Msg("skipping persistent state gc because all states are retained")
		return false, nil
	}

	if s.syncUntilFrozen() {
		s.log.Debug().
			Uint32("sync_until", s.syncUntil).
			Msg("skipping persistent state gc after sync_until")
		return false, nil
	}

	store := s.storage

	lease, err := s.beginExclusiveServiceTask(ctx, exclusiveServiceTaskPersistentStateGC)
	if err != nil {
		s.log.Debug().
			Err(err).
			Msg("skipping persistent state gc because exclusive service task cannot start")
		return false, nil
	}
	defer lease.release()

	started := time.Now()
	nowUnix := uint64(started.Unix())
	s.log.Info().
		Uint64("now_unix", nowUnix).
		Int("keep_recent_groups", s.persistentStateKeepRecent).
		Int("max_files", persistentStateGCMaxFilesPerRun).
		Msg("persistent state gc started")

	stats, err := store.PruneExpiredPersistentStateFiles(ctx, nowUnix, s.persistentStateKeepRecent, persistentStateGCMaxFilesPerRun)
	if err != nil {
		return false, err
	}
	elapsed := time.Since(started)

	s.log.Info().
		Uint64("now_unix", stats.NowUnix).
		Int("scanned_files", stats.ScannedFiles).
		Int("deleted_file_records", stats.DeletedFileRecords).
		Int("deleted_disk_files", stats.DeletedDiskFiles).
		Uint64("freed_bytes", stats.DeletedDiskBytes).
		Str("freed_size", formatByteSize(stats.DeletedDiskBytes)).
		Uint32("deleted_master_seqno", stats.DeletedMasterSeqno).
		Int("retained_recent_groups", stats.RetainedRecentGroups).
		Uint32("oldest_retained_master_seqno", stats.OldestRetainedMasterSeqno).
		Dur("elapsed", elapsed).
		Msg("persistent state gc completed")
	return stats.DeletedFileRecords != 0, nil
}
