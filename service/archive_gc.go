package service

import (
	"context"
	"fmt"
	"math"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	archiveGCInterval          = 30 * time.Minute
	archiveGCRetryDelay        = 5 * time.Minute
	archiveGCStartGroupsPerRun = 1024
)

type archivePruneStore interface {
	ActiveCellGeneration(ctx context.Context) (storage.CellGenerationInfo, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	PruneArchivePackages(ctx context.Context, cutoffUnix uint32, maxStartGroups int) (storage.ArchivePruneStats, error)
}

func (s *Service) runArchiveGCOnce(ctx context.Context) (bool, error) {
	if s.archiveTTL <= 0 {
		return false, nil
	}

	store, ok := s.storage.(archivePruneStore)
	if !ok {
		s.log.Debug().
			Str("storage", fmt.Sprintf("%T", s.storage)).
			Msg("storage does not support archive ttl gc")
		return false, nil
	}

	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		return false, fmt.Errorf("load active cell generation: %w", err)
	}
	if emptyBlockID(active.OriginPersistentState) {
		return false, nil
	}

	originMeta, err := store.BlockMeta(ctx, active.OriginPersistentState)
	if err != nil {
		return false, fmt.Errorf("load active cell generation origin meta %s: %w", storage.FormatBlockRef(active.OriginPersistentState), err)
	}
	if originMeta.GenUTime == 0 {
		return false, nil
	}

	ttlSeconds := uint64(s.archiveTTL / time.Second)
	if ttlSeconds == 0 || ttlSeconds > math.MaxUint32 || uint64(originMeta.GenUTime) <= ttlSeconds {
		return false, nil
	}
	cutoffUnix := originMeta.GenUTime - uint32(ttlSeconds)

	lease, err := s.beginExclusiveServiceTask(ctx, exclusiveServiceTaskArchiveTTLGC)
	if err != nil {
		s.log.Debug().
			Err(err).
			Msg("skipping archive ttl gc because another exclusive service task is active")
		return false, nil
	}
	defer lease.release()

	started := time.Now()
	s.log.Info().
		Uint32("cutoff_unix", cutoffUnix).
		Dur("archive_ttl", s.archiveTTL).
		Str("origin_persistent_state", storage.FormatBlockRef(active.OriginPersistentState)).
		Msg("archive ttl gc started")

	stats, err := store.PruneArchivePackages(ctx, cutoffUnix, archiveGCStartGroupsPerRun)
	if err != nil {
		return false, err
	}
	elapsed := time.Since(started)
	pruned := stats.DeletedPackages != 0 || stats.DeletedBlockMeta != 0

	s.log.Info().
		Uint32("cutoff_unix", stats.CutoffUnix).
		Uint32("deleted_before_seqno", stats.DeletedBeforeSeqno).
		Uint32("retained_boundary_seqno", stats.RetainedBoundarySeqno).
		Int("scanned_packages", stats.ScannedPackages).
		Int("deleted_packages", stats.DeletedPackages).
		Int("deleted_package_files", stats.DeletedPackageFiles).
		Uint64("freed_bytes", stats.DeletedPackageBytes).
		Str("freed_size", formatByteSize(stats.DeletedPackageBytes)).
		Int("deleted_block_meta", stats.DeletedBlockMeta).
		Int("deleted_metadata_keys", stats.DeletedMetadataKeys).
		Dur("archive_ttl", s.archiveTTL).
		Dur("elapsed", elapsed).
		Str("origin_persistent_state", storage.FormatBlockRef(active.OriginPersistentState)).
		Msg("archive ttl gc completed")
	return pruned, nil
}
