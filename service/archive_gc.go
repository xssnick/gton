package service

import (
	"context"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const (
	archiveGCInitialDelay      = 2 * time.Minute
	archiveGCInterval          = 30 * time.Minute
	archiveGCRetryDelay        = 5 * time.Minute
	archiveGCStartGroupsPerRun = 256
)

type archivePruneStore interface {
	ActiveCellGeneration(ctx context.Context) (storage.CellGenerationInfo, error)
	BlockMeta(ctx context.Context, block ton.BlockIDExt) (*storage.BlockMeta, error)
	PruneArchivePackages(ctx context.Context, cutoffUnix uint32, maxStartGroups int) (storage.ArchivePruneStats, error)
}

func (s *Service) runArchiveGC(ctx context.Context) {
	timer := time.NewTimer(archiveGCInitialDelay)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-timer.C:
		}

		delay := archiveGCInterval
		if err := s.runArchiveGCOnce(ctx); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			s.log.Warn().
				Err(err).
				Dur("retry_delay", archiveGCRetryDelay).
				Msg("archive ttl gc failed")
			delay = archiveGCRetryDelay
		}
		timer.Reset(delay)
	}
}

func (s *Service) runArchiveGCOnce(ctx context.Context) error {
	if s.archiveTTL <= 0 {
		return nil
	}

	store, ok := s.storage.(archivePruneStore)
	if !ok {
		s.log.Debug().
			Str("storage", fmt.Sprintf("%T", s.storage)).
			Msg("storage does not support archive ttl gc")
		return nil
	}

	active, err := store.ActiveCellGeneration(ctx)
	if err != nil {
		return fmt.Errorf("load active cell generation: %w", err)
	}
	if emptyBlockID(active.OriginPersistentState) {
		return nil
	}

	originMeta, err := store.BlockMeta(ctx, active.OriginPersistentState)
	if err != nil {
		return fmt.Errorf("load active cell generation origin meta %s: %w", storage.FormatBlockRef(active.OriginPersistentState), err)
	}
	if originMeta.GenUTime == 0 {
		return nil
	}

	ttlSeconds := uint64(s.archiveTTL / time.Second)
	if ttlSeconds == 0 || ttlSeconds > math.MaxUint32 || uint64(originMeta.GenUTime) <= ttlSeconds {
		return nil
	}
	cutoffUnix := originMeta.GenUTime - uint32(ttlSeconds)

	lease, err := s.beginExclusiveServiceTask(ctx, exclusiveServiceTaskArchiveTTLGC)
	if err != nil {
		s.log.Debug().
			Err(err).
			Msg("skipping archive ttl gc because another exclusive service task is active")
		return nil
	}
	defer lease.release()

	stats, err := store.PruneArchivePackages(ctx, cutoffUnix, archiveGCStartGroupsPerRun)
	if err != nil {
		return err
	}
	if stats.DeletedPackages == 0 && stats.DeletedBlockMeta == 0 {
		return nil
	}

	s.log.Info().
		Uint32("cutoff_unix", stats.CutoffUnix).
		Uint32("deleted_before_seqno", stats.DeletedBeforeSeqno).
		Uint32("retained_boundary_seqno", stats.RetainedBoundarySeqno).
		Int("scanned_packages", stats.ScannedPackages).
		Int("deleted_packages", stats.DeletedPackages).
		Int("deleted_package_files", stats.DeletedPackageFiles).
		Int("deleted_block_meta", stats.DeletedBlockMeta).
		Int("deleted_metadata_keys", stats.DeletedMetadataKeys).
		Dur("archive_ttl", s.archiveTTL).
		Str("origin_persistent_state", storage.FormatBlockRef(active.OriginPersistentState)).
		Msg("archive ttl gc pruned old packages")
	return nil
}
