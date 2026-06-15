package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

var errStateSerializationLowDiskSpace = errors.New("free disk space is below persistent state serialization minimum")

func (s *Service) ensurePersistentStateSerializationDiskSpace(ctx context.Context, target ton.BlockIDExt) error {
	path := strings.TrimSpace(s.syncDiskSpacePath)
	if path == "" || s.minStateSerializationDiskFreeBytes == 0 {
		return nil
	}

	status, err := s.checkPersistentStateSerializationDiskSpace(path)
	if err != nil {
		return err
	}
	if status.AvailableBytes >= s.minStateSerializationDiskFreeBytes {
		return nil
	}

	s.stateSerializer.log.Warn().
		Err(errStateSerializationLowDiskSpace).
		Str("target", storage.FormatBlockRef(target)).
		Str("path", path).
		Uint64("available_bytes", status.AvailableBytes).
		Str("available_size", formatByteSize(status.AvailableBytes)).
		Uint64("required_bytes", s.minStateSerializationDiskFreeBytes).
		Str("required_size", formatByteSize(s.minStateSerializationDiskFreeBytes)).
		Msg("not enough free disk space before persistent state serialization, pruning previous state")

	stats, err := s.prunePreviousPersistentStateBeforeSerialization(ctx, target)
	if err != nil {
		return err
	}
	if stats.DeletedFileRecords > 0 {
		s.stateSerializer.log.Info().
			Str("target", storage.FormatBlockRef(target)).
			Uint32("deleted_master_seqno", stats.DeletedMasterSeqno).
			Int("deleted_file_records", stats.DeletedFileRecords).
			Int("deleted_disk_files", stats.DeletedDiskFiles).
			Uint64("freed_bytes", stats.DeletedDiskBytes).
			Str("freed_size", formatByteSize(stats.DeletedDiskBytes)).
			Msg("deleted previous persistent state before serialization")
	}

	status, err = s.checkPersistentStateSerializationDiskSpace(path)
	if err != nil {
		return err
	}
	if status.AvailableBytes >= s.minStateSerializationDiskFreeBytes {
		return nil
	}

	s.stateSerializer.log.Error().
		Err(errStateSerializationLowDiskSpace).
		Str("target", storage.FormatBlockRef(target)).
		Str("path", path).
		Uint64("available_bytes", status.AvailableBytes).
		Str("available_size", formatByteSize(status.AvailableBytes)).
		Uint64("required_bytes", s.minStateSerializationDiskFreeBytes).
		Str("required_size", formatByteSize(s.minStateSerializationDiskFreeBytes)).
		Int("deleted_file_records", stats.DeletedFileRecords).
		Uint64("freed_bytes", stats.DeletedDiskBytes).
		Str("freed_size", formatByteSize(stats.DeletedDiskBytes)).
		Msg("persistent state serialization skipped because disk space is still low")
	return fmt.Errorf("%w: available=%s required=%s path=%s", errStateSerializationLowDiskSpace, formatByteSize(status.AvailableBytes), formatByteSize(s.minStateSerializationDiskFreeBytes), path)
}

func (s *Service) checkPersistentStateSerializationDiskSpace(path string) (syncDiskSpaceStatus, error) {
	probe := s.syncDiskSpaceProbe
	if probe == nil {
		probe = statFSSyncDiskSpace
	}

	status, err := probe(path)
	if err != nil {
		return syncDiskSpaceStatus{}, fmt.Errorf("check free disk space before persistent state serialization: %w", err)
	}
	return status, nil
}

func (s *Service) prunePreviousPersistentStateBeforeSerialization(ctx context.Context, target ton.BlockIDExt) (storage.PersistentStatePruneStats, error) {
	stats, err := s.storage.PrunePreviousPersistentStateFiles(ctx, target.SeqNo)
	if err != nil {
		return stats, fmt.Errorf("delete previous persistent state before serialization %s: %w", storage.FormatBlockRef(target), err)
	}
	return stats, nil
}
