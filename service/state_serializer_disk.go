package service

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

var errStateSerializationLowDiskSpace = errors.New("free disk space is below persistent state serialization minimum")

func (s *StateLifecycle) ensurePersistentStateSerializationDiskSpace(ctx context.Context, target ton.BlockIDExt, probe syncDiskSpaceProbe) error {
	path := strings.TrimSpace(s.syncDiskSpacePath)
	if path == "" {
		return nil
	}

	status, err := checkPersistentStateSerializationDiskSpace(path, probe)
	if err != nil {
		return err
	}
	if status.AvailableBytes >= s.minStateSerializationDiskFreeBytes {
		return nil
	}
	if s.maintenance.persistentStateKeepRecent == PersistentStateKeepAll {
		s.stateSerializationLowDiskFields(s.stateSerializer.log.Error(), target, path, status.AvailableBytes).
			Msg("persistent state serialization skipped because all existing states are retained")
		return s.stateSerializationLowDiskError(path, status.AvailableBytes)
	}

	s.stateSerializationLowDiskFields(s.stateSerializer.log.Warn(), target, path, status.AvailableBytes).
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

	status, err = checkPersistentStateSerializationDiskSpace(path, probe)
	if err != nil {
		return err
	}
	if status.AvailableBytes >= s.minStateSerializationDiskFreeBytes {
		return nil
	}

	s.stateSerializationLowDiskFields(s.stateSerializer.log.Error(), target, path, status.AvailableBytes).
		Int("deleted_file_records", stats.DeletedFileRecords).
		Uint64("freed_bytes", stats.DeletedDiskBytes).
		Str("freed_size", formatByteSize(stats.DeletedDiskBytes)).
		Msg("persistent state serialization skipped because disk space is still low")
	return s.stateSerializationLowDiskError(path, status.AvailableBytes)
}

func (s *StateLifecycle) stateSerializationLowDiskFields(e *zerolog.Event, target ton.BlockIDExt, path string, availableBytes uint64) *zerolog.Event {
	return e.Err(errStateSerializationLowDiskSpace).
		Str("target", storage.FormatBlockRef(target)).
		Str("path", path).
		Uint64("available_bytes", availableBytes).
		Str("available_size", formatByteSize(availableBytes)).
		Uint64("required_bytes", s.minStateSerializationDiskFreeBytes).
		Str("required_size", formatByteSize(s.minStateSerializationDiskFreeBytes))
}

func (s *StateLifecycle) stateSerializationLowDiskError(path string, availableBytes uint64) error {
	return fmt.Errorf("%w: available=%s required=%s path=%s", errStateSerializationLowDiskSpace, formatByteSize(availableBytes), formatByteSize(s.minStateSerializationDiskFreeBytes), path)
}

func checkPersistentStateSerializationDiskSpace(path string, probe syncDiskSpaceProbe) (syncDiskSpaceStatus, error) {
	status, err := probe(path)
	if err != nil {
		return syncDiskSpaceStatus{}, fmt.Errorf("check free disk space before persistent state serialization: %w", err)
	}
	return status, nil
}

func (s *StateLifecycle) prunePreviousPersistentStateBeforeSerialization(ctx context.Context, target ton.BlockIDExt) (storage.PersistentStatePruneStats, error) {
	stats, err := s.stateSerializer.store.PrunePreviousPersistentStateFiles(ctx, target.SeqNo)
	if err != nil {
		return stats, fmt.Errorf("delete previous persistent state before serialization %s: %w", storage.FormatBlockRef(target), err)
	}
	return stats, nil
}
