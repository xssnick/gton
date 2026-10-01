package pebblestore

import (
	"context"
	"errors"
	"fmt"
	"github.com/xssnick/gton/service/validator/collator"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/validator"
)

// Status returns session telemetry from one snapshot together with live
// Pebble pressure metrics.
func (s *ValidatorStore) Status(ctx context.Context) (validator.StorageStatus, error) {
	if err := ctx.Err(); err != nil {
		return validator.StorageStatus{}, err
	}
	if err := s.store.acquireRead(); err != nil {
		return validator.StorageStatus{}, err
	}
	defer s.store.releaseRead()

	snapshot := s.store.db.NewSnapshot()
	defer snapshot.Close()

	status := validator.StorageStatus{
		Sessions:      []validator.StoredSession{},
		PendingWrites: int(s.store.outstanding.Load()),
	}
	ids, err := enumerateSessionIDs(ctx, snapshot)
	if err != nil {
		return validator.StorageStatus{}, err
	}
	for _, id := range ids {
		namespace, namespaceErr := namespaceForSession(id)
		if namespaceErr != nil {
			return validator.StorageStatus{}, namespaceErr
		}
		session, sessionErr := loadStoredSessionStatus(ctx, snapshot, id, namespace)
		if sessionErr != nil {
			return validator.StorageStatus{}, sessionErr
		}
		status.Sessions = append(status.Sessions, session)
	}
	if err = ctx.Err(); err != nil {
		return validator.StorageStatus{}, err
	}

	status.DB = pebbleDBMetrics(s.store.db.Metrics())

	return status, nil
}

func loadStoredSessionStatus(
	ctx context.Context,
	reader pebbleReader,
	id validator.SessionStorageID,
	namespace storageNamespace,
) (validator.StoredSession, error) {
	value, err := readerGetCopy(ctx, reader, summaryKey(namespace))
	if errors.Is(err, pebble.ErrNotFound) {
		return validator.StoredSession{}, errSessionSummaryMissing
	}
	if err != nil {
		return validator.StoredSession{}, fmt.Errorf("validator pebblestore: read session summary: %w", err)
	}
	summary, err := decodeSessionSummary(value)
	if err != nil {
		return validator.StoredSession{}, err
	}

	return summary.storedSession(id), nil
}

// pebbleDBMetrics projects the database pressure both status reports carry.
func pebbleDBMetrics(metrics *pebble.Metrics) collator.DBMetrics {
	return collator.DBMetrics{
		DiskSize:              metrics.DiskSpaceUsage(),
		LiveSize:              metrics.Table.Local.LiveSize,
		ReadAmp:               int64(metrics.ReadAmp()),
		L0Files:               metrics.Levels[0].TablesCount,
		L0Sublevels:           int64(metrics.Levels[0].Sublevels),
		CompactionDebt:        metrics.Compact.EstimatedDebt,
		CompactionsInProgress: metrics.Compact.NumInProgress,
		MemTableSize:          metrics.MemTable.Size,
		WALSize:               metrics.WAL.Size,
	}
}
