package pebblestore

import (
	"context"

	"github.com/cockroachdb/pebble/v2"
)

// SaveFastSyncCertificateSnapshot atomically replaces the validated FastSync
// member certificates. Certificates are refreshed infrequently and must
// survive a process crash, so the write is synced before it is acknowledged.
func (s *Store) SaveFastSyncCertificateSnapshot(ctx context.Context, snapshot []byte) error {
	return s.setHotRecord(ctx, hotKeyFastSyncCertificates(), snapshot, pebble.Sync)
}

// FastSyncCertificateSnapshot returns storage.ErrNotFound when no
// certificate has been persisted.
func (s *Store) FastSyncCertificateSnapshot(ctx context.Context) ([]byte, error) {
	return s.getHotCopy(ctx, hotKeyFastSyncCertificates())
}

func (s *Store) DeleteFastSyncCertificateSnapshot(ctx context.Context) error {
	return s.deleteHotRecord(ctx, hotKeyFastSyncCertificates(), pebble.Sync)
}
