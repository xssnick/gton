package pebblestore

import (
	"context"
	"errors"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
)

// SaveOverlayPeerCache atomically replaces the persisted peer snapshot of one
// overlay. The payload is opaque to the storage layer; entries dropped from a
// snapshot disappear with the overwrite, so the record never grows. NoSync:
// losing the latest snapshot on a crash only costs one save interval.
func (s *Store) SaveOverlayPeerCache(overlayID []byte, snapshot []byte) error {
	return s.setHotRecord(context.Background(), hotKeyOverlayPeerCache(overlayID), snapshot, pebble.NoSync)
}

// LoadOverlayPeerCache returns the persisted peer snapshot of one overlay, or
// nil when none was saved yet.
func (s *Store) LoadOverlayPeerCache(overlayID []byte) ([]byte, error) {
	raw, err := s.getHotCopy(context.Background(), hotKeyOverlayPeerCache(overlayID))
	if errors.Is(err, pebble.ErrNotFound) || errors.Is(err, storage.ErrNotFound) {
		return nil, nil
	}
	return raw, err
}
