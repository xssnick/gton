package pebblestore

import (
	"context"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
)

func (s *Store) ArchiveBackfillProgress(ctx context.Context) (storage.ArchiveBackfillProgress, error) {
	raw, err := s.getHotCopy(ctx, hotKeyArchiveBackfillProgress())
	if err != nil {
		return storage.ArchiveBackfillProgress{}, err
	}
	return decodeArchiveBackfillProgress(raw)
}

func (s *Store) SaveArchiveBackfillProgress(ctx context.Context, progress storage.ArchiveBackfillProgress) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	return s.withHotBatch(func(batch *pebble.Batch) error {
		return batch.Set(hotKeyArchiveBackfillProgress(), encodeArchiveBackfillProgress(progress), pebble.NoSync)
	})
}
