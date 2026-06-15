package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"os"
	"path/filepath"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
)

type packAppendDirtyRecord struct {
	path      string
	cleanSize int64
}

func encodePackAppendDirty(cleanSize int64) []byte {
	return binary.BigEndian.AppendUint64(nil, uint64(cleanSize))
}

func decodePackAppendDirty(data []byte) (int64, error) {
	if len(data) != 8 {
		return 0, fmt.Errorf("pack append dirty payload size mismatch")
	}
	cleanSize := int64(binary.BigEndian.Uint64(data))
	if cleanSize < 0 {
		return 0, fmt.Errorf("pack append dirty clean size is invalid")
	}
	return cleanSize, nil
}

func (s *Store) markPackAppendDirty(path string, cleanSize int64) (string, error) {
	if cleanSize < 0 {
		return "", fmt.Errorf("invalid clean pack size path=%s size=%d", path, cleanSize)
	}
	relPath, err := s.packJournalPath(path)
	if err != nil {
		return "", err
	}

	if err = s.withHotBatchOptions(pebble.Sync, func(batch *pebble.Batch) error {
		return batch.Set(hotKeyPackAppendDirty(relPath), encodePackAppendDirty(cleanSize), pebble.NoSync)
	}); err != nil {
		return "", err
	}
	return relPath, nil
}

func (s *Store) deletePackAppendDirty(batch *pebble.Batch, path string) error {
	relPath, err := s.packJournalPath(path)
	if err != nil {
		return err
	}
	return batch.Delete(hotKeyPackAppendDirty(relPath), pebble.NoSync)
}

func (s *Store) clearPackAppendDirty(path string) error {
	relPath, err := s.packJournalPath(path)
	if err != nil {
		return err
	}
	return s.withHotBatchOptions(pebble.Sync, func(batch *pebble.Batch) error {
		return batch.Delete(hotKeyPackAppendDirty(relPath), pebble.NoSync)
	})
}

func (s *Store) setPackDeletePending(batch *pebble.Batch, path string) error {
	relPath, err := s.packJournalPath(path)
	if err != nil {
		return err
	}
	return batch.Set(hotKeyPackDeletePending(relPath), nil, pebble.NoSync)
}

func (s *Store) clearPackDeletePending(paths []string) error {
	if len(paths) == 0 {
		return nil
	}
	return s.withHotBatchOptions(pebble.Sync, func(batch *pebble.Batch) error {
		for _, path := range paths {
			relPath, err := s.packJournalPath(path)
			if err != nil {
				return err
			}
			if err = batch.Delete(hotKeyPackDeletePending(relPath), pebble.NoSync); err != nil {
				return err
			}
		}
		return nil
	})
}

func (s *Store) packJournalPath(path string) (string, error) {
	relPath, ok := s.archivePackArtifactPath(path)
	if !ok {
		return "", fmt.Errorf("artifact path is not an archive pack: %s", path)
	}
	return relPath, nil
}

func decodePackJournalPath(prefix []byte, key []byte) (string, error) {
	if len(key) <= len(prefix) || !bytes.HasPrefix(key, prefix) {
		return "", fmt.Errorf("invalid pack journal key size %d", len(key))
	}
	path := string(key[len(prefix):])
	if path == "" {
		return "", fmt.Errorf("pack journal path is empty")
	}
	return path, nil
}

func (s *Store) recoverPackJournals(ctx context.Context) error {
	if err := s.recoverPackAppendDirty(ctx); err != nil {
		return err
	}
	return s.recoverPackDeletePending(ctx)
}

func (s *Store) recoverPackAppendDirty(ctx context.Context) error {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: hotKeyPackAppendDirtyPrefix(),
		UpperBound: appendPrefixUpperBound(hotPrefixPackAppendDirty),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	var records []packAppendDirtyRecord
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		relPath, err := decodePackJournalPath(hotPrefixPackAppendDirty, iter.Key())
		if err != nil {
			return err
		}
		cleanSize, err := decodePackAppendDirty(iter.Value())
		if err != nil {
			return err
		}
		records = append(records, packAppendDirtyRecord{path: relPath, cleanSize: cleanSize})
	}
	if err = iter.Error(); err != nil {
		return err
	}

	for _, record := range records {
		if err = s.truncateUncommittedPackTail(s.artifactPath(record.path), record.cleanSize); err != nil {
			return err
		}
	}
	if err = s.deleteRecoveredPackAppendDirty(db, records); err != nil {
		return err
	}
	if len(records) > 0 {
		s.log.Info().Int("packs", len(records)).Msg("recovered dirty pack appends")
	}
	return nil
}

func (s *Store) recoverPackDeletePending(ctx context.Context) error {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	iter, err := db.NewIter(&pebble.IterOptions{
		LowerBound: hotKeyPackDeletePendingPrefix(),
		UpperBound: appendPrefixUpperBound(hotPrefixPackDeletePending),
	})
	if err != nil {
		return err
	}
	defer func() { _ = iter.Close() }()

	var paths []string
	for ok := iter.First(); ok; ok = iter.Next() {
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		relPath, err := decodePackJournalPath(hotPrefixPackDeletePending, iter.Key())
		if err != nil {
			return err
		}
		paths = append(paths, relPath)
	}
	if err = iter.Error(); err != nil {
		return err
	}

	for _, path := range paths {
		if err = removePackFile(s.artifactPath(path)); err != nil {
			return err
		}
	}
	if err = s.deleteRecoveredPackDeletePending(db, paths); err != nil {
		return err
	}
	if len(paths) > 0 {
		s.log.Info().Int("packs", len(paths)).Msg("recovered pending pack deletes")
	}
	return nil
}

func removePackFile(path string) error {
	if err := os.Remove(path); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("remove archive package %s: %w", path, err)
	}
	if err := syncDir(filepath.Dir(path)); err != nil && !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("sync archive package dir %s: %w", filepath.Dir(path), err)
	}
	return nil
}

func (s *Store) deleteRecoveredPackAppendDirty(db *pebble.DB, records []packAppendDirtyRecord) error {
	if len(records) == 0 {
		return nil
	}

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	for _, record := range records {
		if err := batch.Delete(hotKeyPackAppendDirty(record.path), pebble.NoSync); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) deleteRecoveredPackDeletePending(db *pebble.DB, paths []string) error {
	if len(paths) == 0 {
		return nil
	}

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()

	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	for _, path := range paths {
		if err := batch.Delete(hotKeyPackDeletePending(path), pebble.NoSync); err != nil {
			return err
		}
	}
	return batch.Commit(pebble.Sync)
}

func (s *Store) recoverDirtyPackPath(path string) error {
	relPath, err := s.packJournalPath(path)
	if err != nil {
		return err
	}
	raw, err := s.getHotCopy(context.Background(), hotKeyPackAppendDirty(relPath))
	if errors.Is(err, storage.ErrNotFound) {
		return nil
	}
	if err != nil {
		return err
	}
	cleanSize, err := decodePackAppendDirty(raw)
	if err != nil {
		return err
	}
	if err = s.truncateUncommittedPackTail(path, cleanSize); err != nil {
		return err
	}
	return s.clearPackAppendDirty(path)
}
