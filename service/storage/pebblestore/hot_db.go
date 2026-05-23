package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
)

type pebbleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
}

func (s *Store) acquireHotDB(ctx context.Context) (*pebble.DB, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
	}

	s.mu.RLock()
	defer s.mu.RUnlock()
	if s.closed || s.hotClosing.Load() || s.hot == nil {
		return nil, errPebbleClosed
	}
	s.hotRefs.Add(1)
	return s.hot, nil
}

func (s *Store) releaseHotDB() {
	if s.hotRefs.Add(-1) == 0 && s.hotClosing.Load() {
		s.signalHotDrained()
	}
}

func (s *Store) signalHotDrained() {
	s.hotDrainOnce.Do(func() {
		close(s.hotDrained)
	})
}

func (s *Store) ensureWritable() error {
	if s.readOnly {
		return pebble.ErrReadOnly
	}
	return nil
}

func (s *Store) withHotBatch(fn func(batch *pebble.Batch) error) error {
	if err := s.ensureWritable(); err != nil {
		return err
	}

	db, err := s.acquireHotDB(context.Background())
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := fn(batch); err != nil {
		return err
	}
	return batch.Commit(pebble.NoSync)
}

func (s *Store) setHotRecord(ctx context.Context, key, value []byte, writeOptions *pebble.WriteOptions) error {
	if err := s.ensureWritable(); err != nil {
		return err
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Set(key, value, pebble.NoSync); err != nil {
		return err
	}
	return batch.Commit(writeOptions)
}

func (s *Store) deleteHotRecord(ctx context.Context, key []byte, writeOptions *pebble.WriteOptions) error {
	if err := s.ensureWritable(); err != nil {
		return err
	}

	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return err
	}
	defer s.releaseHotDB()

	s.hotWriteMu.Lock()
	defer s.hotWriteMu.Unlock()
	batch := db.NewBatch()
	defer func() { _ = batch.Close() }()
	if err := batch.Delete(key, pebble.NoSync); err != nil {
		return err
	}
	return batch.Commit(writeOptions)
}

func (s *Store) getHotCopy(ctx context.Context, key []byte) ([]byte, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return nil, err
	}
	defer s.releaseHotDB()
	return pebbleReaderGetCopy(db, key, nil)
}

func (s *Store) setHotUnique(batch *pebble.Batch, key, value []byte) error {
	current, closer, err := pebbleReaderGet(s.hot, key)
	if err == nil {
		defer func() { _ = closer.Close() }()
		if !bytes.Equal(current, value) {
			return fmt.Errorf("hot unique record %x already has different value", key)
		}
		return nil
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return err
	}
	return batch.Set(key, value, pebble.NoSync)
}

func pebbleReaderGet(reader pebbleReader, key []byte) ([]byte, io.Closer, error) {
	value, closer, err := reader.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return nil, nil, storage.ErrNotFound
		}
		return nil, nil, err
	}
	return value, closer, nil
}

func pebbleReaderGetCopy(reader pebbleReader, key []byte, dst []byte) ([]byte, error) {
	value, closer, err := pebbleReaderGet(reader, key)
	if err != nil {
		return nil, err
	}
	defer func() { _ = closer.Close() }()
	return append(dst[:0], value...), nil
}

func pebbleReaderHas(reader pebbleReader, key []byte) (bool, error) {
	_, closer, err := reader.Get(key)
	if err != nil {
		if errors.Is(err, pebble.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	if closer != nil {
		_ = closer.Close()
	}
	return true, nil
}
