package pebblestore

import (
	"context"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
)

func setMessageTransactionIndexes(batch *pebble.Batch, entries []storage.MessageTransactionIndexEntry) error {
	for _, entry := range entries {
		key := hotKeyMessageTransaction(entry.Kind, entry.Key)
		value := encodeMessageTransactionRef(entry.Ref)
		if err := batch.Set(key, value, pebble.NoSync); err != nil {
			return err
		}
	}
	return nil
}

func (s *Store) LookupMessageTransaction(ctx context.Context, kind storage.MessageTransactionKind, key storage.MessageTransactionKey) (storage.MessageTransactionRef, error) {
	db, err := s.acquireHotDB(ctx)
	if err != nil {
		return storage.MessageTransactionRef{}, err
	}
	defer s.releaseHotDB()

	raw, closer, err := pebbleReaderGet(db, hotKeyMessageTransaction(kind, key))
	if err != nil {
		return storage.MessageTransactionRef{}, err
	}
	defer func() { _ = closer.Close() }()

	ref, err := decodeMessageTransactionRef(raw)
	if err != nil {
		return storage.MessageTransactionRef{}, err
	}
	if err = requireServedFullBlock(db, ref.Block); err != nil {
		return storage.MessageTransactionRef{}, err
	}
	return ref, nil
}
