package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/validator"
)

// Sessions returns durable validator/observer descriptors without scanning
// their candidate, certificate, or telemetry records.
func (s *ValidatorStore) Sessions(ctx context.Context) ([]validator.SessionStorageID, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.store.acquireRead(); err != nil {
		return nil, err
	}
	defer s.store.releaseRead()

	snapshot := s.store.db.NewSnapshot()
	defer snapshot.Close()

	return enumerateSessionIDs(ctx, snapshot)
}

func enumerateSessionIDs(
	ctx context.Context,
	reader pebbleReader,
) ([]validator.SessionStorageID, error) {
	prefix := []byte{keySession}
	ids := make([]validator.SessionStorageID, 0)
	err := iteratePrefix(ctx, reader, prefix, "enumerate sessions", func(key, value []byte) error {
		if len(key) != 1+len(storageNamespace{}) {
			return fmt.Errorf("validator pebblestore: session key length %d", len(key))
		}
		id, decodeErr := decodeSessionID(value)
		if decodeErr != nil {
			return decodeErr
		}
		namespace, namespaceErr := namespaceForSession(id)
		if namespaceErr != nil {
			return namespaceErr
		}
		if !bytes.Equal(key[1:], namespace[:]) {
			return errors.New("validator pebblestore: session namespace hash mismatch")
		}
		ids = append(ids, id)

		return nil
	})
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}

	return ids, nil
}
