package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/validator/keyring"
)

const (
	validatorKeyVersion   = byte(1)
	validatorKeyValueSize = 1 + 1 + 32 + 4 + 4 + 4 + 32 + 4
)

const (
	validatorKeyFlagPermanent byte = 1 << iota
	validatorKeyFlagADNL
	validatorKeyKnownFlags = validatorKeyFlagPermanent | validatorKeyFlagADNL
)

var _ keyring.Storage = (*ValidatorStore)(nil)

// LoadValidatorKeys returns all durable validator keys in identifier order.
func (s *ValidatorStore) LoadValidatorKeys(ctx context.Context) ([]keyring.StoredKey, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.store.acquireRead(); err != nil {
		return nil, err
	}
	defer s.store.releaseRead()

	snapshot := s.store.db.NewSnapshot()
	defer snapshot.Close()

	keys := []keyring.StoredKey{}
	prefix := validatorKeyPrefix()
	err := iteratePrefix(ctx, snapshot, prefix, "enumerate validator keys", func(key, value []byte) error {
		id, decodeErr := validatorKeyIDFromKey(key)
		if decodeErr != nil {
			return decodeErr
		}

		stored, decodeErr := decodeValidatorKey(id, value)
		if decodeErr != nil {
			return decodeErr
		}
		keys = append(keys, stored)

		return nil
	})
	if err != nil {
		for i := range keys {
			clear(keys[i].Seed[:])
		}

		return nil, err
	}

	return keys, nil
}

// SaveValidatorKey durably replaces one key record through the store's global
// writer. Once admitted, it waits for the commit even if ctx is cancelled so
// the caller's in-memory publication cannot diverge from Pebble.
func (s *ValidatorStore) SaveValidatorKey(ctx context.Context, key keyring.StoredKey) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	value, err := encodeValidatorKey(key)
	if err != nil {
		return err
	}
	result := make(chan error, 1)
	err = s.store.submitContext(ctx, writeRequest{
		sizeHint: len(value),
		apply: func(batch *pebble.Batch) error {
			storageKey := validatorKeyKey(key.ID)
			storedValue, readErr := getBatchCopy(batch, storageKey)
			if readErr == nil {
				stored, decodeErr := decodeValidatorKey(key.ID, storedValue)
				if decodeErr != nil {
					return rejectRequest(decodeErr)
				}
				if stored.Seed != key.Seed {
					return rejectRequest(errors.New(
						"validator pebblestore: validator key seed cannot change",
					))
				}
				if bytes.Equal(storedValue, value) {
					return nil
				}
			} else if !errors.Is(readErr, pebble.ErrNotFound) {
				return fmt.Errorf("validator pebblestore: read validator key: %w", readErr)
			}

			if err := batch.Set(storageKey, value, nil); err != nil {
				return fmt.Errorf("validator pebblestore: save validator key: %w", err)
			}

			return nil
		},
		done: func(err error) {
			result <- err
		},
	})
	if err != nil {
		return err
	}

	return <-result
}

func encodeValidatorKey(key keyring.StoredKey) ([]byte, error) {
	if err := validateValidatorKey(key); err != nil {
		return nil, err
	}

	flags := byte(0)
	if key.Permanent {
		flags |= validatorKeyFlagPermanent
	}
	if key.HasADNL {
		flags |= validatorKeyFlagADNL
	}

	value := make([]byte, validatorKeyValueSize)
	value[0] = validatorKeyVersion
	value[1] = flags
	copy(value[2:34], key.Seed[:])
	binary.LittleEndian.PutUint32(value[34:38], key.ElectionDate)
	binary.LittleEndian.PutUint32(value[38:42], key.PermanentExpireAt)
	binary.LittleEndian.PutUint32(value[42:46], key.TempExpireAt)
	copy(value[46:78], key.ADNLID[:])
	binary.LittleEndian.PutUint32(value[78:82], key.ADNLExpireAt)

	return value, nil
}

func decodeValidatorKey(id [32]byte, value []byte) (keyring.StoredKey, error) {
	if len(value) != validatorKeyValueSize {
		return keyring.StoredKey{}, fmt.Errorf(
			"validator pebblestore: validator key value length %d",
			len(value),
		)
	}
	if value[0] != validatorKeyVersion {
		return keyring.StoredKey{}, fmt.Errorf(
			"validator pebblestore: validator key version %d",
			value[0],
		)
	}
	flags := value[1]
	if flags&^validatorKeyKnownFlags != 0 {
		return keyring.StoredKey{}, fmt.Errorf(
			"validator pebblestore: validator key flags %#x",
			flags,
		)
	}

	key := keyring.StoredKey{
		ID:                id,
		Permanent:         flags&validatorKeyFlagPermanent != 0,
		ElectionDate:      binary.LittleEndian.Uint32(value[34:38]),
		PermanentExpireAt: binary.LittleEndian.Uint32(value[38:42]),
		TempExpireAt:      binary.LittleEndian.Uint32(value[42:46]),
		HasADNL:           flags&validatorKeyFlagADNL != 0,
		ADNLExpireAt:      binary.LittleEndian.Uint32(value[78:82]),
	}
	copy(key.Seed[:], value[2:34])
	copy(key.ADNLID[:], value[46:78])
	if err := validateValidatorKey(key); err != nil {
		return keyring.StoredKey{}, err
	}

	return key, nil
}

func validateValidatorKey(key keyring.StoredKey) error {
	if !key.Permanent {
		hasPermanentMetadata := key.ElectionDate != 0 ||
			key.PermanentExpireAt != 0 ||
			key.TempExpireAt != 0 ||
			key.HasADNL
		if hasPermanentMetadata {
			return errors.New("validator pebblestore: non-permanent key has validator metadata")
		}
	}
	if !key.HasADNL && (key.ADNLID != [32]byte{} || key.ADNLExpireAt != 0) {
		return errors.New("validator pebblestore: key without adnl address has adnl metadata")
	}

	return nil
}
