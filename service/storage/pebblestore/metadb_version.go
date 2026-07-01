package pebblestore

import (
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"
	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/storage"
)

func ensureMetaDBVersion(db *pebble.DB, readOnly bool, log zerolog.Logger) error {
	raw, closer, err := pebbleReaderGet(db, hotKeyMetaDBVersion())
	if err == nil {
		defer func() { _ = closer.Close() }()

		version, err := decodeMetaDBVersion(raw)
		if err != nil {
			return err
		}
		if version == metaDBVersion {
			return nil
		}
		if readOnly {
			return fmt.Errorf("metadb version %d requires writable migration to %d", version, metaDBVersion)
		}
		return runMetaDBMigrations(db, version, log)
	}
	if !errors.Is(err, storage.ErrNotFound) {
		return err
	}

	hasRecords, err := pebbleHasAnyUserRecord(db)
	if err != nil {
		return err
	}
	if hasRecords {
		return fmt.Errorf("unsupported metadb version: version record is missing; rebuild metadb")
	}
	if readOnly {
		return fmt.Errorf("unsupported metadb version: version record is missing in read-only metadb")
	}
	return db.Set(hotKeyMetaDBVersion(), encodeMetaDBVersion(metaDBVersion), pebble.Sync)
}

func encodeMetaDBVersion(version uint32) []byte {
	return binary.BigEndian.AppendUint32(nil, version)
}

func decodeMetaDBVersion(data []byte) (uint32, error) {
	if len(data) != 4 {
		return 0, fmt.Errorf("metadb version payload size mismatch")
	}
	return binary.BigEndian.Uint32(data), nil
}

func isMetaDBVersionKey(key []byte) bool {
	return bytes.Equal(key, hotPrefixMetaDBVersion)
}
