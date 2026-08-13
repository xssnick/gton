package pebblestore

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/xssnick/gton/service/validator/collator"
)

// Status returns logical collator counts from one snapshot together with the
// shared physical database pressure and global write backlog.
func (s *CollatorStore) Status(ctx context.Context) (collator.StorageStatus, error) {
	if err := ctx.Err(); err != nil {
		return collator.StorageStatus{}, err
	}
	if err := s.acquireRead(); err != nil {
		return collator.StorageStatus{}, err
	}
	defer s.store.releaseRead()

	snapshot := s.store.db.NewSnapshot()
	defer snapshot.Close()

	sessions, err := countSessions(ctx, snapshot)
	if err != nil {
		return collator.StorageStatus{}, err
	}
	candidates, err := countCandidates(ctx, snapshot)
	if err != nil {
		return collator.StorageStatus{}, err
	}
	if err = ctx.Err(); err != nil {
		return collator.StorageStatus{}, err
	}

	return collator.StorageStatus{
		Sessions:      sessions,
		Candidates:    candidates,
		PendingWrites: int(s.store.outstanding.Load()),
		DB:            pebbleDBMetrics(s.store.db.Metrics()),
	}, nil
}

func countSessions(ctx context.Context, snapshot *pebble.Snapshot) (uint64, error) {
	prefix := collatorRecordPrefix(collatorKeySession)
	var count uint64
	err := iteratePrefix(ctx, snapshot, prefix, "count collator sessions", func(key, value []byte) error {
		if len(key) != len(prefix)+32 {
			return fmt.Errorf("validator pebblestore: collator session key length %d", len(key))
		}
		record, decodeErr := decodeCollatorSessionRecord(value)
		if decodeErr != nil {
			return decodeErr
		}
		if !bytes.Equal(key[len(prefix):], record.Session.ID[:]) {
			return errors.New("validator pebblestore: collator session key mismatch")
		}
		count++

		return nil
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}

func countCandidates(ctx context.Context, snapshot *pebble.Snapshot) (uint64, error) {
	prefix := collatorRecordPrefix(collatorKeyCandidate)
	var count uint64
	err := iteratePrefix(ctx, snapshot, prefix, "count collator candidates", func(key, value []byte) error {
		if len(key) != len(prefix)+40 {
			return fmt.Errorf("validator pebblestore: collator candidate key length %d", len(key))
		}
		// The record is checked against its key without a full decode: this
		// runs over every stored candidate on an operator status call.
		if len(value) < 41 || value[0] != collatorCandidateRecordVersion {
			return errors.New("validator pebblestore: invalid collator candidate record version")
		}
		if !validCandidateAuthority(collator.CandidateAuthority(value[len(value)-1])) {
			return errors.New("validator pebblestore: invalid collator candidate authority")
		}
		prefixLength := len(prefix)
		if !bytes.Equal(key[prefixLength:prefixLength+32], value[1:33]) ||
			binary.BigEndian.Uint32(key[prefixLength+32:prefixLength+36]) != binary.LittleEndian.Uint32(value[33:37]) ||
			binary.BigEndian.Uint32(key[prefixLength+36:prefixLength+40]) != binary.LittleEndian.Uint32(value[37:41]) {
			return errors.New("validator pebblestore: collator candidate key mismatch")
		}
		count++

		return nil
	})
	if err != nil {
		return 0, err
	}

	return count, nil
}
