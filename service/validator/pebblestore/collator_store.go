package pebblestore

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"errors"
	"fmt"

	"github.com/cockroachdb/pebble/v2"

	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

func (s *CollatorStore) submitSession(sessionID [32]byte, request writeRequest) error {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	if _, deleting := s.deleting[sessionID]; deleting {
		return collator.ErrSessionRetired
	}
	if _, deleted := s.deleted[sessionID]; deleted {
		return collator.ErrSessionRetired
	}

	return collatorStoreError(s.store.submit(request))
}

// submitSessionGeneration opens the only valid path across a completed
// retirement boundary. Consensus session IDs are deterministic and C++ may
// destroy a tentative group at rotated_all_shards, then create the same ID
// again when its shard transition becomes active. Child writes still use
// submitSession and remain rejected until this session record is accepted.
func (s *CollatorStore) submitSessionGeneration(
	ctx context.Context,
	sessionID [32]byte,
	request writeRequest,
) error {
	s.sessionMu.Lock()
	defer s.sessionMu.Unlock()

	if _, deleting := s.deleting[sessionID]; deleting {
		return collator.ErrSessionRetired
	}
	_, reopening := s.deleted[sessionID]
	delete(s.deleted, sessionID)
	err := collatorStoreError(s.store.submitContext(ctx, request))
	if err != nil && reopening {
		s.deleted[sessionID] = struct{}{}
	}

	return err
}

func (s *CollatorStore) submitSessionAsync(sessionID [32]byte, request writeRequest) {
	if err := s.submitSession(sessionID, request); err != nil {
		request.done(err)
	}
}

func (s *CollatorStore) acquireRead() error {
	return collatorStoreError(s.store.acquireRead())
}

func collatorStoreError(err error) error {
	if errors.Is(err, validator.ErrStorageClosed) {
		return collator.ErrClosed
	}

	return err
}

// SaveSession creates a session or advances only its mutable chain view. The
// call itself covers admission to the durable FIFO; only the commit callback
// remains asynchronous after it returns.
func (s *CollatorStore) SaveSession(
	ctx context.Context,
	record collator.SessionRecord,
	done func(error),
) {
	if record.Update.SessionID != record.Session.ID {
		done(collator.ErrSessionConflict)

		return
	}
	if record.Activation != nil && record.Activation.SessionID != record.Session.ID {
		done(collator.ErrSessionConflict)

		return
	}
	value, err := encodeCollatorSessionRecord(record)
	if err != nil {
		done(err)

		return
	}
	ownedRecord, err := decodeCollatorSessionRecord(value)
	if err != nil {
		done(err)

		return
	}
	sessionID := ownedRecord.Session.ID

	request := writeRequest{
		sizeHint:   len(value),
		durability: restartRecoverable,
		apply: func(batch *pebble.Batch) error {
			key := collatorSessionKey(sessionID)
			storedValue, err := getBatchCopy(batch, key)
			if errors.Is(err, pebble.ErrNotFound) {
				if err = batch.Set(key, value, nil); err != nil {
					return fmt.Errorf("validator pebblestore: save collator session: %w", err)
				}

				return nil
			}
			if err != nil {
				return fmt.Errorf("validator pebblestore: read collator session: %w", err)
			}
			// Identical bytes decode to the record we already hold, so the
			// session, activation and update-advance checks below would all
			// compare that record with itself. An idempotent re-save therefore
			// needs no decode of the stored value at all.
			if bytes.Equal(storedValue, value) {
				return nil
			}
			stored, err := decodeCollatorSessionRecord(storedValue)
			if err != nil {
				return err
			}
			if !stored.Session.Equal(ownedRecord.Session) {
				return rejectRequest(collator.ErrSessionConflict)
			}
			if stored.Activation != nil {
				if ownedRecord.Activation == nil ||
					!stored.Activation.Equal(*ownedRecord.Activation) {
					return rejectRequest(collator.ErrSessionConflict)
				}
			}
			if err = collator.ValidateSessionUpdateAdvance(stored.Update, ownedRecord.Update); err != nil {
				return rejectRequest(err)
			}
			if err = batch.Set(key, value, nil); err != nil {
				return fmt.Errorf("validator pebblestore: update collator session: %w", err)
			}

			return nil
		},
		done: done,
	}
	if err = s.submitSessionGeneration(ctx, sessionID, request); err != nil {
		done(err)
	}
}

// Session returns an independently owned session record.
func (s *CollatorStore) Session(
	ctx context.Context,
	sessionID [32]byte,
) (collator.SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return collator.SessionRecord{}, err
	}
	if err := s.acquireRead(); err != nil {
		return collator.SessionRecord{}, err
	}
	defer s.store.releaseRead()

	value, err := readerGetCopy(ctx, s.store.db, collatorSessionKey(sessionID))
	if errors.Is(err, pebble.ErrNotFound) {
		return collator.SessionRecord{}, collator.ErrNotFound
	}
	if err != nil {
		return collator.SessionRecord{}, fmt.Errorf("validator pebblestore: read collator session: %w", err)
	}
	record, err := decodeCollatorSessionRecord(value)
	if err != nil {
		return collator.SessionRecord{}, err
	}
	if record.Session.ID != sessionID {
		return collator.SessionRecord{}, errors.New("validator pebblestore: collator session key mismatch")
	}

	return record, nil
}

// Sessions returns every collator session in session-ID order.
func (s *CollatorStore) Sessions(ctx context.Context) ([]collator.SessionRecord, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := s.acquireRead(); err != nil {
		return nil, err
	}
	defer s.store.releaseRead()

	snapshot := s.store.db.NewSnapshot()
	defer snapshot.Close()

	prefix := collatorRecordPrefix(collatorKeySession)
	records := make([]collator.SessionRecord, 0)
	err := iteratePrefix(ctx, snapshot, prefix, "enumerate collator sessions", func(key, value []byte) error {
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
		records = append(records, record)

		return nil
	})
	if err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}

	// Pebble returns prefix+ID in bytewise key order and the loop above already
	// asserts the key suffix equals the session ID, so the rows arrive sorted.
	return records, nil
}

// SaveCandidate stores one anti-equivocation marker atomically. Both authority
// modes bind to the persisted current session boundary. A delegated marker also
// carries and verifies the leader authorization.
func (s *CollatorStore) SaveCandidate(record collator.CandidateRecord, done func(error)) {
	if !validCandidateAuthority(record.Authority) {
		done(fmt.Errorf(
			"%w: invalid candidate authority %d",
			collator.ErrCandidateConflict,
			record.Authority,
		))

		return
	}
	// SaveCandidate is asynchronous, so it owns every mutable byte slice before
	// returning to the caller. Signature verification and encoding must observe
	// that immutable snapshot even if the producer immediately reuses its value.
	record.Signature = bytes.Clone(record.Signature)
	record.DelegationSignature = bytes.Clone(record.DelegationSignature)
	record.Block.RootHash = bytes.Clone(record.Block.RootHash)
	record.Block.FileHash = bytes.Clone(record.Block.FileHash)
	value, err := encodeCollatorCandidateRecord(record)
	if err != nil {
		done(err)

		return
	}

	s.submitSessionAsync(record.WindowID.SessionID, writeRequest{
		sizeHint:   len(value),
		durability: restartRecoverable,
		apply: func(batch *pebble.Batch) error {
			session, err := collatorSessionFromBatch(batch, record.WindowID.SessionID)
			if err != nil {
				return err
			}
			if err = validateCandidateWindowAuthority(batch, record); err != nil {
				return err
			}

			key := collatorCandidateKey(record.WindowID, record.ID.Slot)
			stored, err := getBatchCopy(batch, key)
			if err == nil {
				if bytes.Equal(stored, value) {
					return nil
				}
				storedRecord, decodeErr := decodeCollatorCandidateRecord(stored)
				if decodeErr != nil {
					return rejectRequest(decodeErr)
				}
				if collatorCandidateRecordsEqual(storedRecord, record) {
					return nil
				}

				return rejectRequest(collator.ErrCandidateConflict)
			}
			if !errors.Is(err, pebble.ErrNotFound) {
				return fmt.Errorf("validator pebblestore: read collator candidate: %w", err)
			}
			switch record.Authority {
			case collator.CandidateAuthorityDelegated:
				if err = validateDelegatedCandidateMarker(session, record); err != nil {
					return err
				}
			case collator.CandidateAuthoritySelf:
				if err = validateSelfCandidateMarker(session, record); err != nil {
					return err
				}
			default:
				return rejectRequest(collator.ErrCandidateConflict)
			}
			if err = batch.Set(key, value, nil); err != nil {
				return fmt.Errorf("validator pebblestore: save collator candidate: %w", err)
			}

			return nil
		},
		done: done,
	})
}

func validateDelegatedCandidateMarker(
	session collator.SessionRecord,
	record collator.CandidateRecord,
) error {
	leader, err := validateCandidateSessionBoundary(session, record)
	if err != nil {
		return err
	}
	delegationKey := ed25519.PublicKey(record.DelegationKey[:])
	collatorID := simplex.KeyNodeIDShort(delegationKey)
	if !simplex.VerifyDelegationSignature(
		ed25519.PublicKey(session.Session.Validators[leader].PublicKey[:]),
		session.Session.ID,
		record.WindowID.StartSlot,
		collatorID,
		record.DelegationSignature,
	) || !simplex.VerifyCandidateSignature(
		delegationKey,
		session.Session.ID,
		record.ID,
		record.Signature,
	) {
		return rejectRequest(collator.ErrCandidateConflict)
	}

	return nil
}

func validateSelfCandidateMarker(session collator.SessionRecord, record collator.CandidateRecord) error {
	leader, err := validateCandidateSessionBoundary(session, record)
	if err != nil {
		return err
	}
	if record.DelegationKey != ([ed25519.PublicKeySize]byte{}) ||
		len(record.DelegationSignature) != 0 ||
		!simplex.VerifyCandidateSignature(
			ed25519.PublicKey(session.Session.Validators[leader].PublicKey[:]),
			session.Session.ID,
			record.ID,
			record.Signature,
		) {
		return rejectRequest(collator.ErrCandidateConflict)
	}

	return nil
}

func validateCandidateSessionBoundary(
	session collator.SessionRecord,
	record collator.CandidateRecord,
) (uint32, error) {
	update := session.Update
	if session.Activation == nil || session.Session.SlotsPerLeaderWindow == 0 ||
		len(session.Session.Validators) == 0 || !update.HasCurrentWindow ||
		update.CurrentWindowStart != record.WindowID.StartSlot ||
		update.CurrentWindowObservedSlot != update.CurrentWindowStart ||
		record.WindowID.StartSlot%session.Session.SlotsPerLeaderWindow != 0 ||
		!candidateSlotInWindow(session.Session, record) {
		return 0, rejectRequest(collator.ErrCandidateConflict)
	}
	leader := record.WindowID.StartSlot / session.Session.SlotsPerLeaderWindow %
		uint32(len(session.Session.Validators))
	if record.Leader != leader {
		return 0, rejectRequest(collator.ErrCandidateConflict)
	}

	return leader, nil
}

// validateCandidateWindowAuthority makes the first marker choose one persisted
// authority for the whole window. Later slots may only extend that exact
// authority, which prevents a restart or mode change from mixing self and
// delegated signatures under the same consensus window.
func validateCandidateWindowAuthority(batch *pebble.Batch, record collator.CandidateRecord) error {
	lower, upper := prefixBounds(collatorCandidatePrefix(record.WindowID))
	iter, err := batch.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return fmt.Errorf("validator pebblestore: inspect collator window authority: %w", err)
	}
	defer iter.Close()

	for valid := iter.First(); valid; valid = iter.Next() {
		stored, decodeErr := decodeCollatorCandidateRecord(iter.Value())
		if decodeErr != nil {
			return rejectRequest(decodeErr)
		}
		if stored.WindowID != record.WindowID || stored.Authority != record.Authority ||
			stored.Leader != record.Leader {
			return rejectRequest(collator.ErrCandidateConflict)
		}
		if record.Authority == collator.CandidateAuthorityDelegated &&
			(stored.DelegationKey != record.DelegationKey ||
				!bytes.Equal(stored.DelegationSignature, record.DelegationSignature)) {
			return rejectRequest(collator.ErrCandidateConflict)
		}
	}
	if err = iter.Error(); err != nil {
		return fmt.Errorf("validator pebblestore: inspect collator window authority: %w", err)
	}

	return nil
}

func candidateSlotInWindow(session collator.Session, record collator.CandidateRecord) bool {
	endSlot := uint64(record.WindowID.StartSlot) + uint64(session.SlotsPerLeaderWindow)

	return record.ID.Slot >= record.WindowID.StartSlot && uint64(record.ID.Slot) < endSlot
}

func collatorCandidateRecordsEqual(a, b collator.CandidateRecord) bool {
	return a.WindowID == b.WindowID && a.Authority == b.Authority && a.ID == b.ID &&
		a.Parent == b.Parent && a.Leader == b.Leader && a.Empty == b.Empty &&
		a.Block.Workchain == b.Block.Workchain && a.Block.Shard == b.Block.Shard &&
		a.Block.SeqNo == b.Block.SeqNo && bytes.Equal(a.Block.RootHash, b.Block.RootHash) &&
		bytes.Equal(a.Block.FileHash, b.Block.FileHash) &&
		a.CollatedFileHash == b.CollatedFileHash && bytes.Equal(a.Signature, b.Signature) &&
		a.DelegationKey == b.DelegationKey && bytes.Equal(a.DelegationSignature, b.DelegationSignature)
}

// Candidate returns an independently owned durable candidate record. Its two
// large payloads may share one private backing array owned by the returned
// record; neither references Pebble-managed memory.
func (s *CollatorStore) Candidate(
	ctx context.Context,
	id collator.WindowID,
	slot uint32,
) (collator.CandidateRecord, error) {
	if err := ctx.Err(); err != nil {
		return collator.CandidateRecord{}, err
	}
	if err := s.acquireRead(); err != nil {
		return collator.CandidateRecord{}, err
	}
	defer s.store.releaseRead()

	value, err := readerGetCopy(ctx, s.store.db, collatorCandidateKey(id, slot))
	if errors.Is(err, pebble.ErrNotFound) {
		return collator.CandidateRecord{}, collator.ErrNotFound
	}
	if err != nil {
		return collator.CandidateRecord{}, fmt.Errorf("validator pebblestore: read collator candidate: %w", err)
	}
	record, err := decodeCollatorCandidateRecord(value)
	if err != nil {
		return collator.CandidateRecord{}, err
	}
	if record.WindowID != id || record.ID.Slot != slot {
		return collator.CandidateRecord{}, errors.New("validator pebblestore: collator candidate key mismatch")
	}

	return record, nil
}

// DeleteSession atomically removes the session and all collator-owned child
// records. Once accepted, child writes stay gated until SaveSession explicitly
// opens a new lifecycle generation for the deterministic session ID.
func (s *CollatorStore) DeleteSession(ctx context.Context, sessionID [32]byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.sessionMu.Lock()
	if _, deleting := s.deleting[sessionID]; deleting {
		s.sessionMu.Unlock()

		return collator.ErrSessionRetired
	}
	if _, deleted := s.deleted[sessionID]; deleted {
		s.sessionMu.Unlock()

		return collator.ErrSessionRetired
	}
	s.deleting[sessionID] = struct{}{}
	s.sessionMu.Unlock()

	result := make(chan error, 1)
	err := collatorStoreError(s.store.submit(writeRequest{
		apply: func(batch *pebble.Batch) error {
			value, err := getBatchCopy(batch, collatorSessionKey(sessionID))
			if errors.Is(err, pebble.ErrNotFound) {
				return rejectRequest(collator.ErrNotFound)
			}
			if err != nil {
				return fmt.Errorf("validator pebblestore: read collator session: %w", err)
			}
			record, err := decodeCollatorSessionRecord(value)
			if err != nil {
				return err
			}
			if record.Session.ID != sessionID {
				return errors.New("validator pebblestore: collator session key mismatch")
			}

			prefixes := [][]byte{collatorCandidateSessionPrefix(sessionID)}
			for _, prefix := range prefixes {
				lower, upper := prefixBounds(prefix)
				if err = batch.DeleteRange(lower, upper, nil); err != nil {
					return fmt.Errorf("validator pebblestore: delete collator session records: %w", err)
				}
			}
			if err = batch.Delete(collatorSessionKey(sessionID), nil); err != nil {
				return fmt.Errorf("validator pebblestore: delete collator session: %w", err)
			}

			return nil
		},
		done: func(err error) { result <- err },
	}))
	if err == nil {
		// Cancellation after acceptance cannot release the gate before the range
		// tombstones are durable, otherwise a later save could recreate data.
		err = <-result
	}

	s.sessionMu.Lock()
	delete(s.deleting, sessionID)
	if err == nil {
		s.deleted[sessionID] = struct{}{}
	}
	s.sessionMu.Unlock()

	return err
}

func collatorSessionFromBatch(
	batch *pebble.Batch,
	sessionID [32]byte,
) (collator.SessionRecord, error) {
	value, err := getBatchCopy(batch, collatorSessionKey(sessionID))
	if errors.Is(err, pebble.ErrNotFound) {
		return collator.SessionRecord{}, rejectRequest(collator.ErrNotFound)
	}
	if err != nil {
		return collator.SessionRecord{}, fmt.Errorf("validator pebblestore: read collator session: %w", err)
	}
	// A corrupt or misfiled session record belongs to the request that reads
	// it, not to the batch: every caller resolves the session before its first
	// mutation, so rejecting here keeps unrelated consensus writes batched
	// alongside it committable.
	record, err := decodeCollatorSessionRecord(value)
	if err != nil {
		return collator.SessionRecord{}, rejectRequest(err)
	}
	if record.Session.ID != sessionID {
		return collator.SessionRecord{}, rejectRequest(
			errors.New("validator pebblestore: collator session key mismatch"),
		)
	}

	return record, nil
}
