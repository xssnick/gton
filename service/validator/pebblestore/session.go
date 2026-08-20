package pebblestore

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"io"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/simplex"
)

type pebbleReader interface {
	Get(key []byte) ([]byte, io.Closer, error)
	NewIter(options *pebble.IterOptions) (*pebble.Iterator, error)
}

// SaveCandidate appends candidate content to a session pack and stores its
// compact ID index in Pebble.
//
// Wire is borrowed, not copied: the write goroutine reads it when it applies
// the request, so it must stay unmodified until done fires and is free
// afterwards. The resolver already publishes this wire as immutable.
//
// Both the pack append and the Pebble pointer are intentionally unsynced. A
// crash may leave an orphan pack tail or a pointer to a lost tail; Candidate
// verifies the record hash and reports the latter as not found, so the resolver
// fetches it again. The callback still gates the vote after both writes have
// reached the operating system, without putting the multi-megabyte payload in
// Pebble's WAL or waiting for an fsync.
func (s *ValidatorStore) SaveCandidate(
	session validator.SessionStorageID,
	candidate validator.CandidateRecord,
	done func(error),
) {
	namespace, err := namespaceForSession(session)
	if err != nil {
		done(err)

		return
	}
	wire := candidate.Wire
	// The caller's digest of these bytes when it has one — the resolver hashed
	// this wire to admit it and to refuse a conflicting duplicate under it — and
	// otherwise our own. Deriving it a second time here cost a full sha256 of a
	// multi-megabyte payload per candidate on the producer's goroutine, for a
	// content-addressing indirection the reference does not have at all: C++
	// keys candidate content by candidate id and hashes no wire on the store
	// path (candidate-resolver.cpp:373).
	wireHash := candidate.ContentHash
	if wireHash == ([32]byte{}) {
		wireHash = sha256.Sum256(wire)
	}

	s.submitSessionAsync(namespace, writeRequest{
		sizeHint:   candidatePackPointerSize,
		durability: restartRecoverable,
		apply: func(batch *pebble.Batch) error {
			summary, err := ensureSession(batch, session, namespace)
			if err != nil {
				return err
			}

			// The integrity checks below describe this session's stored
			// candidate records, so they reject only this request. Reaching
			// them means nothing beyond ensureSession's idempotent namespace
			// bootstrap has been written into the shared batch.
			indexKey := candidateIndexKey(namespace, candidate.ID)
			storedValue, err := getBatchCopy(batch, indexKey)
			isNew := errors.Is(err, pebble.ErrNotFound)
			if err == nil {
				pointer, decodeErr := decodeCandidatePackPointer(storedValue)
				if decodeErr != nil {
					return rejectRequest(decodeErr)
				}
				if pointer.hash != wireHash {
					return rejectRequest(validator.ErrCandidateConflict)
				}
				if _, readErr := s.candidatePacks.read(namespace, pointer); readErr == nil {
					return nil
				} else if !errors.Is(readErr, storage.ErrNotFound) {
					return readErr
				}
				// The pointer itself committed atomically, but its unsynced pack
				// tail may have been lost in a machine crash. Appending the wire
				// again and replacing that pointer is the recovery path.
			}
			if err != nil && !isNew {
				return fmt.Errorf("validator pebblestore: read candidate index: %w", err)
			}

			pointer, err := s.candidatePacks.append(namespace, wire, wireHash)
			if err != nil {
				return err
			}
			if err = batch.Set(indexKey, encodeCandidatePackPointer(pointer), nil); err != nil {
				return fmt.Errorf("validator pebblestore: save candidate index: %w", err)
			}
			if !isNew {
				return nil
			}
			if err = incrementSummaryCount("candidate", &summary.candidates); err != nil {
				return err
			}

			return saveSessionSummary(batch, namespace, summary)
		},
		done: done,
	})
}

// Candidate returns an independently owned copy of canonical candidate wire.
func (s *ValidatorStore) Candidate(
	ctx context.Context,
	session validator.SessionStorageID,
	id simplex.CandidateID,
) (validator.CandidateRecord, error) {
	namespace, err := namespaceForSession(session)
	if err != nil {
		return validator.CandidateRecord{}, err
	}
	if err = s.store.acquireRead(); err != nil {
		return validator.CandidateRecord{}, err
	}
	defer s.store.releaseRead()

	snapshot := s.store.db.NewSnapshot()
	defer snapshot.Close()
	if err = validateStoredDescriptor(ctx, snapshot, session, namespace); err != nil {
		return validator.CandidateRecord{}, err
	}

	value, err := readerGetCopy(ctx, snapshot, candidateIndexKey(namespace, id))
	if errors.Is(err, pebble.ErrNotFound) {
		return validator.CandidateRecord{}, storage.ErrNotFound
	}
	if err != nil {
		return validator.CandidateRecord{}, fmt.Errorf("validator pebblestore: read candidate index: %w", err)
	}
	pointer, err := decodeCandidatePackPointer(value)
	if err != nil {
		return validator.CandidateRecord{}, err
	}
	wire, err := s.candidatePacks.read(namespace, pointer)
	if err != nil {
		return validator.CandidateRecord{}, err
	}
	if err = ctx.Err(); err != nil {
		return validator.CandidateRecord{}, err
	}

	return validator.CandidateRecord{ID: id, Wire: wire, ContentHash: pointer.hash}, nil
}

// MarkFinalized records one candidate finality marker. The authenticated final
// certificate is the authority and can be re-fetched; this derived replay index
// is restart-recoverable.
func (s *ValidatorStore) MarkFinalized(
	session validator.SessionStorageID,
	id simplex.CandidateID,
	done func(error),
) {
	namespace, err := namespaceForSession(session)
	if err != nil {
		done(err)

		return
	}

	s.submitSessionAsync(namespace, writeRequest{
		durability: restartRecoverable,
		apply: func(batch *pebble.Batch) error {
			summary, err := ensureSession(batch, session, namespace)
			if err != nil {
				return err
			}

			key := finalizedKey(namespace, id)
			if _, err = getBatchCopy(batch, key); err == nil {
				return nil
			} else if !errors.Is(err, pebble.ErrNotFound) {
				return fmt.Errorf("validator pebblestore: check finalized marker: %w", err)
			}
			if err = batch.Set(key, []byte{}, nil); err != nil {
				return fmt.Errorf("validator pebblestore: save finalized marker: %w", err)
			}
			if err = incrementSummaryCount("finalized", &summary.finalized); err != nil {
				return err
			}
			if summary.lastFinalized == nil || candidateIDLess(*summary.lastFinalized, id) {
				copyID := id
				summary.lastFinalized = &copyID
			}

			return saveSessionSummary(batch, namespace, summary)
		},
		done: done,
	})
}

// RecordLeaderWindow stores the latest observation for a leader-window start.
//
// Only the session summary is written. There used to be a row per observed
// window under keyLeaderWindow as well, and nothing ever read one: LoadSession
// deliberately skips them, and the count the status and debug views report
// comes from the summary. The reference keeps no such history either — its
// LeaderWindowObserved handler (db.cpp) overwrites one pool-state key and
// stores nothing per window — so the rows were durable bytes with no consumer
// in either implementation, accumulating for the life of a session.
//
// keyLeaderWindow stays reserved and DeleteSession still range-deletes it, so a
// database written before this change is cleaned up by the ordinary teardown
// path rather than needing a migration.
func (s *ValidatorStore) RecordLeaderWindow(
	session validator.SessionStorageID,
	window validator.LeaderWindowRecord,
	done func(error),
) {
	namespace, err := namespaceForSession(session)
	if err != nil {
		done(err)

		return
	}
	if err = validateLeaderWindow(window); err != nil {
		done(err)

		return
	}

	s.submitSessionAsync(namespace, writeRequest{
		durability: restartRecoverable,
		apply: func(batch *pebble.Batch) error {
			summary, err := ensureSession(batch, session, namespace)
			if err != nil {
				return err
			}

			// Windows are observed in order, so "starts later than the last one
			// recorded" counts exactly the distinct windows the per-window keys
			// used to count — without the point read that decided it.
			if summary.lastLeaderWindow == nil || window.StartSlot > summary.lastLeaderWindow.StartSlot {
				if err = incrementSummaryCount("leader window", &summary.leaderWindows); err != nil {
					return err
				}
			}
			if summary.lastLeaderWindow == nil || window.StartSlot >= summary.lastLeaderWindow.StartSlot {
				copyWindow := window
				summary.lastLeaderWindow = &copyWindow
			}

			return saveSessionSummary(batch, namespace, summary)
		},
		done: done,
	})
}

// SaveDelegationAuthorization stores one immutable outbound authority for a
// future local leader window. Its context bounds admission to the shared FIFO;
// once admitted, the callback reports the durable result independently.
func (s *ValidatorStore) SaveDelegationAuthorization(
	ctx context.Context,
	session validator.SessionStorageID,
	authorization validator.DelegationAuthorization,
	done func(error),
) {
	if err := ctx.Err(); err != nil {
		done(err)

		return
	}
	namespace, err := namespaceForSession(session)
	if err != nil {
		done(err)

		return
	}
	value, err := encodeDelegationAuthorization(authorization)
	if err != nil {
		done(err)

		return
	}

	err = s.submitSessionContext(ctx, namespace, writeRequest{
		sizeHint: len(value),
		apply: func(batch *pebble.Batch) error {
			if _, err := ensureSession(batch, session, namespace); err != nil {
				return err
			}

			key := delegationAuthorizationKey(namespace, authorization.StartSlot)
			stored, err := getBatchCopy(batch, key)
			if err == nil {
				if bytes.Equal(stored, value) {
					return nil
				}

				storedAuthorization, decodeErr := decodeDelegationAuthorization(stored)
				if decodeErr != nil {
					return rejectRequest(decodeErr)
				}
				if storedAuthorization.StartSlot != authorization.StartSlot {
					return rejectRequest(errors.New(
						"validator pebblestore: delegation authorization key mismatch",
					))
				}

				return rejectRequest(validator.ErrDelegationConflict)
			}
			if !errors.Is(err, pebble.ErrNotFound) {
				return fmt.Errorf("validator pebblestore: read delegation authorization: %w", err)
			}
			if err = batch.Set(key, value, nil); err != nil {
				return fmt.Errorf("validator pebblestore: save delegation authorization: %w", err)
			}

			return nil
		},
		done: done,
	})
	if err != nil {
		done(err)
	}
}

// DelegationAuthorization returns an independently owned outbound authority.
func (s *ValidatorStore) DelegationAuthorization(
	ctx context.Context,
	session validator.SessionStorageID,
	startSlot uint32,
) (validator.DelegationAuthorization, error) {
	namespace, err := namespaceForSession(session)
	if err != nil {
		return validator.DelegationAuthorization{}, err
	}
	if err = s.store.acquireRead(); err != nil {
		return validator.DelegationAuthorization{}, err
	}
	defer s.store.releaseRead()

	snapshot := s.store.db.NewSnapshot()
	defer snapshot.Close()
	if err = validateStoredDescriptor(ctx, snapshot, session, namespace); err != nil {
		return validator.DelegationAuthorization{}, err
	}

	value, err := readerGetCopy(ctx, snapshot, delegationAuthorizationKey(namespace, startSlot))
	if errors.Is(err, pebble.ErrNotFound) {
		return validator.DelegationAuthorization{}, storage.ErrNotFound
	}
	if err != nil {
		return validator.DelegationAuthorization{}, fmt.Errorf(
			"validator pebblestore: read delegation authorization: %w",
			err,
		)
	}
	authorization, err := decodeDelegationAuthorization(value)
	if err != nil {
		return validator.DelegationAuthorization{}, err
	}
	if authorization.StartSlot != startSlot {
		return validator.DelegationAuthorization{}, errors.New(
			"validator pebblestore: delegation authorization key mismatch",
		)
	}

	return authorization, nil
}

// LoadSession enumerates the candidate and finality state a runtime recovers
// from, using one Pebble snapshot.
//
// Leader windows are durable telemetry, not recovery state: nothing the runtime
// starts reads them, and the counter the status and debug views report comes
// from the session summary. They stay in the schema and are still written; they
// are simply not decoded on the startup path.
func (s *ValidatorStore) LoadSession(
	ctx context.Context,
	session validator.SessionStorageID,
) (validator.StoredSessionState, error) {
	namespace, err := namespaceForSession(session)
	if err != nil {
		return validator.StoredSessionState{}, err
	}
	if err = s.store.acquireRead(); err != nil {
		return validator.StoredSessionState{}, err
	}
	defer s.store.releaseRead()

	snapshot := s.store.db.NewSnapshot()
	defer snapshot.Close()
	if err = validateStoredDescriptor(ctx, snapshot, session, namespace); err != nil {
		return validator.StoredSessionState{}, err
	}

	state := validator.StoredSessionState{
		ID:           session,
		CandidateIDs: []simplex.CandidateID{},
		Finalized:    []simplex.CandidateID{},
	}
	if err = loadCandidateIDs(ctx, snapshot, namespace, &state); err != nil {
		return validator.StoredSessionState{}, err
	}
	if err = loadFinalized(ctx, snapshot, namespace, &state); err != nil {
		return validator.StoredSessionState{}, err
	}

	return state, nil
}

// DeleteSession explicitly removes only the addressed namespace.
func (s *ValidatorStore) DeleteSession(ctx context.Context, session validator.SessionStorageID) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	namespace, err := namespaceForSession(session)
	if err != nil {
		return err
	}

	s.namespaceMu.Lock()
	if _, isDeleting := s.deleting[namespace]; isDeleting {
		s.namespaceMu.Unlock()

		return validator.ErrSessionClosed
	}
	s.deleting[namespace] = struct{}{}
	s.namespaceMu.Unlock()

	// Once accepted, deletion must remain gated until its durable completion;
	// returning early on ctx cancellation would let a later save queue behind
	// the still-running range tombstones and recreate the namespace.
	err = s.store.submitAndWait(context.Background(), func(batch *pebble.Batch) error {
		stored, err := getBatchCopy(batch, sessionKey(namespace))
		if errors.Is(err, pebble.ErrNotFound) {
			return rejectRequest(storage.ErrNotFound)
		}
		if err != nil {
			return fmt.Errorf("validator pebblestore: read session descriptor: %w", err)
		}
		storedID, err := decodeSessionID(stored)
		if err != nil {
			return err
		}
		if storedID != session {
			return rejectRequest(validator.ErrSessionConflict)
		}
		summary, err := getBatchCopy(batch, summaryKey(namespace))
		if errors.Is(err, pebble.ErrNotFound) {
			return errSessionSummaryMissing
		}
		if err != nil {
			return fmt.Errorf("validator pebblestore: read session summary: %w", err)
		}
		if _, err = decodeSessionSummary(summary); err != nil {
			return err
		}

		// Every namespaced prefix in the key schema, including the retired one.
		// keyAcceptedProof carries no new rows, but a database written before it
		// was retired holds them, and once the descriptor is gone nothing can
		// name them again — so they would outlive the namespace with no path
		// left to reach them.
		prefixes := [][]byte{
			votePrefix(namespace),
			poolStateKey(namespace),
			candidateIndexPrefix(namespace),
			candidateContentPrefix(namespace),
			finalizedPrefix(namespace),
			leaderWindowPrefix(namespace),
			acceptedProofPrefix(namespace),
			delegationAuthorizationPrefix(namespace),
			summaryKey(namespace),
		}
		for _, prefix := range prefixes {
			lower, upper := prefixBounds(prefix)
			if err = batch.DeleteRange(lower, upper, nil); err != nil {
				return fmt.Errorf("validator pebblestore: delete session records: %w", err)
			}
		}
		if err = batch.Delete(sessionKey(namespace), nil); err != nil {
			return fmt.Errorf("validator pebblestore: delete session descriptor: %w", err)
		}
		if err = s.candidatePacks.delete(namespace); err != nil {
			return err
		}

		return nil
	})
	if err == nil {
		s.journalMu.Lock()
		if existing := s.journals[namespace]; existing != nil {
			existing.markDeleted()
			delete(s.journals, namespace)
		}
		s.journalMu.Unlock()
	}

	s.namespaceMu.Lock()
	delete(s.deleting, namespace)
	if err == nil {
		s.deleted[namespace] = struct{}{}
	}
	s.namespaceMu.Unlock()

	return err
}

func validateStoredDescriptor(
	ctx context.Context,
	reader pebbleReader,
	expected validator.SessionStorageID,
	namespace storageNamespace,
) error {
	stored, err := readerGetCopy(ctx, reader, sessionKey(namespace))
	if errors.Is(err, pebble.ErrNotFound) {
		return storage.ErrNotFound
	}
	if err != nil {
		return fmt.Errorf("validator pebblestore: read session descriptor: %w", err)
	}
	storedID, err := decodeSessionID(stored)
	if err != nil {
		return err
	}
	if storedID != expected {
		return validator.ErrSessionConflict
	}
	summary, err := readerGetCopy(ctx, reader, summaryKey(namespace))
	if errors.Is(err, pebble.ErrNotFound) {
		return errSessionSummaryMissing
	}
	if err != nil {
		return fmt.Errorf("validator pebblestore: read session summary: %w", err)
	}
	if _, err = decodeSessionSummary(summary); err != nil {
		return err
	}

	return nil
}

func readerGetCopy(ctx context.Context, reader pebbleReader, key []byte) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	value, closer, err := reader.Get(key)
	if err != nil {
		return nil, err
	}
	copyValue := append([]byte(nil), value...)
	if err = closer.Close(); err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}

	return copyValue, nil
}

func loadCandidateIDs(
	ctx context.Context,
	reader pebbleReader,
	namespace storageNamespace,
	state *validator.StoredSessionState,
) error {
	prefix := candidateIndexPrefix(namespace)

	return iteratePrefix(ctx, reader, prefix, "enumerate candidates", func(key, value []byte) error {
		id, decodeErr := candidateIDFromKey(key, prefix)
		if decodeErr != nil {
			return decodeErr
		}
		if _, decodeErr = decodeCandidatePackPointer(value); decodeErr != nil {
			return decodeErr
		}
		state.CandidateIDs = append(state.CandidateIDs, id)

		return nil
	})
}

func loadFinalized(
	ctx context.Context,
	reader pebbleReader,
	namespace storageNamespace,
	state *validator.StoredSessionState,
) error {
	prefix := finalizedPrefix(namespace)

	return iteratePrefix(ctx, reader, prefix, "enumerate finality", func(key, _ []byte) error {
		id, decodeErr := candidateIDFromKey(key, prefix)
		if decodeErr != nil {
			return decodeErr
		}
		state.Finalized = append(state.Finalized, id)

		return nil
	})
}

// iteratePrefix runs visit over every row under prefix, in key order. It owns
// the iterator lifetime: visit never sees a closed iterator and never has to
// close one, which is what removes the close-on-every-error-path shape from the
// callers. Errors from visit and from the context are returned raw so callers
// keep their own classification; only the iterator's own failures are wrapped.
func iteratePrefix(
	ctx context.Context,
	reader pebbleReader,
	prefix []byte,
	what string,
	visit func(key, value []byte) error,
) error {
	lower, upper := prefixBounds(prefix)
	iter, err := reader.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return fmt.Errorf("validator pebblestore: %s: %w", what, err)
	}
	for valid := iter.First(); valid; valid = iter.Next() {
		if err = ctx.Err(); err != nil {
			break
		}
		if err = visit(iter.Key(), iter.Value()); err != nil {
			break
		}
	}
	if closeErr := errors.Join(iter.Error(), iter.Close()); closeErr != nil && err == nil {
		return fmt.Errorf("validator pebblestore: %s: %w", what, closeErr)
	}

	return err
}
