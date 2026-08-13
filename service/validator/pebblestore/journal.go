package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sort"
	"sync"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/simplex"
)

var _ simplex.Journal = (*journal)(nil)

type journal struct {
	store         *ValidatorStore
	session       validator.SessionStorageID
	namespace     storageNamespace
	maxSignatures int
	initErr       error
	ready         chan struct{}

	mu        sync.Mutex
	reserved  map[[32]byte]struct{}
	nextSeq   int64
	isDeleted bool
	// bootstrap caches the state produced by the last full scan of this
	// session's journal keyspace. Starting a session reads it three times with
	// no write in between — recoverReservations, the runtime constructor and
	// Engine.Start — and every scan decodes each stored certificate afresh.
	// writeGen advances on every completed durable write and on deletion, so a
	// scan may only be cached when the generation it started from is still
	// current; that keeps Bootstrap a live read for callers that save first.
	bootstrap *simplex.BootstrapState
	writeGen  uint64
}

// Journal returns the shared durable journal for one exact session
// descriptor. Initialization failures are surfaced by Bootstrap and every
// asynchronous save callback because the interface intentionally has no
// constructor error return.
func (s *ValidatorStore) Journal(session validator.SessionStorageID, maxSignatures int) simplex.Journal {
	namespace, err := namespaceForSession(session)
	if err != nil {
		return failedJournal(err)
	}
	if maxSignatures <= 0 {
		return failedJournal(errors.New("validator pebblestore: max signatures must be positive"))
	}

	s.namespaceMu.Lock()
	_, isDeleting := s.deleting[namespace]
	if !isDeleting {
		// Journal is the explicit session-open operation. It is the only call
		// allowed to reopen a namespace after a completed DeleteSession.
		delete(s.deleted, namespace)
	}
	s.namespaceMu.Unlock()
	if isDeleting {
		return failedJournal(validator.ErrSessionClosed)
	}

	s.journalMu.Lock()
	if existing := s.journals[namespace]; existing != nil {
		s.journalMu.Unlock()
		if existing.session != session {
			return failedJournal(validator.ErrSessionConflict)
		}
		if existing.maxSignatures != maxSignatures {
			return failedJournal(fmt.Errorf(
				"validator pebblestore: journal signature limit changed from %d to %d",
				existing.maxSignatures,
				maxSignatures,
			))
		}

		return existing
	}

	j := &journal{
		store:         s,
		session:       session,
		namespace:     namespace,
		maxSignatures: maxSignatures,
		reserved:      make(map[[32]byte]struct{}),
		ready:         make(chan struct{}),
	}
	s.journals[namespace] = j
	s.journalMu.Unlock()

	if !s.reserveJournalInit() {
		j.finishInitialization(validator.ErrStorageClosed)

		return j
	}
	go s.initializeJournal(j)

	return j
}

func (s *ValidatorStore) initializeJournal(j *journal) {
	defer s.initWG.Done()

	err := s.submitSessionAndWait(context.Background(), j.namespace, func(batch *pebble.Batch) error {
		_, err := ensureSession(batch, j.session, j.namespace)

		return err
	})
	if err == nil {
		err = j.recoverReservations()
	}
	j.finishInitialization(err)
}

func (j *journal) finishInitialization(err error) {
	j.mu.Lock()
	if err != nil {
		j.initErr = err
	}
	close(j.ready)
	j.mu.Unlock()
	if err != nil {
		j.store.journalMu.Lock()
		if j.store.journals[j.namespace] == j {
			delete(j.store.journals, j.namespace)
		}
		j.store.journalMu.Unlock()
	}
}

func failedJournal(err error) *journal {
	ready := make(chan struct{})
	close(ready)

	return &journal{initErr: err, reserved: make(map[[32]byte]struct{}), ready: ready}
}

func (j *journal) Bootstrap() (*simplex.BootstrapState, error) {
	j.waitReady()

	j.mu.Lock()
	err := j.stateErrorLocked()
	cached := j.bootstrap
	generation := j.writeGen
	j.mu.Unlock()
	if err != nil {
		return nil, err
	}
	if cached != nil {
		// The cached path must still fail closed on a store that has been
		// closed or a namespace deleted underneath it, so scan's liveness check
		// is repeated here. It is two point reads, not a keyspace scan.
		if err = j.checkLive(); err != nil {
			return nil, err
		}

		return cached, nil
	}

	state, _, _, err := j.scan()
	if err != nil {
		return nil, err
	}

	j.mu.Lock()
	// A write that completed while the scan ran may or may not be visible in
	// its snapshot, so only a scan started from the current generation may be
	// cached. Any later completion clears the cache again.
	if j.writeGen == generation {
		j.bootstrap = state
	}
	j.mu.Unlock()

	return state, nil
}

// checkLive repeats the fail-closed checks scan performs before it reads the
// journal keyspace.
func (j *journal) checkLive() error {
	if err := j.store.store.acquireRead(); err != nil {
		return err
	}
	defer j.store.store.releaseRead()

	snapshot := j.store.store.db.NewSnapshot()
	defer snapshot.Close()

	return validateStoredDescriptor(context.Background(), snapshot, j.session, j.namespace)
}

// invalidateBootstrapLocked drops the cached state after the journal content
// changed. Callers hold j.mu.
func (j *journal) invalidateBootstrapLocked() {
	j.bootstrap = nil
	j.writeGen++
}

// finishWrite invalidates the cached bootstrap state once a record is durable,
// before the caller's completion runs.
func (j *journal) finishWrite(done func(error)) func(error) {
	return func(err error) {
		if err == nil {
			j.mu.Lock()
			j.invalidateBootstrapLocked()
			j.mu.Unlock()
		}

		done(err)
	}
}

func (j *journal) SaveOurVote(v simplex.Vote, done func(error)) {
	j.waitReady()

	hash := simplex.OurVoteRecordHash(v)

	j.mu.Lock()
	if err := j.stateErrorLocked(); err != nil {
		j.mu.Unlock()
		done(err)

		return
	}
	if _, exists := j.reserved[hash]; exists {
		j.mu.Unlock()
		done(simplex.ErrAlreadySaved)

		return
	}
	seqno := j.nextSeq
	j.nextSeq++
	j.reserved[hash] = struct{}{}

	key := voteKey(j.namespace, simplex.VoteRecordKey(hash))
	value := simplex.EncodeOurVoteRecord(v, seqno)
	err := j.store.submitSession(j.namespace, writeRequest{
		apply: func(batch *pebble.Batch) error {
			summary, err := ensureSession(batch, j.session, j.namespace)
			if err != nil {
				return err
			}
			if _, err := getBatchCopy(batch, key); err == nil {
				return rejectRequest(simplex.ErrAlreadySaved)
			} else if !errors.Is(err, pebble.ErrNotFound) {
				return fmt.Errorf("validator pebblestore: check vote record: %w", err)
			}
			if err := batch.Set(key, value, nil); err != nil {
				return fmt.Errorf("validator pebblestore: save vote record: %w", err)
			}
			if err = incrementSummaryCount("vote", &summary.votes); err != nil {
				return err
			}

			return saveSessionSummary(batch, j.namespace, summary)
		},
		done: j.finishReserved(hash, done),
	})
	if err != nil {
		j.nextSeq--
		delete(j.reserved, hash)
	}
	j.mu.Unlock()
	if err != nil {
		done(err)
	}
}

func (j *journal) SaveCertificate(c *simplex.Certificate, done func(error)) {
	j.waitReady()

	hash := simplex.CertificateRecordHash(c)

	j.mu.Lock()
	if err := j.stateErrorLocked(); err != nil {
		j.mu.Unlock()
		done(err)

		return
	}
	if _, exists := j.reserved[hash]; exists {
		j.mu.Unlock()
		done(simplex.ErrAlreadySaved)

		return
	}
	j.reserved[hash] = struct{}{}

	key := voteKey(j.namespace, simplex.VoteRecordKey(hash))
	value := simplex.EncodeCertificateRecord(c)
	err := j.store.submitSession(j.namespace, writeRequest{
		apply: func(batch *pebble.Batch) error {
			summary, err := ensureSession(batch, j.session, j.namespace)
			if err != nil {
				return err
			}
			if _, err := getBatchCopy(batch, key); err == nil {
				return rejectRequest(simplex.ErrAlreadySaved)
			} else if !errors.Is(err, pebble.ErrNotFound) {
				return fmt.Errorf("validator pebblestore: check certificate record: %w", err)
			}
			if err := batch.Set(key, value, nil); err != nil {
				return fmt.Errorf("validator pebblestore: save certificate record: %w", err)
			}
			if err = incrementSummaryCount("certificate", &summary.certificates); err != nil {
				return err
			}

			return saveSessionSummary(batch, j.namespace, summary)
		},
		done: j.finishReserved(hash, done),
	})
	if err != nil {
		delete(j.reserved, hash)
	}
	j.mu.Unlock()
	if err != nil {
		done(err)
	}
}

func (j *journal) SaveFirstNonAnnouncedWindow(window uint32, done func(error)) {
	j.waitReady()

	j.mu.Lock()
	if err := j.stateErrorLocked(); err != nil {
		j.mu.Unlock()
		done(err)

		return
	}
	if window == 0 {
		j.mu.Unlock()
		done(errors.New("validator pebblestore: first non-announced window must advance beyond 0"))

		return
	}

	value := simplex.EncodePoolStateRecord(window)
	err := j.store.submitSession(j.namespace, writeRequest{
		apply: func(batch *pebble.Batch) error {
			summary, err := ensureSession(batch, j.session, j.namespace)
			if err != nil {
				return err
			}

			key := poolStateKey(j.namespace)
			stored, err := getBatchCopy(batch, key)
			var current uint32
			if err == nil {
				var decodeErr error
				current, decodeErr = simplex.DecodePoolStateRecord(stored)
				if decodeErr != nil {
					return fmt.Errorf("validator pebblestore: decode pool state: %w", decodeErr)
				}
				if current != summary.firstNonAnnouncedWindow {
					return errors.New("validator pebblestore: pool state and session summary disagree")
				}
			} else if !errors.Is(err, pebble.ErrNotFound) {
				return fmt.Errorf("validator pebblestore: read pool state: %w", err)
			} else if summary.firstNonAnnouncedWindow != 0 {
				return errors.New("validator pebblestore: summarized pool state is missing")
			}
			if window <= current {
				return rejectRequest(fmt.Errorf(
					"validator pebblestore: first non-announced window %d does not advance beyond %d",
					window,
					current,
				))
			}
			if err = batch.Set(key, value, nil); err != nil {
				return fmt.Errorf("validator pebblestore: save pool state: %w", err)
			}
			summary.firstNonAnnouncedWindow = window

			return saveSessionSummary(batch, j.namespace, summary)
		},
		done: j.finishWrite(done),
	})
	j.mu.Unlock()
	if err != nil {
		done(err)
	}
}

func (j *journal) finishReserved(hash [32]byte, done func(error)) func(error) {
	return func(err error) {
		j.mu.Lock()
		if err != nil && !errors.Is(err, simplex.ErrAlreadySaved) {
			delete(j.reserved, hash)
		}
		if err == nil {
			j.invalidateBootstrapLocked()
		}
		j.mu.Unlock()

		done(err)
	}
}

func (j *journal) recoverReservations() error {
	j.mu.Lock()
	generation := j.writeGen
	j.mu.Unlock()

	state, reserved, nextSeq, err := j.scan()
	if err != nil {
		return err
	}

	j.mu.Lock()
	j.reserved = reserved
	j.nextSeq = nextSeq
	// This is the session-start scan; unless a write lands first, it is also
	// what the runtime constructor and Engine.Start read back.
	if j.writeGen == generation {
		j.bootstrap = state
	}
	j.mu.Unlock()

	return nil
}

type sequencedVote struct {
	seqno int64
	vote  simplex.Vote
}

func (j *journal) scan() (*simplex.BootstrapState, map[[32]byte]struct{}, int64, error) {
	if err := j.store.store.acquireRead(); err != nil {
		return nil, nil, 0, err
	}
	defer j.store.store.releaseRead()

	snapshot := j.store.store.db.NewSnapshot()
	defer snapshot.Close()
	if err := validateStoredDescriptor(context.Background(), snapshot, j.session, j.namespace); err != nil {
		return nil, nil, 0, err
	}

	state := &simplex.BootstrapState{
		OurVotes:     []simplex.Vote{},
		Certificates: []*simplex.Certificate{},
	}
	pool, err := readerGetCopy(context.Background(), snapshot, poolStateKey(j.namespace))
	if err == nil {
		state.FirstNonAnnouncedWindow, err = simplex.DecodePoolStateRecord(pool)
		if err != nil {
			return nil, nil, 0, fmt.Errorf("validator pebblestore: decode pool state: %w", err)
		}
	} else if !errors.Is(err, pebble.ErrNotFound) {
		return nil, nil, 0, fmt.Errorf("validator pebblestore: read pool state: %w", err)
	}

	prefix := votePrefix(j.namespace)
	lower, upper := prefixBounds(prefix)
	iter, err := snapshot.NewIter(&pebble.IterOptions{LowerBound: lower, UpperBound: upper})
	if err != nil {
		return nil, nil, 0, fmt.Errorf("validator pebblestore: enumerate journal: %w", err)
	}
	defer iter.Close()

	reserved := make(map[[32]byte]struct{})
	votes := []sequencedVote{}
	var nextSeq int64
	for valid := iter.First(); valid; valid = iter.Next() {
		record, decodeErr := simplex.DecodeVoteJournalRecord(iter.Value(), j.maxSignatures)
		if decodeErr != nil {
			return nil, nil, 0, fmt.Errorf("validator pebblestore: decode journal record: %w", decodeErr)
		}

		var hash [32]byte
		if record.Vote != nil {
			if record.Seqno < 0 {
				return nil, nil, 0, fmt.Errorf("validator pebblestore: negative vote seqno %d", record.Seqno)
			}
			hash = simplex.OurVoteRecordHash(*record.Vote)
			votes = append(votes, sequencedVote{seqno: record.Seqno, vote: *record.Vote})
			if record.Seqno >= nextSeq {
				nextSeq = record.Seqno + 1
			}
		} else {
			hash = simplex.CertificateRecordHash(record.Cert)
			state.Certificates = append(state.Certificates, record.Cert)
		}
		expectedKey := voteKey(j.namespace, simplex.VoteRecordKey(hash))
		if !bytes.Equal(iter.Key(), expectedKey) {
			return nil, nil, 0, errors.New("validator pebblestore: journal key hash mismatch")
		}
		if _, exists := reserved[hash]; exists {
			return nil, nil, 0, errors.New("validator pebblestore: duplicate journal record hash")
		}
		reserved[hash] = struct{}{}
	}
	if err = iter.Error(); err != nil {
		return nil, nil, 0, fmt.Errorf("validator pebblestore: enumerate journal: %w", err)
	}

	sort.Slice(votes, func(i, k int) bool {
		return votes[i].seqno < votes[k].seqno
	})
	for i := range votes {
		if i > 0 && votes[i-1].seqno == votes[i].seqno {
			return nil, nil, 0, fmt.Errorf("validator pebblestore: duplicate vote seqno %d", votes[i].seqno)
		}
		state.OurVotes = append(state.OurVotes, votes[i].vote)
	}

	return state, reserved, nextSeq, nil
}

func (j *journal) stateErrorLocked() error {
	if j.initErr != nil {
		return j.initErr
	}
	if j.isDeleted {
		return validator.ErrSessionClosed
	}

	return nil
}

func (j *journal) markDeleted() {
	j.waitReady()

	j.mu.Lock()
	j.isDeleted = true
	j.invalidateBootstrapLocked()
	j.mu.Unlock()
}

func (j *journal) waitReady() {
	if j.ready != nil {
		<-j.ready
	}
}
