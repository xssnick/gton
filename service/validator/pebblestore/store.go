package pebblestore

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/collator"
)

const (
	maxWriteBatch      = 128
	maxWriteBatchBytes = 16 << 20
)

var _ validator.ValidatorStorage = (*ValidatorStore)(nil)
var _ collator.CollatorStorage = (*CollatorStore)(nil)

// durabilityClass is what a write needs from the disk before its callback fires.
//
// The zero value is the strict one, deliberately: a write that says nothing about
// itself is fsynced. There is no way to end up in the relaxed class by omission,
// only by naming it — and TestDurabilityClassificationIsExplicit enumerates every
// site that names it, so adding one is a visible edit rather than a default.
type durabilityClass uint8

const (
	// durableCommitment is a record whose purpose is to constrain this node's
	// future behaviour: something it has signed, voted, notarized, finalized or
	// authorized. Losing one across a restart does not cost a resync, it lets this
	// node produce a CONFLICTING record — a safety violation. These are fsynced,
	// and the caller waits for the fsync, which is the whole point.
	durableCommitment durabilityClass = iota
	// restartRecoverable is state that a restart may lose without changing what
	// this node is allowed to sign. The network or a durable consensus record can
	// reconstruct it; losing it costs replay, re-fetching, or telemetry only. The
	// collator marker is the explicit accepted exception to that rule. The
	// callback of this class fires once the write is in the write-ahead log
	// rather than after the log has been fsynced.
	//
	// Error reporting is unchanged. Only the stable-media wait goes away.
	restartRecoverable
)

type writeRequest struct {
	apply func(*pebble.Batch) error
	done  func(error)
	// sizeHint is the approximate encoded value size. Zero marks small or
	// unknown writes, which remain bounded by maxWriteBatch.
	sizeHint int
	// durability is this write's class. Left unset it is durableCommitment, so
	// nothing becomes unsynced by accident.
	durability durabilityClass
}

type requestError struct {
	err error
}

func (e *requestError) Error() string {
	return e.err.Error()
}

func (e *requestError) Unwrap() error {
	return e.err
}

// rejectRequest marks an error as belonging to one request instead of to the
// whole batch: commitRequests fails only that request and still commits the
// rest. pebble.Batch has no per-request rollback, so whatever the request
// already wrote into the shared batch stays in the committed batch. A site may
// therefore reject only before it has written any state change of its own —
// the sole permitted exception is ensureSession's namespace bootstrap, which
// is idempotent and not conditional on the rejecting request succeeding.
// Anything that can leave the batch inconsistent (batch.Set, batch.Get I/O
// failures, Commit, Close) must stay fatal to the batch.
func rejectRequest(err error) error {
	return &requestError{err: err}
}

// Store owns the physical Pebble metadata database, candidate packs and their
// global writer.
type Store struct {
	db    *pebble.DB
	cache *pebble.Cache

	validator *ValidatorStore
	collator  *CollatorStore

	queue      chan writeRequest
	writerDone chan struct{}

	// stateMu is held shared across every queue send and exclusively by Close
	// while it closes the queue. isClosed is stored under the exclusive lock but
	// read without it, so reads and journal reservations never wait behind a
	// send that is blocked on a full queue.
	stateMu   sync.RWMutex
	isClosed  atomic.Bool
	closeErr  error
	closeDone chan struct{}

	readMu sync.RWMutex

	outstanding atomic.Int64

	callbackWG sync.WaitGroup
}

// ValidatorStore is the stable validator persistence view of a Store.
type ValidatorStore struct {
	store          *Store
	candidatePacks *candidatePackStore

	namespaceMu sync.Mutex
	deleting    map[storageNamespace]struct{}
	deleted     map[storageNamespace]struct{}
	// admitting counts the writes per namespace that passed the deletion gate
	// and are still being sent to the queue. namespaceMu is not held across that
	// send, so DeleteSession waits on admitted until its namespace has none left
	// and its tombstones queue behind every write the gate let through.
	admitting map[storageNamespace]int
	admitted  *sync.Cond

	journalMu sync.Mutex
	journals  map[storageNamespace]*journal

	initMu sync.Mutex
	initWG sync.WaitGroup
}

// CollatorStore is the stable collator persistence view of a Store. The
// physical owner remains Store, so this view deliberately has no Close.
type CollatorStore struct {
	store *Store

	sessionMu sync.Mutex
	deleting  map[[32]byte]struct{}
	deleted   map[[32]byte]struct{}
}

// Validator returns the validator persistence view owned by this store.
func (s *Store) Validator() *ValidatorStore {
	return s.validator
}

// Collator returns the collator persistence view owned by this physical
// database. Only the standalone collator gets a Store of its own; the
// validator-local collator is this view of the consensus database, so its
// candidate writes share one queue and one writer goroutine with every consensus
// write. That single queue is also the group-commit mechanism: small writes ride
// one fsync, so separating the two workloads is not free and must be measured
// before it is done.
//
// Restart-recoverable writes share this queue too. In particular the collator
// candidate marker is intentionally NoSync: an abrupt machine failure can lose
// the fence and permit another candidate for an already expired slot, a risk the
// operator accepts to keep the block path independent of disk fsync latency.
func (s *Store) Collator() *CollatorStore {
	return s.collator
}

// Close rejects new work, drains every accepted write and callback, then
// closes the database and its cache. It is safe to call more than once.
func (s *Store) Close() error {
	s.stateMu.Lock()
	if s.isClosed.Load() {
		done := s.closeDone
		s.stateMu.Unlock()
		<-done

		s.stateMu.Lock()
		err := s.closeErr
		s.stateMu.Unlock()

		return err
	}

	s.isClosed.Store(true)
	close(s.queue)
	done := s.closeDone
	s.stateMu.Unlock()

	// Pair with reserveJournalInit before waiting: once shutdown is visible,
	// no new positive WaitGroup delta can race this Wait.
	s.validator.initMu.Lock()
	s.validator.initMu.Unlock()

	<-s.writerDone
	s.validator.initWG.Wait()
	s.callbackWG.Wait()

	s.readMu.Lock()
	err := errors.Join(s.validator.candidatePacks.close(), s.db.Close())
	s.cache.Unref()
	s.readMu.Unlock()

	s.stateMu.Lock()
	s.closeErr = err
	close(done)
	s.stateMu.Unlock()

	return err
}

func (s *ValidatorStore) reserveJournalInit() bool {
	s.initMu.Lock()
	defer s.initMu.Unlock()

	if s.store.isClosed.Load() {
		return false
	}
	s.initWG.Add(1)

	return true
}

func (s *Store) submit(req writeRequest) error {
	return s.submitContext(context.Background(), req)
}

// submitContext admits one request into the durable FIFO before returning.
// Holding stateMu across the cancellable send keeps Close from closing queue
// between the state check and admission. The writer never takes stateMu.
func (s *Store) submitContext(ctx context.Context, req writeRequest) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	s.stateMu.RLock()
	defer s.stateMu.RUnlock()

	if s.isClosed.Load() {
		return validator.ErrStorageClosed
	}

	// Holding stateMu across the bounded send prevents Close from closing the
	// channel between the state check and submission. Senders hold it shared:
	// the channel already orders them. The writer never takes this lock, so
	// backpressure cannot deadlock shutdown.
	s.outstanding.Add(1)
	select {
	case s.queue <- req:
		return nil
	case <-ctx.Done():
		s.outstanding.Add(-1)

		return ctx.Err()
	}
}

func (s *ValidatorStore) submitSession(namespace storageNamespace, req writeRequest) error {
	return s.submitSessionContext(context.Background(), namespace, req)
}

func (s *ValidatorStore) submitSessionContext(
	ctx context.Context,
	namespace storageNamespace,
	req writeRequest,
) error {
	s.namespaceMu.Lock()
	_, isDeleting := s.deleting[namespace]
	_, isDeleted := s.deleted[namespace]
	if isDeleting || isDeleted {
		s.namespaceMu.Unlock()

		return validator.ErrSessionClosed
	}
	s.admitting[namespace]++
	s.namespaceMu.Unlock()

	// The send may wait for room in a full queue, so it runs without
	// namespaceMu: Journal and the gates of other namespaces stay available.
	err := s.store.submitContext(ctx, req)

	s.namespaceMu.Lock()
	s.admitting[namespace]--
	if s.admitting[namespace] == 0 {
		delete(s.admitting, namespace)
		s.admitted.Broadcast()
	}
	s.namespaceMu.Unlock()

	return err
}

func (s *ValidatorStore) submitSessionAsync(namespace storageNamespace, req writeRequest) {
	if err := s.submitSession(namespace, req); err != nil {
		req.done(err)
	}
}

func (s *ValidatorStore) submitSessionAndWait(
	ctx context.Context,
	namespace storageNamespace,
	apply func(*pebble.Batch) error,
) error {
	result := make(chan error, 1)
	err := s.submitSession(namespace, writeRequest{
		apply:      apply,
		durability: restartRecoverable,
		done: func(err error) {
			result <- err
		},
	})
	if err != nil {
		return err
	}

	select {
	case err = <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) submitAndWait(ctx context.Context, apply func(*pebble.Batch) error) error {
	result := make(chan error, 1)
	err := s.submit(writeRequest{
		apply: apply,
		done: func(err error) {
			result <- err
		},
	})
	if err != nil {
		return err
	}

	select {
	case err = <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (s *Store) runWriter() {
	defer close(s.writerDone)

	var carried writeRequest
	hasCarried := false
	queueOpen := true
	for hasCarried || queueOpen {
		var first writeRequest
		if hasCarried {
			first = carried
			hasCarried = false
		} else {
			var open bool
			first, open = <-s.queue
			if !open {
				return
			}
		}

		requests := make([]writeRequest, 0, maxWriteBatch)
		requests = append(requests, first)
		batchBytes := first.sizeHint
	drain:
		for len(requests) < maxWriteBatch {
			select {
			case req, ok := <-s.queue:
				if !ok {
					queueOpen = false

					break drain
				}
				if !writeBatchCanAppend(batchBytes, first.durability, req) {
					carried = req
					hasCarried = true

					break drain
				}
				requests = append(requests, req)
				batchBytes += req.sizeHint
			default:
				break drain
			}
		}

		s.commitRequests(requests)
	}
}

// writeBatchCanAppend bounds one batch by bytes and by durability class. The
// first request of a batch is appended unconditionally by the caller, so an
// oversized single write still goes through — the cap only stops it from being
// joined by others.
//
// The class check is what partitions the two completions: the callbacks of one
// batch either wait for a write-ahead log fsync or they do not, so a payload
// never joins a commitment batch to wait for its fsync. A request of the other
// class is carried to the next batch rather than dropped or downgraded, which
// keeps coalescing intact WITHIN each class — the group-commit property the
// single queue exists for, where small writes ride one fsync.
func writeBatchCanAppend(batchBytes int, class durabilityClass, request writeRequest) bool {
	if request.durability != class {
		return false
	}

	return batchBytes < maxWriteBatchBytes && request.sizeHint <= maxWriteBatchBytes-batchBytes
}

func (s *Store) commitRequests(requests []writeRequest) {
	batch := s.db.NewIndexedBatch()
	results := make([]error, len(requests))
	var fatalErr error
	for i := range requests {
		err := requests[i].apply(batch)
		if err == nil {
			continue
		}

		var rejected *requestError
		if errors.As(err, &rejected) {
			results[i] = rejected.err

			continue
		}

		fatalErr = err

		break
	}

	// Every batch commits without waiting for the disk, so the fsync of a
	// commitment never holds the writer and the requests queued behind it. A
	// failed commit is fatal in either class.
	if fatalErr == nil && !batch.Empty() {
		fatalErr = batch.Commit(pebble.NoSync)
	}
	closeErr := batch.Close()
	if fatalErr == nil {
		fatalErr = closeErr
	} else if closeErr != nil {
		fatalErr = errors.Join(fatalErr, closeErr)
	}

	// Callbacks may synchronously submit more storage work. Running them away
	// from the sole writer lets that reentrant work drain from the bounded
	// queue. Close joins these goroutines before releasing Pebble resources.
	s.callbackWG.Add(1)
	go func() {
		defer s.callbackWG.Done()

		// One batch, one durability class: writeBatchCanAppend refuses to mix them,
		// so the first request's class is the batch's. A commitment completes only
		// after the write-ahead log is fsynced past it: the empty synced record
		// flushes and syncs every write committed before it — this batch and, for
		// a commitment that found its record already committed and wrote nothing,
		// the earlier batch that wrote it. A failed fsync does not return here:
		// open.go leaves pebble.Options.Logger unset, and the default logger's
		// Fatalf exits the process on it exactly as on a synced commit.
		if fatalErr == nil && requests[0].durability == durableCommitment {
			fatalErr = s.db.LogData(nil, pebble.Sync)
		}
		if fatalErr != nil {
			for i := range results {
				if results[i] == nil {
					results[i] = fatalErr
				}
			}
		}

		for i := range requests {
			requests[i].done(results[i])
			s.outstanding.Add(-1)
		}
	}()
}

// acquireRead keeps the database open until releaseRead. Close stores isClosed
// before it takes readMu exclusively to release Pebble, so a reader holding
// readMu that still sees the store open reads an open database.
func (s *Store) acquireRead() error {
	s.readMu.RLock()

	if s.isClosed.Load() {
		s.readMu.RUnlock()

		return validator.ErrStorageClosed
	}

	return nil
}

func (s *Store) releaseRead() {
	s.readMu.RUnlock()
}

func getBatchCopy(batch *pebble.Batch, key []byte) ([]byte, error) {
	value, closer, err := batch.Get(key)
	if err != nil {
		return nil, err
	}
	copyValue := append([]byte(nil), value...)
	if err = closer.Close(); err != nil {
		return nil, err
	}

	return copyValue, nil
}

func ensureSession(
	batch *pebble.Batch,
	id validator.SessionStorageID,
	namespace storageNamespace,
) (sessionSummary, error) {
	key := sessionKey(namespace)
	stored, err := getBatchCopy(batch, key)
	if errors.Is(err, pebble.ErrNotFound) {
		if err = batch.Set(key, encodeSessionID(id), nil); err != nil {
			return sessionSummary{}, fmt.Errorf("validator pebblestore: save session descriptor: %w", err)
		}
		summary := sessionSummary{}
		if err = saveSessionSummary(batch, namespace, summary); err != nil {
			return sessionSummary{}, err
		}

		return summary, nil
	}
	if err != nil {
		return sessionSummary{}, fmt.Errorf("validator pebblestore: read session descriptor: %w", err)
	}
	if !bytes.Equal(stored, encodeSessionID(id)) {
		storedID, decodeErr := decodeSessionID(stored)
		if decodeErr != nil {
			return sessionSummary{}, rejectRequest(decodeErr)
		}

		return sessionSummary{}, rejectRequest(fmt.Errorf(
			"%w: namespace belongs to %+v",
			validator.ErrSessionConflict,
			storedID,
		))
	}

	summary, err := loadSessionSummary(batch, namespace)
	if errors.Is(err, pebble.ErrNotFound) {
		// One namespace missing its summary is a per-record integrity problem,
		// not a batch failure: unrelated sessions batched alongside it must
		// still commit. Nothing has been written for this request yet.
		return sessionSummary{}, rejectRequest(errSessionSummaryMissing)
	}
	if err != nil {
		return sessionSummary{}, fmt.Errorf("validator pebblestore: load session summary: %w", err)
	}

	return summary, nil
}
