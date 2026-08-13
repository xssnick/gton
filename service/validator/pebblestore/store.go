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

type writeRequest struct {
	apply func(*pebble.Batch) error
	done  func(error)
	// sizeHint is the approximate encoded value size. Zero marks small or
	// unknown writes, which remain bounded by maxWriteBatch.
	sizeHint int
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

// Store is one physical Pebble database and its global durable writer.
type Store struct {
	db    *pebble.DB
	cache *pebble.Cache

	validator *ValidatorStore
	collator  *CollatorStore

	queue      chan writeRequest
	writerDone chan struct{}

	stateMu   sync.Mutex
	isClosed  bool
	closeErr  error
	closeDone chan struct{}

	readMu sync.RWMutex

	outstanding atomic.Int64

	callbackWG sync.WaitGroup
}

// ValidatorStore is the stable validator persistence view of a Store.
type ValidatorStore struct {
	store *Store

	namespaceMu sync.Mutex
	deleting    map[storageNamespace]struct{}
	deleted     map[storageNamespace]struct{}

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
// candidate writes share one queue, one writer goroutine and one synced batch
// with every consensus write. That single queue is also the group-commit
// mechanism: small writes ride the fsync of a large one, so separating the two
// workloads is not free and must be measured before it is done.
func (s *Store) Collator() *CollatorStore {
	return s.collator
}

// Close rejects new work, drains every accepted write and callback, then
// closes the database and its cache. It is safe to call more than once.
func (s *Store) Close() error {
	s.stateMu.Lock()
	if s.isClosed {
		done := s.closeDone
		s.stateMu.Unlock()
		<-done

		s.stateMu.Lock()
		err := s.closeErr
		s.stateMu.Unlock()

		return err
	}

	s.isClosed = true
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
	err := s.db.Close()
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

	s.store.stateMu.Lock()
	isClosed := s.store.isClosed
	s.store.stateMu.Unlock()
	if isClosed {
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
	s.stateMu.Lock()
	defer s.stateMu.Unlock()

	if s.isClosed {
		return validator.ErrStorageClosed
	}

	// Holding stateMu across the bounded send prevents Close from closing the
	// channel between the state check and submission. The writer never takes
	// this lock, so backpressure cannot deadlock shutdown.
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
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()

	if _, isDeleting := s.deleting[namespace]; isDeleting {
		return validator.ErrSessionClosed
	}
	if _, isDeleted := s.deleted[namespace]; isDeleted {
		return validator.ErrSessionClosed
	}

	return s.store.submit(req)
}

func (s *ValidatorStore) submitSessionContext(
	ctx context.Context,
	namespace storageNamespace,
	req writeRequest,
) error {
	s.namespaceMu.Lock()
	defer s.namespaceMu.Unlock()

	if _, isDeleting := s.deleting[namespace]; isDeleting {
		return validator.ErrSessionClosed
	}
	if _, isDeleted := s.deleted[namespace]; isDeleted {
		return validator.ErrSessionClosed
	}

	return s.store.submitContext(ctx, req)
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
				if !writeBatchCanAppend(batchBytes, req) {
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

// writeBatchCanAppend bounds one batch by bytes. The first request of a batch
// is appended unconditionally by the caller, so an oversized single write still
// goes through — the cap only stops it from being joined by others.
func writeBatchCanAppend(batchBytes int, request writeRequest) bool {
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

	if fatalErr == nil && !batch.Empty() {
		fatalErr = batch.Commit(pebble.Sync)
	}
	closeErr := batch.Close()
	if fatalErr == nil {
		fatalErr = closeErr
	} else if closeErr != nil {
		fatalErr = errors.Join(fatalErr, closeErr)
	}
	if fatalErr != nil {
		for i := range results {
			if results[i] == nil {
				results[i] = fatalErr
			}
		}
	}

	// Callbacks may synchronously submit more storage work. Running them away
	// from the sole writer lets that reentrant work drain from the bounded
	// queue. Close joins these goroutines before releasing Pebble resources.
	s.callbackWG.Add(1)
	go func() {
		defer s.callbackWG.Done()

		for i := range requests {
			requests[i].done(results[i])
			s.outstanding.Add(-1)
		}
	}()
}

func (s *Store) acquireRead() error {
	s.readMu.RLock()

	s.stateMu.Lock()
	isClosed := s.isClosed
	s.stateMu.Unlock()
	if isClosed {
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
