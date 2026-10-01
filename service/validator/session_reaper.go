package validator

import (
	"context"
	"errors"
	"sync"
	"time"

	"github.com/rs/zerolog"

	corestorage "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
)

const (
	// sessionReapTimeout bounds one reap pass. The pass shares the store's write
	// FIFO with candidate writes, so it can queue behind a durable candidate;
	// giving up merely defers the pass to the next rotation.
	sessionReapTimeout = 5 * time.Second
	// sessionReapRetryDelay keeps a failing pass from re-scanning the namespace
	// index on every reconciliation.
	sessionReapRetryDelay = time.Minute
	// sessionReapMaxDeleteAttempts bounds how many passes one namespace may fail
	// to delete after storage has explicitly classified the error as permanent.
	// An untyped I/O error is never counted: a temporary write-path failure must
	// not turn into a permanent leak merely because three maintenance passes met
	// it. Giving up a classified poison namespace leaks those bytes — reported at
	// Error with the namespace named, so an operator can remove them — rather
	// than holding the lineage watermark down and rescanning it forever.
	sessionReapMaxDeleteAttempts = 3
)

// reapLineage names one durable consensus lineage: one local signing identity
// participating in one shard. The catchain seqno is what advances within a
// lineage, so it is the one field a supersede is proven by and therefore the
// one field this key excludes. The validator index is excluded too: the same
// identity routinely lands at a different index in the next validator set.
type reapLineage struct {
	shard          groups.ShardID
	localADNLID    [32]byte
	validatorKeyID [32]byte
}

func lineageOf(id SessionStorageID) reapLineage {
	return reapLineage{
		shard:          id.Shard,
		localADNLID:    id.LocalADNLID,
		validatorKeyID: id.ValidatorKeyID,
	}
}

// sessionReaper deletes durable validator namespaces the masterchain has
// superseded.
//
// # Why this exists
//
// A validator session's namespace holds its candidate wire, its finality
// markers and its vote journal. Nothing ever deleted one. ValidatorStore's
// DeleteSession had a single caller — the consensus observer — and that path
// only ever sees observer namespaces, so every validator session this node has
// ever run stayed on disk in full: on the order of a catchain generation's
// worth of candidates per rotation, per group, forever. The reference deletes
// the equivalent bytes, wiping the whole per-session RocksDB directory when a
// group is destroyed (validator-group.cpp Group::~Group → bridge.cpp
// destroy_inner → RocksDb::destroy + rmrf).
//
// # Why it is not driven by session teardown
//
// The obvious hook — delete when a session is retired — is wrong here, and the
// reference shows why. C++ drops a group both when its session id changes and
// when the shard's validator set is momentarily unreadable (GroupSlot::reconcile
// calls drop() on a null validator set), and its own source carries a FIXME
// saying the wipe "is correct when a session is superseded but not when
// NetworkState itself is destroyed with sessions still active."
//
// Our teardown paths have the same shape. sessionSupervisor retires a session
// when it leaves the desired set — and desiredSessions returns an empty set
// whenever the group snapshot is not ready or lifecycle is disabled — and also
// when its config changes in place, which reuses the very same namespace. So
// "retired" proves nothing about supersession, and deleting on it would throw
// away the vote journal of a group that is still running. That journal is the
// double-vote record; losing it is the one outcome this change must not have.
//
// # The rule
//
// A validator namespace is deleted only when the desired set proves it
// superseded: another session for the same lineage carries a strictly greater
// catchain seqno, and no live actor claims the namespace. That is C++'s own
// stated correctness condition, applied where it actually holds. It means we
// keep bytes the reference deletes in exactly the cases the reference calls a
// bug, and it leaves one bounded residue: the newest namespace of a shard this
// node stops validating outright is never proven superseded and survives. That
// is one namespace per shard per identity, against today's one per rotation
// without end.
type sessionReaper struct {
	storage ValidatorStorage
	log     *zerolog.Logger

	workerOnce   sync.Once
	workerMu     sync.Mutex
	workerCancel context.CancelFunc
	workerDone   chan struct{}
	activePass   *sessionReapPass
	passBarrier  uint64
	pending      *sessionReapRequest
	wake         chan struct{}

	// watermark is the highest desired catchain seqno a completed pass has
	// already reaped for a lineage. It keeps the namespace scan to once per
	// rotation instead of once per reconciliation.
	watermark map[reapLineage]uint32
	// deleteFailures counts consecutive failed deletions per namespace, and
	// abandoned is the set that exhausted them. Both are keyed by the namespace
	// rather than by the descriptor, for the same reason the claim set is.
	deleteFailures map[sessionNamespaceKey]int
	abandoned      map[sessionNamespaceKey]struct{}
	retryAt        time.Time
}

type sessionReapRequest struct {
	highest map[reapLineage]uint32
	claimed map[sessionNamespaceKey]struct{}
	now     time.Time
	err     error
	what    string
	barrier uint64
}

type sessionReapPass struct {
	cancel context.CancelFunc
}

// permanentSessionDeleteError is the storage boundary for errors which a retry
// cannot change. Untyped I/O errors are deliberately transient: classifying an
// error by its text is how a shared pass deadline used to turn healthy tail
// namespaces into permanent leaks.
type permanentSessionDeleteError interface {
	PermanentSessionDelete() bool
}

func newSessionReaper(storage ValidatorStorage, log *zerolog.Logger) *sessionReaper {
	return &sessionReaper{
		storage:        storage,
		log:            log,
		watermark:      make(map[reapLineage]uint32),
		deleteFailures: make(map[sessionNamespaceKey]int),
		abandoned:      make(map[sessionNamespaceKey]struct{}),
		wake:           make(chan struct{}, 1),
	}
}

// schedule publishes the newest complete reconciliation snapshot to one owned
// maintenance worker. It does no storage I/O and therefore never blocks the
// supervisor actor behind the reaper's five-second pass deadline.
func (r *sessionReaper) schedule(
	ctx context.Context,
	desired map[sessionActorID]desiredSession,
	claimed map[sessionNamespaceKey]struct{},
	now time.Time,
) {
	if r == nil || r.storage == nil || ctx.Err() != nil {
		return
	}

	r.startWorker(ctx)
	r.publish(sessionReapRequest{
		highest: highestDesiredSeqnos(desired),
		claimed: cloneSessionNamespaceSet(claimed),
		now:     now,
	}, false)
}

// scheduleFailure serializes bookkeeping with the same worker as reap passes.
// In particular, it cannot race retryAt while a storage call is outstanding.
func (r *sessionReaper) scheduleFailure(ctx context.Context, now time.Time, err error, what string) {
	if r == nil || ctx.Err() != nil {
		return
	}

	r.startWorker(ctx)
	r.publish(sessionReapRequest{now: now, err: err, what: what}, true)
}

func (r *sessionReaper) startWorker(ctx context.Context) {
	r.workerOnce.Do(func() {
		workerCtx, cancel := context.WithCancel(ctx)
		done := make(chan struct{})

		r.workerMu.Lock()
		r.workerCancel = cancel
		r.workerDone = done
		r.workerMu.Unlock()

		go r.run(workerCtx, done)
	})
}

func (r *sessionReaper) publish(request sessionReapRequest, cancelActive bool) {
	r.workerMu.Lock()
	if cancelActive {
		r.passBarrier++
	}
	request.barrier = r.passBarrier
	r.pending = &request
	var cancel context.CancelFunc
	if cancelActive && r.activePass != nil {
		cancel = r.activePass.cancel
	}
	r.workerMu.Unlock()
	if cancel != nil {
		cancel()
	}

	select {
	case r.wake <- struct{}{}:
	default:
	}
}

func (r *sessionReaper) run(ctx context.Context, done chan struct{}) {
	defer close(done)

	for {
		select {
		case <-ctx.Done():
			return
		case <-r.wake:
		}

		for {
			r.workerMu.Lock()
			request := r.pending
			r.pending = nil
			r.workerMu.Unlock()
			if request == nil {
				break
			}
			if request.err != nil {
				r.fail(request.now, request.err, request.what)

				continue
			}

			passCtx, cancel := context.WithCancel(ctx)
			pass := &sessionReapPass{cancel: cancel}
			r.workerMu.Lock()
			r.activePass = pass
			cancelImmediately := request.barrier != r.passBarrier
			r.workerMu.Unlock()
			if cancelImmediately {
				cancel()
			}

			r.reapHighest(passCtx, request.highest, request.claimed, request.now)
			cancel()
			r.workerMu.Lock()
			if r.activePass == pass {
				r.activePass = nil
			}
			r.workerMu.Unlock()
		}
	}
}

func (r *sessionReaper) close() {
	if r == nil {
		return
	}

	r.workerMu.Lock()
	cancel := r.workerCancel
	done := r.workerDone
	r.workerMu.Unlock()
	if cancel == nil {
		return
	}

	cancel()
	<-done
}

func cloneSessionNamespaceSet(source map[sessionNamespaceKey]struct{}) map[sessionNamespaceKey]struct{} {
	result := make(map[sessionNamespaceKey]struct{}, len(source))
	for key := range source {
		result[key] = struct{}{}
	}

	return result
}

func (r *sessionReaper) reapHighest(
	ctx context.Context,
	highest map[reapLineage]uint32,
	claimed map[sessionNamespaceKey]struct{},
	now time.Time,
) {
	if !r.retryAt.IsZero() && now.Before(r.retryAt) {
		return
	}

	if !r.hasNewGeneration(highest) {
		return
	}

	passCtx, cancel := context.WithTimeout(ctx, sessionReapTimeout)
	defer cancel()

	stored, err := r.storage.Sessions(passCtx)
	if err != nil {
		r.fail(now, err, "enumerate durable validator namespaces")

		return
	}

	// A lineage whose superseded namespace is still claimed has not been fully
	// reaped, so its watermark must not advance: a session whose Close failed is
	// retried on a later reconciliation, and the pass that finally frees the
	// namespace has to be allowed to scan again rather than wait a whole
	// catchain generation for the next rotation to unblock it.
	blocked := make(map[reapLineage]struct{})
	deferred := false
	completed := true
	for _, id := range stored {
		if passCtx.Err() != nil {
			completed = false

			break
		}
		if !supersededByGeneration(id, highest) {
			continue
		}
		// One namespace that cannot be keyed, claimed or deleted must not end
		// the pass: every other superseded lineage on this node depends on it
		// continuing, and abandoning them restores exactly the unbounded growth
		// this reaper removes.
		key, err := sessionNamespaceKeyOf(id)
		if err != nil {
			// The store addresses its rows through the same derivation, so this
			// descriptor names no rows anyone can read or delete. It is not
			// evidence that the lineage is unfinished.
			r.warnNamespace(id, err, "key superseded validator namespace")
			deferred = true

			continue
		}
		if _, live := claimed[key]; live {
			blocked[lineageOf(id)] = struct{}{}

			continue
		}
		if _, gone := r.abandoned[key]; gone {
			continue
		}
		if err = r.storage.DeleteSession(passCtx, id); err != nil {
			if errors.Is(err, corestorage.ErrNotFound) {
				delete(r.deleteFailures, key)

				continue
			}
			if passCtx.Err() != nil || errors.Is(err, context.Canceled) ||
				errors.Is(err, context.DeadlineExceeded) {
				completed = false
				r.warnNamespace(id, err, "delete superseded validator namespace")

				break
			}
			if isPermanentSessionDeleteError(err) {
				if r.recordDeleteFailure(key, id, err) {
					blocked[lineageOf(id)] = struct{}{}
				}
			} else {
				blocked[lineageOf(id)] = struct{}{}
				r.warnNamespace(id, err, "delete superseded validator namespace")
			}
			deferred = true

			continue
		}
		delete(r.deleteFailures, key)
		if r.log != nil {
			r.log.Info().
				Hex("session_id", id.SessionID[:]).
				Int32("workchain", id.Shard.Workchain).
				Uint64("shard", uint64(id.Shard.Shard)).
				Uint32("catchain_seqno", id.CatchainSeqno).
				Uint32("superseded_by", highest[lineageOf(id)]).
				Msg("deleted superseded validator session storage")
		}
	}

	if deferred {
		r.deferPass(now)
	} else {
		r.retryAt = time.Time{}
	}
	if !completed {
		r.deferPass(now)

		return
	}
	for lineage, seqno := range highest {
		if _, incomplete := blocked[lineage]; incomplete {
			continue
		}
		r.watermark[lineage] = seqno
	}
}

func isPermanentSessionDeleteError(err error) bool {
	var permanent permanentSessionDeleteError

	return errors.As(err, &permanent) && permanent.PermanentSessionDelete()
}

// recordDeleteFailure counts one namespace's failure and reports whether it is
// still worth retrying. A namespace that exhausts its attempts is retired
// instead, so a permanently undeletable one stops holding its lineage's
// watermark down and the index scan returns to once per rotation.
func (r *sessionReaper) recordDeleteFailure(
	key sessionNamespaceKey,
	id SessionStorageID,
	err error,
) (retry bool) {
	r.deleteFailures[key]++
	if r.deleteFailures[key] < sessionReapMaxDeleteAttempts {
		r.warnNamespace(id, err, "delete superseded validator namespace")

		return true
	}

	delete(r.deleteFailures, key)
	r.abandoned[key] = struct{}{}
	if r.log != nil {
		r.log.Error().
			Err(err).
			Hex("session_id", id.SessionID[:]).
			Int32("workchain", id.Shard.Workchain).
			Uint64("shard", uint64(id.Shard.Shard)).
			Uint32("catchain_seqno", id.CatchainSeqno).
			Int("attempts", sessionReapMaxDeleteAttempts).
			Msg("validator session storage is undeletable and is left on disk")
	}

	return false
}

func (r *sessionReaper) warnNamespace(id SessionStorageID, err error, what string) {
	if r.log == nil {
		return
	}
	r.log.Warn().
		Err(err).
		Hex("session_id", id.SessionID[:]).
		Int32("workchain", id.Shard.Workchain).
		Uint64("shard", uint64(id.Shard.Shard)).
		Uint32("catchain_seqno", id.CatchainSeqno).
		Msg("validator session reaper: " + what)
}

func (r *sessionReaper) fail(now time.Time, err error, what string) {
	if r == nil {
		return
	}
	r.deferPass(now)
	if r.log != nil {
		r.log.Warn().Err(err).Msg("validator session reaper: " + what)
	}
}

// deferPass holds the next namespace scan off for one backoff. A pass that
// could not finish its work has not consumed the generation — the watermark of
// every lineage it left work in stays where it was — so this only decides how
// soon it is tried again.
func (r *sessionReaper) deferPass(now time.Time) {
	r.retryAt = now.Add(sessionReapRetryDelay)
}

// hasNewGeneration reports whether any lineage has advanced past the last
// completed pass. A lineage first seen in this reconciliation always qualifies,
// which is what reaps namespaces a crash orphaned before the process restarted.
func (r *sessionReaper) hasNewGeneration(highest map[reapLineage]uint32) bool {
	for lineage, seqno := range highest {
		if known, seen := r.watermark[lineage]; !seen || seqno > known {
			return true
		}
	}

	return false
}

func highestDesiredSeqnos(desired map[sessionActorID]desiredSession) map[reapLineage]uint32 {
	highest := make(map[reapLineage]uint32, len(desired))
	for _, session := range desired {
		id := session.config.StorageID
		if !id.IsValidator {
			continue
		}
		lineage := lineageOf(id)
		if known, seen := highest[lineage]; !seen || id.CatchainSeqno > known {
			highest[lineage] = id.CatchainSeqno
		}
	}

	return highest
}

// supersededByGeneration is the supersede proof on its own: a validator
// namespace the desired set has moved past. It is only half of the safety
// condition — the claim check in reap is the other half — and it is separate so
// a pass can tell "nothing to do here" from "something to do here that a live
// actor is still holding".
//
// Observer namespaces are excluded because the consensus observer owns their
// lifecycle and the supervisor cannot see which of them are still desired.
func supersededByGeneration(id SessionStorageID, highest map[reapLineage]uint32) bool {
	if !id.IsValidator {
		return false
	}
	desiredSeqno, known := highest[lineageOf(id)]

	return known && desiredSeqno > id.CatchainSeqno
}
