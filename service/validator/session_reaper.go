package validator

import (
	"context"
	"errors"
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
	// to delete before the reaper stops offering it. DeleteSession refuses a
	// namespace whose summary row is missing or undecodable, and no later pass
	// will make that row appear, so an unbounded retry would hold this lineage's
	// watermark down forever and rescan the whole index once per backoff for as
	// long as the node runs. Giving up leaks those bytes — reported at Error
	// with the namespace named, so it is a leak an operator can see and remove,
	// rather than the silent unbounded one this reaper exists to end.
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

func newSessionReaper(storage ValidatorStorage, log *zerolog.Logger) *sessionReaper {
	return &sessionReaper{
		storage:        storage,
		log:            log,
		watermark:      make(map[reapLineage]uint32),
		deleteFailures: make(map[sessionNamespaceKey]int),
		abandoned:      make(map[sessionNamespaceKey]struct{}),
	}
}

// reap runs at most one namespace pass. desired supplies the supersede proof;
// claimed is every namespace a live actor still owns, which is never deleted
// even when a newer generation exists, because a session that has not finished
// closing is still writing.
//
// claimed is keyed by the namespace, not by the descriptor: liveness has to be
// proven on the same identity the rows are addressed by, or a namespace whose
// stored descriptor differs from the desired one in a field no derivation reads
// looks unclaimed and its live vote journal is deleted.
func (r *sessionReaper) reap(
	ctx context.Context,
	desired map[sessionActorID]desiredSession,
	claimed map[sessionNamespaceKey]struct{},
	now time.Time,
) {
	if r == nil || r.storage == nil || ctx.Err() != nil {
		return
	}
	if !r.retryAt.IsZero() && now.Before(r.retryAt) {
		return
	}

	highest := highestDesiredSeqnos(desired)
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
	for _, id := range stored {
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
			if r.recordDeleteFailure(key, id, err) {
				blocked[lineageOf(id)] = struct{}{}
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
	for lineage, seqno := range highest {
		if _, incomplete := blocked[lineage]; incomplete {
			continue
		}
		r.watermark[lineage] = seqno
	}
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
