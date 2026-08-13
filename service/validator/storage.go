package validator

import (
	"context"
	"crypto/sha256"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"time"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
	"github.com/xssnick/tonutils-go/tl"
)

const consensusDBIDSchema = "consensus.dbId session_id:int256 is_validator:Bool validator_key:int256 adnl_id:int256 = consensus.DbId"

var consensusDBIDConstructorID = tl.CRC(consensusDBIDSchema)

var (
	// ErrStorageClosed reports an operation submitted after validator storage
	// shutdown began.
	ErrStorageClosed = errors.New("validator storage: closed")
	// ErrSessionClosed reports an operation through a retired session handle
	// or while that session namespace is being deleted.
	ErrSessionClosed = errors.New("validator storage: session closed")
	// ErrSessionConflict reports that a namespace is already bound to a
	// different complete session descriptor.
	ErrSessionConflict = errors.New("validator storage: session descriptor conflict")
	// ErrCandidateConflict reports a second wire representation for an already
	// stored candidate ID.
	ErrCandidateConflict = errors.New("validator storage: candidate conflict")
	// ErrDelegationConflict reports a second collator authority for one local
	// leader window. Once stored, an authorization is immutable until the
	// session namespace is retired.
	ErrDelegationConflict = errors.New("validator storage: delegation authorization conflict")
)

// SessionStorageID is the complete durable identity of one local consensus
// runtime. Namespace is the consensus.dbId hash; the remaining fields
// are persisted in its descriptor and prevent a namespace from being reused
// for a different shard, catchain rotation, validator index, or protocol.
type SessionStorageID struct {
	SessionID      [32]byte
	Shard          groups.ShardID
	CatchainSeqno  uint32
	IsValidator    bool
	ValidatorKeyID [32]byte
	LocalADNLID    [32]byte
	ValidatorIndex int
	// Protocol is persisted only in the namespace descriptor. It is excluded
	// from Namespace to match consensus.dbId, but prevents a cold restart from
	// interpreting recovered window progress with different critical config.
	Protocol SessionProtocol
}

// Validate checks the role-specific descriptor invariants before it is used
// as a durable namespace.
func (id SessionStorageID) Validate() error {
	if id.SessionID == ([32]byte{}) {
		return errors.New("validator storage: session id is zero")
	}
	if !id.Shard.IsValid() {
		return fmt.Errorf("validator storage: invalid shard %d:%d", id.Shard.Workchain, id.Shard.Shard)
	}
	if id.LocalADNLID == ([32]byte{}) {
		return errors.New("validator storage: local adnl id is zero")
	}
	if id.ValidatorIndex < simplex.ObserverIndex || id.ValidatorIndex > math.MaxInt32 {
		return fmt.Errorf("validator storage: validator index %d is invalid", id.ValidatorIndex)
	}

	if id.IsValidator {
		if id.ValidatorIndex == simplex.ObserverIndex {
			return errors.New("validator storage: validator has observer index")
		}
		if id.ValidatorKeyID == ([32]byte{}) {
			return errors.New("validator storage: validator key id is zero")
		}

		return nil
	}

	if id.ValidatorIndex != simplex.ObserverIndex {
		return fmt.Errorf("validator storage: observer index is %d, want %d", id.ValidatorIndex, simplex.ObserverIndex)
	}
	if id.ValidatorKeyID != ([32]byte{}) {
		return errors.New("validator storage: observer has validator key id")
	}

	return nil
}

// Namespace returns the consensus.dbId boxed-TL SHA-256 hash. Shard and
// catchain identity are deliberately descriptor fields rather than part of the
// hash: canonically they live outside dbId, in the containing database path.
func (id SessionStorageID) Namespace() ([32]byte, error) {
	if err := id.Validate(); err != nil {
		return [32]byte{}, err
	}

	var wire [104]byte
	binary.LittleEndian.PutUint32(wire[0:4], consensusDBIDConstructorID)
	copy(wire[4:36], id.SessionID[:])

	encoded := tl.AppendBool(wire[:36], id.IsValidator)
	encoded = encoded[:len(wire)]
	copy(encoded[40:72], id.ValidatorKeyID[:])
	copy(encoded[72:104], id.LocalADNLID[:])

	return sha256.Sum256(encoded), nil
}

// CandidateRecord is the canonical, opaque candidate wire representation.
// SaveCandidate snapshots Wire before returning, so callers retain their
// buffer; the slice returned by Candidate is independently owned by its caller.
type CandidateRecord struct {
	ID   simplex.CandidateID
	Wire []byte
}

// LeaderWindowRecord is durable telemetry for one locally observed leader
// window.
type LeaderWindowRecord struct {
	Base       simplex.ParentID
	StartSlot  uint32
	EndSlot    uint32
	ObservedAt time.Time
}

// DelegationAuthorization is the durable outbound authority selected for a
// future local leader window. The exact signature fences every retry of that
// window to the same collator and prevents a fallback to self production.
type DelegationAuthorization struct {
	StartSlot uint32
	Collator  [32]byte
	Signature []byte
}

// StoredSessionState contains metadata needed to restore resolvers outside the
// Simplex journal. Entries are returned in key order; candidate payloads stay
// lazy and are fetched individually through Candidate, keeping the resolver's
// restart path bounded.
// Leader windows are deliberately absent: they are durable telemetry that no
// recovering runtime reads, and StoredSession reports their count.
type StoredSessionState struct {
	ID           SessionStorageID
	CandidateIDs []simplex.CandidateID
	Finalized    []simplex.CandidateID
}

// StoredSession is the status projection of one durable session namespace.
type StoredSession struct {
	ID                      SessionStorageID
	Candidates              uint64
	Finalized               uint64
	Votes                   uint64
	Certificates            uint64
	LeaderWindows           uint64
	FirstNonAnnouncedWindow uint32
	LastFinalized           *simplex.CandidateID
	LastLeaderWindow        *LeaderWindowRecord
}

// StorageStatus reports durable session telemetry and database pressure.
type StorageStatus struct {
	Sessions      []StoredSession
	DB            collator.DBMetrics
	PendingWrites int
}

// awaitStorageWrite submits one asynchronous storage write and waits for its
// completion or for ctx, whichever happens first. The result channel is
// buffered, so a completion that arrives after the wait was abandoned never
// blocks the store's callback goroutine.
//
// Abandoning the wait does not cancel the write: the record may still commit.
// A ctx error therefore means "outcome unknown", not "not written", and
// callers must not derive durable in-memory state from it. It exists so that a
// shutdown is bounded by the caller's own context instead of by an
// implementation promising to always invoke done.
func awaitStorageWrite(ctx context.Context, submit func(done func(error))) error {
	result := make(chan error, 1)
	submit(func(err error) { result <- err })

	select {
	case err := <-result:
		return err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// ValidatorStorage is the complete persistence contract consumed by the
// validator workflow. Lifecycle ownership intentionally remains on the
// concrete store at the composition root. Implementations must invoke every
// done callback exactly once on every path, including rejection before any
// work is queued.
type ValidatorStorage interface {
	Journal(session SessionStorageID, maxSignatures int) simplex.Journal
	// Sessions returns the complete durable namespace identities in key order
	// without loading candidate or telemetry payloads.
	Sessions(context.Context) ([]SessionStorageID, error)
	SaveCandidate(session SessionStorageID, candidate CandidateRecord, done func(error))
	Candidate(ctx context.Context, session SessionStorageID, id simplex.CandidateID) (CandidateRecord, error)
	// MarkFinalized writes one accepted finality marker. The resolver owns the
	// acceptance-before-persist ordering and recursively marks every finalized
	// parent, including empty candidates.
	MarkFinalized(session SessionStorageID, id simplex.CandidateID, done func(error))
	RecordLeaderWindow(session SessionStorageID, window LeaderWindowRecord, done func(error))
	LoadSession(ctx context.Context, session SessionStorageID) (StoredSessionState, error)
	DeleteSession(ctx context.Context, session SessionStorageID) error
	Status(ctx context.Context) (StorageStatus, error)
}

// DelegationAuthorizationStorage is the validator-side authority fence used
// only when this validator delegates future leader windows to a remote
// collator. Self-only and observer runtimes do not depend on it.
type DelegationAuthorizationStorage interface {
	// SaveDelegationAuthorization returns from the call after the write has
	// either entered the existing durable FIFO or its callback has received an
	// admission error. The context bounds queue admission, not an already
	// accepted commit.
	SaveDelegationAuthorization(
		ctx context.Context,
		session SessionStorageID,
		authorization DelegationAuthorization,
		done func(error),
	)
	DelegationAuthorization(
		ctx context.Context,
		session SessionStorageID,
		startSlot uint32,
	) (DelegationAuthorization, error)
}
