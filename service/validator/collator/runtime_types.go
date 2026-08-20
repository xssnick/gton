package collator

import (
	"context"
	"crypto/ed25519"
	"errors"
	"time"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/simplex"
)

var (
	// ErrNotStarted reports an operation submitted before Start.
	ErrNotStarted = errors.New("collator runtime: not started")
	// ErrUnavailable reports a temporary failure of the remote collator
	// transport. It is distinct from local pipeline or storage failures: a
	// validator may skip production for that window and keep participating in
	// consensus.
	ErrUnavailable = errors.New("collator runtime: unavailable")
	// ErrClosed reports an operation submitted after shutdown began.
	ErrClosed = errors.New("collator runtime: closed")
	// ErrSessionRetired reports an operation against a completed session
	// generation. PrepareSession may later admit the same deterministic ID as a
	// new generation.
	ErrSessionRetired = errors.New("collator runtime: session retired")
	// ErrSessionUnavailable reports a session whose opaque pipeline state may
	// no longer match its durable state. Only retirement or service shutdown is
	// safe after this error.
	ErrSessionUnavailable = errors.New("collator runtime: session unavailable")
	// ErrAlreadyDelegated reports a second final delegation for one window.
	ErrAlreadyDelegated = errors.New("collator runtime: window already delegated")
	// ErrUnauthorized reports an unrecognized or locally disallowed validator.
	ErrUnauthorized = errors.New("collator runtime: validator is not authorized")
	// ErrStaleWindow reports a delegation older than the observed window.
	ErrStaleWindow = errors.New("collator runtime: stale window")
	// ErrWindowTooFar reports a delegation outside the protocol horizon.
	ErrWindowTooFar = errors.New("collator runtime: window is too far in the future")
)

// ProductionMode fixes the authority model for one collator service. It is a
// construction-time policy: a running service never falls back from delegated
// production to self production, or the other way around.
type ProductionMode uint8

const (
	ProductionModeSelf ProductionMode = iota + 1
	ProductionModeDelegated
)

// CandidateAuthority is persisted with every anti-equivocation marker. It is
// explicit because a missing delegation is valid only for self production,
// never an alternate encoding of delegated authority.
type CandidateAuthority uint8

const (
	CandidateAuthoritySelf CandidateAuthority = iota + 1
	CandidateAuthorityDelegated
)

// WindowPreparation is the protocol-neutral view of a
// consensus.pleaseCollatePrepare probe. A successful probe reserves no state.
type WindowPreparation struct {
	SessionID  [32]byte
	SourceADNL [32]byte
	StartSlot  uint32
}

// WindowRequest combines the exact consensus.pleaseCollate wire payload with
// the authenticated query source and local Simplex chain position that are
// not carried by that message.
type WindowRequest struct {
	SessionID     [32]byte
	SourceADNL    [32]byte
	PleaseCollate simplex.ConsensusPleaseCollate
}

// SelfWindowRequest activates one local-validator leader window in memory.
// Session progress is persisted separately before production becomes eligible;
// Deadline prevents a late session-WAL completion from starting an expired
// window. Signer is the validator session key used for the actual non-delegated
// consensus candidate.
type SelfWindowRequest struct {
	SessionID [32]byte
	StartSlot uint32
	Deadline  time.Time
	Signer    simplex.Signer
}

// ID returns the storage identity of the requested window.
func (r WindowRequest) ID() WindowID {
	return WindowID{SessionID: r.SessionID, StartSlot: uint32(r.PleaseCollate.WindowStartSlot)}
}

// CandidateArtifact is the in-process result delivered to a validator. The
// artifact and every byte slice reachable from it are immutable after
// publication; an emitter may retain or share them after returning.
type CandidateArtifact struct {
	SessionID    [32]byte
	WindowID     WindowID
	Candidate    simplex.Candidate
	BlockBOC     []byte
	CollatedData []byte

	prepared *simplex.PreparedCandidate

	// digested records that Candidate.Block.FileHash and
	// Candidate.CollatedFileHash are the sha256 of BlockBOC and CollatedData as
	// they stand here. It is set where that has just been established — by the
	// builder that took both digests of these buffers, or by the resume path
	// that compared them against the persisted marker — and it is unexported so
	// an artifact assembled outside this package cannot assert it.
	digested bool

	// blockRoot and collatedRoots are a borrowed, validation-call-scoped handoff
	// from the network decoder. They never leave prepareValidationCandidate.
	blockRoot     *cell.Cell
	collatedRoots []*cell.Cell
}

// Prepared is the broadcast payload this candidate was built with, or nil for
// an artifact that did not come from a local build — an empty candidate, one
// restored from storage, or one a Pipeline supplied itself. A caller that gets
// nil serializes from BlockBOC and CollatedData instead.
//
// The field behind it is unexported so that only the canonical build path can
// claim a candidate's payload was compressed from its own roots.
func (a CandidateArtifact) Prepared() *simplex.PreparedCandidate {
	return a.prepared
}

// Digested reports that this artifact's two file hashes are already known to be
// the digests of the two payloads beside them, so a consumer that would
// otherwise re-derive them to check has nothing to learn from doing so.
//
// False is the safe answer and the answer every artifact this package did not
// itself digest gives.
func (a CandidateArtifact) Digested() bool {
	return a.digested
}

// FinalizedAnchorState is the immutable exact state a validator resolver has
// already loaded for a finalized candidate. It is optional because remote
// collators do not receive node-resident cells. BlockBOC is absent only for a
// zerostate. The local acquisition pipeline validates the BOC, state root,
// and their Merkle-update edge before using this handoff.
type FinalizedAnchorState struct {
	Block    ton.BlockIDExt
	BlockBOC []byte
	State    *cell.Cell
}

// ConsensusProgress is one authenticated observer transition delivered to a
// standalone collator. Candidates are the exact oldest-to-newest lineage
// needed to materialize Window.Base locally. FinalizedAnchor is the newest
// candidate in that lineage whose exact state is already available from node
// storage; FinalizedAnchorState carries that already-loaded state for the
// in-process path. It can be newer than the masterchain-confirmed block in
// the collator's asynchronous SessionUpdate. StartAt is resolved from the
// lineage and includes the parent gen_utime_exact clamp;
// Window.ObservedAt is only the observation time.
type ConsensusProgress struct {
	SessionID            [32]byte
	Window               simplex.Window
	StartAt              time.Time
	FinalizedAnchor      *ton.BlockIDExt
	FinalizedAnchorState *FinalizedAnchorState
	Candidates           []CandidateArtifact
}

// SoftTimeoutAction is the explicit producer decision at
// slotStart+TargetRate while the hard collation future is still running.
type SoftTimeoutAction uint8

const (
	// SoftTimeoutWait keeps awaiting the same future for the same slot. This
	// is the required behavior while empty candidates are forbidden.
	SoftTimeoutWait SoftTimeoutAction = iota + 1
	// SoftTimeoutEmitEmpty emits an empty candidate and reuses the same build
	// future in the next slot.
	SoftTimeoutEmitEmpty
)

// SoftTimeoutDecision carries the authenticated block referenced by an empty
// candidate. Block is ignored for SoftTimeoutWait.
type SoftTimeoutDecision struct {
	Action SoftTimeoutAction
	Block  ton.BlockIDExt
}

// BuildRequest is the sequential input passed to the authenticated
// acquisition/build pipeline. Previous is nil for the first slot and points
// to the already durable preceding artifact otherwise. FinalizedAnchor is set
// only while restoring the first candidate of a finalized consensus lineage.
// FinalizedAnchorState, when present, is the already verified resolver state
// for that anchor and avoids a second node-storage read. Both pointers are
// borrowed immutable views valid only for the pipeline call.
type BuildRequest struct {
	Session              ActivatedSession
	Update               SessionUpdate
	Slot                 uint32
	Leader               uint32
	Parent               simplex.ParentID
	Previous             *CandidateArtifact
	FinalizedAnchor      *ton.BlockIDExt
	FinalizedAnchorState *FinalizedAnchorState
	// ExternalWaitUntil is the shardchain slot boundary. A local shard collator
	// may consume ingress-admitted external messages until this instant;
	// masterchain and restore requests leave it zero.
	ExternalWaitUntil time.Time
	// ExternalProcessUntil is the reference collator's external phase timeout:
	// slot start for shardchain and three quarters of one target rate after slot
	// start for masterchain. Restore requests leave it zero.
	ExternalProcessUntil time.Time
	// BuildSoftDeadline is the instant awaitBuildUntil gives up waiting for this
	// build. It is carried in so the collation can size its own internal
	// budgets — today only out-queue cleanup — the way the reference collator
	// derives them from params_.soft_timeout. Restore requests leave it zero,
	// which leaves those budgets inert.
	BuildSoftDeadline time.Time
}

// CandidateCommit publishes one already durable candidate transition to the
// acquisition pipeline. Built carries the in-memory state root while Artifact
// binds it to the signed consensus candidate used by subsequent windows. The
// pipeline may retain immutable State, StateUpdate, and StorageStats cells;
// byte slices remain Service-owned and must not be retained.
type CandidateCommit struct {
	Request  BuildRequest
	Built    *Candidate
	Artifact CandidateArtifact
}

// SoftTimeoutRequest distinguishes the build future being kept alive from the
// slot that currently needs a timeout decision. They differ after an empty
// candidate advances the window while reusing the original future.
type SoftTimeoutRequest struct {
	Active  BuildRequest
	Current BuildRequest
}

// CandidateState is the normal chain state on which one candidate would be
// built. It is resolved before starting the expensive collation future so the
// producer can apply the protocol's EmptyBlockPolicy without doing work that
// must be discarded. Block is also the exact block referenced by an empty
// candidate.
type CandidateState struct {
	Block       ton.BlockIDExt
	NextSeqno   uint32
	BeforeSplit bool
}

// EmitCandidate publishes an immutable artifact after its storage write
// completed. Implementations may retain or share the artifact and must
// tolerate at-least-once delivery across retries.
type EmitCandidate func(context.Context, CandidateArtifact) error

// Pipeline owns authenticated input acquisition and candidate construction.
// Lifecycle calls for a session are serialized by Service; BuildCandidate
// calls for independent sessions may run concurrently. SoftTimeout receives
// both the active BuildCandidate request and the current slot request while
// that build is still running; they differ when an empty candidate reuses the
// prior build future. Implementations must keep that decision path
// concurrency-safe and must not mutate build-owned state. PrepareSession
// creates a query-ready tentative session. ActivateSession binds its chain
// anchors exactly once; no build is issued before it succeeds. PrepareSession,
// ActivateSession, UpdateSession, and RetireSession must be atomic from the
// caller's view: an error must leave the previous pipeline state usable and
// unchanged. RestoreCandidate is idempotent per candidate. An error may retain
// an exact successfully restored prefix, but retrying the same ordered lineage
// must resume deterministically. These properties are required because the
// service cannot roll back opaque acquisition state.
// RetireSession is additionally idempotent so bounded Close can retry cleanup.
// A successful BuildCandidate transfers ownership of the returned Candidate
// and all of its byte slices to Service; the pipeline must not retain or
// mutate them after returning. CommitCandidate later permits retaining the
// immutable cell-backed state named by CandidateCommit, but never its byte
// slices. On build error, ownership remains with the pipeline.
type Pipeline interface {
	PrepareSession(context.Context, Session, SessionUpdate) error
	ActivateSession(context.Context, SessionActivation, SessionUpdate) error
	UpdateSession(context.Context, Session, SessionUpdate) error
	ResolveCandidateState(context.Context, BuildRequest) (CandidateState, error)
	BuildCandidate(context.Context, BuildRequest) (*Candidate, error)
	RestoreCandidate(context.Context, BuildRequest, CandidateArtifact) error
	CommitCandidate(context.Context, CandidateCommit) error
	SoftTimeout(context.Context, SoftTimeoutRequest) (SoftTimeoutDecision, error)
	RetireSession(context.Context, [32]byte) error
}

// SigningKeys keeps private key material behind the key owner. KeyIDs is the
// explicit lookup surface; PublicKeyFor and Sign resolve one selected key.
type SigningKeys interface {
	KeyIDs() [][32]byte
	PublicKeyFor([32]byte) (ed25519.PublicKey, error)
	Sign([32]byte, []byte) ([]byte, error)
}

// Collator is the transport-independent validator-facing collation boundary.
// Service executes it in-process, while RemoteCollator translates the same
// operations to the delegated-collation client transport.
type Collator interface {
	CollatorID() [32]byte
	Start(context.Context) error
	Close(context.Context) error
	Session(context.Context, [32]byte) (SessionRecord, error)
	PrepareSession(context.Context, Session, SessionUpdate) error
	ActivateSession(context.Context, SessionActivation) error
	UpdateSession(context.Context, SessionUpdate) error
	RetireSession(context.Context, [32]byte) error
	Probe(context.Context, WindowPreparation) error
	CommitDelegation(context.Context, WindowRequest) error
	Status(context.Context) (Status, error)
}

// RemoteCollatorTransport is the wire/control-plane boundary used by a
// validator connected to a standalone collator. It deliberately exposes the
// exact authenticated delegated-collation query payloads while keeping transport,
// reconnect, framing, and remote status protocols outside this package.
// Implementations must wrap only failures to deliver a request or receive its
// response with ErrUnavailable. Authenticated remote domain verdicts and
// server-side failures must be returned without ErrUnavailable because the
// validator treats that sentinel as advisory for delegated production.
type RemoteCollatorTransport interface {
	CollatorID() [32]byte
	Start(context.Context) error
	Close(context.Context) error
	Probe(context.Context, AuthenticatedQuery, simplex.ConsensusPleaseCollatePrepare) error
	Commit(context.Context, AuthenticatedQuery, simplex.ConsensusPleaseCollate) error
}

// AuthenticatedQuery identifies the private overlay and authenticated ADNL
// peer from which a collator query arrived. Neither field is present in the
// pleaseCollate TL payload: the consensus observer network must derive them from its local
// overlay routing and authenticated ADNL callback, never from peer data.
type AuthenticatedQuery struct {
	SessionID  [32]byte
	SourceADNL [32]byte
}

// RemoteHandlers is the server-side delegated-collation wire endpoint table. Session
// lifecycle is deliberately absent: it is a trusted local control-plane
// operation, not an ADNL query understood by other nodes.
type RemoteHandlers struct {
	Probe  func(context.Context, AuthenticatedQuery, simplex.ConsensusPleaseCollatePrepare) error
	Commit func(context.Context, AuthenticatedQuery, simplex.ConsensusPleaseCollate) error
}
