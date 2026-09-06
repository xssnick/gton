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

	// blockRoot and collatedRoots are a borrowed handoff from whoever already
	// held this candidate's parsed payload: the network decoder for a received
	// candidate, this node's own builder for one it produced. On the way in they
	// never leave prepareValidationCandidate; on the way out they are lent to the
	// validation or observer state preparation the emission feeds. They pin the
	// block DAG and the whole collated proof set. Every holder that
	// keeps an artifact past that call must therefore keep what retained()
	// returns instead of the artifact it was handed.
	blockRoot     *cell.Cell
	collatedRoots []*cell.Cell
	// builtSuccessor is the same one-use handoff for a standalone observer,
	// which need not reconstruct the state its own builder just produced.
	builtSuccessor LiveSuccessorState

	// generationTimeMS is the exact timestamp serialized into the consensus
	// extra root. The known bit is provenance: only the sealed builder path can
	// set it, so an external Pipeline cannot attach an unrelated scalar to
	// mutable CollatedData.
	generationTimeMS    uint64
	generationTimeKnown bool
}

// retained is the form of this artifact a holder keeps past the call it was
// handed to: the producer's own lineage pointer into the next slot, the window's
// memory of what it emitted, or anything else that outlives one validation.
//
// It drops the borrowed roots, which are the whole block cell DAG and the entire
// collated proof set — the two largest objects a slot produces, held for as long
// as the holder lives. Every reader downstream of such a holder parses the two
// BOCs beside them anyway, so nothing but the memory is lost.
func (a CandidateArtifact) retained() CandidateArtifact {
	a.blockRoot = nil
	a.collatedRoots = nil
	a.builtSuccessor = LiveSuccessorState{}

	return a
}

// ValidationRoots is the parsed payload of a candidate this process already
// decoded or built, or nil for one that must be parsed from its bytes.
//
// The two BOC fields beside them are the serialization of these very roots and
// are pinned to them by the candidate's two file hashes, which is what lets a
// validation handed both skip decoding either. Fields, and therefore this
// accessor, are unexported-backed for the same reason prepared is: nothing
// outside this package can claim a root belongs to bytes it does not.
func (a CandidateArtifact) ValidationRoots() (*cell.Cell, []*cell.Cell) {
	return a.blockRoot, a.collatedRoots
}

// BuiltSuccessor returns this process's sealed build result. It opens only for
// the exact predecessor trees the builder used; consensus must still certify
// the candidate before this state can become a selected parent.
func (a CandidateArtifact) BuiltSuccessor() LiveSuccessorState {
	return a.builtSuccessor
}

// GenerationTimeMS returns the exact millisecond timestamp carried by this
// process's sealed build. False means the artifact crossed a public Pipeline
// boundary and its CollatedData must be decoded before the value can be used.
func (a CandidateArtifact) GenerationTimeMS() (uint64, bool) {
	return a.generationTimeMS, a.generationTimeKnown
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

// ConsensusProgress is one authenticated observer transition delivered to a
// standalone collator. Base is the resolver-owned exact state selected by
// Window.Base. It is absent only for consensus genesis. StartAt includes the
// selected parent's gen_utime_exact clamp; Window.ObservedAt is only the
// observation time.
type ConsensusProgress struct {
	SessionID [32]byte
	Window    simplex.Window
	StartAt   time.Time
	Base      *SelectedBaseState
}

// ConsensusBaseUpdate atomically installs the exact state selected by
// Update.CurrentBase and advances the pipeline's live session view. Base is
// nil exactly while CurrentBase is consensus genesis.
type ConsensusBaseUpdate struct {
	Session ActivatedSession
	Update  SessionUpdate
	Base    *SelectedBaseState
}

// SpeculativeWindowRequest opens the first slot of the leader window that is
// about to start, before consensus has observed it.
//
// The bet it places is that the window will open on Base: a candidate this node
// has already validated, and whose notarization certificate is the very event
// that will open the window. Nothing about the session is advanced by placing
// it — the acquisition keeps the window it is in, and the candidate this build
// produces is held until the real window arrives and names the same base. A bet
// that loses costs the CPU of one collation and nothing else.
//
// StartAt is the instant the runtime expects the window to start, computed by
// the same rule the observed window uses. It only stamps the block's time; the
// producer's schedule still comes from the observed window.
type SpeculativeWindowRequest struct {
	SessionID [32]byte
	StartSlot uint32
	Leader    uint32
	Base      *SelectedBaseState
	StartAt   time.Time
	// Deadline bounds the speculative build the way a window deadline bounds a
	// real one: a bet nobody comes to collect is dropped rather than left
	// holding an acquisition slot.
	Deadline time.Time
}

// SpeculativeSessionStartRequest asks for the first slot of a session's window
// zero to be built before consensus has opened that window. The bet is placed
// the moment the session is activated, so the build overlaps what remains of
// the session's start — the network, the consensus engine and its observation
// of window zero — instead of following it. The window's base is the session
// genesis: a producer that opens window zero on the genesis adopts the build,
// anything else drops it.
type SpeculativeSessionStartRequest struct {
	SessionID [32]byte
	Leader    uint32
	StartAt   time.Time
	// Deadline bounds the bet: a window zero nobody has opened by then was lost
	// to the committee's first-block timeout regardless.
	Deadline time.Time
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
// only while recovering the first locally stored candidate whose finalized
// predecessor can be loaded from node storage.
type BuildRequest struct {
	Session         ActivatedSession
	Update          SessionUpdate
	Slot            uint32
	Leader          uint32
	Parent          simplex.ParentID
	Previous        *CandidateArtifact
	FinalizedAnchor *ton.BlockIDExt
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
	// MaxTransactions caps the transactions the build admits; zero leaves the
	// block bounded by its limits alone. The producer sets it for the first slot
	// of a leader window, whose block has to be notarized inside the committee's
	// first-block timeout rather than inside a slot; see firstSlotTransactions.
	MaxTransactions uint32
	// speculative marks a build started before its leader window was observed
	// and carries the predecessor it bet on. It is unexported because the only
	// legitimate producer of one is this package's speculation entry point:
	// every other request resolves its predecessor out of the session, and a
	// caller that could set this could make a build read a state consensus never
	// selected.
	speculative *speculativeBase
	// crossWindowBet marks the other bet this package places: the first slot of
	// the next window, started by the producer of the current one from its own
	// handoff. It carries no speculativeBase because it does not guess a base —
	// PreviousPending names it — but it is a wager just the same, dropped
	// whenever consensus opens that window somewhere else. It exists as its own
	// field because speculative is read by the acquisition layer to decide how a
	// build finds its predecessor, and this build finds it the ordinary way.
	crossWindowBet bool
	// sessionStartAt marks the third bet: the first slot of window zero of a
	// fresh session, started at activation before consensus has observed any
	// window. It carries the instant the bet was placed, which is what the
	// header is stamped with — the slot offset every other build derives its
	// time from needs a window start, and this build has none. Unexported for
	// the same reason speculative is: only SpeculateSessionStart may set it.
	sessionStartAt time.Time
	// PaceStartedAt is the instant this build was scheduled to begin. The
	// CPU-bound split heuristic measures its spans from here rather than from
	// the wall clock, so a build the producer starts ahead of its schedule
	// reports the total, body and external wait a build started on time would
	// have reported.
	//
	// It matters because that heuristic writes into the block: a long collation
	// raises the overload class, the class enters OverloadHistory, and the
	// history decides header.WantSplit — silently, because validators mask that
	// bit.
	//
	// Which way an unclamped measurement errs depends on where the head start
	// went. The overload predicate compares the external wait against the total,
	// and the head start lands in both; whether that makes overload harder or
	// easier to declare turns on how much of the head start was spent waiting for
	// external messages. The point of the clamp is not that it errs safely — it
	// is that a build's verdict should not depend on when the producer happened
	// to be free to start it.
	//
	// Zero leaves the clamp inert, which is the convention every deterministic
	// entry point already relies on.
	PaceStartedAt time.Time
	// PreviousPending is a predecessor that has been built but not committed,
	// handed over by the collation that produced it. It exists because the state
	// this build needs is complete a long way before the candidate it belongs to
	// is installed, and waiting for the installation is the whole of what the
	// pipeline removes.
	//
	// Set only by the producer's handoff path. When it is set the request's
	// Parent and Previous are provisional and unread: the lineage is bound by the
	// producer when it adopts the offer, against the predecessor it actually
	// committed.
	PreviousPending *SuccessorOffer
	// excludeExternals are the external messages the pending predecessor
	// consumed. Its consumption is reported to the pool at its commit, which has
	// not happened yet, so without this the successor's stream offers them again.
	excludeExternals [][32]byte
	// onSuccessor is where a build hands its result the moment it stops
	// recording, before it has serialized anything. The producer installs it on
	// every request it starts; the acquisition layer turns it into the
	// collation's port once it knows the chain the offer has to carry.
	onSuccessor func(SuccessorOffer)
	// revokeSuccessor withdraws an offer this build already made, naming the
	// predecessor root it named. Installed alongside onSuccessor.
	revokeSuccessor func(*successorToken, [32]byte, PipelineHandoffOutcome)
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
// ActivateSession, UpdateSession, AdvanceConsensusBase, and RetireSession must
// be atomic from the caller's view: an error must leave the previous pipeline
// state usable and unchanged. AdvanceConsensusBase installs its selected state
// and Update together; production must never observe one without the other.
// Service advances CurrentBase only through AdvanceConsensusBase; ordinary
// UpdateSession calls carry masterchain/finalization/window metadata for the
// already installed base.
// RestoreCandidate is idempotent per locally durable candidate recovered after
// a restart.
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
	AdvanceConsensusBase(context.Context, ConsensusBaseUpdate) error
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
