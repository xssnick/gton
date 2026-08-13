package collator

import (
	"context"
	"crypto/ed25519"
	"errors"
	"slices"
	"time"

	"github.com/xssnick/tonutils-go/ton"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

var (
	// ErrNotFound reports an absent optional collator storage record.
	ErrNotFound = errors.New("collator runtime: not found")
	// ErrSessionConflict reports reuse of a session ID with different inputs.
	ErrSessionConflict = errors.New("collator runtime: session conflict")
	// ErrWindowConflict reports reuse of a window ID with different inputs.
	ErrWindowConflict = errors.New("collator runtime: window conflict")
	// ErrCandidateConflict reports different candidates for one window slot.
	ErrCandidateConflict = errors.New("collator runtime: candidate conflict")
)

// SessionValidator is one validator in the canonical session roster. Its
// slice index is the validator index carried by Simplex messages.
type SessionValidator struct {
	PublicKey [ed25519.PublicKeySize]byte
	ADNLID    [32]byte
	Weight    uint64
}

// Session contains the immutable inputs needed to authenticate Delegated-v3
// requests for one consensus session. Activation anchors are deliberately
// separate: a future session is fully routable and may accept delegations
// before its exact predecessor set is known.
type Session struct {
	ID                   [32]byte
	Shard                groups.ShardID
	CatchainSeqno        uint32
	ValidatorSetHash     uint32
	ConsensusVersion     uint8
	ConsensusFlags       uint8
	ProtocolVersion      uint8
	UseQUIC              bool
	SlotsPerLeaderWindow uint32
	Validators           []SessionValidator
}

// Equal compares the complete immutable session identity.
func (s Session) Equal(other Session) bool {
	return s.ID == other.ID && s.Shard == other.Shard && s.CatchainSeqno == other.CatchainSeqno &&
		s.ValidatorSetHash == other.ValidatorSetHash &&
		s.ConsensusVersion == other.ConsensusVersion && s.ConsensusFlags == other.ConsensusFlags &&
		s.ProtocolVersion == other.ProtocolVersion && s.UseQUIC == other.UseQUIC &&
		s.SlotsPerLeaderWindow == other.SlotsPerLeaderWindow &&
		slices.Equal(s.Validators, other.Validators)
}

// ActivatedSession binds an immutable tentative descriptor to the exact
// one-shot chain anchors learned when the group becomes active. Genesis is the
// ordered predecessor set: one block for a linear/split start and two child
// blocks for a merge start. A newer masterchain context still has to prove
// MinMasterchain as its ancestor.
type ActivatedSession struct {
	Session
	Genesis        []ton.BlockIDExt
	MinMasterchain ton.BlockIDExt
}

// SessionActivation is the compact durable/API form of an activation. The
// prepared pipeline already owns the immutable Session descriptor identified
// by SessionID; build paths combine both into ActivatedSession once.
type SessionActivation struct {
	SessionID      [32]byte
	Genesis        []ton.BlockIDExt
	MinMasterchain ton.BlockIDExt
}

// SessionUpdate is the latest authenticated chain and noncritical config view
// made available to the acquisition pipeline. HasFinalizedBlock keeps the
// persisted form explicit.
type SessionUpdate struct {
	SessionID                 [32]byte
	TargetRate                time.Duration
	NoEmptyBlocksOnErrTimeout time.Duration
	MasterchainBlock          ton.BlockIDExt
	HasFinalizedBlock         bool
	FinalizedBlock            ton.BlockIDExt
	Registered                []groups.ShardDescription
	HasCurrentWindow          bool
	CurrentWindowStart        uint32
	// CurrentWindowObservedSlot preserves mid-window consensus progress. Local
	// production is eligible only when it equals CurrentWindowStart.
	CurrentWindowObservedSlot uint32
	// CurrentWindowStartAt is the production start_time computed after
	// resolving CurrentBase, including the parent gen_utime_exact clamp. It is
	// not the raw time at which Simplex observed the window.
	CurrentWindowStartAt time.Time
	CurrentBase          simplex.ParentID
}

// SessionRecord is the durable session descriptor, optional one-shot
// activation, and latest chain view. Activation is nil only while the session
// is tentative.
type SessionRecord struct {
	Session    Session
	Activation *SessionActivation
	Update     SessionUpdate
}

// WindowID binds each durable candidate marker to its consensus leader window.
// A session cannot have two different windows beginning at the same slot.
type WindowID struct {
	SessionID [32]byte
	StartSlot uint32
}

// CandidateRecord is the durable proof that the selected self or delegated
// authority signed one candidate for one slot. It deliberately carries no
// block or collated payload: those megabytes are already written per slot by
// the consensus side, and the only
// thing durability has to guarantee here is that a restarted producer can never
// sign a second, different candidate for a slot it already broadcast.
//
// Re-emitting the exact bytes of an already signed slot therefore only works
// while the artifact is still in memory, which covers every in-process retry.
// A producer that lost that memory — a restart — ends the window instead of
// rebuilding it. Rebuilding is not an option at any price: collation is not
// byte-reproducible, so a rebuilt candidate would be a second signature on the
// same slot.
type CandidateRecord struct {
	WindowID            WindowID
	Authority           CandidateAuthority
	ID                  simplex.CandidateID
	Parent              simplex.ParentID
	Leader              uint32
	Empty               bool
	Block               ton.BlockIDExt
	CollatedFileHash    [32]byte
	Signature           []byte
	DelegationKey       [ed25519.PublicKeySize]byte
	DelegationSignature []byte
}

// CollatorStorage is the complete durable contract consumed by Service. Its
// concrete owner controls database lifetime; this view deliberately has no
// Close method. Exact duplicate saves are idempotent, while conflicting
// records return the matching conflict error declared above.
type CollatorStorage interface {
	// SaveSession returns from its call only after the write has either entered
	// the durable FIFO or its callback has received the admission error. This
	// lets the session lifecycle lock order a successfully admitted write before
	// a later retirement while still making queue backpressure cancellable.
	SaveSession(context.Context, SessionRecord, func(error))
	Session(context.Context, [32]byte) (SessionRecord, error)
	Sessions(context.Context) ([]SessionRecord, error)
	DeleteSession(context.Context, [32]byte) error

	SaveCandidate(CandidateRecord, func(error))
	Candidate(context.Context, WindowID, uint32) (CandidateRecord, error)

	Status(context.Context) (StorageStatus, error)
}

// StorageStatus is a bounded projection suitable for operator status output.
type StorageStatus struct {
	Sessions      uint64
	Candidates    uint64
	PendingWrites int
	DB            DBMetrics
}

// DBMetrics is the bounded physical Pebble projection reported by the shared
// database owner without exposing its implementation to the collator domain.
type DBMetrics struct {
	DiskSize              uint64
	LiveSize              uint64
	ReadAmp               int64
	L0Files               int64
	L0Sublevels           int64
	CompactionDebt        uint64
	CompactionsInProgress int64
	MemTableSize          uint64
	WALSize               uint64
}

// Status is an immutable snapshot of active production and durable state.
type Status struct {
	Started          bool
	Closing          bool
	Closed           bool
	ActiveWindows    int
	RetryingWindows  int
	CompletedWindows uint64
	FailedWindows    uint64
	LastError        string
	LastCompleted    time.Time
	Storage          StorageStatus
}

// CandidateBroadcastMode selects the overlay used for candidate FEC
// broadcasts. Delegation queries and consensus messages always use the
// private consensus overlay in both modes.
type CandidateBroadcastMode uint8

const (
	CandidateBroadcastPrivateOverlay CandidateBroadcastMode = iota + 1
	CandidateBroadcastBlockSyncOverlay
)

// OverlayRole controls whether the local ADNL identity only observes a private
// consensus overlay or may receive Delegated-v3 collation requests for it.
type OverlayRole uint8

const (
	OverlayRoleObserver OverlayRole = iota + 1
	OverlayRoleCollator
)

// OverlaySession contains the immutable inputs needed to create the private
// consensus overlay and, when selected, its block-sync broadcast overlay.
// Session.Validators supplies the canonical roster order, validator public
// keys, and validator ADNL identities. AllOverlayNodes is the wider persistent
// membership used by block sync and observer-enabled private overlays. Every
// slice is immutable after it is passed to ConsensusObserver, so an implementation
// may retain it without copying.
type OverlaySession struct {
	Session                   Session
	Role                      OverlayRole
	CollatorsByValidator      []groups.CollatorRegistryEntry
	AllOverlayNodes           [][32]byte
	MaxBlockSize              uint32
	MaxCollatedDataSize       uint32
	BroadcastMode             CandidateBroadcastMode
	ObserversInPrivateOverlay bool
}
