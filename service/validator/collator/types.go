package collator

import (
	"context"
	"errors"
	"sync/atomic"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
	"github.com/xssnick/gton/service/validator/simplex"
)

var (
	ErrInvalidInput         = errors.New("collator: invalid input")
	ErrUnsupported          = errors.New("collator: unsupported block")
	ErrSizeLimit            = errors.New("collator: candidate size limit exceeded")
	ErrCollatedRootNotFound = errors.New("collator: collated proof root not found")
)

type PreviousBlock struct {
	ID ton.BlockIDExt
	// Block is required for a non-zerostate predecessor when
	// capFullCollatedData is active. Its state update binds ID.RootHash to
	// State in the collated proof set; a zerostate ID is bound directly to
	// State because there is no preceding block cell.
	Block *cell.Cell
	State *cell.Cell
	// OutQueueSize is storage-verified metadata required for a non-empty queue
	// when State does not store it.
	OutQueueSize *uint64
}

// MasterchainContext contains data verified against the state identified by ID.
// Its shard descriptor must admit Previous as a linear successor.
type MasterchainContext struct {
	ID        ton.BlockIDExt
	EndLT     uint64
	GenUtime  uint32
	VertSeqno uint32
	Config    *Config
	// OutMsgQueueInfo is the exact queue field of the state identified by ID.
	// Full-collated shard validation uses this already-authenticated state view
	// because the protocol does not duplicate the masterchain state proof in
	// CollatedData.
	OutMsgQueueInfo *cell.Cell
	// Groups is the immutable validator-group view derived from ID. The
	// candidate header session fields are taken from it rather than supplied
	// independently, so a stale session cannot be mixed with a newer state.
	Groups     *groups.Snapshot
	PrevBlocks tuple.Tuple
	Libraries  []*cell.Cell
}

type HeaderParams struct {
	GenUtime   uint32
	GenUtimeMS uint64
}

// Neighbor is the processed frontier of one topology neighbor, verified from
// the state of its Block. EndLT is recorded in dequeue descriptors when that
// neighbor already imported one of our outbound messages.
type Neighbor struct {
	Block     ton.BlockIDExt
	Shard     msgpool.ShardIdent
	EndLT     uint64
	Processed []tlb.ProcessedUptoRecord
	// OutMsgQueueInfo is the exact queue field authenticated by Block's state.
	// Semantic validation uses it to prove that every imported message existed
	// in its source queue. Full-collated validation additionally binds the same
	// field to the state proof exposed through CollatedProofs.
	OutMsgQueueInfo *cell.Cell
}

// FullCollatedProofRequest identifies the authenticated block and state proofs
// that the acquisition pipeline must materialize for non-masterchain
// neighbors. The collator already constructs predecessor and account-storage
// proofs itself.
type FullCollatedProofRequest struct {
	Previous  PreviousBlock
	Previous2 *PreviousBlock
	Neighbors []Neighbor
	// Internals is the exact acquired cut. It is retained so the proof provider
	// can bind every acquired envelope to the authenticated queue it traverses.
	Internals []*msgpool.InternalMessage
	// QueueScan is the greatest queue position the candidate may claim after
	// processing Internals. Proof providers traverse every neighbor queue up to
	// this boundary, including entries already covered by predecessor
	// ProcessedInfo: validators must see those entries before accepting the new
	// frontier even though the message pool correctly omits them from Internals.
	QueueScan *FullCollatedQueueScan
}

// FullCollatedQueueScan is the authenticated outbound-queue prefix required by
// semantic validation of one candidate ProcessedInfo update.
type FullCollatedQueueScan struct {
	Target msgpool.ShardIdent
	LT     uint64
	Hash   [32]byte
}

// FullCollatedProofProvider supplies Merkle proofs for neighbor blocks and the
// queue-bearing state views used during collation. The deterministic core
// validates their shape and block-to-state bindings before serializing them.
type FullCollatedProofProvider interface {
	BuildFullCollatedProofs(context.Context, FullCollatedProofRequest) ([]*cell.Cell, error)
}

// AccountStorageStats is a content-addressed cache of account storage
// dictionaries used to update large account states incrementally.
type AccountStorageStats map[cell.Hash]*cell.Cell

// DispatchAccount identifies one account in the node-local dispatch policy.
// AccountID is the rewritten 256-bit account identifier used as a dispatch
// queue key.
type DispatchAccount struct {
	Workchain int32
	AccountID [32]byte
}

// DispatchPolicy contains the node-local controls for DispatchQueue selection and creation of new deferred messages.
// It is deliberately supplied with every request instead of being hidden in
// blockchain Config. The zero value disables the optional phase-2/phase-3
// passes and creation-time deferral; the mandatory first pass still takes one
// queued message per account.
type DispatchPolicy struct {
	AttemptIndex uint32

	DeferringEnabled       bool
	DeferMessagesAfter     uint32
	DeferOutQueueSizeLimit uint64

	Phase2MaxTotal        uint32
	Phase2MaxPerInitiator uint32
	Phase3MaxTotal        uint32
	Phase3MaxPerInitiator uint32
	// Phase3AdaptivePerInitiator selects the queue-size rule (10 at
	// <=256 messages, 2 at <=512, 1 at <=1500, otherwise 0). It cannot be
	// combined with an explicit Phase3MaxPerInitiator.
	Phase3AdaptivePerInitiator bool

	Whitelist    []DispatchAccount
	PriorityList []DispatchAccount
}

// ReferenceDispatchPolicy is the policy every collator is expected to run
// with. The zero DispatchPolicy turns deferring off and
// skips dispatch phases 2 and 3 entirely, which drains DispatchQueue at one
// message per account per block — a silently different message schedule from
// every other collator in the same shard. Candidate validation does not check
// the policy, so the divergence shows up as throughput and fairness rather than a
// rejected block; a composition root with no reason to deviate starts here.
func ReferenceDispatchPolicy() DispatchPolicy {
	return DispatchPolicy{
		DeferringEnabled:           true,
		DeferMessagesAfter:         10,
		DeferOutQueueSizeLimit:     2048,
		Phase2MaxTotal:             150,
		Phase2MaxPerInitiator:      20,
		Phase3MaxTotal:             150,
		Phase3AdaptivePerInitiator: true,
	}
}

// ShardRequest contains every state, epoch and session input used to build one
// shardchain block. BuildShard derives the topology transition, block
// references, sequence numbers, EndLt and load flags from these inputs.
type ShardRequest struct {
	// Shard is the explicit target because a parent predecessor does not say
	// which child is being built after a split.
	Shard    groups.ShardID
	Previous PreviousBlock
	// Previous2 is present only for the merge of two sibling shard states.
	Previous2   *PreviousBlock
	Masterchain MasterchainContext
	Header      HeaderParams
	// BeforeSplit is the decision verified from the current masterchain shard
	// descriptor. It marks this candidate and its next state as the final
	// parent block consumed by two child sessions.
	BeforeSplit bool

	RandSeed  [32]byte
	CreatedBy [32]byte

	Externals []ExternalInput
	// MaxExternalAttempts is the local work budget for TVM admission attempts.
	// It must be positive when Externals is non-empty. Messages past the budget
	// are reported as msgpool.ExternalSkippedLimit and remain retryable.
	MaxExternalAttempts int
	StorageStats        AccountStorageStats
	Dispatch            DispatchPolicy

	// Internals is the pool cut of inbound internal messages, taken as one
	// value so the slice and its completeness cannot come apart:
	// (EnqueuedLT, MsgHash)-ordered messages with exact queue envelope
	// cells, and the More flag marking a window cut short. The import phase
	// consumes it before externals. An incomplete window (More, or a nil cut
	// — queues never inspected) forces generated messages into the outbound
	// queue, as when inbound queues are not
	// drained: immediate delivery would advance the processed bound past
	// the unimported queue tail and lose it.
	Internals *msgpool.Cut

	// Neighbors contains the individual processed frontiers of the topology
	// neighbors. Keeping records grouped by owner is required for multishard
	// cleanup: one uncovered frontier must not stop scanning another neighbor.
	Neighbors []Neighbor
	// FullCollatedProofs is required when capFullCollatedData is active and a
	// non-masterchain neighbor is not one of the candidate predecessors.
	FullCollatedProofs FullCollatedProofProvider
	// NeighborShardEndLT resolves the registered end lt of the shard holding
	// an account prefix as of a record's masterchain seqno — the
	// shard end lt binding, clamped by the supplier to the states it
	// has, clamped to min(record seqno, Masterchain.ID.SeqNo).
	// Required for Neighbors and whenever a parent ProcessedUpto
	// record may cover a message generated outside that record's shard,
	// without it the coverage check cannot decide those messages.
	NeighborShardEndLT tlb.ShardEndLTFunc
}

// MasterRequest contains the previous masterchain state and the deterministic
// inputs selected for one masterchain block. Each ShardTop binds an exact
// TopBlockDescr selected by the acquisition pipeline to the transition that
// the builder validates and applies atomically.
type MasterRequest struct {
	Previous PreviousBlock
	Config   *Config
	Groups   *groups.Snapshot
	Header   HeaderParams

	RandSeed  [32]byte
	CreatedBy [32]byte

	Externals           []ExternalInput
	MaxExternalAttempts int
	StorageStats        AccountStorageStats
	Dispatch            DispatchPolicy
	Internals           *msgpool.Cut
	Neighbors           []Neighbor
	NeighborShardEndLT  tlb.ShardEndLTFunc
	// FullCollatedProofs supplies the post-transition shard-neighbor block and
	// state proofs when capFullCollatedData is active. The masterchain
	// predecessor state is already authenticated by Previous.
	FullCollatedProofs FullCollatedProofProvider

	ShardTops []ShardTop
}

// CandidateTransitionVerifier closes the semantic validation boundary that
// needs acquisition-verified neighbor queues and auxiliary states. A
// successful implementation must replay transactions and derive account,
// message, queue, processed-frontier, library and dynamic value-flow changes;
// when CandidateTransition.FullCollatedData is set, it must also require every
// acquisition-verified non-masterchain neighbor queue to be covered by the
// collated proof set. The structural core never treats an absent verifier as
// success.
type CandidateTransitionVerifier interface {
	VerifyCandidateTransition(context.Context, CandidateTransition) error
}

// CollatedProofView exposes the virtual roots already authenticated by the
// structural verifier. Returned cells are immutable views and must not be
// modified by semantic replay.
type CollatedProofView interface {
	Full() bool
	StateRoot(ton.BlockIDExt) (*cell.Cell, error)
	AccountStorageRoot(cell.Hash) (*cell.Cell, error)
}

// CandidateTransition binds a candidate to the exact predecessor and epoch
// inputs already checked by the deterministic core. Masterchain is present for
// shard candidates; ShardTops are present for masterchain candidates.
type CandidateTransition struct {
	Config           *Config
	Previous         PreviousBlock
	Previous2        *PreviousBlock
	Masterchain      *MasterchainContext
	ShardTops        []ShardTop
	Neighbors        []Neighbor
	Candidate        *Candidate
	FullCollatedData bool
	CollatedProofs   CollatedProofView
	// NeighborShardEndLT is the state-acquisition implementation of the
	// shard end lt query. It is required whenever a
	// ProcessedUpto record covers a message generated outside its owner shard.
	NeighborShardEndLT tlb.ShardEndLTFunc

	// prepared is populated only by the structural verifier. It lets the
	// production semantic verifier reuse the already decoded candidate and
	// effective predecessor (including split/merge normalization) without a
	// second BOC parse on the validation hot path.
	prepared *preparedCandidateTransition
}

type preparedCandidateTransition struct {
	candidate     *verifiedCandidate
	previous      *tlb.ShardStateUnsplit
	minimumBurned tlb.CurrencyCollection
}

// ShardVerificationRequest contains the authenticated context and semantic
// verifier required for a complete shard candidate decision.
type ShardVerificationRequest struct {
	Previous  PreviousBlock
	Previous2 *PreviousBlock

	Masterchain MasterchainContext
	Neighbors   []Neighbor
	// NeighborShardEndLT must be derived from the authenticated masterchain
	// history used to acquire Neighbors.
	NeighborShardEndLT tlb.ShardEndLTFunc
	Semantics          CandidateTransitionVerifier
	Candidate          *Candidate
	// stateProven marks Candidate.State as already applied on top of the
	// predecessor that collated data proves, which is how the acquisition
	// pipeline builds it. Verification then skips rebuilding the same tree:
	// that would produce a byte-identical root at the price of a second walk
	// of the state update. Entry points handed a resident state leave it clear.
	stateProven bool
}

// MasterVerificationRequest contains the deterministic inputs required to
// validate a masterchain candidate. ShardTops are supplied only after the
// acquisition pipeline has verified their proof chains and signatures; this
// core binds those exact descriptors and creator lists to the candidate.
type MasterVerificationRequest struct {
	Previous PreviousBlock
	Config   *Config
	Groups   *groups.Snapshot

	ShardTops []ShardTop
	Neighbors []Neighbor
	// NeighborShardEndLT must be derived from authenticated masterchain state.
	NeighborShardEndLT tlb.ShardEndLTFunc
	Semantics          CandidateTransitionVerifier
	Candidate          *Candidate
}

// internalsIncomplete reports whether the inbound window is not fully
// drained — a cut short window or no cut at all.
func (r *ShardRequest) internalsIncomplete() bool {
	return r.Internals == nil || r.Internals.More
}

type collationRequest struct {
	previous  PreviousBlock
	previous2 *PreviousBlock
	header    HeaderParams

	randSeed  [32]byte
	createdBy [32]byte

	externals           []ExternalInput
	maxExternalAttempts int
	storageStats        AccountStorageStats
	dispatch            DispatchPolicy
	internals           *msgpool.Cut
	neighbors           []Neighbor
	neighborShardEndLT  tlb.ShardEndLTFunc
	fullCollatedProofs  FullCollatedProofProvider
}

func shardCollationRequest(req ShardRequest) collationRequest {
	return collationRequest{
		previous:            req.Previous,
		previous2:           req.Previous2,
		header:              req.Header,
		randSeed:            req.RandSeed,
		createdBy:           req.CreatedBy,
		externals:           req.Externals,
		maxExternalAttempts: req.MaxExternalAttempts,
		storageStats:        req.StorageStats,
		dispatch:            req.Dispatch,
		internals:           req.Internals,
		neighbors:           req.Neighbors,
		neighborShardEndLT:  req.NeighborShardEndLT,
		fullCollatedProofs:  req.FullCollatedProofs,
	}
}

func masterCollationRequest(req MasterRequest) collationRequest {
	return collationRequest{
		previous:            req.Previous,
		header:              req.Header,
		randSeed:            req.RandSeed,
		createdBy:           req.CreatedBy,
		externals:           req.Externals,
		maxExternalAttempts: req.MaxExternalAttempts,
		storageStats:        req.StorageStats,
		dispatch:            req.Dispatch,
		internals:           req.Internals,
		neighbors:           req.Neighbors,
		neighborShardEndLT:  req.NeighborShardEndLT,
		fullCollatedProofs:  req.FullCollatedProofs,
	}
}

func (r *collationRequest) internalMessages() []*msgpool.InternalMessage {
	if r.internals == nil {
		return nil
	}
	return r.internals.Messages
}

func (r *collationRequest) internalsIncomplete() bool {
	return r.internals == nil || r.internals.More
}

// ExternalInput binds a prepared message to the exact pool generation that
// selected it. Candidate.Externals can therefore be passed directly to
// msgpool.Pool.Complete without a hash-based reconciliation step.
type ExternalInput struct {
	Ref     msgpool.ExternalRef
	message *tvm.PreparedMessage
}

// NewExternalInput prepares one immutable pool snapshot for collation.
func NewExternalInput(snapshot msgpool.ExternalSnapshot) (ExternalInput, error) {
	prepared, err := tvm.PrepareMessage(snapshot.Root())
	if err != nil {
		return ExternalInput{}, err
	}

	return ExternalInput{Ref: snapshot.Reference(), message: prepared}, nil
}

type LoadClass uint8

const (
	LoadUnderload LoadClass = iota
	LoadNormal
	LoadSoft
	LoadMedium
	LoadHard
)

type Stats struct {
	Transactions         uint32
	ExternalAttempts     uint32
	ExternalIncluded     uint32
	ExternalInvalid      uint32
	ExternalNotAccepted  uint32
	ExternalSkippedLimit uint32
	ExternalBatches      uint32
	ExternalWait         time.Duration
	ExternalStop         ExternalStopReason
	InternalsImported    uint32
	InternalsSkipped     uint32
	ImmediateDelivered   uint32
	NewMessages          uint32
	EnqueuedMessages     uint32
	DeferredMessages     uint32
	DispatchedMessages   uint32
	GasUsed              uint64
	EndLT                uint64
	OutQueueSize         uint64
	Load                 LoadClass
	OverloadReason       OverloadReason
}

// OverloadReason records why a collation set the overload bit that eventually
// makes a shard want to split. It is local observability state, not block data:
// the bit itself travels in the state, its cause does not.
type OverloadReason uint8

const (
	OverloadNone OverloadReason = iota
	// OverloadBlockLimit is a block-limit axis at or above its soft threshold.
	// It also covers the reference node's separate "long dispatch queue
	// processing" cause, which has no flag of its own here: reaching the
	// dispatch total limit raises the peak load class to soft instead.
	OverloadBlockLimit
	// OverloadForceSplitQueue is the outbound queue reaching the force-split
	// size while the block itself was not overloaded.
	OverloadForceSplitQueue
	// OverloadLongCollation is a CPU-bound collation: the block stayed under
	// every limit but took too long to build.
	OverloadLongCollation
)

func (r OverloadReason) String() string {
	switch r {
	case OverloadNone:
		return "none"
	case OverloadBlockLimit:
		return "block_limit"
	case OverloadForceSplitQueue:
		return "force_split_queue"
	case OverloadLongCollation:
		return "long_collation"
	default:
		return "unknown"
	}
}

// ExternalStopReason records why a live shard collation stopped consuming
// ready external messages. It is local observability state, not block data.
type ExternalStopReason uint8

const (
	ExternalStopUnknown ExternalStopReason = iota
	ExternalStopReadyDrained
	ExternalStopSoftLimit
	ExternalStopDeadline
	ExternalStopAttemptLimit
)

func (r ExternalStopReason) String() string {
	switch r {
	case ExternalStopReadyDrained:
		return "ready_drained"
	case ExternalStopSoftLimit:
		return "soft_limit"
	case ExternalStopDeadline:
		return "deadline"
	case ExternalStopAttemptLimit:
		return "attempt_limit"
	default:
		return "unknown"
	}
}

type Candidate struct {
	ID               ton.BlockIDExt
	CreatedBy        [32]byte
	BlockBOC         []byte
	CollatedData     []byte
	CollatedFileHash [32]byte
	State            *cell.Cell
	StateUpdate      *cell.Cell
	StorageStats     AccountStorageStats

	Externals []msgpool.ExternalFeedback
	Stats     Stats

	// prepared is the broadcast payload of this candidate, compressed in the
	// background from the roots the build produced. It is set only by the
	// canonical build path; a Pipeline that returns a Candidate of its own
	// leaves it nil and its candidate is serialized from the BOCs as before.
	prepared *simplex.PreparedCandidate
}

// Builder carries no state a candidate depends on and may build independent
// candidates concurrently.
type Builder struct {
	machine  *tvm.TVM
	software tlb.GlobalVersion

	// readSetCells is how many cells the last finished build recorded. It sizes
	// the next build's recorder, which otherwise reaches six figures of cells
	// from sixteen slots per shard by doubling, reallocating and rehashing at
	// every step. Consecutive blocks of one chain read the same order of
	// magnitude, and a wrong estimate only costs the growth it failed to avoid,
	// so this is a hint and nothing reads it for anything else.
	readSetCells atomic.Int64
}

// NewBuilder creates a candidate builder backed by a prepared TVM instance.
func NewBuilder(machine *tvm.TVM, software tlb.GlobalVersion) *Builder {
	return &Builder{machine: machine, software: software}
}

func (b *Builder) readSetHint() int {
	return int(b.readSetCells.Load())
}

func (b *Builder) observeReadSetSize(cells int) {
	b.readSetCells.Store(int64(cells))
}
