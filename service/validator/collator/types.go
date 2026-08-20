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
	// ErrMandatoryDequeueOverflow ends a collation whose predecessor left more
	// already-processed own-shard queue entries than one block can dequeue. It
	// is deliberately not an ErrSizeLimit: that one is answered by admitting
	// less, and this drain is not admission — there is nothing optional in it to
	// give up. See cleanupMandatoryDrain.
	ErrMandatoryDequeueOverflow = errors.New("collator: mandatory own-shard dequeue exceeds the block limits")
)

// collationParallelism is the total worker budget for independent Patricia and
// Merkle-proof branches. Small operations remain sequential inside tonutils.
const collationParallelism = 16

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
	// Proven marks State as a Merkle proof taken from the candidate's own
	// collated data rather than a full live tree. It is set exactly where that
	// substitution happens — proofBackedPredecessors and provenPredecessorStates
	// — and read where a full tree is required for something other than
	// continuing this candidate: a proof covers what the candidate's replay
	// reads and may stop at a pruned boundary anywhere else, so a proven state
	// must never be taken as a neighbour source (see loadExpectedNeighbors).
	// The field is process-local: PreviousBlock is not serialized, so nothing on
	// the wire can claim a narrow state is full.
	Proven bool
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
	// QueueCleanupUntil is the wall-clock instant out-queue cleanup stops at,
	// the port of C++ Collator::queue_cleanup_timeout_. Zero disables the
	// budget, which is what every deterministic entry point wants: BuildShard,
	// restore requests and the whole test surface leave it zero and reproduce
	// the pre-budget candidate byte for byte. Derive it with queueCleanupUntil.
	QueueCleanupUntil time.Time
	// InternalMsgUntil is the wall-clock instant the three internal message
	// phases — inbound internals, dispatch queue, generated messages — stop
	// admitting work at, the port of C++ Collator::internal_msg_timeout_. Zero
	// disables the budget under the same convention QueueCleanupUntil uses.
	// Derive it with internalMsgUntil.
	InternalMsgUntil time.Time

	accountPrewarmer AccountPrewarmer
	assembly         *candidateAssemblyDurations
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
	// QueueCleanupUntil bounds out-queue cleanup exactly as the shardchain
	// field does. Masterchain builds carry no external wait, so the derivation
	// gives them a quarter of the remaining soft budget.
	QueueCleanupUntil time.Time
	// InternalMsgUntil bounds the internal message phases exactly as the
	// shardchain field does; with no external wait the derivation gives the
	// masterchain half of the remaining soft budget.
	InternalMsgUntil time.Time

	ShardTops []ShardTop

	accountPrewarmer AccountPrewarmer
	assembly         *candidateAssemblyDurations
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
	// previousStats and previousInfo are the predecessor statistics and block
	// info the masterchain structural pass already decoded. They are nil on the
	// shard path and whenever the caller did not run that pass, so consumers
	// must go through masterPredecessorState and keep their own fallback.
	previousStats *tlb.ShardStateStats
	previousInfo  *tlb.McStateExtraBlockInfo
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
	// previousState is the state verifyPredecessor already returned for this
	// exact Previous value while the acquisition pipeline resolved the
	// masterchain view around it. It is only ever the result of that same call,
	// so supplying it removes a duplicate decode and accounts-prefix walk
	// without removing a check. Entry points that did not run it leave it nil
	// and verification performs the call itself.
	previousState *acquiredPredecessorState
}

// acquiredPredecessorState is a predecessor state parse together with the exact
// root it was parsed from. The root is what makes the plumbing assertion on the
// receiving side a real one: a state carries no identity of its own — two
// forks at the same sequence number in the same shard produce states that agree
// on every field the verifier could compare — while the tree it came from is
// the very cell the rest of verification runs against, and can be compared to
// it by pointer.
type acquiredPredecessorState struct {
	root  *cell.Cell
	state tlb.ShardStateUnsplit
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
	// queueCleanupUntil bounds out-queue cleanup in wall-clock time. Zero
	// leaves the budget inert, which is what every deterministic entry point
	// relies on; see queueCleanupUntil for the derivation.
	queueCleanupUntil time.Time
	// internalMsgUntil bounds the inbound-internal, dispatch-queue and
	// generated-message phases in wall-clock time. Zero leaves the budget
	// inert; see internalMsgUntil for the derivation.
	internalMsgUntil time.Time
	accountPrewarmer AccountPrewarmer
	assembly         *candidateAssemblyDurations
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
		queueCleanupUntil:   req.QueueCleanupUntil,
		internalMsgUntil:    req.InternalMsgUntil,
		accountPrewarmer:    req.accountPrewarmer,
		assembly:            req.assembly,
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
		queueCleanupUntil:   req.QueueCleanupUntil,
		internalMsgUntil:    req.InternalMsgUntil,
		accountPrewarmer:    req.accountPrewarmer,
		assembly:            req.assembly,
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
	QueueCleaned         uint32
	QueueCleanupStop     CleanupStopReason
	// InternalMsgTimeouts counts the internal phases that stopped admitting work
	// because the ported internal_msg_timeout_ had passed rather than because a
	// limit axis filled. Non-zero means the block was truncated by wall clock,
	// which is a legal published block on both sides but the only signal that it
	// happened.
	InternalMsgTimeouts uint32
	Load                LoadClass
	OverloadReason      OverloadReason

	// ProcessedScanTraceError is empty on every healthy block. It is set when
	// traceProcessedQueueValidationClosure could not finish replaying the
	// validator's predecessor-queue scan, which leaves the collated proof short
	// of what a proof-backed validator will open. The block still ships — see
	// that function for why refusing to ship is worse — but it is very likely
	// to be rejected, and this carries the real reason, which the rejection
	// itself will not.
	ProcessedScanTraceError string
}

// CleanupStopReason records why out-queue cleanup stopped. It is local
// observability state, not block data: a partially cleaned ordinary block is
// legal on both sides (see queueCleanupUntil), so nothing downstream can tell
// these apart from the block alone. C++ carries the same information as two
// LOG(WARNING) lines and stats_.msg_queue_cleaned.
type CleanupStopReason uint8

const (
	// CleanupStopExhausted is the normal end: every neighbor's frontier ran out
	// of entries or hit its first undelivered message.
	CleanupStopExhausted CleanupStopReason = iota
	CleanupStopBlockFull
	CleanupStopBudget
	cleanupStopReasonCount
)

func (r CleanupStopReason) String() string {
	switch r {
	case CleanupStopExhausted:
		return "exhausted"
	case CleanupStopBlockFull:
		return "block_full"
	case CleanupStopBudget:
		return "budget"
	default:
		return "unknown"
	}
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

	// provenance binds every mutable field an optimization capsule depends on
	// to the exact values the complete Builder entry point produced. A Pipeline
	// may decorate the canonical builder and return the same Candidate pointer,
	// so an unexported flag alone is not provenance: the exported fields can
	// have changed while the flag survives. Service checks this seal at the
	// public Pipeline boundary before retaining prepared or built.
	provenance *candidateProvenance

	// built is the derivation finish() already performed for this block: its
	// parsed root, the header's start logical time, the successor state's
	// generation time and the predecessor states the update was built over.
	// The field is unexported so that only the canonical build path can claim
	// it — a Pipeline implemented outside this package returns a Candidate with
	// no capsule, and its block is committed by replaying the bytes, which is
	// genuine work there. See candidate_built_commit.go.
	built *builtCandidate

	// digested records that ID.FileHash and CollatedFileHash are the digests
	// finish() computed from these very BlockBOC and CollatedData buffers, in
	// the same composite literal that carries them. Nothing between there and
	// the signature touches either slice, so re-deriving the two digests to
	// compare them with themselves is a megabyte of sha256 that can only fail
	// on local memory corruption.
	//
	// It is separate from built, which may legitimately be nil for a build whose
	// predecessor list a capsule cannot describe, and it is unexported for the
	// same reason built is: a Pipeline supplied from outside this package
	// returns a Candidate whose hashes this process did not compute, and there
	// the comparison is the only thing standing between a mis-serialized block
	// and a signature over an ID nobody else will reproduce.
	digested bool
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

	// The three below are the same idea for the other scratch structures a build
	// fills, and they are separate counters because the read count does not
	// predict any of them. Measured over four mainnet block shapes the storage
	// walk's populations were 0.57-0.85 of the read set and the update memo's was
	// 0.27-0.50 of it, so the read hint sizes them anywhere from a fifth to three
	// times over — and over-sizing here is megabytes per block, since a slot in
	// these tables drags 40 to 56 bytes of entry with it. Each population is
	// steady from block to block in its own right, which is what makes the
	// previous build the right ruler for it.
	//
	// CAPACITY ONLY — these are integers, deliberately. Nothing that holds cells
	// may be carried from one build to the next: the proof arena hands out
	// interior pointers that become cells of the produced block and of the
	// collated data a background goroutine is still serializing, so a reused
	// buffer aliases one block's proof into the next one's.
	storageCells      atomic.Int64
	storageProofCells atomic.Int64
	updateMemoCells   atomic.Int64
}

// NewBuilder creates a candidate builder backed by a prepared TVM instance.
func NewBuilder(machine *tvm.TVM, software tlb.GlobalVersion) *Builder {
	return &Builder{machine: machine, software: software}
}

func (b *Builder) readSetHint() int {
	return int(b.readSetCells.Load())
}

// storageHints sizes the next build's storage stat: how many distinct ordinary
// cells it walked and how many cells its proofs held outside the read set.
func (b *Builder) storageHints() (cells, proofCells int) {
	return int(b.storageCells.Load()), int(b.storageProofCells.Load())
}

// updateMemoMaxPresizedCells bounds what the memo hint asks for, so a figure
// wrong by orders of magnitude sizes a bounded table rather than an arbitrary
// one — the same ceiling readSetMaxPresizedCells, storageSeenMaxPresizedCells
// and proofSeenMaxPresizedCells carry for their own structures.
//
// What it protects against is specific to this hint. The walk it sizes used to
// be sized from rs.Size(), the count of cells the build in front of it had just
// read: an over-estimate, but one that could not exceed the block being built.
// The hint replaces that bound with a number carried over from the previous
// build, and one builder serves collations whose shapes differ by an order of
// magnitude — a masterchain build's memo sizing the next shard build's. This is
// what stops that from becoming an allocation nothing in the current block
// justifies.
const updateMemoMaxPresizedCells = 1 << 20

// updateMemoHint sizes the memo the next state update's destination walk fills.
// An eighth is added because the walk grows its entry array by doubling, so
// landing just under costs a copy of everything memoised so far, while landing
// an eighth over costs an eighth of one array.
func (b *Builder) updateMemoHint() int {
	if b == nil {
		return 0
	}
	cells := int(b.updateMemoCells.Load())
	if cells > updateMemoMaxPresizedCells {
		cells = updateMemoMaxPresizedCells
	}
	hint := cells + cells/8
	if hint > updateMemoMaxPresizedCells {
		hint = updateMemoMaxPresizedCells
	}
	return hint
}

// observeBuildSizes records what a finished build measured, for the next one to
// size against. Only a build that produced a candidate reports, so a run
// abandoned part-way cannot shrink the next recorder to the size it stopped at.
//
// All four are written unconditionally, because they are one observation of one
// build rather than four running maxima. A build that legitimately reports zero
// for a field is reporting that the structure behind it was not filled, and the
// next build is better sized from that than from a figure left behind by a build
// two ago: a zero costs the ordinary lazy growth, which is what every one of
// these structures did before there were hints at all.
func (b *Builder) observeBuildSizes(readSetCells, storageCells, storageProofCells, updateMemoCells int) {
	b.readSetCells.Store(int64(readSetCells))
	b.storageCells.Store(int64(storageCells))
	b.storageProofCells.Store(int64(storageProofCells))
	b.updateMemoCells.Store(int64(updateMemoCells))
}
