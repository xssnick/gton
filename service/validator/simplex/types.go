// Package simplex implements the TON Simplex consensus engine: a durable,
// headless, single-threaded state machine that is wire- and signature-
// compatible with the protocol as other nodes implement it.
//
// The package is intentionally isolated: it does not touch the node runtime,
// networking, or storage. Everything environment-specific is injected through
// small interfaces for transport, persistence, candidate processing, events,
// signing and time, which makes the engine fully deterministic under test and
// embeddable into the validator service.
//
// Architecture (the protocol's actors collapsed into one event loop):
//
//   - Engine — owns all state; NOT goroutine-safe. All methods must be called
//     from a single goroutine. Use Runner for a production-ready wrapper.
//
//   - pool part — per-slot vote/certificate accounting, weighted
//     quorum, parent-chain waits, skip runs, present-slot advancement,
//     standstill detection and paced resolution re-broadcast.
//
//   - voter part — the voting policy: the candidate
//     pipeline (the durable store starts on acceptance and runs concurrently
//     with the parent wait and the validation gate; the notarize vote waits
//     for both), the finalize rule, slot timeouts with adaptive first-block
//     timeout, startup skip votes for the last announced leader window.
//
//   - Journal — durable intent log. The engine enforces the
//     critical order for every own vote:
//
//     decision -> durable journal write -> sign -> apply to pool -> send
//
//     Writes are asynchronous: the engine keeps
//     serving inputs while a record persists (an fsync never stalls the
//     loop), and the dependent continuation — sign+apply+send for votes,
//     application and gossip for certificates, the leader-window
//     announcement — runs only from the completion. A journal failure is
//     fatal for the session (fail-closed).
//
// Recovery: Engine.Start replays the journal — certificates first (without
// re-gossip), then own votes in seqno order (re-signed, not re-broadcast,
// conflicts tolerated) — and re-casts skip votes for every non-finalized slot
// of the last announced leader window.
//
// Protocol version: the engine targets only the newest wire protocol
// (protocol_version >= 3 semantics — delegated collator candidates and the
// per-shard standstill egress split). Legacy version gates (the v1 block-sync
// overlay, observer/plumtree switches) are intentionally absent; the config parser of a later stage must refuse older networks
// instead of expecting downgraded behavior here.
package simplex

import (
	"crypto/ed25519"
	"encoding/hex"
	"errors"
	"fmt"
	"math"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/tl"
)

// CandidateID identifies a candidate inside one consensus session
// (consensus.candidateId slot:int hash:int256).
type CandidateID struct {
	Slot uint32
	Hash [32]byte
}

// Compare orders ids by (slot, hash), byte-lexicographic on hash — the
// canonical total order on (uint32, Bits256).
func (id CandidateID) Compare(o CandidateID) int {
	if id.Slot != o.Slot {
		if id.Slot < o.Slot {
			return -1
		}
		return 1
	}
	for i := 0; i < 32; i++ {
		if id.Hash[i] != o.Hash[i] {
			if id.Hash[i] < o.Hash[i] {
				return -1
			}
			return 1
		}
	}
	return 0
}

func (id CandidateID) String() string {
	return fmt.Sprintf("{%d, %s}", id.Slot, hex.EncodeToString(id.Hash[:4]))
}

// ParentID is an optional CandidateID. The zero value means "consensus
// genesis" (a candidate with no parent).
type ParentID struct {
	Exists bool
	ID     CandidateID
}

// Parent wraps a CandidateID into a defined ParentID.
func Parent(id CandidateID) ParentID { return ParentID{Exists: true, ID: id} }

// Genesis returns the empty ParentID.
func Genesis() ParentID { return ParentID{} }

// Compare orders parents the way an optional does: genesis sorts before any
// defined id, defined ids compare by CandidateID.Compare.
func (p ParentID) Compare(o ParentID) int {
	if p.Exists != o.Exists {
		if !p.Exists {
			return -1
		}
		return 1
	}
	if !p.Exists {
		return 0
	}
	return p.ID.Compare(o.ID)
}

func (p ParentID) String() string {
	if !p.Exists {
		return "consensus genesis"
	}
	return p.ID.String()
}

// VoteKind enumerates the three Simplex vote types.
type VoteKind uint8

const (
	VoteNotarize VoteKind = iota + 1
	VoteFinalize
	VoteSkip
)

func (k VoteKind) String() string {
	switch k {
	case VoteNotarize:
		return "notarize"
	case VoteFinalize:
		return "finalize"
	case VoteSkip:
		return "skip"
	}
	return fmt.Sprintf("kind(%d)", uint8(k))
}

// Vote is an unsigned Simplex vote (consensus.simplex.UnsignedVote).
// For skip votes ID.Hash is always zero and ID.Slot carries the slot, so the
// struct stays comparable with == across all kinds.
type Vote struct {
	Kind VoteKind
	ID   CandidateID
}

// NotarizeVote builds a notarization vote for a candidate.
func NotarizeVote(id CandidateID) Vote { return Vote{Kind: VoteNotarize, ID: id} }

// FinalizeVote builds a finalization vote for a candidate.
func FinalizeVote(id CandidateID) Vote { return Vote{Kind: VoteFinalize, ID: id} }

// SkipVote builds a skip vote for a slot.
func SkipVote(slot uint32) Vote { return Vote{Kind: VoteSkip, ID: CandidateID{Slot: slot}} }

// Slot returns the slot the vote refers to.
func (v Vote) Slot() uint32 { return v.ID.Slot }

func (v Vote) String() string {
	if v.Kind == VoteSkip {
		return fmt.Sprintf("SkipVote{slot=%d}", v.ID.Slot)
	}
	return fmt.Sprintf("%sVote{id=%s}", map[VoteKind]string{VoteNotarize: "Notarize", VoteFinalize: "Finalize"}[v.Kind], v.ID)
}

// SignedVote is a vote together with its author and detached signature
// (consensus.simplex.vote). The signature covers
// dataToSign(sessionID, boxed UnsignedVote).
type SignedVote struct {
	ValidatorIndex uint32
	Vote           Vote
	Signature      []byte
}

// VoteSignature is one entry of a certificate signature set
// (consensus.simplex.voteSignature).
type VoteSignature struct {
	ValidatorIndex uint32
	Signature      []byte
}

// Certificate is a weighted-quorum proof for one vote
// (consensus.simplex.certificate). Certificates are immutable after
// construction; Serialize caches the wire form.
type Certificate struct {
	Vote       Vote
	Signatures []VoteSignature

	wireOnce sync.Once
	wire     []byte
}

// Validator describes one member of the session validator set.
type Validator struct {
	PublicKey ed25519.PublicKey
	Weight    uint64
}

// KeyNodeIDShort returns the TON short id of an ed25519 key —
// sha256(boxed pub.ed25519), computed through the shared tonutils-go key
// type. It is the node_id_short used in TON block signature sets, and
// delegations identify collators by it.
func KeyNodeIDShort(key ed25519.PublicKey) [32]byte {
	h, err := tl.Hash(keys.PublicKeyED25519{Key: key})
	if err != nil {
		// Key lengths are validated on every ingestion path; this is
		// unreachable for engine-owned keys.
		panic(fmt.Sprintf("simplex: node id: %v", err))
	}
	var out [32]byte
	copy(out[:], h)
	return out
}

// PeerID is an opaque transport-level address (e.g. ADNL short id) used only
// for temporary bad-signature bans.
type PeerID [32]byte

// Params is the noncritical part of the consensus configuration. All fields
// carry the protocol defaults (see DefaultParams). Fields not used by the
// headless engine (candidate resolver knobs, min block interval,
// empty-block timeout) are kept so that the config-30 parser of a later stage
// has a single destination type.
type Params struct {
	TargetRate                  time.Duration // pacing of one slot
	FirstBlockTimeout           time.Duration
	FirstBlockTimeoutMultiplier float64
	FirstBlockTimeoutCap        time.Duration

	CandidateResolveTimeout           time.Duration
	CandidateResolveTimeoutMultiplier float64
	CandidateResolveTimeoutCap        time.Duration
	CandidateResolveCooldown          time.Duration
	CandidateResolveRateLimit         uint32

	StandstillTimeout            time.Duration
	StandstillMaxEgressBytesPerS uint32
	// StandstillMinEgressBytesPerS is the floor of the per-shard egress
	// budget (noncritical param id 16): the standstill drain rate is
	// max(min, max / 2^shard_pfx_len).
	StandstillMinEgressBytesPerS uint32

	MaxLeaderWindowDesync   uint32
	BadSignatureBanDuration time.Duration

	MinBlockInterval           time.Duration
	NoEmptyBlocksOnErrTimeout  time.Duration
	CertificateGossipNeighbors uint32
}

// Validate rejects parameter sets the engine cannot run with. The protocol
// mandates no such check, and a zero pacing, a zero standstill timeout or a
// zero egress budget would silently wedge the pool (the egress budget is a
// divisor). They are refused up front instead, both at construction and on a
// live update.
func (p Params) Validate() error {
	switch {
	case p.TargetRate <= 0:
		return fmt.Errorf("simplex: target rate must be positive")
	case p.FirstBlockTimeout <= 0:
		return fmt.Errorf("simplex: first block timeout must be positive")
	case p.FirstBlockTimeoutMultiplier <= 0 || math.IsNaN(p.FirstBlockTimeoutMultiplier) ||
		math.IsInf(p.FirstBlockTimeoutMultiplier, 0):
		return fmt.Errorf("simplex: first block timeout multiplier must be finite and positive")
	case p.CandidateResolveTimeoutMultiplier <= 0 || math.IsNaN(p.CandidateResolveTimeoutMultiplier) ||
		math.IsInf(p.CandidateResolveTimeoutMultiplier, 0):
		return fmt.Errorf("simplex: candidate resolve timeout multiplier must be finite and positive")
	case p.StandstillTimeout <= 0:
		return fmt.Errorf("simplex: standstill timeout must be positive")
	case p.StandstillMaxEgressBytesPerS == 0 && p.StandstillMinEgressBytesPerS == 0:
		return fmt.Errorf("simplex: standstill egress budget must be positive")
	}
	return nil
}

// DefaultParams returns the protocol defaults for every noncritical parameter.
func DefaultParams() Params {
	return Params{
		TargetRate:                  2400 * time.Millisecond,
		FirstBlockTimeout:           1000 * time.Millisecond,
		FirstBlockTimeoutMultiplier: 1.2,
		FirstBlockTimeoutCap:        100 * time.Second,

		CandidateResolveTimeout:           1000 * time.Millisecond,
		CandidateResolveTimeoutMultiplier: 1.2,
		CandidateResolveTimeoutCap:        10 * time.Second,
		CandidateResolveCooldown:          10 * time.Millisecond,
		CandidateResolveRateLimit:         10,

		StandstillTimeout:            10 * time.Second,
		StandstillMaxEgressBytesPerS: 50 << 17,
		StandstillMinEgressBytesPerS: 1 << 17,

		MaxLeaderWindowDesync:   250,
		BadSignatureBanDuration: 5 * time.Second,

		MinBlockInterval:           0,
		NoEmptyBlocksOnErrTimeout:  15 * time.Second,
		CertificateGossipNeighbors: 20,
	}
}

// LeaderSchedule assigns an expected leader (collator) to every slot.
type LeaderSchedule interface {
	ExpectedLeader(slot uint32) uint32
}

// RoundRobinSchedule is the protocol default: leader window w is led by
// validator (w mod n).
type RoundRobinSchedule struct {
	SlotsPerLeaderWindow uint32
	Validators           uint32
}

func (s RoundRobinSchedule) ExpectedLeader(slot uint32) uint32 {
	return slot / s.SlotsPerLeaderWindow % s.Validators
}

// Transport delivers already-serialized protocol messages to session peers.
// Implementations own peer selection for the random gossip.
type Transport interface {
	BroadcastToAll(msg []byte)
	// BroadcastToValidators targets only the current shard validator roster.
	// Standstill replay must not be fanned out to persistent observers or
	// delegated collators which share the private overlay.
	BroadcastToValidators(msg []byte)
	BroadcastToRandom(count uint32, msg []byte)
}

// Window describes one durably observed consensus leader window. Every
// validator and observer receives it. ObservedSlot may be in the middle of
// the aligned [StartSlot, EndSlot) range after recovery or prefetched
// certificates; Base belongs to ObservedSlot. Production can start only when
// ObservedSlot equals StartSlot. LocalLeader lets a validator choose between
// direct production and one-window-ahead delegation without hiding the
// intervening windows from a standalone collator.
type Window struct {
	Base         ParentID
	ObservedSlot uint32
	StartSlot    uint32
	EndSlot      uint32
	Leader       uint32
	LocalLeader  bool
	ObservedAt   time.Time
}

// Misbehavior kinds, mirroring simplex/misbehavior.h.
type MisbehaviorKind uint8

const (
	MisbehaviorConflictingVotes MisbehaviorKind = iota + 1
	MisbehaviorConflictingCandidateAndCertificate
)

// Misbehavior is a provable protocol violation by a validator. Proof1/Proof2
// are serialized votes/certificates (TL, boxed) demonstrating the conflict.
type Misbehavior struct {
	Kind   MisbehaviorKind
	Proof1 []byte
	Proof2 []byte
}

// Hooks connects the consensus state machine to candidate validation,
// persistence, production and outcome handling owned by the session runtime.
//
// ValidateCandidate and StoreCandidate run on the engine goroutine and must
// hand work off without blocking. Each must invoke done exactly once. With
// Runner, done is safe to invoke from any goroutine; direct Engine users must
// complete inline on the engine goroutine. A nil validation verdict accepts
// the candidate. A store error is fatal because a validator must be able to
// serve every candidate it votes to notarize.
//
// HandleWindow and the outcome methods run on the engine goroutine and must
// not block. Every validator and observer receives HandleWindow after the
// corresponding pool-state journal record is durable. Notarized and finalized
// events are re-emitted during journal replay, so implementations must be
// idempotent. Candidate methods are not called for observer engines.
type Hooks interface {
	ValidateCandidate(c *Candidate, done func(error))
	StoreCandidate(c *Candidate, done func(error))
	HandleWindow(w Window)

	OnNotarized(id CandidateID, cert *Certificate)
	OnFinalized(id CandidateID, cert *Certificate)
	OnMisbehavior(validator uint32, m Misbehavior)
	// OnFatal reports an unrecoverable session error (journal failure, own
	// conflicting votes, quorum-level consensus violation). The engine is
	// halted fail-closed before the call.
	OnFatal(err error)
}

// Signer signs raw payloads (already wrapped into consensus.dataToSign) with
// the validator session key. Implementations must not retain or modify data:
// the engine passes internally cached payload buffers.
type Signer interface {
	Sign(data []byte) ([]byte, error)
}

// Clock abstracts wall time for deterministic tests.
type Clock interface {
	Now() time.Time
}

// SystemClock uses time.Now.
type SystemClock struct{}

func (SystemClock) Now() time.Time { return time.Now() }

// ObserverIndex is the LocalIndex value for a non-voting observer engine.
const ObserverIndex = -1

// maxTotalWeight is the 2^61 cap that keeps total*2 from overflowing uint64.
const maxTotalWeight = uint64(1) << 61

// totalValidatorWeight sums a roster after rejecting the two shapes that would
// break signature verification and the quorum threshold: a key ed25519.Verify
// would panic on, and a total whose doubling overflows uint64.
func totalValidatorWeight(validators []Validator) (uint64, error) {
	var total uint64
	for i := range validators {
		if len(validators[i].PublicKey) != ed25519.PublicKeySize {
			return 0, fmt.Errorf("simplex: validator %d has invalid public key", i)
		}
		if validators[i].Weight == 0 {
			return 0, fmt.Errorf("simplex: validator %d has zero weight", i)
		}
		total += validators[i].Weight
		if total > maxTotalWeight {
			return 0, fmt.Errorf("simplex: total validator weight exceeds 2^61")
		}
	}

	return total, nil
}

// Config assembles an Engine.
type Config struct {
	// SessionID is the validator session id; it domain-separates every
	// signature via consensus.dataToSign.
	SessionID [32]byte
	// ProtocolVersion is the config-29/30 consensus protocol version. This
	// engine implements only the current Simplex protocol (version 3+).
	ProtocolVersion uint8
	// Validators is the session validator set in canonical order; indexes
	// into this slice are the wire validator indexes.
	Validators []Validator
	// LocalIndex is our index in Validators, or ObserverIndex (-1) for a
	// non-voting observer.
	LocalIndex int
	// SlotsPerLeaderWindow is the critical consensus parameter from config 30.
	SlotsPerLeaderWindow uint32
	// ShardPrefixLen is the prefix length of the session shard (0 for the
	// masterchain and a full-workchain shard); it divides the standstill
	// egress budget.
	ShardPrefixLen uint32
	// Workchain, Shard and CCSeqno identify the shard and the validator set
	// rotation of the session. They carry no consensus meaning and are only
	// reported in the consensus.stats.id trace event.
	Workchain int32
	Shard     uint64
	CCSeqno   uint32
	// Params are the noncritical parameters; zero value means DefaultParams.
	Params *Params
	// Schedule is the leader schedule; nil means the default round-robin.
	Schedule LeaderSchedule
	Journal  Journal
	// Bootstrap is the journal state with its certificates already verified,
	// for a caller that had to read the journal before building the engine. The
	// zero value means Start loads and verifies it itself.
	Bootstrap ValidatedBootstrap
	Transport Transport
	// Hooks is required for validators and observers. Candidate validation,
	// storage and leader-window methods are unused by observers.
	Hooks Hooks
	// Tracer is optional; it receives the consensus trace events the
	// reference publishes to its TraceCollector.
	Tracer Tracer
	// Signer is required for validators (LocalIndex >= 0).
	Signer Signer
	// Clock is optional; nil means SystemClock.
	Clock Clock
	// Logger is optional; nil discards logs.
	Logger *zerolog.Logger
}

// Errors returned by the package.
var (
	// ErrAlreadySaved is returned by Journal implementations when a record
	// with the same content hash is already durably stored.
	ErrAlreadySaved = errors.New("simplex: record already saved")
	// ErrStopped is returned for inputs after the engine halted.
	ErrStopped = errors.New("simplex: engine stopped")
)
