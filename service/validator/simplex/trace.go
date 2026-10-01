package simplex

import (
	"encoding/binary"
	"fmt"
	"math"
	"time"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// Trace events are the protocol's consensus.stats.* events. This package emits
// only the ones the pool and voter parts are responsible for; the collate /
// validation / accept events belong to the producer, validator and accepter
// stages.
//
// Scheme strings are exact lines of ton_api.tl; golden constructor ids are
// pinned in tl_test.go.
const (
	schemeStatsID = "consensus.stats.id workchain:int shard:long cc_seqno:int idx:int total_validators:int weight:long total_weight:long slots_per_leader_window:int = consensus.stats.Event"

	schemeStatsCandidateBlock = "consensus.stats.block id:tonNode.blockIdExt = consensus.stats.CandidateBlock"
	schemeStatsCandidateEmpty = "consensus.stats.empty = consensus.stats.CandidateBlock"

	schemeStatsCandidateReceived = "consensus.stats.candidateReceived id:consensus.candidateId parent:consensus.CandidateParent block:consensus.stats.CandidateBlock is_collator:Bool = consensus.stats.Event"

	schemeStatsTimestampedEvent = "consensus.stats.timestampedEvent ts:double event:consensus.stats.Event = consensus.stats.TimestampedEvent"

	schemeSimplexStatsVoted        = "consensus.simplex.stats.voted vote:consensus.simplex.UnsignedVote = consensus.stats.Event"
	schemeSimplexStatsCertObserved = "consensus.simplex.stats.certObserved vote:consensus.simplex.UnsignedVote = consensus.stats.Event"
)

// ConsensusStatsID is consensus.stats.id.
type ConsensusStatsID struct {
	Workchain            int32 `tl:"int"`
	Shard                int64 `tl:"long"`
	CCSeqno              int32 `tl:"int"`
	Idx                  int32 `tl:"int"`
	TotalValidators      int32 `tl:"int"`
	Weight               int64 `tl:"long"`
	TotalWeight          int64 `tl:"long"`
	SlotsPerLeaderWindow int32 `tl:"int"`
}

// ConsensusStatsCandidateBlock is consensus.stats.block.
type ConsensusStatsCandidateBlock struct {
	ID ton.BlockIDExt `tl:"struct"`
}

// ConsensusStatsCandidateEmpty is consensus.stats.empty.
type ConsensusStatsCandidateEmpty struct{}

// ConsensusStatsCandidateReceived is consensus.stats.candidateReceived.
type ConsensusStatsCandidateReceived struct {
	ID         ton.ConsensusCandidateID `tl:"struct"`
	Parent     any                      `tl:"struct boxed [consensus.candidateParent,consensus.candidateWithoutParents]"`
	Block      any                      `tl:"struct boxed [consensus.stats.block,consensus.stats.empty]"`
	IsCollator bool                     `tl:"bool"`
}

// ConsensusSimplexStatsVoted is consensus.simplex.stats.voted.
type ConsensusSimplexStatsVoted struct {
	Vote any `tl:"struct boxed [consensus.simplex.notarizeVote,consensus.simplex.finalizeVote,consensus.simplex.skipVote]"`
}

// ConsensusSimplexStatsCertObserved is consensus.simplex.stats.certObserved.
type ConsensusSimplexStatsCertObserved struct {
	Vote any `tl:"struct boxed [consensus.simplex.notarizeVote,consensus.simplex.finalizeVote,consensus.simplex.skipVote]"`
}

func init() {
	tl.Register(ConsensusStatsID{}, schemeStatsID)
	tl.Register(ConsensusStatsCandidateBlock{}, schemeStatsCandidateBlock)
	tl.Register(ConsensusStatsCandidateEmpty{}, schemeStatsCandidateEmpty)
	tl.Register(ConsensusStatsCandidateReceived{}, schemeStatsCandidateReceived)
	tl.Register(ConsensusSimplexStatsVoted{}, schemeSimplexStatsVoted)
	tl.Register(ConsensusSimplexStatsCertObserved{}, schemeSimplexStatsCertObserved)
}

var idStatsTimestampedEvent = tl.CRC(schemeStatsTimestampedEvent)

// Tracer receives consensus trace events. It is optional; implementations are
// called synchronously from the engine goroutine and must not block.
type Tracer interface {
	Trace(at time.Time, ev TraceEvent)
}

// TraceEvent is one consensus.stats.Event.
type TraceEvent interface {
	// Serialize returns the boxed consensus.stats.Event wire form.
	Serialize() []byte
	String() string
}

// EncodeTimestampedTraceEvent wraps an event into the boxed
// consensus.stats.timestampedEvent that consensus.stats.events batches. The
// timestamp is unix seconds as a double, taken from the system clock.
// No Tracer batches events yet — every implementation consumes them one by
// one — so this exists for the stats-publishing stage; its wire form is
// pinned by a test in the meantime.
func EncodeTimestampedTraceEvent(at time.Time, ev TraceEvent) []byte {
	body := ev.Serialize()
	out := make([]byte, 0, 12+len(body))
	out = binary.LittleEndian.AppendUint32(out, idStatsTimestampedEvent)
	seconds := float64(at.UnixNano()) / float64(time.Second)
	out = binary.LittleEndian.AppendUint64(out, math.Float64bits(seconds))
	return append(out, body...)
}

// TraceSessionStarted is the session identity event (consensus.stats.id),
// emitted once when the engine starts.
type TraceSessionStarted struct {
	Workchain int32
	Shard     uint64
	CCSeqno   uint32
	// LocalIndex is our validator index, or ObserverIndex (-1) for observers —
	// the canonical -1 encoding for a missing index.
	LocalIndex           int
	TotalValidators      int
	Weight               uint64
	TotalWeight          uint64
	SlotsPerLeaderWindow uint32
}

func (e TraceSessionStarted) Serialize() []byte {
	return mustSerialize(ConsensusStatsID{
		Workchain:            e.Workchain,
		Shard:                int64(e.Shard),
		CCSeqno:              int32(e.CCSeqno),
		Idx:                  int32(e.LocalIndex),
		TotalValidators:      int32(e.TotalValidators),
		Weight:               int64(e.Weight),
		TotalWeight:          int64(e.TotalWeight),
		SlotsPerLeaderWindow: int32(e.SlotsPerLeaderWindow),
	})
}

func (e TraceSessionStarted) String() string {
	return fmt.Sprintf("Id{workchain=%d, shard=%016x, cc_seqno=%d, idx=%d, total_validators=%d, weight=%d, total_weight=%d, slots_per_leader_window=%d}",
		e.Workchain, e.Shard, e.CCSeqno, e.LocalIndex, e.TotalValidators, e.Weight, e.TotalWeight, e.SlotsPerLeaderWindow)
}

// TraceVoted is emitted for every vote we cast (consensus.simplex.stats.voted).
type TraceVoted struct{ Vote Vote }

func (e TraceVoted) Serialize() []byte {
	return mustSerialize(ConsensusSimplexStatsVoted{Vote: voteToTL(e.Vote)})
}

func (e TraceVoted) String() string { return fmt.Sprintf("Voted{%s}", e.Vote) }

// TraceCertObserved is emitted for every certificate the pool stores
// (consensus.simplex.stats.certObserved).
type TraceCertObserved struct{ Vote Vote }

func (e TraceCertObserved) Serialize() []byte {
	return mustSerialize(ConsensusSimplexStatsCertObserved{Vote: voteToTL(e.Vote)})
}

func (e TraceCertObserved) String() string { return fmt.Sprintf("CertObserved{%s}", e.Vote) }

// TraceCandidateReceived is emitted for a candidate proposed by another
// validator (consensus.stats.candidateReceived).
type TraceCandidateReceived struct {
	ID     CandidateID
	Parent ParentID
	// Block is the proposed block id; nil for an empty candidate, which the
	// reference reports as consensus.stats.empty.
	Block      *ton.BlockIDExt
	IsCollator bool
}

func (e TraceCandidateReceived) Serialize() []byte {
	var block any = ConsensusStatsCandidateEmpty{}
	if e.Block != nil {
		block = ConsensusStatsCandidateBlock{ID: *e.Block}
	}
	return mustSerialize(ConsensusStatsCandidateReceived{
		ID:         tlCandidateID(e.ID),
		Parent:     parentToTL(e.Parent),
		Block:      block,
		IsCollator: e.IsCollator,
	})
}

func (e TraceCandidateReceived) String() string {
	block := "empty"
	if e.Block != nil {
		block = fmt.Sprintf("(%d,%016x,%d)", e.Block.Workchain, uint64(e.Block.Shard), e.Block.SeqNo)
	}
	return fmt.Sprintf("CandidateReceived{id=%s, parent=%s, block_id=%s}", e.ID, e.Parent, block)
}
