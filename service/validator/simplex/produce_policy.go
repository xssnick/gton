package simplex

import "time"

// EmptyBlockPolicy is the producer-side decision of when a leader window
// slot must degrade to an empty candidate — a verbatim port of
// consensus::EmptyBlockPolicy (validator/consensus/window-producer.h). It is
// fed by the same observations both reference producers use (the validator
// BlockProducer and the delegated CollatorProducer) and will drive the
// BuildBlock loop of the validator service.
//
// Not safe for concurrent use; the producer owns it from one goroutine.
type EmptyBlockPolicy struct {
	// LastConsensusFinalizedSeqno is the highest seqno finalized by the
	// session (or seen finalized in the masterchain, which implies it).
	LastConsensusFinalizedSeqno uint32
	// LastMCFinalizedSeqno is the highest seqno of this chain finalized in
	// the masterchain.
	LastMCFinalizedSeqno uint32
	// LastConsensusFinalizedAt is when the session last finalized a block;
	// masterchain finality does not refresh it.
	LastConsensusFinalizedAt time.Time

	// Clock is optional; nil means the system clock.
	Clock Clock
}

func (p *EmptyBlockPolicy) now() time.Time {
	if p.Clock != nil {
		return p.Clock.Now()
	}
	return time.Now()
}

// ObserveSessionStart seeds the policy with the last block before the
// session (state.next_seqno() - 1).
func (p *EmptyBlockPolicy) ObserveSessionStart(seqno uint32) {
	p.LastMCFinalizedSeqno = max(p.LastMCFinalizedSeqno, seqno)
	p.LastConsensusFinalizedSeqno = max(p.LastConsensusFinalizedSeqno, seqno)
	p.LastConsensusFinalizedAt = p.now()
}

// ObserveConsensusFinalized records a final certificate of the session.
func (p *EmptyBlockPolicy) ObserveConsensusFinalized(seqno uint32) {
	p.LastConsensusFinalizedSeqno = max(p.LastConsensusFinalizedSeqno, seqno)
	p.LastConsensusFinalizedAt = p.now()
}

// ObserveMCFinalized records a block of this chain finalized in the
// masterchain; it advances the consensus watermark too but deliberately
// does not refresh the timestamp.
func (p *EmptyBlockPolicy) ObserveMCFinalized(seqno uint32) {
	p.LastMCFinalizedSeqno = max(seqno, p.LastMCFinalizedSeqno)
	p.LastConsensusFinalizedSeqno = max(p.LastMCFinalizedSeqno, p.LastConsensusFinalizedSeqno)
}

// ShouldGenerateEmptyBlock reports whether the producer must emit an empty
// candidate instead of collating: always before a split, when masterchain
// consensus lags by more than one block, or when a shard ran more than 8
// blocks ahead of its masterchain inclusion.
func (p *EmptyBlockPolicy) ShouldGenerateEmptyBlock(isMasterchain, beforeSplit bool, nextSeqno uint32) bool {
	if beforeSplit {
		return true
	}
	if isMasterchain {
		return p.LastConsensusFinalizedSeqno+1 < nextSeqno
	}
	return p.LastMCFinalizedSeqno+8 < nextSeqno
}

// AllowEmptyOnGenerationFailure reports whether a failed or slow collation
// may degrade to an empty candidate: only while the session finalized
// something within the timeout (NoEmptyBlocksOnErrTimeout), so a stuck
// session keeps retrying real blocks instead of emitting empty ones.
func (p *EmptyBlockPolicy) AllowEmptyOnGenerationFailure(timeout time.Duration) bool {
	return !p.now().After(p.LastConsensusFinalizedAt.Add(timeout))
}
