package collator

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"slices"
	"time"
)

const (
	paceHistoryLimit     = 64
	paceHistoryFreshness = 2 * time.Minute
	paceHistoryRetention = 10 * time.Minute
)

// paceEstimate contains only measured numeric capacity. Candidate identity,
// delivery timestamps and the previous certificate belong to one session and
// must never cross a rotation.
type paceEstimate struct {
	millisPerTransaction float64
	samples              int
	sampledAt            time.Time
}

type paceHistoryKey [sha256.Size]byte

type paceHistoryEntry struct {
	estimate      paceEstimate
	catchainSeqno uint32
}

type sessionPace struct {
	pace          *committeePace
	key           paceHistoryKey
	targetRate    time.Duration
	catchainSeqno uint32
}

// committeePaceKey deliberately excludes the session ID, catchain sequence
// number and ValidatorSetHash (which includes the catchain sequence number).
// Shard committees reshuffle validator indices each rotation even when their
// membership is unchanged. Hash a canonical copy of the complete membership;
// the live roster and its protocol-significant indices must remain untouched.
func committeePaceKey(session Session, targetRate time.Duration) paceHistoryKey {
	h := sha256.New()
	var buf [32]byte
	binary.BigEndian.PutUint32(buf[:4], uint32(session.Shard.Workchain))
	binary.BigEndian.PutUint64(buf[4:12], uint64(session.Shard.Shard))
	buf[12] = session.ConsensusVersion
	buf[13] = session.ConsensusFlags
	buf[14] = session.ProtocolVersion
	if session.UseQUIC {
		buf[15] = 1
	}
	binary.BigEndian.PutUint32(buf[16:20], session.SlotsPerLeaderWindow)
	binary.BigEndian.PutUint64(buf[20:28], uint64(targetRate))
	binary.BigEndian.PutUint32(buf[28:], uint32(len(session.Validators)))
	_, _ = h.Write(buf[:])
	validators := slices.Clone(session.Validators)
	slices.SortFunc(validators, func(a, b SessionValidator) int {
		return bytes.Compare(a.PublicKey[:], b.PublicKey[:])
	})
	for _, validator := range validators {
		_, _ = h.Write(validator.PublicKey[:])
		_, _ = h.Write(validator.ADNLID[:])
		binary.BigEndian.PutUint64(buf[:8], validator.Weight)
		_, _ = h.Write(buf[:8])
	}

	var key paceHistoryKey
	h.Sum(key[:0])
	return key
}

// pace resolves numeric history at use time, not when a future session is
// prepared. Its predecessor may obtain useful measurements after preparation.
// Once this session has observed its own certificate, its model is independent.
func (s *Service) pace(session Session, targetRate time.Duration) *committeePace {
	now := time.Now()
	s.pacesMu.Lock()
	defer s.pacesMu.Unlock()

	if s.paces == nil {
		s.paces = make(map[[32]byte]*sessionPace)
	}
	s.rememberPaceLocked(session.ID, now)
	active := s.paces[session.ID]
	if active == nil || active.targetRate != targetRate {
		active = &sessionPace{
			pace:          newCommitteePace(),
			key:           committeePaceKey(session, targetRate),
			targetRate:    targetRate,
			catchainSeqno: session.CatchainSeqno,
		}
		s.paces[session.ID] = active
	}
	if previous, exists := s.paceHistory[active.key]; exists {
		age := now.Sub(previous.estimate.sampledAt)
		if age > paceHistoryRetention {
			delete(s.paceHistory, active.key)
		} else {
			active.pace.seed(previous.estimate, age, targetRate)
		}
	}

	return active.pace
}

func (p *committeePace) snapshot() paceEstimate {
	p.mu.Lock()
	defer p.mu.Unlock()

	return paceEstimate{
		millisPerTransaction: p.millisPerTransaction,
		samples:              p.samples,
		sampledAt:            p.lastSample,
	}
}

func (p *committeePace) seed(estimate paceEstimate, age time.Duration, targetRate time.Duration) {
	p.mu.Lock()
	defer p.mu.Unlock()

	if !p.lastSample.IsZero() {
		return
	}
	if age > paceHistoryFreshness {
		// A quiet interval is not proof that a previous peak is still safe.
		// Keep a restrictive estimate, but bring an optimistic one back to
		// the cold-start ceiling and require fresh evidence for fast growth.
		estimate.millisPerTransaction = max(estimate.millisPerTransaction,
			impliedMillisPerTransaction(targetRate, adaptiveTransactionStart))
	}
	p.millisPerTransaction = estimate.millisPerTransaction
	p.samples = estimate.samples
	// lastSample remains unset: inherited estimates are not local evidence,
	// and must not extend the history lifetime when a session stays idle.
}

func (s *Service) rememberPace(sessionID [32]byte, pace *committeePace, now time.Time) {
	s.pacesMu.Lock()
	defer s.pacesMu.Unlock()

	// Retirement or a target-rate update may have replaced this instance
	// while the consensus callback was updating it outside pacesMu.
	if active := s.paces[sessionID]; active != nil && active.pace == pace {
		s.rememberPaceLocked(sessionID, now)
	}
}

// rememberPaceLocked is called under pacesMu. The lock order is always pacesMu
// then committeePace.mu; certificate processing releases its lock first.
func (s *Service) rememberPaceLocked(sessionID [32]byte, now time.Time) {
	active := s.paces[sessionID]
	if active == nil {
		return
	}
	estimate := active.pace.snapshot()
	if estimate.millisPerTransaction <= 0 || estimate.sampledAt.IsZero() || now.Sub(estimate.sampledAt) > paceHistoryRetention {
		return
	}
	if previous, exists := s.paceHistory[active.key]; exists {
		// A late callback or retirement from the previous generation must not
		// overwrite measurements already obtained by the new active session.
		if active.catchainSeqno < previous.catchainSeqno || !estimate.sampledAt.After(previous.estimate.sampledAt) {
			return
		}
	} else {
		if s.paceHistory == nil {
			s.paceHistory = make(map[paceHistoryKey]paceHistoryEntry)
		}
		var oldestKey paceHistoryKey
		var oldestAt time.Time
		for key, previous := range s.paceHistory {
			if now.Sub(previous.estimate.sampledAt) > paceHistoryRetention {
				delete(s.paceHistory, key)
				continue
			}
			if oldestAt.IsZero() || previous.estimate.sampledAt.Before(oldestAt) {
				oldestKey = key
				oldestAt = previous.estimate.sampledAt
			}
		}
		if len(s.paceHistory) >= paceHistoryLimit {
			delete(s.paceHistory, oldestKey)
		}
	}
	s.paceHistory[active.key] = paceHistoryEntry{estimate: estimate, catchainSeqno: active.catchainSeqno}
}
