package validator

import (
	"strconv"
	"sync"
	"time"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

// This file exports the consensus state itself. Before it, 167 gton_* series
// described what the validator did to candidates and not one described where
// consensus was: no slot, no finalization age, no skip certificates, no vote
// participation, no retention lag. A five-hour standstill on the test network
// was therefore invisible to metrics and had to be reconstructed from 500,000
// log lines.
//
// Every value here is one the engine already computes. Nothing new is measured
// on the consensus path; what is new is that it leaves the session.

// LineageWalkResult is the bounded outcome of one leader window's lineage walk.
type LineageWalkResult uint8

const (
	LineageWalkSuccess LineageWalkResult = iota
	LineageWalkFailure
	lineageWalkResultCount
)

// LineageStepSource says where one step of that walk had to get its candidate.
// It is the difference between a walk that costs nothing and a walk that pays
// a storage read per step, which is exactly what the retention floor decides.
type LineageStepSource uint8

const (
	// LineageStepMemory is a candidate still resident in the session cache.
	LineageStepMemory LineageStepSource = iota
	// LineageStepStorage is a candidate whose payload was released and whose
	// durable copy has to be read back.
	LineageStepStorage
	// LineageStepPeer is a candidate with no durable copy: consensus never
	// accepted it here, so resolving it means asking the network.
	LineageStepPeer
	lineageStepSourceCount
)

// LineageWalkObservation is one completed walk from a leader window's base back
// to the applied anchor. Candidates is the depth, Steps says what each step
// cost, and Duration is the whole thing including the anchor state load — the
// three questions asked of every abstain in the field investigation, none of
// which the node could answer about itself.
type LineageWalkObservation struct {
	Chain      collator.MetricChain
	Result     LineageWalkResult
	Candidates int
	Duration   time.Duration
	Steps      [lineageStepSourceCount]int
}

// ConsensusSessionKey identifies one live session's telemetry registration.
type ConsensusSessionKey = SessionStorageID

// ConsensusRetentionStats is the retention floor as of the last finalization.
// The lag is reported for diagnosis and is deliberately not what bounds
// anything: see retention.go.
type ConsensusRetentionStats struct {
	AnchorSlot       uint32
	AnchorKnown      bool
	Capped           bool
	BudgetBytes      int64
	RetainedPayloads int
	RetainedBytes    int64
}

// ConsensusSessionStats is one live session's whole metric contribution.
type ConsensusSessionStats struct {
	Chain     collator.MetricChain
	Stats     simplex.Stats
	Retention ConsensusRetentionStats
}

// ConsensusSessionSource is a live session that can report the above without
// entering its consensus loop. That restriction is the point: a scrape must
// never block behind the goroutine whose stall it is being scraped to explain.
type ConsensusSessionSource interface {
	ConsensusStats() ConsensusSessionStats
}

// consensusValidatorGaugeMaxRoster bounds the per-validator participation
// gauge. Twelve series per chain is a small test network's whole roster; a
// mainnet-sized set is a deliberate decision and not an accident of enabling
// metrics, so it is simply not exported above this size.
const consensusValidatorGaugeMaxRoster = 64

// consensusCounters is the monotone part of one session's contribution. It is
// accumulated by difference, because sessions are per catchain and the labels
// are per chain: summing the live ones would make the totals fall at every
// rotation.
type consensusCounters struct {
	standstills    uint64
	slotsFinalized uint64
	certificates   [simplex.VoteKindCount]uint64
	signatures     [simplex.VoteKindCount]uint64
	votes          [consensusVoteOutcomeCount]uint64
}

const (
	consensusVoteCast = iota
	consensusVoteAccepted
	consensusVoteRejected
	consensusVoteAbstained
	consensusVoteOutcomeCount
)

func sessionCounters(stats simplex.Stats) consensusCounters {
	counters := consensusCounters{
		standstills:    stats.Standstills,
		slotsFinalized: stats.SlotsFinalized,
	}
	copy(counters.certificates[:], stats.CertificatesByKind[:])
	copy(counters.signatures[:], stats.SignaturesByKind[:])
	counters.votes[consensusVoteCast] = stats.OwnVotesCast
	counters.votes[consensusVoteAccepted] = stats.CandidatesAccepted
	counters.votes[consensusVoteRejected] = stats.CandidatesRejected
	counters.votes[consensusVoteAbstained] = stats.CandidatesAbstained

	return counters
}

func (c *consensusCounters) addDelta(previous, current consensusCounters) {
	c.standstills += current.standstills - previous.standstills
	c.slotsFinalized += current.slotsFinalized - previous.slotsFinalized
	for i := range c.certificates {
		c.certificates[i] += current.certificates[i] - previous.certificates[i]
		c.signatures[i] += current.signatures[i] - previous.signatures[i]
	}
	for i := range c.votes {
		c.votes[i] += current.votes[i] - previous.votes[i]
	}
}

// consensusCollector renders the live sessions at scrape time.
//
// Counters are accumulated as differences against the last scrape and survive
// the session that produced them; gauges are read from the newest live session
// of each chain, which is the one a rotation has just handed the chain to.
//
// "Newest" is the registration order held in registered below, and it is
// emphatically not the consensus slot. Slots are per session and start again at
// zero on every rotation — a five-hour test run went through about sixteen of
// them — so the session with the higher slot is routinely the one being retired,
// and selecting on it made the entire gauge set describe the outgoing session
// for as long as the two overlapped: the slot, the finalization age, the
// retention floor and every per-validator participation series at once, at the
// exact moment a rotation is the most likely explanation for what an operator
// is looking at.
type consensusCollector struct {
	mu       sync.Mutex
	sessions map[ConsensusSessionKey]ConsensusSessionSource
	// registered is when each live session was handed to this collector, on a
	// counter that never resets. It is the only monotone axis available here:
	// the collector is told about a session, it is never told what came before
	// it, and a rotation registers the incoming session while the outgoing one
	// is still live and still reporting.
	registered map[ConsensusSessionKey]uint64
	nextSeq    uint64
	last       map[ConsensusSessionKey]consensusCounters
	totals     [2]consensusCounters

	slot                  *prometheus.Desc
	finalizedSlot         *prometheus.Desc
	lineageAnchorSlot     *prometheus.Desc
	retentionLagSlots     *prometheus.Desc
	retentionCapped       *prometheus.Desc
	retentionBudgetBytes  *prometheus.Desc
	retainedPayloads      *prometheus.Desc
	lastFinalization      *prometheus.Desc
	firstBlockTimeout     *prometheus.Desc
	standstills           *prometheus.Desc
	slotsFinalized        *prometheus.Desc
	certificates          *prometheus.Desc
	certificateSignatures *prometheus.Desc
	votes                 *prometheus.Desc
	validatorLastSigned   *prometheus.Desc
}

func newConsensusCollector(namespace string) *consensusCollector {
	name := func(metric string) string {
		return prometheus.BuildFQName(namespace, "validator", metric)
	}
	chain := []string{"chain"}

	return &consensusCollector{
		sessions:   make(map[ConsensusSessionKey]ConsensusSessionSource),
		registered: make(map[ConsensusSessionKey]uint64),
		last:       make(map[ConsensusSessionKey]consensusCounters),
		slot: prometheus.NewDesc(name("consensus_slot"),
			"Present consensus slot of the newest live session on this chain.", chain, nil),
		finalizedSlot: prometheus.NewDesc(name("consensus_finalized_slot"),
			"Highest finalized consensus slot of the newest live session on this chain.", chain, nil),
		lineageAnchorSlot: prometheus.NewDesc(name("consensus_lineage_anchor_slot"),
			"Slot the last completed leader-window lineage walk reached.", chain, nil),
		retentionLagSlots: prometheus.NewDesc(name("consensus_retention_lag_slots"),
			"Finalized slot minus the lineage anchor. Diagnostic only: skipped slots inflate it "+
				"and nothing is bounded by it.", chain, nil),
		retentionCapped: prometheus.NewDesc(name("consensus_retention_capped"),
			"1 while candidate retention is pruning below what the local producer asked to keep.",
			chain, nil),
		retentionBudgetBytes: prometheus.NewDesc(name("consensus_retention_budget_bytes"),
			"Candidate payload bytes this session may retain below the fixed margin.", chain, nil),
		retainedPayloads: prometheus.NewDesc(name("consensus_retained_payloads"),
			"Candidate payloads the session retains; the quantity the retention budget bounds.",
			chain, nil),
		lastFinalization: prometheus.NewDesc(name("consensus_last_finalization_timestamp_seconds"),
			"Unix time of the last observed finalization. Its age is the liveness signal.", chain, nil),
		firstBlockTimeout: prometheus.NewDesc(name("consensus_first_block_timeout_seconds"),
			"Current leader-window first-block timeout, as scaled by the skip ladder.", chain, nil),
		standstills: prometheus.NewDesc(name("consensus_standstills_total"),
			"Standstill alarms fired by the consensus engine.", chain, nil),
		slotsFinalized: prometheus.NewDesc(name("consensus_slots_finalized_total"),
			"Slots finalized by the consensus engine.", chain, nil),
		certificates: prometheus.NewDesc(name("consensus_certificates_total"),
			"Certificates stored, by vote kind. A skip and a finalize certificate are opposite "+
				"facts about a session.", []string{"chain", "kind"}, nil),
		certificateSignatures: prometheus.NewDesc(name("consensus_certificate_signatures_total"),
			"Signatures carried by those certificates. Divided by the certificate count it is the "+
				"mean quorum size.", []string{"chain", "kind"}, nil),
		votes: prometheus.NewDesc(name("consensus_votes_total"),
			"Local voting decisions by outcome.", []string{"chain", "outcome"}, nil),
		validatorLastSigned: prometheus.NewDesc(name("consensus_validator_last_signed_slot"),
			"Highest slot at which each validator's signature appeared in a stored certificate of "+
				"the current session. Resets per session, because the index means a different node "+
				"after a rotation.", []string{"chain", "validator_index"}, nil),
	}
}

func (c *consensusCollector) register(key ConsensusSessionKey, source ConsensusSessionSource) {
	if source == nil {
		return
	}

	c.mu.Lock()
	c.sessions[key] = source
	// A re-registration of the same key is the same session announcing itself
	// twice, not a newer one, so its place in the order is the one it already
	// has.
	if _, known := c.registered[key]; !known {
		c.nextSeq++
		c.registered[key] = c.nextSeq
	}
	c.mu.Unlock()
}

// unregister takes one last reading before dropping the session, so the work it
// did between the final scrape and its retirement is not lost from the totals.
func (c *consensusCollector) unregister(key ConsensusSessionKey) {
	c.mu.Lock()
	source := c.sessions[key]
	c.mu.Unlock()
	if source == nil {
		return
	}
	stats := source.ConsensusStats()

	c.mu.Lock()
	defer c.mu.Unlock()
	c.accumulateLocked(key, stats)
	delete(c.sessions, key)
	delete(c.registered, key)
	delete(c.last, key)
}

func (c *consensusCollector) accumulateLocked(key ConsensusSessionKey, stats ConsensusSessionStats) {
	chain := boundedValidationChain(stats.Chain)
	current := sessionCounters(stats.Stats)
	c.totals[chain].addDelta(c.last[key], current)
	c.last[key] = current
}

func (c *consensusCollector) Describe(ch chan<- *prometheus.Desc) {
	for _, desc := range c.descs() {
		ch <- desc
	}
}

func (c *consensusCollector) descs() []*prometheus.Desc {
	return []*prometheus.Desc{
		c.slot, c.finalizedSlot, c.lineageAnchorSlot, c.retentionLagSlots, c.retentionCapped,
		c.retentionBudgetBytes, c.retainedPayloads, c.lastFinalization, c.firstBlockTimeout,
		c.standstills, c.slotsFinalized, c.certificates, c.certificateSignatures, c.votes,
		c.validatorLastSigned,
	}
}

func (c *consensusCollector) Collect(ch chan<- prometheus.Metric) {
	c.mu.Lock()
	sessions := make(map[ConsensusSessionKey]ConsensusSessionSource, len(c.sessions))
	order := make(map[ConsensusSessionKey]uint64, len(c.sessions))
	for key, source := range c.sessions {
		sessions[key] = source
		order[key] = c.registered[key]
	}
	c.mu.Unlock()

	// Every ConsensusStats call is an atomic load and a mutex the consensus
	// loop never holds across work, so this cannot block on a stalled session.
	readings := make(map[ConsensusSessionKey]ConsensusSessionStats, len(sessions))
	for key, source := range sessions {
		readings[key] = source.ConsensusStats()
	}

	c.mu.Lock()
	for key, stats := range readings {
		c.accumulateLocked(key, stats)
	}
	for key := range c.last {
		if _, live := sessions[key]; !live {
			// Its contribution is already in the totals.
			delete(c.last, key)
		}
	}
	totals := c.totals
	c.mu.Unlock()

	chains := [...]string{"masterchain", "shardchain"}
	kinds := map[simplex.VoteKind]string{
		simplex.VoteNotarize: "notarize",
		simplex.VoteFinalize: "finalize",
		simplex.VoteSkip:     "skip",
	}
	outcomes := [...]string{"cast", "accepted", "rejected", "abstained"}
	for chain := range totals {
		label := chains[chain]
		counters := totals[chain]
		ch <- prometheus.MustNewConstMetric(c.standstills, prometheus.CounterValue,
			float64(counters.standstills), label)
		ch <- prometheus.MustNewConstMetric(c.slotsFinalized, prometheus.CounterValue,
			float64(counters.slotsFinalized), label)
		for kind, kindLabel := range kinds {
			ch <- prometheus.MustNewConstMetric(c.certificates, prometheus.CounterValue,
				float64(counters.certificates[kind]), label, kindLabel)
			ch <- prometheus.MustNewConstMetric(c.certificateSignatures, prometheus.CounterValue,
				float64(counters.signatures[kind]), label, kindLabel)
		}
		for outcome, outcomeLabel := range outcomes {
			ch <- prometheus.MustNewConstMetric(c.votes, prometheus.CounterValue,
				float64(counters.votes[outcome]), label, outcomeLabel)
		}
	}

	// Gauges describe a position, and two sessions of one chain overlap only
	// across a rotation. The one registered last is the answer; see the type
	// comment for why the slot cannot be.
	newest := map[collator.MetricChain]ConsensusSessionStats{}
	newestSeq := map[collator.MetricChain]uint64{}
	for key, stats := range readings {
		chain := boundedValidationChain(stats.Chain)
		if seq, exists := newestSeq[chain]; exists && seq >= order[key] {
			continue
		}
		newest[chain] = stats
		newestSeq[chain] = order[key]
	}
	for chain, stats := range newest {
		label := chains[chain]
		ch <- prometheus.MustNewConstMetric(c.slot, prometheus.GaugeValue,
			float64(stats.Stats.Slot), label)
		if stats.Stats.HasFinalized {
			ch <- prometheus.MustNewConstMetric(c.finalizedSlot, prometheus.GaugeValue,
				float64(stats.Stats.FinalizedSlot), label)
		}
		if !stats.Stats.LastFinalizedAt.IsZero() {
			ch <- prometheus.MustNewConstMetric(c.lastFinalization, prometheus.GaugeValue,
				float64(stats.Stats.LastFinalizedAt.UnixNano())/1e9, label)
		}
		if stats.Stats.FirstBlockTimeout > 0 {
			ch <- prometheus.MustNewConstMetric(c.firstBlockTimeout, prometheus.GaugeValue,
				stats.Stats.FirstBlockTimeout.Seconds(), label)
		}
		if stats.Retention.AnchorKnown {
			ch <- prometheus.MustNewConstMetric(c.lineageAnchorSlot, prometheus.GaugeValue,
				float64(stats.Retention.AnchorSlot), label)
			lag := uint32(0)
			if stats.Stats.HasFinalized && stats.Stats.FinalizedSlot > stats.Retention.AnchorSlot {
				lag = stats.Stats.FinalizedSlot - stats.Retention.AnchorSlot
			}
			ch <- prometheus.MustNewConstMetric(c.retentionLagSlots, prometheus.GaugeValue,
				float64(lag), label)
		}
		capped := float64(0)
		if stats.Retention.Capped {
			capped = 1
		}
		ch <- prometheus.MustNewConstMetric(c.retentionCapped, prometheus.GaugeValue, capped, label)
		ch <- prometheus.MustNewConstMetric(c.retentionBudgetBytes, prometheus.GaugeValue,
			float64(stats.Retention.BudgetBytes), label)
		ch <- prometheus.MustNewConstMetric(c.retainedPayloads, prometheus.GaugeValue,
			float64(stats.Retention.RetainedPayloads), label)

		if len(stats.Stats.LastSignedSlot) > consensusValidatorGaugeMaxRoster {
			continue
		}
		for index, slot := range stats.Stats.LastSignedSlot {
			ch <- prometheus.MustNewConstMetric(c.validatorLastSigned, prometheus.GaugeValue,
				float64(slot), label, strconv.Itoa(index))
		}
	}
}
