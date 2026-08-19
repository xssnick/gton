package validator

import (
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus"
	dto "github.com/prometheus/client_model/go"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

type consensusStatsSource struct {
	stats ConsensusSessionStats
}

func (s *consensusStatsSource) ConsensusStats() ConsensusSessionStats { return s.stats }

func consensusTestFamilies(t *testing.T, registry *prometheus.Registry) map[string]*dto.MetricFamily {
	t.Helper()

	families, err := registry.Gather()
	if err != nil {
		t.Fatal(err)
	}
	byName := make(map[string]*dto.MetricFamily, len(families))
	for _, family := range families {
		byName[family.GetName()] = family
	}

	return byName
}

func consensusGaugeValue(family *dto.MetricFamily, labels map[string]string) (float64, bool) {
	if family == nil {
		return 0, false
	}
	for _, metric := range family.Metric {
		matched := 0
		for _, pair := range metric.Label {
			if labels[pair.GetName()] == pair.GetValue() {
				matched++
			}
		}
		if matched == len(labels) {
			return metric.GetGauge().GetValue(), true
		}
	}

	return 0, false
}

// The consensus state itself had no metrics at all: not the slot, not the age
// of the last finalization, not the skip certificates, not who is still
// signing. Every value here is one the engine already computed and kept to
// itself, which is why a five-hour standstill had to be reconstructed from
// half a million log lines.
func TestConsensusCollectorExportsSessionState(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusValidationMetrics(validatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	observer := metrics.Observer()

	finalizedAt := time.Now().Add(-90 * time.Second)
	source := &consensusStatsSource{stats: ConsensusSessionStats{
		Chain: collator.MetricChainShardchain,
		Stats: simplex.Stats{
			Standstills:         9,
			SlotsFinalized:      4,
			OwnVotesCast:        21,
			CandidatesAbstained: 5,
			Slot:                468,
			FinalizedSlot:       392,
			HasFinalized:        true,
			LastFinalizedAt:     finalizedAt,
			FirstBlockTimeout:   28 * time.Second,
			LastSignedSlot:      []uint32{468, 468, 12},
		},
		Retention: ConsensusRetentionStats{
			AnchorSlot: 392, AnchorKnown: true, Capped: true,
			BudgetBytes: retentionFloorCapBytes, RetainedPayloads: 56, RetainedBytes: 27 << 20,
		},
	}}
	source.stats.Stats.CertificatesByKind[simplex.VoteSkip] = 64
	source.stats.Stats.CertificatesByKind[simplex.VoteFinalize] = 4
	source.stats.Stats.SignaturesByKind[simplex.VoteSkip] = 192

	key := ConsensusSessionKey{SessionID: [32]byte{0x11}}
	observer.RegisterConsensusSession(key, source)
	observer.ObserveLineageWalk(LineageWalkObservation{
		Chain:      collator.MetricChainShardchain,
		Result:     LineageWalkSuccess,
		Candidates: 5,
		Duration:   60 * time.Second,
		Steps:      [lineageStepSourceCount]int{LineageStepMemory: 4, LineageStepStorage: 1},
	})

	byName := consensusTestFamilies(t, registry)
	shard := map[string]string{"chain": "shardchain"}
	for name, want := range map[string]float64{
		"gton_validator_consensus_slot":                        468,
		"gton_validator_consensus_finalized_slot":              392,
		"gton_validator_consensus_lineage_anchor_slot":         392,
		"gton_validator_consensus_retention_lag_slots":         0,
		"gton_validator_consensus_retention_capped":            1,
		"gton_validator_consensus_retained_payloads":           56,
		"gton_validator_consensus_retention_budget_bytes":      float64(retentionFloorCapBytes),
		"gton_validator_consensus_first_block_timeout_seconds": 28,
	} {
		got, exists := consensusGaugeValue(byName[name], shard)
		if !exists {
			t.Fatalf("%s was not exported", name)
		}
		if got != want {
			t.Fatalf("%s = %v, want %v", name, got, want)
		}
	}
	// The lag is reported and is deliberately not what bounds anything: here it
	// is the skipped distance between the anchor and the finalized slot.
	if got, _ := consensusGaugeValue(byName["gton_validator_consensus_retention_lag_slots"], shard); got != 0 {
		t.Fatalf("retention lag = %v, want 0 for an anchor at the finalized slot", got)
	}
	age, exists := consensusGaugeValue(
		byName["gton_validator_consensus_last_finalization_timestamp_seconds"], shard)
	if !exists || time.Since(time.Unix(int64(age), 0)) < 80*time.Second {
		t.Fatalf("last finalization timestamp = %v, want about 90 s ago", age)
	}
	for index, want := range map[string]float64{"0": 468, "1": 468, "2": 12} {
		got, exists := consensusGaugeValue(
			byName["gton_validator_consensus_validator_last_signed_slot"],
			map[string]string{"chain": "shardchain", "validator_index": index},
		)
		if !exists || got != want {
			t.Fatalf("validator %s last signed slot = %v (exported=%v), want %v", index, got, exists, want)
		}
	}
	for _, check := range []struct {
		name   string
		labels map[string]string
		want   float64
	}{
		{"gton_validator_consensus_certificates_total", map[string]string{"chain": "shardchain", "kind": "skip"}, 64},
		{"gton_validator_consensus_certificates_total", map[string]string{"chain": "shardchain", "kind": "finalize"}, 4},
		{"gton_validator_consensus_certificate_signatures_total", map[string]string{"chain": "shardchain", "kind": "skip"}, 192},
		{"gton_validator_consensus_standstills_total", shard, 9},
		{"gton_validator_consensus_slots_finalized_total", shard, 4},
		{"gton_validator_consensus_votes_total", map[string]string{"chain": "shardchain", "outcome": "abstained"}, 5},
		{"gton_validator_lineage_walk_steps_total", map[string]string{"chain": "shardchain", "source": "storage"}, 1},
		{"gton_validator_lineage_walk_steps_total", map[string]string{"chain": "shardchain", "source": "memory"}, 4},
	} {
		if got := validatorCounterValue(byName[check.name], check.labels); got != check.want {
			t.Fatalf("%s%v = %v, want %v", check.name, check.labels, got, check.want)
		}
	}
	if byName["gton_validator_lineage_walk_candidates"] == nil ||
		byName["gton_validator_lineage_walk_duration_seconds"] == nil {
		t.Fatal("lineage walk depth and duration were not exported")
	}

	// A session retires at every rotation, and the work it did is work the
	// process did: its counters must not fall back to zero with it.
	source.stats.Stats.Standstills = 11
	observer.UnregisterConsensusSession(key)
	byName = consensusTestFamilies(t, registry)
	if got := validatorCounterValue(byName["gton_validator_consensus_standstills_total"], shard); got != 11 {
		t.Fatalf("standstills after the session retired = %v, want its final 11", got)
	}
	if _, exists = consensusGaugeValue(byName["gton_validator_consensus_slot"], shard); exists {
		t.Fatal("a retired session still reports a consensus slot")
	}
}

// Two sessions of one chain overlap across a rotation, and the gauges describe
// a position, so they must all come from the incoming one.
//
// The trap this pins is that a consensus slot is per session and starts again
// at zero on every rotation — a five-hour run of the test network went through
// about sixteen of them — so during the overlap the outgoing session is the one
// with the larger slot. Selecting on it hands the whole gauge set to the
// session that is about to disappear: the slot, the finalization age, the
// retention floor, and every per-validator participation series, all of them
// describing the wrong session at the one moment a rotation is the likeliest
// explanation for what an operator is reading.
func TestConsensusCollectorPrefersTheNewerSessionForGauges(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusValidationMetrics(validatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	observer := metrics.Observer()

	finalizedAt := time.Now().Add(-3 * time.Second)
	outgoing := &consensusStatsSource{stats: ConsensusSessionStats{
		Chain: collator.MetricChainMasterchain,
		Stats: simplex.Stats{
			Slot: 4096, FinalizedSlot: 4090, HasFinalized: true, SlotsFinalized: 90,
			LastFinalizedAt: time.Now().Add(-9 * time.Minute),
			LastSignedSlot:  []uint32{4090, 4090},
		},
		Retention: ConsensusRetentionStats{
			AnchorSlot: 4000, AnchorKnown: true, Capped: true,
			BudgetBytes: 1, RetainedPayloads: 4096,
		},
	}}
	incoming := &consensusStatsSource{stats: ConsensusSessionStats{
		Chain: collator.MetricChainMasterchain,
		Stats: simplex.Stats{
			Slot: 7, FinalizedSlot: 5, HasFinalized: true, SlotsFinalized: 3,
			LastFinalizedAt: finalizedAt,
			LastSignedSlot:  []uint32{5, 4},
		},
		Retention: ConsensusRetentionStats{
			AnchorSlot: 4, AnchorKnown: true, Capped: false,
			BudgetBytes: retentionFloorCapMasterchainBytes, RetainedPayloads: 2,
		},
	}}
	// Registration order is the rotation: the incoming session is announced
	// while the outgoing one is still live and still reporting a far larger slot.
	observer.RegisterConsensusSession(ConsensusSessionKey{SessionID: [32]byte{0x01}}, outgoing)
	observer.RegisterConsensusSession(ConsensusSessionKey{SessionID: [32]byte{0x02}}, incoming)

	byName := consensusTestFamilies(t, registry)
	master := map[string]string{"chain": "masterchain"}
	for name, want := range map[string]float64{
		"gton_validator_consensus_slot":                   7,
		"gton_validator_consensus_finalized_slot":         5,
		"gton_validator_consensus_lineage_anchor_slot":    4,
		"gton_validator_consensus_retention_lag_slots":    1,
		"gton_validator_consensus_retention_capped":       0,
		"gton_validator_consensus_retention_budget_bytes": float64(retentionFloorCapMasterchainBytes),
		"gton_validator_consensus_retained_payloads":      2,
	} {
		got, exists := consensusGaugeValue(byName[name], master)
		if !exists {
			t.Fatalf("%s was not exported", name)
		}
		if got != want {
			t.Fatalf("%s = %v, want the incoming session's %v", name, got, want)
		}
	}
	age, _ := consensusGaugeValue(
		byName["gton_validator_consensus_last_finalization_timestamp_seconds"], master)
	if time.Since(time.Unix(int64(age), 0)) > time.Minute {
		t.Fatalf("last finalization timestamp is %v old, want the incoming session's few seconds",
			time.Since(time.Unix(int64(age), 0)))
	}
	// The per-validator series is the one where reading the wrong session names
	// the wrong node: index 1 is at 4 in the incoming session and at 4090 in the
	// outgoing one.
	got, exists := consensusGaugeValue(
		byName["gton_validator_consensus_validator_last_signed_slot"],
		map[string]string{"chain": "masterchain", "validator_index": "1"},
	)
	if !exists || got != 4 {
		t.Fatalf("validator 1 last signed slot = %v (exported=%v), want the incoming session's 4",
			got, exists)
	}

	// Counters, unlike gauges, are the sum of both.
	if got := validatorCounterValue(byName["gton_validator_consensus_slots_finalized_total"], master); got != 93 {
		t.Fatalf("finalized slots = %v, want both sessions' 93", got)
	}

	// The outgoing session retires; the gauges stay with the survivor rather
	// than reverting to whatever the map hands over first.
	observer.UnregisterConsensusSession(ConsensusSessionKey{SessionID: [32]byte{0x01}})
	byName = consensusTestFamilies(t, registry)
	if got, _ := consensusGaugeValue(byName["gton_validator_consensus_slot"], master); got != 7 {
		t.Fatalf("consensus slot after the rotation completed = %v, want 7", got)
	}
}
