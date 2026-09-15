package validator

import (
	"sync/atomic"
	"testing"

	"github.com/prometheus/client_golang/prometheus"

	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

// parkedConsensusStatsSource holds its first reading until released, parking a
// scrape between reading a session and adding up what it read.
type parkedConsensusStatsSource struct {
	stats   ConsensusSessionStats
	calls   atomic.Int32
	reading chan struct{}
	release chan struct{}
}

func (s *parkedConsensusStatsSource) ConsensusStats() ConsensusSessionStats {
	if s.calls.Add(1) == 1 {
		close(s.reading)
		<-s.release
	}

	return s.stats
}

// A session that retires while a scrape is reading it has already added its
// final reading to the totals. The scrape then added its own reading of the same
// session against the baseline the retirement had dropped, counting the whole
// session a second time and re-creating that baseline.
func TestConsensusCollectorScrapeDoesNotRecountARetiredSession(t *testing.T) {
	registry := prometheus.NewRegistry()
	metrics, err := NewPrometheusValidationMetrics(validatorMetricsTestRegistry{registry: registry})
	if err != nil {
		t.Fatal(err)
	}
	observer := metrics.Observer()

	source := &parkedConsensusStatsSource{
		stats: ConsensusSessionStats{
			Chain: collator.MetricChainShardchain,
			Stats: simplex.Stats{SlotsFinalized: 101, Standstills: 3},
		},
		reading: make(chan struct{}),
		release: make(chan struct{}),
	}
	key := ConsensusSessionKey{SessionID: [32]byte{0x30}}
	observer.RegisterConsensusSession(key, source)

	scraped := make(chan error, 1)
	go func() {
		_, gatherErr := registry.Gather()
		scraped <- gatherErr
	}()
	<-source.reading
	observer.UnregisterConsensusSession(key)
	close(source.release)
	if err = <-scraped; err != nil {
		t.Fatal(err)
	}

	byName := consensusTestFamilies(t, registry)
	shard := map[string]string{"chain": "shardchain"}
	if got := validatorCounterValue(byName["gton_validator_consensus_slots_finalized_total"], shard); got != 101 {
		t.Fatalf("finalized slots after a retirement raced a scrape = %v, want 101", got)
	}
	if got := validatorCounterValue(byName["gton_validator_consensus_standstills_total"], shard); got != 3 {
		t.Fatalf("standstills after a retirement raced a scrape = %v, want 3", got)
	}
}
