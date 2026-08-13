package localnet

import (
	"path/filepath"
	"testing"
	"time"

	"github.com/xssnick/gton/service/shard"
)

func TestBuildReportReadsCapturedRangeAfterEndGenerationRotates(t *testing.T) {
	directory := t.TempDir()
	node := NodeConfig{Name: "go-0", Kind: "go", LogPath: filepath.Join(directory, "go.jsonl")}
	appendLogRangeEvent(t, node.LogPath, 100)
	start, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	appendLogRangeEvent(t, node.LogPath, 1)
	rotateLogRangeTest(t, node.LogPath, "2026-08-13T01-02-03.004", true)
	appendLogRangeEvent(t, node.LogPath, 2)
	end, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}

	rotateLogRangeTest(t, node.LogPath, "2026-08-13T01-02-04.005", true)
	appendLogRangeEvent(t, node.LogPath, 3)
	manifest := RunManifest{
		Version:      runManifestVersion,
		Scenario:     "load",
		StartedAt:    time.Now().UTC(),
		FinishedAt:   time.Now().UTC(),
		Config:       Config{Nodes: []NodeConfig{node}},
		Baseline:     Baseline{Nodes: []NodeBaseline{{Name: node.Name, Log: start}}},
		EndPositions: map[string]LogPosition{node.Name: end},
		Load:         LoadResult{Outcome: loadOutcomeComplete},
	}
	if err = writeJSON(filepath.Join(directory, "manifest.json"), manifest); err != nil {
		t.Fatal(err)
	}

	summary, err := BuildReport(directory)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Events != 2 || len(summary.Nodes) != 1 || summary.Nodes[0].CollatedBlocks != 2 {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestEvaluateTopologyFullCycleAndParity(t *testing.T) {
	left, err := shard.Child(shard.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(shard.Root, false)
	if err != nil {
		t.Fatal(err)
	}
	nodes := []NodeConfig{
		{Name: "go-0", Kind: "go"},
		{Name: "go-2", Kind: "go"},
		{Name: "cpp", Kind: "cpp"},
	}
	events := []Event{
		{Node: "go-0", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root-1"},
		{Node: "go-0", Kind: "block_collated", Workchain: 0, Shard: shard.Root, CandidateHash: "go-root"},
		{Node: "go-0", Kind: "block_validated", Workchain: 0, Shard: shard.Root, CandidateHash: "cpp-candidate"},
		{Node: "go-0", Kind: "group_stopped", Workchain: 0, Shard: shard.Root, SessionID: "root-1"},
		{Node: "go-0", Kind: "group_started", Workchain: 0, Shard: left, SessionID: "left-1"},
		{Node: "go-0", Kind: "group_started", Workchain: 0, Shard: right, SessionID: "right-1"},
		{Node: "go-0", Kind: "block_collated", Workchain: 0, Shard: left},
		{Node: "go-0", Kind: "block_collated", Workchain: 0, Shard: right},
		{Node: "go-0", Kind: "group_started", Workchain: 0, Shard: left, SessionID: "left-2"},
		{Node: "go-0", Kind: "group_stopped", Workchain: 0, Shard: left, SessionID: "left-2"},
		{Node: "go-0", Kind: "group_stopped", Workchain: 0, Shard: right, SessionID: "right-1"},
		{Node: "go-0", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root-2"},
		{Node: "go-0", Kind: "block_collated", Workchain: 0, Shard: shard.Root},
		{Node: "go-0", Kind: "block_collated", Workchain: 0, Shard: shard.Root},
		{Node: "go-2", Kind: "block_collated", Workchain: 0, Shard: shard.Root, CandidateHash: "go-2"},
		{Node: "go-2", Kind: "block_validated", Workchain: 0, Shard: shard.Root, CandidateHash: "cpp-candidate"},
		{Node: "cpp", Kind: "block_collated", Workchain: 0, Shard: shard.Root, CandidateHash: "cpp-candidate"},
	}

	coverage := evaluateTopology(events, nodes)
	if !coverage.Complete {
		t.Fatalf("coverage incomplete: %+v", coverage)
	}
	if !coverage.CppToGoValidated || !coverage.RequiredNodeCoverage {
		t.Fatalf("parity coverage = %+v", coverage)
	}
}

func TestEvaluateTopologyDoesNotClaimUncorrelatedCPPParity(t *testing.T) {
	nodes := []NodeConfig{{Name: "go", Kind: "go"}, {Name: "cpp", Kind: "cpp"}}
	events := []Event{
		{Node: "cpp", Kind: "block_collated", CandidateHash: "cpp"},
		{Node: "go", Kind: "block_collated", CandidateHash: "go"},
		{Node: "go", Kind: "block_validated", CandidateHash: "other"},
	}
	coverage := evaluateTopology(events, nodes)
	if coverage.CppToGoValidated || coverage.Complete {
		t.Fatalf("false parity coverage: %+v", coverage)
	}
}

func TestEvaluateTopologyRequiresOrderedCycle(t *testing.T) {
	left, _ := shard.Child(shard.Root, true)
	right, _ := shard.Child(shard.Root, false)
	nodes := []NodeConfig{{Name: "go", Kind: "go"}, {Name: "cpp", Kind: "cpp"}}
	events := []Event{
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root-1"},
		{Node: "go", Kind: "block_collated", Workchain: 0, Shard: shard.Root},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root-2"},
		{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: shard.Root, SessionID: "root-2"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: left, SessionID: "left-1"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: right, SessionID: "right-1"},
		{Node: "go", Kind: "block_collated", Workchain: 0, Shard: left},
		{Node: "go", Kind: "block_collated", Workchain: 0, Shard: right},
	}
	coverage := evaluateTopology(events, nodes)
	if coverage.Rotation || coverage.Complete {
		t.Fatalf("pre-split rotation satisfied ordered cycle: %+v", coverage)
	}
}

func TestEvaluateTopologyTracksRotationWithinSplitGeneration(t *testing.T) {
	left, err := shard.Child(shard.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(shard.Root, false)
	if err != nil {
		t.Fatal(err)
	}
	base := []Event{
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root-1"},
		{Node: "go", Kind: "block_collated", Workchain: 0, Shard: shard.Root},
		{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: shard.Root, SessionID: "root-1"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: left, SessionID: "left-a"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: right, SessionID: "right-b"},
		{Node: "go", Kind: "block_collated", Workchain: 0, Shard: left},
		{Node: "go", Kind: "block_collated", Workchain: 0, Shard: right},
		{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: left, SessionID: "left-a"},
		{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: right, SessionID: "right-b"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root-2"},
		{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: shard.Root, SessionID: "root-2"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: left, SessionID: "left-c"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: right, SessionID: "right-d"},
	}
	coverage := evaluateTopology(base, []NodeConfig{{Name: "go", Kind: "go"}})
	if coverage.Rotation || coverage.Merge {
		t.Fatalf("merge and re-split changed rotation coverage: %+v", coverage)
	}
	tests := []struct {
		name         string
		beforeMerge  []Event
		wantRotation bool
		wantMerge    bool
	}{
		{
			name: "merge and re-split do not replace a child session",
			beforeMerge: []Event{
				{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: left, SessionID: "left-c"},
				{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: right, SessionID: "right-d"},
			},
		},
		{
			name: "promotion replaces a child session before merge",
			beforeMerge: []Event{
				{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: left, SessionID: "left-c"},
				{Node: "go", Kind: "group_promoted", Workchain: 0, Shard: left, SessionID: "left-e"},
				{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: left, SessionID: "left-e"},
				{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: right, SessionID: "right-d"},
			},
			wantRotation: true,
			wantMerge:    true,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			events := append([]Event{}, base...)
			events = append(events, test.beforeMerge...)
			events = append(events, Event{
				Node: "go", Kind: "group_started", Workchain: 0,
				Shard: shard.Root, SessionID: "root-3",
			})

			coverage := evaluateTopology(events, []NodeConfig{{Name: "go", Kind: "go"}})
			if coverage.Rotation != test.wantRotation || coverage.Merge != test.wantMerge {
				t.Fatalf("rotation=%t merge=%t, want rotation=%t merge=%t; coverage=%+v",
					coverage.Rotation, coverage.Merge, test.wantRotation, test.wantMerge, coverage)
			}
		})
	}
}

func TestTopologyCycleTreatsDeliveryShortfallAsAdvisory(t *testing.T) {
	load := LoadResult{
		ExitCode: 1,
		Outcome:  loadOutcomeDeliveryIncomplete,
		Advisory: true,
	}
	if loadStopsScenario("topology-cycle", load) {
		t.Fatal("topology-cycle stopped on delivery-only shortfall")
	}
	if got := scenarioVerdict("topology-cycle", "passed", load); got != "passed" {
		t.Fatalf("topology-cycle verdict = %q, want passed", got)
	}
	if !loadStopsScenario("full-cycle", load) {
		t.Fatal("strict full-cycle ignored delivery shortfall")
	}
	if got := scenarioVerdict("full-cycle", "passed", load); got != "failed" {
		t.Fatalf("full-cycle verdict = %q, want failed", got)
	}
}

func TestTopologyCycleKeepsFailedBatchHard(t *testing.T) {
	load := LoadResult{
		ExitCode:      1,
		Outcome:       loadOutcomeFailed,
		HardFailure:   true,
		FailedBatches: 1,
	}
	if !loadStopsScenario("topology-cycle", load) {
		t.Fatal("topology-cycle ignored failed batch")
	}
	if got := scenarioVerdict("topology-cycle", "passed", load); got != "failed" {
		t.Fatalf("topology-cycle verdict = %q, want failed", got)
	}
}

func TestTopologyIgnoresDiscardedFutureGroups(t *testing.T) {
	left, _ := shard.Child(shard.Root, true)
	right, _ := shard.Child(shard.Root, false)
	events := []Event{
		{Node: "go", Kind: "group_discarded", Workchain: 0, Shard: left, SessionID: "future-left"},
		{Node: "go", Kind: "group_discarded", Workchain: 0, Shard: right, SessionID: "future-right"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root"},
	}
	coverage := evaluateTopology(events, []NodeConfig{{Name: "go", Kind: "go"}})
	if coverage.Split || coverage.Merge || coverage.Rotation {
		t.Fatalf("discarded future groups changed topology: %+v", coverage)
	}
}
