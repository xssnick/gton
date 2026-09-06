package localnet

import (
	"os"
	"path/filepath"
	"slices"
	"strings"
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

func TestBuildReportSeparatesBuildsFromUniqueEmissions(t *testing.T) {
	directory := t.TempDir()
	node := NodeConfig{
		Name: "go-collator", Kind: "go", Roles: []string{nodeRoleProducer},
		LogPath: filepath.Join(directory, "go.jsonl"),
	}
	if err := os.WriteFile(node.LogPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	start, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Join([]string{
		`{"message":"block collated","session_id":"session","slot":0,"candidate_hash":"candidate"}`,
		`{"message":"candidate emitted","session_id":"session","slot":0,"candidate_hash":"candidate","block_root_hash":"root","block_file_hash":"file"}`,
		`{"message":"candidate emitted","session_id":"session","slot":0,"candidate_hash":"candidate","block_root_hash":"root","block_file_hash":"file","replayed":true}`,
	}, "\n") + "\n"
	if err = os.WriteFile(node.LogPath, []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	end, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	manifest := RunManifest{
		Version: runManifestVersion, Scenario: "load", StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(),
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
	if summary.Events != 3 || len(summary.Nodes) != 1 || summary.Nodes[0].CollatedBlocks != 1 ||
		summary.Nodes[0].EmittedCandidates != 1 ||
		!slices.Equal(summary.Topology.ProducerNodes, []string{"go-collator"}) {
		t.Fatalf("summary = %+v", summary)
	}
}

func TestBuildReportDeduplicatesNodeBlockCountersAndPreservesRawEvents(t *testing.T) {
	directory := t.TempDir()
	node := NodeConfig{Name: "cpp", Kind: "cpp", LogPath: filepath.Join(directory, "cpp.log")}
	if err := os.WriteFile(node.LogPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	start, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	candidate := "AAECAwQFBgcICQoLDA0ODxAREhMUFRYXGBkaGxwdHh8="
	block := "(0,4000000000000000,9):" + strings.Repeat("A", 64) + ":" + strings.Repeat("B", 64)
	lines := strings.Join([]string{
		"Published event CandidateReceived {candidate=Candidate{id={17, " + candidate + ", ?}, parent=consensus genesis, block=BlockCandidate{id=" + block + "}}}",
		"Published event TraceEvent {event=CandidateReceived{id={17, " + candidate + ", ?}, parent=consensus genesis, block_id=" + block + "}}",
		"Published event FinalizeBlock {block=" + block + "}",
		"Published event FinalizeBlock {block=" + block + "}",
	}, "\n") + "\n"
	if err = os.WriteFile(node.LogPath, []byte(lines), 0o600); err != nil {
		t.Fatal(err)
	}
	end, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	manifest := RunManifest{
		Version: runManifestVersion, Scenario: "load", StartedAt: time.Now().UTC(), FinishedAt: time.Now().UTC(),
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
	if summary.Events != 4 || len(summary.Nodes) != 1 || summary.Nodes[0].ValidatedBlocks != 1 ||
		summary.Nodes[0].FinalizedBlocks != 1 {
		t.Fatalf("summary = %+v", summary)
	}
	rawEvents, err := os.ReadFile(filepath.Join(directory, "events.ndjson"))
	if err != nil {
		t.Fatal(err)
	}
	if strings.Count(string(rawEvents), "\n") != 4 ||
		strings.Count(string(rawEvents), `"kind":"block_validated"`) != 2 ||
		strings.Count(string(rawEvents), `"kind":"block_finalized"`) != 2 {
		t.Fatalf("events.ndjson lost raw duplicate evidence: %s", rawEvents)
	}
}

func TestBuildReportLoadScenarioFailsIncompleteRequiredRolesAndCandidateFlow(t *testing.T) {
	directory := t.TempDir()
	goNode := NodeConfig{
		Name: "go-collator", Kind: "go", Roles: []string{nodeRoleProducer},
		LogPath: filepath.Join(directory, "go.jsonl"),
	}
	cppNode := NodeConfig{
		Name: "cpp", Kind: "cpp", Roles: []string{nodeRoleValidator, nodeRoleFinalizer},
		LogPath: filepath.Join(directory, "cpp.log"),
	}
	for _, node := range []NodeConfig{goNode, cppNode} {
		if err := os.WriteFile(node.LogPath, nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	goStart, err := captureLogPosition(goNode)
	if err != nil {
		t.Fatal(err)
	}
	cppStart, err := captureLogPosition(cppNode)
	if err != nil {
		t.Fatal(err)
	}
	goEvent := `{"message":"candidate emitted","candidate_hash":"candidate","block_root_hash":"root","block_file_hash":"file"}` + "\n"
	if err = os.WriteFile(goNode.LogPath, []byte(goEvent), 0o600); err != nil {
		t.Fatal(err)
	}
	goEnd, err := captureLogPosition(goNode)
	if err != nil {
		t.Fatal(err)
	}
	cppEnd, err := captureLogPosition(cppNode)
	if err != nil {
		t.Fatal(err)
	}

	manifest := RunManifest{
		Version:    runManifestVersion,
		Scenario:   "load",
		StartedAt:  time.Now().UTC(),
		FinishedAt: time.Now().UTC(),
		Config: Config{
			Nodes: []NodeConfig{goNode, cppNode},
			Conditions: ConditionsConfig{CandidateFlows: []CandidateFlowConfig{{
				Producer: "go-collator", Validators: []string{"cpp"}, Finalizers: []string{"cpp"},
			}}},
		},
		Baseline: Baseline{Nodes: []NodeBaseline{
			{Name: goNode.Name, Log: goStart},
			{Name: cppNode.Name, Log: cppStart},
		}},
		EndPositions: map[string]LogPosition{
			goNode.Name:  goEnd,
			cppNode.Name: cppEnd,
		},
		Load: LoadResult{Outcome: loadOutcomeComplete},
	}
	if err = writeJSON(filepath.Join(directory, "manifest.json"), manifest); err != nil {
		t.Fatal(err)
	}

	summary, err := BuildReport(directory)
	if err != nil {
		t.Fatal(err)
	}
	if summary.Verdict != "failed" || summary.ConsensusVerdict != "failed" {
		t.Fatalf("load verdict=%q consensus=%q, want failed", summary.Verdict, summary.ConsensusVerdict)
	}
	checks := make(map[string]Check, len(summary.Checks))
	for _, check := range summary.Checks {
		checks[check.Name] = check
	}
	roleCheck, exists := checks["required_role_coverage"]
	if !exists || roleCheck.Passed || !strings.Contains(roleCheck.Actual, "validators=[]") ||
		!strings.Contains(roleCheck.Actual, "finalizers=[]") {
		t.Fatalf("required role check = %+v, exists=%t", roleCheck, exists)
	}
	flowCheck, exists := checks["candidate_flow_0"]
	if !exists || flowCheck.Passed || !strings.Contains(flowCheck.Actual, "validated_by=[]") ||
		!strings.Contains(flowCheck.Actual, "finalized_by=[]") ||
		!strings.Contains(flowCheck.Actual, "validator_candidate_hash:cpp") ||
		!strings.Contains(flowCheck.Wanted, "validators=[cpp] finalizers=[cpp] complete=true") {
		t.Fatalf("candidate flow check = %+v, exists=%t", flowCheck, exists)
	}
}

func TestEvaluateTopologyFullCycleAndDirectedCandidateFlow(t *testing.T) {
	left, err := shard.Child(shard.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(shard.Root, false)
	if err != nil {
		t.Fatal(err)
	}
	cfg := Config{
		Nodes: []NodeConfig{
			{Name: "go-collator", Kind: "go", Roles: []string{nodeRoleProducer}},
			{Name: "cpp", Kind: "cpp", Roles: []string{nodeRoleValidator, nodeRoleFinalizer}},
		},
		Conditions: ConditionsConfig{CandidateFlows: []CandidateFlowConfig{{
			Producer: "go-collator", Validators: []string{"cpp"}, Finalizers: []string{"cpp"},
		}}},
	}
	events := []Event{
		{Node: "go-collator", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root-1"},
		{Node: "go-collator", Kind: "candidate_emitted", Workchain: 0, Shard: shard.Root, CandidateHash: "go-root", BlockRootHash: "root", BlockFileHash: "file"},
		{Node: "cpp", Kind: "block_validated", Workchain: 0, Shard: shard.Root, CandidateHash: "go-root"},
		{Node: "cpp", Kind: "block_finalized", Workchain: 0, Shard: shard.Root, BlockRootHash: "root", BlockFileHash: "file"},
		{Node: "go-collator", Kind: "group_stopped", Workchain: 0, Shard: shard.Root, SessionID: "root-1"},
		{Node: "go-collator", Kind: "group_started", Workchain: 0, Shard: left, SessionID: "left-1"},
		{Node: "go-collator", Kind: "group_started", Workchain: 0, Shard: right, SessionID: "right-1"},
		{Node: "go-collator", Kind: "candidate_emitted", Workchain: 0, Shard: left},
		{Node: "go-collator", Kind: "candidate_emitted", Workchain: 0, Shard: right},
		{Node: "go-collator", Kind: "group_started", Workchain: 0, Shard: left, SessionID: "left-2"},
		{Node: "go-collator", Kind: "group_stopped", Workchain: 0, Shard: left, SessionID: "left-2"},
		{Node: "go-collator", Kind: "group_stopped", Workchain: 0, Shard: right, SessionID: "right-1"},
		{Node: "go-collator", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root-2"},
		{Node: "go-collator", Kind: "candidate_emitted", Workchain: 0, Shard: shard.Root},
		{Node: "go-collator", Kind: "candidate_emitted", Workchain: 0, Shard: shard.Root},
	}

	coverage := evaluateTopology(events, cfg)
	if !coverage.Complete {
		t.Fatalf("coverage incomplete: %+v", coverage)
	}
	if !coverage.RequiredRoleCoverage || len(coverage.CandidateFlows) != 1 || !coverage.CandidateFlows[0].Complete {
		t.Fatalf("parity coverage = %+v", coverage)
	}
}

func TestEvaluateTopologyDoesNotClaimUncorrelatedCandidateFlow(t *testing.T) {
	cfg := Config{
		Nodes: []NodeConfig{
			{Name: "go", Kind: "go", Roles: []string{nodeRoleProducer}},
			{Name: "cpp", Kind: "cpp", Roles: []string{nodeRoleValidator, nodeRoleFinalizer}},
		},
		Conditions: ConditionsConfig{CandidateFlows: []CandidateFlowConfig{{
			Producer: "go", Validators: []string{"cpp"}, Finalizers: []string{"cpp"},
		}}},
	}
	events := []Event{
		{Node: "go", Kind: "candidate_emitted", Workchain: 0, Shard: shard.Root, Seqno: 42, CandidateHash: "go", BlockRootHash: "root", BlockFileHash: "file"},
		{Node: "cpp", Kind: "block_validated", Workchain: 0, Shard: shard.Root, Seqno: 42, CandidateHash: "other"},
		{Node: "cpp", Kind: "block_finalized", Workchain: 0, Shard: shard.Root, Seqno: 42, BlockRootHash: "other-root", BlockFileHash: "other-file"},
	}
	coverage := evaluateTopology(events, cfg)
	if coverage.CandidateFlows[0].Complete || coverage.Complete {
		t.Fatalf("false parity coverage: %+v", coverage)
	}
}

func TestEvaluateTopologyRequiresHashForValidationAndBlockIDForFinalization(t *testing.T) {
	cfg := Config{
		Nodes: []NodeConfig{
			{Name: "go", Kind: "go", Roles: []string{nodeRoleProducer}},
			{Name: "cpp", Kind: "cpp", Roles: []string{nodeRoleValidator, nodeRoleFinalizer}},
		},
		Conditions: ConditionsConfig{CandidateFlows: []CandidateFlowConfig{{
			Producer: "go", Validators: []string{"cpp"}, Finalizers: []string{"cpp"},
		}}},
	}
	events := []Event{
		{Node: "go", Kind: "candidate_emitted", CandidateHash: "candidate", BlockRootHash: "root", BlockFileHash: "file"},
		{Node: "cpp", Kind: "block_validated", CandidateHash: "candidate"},
		{Node: "cpp", Kind: "block_finalized", BlockRootHash: "root", BlockFileHash: "file"},
	}

	coverage := evaluateTopology(events, cfg)
	if !coverage.RequiredRoleCoverage || !coverage.CandidateFlows[0].Complete {
		t.Fatalf("directed candidate evidence was not matched: %+v", coverage)
	}
	if slices.Contains(coverage.ProducerNodes, "cpp") {
		t.Fatalf("validate-only C++ node was treated as a producer: %+v", coverage)
	}
}

func TestEvaluateTopologyAllowsSeparateCPPFinalizerAndValidateOnlyNode(t *testing.T) {
	cfg := Config{
		Nodes: []NodeConfig{
			{Name: "go-collator", Kind: "go", Roles: []string{nodeRoleProducer}},
			{Name: "cpp-finalizer", Kind: "cpp", Roles: []string{nodeRoleValidator, nodeRoleFinalizer}},
			{Name: "cpp-validator", Kind: "cpp", Roles: []string{nodeRoleValidator}},
		},
		Conditions: ConditionsConfig{CandidateFlows: []CandidateFlowConfig{{
			Producer:   "go-collator",
			Validators: []string{"cpp-finalizer", "cpp-validator"},
			Finalizers: []string{"cpp-finalizer"},
		}}},
	}
	events := []Event{
		{Node: "go-collator", Kind: "candidate_emitted", CandidateHash: "candidate", BlockRootHash: "root", BlockFileHash: "file"},
		{Node: "cpp-finalizer", Kind: "block_validated", CandidateHash: "candidate"},
		{Node: "cpp-validator", Kind: "block_validated", CandidateHash: "candidate"},
		{Node: "cpp-finalizer", Kind: "block_finalized", BlockRootHash: "root", BlockFileHash: "file"},
	}

	coverage := evaluateTopology(events, cfg)
	if !coverage.RequiredRoleCoverage || !coverage.CandidateFlows[0].Complete {
		t.Fatalf("directed C++ role coverage = %+v", coverage)
	}
	if !slices.Equal(coverage.ProducerNodes, []string{"go-collator"}) {
		t.Fatalf("producer nodes = %v, want standalone Go collator only", coverage.ProducerNodes)
	}
}

func TestEvaluateTopologyReportsMissingDirectedEvidence(t *testing.T) {
	cfg := Config{
		Nodes: []NodeConfig{
			{Name: "go", Kind: "go", Roles: []string{nodeRoleProducer}},
			{Name: "cpp", Kind: "cpp", Roles: []string{nodeRoleValidator, nodeRoleFinalizer}},
		},
		Conditions: ConditionsConfig{CandidateFlows: []CandidateFlowConfig{{
			Producer: "go", Validators: []string{"cpp"}, Finalizers: []string{"cpp"},
		}}},
	}
	events := []Event{
		{Node: "go", Kind: "candidate_emitted", CandidateHash: "candidate"},
		{Node: "cpp", Kind: "block_validated", CandidateHash: "candidate"},
		{Node: "cpp", Kind: "block_finalized", Seqno: 42},
	}

	flow := evaluateTopology(events, cfg).CandidateFlows[0]
	if flow.Complete || !slices.Contains(flow.MissingEvidence, "producer_block_id") ||
		!slices.Contains(flow.MissingEvidence, "finalizer_block_id:cpp") {
		t.Fatalf("missing evidence was not reported: %+v", flow)
	}
}

func TestCandidateFlowMatchesSlotAndHashWhenBothAreAvailable(t *testing.T) {
	producerSlot := uint32(4)
	otherSlot := uint32(5)
	produced := Event{
		Node: "go", Kind: "candidate_emitted", Slot: &producerSlot, CandidateHash: "candidate",
		BlockRootHash: "root", BlockFileHash: "file",
	}
	finalized := Event{Node: "cpp", Kind: "block_finalized", BlockRootHash: "root", BlockFileHash: "file"}
	flow := CandidateFlowConfig{Producer: "go", Validators: []string{"cpp"}, Finalizers: []string{"cpp"}}

	coverage := evaluateCandidateFlow([]Event{
		produced,
		{Node: "cpp", Kind: "block_validated", Slot: &otherSlot, CandidateHash: "candidate"},
		finalized,
	}, flow)
	if coverage.Complete || len(coverage.ValidatedBy) != 0 {
		t.Fatalf("same hash at a different known slot matched: %+v", coverage)
	}

	coverage = evaluateCandidateFlow([]Event{
		produced,
		{Node: "cpp", Kind: "block_validated", Slot: &producerSlot, CandidateHash: "candidate"},
		finalized,
	}, flow)
	if !coverage.Complete || !slices.Equal(coverage.ValidatedBy, []string{"cpp"}) {
		t.Fatalf("same hash and slot did not match: %+v", coverage)
	}

	coverage = evaluateCandidateFlow([]Event{
		produced,
		{Node: "cpp", Kind: "block_validated", CandidateHash: "candidate"},
		finalized,
	}, flow)
	if !coverage.Complete {
		t.Fatalf("hash-only fallback did not match unavailable consumer slot: %+v", coverage)
	}
}

func TestTopologyUsesUniqueEmissionsNotBuildsOrReplays(t *testing.T) {
	slot := uint32(7)
	cfg := Config{Nodes: []NodeConfig{{Name: "go", Roles: []string{nodeRoleProducer}}}}
	events := []Event{
		{Node: "go", Kind: "block_collated", Workchain: 0, Shard: shard.Root, Slot: &slot, CandidateHash: "candidate"},
		{Node: "go", Kind: "candidate_emitted", Workchain: 0, Shard: shard.Root, SessionID: "session", Slot: &slot, CandidateHash: "candidate"},
		{Node: "go", Kind: "candidate_emitted", Workchain: 0, Shard: shard.Root, SessionID: "session", Slot: &slot, CandidateHash: "candidate", Replayed: true},
	}

	coverage := evaluateTopology(events, cfg)
	if !coverage.RequiredRoleCoverage || !slices.Equal(coverage.ProducerNodes, []string{"go"}) || !coverage.LinearProof {
		t.Fatalf("unique emission was not authoritative producer evidence: %+v", coverage)
	}
	deduplicated := deduplicateCandidateEmissions(events)
	if len(deduplicated) != 2 || deduplicated[0].Kind != "block_collated" || deduplicated[1].Kind != "candidate_emitted" {
		t.Fatalf("deduplicated events = %+v, want build plus one emission", deduplicated)
	}

	buildOnly := evaluateTopology(events[:1], cfg)
	if buildOnly.RequiredRoleCoverage || len(buildOnly.ProducerNodes) != 0 || buildOnly.LinearProof {
		t.Fatalf("a built but un-emitted candidate counted as production: %+v", buildOnly)
	}
}

func TestEvaluateTopologyRequiresOrderedCycle(t *testing.T) {
	left, _ := shard.Child(shard.Root, true)
	right, _ := shard.Child(shard.Root, false)
	nodes := []NodeConfig{{Name: "go", Kind: "go"}, {Name: "cpp", Kind: "cpp"}}
	events := []Event{
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root-1"},
		{Node: "go", Kind: "candidate_emitted", Workchain: 0, Shard: shard.Root},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root-2"},
		{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: shard.Root, SessionID: "root-2"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: left, SessionID: "left-1"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: right, SessionID: "right-1"},
		{Node: "go", Kind: "candidate_emitted", Workchain: 0, Shard: left},
		{Node: "go", Kind: "candidate_emitted", Workchain: 0, Shard: right},
	}
	coverage := evaluateTopology(events, Config{Nodes: nodes})
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
		{Node: "go", Kind: "candidate_emitted", Workchain: 0, Shard: shard.Root},
		{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: shard.Root, SessionID: "root-1"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: left, SessionID: "left-a"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: right, SessionID: "right-b"},
		{Node: "go", Kind: "candidate_emitted", Workchain: 0, Shard: left},
		{Node: "go", Kind: "candidate_emitted", Workchain: 0, Shard: right},
		{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: left, SessionID: "left-a"},
		{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: right, SessionID: "right-b"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: shard.Root, SessionID: "root-2"},
		{Node: "go", Kind: "group_stopped", Workchain: 0, Shard: shard.Root, SessionID: "root-2"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: left, SessionID: "left-c"},
		{Node: "go", Kind: "group_started", Workchain: 0, Shard: right, SessionID: "right-d"},
	}
	coverage := evaluateTopology(base, Config{Nodes: []NodeConfig{{Name: "go", Kind: "go"}}})
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

			coverage := evaluateTopology(events, Config{Nodes: []NodeConfig{{Name: "go", Kind: "go"}}})
			if coverage.Rotation != test.wantRotation || coverage.Merge != test.wantMerge {
				t.Fatalf("rotation=%t merge=%t, want rotation=%t merge=%t; coverage=%+v",
					coverage.Rotation, coverage.Merge, test.wantRotation, test.wantMerge, coverage)
			}
		})
	}
}

func TestSemanticDeliveryFailureStopsEveryScenario(t *testing.T) {
	load := LoadResult{
		ExitCode:    1,
		Outcome:     loadOutcomeDeliveryIncomplete,
		HardFailure: true,
	}
	if !loadStopsScenario(load) {
		t.Fatal("topology-cycle ignored semantic delivery failure")
	}
	if got := scenarioVerdict("passed", load); got != "failed" {
		t.Fatalf("topology verdict = %q, want failed", got)
	}
	if got := scenarioVerdict("passed", LoadResult{Outcome: loadOutcomeWorkloadInvalid}); got != "failed" {
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
	if !loadStopsScenario(load) {
		t.Fatal("topology-cycle ignored failed batch")
	}
	if got := scenarioVerdict("passed", load); got != "failed" {
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
	coverage := evaluateTopology(events, Config{Nodes: []NodeConfig{{Name: "go", Kind: "go"}}})
	if coverage.Split || coverage.Merge || coverage.Rotation {
		t.Fatalf("discarded future groups changed topology: %+v", coverage)
	}
}
