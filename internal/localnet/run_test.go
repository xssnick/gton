package localnet

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestFinalizeCanceledRunPersistsLoadOffsetsAndReport(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "go.jsonl")
	logLine := `{"message":"block collated","workchain":0,"shard":-9223372036854775808,"block_seqno":42}` + "\n"
	if err := os.WriteFile(logPath, nil, 0o600); err != nil {
		t.Fatal(err)
	}
	node := NodeConfig{Name: "go-0", Kind: "go", LogPath: logPath}
	baselinePosition, err := captureLogPosition(node)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(logPath, []byte(logLine), 0o600); err != nil {
		t.Fatal(err)
	}
	started := time.Now().UTC().Add(-time.Minute)
	manifest := RunManifest{
		Version:   runManifestVersion,
		Scenario:  "topology-cycle",
		StartedAt: started,
		Config: Config{Nodes: []NodeConfig{{
			Name:    "go-0",
			Kind:    "go",
			LogPath: logPath,
		}}},
		Baseline: Baseline{
			CapturedAt: started,
			Nodes:      []NodeBaseline{{Name: "go-0", Log: baselinePosition}},
		},
		Load: LoadResult{
			Outcome:           loadOutcomeDeliveryIncomplete,
			HardFailure:       true,
			ExitCode:          1,
			Submitted:         100,
			Accepted:          12,
			IncompleteSenders: []uint64{0},
			Senders:           []SenderResult{},
		},
	}

	summary, err := finalizeCanceledRun(directory, &manifest, context.Canceled)
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("finalize error = %v, want context cancellation", err)
	}
	stored, err := readManifest(directory)
	if err != nil {
		t.Fatal(err)
	}
	if stored.Load.Outcome != loadOutcomeDeliveryIncomplete || stored.Load.Submitted != 100 ||
		stored.Load.Accepted != 12 {
		t.Fatalf("stored load = %+v", stored.Load)
	}
	if stored.FinishedAt.IsZero() || stored.EndPositions["go-0"].Offset != int64(len(logLine)) {
		t.Fatalf("stored manifest = %+v", stored)
	}
	if summary.RunDirectory != directory || summary.LoadDeliveryStatus != loadOutcomeDeliveryIncomplete ||
		summary.ConsensusVerdict != "inconclusive" {
		t.Fatalf("summary = %+v", summary)
	}
	if _, statErr := os.Stat(filepath.Join(directory, "summary.json")); statErr != nil {
		t.Fatalf("summary file: %v", statErr)
	}
}

func TestFinalizeCanceledRunReportsIncompleteFinalization(t *testing.T) {
	directory := t.TempDir()
	manifest := RunManifest{
		Version:   runManifestVersion,
		Scenario:  "topology-cycle",
		StartedAt: time.Now().UTC(),
		Config: Config{Nodes: []NodeConfig{{
			Name:    "missing",
			Kind:    "go",
			LogPath: filepath.Join(directory, "missing.jsonl"),
		}}},
		Baseline: Baseline{Nodes: []NodeBaseline{{
			Name: "missing", Log: LogPosition{FileID: "missing", CapturedAt: time.Now().UTC()},
		}}},
		Load: LoadResult{
			Outcome:           loadOutcomeComplete,
			IncompleteSenders: []uint64{},
			Senders:           []SenderResult{},
		},
	}

	summary, err := finalizeCanceledRun(directory, &manifest, context.Canceled)
	if !errors.Is(err, context.Canceled) || !strings.Contains(err.Error(), "capture log position") {
		t.Fatalf("finalize error = %v", err)
	}
	if summary.RunDirectory != directory || summary.Load.Outcome != loadOutcomeComplete {
		t.Fatalf("partial summary = %+v", summary)
	}
	stored, readErr := readManifest(directory)
	if readErr != nil {
		t.Fatal(readErr)
	}
	if stored.Load.Outcome != loadOutcomeComplete || stored.FinishedAt.IsZero() {
		t.Fatalf("stored manifest = %+v", stored)
	}
}

func TestExecuteLoadSetsUpSequentiallyBeforeParallelRun(t *testing.T) {
	directory := t.TempDir()
	trace := filepath.Join(directory, "trace")
	script := filepath.Join(directory, "load.sh")
	body := `#!/bin/sh
phase="$1"
shift
sender=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--sender-index" ]; then sender="$2"; shift 2; continue; fi
  shift
done
if [ "$phase" = "run" ] && [ "$(grep -c '^setup ' '` + trace + `' 2>/dev/null)" -ne 3 ]; then exit 9; fi
echo "$phase $sender" >> '` + trace + `'
echo "{\"schema_version\":1,\"command\":\"$phase\",\"outcome\":\"complete\",\"contract_profile\":\"test\",\"minter_code_hash\":\"aa\",\"wallet_code_hash\":\"bb\",\"sender_index\":$sender,\"submitted\":0,\"accepted\":0,\"failed_batches\":0,\"external_batches\":0,\"rpc_accepted_batches\":0,\"canary_submitted\":0,\"canary_accepted\":0,\"undelivered\":0,\"submitted_tps\":0,\"minter\":\"minter\",\"highload_wallet\":\"highload\",\"source_jetton_wallet\":\"source\"}"
`
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	load := LoadConfig{
		Binary: script, NodeConfig: "node.json", LiteAddress: "127.0.0.1:1",
		StatePath: filepath.Join(directory, "state-{sender}.json"), FirstSender: 4,
		SenderCount: 3, SetupBeforeRun: true, Rate: 1,
		Duration: Duration{Duration: time.Second},
	}
	result := executeLoadForTest(context.Background(), load, directory)
	if result.ExitCode != 0 {
		t.Fatalf("load result = %+v", result)
	}
	data, err := os.ReadFile(trace)
	if err != nil {
		t.Fatal(err)
	}
	lines := strings.Fields(string(data))
	if len(lines) != 12 {
		t.Fatalf("trace = %q", data)
	}
	for i, sender := range []string{"4", "5", "6"} {
		if lines[i*2] != "setup" || lines[i*2+1] != sender {
			t.Fatalf("setup trace = %q", data)
		}
	}
}

func TestRunCapturesBaselineAfterSetupAndRecoversAfterSemanticFailure(t *testing.T) {
	directory := t.TempDir()
	logPath := filepath.Join(directory, "go.jsonl")
	initial := `{"message":"next-block masterchain head applied","workchain":-1,"seqno":100}` + "\n"
	if err := os.WriteFile(logPath, []byte(initial), 0o600); err != nil {
		t.Fatal(err)
	}

	script := filepath.Join(directory, "load.sh")
	body := `#!/bin/sh
phase="$1"
shift
sender=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--sender-index" ]; then sender="$2"; shift 2; continue; fi
  shift
done
if [ "$phase" = "setup" ]; then
  echo '{"message":"next-block masterchain head applied","workchain":-1,"seqno":101}' >> '` + logPath + `'
  echo "{\"schema_version\":1,\"command\":\"setup\",\"outcome\":\"complete\",\"contract_profile\":\"test\",\"minter_code_hash\":\"aa\",\"wallet_code_hash\":\"bb\",\"sender_index\":$sender,\"submitted\":0,\"accepted\":0,\"failed_batches\":0,\"external_batches\":0,\"rpc_accepted_batches\":0,\"canary_submitted\":0,\"canary_accepted\":0,\"undelivered\":0,\"submitted_tps\":0,\"minter\":\"minter\",\"highload_wallet\":\"highload\",\"source_jetton_wallet\":\"source\"}"
  exit 0
fi
echo '{"message":"next-block masterchain head applied","workchain":-1,"seqno":102}' >> '` + logPath + `'
echo "{\"schema_version\":1,\"command\":\"run\",\"outcome\":\"delivery_incomplete\",\"error\":\"drain timeout\",\"failure_stage\":\"delivery\",\"sender_index\":$sender,\"submitted\":1,\"accepted\":0,\"failed_batches\":0,\"external_batches\":1,\"rpc_accepted_batches\":1,\"canary_submitted\":1,\"canary_accepted\":1,\"undelivered\":1,\"submitted_tps\":1}"
exit 1
`
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}

	const recovery = 25 * time.Millisecond
	cfg := Config{
		RunRoot: filepath.Join(directory, "runs"),
		Nodes: []NodeConfig{{
			Name: "go", Kind: "go", Roles: []string{nodeRoleProducer},
			LogPath: logPath, TMUXSession: "unused",
		}},
		Load: LoadConfig{
			Binary: script, NodeConfig: "node.json", LiteAddress: "unused",
			StatePath: filepath.Join(directory, "state.json"), SetupBeforeRun: true,
			Rate: 1, Duration: Duration{Duration: time.Millisecond}, Settle: Duration{Duration: recovery},
		},
		Conditions: ConditionsConfig{MinMasterchainAdvance: 1},
	}

	summary, runErr := executeRun(context.Background(), cfg, "load")
	if runErr == nil || summary.Load.Outcome != loadOutcomeDeliveryIncomplete {
		t.Fatalf("Run result: summary=%+v error=%v", summary, runErr)
	}
	if len(summary.Nodes) != 1 || summary.Nodes[0].MasterchainStart != 101 ||
		summary.Nodes[0].MasterchainEnd != 102 {
		t.Fatalf("baseline includes setup activity: %+v", summary.Nodes)
	}
	if summary.Phases.Setup == nil || summary.Phases.Load == nil || summary.Phases.Recovery == nil {
		t.Fatalf("phase markers = %+v", summary.Phases)
	}
	setupEnd := summary.Phases.Setup.EndPositions["go"]
	loadStart := summary.Phases.Load.StartPositions["go"]
	if setupEnd.FileID != loadStart.FileID || setupEnd.Offset != loadStart.Offset {
		t.Fatalf("load baseline %v does not start at setup end %v", loadStart, setupEnd)
	}
	if elapsed := summary.Phases.Recovery.FinishedAt.Sub(summary.Phases.Recovery.StartedAt); elapsed < recovery {
		t.Fatalf("semantic failure recovery = %s, want at least %s", elapsed, recovery)
	}
}

func TestClassifyLoadProcessDeliveryIncomplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "load.stdout.json")
	if err := writeJSON(path, LoadEvidence{
		SchemaVersion: 1, Command: "run", Outcome: loadOutcomeDeliveryIncomplete,
		Error: "drain timeout", FailureStage: "delivery", SenderIndex: 7,
		Submitted: 13500, Accepted: 1121, ExternalBatches: 900,
		RPCAcceptedBatches: 900, Undelivered: 12379, SubmittedTPS: 30,
		RunEpoch: "00112233445566778899aabbccddeeff",
	}); err != nil {
		t.Fatal(err)
	}
	result, err := classifyLoadProcess(path, "run", 7, 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != loadOutcomeDeliveryIncomplete || result.Submitted != 13500 ||
		result.Accepted != 1121 || result.FailedBatches != 0 ||
		result.RunEpoch != "00112233445566778899aabbccddeeff" {
		t.Fatalf("classified result = %+v", result)
	}
}

func TestClassifyLoadProcessKeepsFailedBatchesHard(t *testing.T) {
	path := filepath.Join(t.TempDir(), "load.stdout.json")
	if err := writeJSON(path, LoadEvidence{
		SchemaVersion: 1, Command: "run", Outcome: loadOutcomeSubmissionFailed,
		Error: "batch submission failed", FailureStage: "submit", SenderIndex: 3,
		Submitted: 100, Accepted: 0, FailedBatches: 1, ExternalBatches: 10,
		RPCAcceptedBatches: 9, Undelivered: 100, SubmittedTPS: 10,
	}); err != nil {
		t.Fatal(err)
	}
	result, err := classifyLoadProcess(path, "run", 3, 1)
	if err != nil {
		t.Fatal(err)
	}
	if result.Outcome != loadOutcomeSubmissionFailed || result.FailedBatches != 1 {
		t.Fatalf("classified result = %+v", result)
	}
}

func TestClassifyLoadProcessRejectsInvalidStructuredEvidence(t *testing.T) {
	valid := `{"schema_version":1,"command":"run","outcome":"complete","contract_profile":"test","minter_code_hash":"aa","wallet_code_hash":"bb","sender_index":5,"submitted":0,"accepted":0,"failed_batches":0,"external_batches":0,"rpc_accepted_batches":0,"canary_submitted":0,"canary_accepted":0,"undelivered":0,"submitted_tps":0}`
	tests := []struct {
		name string
		data string
	}{
		{name: "missing required counter", data: strings.Replace(valid, `,"accepted":0`, "", 1)},
		{name: "null required counter", data: strings.Replace(valid, `"accepted":0`, `"accepted":null`, 1)},
		{name: "unknown field", data: strings.TrimSuffix(valid, "}") + `,"extra":1}`},
		{name: "multiple values", data: valid + "\n" + valid},
		{name: "missing contract evidence", data: strings.Replace(valid, `,"contract_profile":"test"`, "", 1)},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "load.stdout.json")
			if err := os.WriteFile(path, []byte(test.data), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := classifyLoadProcess(path, "run", 5, 0); err == nil {
				t.Fatal("invalid structured result was accepted")
			}
		})
	}
}

func TestExecuteLoadAggregatesDeliveryIncompleteAsSemanticFailure(t *testing.T) {
	directory := t.TempDir()
	script := filepath.Join(directory, "load.sh")
	body := `#!/bin/sh
sender=""
while [ "$#" -gt 0 ]; do
  if [ "$1" = "--sender-index" ]; then sender="$2"; shift 2; continue; fi
  shift
done
echo "{\"schema_version\":1,\"command\":\"run\",\"outcome\":\"delivery_incomplete\",\"error\":\"drain timeout\",\"failure_stage\":\"delivery\",\"sender_index\":$sender,\"submitted\":100,\"accepted\":12,\"failed_batches\":0,\"external_batches\":10,\"rpc_accepted_batches\":10,\"canary_submitted\":1,\"canary_accepted\":1,\"undelivered\":88,\"submitted_tps\":10}"
exit 1
`
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	result := executeLoadForTest(context.Background(), LoadConfig{
		Binary:      script,
		NodeConfig:  "node.json",
		LiteAddress: "127.0.0.1:1",
		StatePath:   filepath.Join(directory, "state-{sender}.json"),
		FirstSender: 3,
		SenderCount: 2,
		Rate:        1,
		Duration:    Duration{Duration: time.Second},
	}, directory)
	if result.Outcome != loadOutcomeDeliveryIncomplete || !result.HardFailure {
		t.Fatalf("load result = %+v", result)
	}
	if result.Submitted != 200 || result.Accepted != 24 ||
		len(result.IncompleteSenders) != 2 || result.IncompleteSenders[0] != 3 ||
		result.IncompleteSenders[1] != 4 {
		t.Fatalf("load aggregate = %+v", result)
	}
	if result.OutcomeCounts[loadOutcomeDeliveryIncomplete] != 2 || result.FailureStages["delivery"] != 2 {
		t.Fatalf("load failure categories = %+v / %+v", result.OutcomeCounts, result.FailureStages)
	}
}

func TestExecuteLoadTreatsLaunchFailureAsHard(t *testing.T) {
	directory := t.TempDir()
	result := executeLoadForTest(context.Background(), LoadConfig{
		Binary:      filepath.Join(directory, "missing-load-binary"),
		NodeConfig:  "node.json",
		LiteAddress: "127.0.0.1:1",
		StatePath:   filepath.Join(directory, "state.json"),
		Rate:        1,
		Duration:    Duration{Duration: time.Second},
	}, directory)
	if !result.HardFailure || result.Outcome != loadOutcomeFailed {
		t.Fatalf("load result = %+v", result)
	}
}

func executeLoadForTest(ctx context.Context, load LoadConfig, runDirectory string) LoadResult {
	result := newLoadResult(load)
	if load.SetupBeforeRun && !executeLoadSetup(ctx, load, runDirectory, &result) {
		return result
	}
	executeLoadRun(ctx, load, runDirectory, &result)
	return result
}

func TestDeploymentCommandAddsSafeLoggingDefaults(t *testing.T) {
	node := NodeConfig{
		Name: "go-0", LogPath: "/tmp/go-0.jsonl",
		StartCommand: []string{"/bin/bash", "-lc", "cd /srv/node && exec {binary} --config config.json"},
	}
	command := deploymentCommand("/srv/bin/gton-node", node)
	if len(command) != 3 || !strings.Contains(command[2], "--verbosity' 'info") || !strings.Contains(command[2], "--log-json") || !strings.Contains(command[2], "--log-file") {
		t.Fatalf("deployment command = %#v", command)
	}
}

func TestTmuxShellCommandPreservesExitDiagnostic(t *testing.T) {
	command := tmuxShellCommand([]string{"/srv/gton node", "--config", "it's.json"})
	for _, required := range []string{"'/srv/gton node'", `'it'"'"'s.json'`, "process exited status=%s", `exit "$status"`} {
		if !strings.Contains(command, required) {
			t.Fatalf("tmux command %q does not contain %q", command, required)
		}
	}
}

func TestPersistDeployDiagnostic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "deploy", "node.log")
	if err := persistDeployDiagnostic(path, 2, 1, "address already in use"); err != nil {
		t.Fatal(err)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	text := string(data)
	if !strings.Contains(text, "attempt=2 exit_status=1") || !strings.Contains(text, "address already in use") {
		t.Fatalf("diagnostic = %q", text)
	}
}
