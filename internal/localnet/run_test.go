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
			Advisory:          true,
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
	result := executeLoad(context.Background(), load, directory)
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

func TestClassifyLoadProcessDeliveryIncomplete(t *testing.T) {
	path := filepath.Join(t.TempDir(), "load.stderr.log")
	diagnostics := "\x1b[32mINF\x1b[0m jetton load completed " +
		"accepted=1121 failed_batches=0 submitted=13500\n" +
		"ERR jetton load failed error=\"drain timeout after observing 1121 of 13500 transfers\"\n"
	if err := os.WriteFile(path, []byte(diagnostics), 0o600); err != nil {
		t.Fatal(err)
	}
	result := ProcessResult{ExitCode: 1, Outcome: loadOutcomeFailed}
	if err := classifyLoadProcess(path, &result); err != nil {
		t.Fatal(err)
	}
	if result.Outcome != loadOutcomeDeliveryIncomplete || result.Submitted != 13500 ||
		result.Accepted != 1121 || result.FailedBatches != 0 {
		t.Fatalf("classified result = %+v", result)
	}
}

func TestClassifyLoadProcessKeepsFailedBatchesHard(t *testing.T) {
	path := filepath.Join(t.TempDir(), "load.stderr.log")
	diagnostics := "jetton load completed accepted=100 failed_batches=1 submitted=100\n" +
		"jetton load failed error=\"only 100 of 100 submitted transfers were observed\"\n"
	if err := os.WriteFile(path, []byte(diagnostics), 0o600); err != nil {
		t.Fatal(err)
	}
	result := ProcessResult{ExitCode: 1, Outcome: loadOutcomeFailed}
	if err := classifyLoadProcess(path, &result); err != nil {
		t.Fatal(err)
	}
	if result.Outcome != loadOutcomeFailed || result.FailedBatches != 1 {
		t.Fatalf("classified result = %+v", result)
	}
}

func TestExecuteLoadAggregatesDeliveryIncompleteAsAdvisory(t *testing.T) {
	directory := t.TempDir()
	script := filepath.Join(directory, "load.sh")
	body := `#!/bin/sh
echo 'jetton load completed accepted=12 failed_batches=0 submitted=100' >&2
echo 'jetton load failed error="drain timeout after observing 12 of 100 transfers"' >&2
exit 1
`
	if err := os.WriteFile(script, []byte(body), 0o700); err != nil {
		t.Fatal(err)
	}
	result := executeLoad(context.Background(), LoadConfig{
		Binary:      script,
		NodeConfig:  "node.json",
		LiteAddress: "127.0.0.1:1",
		StatePath:   filepath.Join(directory, "state-{sender}.json"),
		FirstSender: 3,
		SenderCount: 2,
		Rate:        1,
		Duration:    Duration{Duration: time.Second},
	}, directory)
	if result.Outcome != loadOutcomeDeliveryIncomplete || !result.Advisory || result.HardFailure {
		t.Fatalf("load result = %+v", result)
	}
	if result.Submitted != 200 || result.Accepted != 24 ||
		len(result.IncompleteSenders) != 2 || result.IncompleteSenders[0] != 3 ||
		result.IncompleteSenders[1] != 4 {
		t.Fatalf("load aggregate = %+v", result)
	}
}

func TestExecuteLoadTreatsLaunchFailureAsHard(t *testing.T) {
	directory := t.TempDir()
	result := executeLoad(context.Background(), LoadConfig{
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
