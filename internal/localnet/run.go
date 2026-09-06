package localnet

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const runManifestVersion = 3

func Run(ctx context.Context, cfg Config, scenario string) (Summary, error) {
	if err := cfg.Validate(); err != nil {
		return Summary{}, err
	}
	if err := cfg.ValidateRun(); err != nil {
		return Summary{}, err
	}
	if scenario == "" {
		scenario = "load"
	}
	if !isRunScenario(scenario) {
		return Summary{}, fmt.Errorf("unsupported scenario %q", scenario)
	}
	if observesTopology(scenario) && cfg.Load.TopologyTimeout.Duration <= 0 {
		return Summary{}, errors.New("topology scenario requires a positive load.topology_timeout")
	}
	if observesTopology(scenario) && len(cfg.Conditions.CandidateFlows) == 0 {
		return Summary{}, errors.New("topology scenario requires at least one conditions.candidate_flows entry")
	}
	if _, err := Preflight(ctx, cfg); err != nil {
		return Summary{}, err
	}
	return executeRun(ctx, cfg, scenario)
}

func executeRun(ctx context.Context, cfg Config, scenario string) (Summary, error) {
	if err := os.MkdirAll(cfg.RunRoot, 0o755); err != nil {
		return Summary{}, fmt.Errorf("create run root: %w", err)
	}
	lockPath := filepath.Join(cfg.RunRoot, ".gton-lab.lock")
	lock, err := os.OpenFile(lockPath, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if errors.Is(err, os.ErrExist) {
		return Summary{}, errRunLocked
	}
	if err != nil {
		return Summary{}, fmt.Errorf("acquire run lock: %w", err)
	}
	_, _ = fmt.Fprintf(lock, "%d\n", os.Getpid())
	_ = lock.Close()
	defer os.Remove(lockPath)

	started := time.Now().UTC()
	runDirectory := filepath.Join(cfg.RunRoot, started.Format("20060102T150405Z")+"-"+safeName(scenario))
	if err = os.Mkdir(runDirectory, 0o755); err != nil {
		return Summary{}, fmt.Errorf("create run directory: %w", err)
	}

	manifest := RunManifest{
		Version: runManifestVersion, Scenario: scenario, StartedAt: started, Config: cfg,
		Load: newLoadResult(cfg.Load),
	}
	setupComplete := true
	if cfg.Load.SetupBeforeRun {
		manifest.Phases.Setup, err = beginRunPhase(cfg)
		if err != nil {
			return Summary{}, err
		}
		setupComplete = executeLoadSetup(ctx, cfg.Load, runDirectory, &manifest.Load)
		if err = finishRunPhase(cfg, manifest.Phases.Setup); err != nil {
			return Summary{}, err
		}
	}

	manifest.Baseline, err = captureBaseline(ctx, cfg)
	if err != nil {
		return Summary{}, err
	}
	if err = writeJSON(filepath.Join(runDirectory, "manifest.json"), manifest); err != nil {
		return Summary{}, err
	}
	if err = writeJSON(filepath.Join(runDirectory, "baseline.json"), manifest.Baseline); err != nil {
		return Summary{}, err
	}
	if !setupComplete {
		summary, reportErr := finalizeRun(runDirectory, &manifest)
		if reportErr != nil {
			return summary, reportErr
		}
		return summary, fmt.Errorf("scenario verdict: %s", summary.Verdict)
	}

	manifest.Phases.Load = beginRunPhaseFromBaseline(manifest.Baseline)
	executeLoadRun(ctx, cfg.Load, runDirectory, &manifest.Load)
	if err = finishRunPhase(cfg, manifest.Phases.Load); err != nil {
		return Summary{}, err
	}
	if cancelErr := ctx.Err(); cancelErr != nil {
		return finalizeCanceledRun(runDirectory, &manifest, cancelErr)
	}
	if cfg.Load.Settle.Duration > 0 && loadHasStructuredResult(manifest.Load) {
		manifest.Phases.Recovery, err = beginRunPhase(cfg)
		if err != nil {
			return Summary{}, err
		}
		if err = waitContext(ctx, cfg.Load.Settle.Duration); err != nil {
			return finalizeCanceledRun(runDirectory, &manifest, err)
		}
		if err = finishRunPhase(cfg, manifest.Phases.Recovery); err != nil {
			return Summary{}, err
		}
	}
	if loadStopsScenario(manifest.Load) || !observesTopology(scenario) {
		summary, reportErr := finalizeRun(runDirectory, &manifest)
		if reportErr != nil {
			return summary, reportErr
		}
		if summary.Verdict != "passed" {
			return summary, fmt.Errorf("scenario verdict: %s", summary.Verdict)
		}
		return summary, nil
	}

	manifest.Phases.Topology, err = beginRunPhase(cfg)
	if err != nil {
		return Summary{}, err
	}
	topologyDeadline := time.Now().Add(cfg.Load.TopologyTimeout.Duration)
	cursors := make(map[string]LogPosition, len(manifest.Baseline.Nodes))
	for _, node := range manifest.Baseline.Nodes {
		cursors[node.Name] = node.Log
	}
	observed := make([]Event, 0, 1024)
	for {
		if cancelErr := ctx.Err(); cancelErr != nil {
			return finalizeCanceledRun(runDirectory, &manifest, cancelErr)
		}
		manifest.EndPositions, err = captureLogPositions(cfg)
		if err != nil {
			return Summary{}, err
		}
		complete := false
		if observesTopology(scenario) {
			for _, node := range cfg.Nodes {
				nodeEvents, _, rangeErr := readLogRange(node, cursors[node.Name], manifest.EndPositions[node.Name])
				if rangeErr != nil {
					return Summary{}, rangeErr
				}
				observed = append(observed, nodeEvents...)
				cursors[node.Name] = manifest.EndPositions[node.Name]
			}
			complete = evaluateTopology(observed, cfg).Complete
		}
		if cancelErr := ctx.Err(); cancelErr != nil {
			return finalizeCanceledRun(runDirectory, &manifest, cancelErr)
		}
		if complete || !time.Now().Before(topologyDeadline) {
			if err = finishRunPhase(cfg, manifest.Phases.Topology); err != nil {
				return Summary{}, err
			}
			summary, reportErr := finalizeRun(runDirectory, &manifest)
			if reportErr != nil {
				return summary, reportErr
			}
			if summary.Verdict != "passed" {
				return summary, fmt.Errorf("scenario verdict: %s", summary.Verdict)
			}
			return summary, nil
		}
		if err = waitContext(ctx, time.Second); err != nil {
			return finalizeCanceledRun(runDirectory, &manifest, err)
		}
	}
}

func finalizeCanceledRun(
	runDirectory string,
	manifest *RunManifest,
	cancelErr error,
) (Summary, error) {
	summary, finalizeErr := finalizeRun(runDirectory, manifest)
	return summary, errors.Join(cancelErr, finalizeErr)
}

func finalizeRun(runDirectory string, manifest *RunManifest) (Summary, error) {
	positions, positionErr := captureLogPositions(manifest.Config)
	if positionErr == nil {
		manifest.EndPositions = positions
	}
	manifest.FinishedAt = time.Now().UTC()
	if err := writeJSON(filepath.Join(runDirectory, "manifest.json"), *manifest); err != nil {
		return partialRunSummary(runDirectory, *manifest), errors.Join(positionErr, err)
	}

	summary, reportErr := BuildReport(runDirectory)
	if reportErr != nil {
		return partialRunSummary(runDirectory, *manifest), errors.Join(positionErr, reportErr)
	}
	return summary, positionErr
}

func partialRunSummary(runDirectory string, manifest RunManifest) Summary {
	return Summary{
		RunDirectory:       runDirectory,
		Scenario:           manifest.Scenario,
		StartedAt:          manifest.StartedAt,
		FinishedAt:         manifest.FinishedAt,
		LoadDeliveryStatus: manifest.Load.Outcome,
		Load:               manifest.Load,
		Phases:             manifest.Phases,
	}
}

func captureBaseline(ctx context.Context, cfg Config) (Baseline, error) {
	status, err := CaptureStatus(ctx, cfg)
	if err != nil {
		return Baseline{}, err
	}
	baseline := Baseline{CapturedAt: status.CapturedAt, Nodes: make([]NodeBaseline, 0, len(status.Nodes))}
	for _, node := range cfg.Nodes {
		position, captureErr := captureLogPosition(node)
		if captureErr != nil {
			return Baseline{}, captureErr
		}
		masterchainSeqno, statusErr := statusMasterchainSeqno(status, node.Name)
		if statusErr != nil {
			return Baseline{}, statusErr
		}
		baseline.Nodes = append(baseline.Nodes, NodeBaseline{
			Name: node.Name, Log: position, MasterchainSeqno: masterchainSeqno,
		})
	}
	return baseline, nil
}

func captureLogPositions(cfg Config) (map[string]LogPosition, error) {
	positions := make(map[string]LogPosition, len(cfg.Nodes))
	for _, node := range cfg.Nodes {
		position, err := captureLogPosition(node)
		if err != nil {
			return nil, err
		}
		positions[node.Name] = position
	}
	return positions, nil
}

func beginRunPhase(cfg Config) (*RunPhase, error) {
	started := time.Now().UTC()
	positions, err := captureLogPositions(cfg)
	if err != nil {
		return nil, err
	}
	return &RunPhase{StartedAt: started, StartPositions: positions}, nil
}

func beginRunPhaseFromBaseline(baseline Baseline) *RunPhase {
	positions := make(map[string]LogPosition, len(baseline.Nodes))
	for _, node := range baseline.Nodes {
		positions[node.Name] = node.Log
	}
	return &RunPhase{StartedAt: time.Now().UTC(), StartPositions: positions}
}

func finishRunPhase(cfg Config, phase *RunPhase) error {
	positions, err := captureLogPositions(cfg)
	if err != nil {
		return err
	}
	phase.EndPositions = positions
	phase.FinishedAt = time.Now().UTC()
	return nil
}

func statusMasterchainSeqno(status Status, name string) (uint32, error) {
	for _, node := range status.Nodes {
		if node.Name == name {
			return node.MasterchainSeqno, nil
		}
	}
	return 0, fmt.Errorf("status has no node %q", name)
}

func newLoadResult(load LoadConfig) LoadResult {
	result := LoadResult{
		StartedAt:         time.Now().UTC(),
		ExitCode:          0,
		Outcome:           loadOutcomeComplete,
		IncompleteSenders: []uint64{},
	}
	count := load.SenderCount
	if count == 0 {
		count = 1
	}
	first := load.FirstSender
	result.Senders = make([]SenderResult, count)

	for i := range count {
		result.Senders[i] = SenderResult{
			SenderIndex: first + uint64(i),
			Setup:       ProcessResult{ExitCode: 0},
		}
	}
	return result
}

func executeLoadSetup(ctx context.Context, load LoadConfig, runDirectory string, result *LoadResult) bool {
	for i := range result.Senders {
		result.Senders[i].Setup = executeLoadProcess(ctx, load, runDirectory, "setup", result.Senders[i].SenderIndex, load.SetupExtraArgs)
		if !result.Senders[i].Setup.EvidenceValid || result.Senders[i].Setup.Outcome != loadOutcomeComplete {
			result.ExitCode = 1
			result.Outcome = loadOutcomeFailed
			result.HardFailure = true
			result.Error = "one or more sender setup processes failed"
			finishLoadResult(result)
			return false
		}
	}
	return true
}

func executeLoadRun(ctx context.Context, load LoadConfig, runDirectory string, result *LoadResult) {
	executeSenderPhase(ctx, load, runDirectory, result.Senders, "run", load.ExtraArgs)
	result.OutcomeCounts = make(map[string]int)
	result.FailureStages = make(map[string]int)

	for _, sender := range result.Senders {
		result.Submitted += sender.Load.Submitted
		result.Accepted += sender.Load.Accepted
		result.FailedBatches += sender.Load.FailedBatches
		result.OutcomeCounts[sender.Load.Outcome]++
		if sender.Load.FailureStage != "" {
			result.FailureStages[sender.Load.FailureStage]++
		}

		if !sender.Load.EvidenceValid {
			result.ExitCode = 1
			result.HardFailure = true
			result.FailureStages["result_protocol"]++
			continue
		}
		switch sender.Load.Outcome {
		case loadOutcomeDeliveryIncomplete:
			result.ExitCode = 1
			result.HardFailure = true
			result.IncompleteSenders = append(result.IncompleteSenders, sender.SenderIndex)
		case loadOutcomeExecutionFailed, loadOutcomeSubmissionFailed, loadOutcomeWorkloadInvalid, loadOutcomeFailed:
			result.ExitCode = 1
			result.HardFailure = true
		}
	}
	failureOutcomes := make([]string, 0, 5)
	for _, outcome := range []string{
		loadOutcomeDeliveryIncomplete,
		loadOutcomeExecutionFailed,
		loadOutcomeSubmissionFailed,
		loadOutcomeWorkloadInvalid,
		loadOutcomeFailed,
	} {
		if result.OutcomeCounts[outcome] > 0 {
			failureOutcomes = append(failureOutcomes, outcome)
		}
	}
	switch {
	case result.FailureStages["result_protocol"] > 0 || len(failureOutcomes) > 1:
		result.Outcome = loadOutcomeFailed
		result.Error = "load senders returned different failures or invalid result evidence"
	case len(failureOutcomes) == 1:
		result.Outcome = failureOutcomes[0]
		result.Error = "one or more load senders returned " + failureOutcomes[0]
	case !result.HardFailure:
		result.Outcome = loadOutcomeComplete
	default:
		result.Outcome = loadOutcomeFailed
		result.Error = "one or more load senders returned invalid result evidence"
	}
	finishLoadResult(result)
}

func finishLoadResult(result *LoadResult) {
	result.FinishedAt = time.Now().UTC()
	result.Duration = result.FinishedAt.Sub(result.StartedAt)
}

func loadHasStructuredResult(load LoadResult) bool {
	for _, sender := range load.Senders {
		if sender.Load.EvidenceValid {
			return true
		}
	}
	return false
}

func executeSenderPhase(ctx context.Context, load LoadConfig, runDirectory string, senders []SenderResult, phase string, extra []string) {
	var group sync.WaitGroup
	group.Add(len(senders))
	for i := range senders {
		go func(position int) {
			defer group.Done()
			process := executeLoadProcess(ctx, load, runDirectory, phase, senders[position].SenderIndex, extra)
			if phase == "setup" {
				senders[position].Setup = process
			} else {
				senders[position].Load = process
			}
		}(i)
	}
	group.Wait()
}

func executeLoadProcess(ctx context.Context, load LoadConfig, runDirectory, phase string, sender uint64, extra []string) (result ProcessResult) {
	result = ProcessResult{StartedAt: time.Now().UTC(), ExitCode: -1}
	defer func() {
		result.FinishedAt = time.Now().UTC()
		result.Duration = result.FinishedAt.Sub(result.StartedAt)
	}()
	prefix := fmt.Sprintf("load-%d-%s", sender, phase)
	stdoutPath := filepath.Join(runDirectory, prefix+".stdout.log")
	stdout, err := os.Create(stdoutPath)
	if err != nil {
		result.ProcessError = fmt.Sprintf("create stdout: %v", err)
		return result
	}
	stderr, err := os.Create(filepath.Join(runDirectory, prefix+".stderr.log"))
	if err != nil {
		_ = stdout.Close()
		result.ProcessError = fmt.Sprintf("create stderr: %v", err)
		return result
	}

	args := []string{
		phase,
		"--node-config", load.NodeConfig,
		"--lite", load.LiteAddress,
		"--state", strings.ReplaceAll(load.StatePath, "{sender}", strconv.FormatUint(sender, 10)),
		"--sender-index", strconv.FormatUint(sender, 10),
	}
	if phase == "run" {
		args = append(args,
			"--rate", strconv.Itoa(load.Rate),
			"--duration", load.Duration.String(),
			"--drain", load.Drain.String(),
		)
		if load.Recipients > 0 {
			args = append(args, "--recipients", strconv.Itoa(load.Recipients))
		}
		if load.MessageTON != "" {
			args = append(args, "--message-ton", load.MessageTON)
		}
		if load.Jettons != "" {
			args = append(args, "--jettons", load.Jettons)
		}
	}
	args = append(args, extra...)

	command := exec.CommandContext(ctx, load.Binary, args...)
	command.Dir = load.WorkDir
	command.Stdout = stdout
	command.Stderr = stderr
	if err = command.Run(); err != nil {
		result.ProcessError = err.Error()
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			result.ExitCode = exitError.ExitCode()
		}
	} else {
		result.ExitCode = 0
	}
	closeErr := errors.Join(stdout.Close(), stderr.Close())
	if closeErr != nil {
		if result.ProcessError == "" {
			result.ProcessError = closeErr.Error()
		} else {
			result.ProcessError = errors.Join(errors.New(result.ProcessError), closeErr).Error()
		}
	}
	evidence, evidenceErr := classifyLoadProcess(stdoutPath, phase, sender, result.ExitCode)
	if closeErr != nil {
		evidenceErr = errors.Join(evidenceErr, fmt.Errorf("close load result files: %w", closeErr))
	}
	result.LoadEvidence = evidence
	result.EvidenceValid = evidenceErr == nil
	if evidenceErr != nil {
		result.ProtocolError = evidenceErr.Error()
	}
	return result
}

func classifyLoadProcess(stdoutPath, phase string, sender uint64, exitCode int) (LoadEvidence, error) {
	data, err := os.ReadFile(stdoutPath)
	if err != nil {
		return LoadEvidence{}, fmt.Errorf("read load result: %w", err)
	}
	var fields map[string]json.RawMessage
	if err = json.Unmarshal(data, &fields); err != nil {
		return LoadEvidence{}, fmt.Errorf("decode load result fields: %w", err)
	}
	for _, name := range requiredLoadEvidenceFields {
		value, exists := fields[name]
		if !exists {
			return LoadEvidence{}, fmt.Errorf("load result has no %q field", name)
		}
		if bytes.Equal(bytes.TrimSpace(value), []byte("null")) {
			return LoadEvidence{}, fmt.Errorf("load result field %q is null", name)
		}
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	var evidence LoadEvidence
	if err = decoder.Decode(&evidence); err != nil {
		return LoadEvidence{}, fmt.Errorf("decode load result: %w", err)
	}
	var trailing json.RawMessage
	if err = decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			err = errors.New("multiple JSON values")
		}
		return evidence, fmt.Errorf("decode load result trailing data: %w", err)
	}
	if err = validateLoadEvidence(evidence, phase, sender, exitCode); err != nil {
		return evidence, err
	}
	return evidence, nil
}

var requiredLoadEvidenceFields = []string{
	"schema_version",
	"command",
	"outcome",
	"sender_index",
	"submitted",
	"accepted",
	"failed_batches",
	"external_batches",
	"rpc_accepted_batches",
	"canary_submitted",
	"canary_accepted",
	"undelivered",
	"submitted_tps",
}

func validateLoadEvidence(evidence LoadEvidence, phase string, sender uint64, exitCode int) error {
	if evidence.SchemaVersion != 1 {
		return fmt.Errorf("unsupported load result schema version %d", evidence.SchemaVersion)
	}
	if evidence.Command != phase {
		return fmt.Errorf("load result command %q does not match %q", evidence.Command, phase)
	}
	if evidence.SenderIndex != sender {
		return fmt.Errorf("load result sender %d does not match %d", evidence.SenderIndex, sender)
	}
	if evidence.Submitted < 0 || evidence.Accepted < 0 || evidence.FailedBatches < 0 ||
		evidence.ExternalBatches < 0 || evidence.RPCAcceptedBatches < 0 ||
		evidence.CanarySubmitted < 0 || evidence.CanaryAccepted < 0 || evidence.Undelivered < 0 ||
		evidence.SubmittedTPS < 0 {
		return errors.New("load result counters cannot be negative")
	}
	if evidence.Accepted > evidence.Submitted || evidence.CanaryAccepted > evidence.CanarySubmitted ||
		evidence.RPCAcceptedBatches > evidence.ExternalBatches {
		return errors.New("load result accepted counters exceed submitted counters")
	}
	if evidence.Undelivered != evidence.Submitted-evidence.Accepted {
		return errors.New("load result undelivered count does not match submitted minus accepted")
	}
	if !validLoadOutcome(evidence.Outcome) {
		return fmt.Errorf("unknown load result outcome %q", evidence.Outcome)
	}
	if evidence.Outcome == loadOutcomeComplete {
		if exitCode != 0 || evidence.Error != "" || evidence.FailureStage != "" {
			return errors.New("complete load result conflicts with process failure")
		}
		if evidence.FailedBatches != 0 || evidence.Undelivered != 0 {
			return errors.New("complete load result contains failed or undelivered work")
		}
		if evidence.ContractProfile == "" || evidence.MinterCodeHash == "" || evidence.WalletCodeHash == "" {
			return errors.New("complete load result has no contract profile evidence")
		}
		if phase == "setup" && (evidence.Minter == "" || evidence.HighloadWallet == "" ||
			evidence.SourceJettonWallet == "") {
			return errors.New("complete setup result has no contract address evidence")
		}
		return nil
	}
	if exitCode == 0 || evidence.Error == "" || evidence.FailureStage == "" {
		return errors.New("failed load result has no process failure evidence")
	}
	if evidence.Outcome == loadOutcomeDeliveryIncomplete &&
		(evidence.FailedBatches != 0 || evidence.Undelivered == 0) {
		return errors.New("delivery-incomplete result has inconsistent counters")
	}
	return nil
}

func validLoadOutcome(outcome string) bool {
	return outcome == loadOutcomeComplete || outcome == loadOutcomeDeliveryIncomplete ||
		outcome == loadOutcomeExecutionFailed || outcome == loadOutcomeSubmissionFailed ||
		outcome == loadOutcomeWorkloadInvalid || outcome == loadOutcomeFailed
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func safeName(value string) string {
	var builder strings.Builder
	for _, character := range value {
		if character >= 'a' && character <= 'z' || character >= 'A' && character <= 'Z' || character >= '0' && character <= '9' || character == '-' || character == '_' {
			builder.WriteRune(character)
		}
	}
	if builder.Len() == 0 {
		return "run"
	}
	return builder.String()
}
