package localnet

import (
	"context"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

const runManifestVersion = 2

func Run(ctx context.Context, cfg Config, scenario string) (Summary, error) {
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
	if _, err := Preflight(ctx, cfg); err != nil {
		return Summary{}, err
	}

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

	baseline, err := captureBaseline(ctx, cfg)
	if err != nil {
		return Summary{}, err
	}
	manifest := RunManifest{Version: runManifestVersion, Scenario: scenario, StartedAt: started, Config: cfg, Baseline: baseline}
	if err = writeJSON(filepath.Join(runDirectory, "manifest.json"), manifest); err != nil {
		return Summary{}, err
	}
	if err = writeJSON(filepath.Join(runDirectory, "baseline.json"), baseline); err != nil {
		return Summary{}, err
	}

	manifest.Load = executeLoad(ctx, cfg.Load, runDirectory)
	if cancelErr := ctx.Err(); cancelErr != nil {
		return finalizeCanceledRun(runDirectory, &manifest, cancelErr)
	}
	if !observesTopology(scenario) && !loadStopsScenario(scenario, manifest.Load) && cfg.Load.Settle.Duration > 0 {
		if err = waitContext(ctx, cfg.Load.Settle.Duration); err != nil {
			return finalizeCanceledRun(runDirectory, &manifest, err)
		}
	}
	topologyDeadline := time.Now().Add(cfg.Load.TopologyTimeout.Duration)
	cursors := make(map[string]LogPosition, len(baseline.Nodes))
	for _, node := range baseline.Nodes {
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
			complete = evaluateTopology(observed, cfg.Nodes).Complete
		}
		if cancelErr := ctx.Err(); cancelErr != nil {
			return finalizeCanceledRun(runDirectory, &manifest, cancelErr)
		}
		if loadStopsScenario(scenario, manifest.Load) || !observesTopology(scenario) ||
			complete || !time.Now().Before(topologyDeadline) {
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

func statusMasterchainSeqno(status Status, name string) (uint32, error) {
	for _, node := range status.Nodes {
		if node.Name == name {
			return node.MasterchainSeqno, nil
		}
	}
	return 0, fmt.Errorf("status has no node %q", name)
}

func executeLoad(ctx context.Context, load LoadConfig, runDirectory string) LoadResult {
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
			Setup:       ProcessResult{ExitCode: 0, Outcome: loadOutcomeComplete},
		}
	}
	if load.SetupBeforeRun {
		for i := range result.Senders {
			result.Senders[i].Setup = executeLoadProcess(ctx, load, runDirectory, "setup", result.Senders[i].SenderIndex, load.SetupExtraArgs)
			sender := result.Senders[i]
			if sender.Setup.ExitCode != 0 {
				result.ExitCode = 1
				result.Outcome = loadOutcomeFailed
				result.HardFailure = true
				result.Error = "one or more sender setup processes failed"
				result.FinishedAt = time.Now().UTC()
				result.Duration = result.FinishedAt.Sub(result.StartedAt)
				return result
			}
		}
	}
	executeSenderPhase(ctx, load, runDirectory, result.Senders, "run", load.ExtraArgs)

	for _, sender := range result.Senders {
		result.Submitted += sender.Load.Submitted
		result.Accepted += sender.Load.Accepted
		result.FailedBatches += sender.Load.FailedBatches

		switch sender.Load.Outcome {
		case loadOutcomeDeliveryIncomplete:
			result.ExitCode = 1
			result.IncompleteSenders = append(result.IncompleteSenders, sender.SenderIndex)
		case loadOutcomeFailed:
			result.ExitCode = 1
			result.HardFailure = true
		}
	}
	switch {
	case result.HardFailure:
		result.Outcome = loadOutcomeFailed
		result.Error = "one or more load senders failed"
	case len(result.IncompleteSenders) > 0:
		result.Outcome = loadOutcomeDeliveryIncomplete
		result.Advisory = true
		result.Error = "one or more load senders did not confirm all submitted transfers"
	default:
		result.Outcome = loadOutcomeComplete
	}
	result.FinishedAt = time.Now().UTC()
	result.Duration = result.FinishedAt.Sub(result.StartedAt)
	return result
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

func executeLoadProcess(ctx context.Context, load LoadConfig, runDirectory, phase string, sender uint64, extra []string) ProcessResult {
	result := ProcessResult{StartedAt: time.Now().UTC(), ExitCode: -1, Outcome: loadOutcomeFailed}
	prefix := fmt.Sprintf("load-%d-%s", sender, phase)
	stdout, err := os.Create(filepath.Join(runDirectory, prefix+".stdout.log"))
	if err != nil {
		result.Error = fmt.Sprintf("create stdout: %v", err)
		return result
	}
	defer stdout.Close()
	stderr, err := os.Create(filepath.Join(runDirectory, prefix+".stderr.log"))
	if err != nil {
		result.Error = fmt.Sprintf("create stderr: %v", err)
		return result
	}
	defer stderr.Close()

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
		result.Error = err.Error()
		var exitError *exec.ExitError
		if errors.As(err, &exitError) {
			result.ExitCode = exitError.ExitCode()
		}
	} else {
		result.ExitCode = 0
	}
	if phase == "setup" {
		if result.ExitCode == 0 {
			result.Outcome = loadOutcomeComplete
		}
	} else if evidenceErr := classifyLoadProcess(stderr.Name(), &result); evidenceErr != nil {
		result.Outcome = loadOutcomeFailed
		if result.Error == "" {
			result.Error = evidenceErr.Error()
		} else {
			result.Error = errors.Join(errors.New(result.Error), evidenceErr).Error()
		}
	}
	result.FinishedAt = time.Now().UTC()
	result.Duration = result.FinishedAt.Sub(result.StartedAt)
	return result
}

func classifyLoadProcess(stderrPath string, result *ProcessResult) error {
	data, err := os.ReadFile(stderrPath)
	if err != nil {
		return fmt.Errorf("read load diagnostics: %w", err)
	}
	plain := ansiPattern.ReplaceAllString(string(data), "")
	result.Submitted = loadMetric(plain, "submitted")
	result.Accepted = loadMetric(plain, "accepted")
	result.FailedBatches = loadMetric(plain, "failed_batches")

	if result.ExitCode == 0 {
		if result.FailedBatches > 0 {
			return errors.New("load reported failed batches with a successful exit")
		}
		result.Outcome = loadOutcomeComplete
		return nil
	}
	if result.FailedBatches > 0 {
		result.Outcome = loadOutcomeFailed
		return nil
	}
	deliveryShortfall := result.Submitted > 0 && result.Accepted < result.Submitted
	if deliveryShortfall && isDeliveryIncompleteDiagnostic(plain) {
		result.Outcome = loadOutcomeDeliveryIncomplete
		return nil
	}
	result.Outcome = loadOutcomeFailed
	return nil
}

func loadMetric(output, name string) int {
	marker := name + "="
	position := strings.LastIndex(output, marker)
	if position < 0 {
		marker = `"` + name + `":`
		position = strings.LastIndex(output, marker)
	}
	if position < 0 {
		return 0
	}
	value := strings.TrimLeft(output[position+len(marker):], " \t")
	end := 0
	for end < len(value) && value[end] >= '0' && value[end] <= '9' {
		end++
	}
	if end == 0 {
		return 0
	}
	number, _ := strconv.Atoi(value[:end])
	return number
}

func isDeliveryIncompleteDiagnostic(output string) bool {
	if strings.Contains(output, "drain timeout after observing ") {
		return true
	}
	return strings.Contains(output, " submitted transfers were observed") &&
		strings.Contains(output, "only ")
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
