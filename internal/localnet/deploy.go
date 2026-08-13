package localnet

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"
)

type DeployNodeResult struct {
	Name                string        `json:"name"`
	StartedAt           time.Time     `json:"started_at"`
	HealthyAt           time.Time     `json:"healthy_at,omitempty"`
	Duration            time.Duration `json:"duration_ns"`
	MasterchainBaseline uint32        `json:"masterchain_baseline,omitempty"`
	MasterchainHealthy  uint32        `json:"masterchain_healthy,omitempty"`
	Attempts            int           `json:"attempts"`
	DiagnosticLog       string        `json:"diagnostic_log,omitempty"`
	Error               string        `json:"error,omitempty"`
}

type DeployResult struct {
	Binary     string             `json:"binary"`
	Target     string             `json:"target"`
	StartedAt  time.Time          `json:"started_at"`
	FinishedAt time.Time          `json:"finished_at"`
	Nodes      []DeployNodeResult `json:"nodes"`
	Succeeded  bool               `json:"succeeded"`
}

func Deploy(ctx context.Context, cfg Config, binary string) (DeployResult, error) {
	if err := cfg.ValidateDeploy(); err != nil {
		return DeployResult{}, err
	}
	if binary == "" {
		return DeployResult{}, fmt.Errorf("deploy binary is required")
	}

	result := DeployResult{Binary: binary, Target: cfg.Deploy.TargetBinary, StartedAt: time.Now().UTC(), Nodes: make([]DeployNodeResult, 0, len(cfg.Nodes))}
	if err := installBinary(binary, cfg.Deploy.TargetBinary); err != nil {
		return result, err
	}

	timeout := cfg.Deploy.HealthTimeout.Duration
	if timeout <= 0 {
		timeout = 90 * time.Second
	}
	for _, node := range cfg.Nodes {
		if node.Kind != "go" || len(node.StartCommand) == 0 {
			continue
		}
		diagnosticPath := filepath.Join(cfg.RunRoot, "deploy-logs", result.StartedAt.Format("20060102T150405Z"), safeArchiveName(node.Name)+".startup.log")
		nodeResult := deployNode(ctx, cfg.Deploy.TargetBinary, node, timeout, diagnosticPath)
		result.Nodes = append(result.Nodes, nodeResult)
		if nodeResult.Error != "" {
			result.FinishedAt = time.Now().UTC()
			return result, fmt.Errorf("deploy node %q: %s", node.Name, nodeResult.Error)
		}
	}
	result.FinishedAt = time.Now().UTC()
	result.Succeeded = true
	return result, nil
}

func deployNode(ctx context.Context, binary string, node NodeConfig, timeout time.Duration, diagnosticPath string) DeployNodeResult {
	result := DeployNodeResult{Name: node.Name, StartedAt: time.Now().UTC()}
	deadline := time.Now().Add(timeout)
	_, before, offset, _, err := readLogTail(node)
	if err != nil && !errors.Is(err, os.ErrNotExist) {
		result.Error = err.Error()
		return result
	}
	result.MasterchainBaseline = before.MasterchainSeqno

	oldPIDs, _ := tmuxRawPanePIDs(ctx, node.TMUXSession)
	_ = runCommand(ctx, "tmux", "kill-session", "-t", node.TMUXSession)
	if err = waitTmuxStopped(ctx, node.TMUXSession, oldPIDs, deadline); err != nil {
		result.Error = fmt.Sprintf("stop old process: %v", err)
		return result
	}

	command := deploymentCommand(binary, node)
	for time.Now().Before(deadline) {
		result.Attempts++
		if err = startTmuxProcess(ctx, node.TMUXSession, command); err != nil {
			result.Error = fmt.Sprintf("start process: %v", err)
			return result
		}
		attemptPIDs, _ := tmuxRawPanePIDs(ctx, node.TMUXSession)
		retry := false

		for time.Now().Before(deadline) {
			if err = waitContext(ctx, time.Second); err != nil {
				result.Error = err.Error()
				return result
			}
			state, stateErr := readTmuxPaneState(ctx, node.TMUXSession)
			if stateErr != nil || state.Dead {
				diagnostic := captureTmuxPane(ctx, node.TMUXSession)
				if persistErr := persistDeployDiagnostic(diagnosticPath, result.Attempts, state.ExitStatus, diagnostic); persistErr == nil {
					result.DiagnosticLog = diagnosticPath
				}
				_ = runCommand(ctx, "tmux", "kill-session", "-t", node.TMUXSession)
				if stopErr := waitTmuxStopped(ctx, node.TMUXSession, attemptPIDs, deadline); stopErr != nil {
					result.Error = fmt.Sprintf("failed start did not stop: %v", stopErr)
					return result
				}
				retry = true
				break
			}

			events, stats, rangeErr := readDeployLog(node, offset)
			if rangeErr != nil {
				if errors.Is(rangeErr, os.ErrNotExist) {
					continue
				}
				result.Error = rangeErr.Error()
				return result
			}
			if allowedHardErrors(events, nil) > 0 {
				diagnostic := captureTmuxPane(ctx, node.TMUXSession)
				if persistErr := persistDeployDiagnostic(diagnosticPath, result.Attempts, -1, diagnostic); persistErr == nil {
					result.DiagnosticLog = diagnosticPath
				}
				result.Error = "hard validator error observed during startup"
				return result
			}
			if stats.MasterchainSeqno > result.MasterchainBaseline || result.MasterchainBaseline == 0 && stats.MasterchainSeqno > 0 {
				result.MasterchainHealthy = stats.MasterchainSeqno
				result.HealthyAt = time.Now().UTC()
				result.Duration = result.HealthyAt.Sub(result.StartedAt)
				return result
			}
		}
		if !retry {
			break
		}
		if err = waitContext(ctx, 500*time.Millisecond); err != nil {
			result.Error = err.Error()
			return result
		}
	}
	if diagnostic := captureTmuxPane(ctx, node.TMUXSession); diagnostic != "" {
		if err = persistDeployDiagnostic(diagnosticPath, result.Attempts, -1, diagnostic); err == nil {
			result.DiagnosticLog = diagnosticPath
		}
	}
	result.Error = fmt.Sprintf("health timeout after %s without masterchain progress", timeout)
	return result
}

type tmuxPaneState struct {
	Dead       bool
	ExitStatus int
}

func startTmuxProcess(ctx context.Context, session string, command []string) error {
	if err := runCommand(ctx, "tmux", "new-session", "-d", "-s", session); err != nil {
		return err
	}
	if err := runCommand(ctx, "tmux", "set-option", "-w", "-t", session, "remain-on-exit", "on"); err != nil {
		_ = runCommand(ctx, "tmux", "kill-session", "-t", session)
		return err
	}
	shell := tmuxShellCommand(command)
	if err := runCommand(ctx, "tmux", "send-keys", "-t", session, "-l", shell); err != nil {
		_ = runCommand(ctx, "tmux", "kill-session", "-t", session)
		return err
	}
	if err := runCommand(ctx, "tmux", "send-keys", "-t", session, "Enter"); err != nil {
		_ = runCommand(ctx, "tmux", "kill-session", "-t", session)
		return err
	}
	return nil
}

func tmuxShellCommand(command []string) string {
	return shellCommand(command) + `; status=$?; printf '\n[gton-lab] process exited status=%s\n' "$status"; exit "$status"`
}

func readTmuxPaneState(ctx context.Context, session string) (tmuxPaneState, error) {
	command := exec.CommandContext(ctx, "tmux", "list-panes", "-t", session, "-F", "#{pane_dead} #{pane_dead_status}")
	output, err := command.Output()
	if err != nil {
		return tmuxPaneState{}, err
	}
	fields := strings.Fields(string(output))
	if len(fields) == 0 {
		return tmuxPaneState{}, fmt.Errorf("tmux session %q has no pane", session)
	}
	state := tmuxPaneState{Dead: fields[0] == "1", ExitStatus: -1}
	if len(fields) > 1 {
		state.ExitStatus, _ = strconv.Atoi(fields[1])
	}
	return state, nil
}

func tmuxRawPanePIDs(ctx context.Context, session string) ([]int, error) {
	command := exec.CommandContext(ctx, "tmux", "list-panes", "-t", session, "-F", "#{pane_pid}")
	output, err := command.Output()
	if err != nil {
		return nil, err
	}
	fields := strings.Fields(string(output))
	pids := make([]int, 0, len(fields)*2)
	for _, field := range fields {
		pid, parseErr := strconv.Atoi(field)
		if parseErr == nil {
			pids = append(pids, pid)
			if foregroundPID, foregroundErr := paneForegroundPID(ctx, pid); foregroundErr == nil && foregroundPID > 0 && foregroundPID != pid {
				pids = append(pids, foregroundPID)
			}
		}
	}
	return pids, nil
}

func paneForegroundPID(ctx context.Context, panePID int) (int, error) {
	command := exec.CommandContext(ctx, "ps", "-o", "tpgid=", "-p", strconv.Itoa(panePID))
	output, err := command.Output()
	if err != nil {
		return 0, err
	}
	pid, err := strconv.Atoi(strings.TrimSpace(string(output)))
	if err != nil {
		return 0, err
	}
	return pid, nil
}

func waitTmuxStopped(ctx context.Context, session string, pids []int, deadline time.Time) error {
	for time.Now().Before(deadline) {
		sessionGone := runCommand(ctx, "tmux", "has-session", "-t", session) != nil
		pidsGone := true
		for _, pid := range pids {
			if processExists(pid) {
				pidsGone = false
				break
			}
		}
		if sessionGone && pidsGone {
			return nil
		}
		if err := waitContext(ctx, 100*time.Millisecond); err != nil {
			return err
		}
	}
	return fmt.Errorf("tmux session or pane process remained alive until deployment deadline")
}

func processExists(pid int) bool {
	err := syscall.Kill(pid, 0)
	return err == nil || errors.Is(err, syscall.EPERM)
}

func captureTmuxPane(ctx context.Context, session string) string {
	command := exec.CommandContext(ctx, "tmux", "capture-pane", "-p", "-t", session, "-S", "-200")
	output, err := command.Output()
	if err != nil {
		return ""
	}
	const maxDiagnosticBytes = 64 << 10
	if len(output) > maxDiagnosticBytes {
		output = output[len(output)-maxDiagnosticBytes:]
	}
	return string(output)
}

func persistDeployDiagnostic(path string, attempt, exitStatus int, diagnostic string) error {
	if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
		return err
	}
	file, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0o600)
	if err != nil {
		return err
	}
	defer file.Close()
	if _, err = fmt.Fprintf(file, "[gton-lab] attempt=%d exit_status=%d\n%s\n", attempt, exitStatus, diagnostic); err != nil {
		return err
	}
	return file.Sync()
}

func readDeployLog(node NodeConfig, offset int64) ([]Event, logStats, error) {
	info, err := os.Stat(node.LogPath)
	if err != nil {
		return nil, logStats{}, err
	}
	// A lumberjack rotation between restart and health polling creates a new
	// active file. Its zero-based range is the authoritative startup log.
	if info.Size() < offset {
		offset = 0
	}
	return readActiveLogRange(node, offset, info.Size())
}

func deploymentCommand(binary string, node NodeConfig) []string {
	command := make([]string, 0, len(node.StartCommand)+len(node.Args)+10)
	for _, argument := range node.StartCommand {
		argument = strings.ReplaceAll(argument, "{binary}", binary)
		argument = strings.ReplaceAll(argument, "{node}", node.Name)
		argument = strings.ReplaceAll(argument, "{log}", node.LogPath)
		command = append(command, argument)
	}
	defaultArgs := append([]string(nil), node.Args...)
	configured := append(append([]string(nil), command...), node.Args...)
	if !hasFlag(configured, "--verbosity") {
		defaultArgs = append(defaultArgs, "--verbosity", "info")
	}
	if !hasFlag(configured, "--log-json") {
		defaultArgs = append(defaultArgs, "--log-json")
	}
	if !hasFlag(configured, "--log-file") {
		defaultArgs = append(defaultArgs, "--log-file", node.LogPath)
	}
	if !hasFlag(configured, "--log-file-max-size") {
		defaultArgs = append(defaultArgs, "--log-file-max-size", "100")
	}
	if !hasFlag(configured, "--log-file-max-backups") {
		defaultArgs = append(defaultArgs, "--log-file-max-backups", "5")
	}
	if !hasFlag(configured, "--log-file-compress") {
		defaultArgs = append(defaultArgs, "--log-file-compress")
	}
	if len(command) >= 3 && (command[0] == "/bin/bash" || command[0] == "bash") && command[1] == "-lc" {
		command[2] += " " + shellCommand(defaultArgs)
		return command
	}
	command = append(command, defaultArgs...)
	return command
}

func hasFlag(command []string, flag string) bool {
	for _, argument := range command {
		if argument == flag || strings.HasPrefix(argument, flag+"=") || strings.Contains(argument, " "+flag+" ") || strings.Contains(argument, " "+flag+"=") {
			return true
		}
	}
	return false
}

func shellCommand(command []string) string {
	quoted := make([]string, len(command))
	for i, argument := range command {
		if argument == "" {
			quoted[i] = "''"
			continue
		}
		quoted[i] = "'" + strings.ReplaceAll(argument, "'", "'\"'\"'") + "'"
	}
	return strings.Join(quoted, " ")
}

func installBinary(source, target string) error {
	sourceInfo, err := os.Stat(source)
	if err != nil {
		return fmt.Errorf("stat deploy binary: %w", err)
	}
	if !sourceInfo.Mode().IsRegular() {
		return fmt.Errorf("deploy binary %q is not a regular file", source)
	}
	if err = os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
		return fmt.Errorf("create deploy target directory: %w", err)
	}

	input, err := os.Open(source)
	if err != nil {
		return fmt.Errorf("open deploy binary: %w", err)
	}
	defer input.Close()
	temporary := fmt.Sprintf("%s.gton-lab-%d", target, os.Getpid())
	output, err := os.OpenFile(temporary, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o755)
	if err != nil {
		return fmt.Errorf("create staged deploy binary: %w", err)
	}
	committed := false
	defer func() {
		_ = output.Close()
		if !committed {
			_ = os.Remove(temporary)
		}
	}()
	if _, err = io.Copy(output, input); err != nil {
		return fmt.Errorf("copy deploy binary: %w", err)
	}
	if err = output.Sync(); err != nil {
		return fmt.Errorf("sync deploy binary: %w", err)
	}
	if err = output.Close(); err != nil {
		return fmt.Errorf("close deploy binary: %w", err)
	}
	if err = os.Rename(temporary, target); err != nil {
		return fmt.Errorf("activate deploy binary: %w", err)
	}
	committed = true
	return nil
}

func runCommand(ctx context.Context, name string, args ...string) error {
	command := exec.CommandContext(ctx, name, args...)
	if output, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("%s: %w: %s", name, err, strings.TrimSpace(string(output)))
	}
	return nil
}
