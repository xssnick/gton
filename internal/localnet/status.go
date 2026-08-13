package localnet

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"net"
	"os"
	"os/exec"
	"strconv"
	"strings"
	"time"
)

func CaptureStatus(ctx context.Context, cfg Config) (Status, error) {
	var processes []process
	for _, node := range cfg.Nodes {
		if node.TMUXSession == "" {
			var err error
			processes, err = processTable(ctx)
			if err != nil {
				return Status{}, err
			}
			break
		}
	}

	status := Status{CapturedAt: time.Now().UTC(), Healthy: true, Nodes: make([]NodeStatus, 0, len(cfg.Nodes))}
	for _, node := range cfg.Nodes {
		pids := matchProcesses(processes, node.ProcessMatch)
		if node.TMUXSession != "" {
			var paneErr error
			pids, paneErr = tmuxPanePIDs(ctx, node.TMUXSession)
			if paneErr != nil {
				pids = []int{}
			}
		}
		nodeStatus := NodeStatus{
			Name: node.Name, Kind: node.Kind, Optional: node.Optional,
			Running: len(pids) > 0, PIDs: pids, LogPath: node.LogPath,
		}
		_, stats, size, modified, logErr := readLogTail(node)
		if logErr == nil {
			nodeStatus.LogSize = size
			nodeStatus.LogModified = modified
			nodeStatus.MasterchainSeqno = stats.MasterchainSeqno
			nodeStatus.FinalizedBlocks = stats.Finalized
			nodeStatus.CollatedBlocks = stats.Collated
			nodeStatus.ValidatedBlocks = stats.Validated
			nodeStatus.HardErrors = stats.HardErrors
			nodeStatus.AdvisoryWarnings = stats.AdvisoryWarnings
		} else if !node.Optional {
			status.Healthy = false
		}
		if !node.Optional && !nodeStatus.Running {
			status.Healthy = false
		}
		status.Nodes = append(status.Nodes, nodeStatus)
	}
	return status, nil
}

func tmuxPanePIDs(ctx context.Context, session string) ([]int, error) {
	command := exec.CommandContext(ctx, "tmux", "list-panes", "-t", session, "-F", "#{pane_dead} #{pane_pid} #{pane_current_command}")
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("inspect tmux session %q: %w", session, err)
	}
	pids := make([]int, 0, 1)
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		fields := strings.Fields(scanner.Text())
		if len(fields) < 3 || fields[0] == "1" || fields[2] == "bash" || fields[2] == "sh" || fields[2] == "zsh" {
			continue
		}
		pid, parseErr := strconv.Atoi(fields[1])
		if parseErr == nil {
			pids = append(pids, pid)
		}
	}
	if err = scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan tmux session %q: %w", session, err)
	}
	return pids, nil
}

func Preflight(ctx context.Context, cfg Config) (Status, error) {
	status, err := CaptureStatus(ctx, cfg)
	if err != nil {
		return Status{}, err
	}
	if !status.Healthy {
		return status, errors.New("localnet preflight failed: required process or log is unavailable")
	}
	if cfg.Load.LiteAddress != "" {
		dialer := net.Dialer{Timeout: 2 * time.Second}
		connection, dialErr := dialer.DialContext(ctx, "tcp", cfg.Load.LiteAddress)
		if dialErr != nil {
			return status, fmt.Errorf("localnet preflight: liteserver %s: %w", cfg.Load.LiteAddress, dialErr)
		}
		_ = connection.Close()
	}
	return status, nil
}

type process struct {
	PID     int
	Command string
}

func processTable(ctx context.Context) ([]process, error) {
	command := exec.CommandContext(ctx, "ps", "-eo", "pid=,args=")
	output, err := command.Output()
	if err != nil {
		return nil, fmt.Errorf("read process table: %w", err)
	}

	processes := make([]process, 0, 64)
	scanner := bufio.NewScanner(strings.NewReader(string(output)))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		pid, parseErr := strconv.Atoi(fields[0])
		if parseErr != nil || pid == os.Getpid() {
			continue
		}
		processes = append(processes, process{PID: pid, Command: strings.TrimSpace(line[len(fields[0]):])})
	}
	if err = scanner.Err(); err != nil {
		return nil, fmt.Errorf("scan process table: %w", err)
	}
	return processes, nil
}

func matchProcesses(processes []process, match string) []int {
	pids := make([]int, 0, 1)
	if match == "" {
		return pids
	}
	for _, process := range processes {
		if strings.Contains(process.Command, match) {
			pids = append(pids, process.PID)
		}
	}
	return pids
}
