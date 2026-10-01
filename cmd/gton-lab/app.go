package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"path/filepath"

	"github.com/xssnick/gton/internal/localnet"
)

type usageError struct {
	message string
}

func (e usageError) Error() string { return e.message }

func runCLI(ctx context.Context, args []string, stdout io.Writer) (int, error) {
	if len(args) == 0 {
		return 2, usageError{message: "usage: gton-lab <preflight|status|deploy|run|report|trim-logs> [flags]"}
	}

	var value any
	var err error
	switch args[0] {
	case "preflight":
		var cfg localnet.Config
		cfg, err = commandConfig("preflight", args[1:])
		if err == nil {
			value, err = localnet.Preflight(ctx, cfg)
		}
	case "status":
		var cfg localnet.Config
		cfg, err = commandConfig("status", args[1:])
		if err == nil {
			value, err = localnet.CaptureStatus(ctx, cfg)
		}
	case "deploy":
		var cfg localnet.Config
		var binary string
		cfg, binary, err = deployOptions(args[1:])
		if err == nil {
			result, deployErr := localnet.Deploy(ctx, cfg, binary)
			if !result.StartedAt.IsZero() {
				value = result
			}
			err = deployErr
		}
	case "run":
		var cfg localnet.Config
		var scenario string
		cfg, scenario, err = runOptions(args[1:])
		if err == nil {
			result, runErr := localnet.Run(ctx, cfg, scenario)
			if result.RunDirectory != "" {
				value = result
			}
			err = runErr
		}
	case "report":
		var runDirectory string
		runDirectory, err = reportOptions(args[1:])
		if err == nil {
			result, reportErr := localnet.BuildReport(runDirectory)
			if result.RunDirectory != "" {
				value = result
			}
			err = reportErr
		}
	case "trim-logs":
		var cfg localnet.Config
		var keepBytes int64
		var apply bool
		cfg, keepBytes, apply, err = trimOptions(args[1:])
		if err == nil {
			value, err = localnet.TrimLogs(cfg, keepBytes, apply)
		}
	default:
		return 2, usageError{message: fmt.Sprintf("unknown command %q", args[0])}
	}

	if value != nil {
		if encodeErr := json.NewEncoder(stdout).Encode(value); encodeErr != nil {
			return 1, fmt.Errorf("encode command result: %w", encodeErr)
		}
	}
	if err != nil {
		var usage usageError
		if errors.As(err, &usage) || errors.Is(err, flag.ErrHelp) {
			return 2, err
		}
		return 1, err
	}
	return 0, nil
}

func trimOptions(args []string) (localnet.Config, int64, bool, error) {
	set := flag.NewFlagSet("trim-logs", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	var configPath string
	var keepBytes int64
	var apply bool
	set.StringVar(&configPath, "config", "", "lab config JSON")
	set.Int64Var(&keepBytes, "keep-bytes", 16<<20, "bytes archived from the end of each configured log")
	set.BoolVar(&apply, "apply", false, "archive and truncate; default is dry-run")
	if err := set.Parse(args); err != nil {
		return localnet.Config{}, 0, false, usageError{message: err.Error()}
	}
	if set.NArg() != 0 || configPath == "" || keepBytes < 0 {
		return localnet.Config{}, 0, false, usageError{message: "trim-logs requires --config and non-negative --keep-bytes"}
	}
	cfg, err := localnet.LoadConfigFile(configPath)
	return cfg, keepBytes, apply, err
}

func commandConfig(command string, args []string) (localnet.Config, error) {
	set := flag.NewFlagSet(command, flag.ContinueOnError)
	set.SetOutput(io.Discard)
	var path string
	set.StringVar(&path, "config", "", "lab config JSON")
	if err := set.Parse(args); err != nil {
		return localnet.Config{}, usageError{message: err.Error()}
	}
	if set.NArg() != 0 || path == "" {
		return localnet.Config{}, usageError{message: command + " requires --config and no positional arguments"}
	}
	return localnet.LoadConfigFile(path)
}

func deployOptions(args []string) (localnet.Config, string, error) {
	set := flag.NewFlagSet("deploy", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	var configPath, binary string
	set.StringVar(&configPath, "config", "", "lab config JSON")
	set.StringVar(&binary, "binary", "", "new gton-node binary")
	if err := set.Parse(args); err != nil {
		return localnet.Config{}, "", usageError{message: err.Error()}
	}
	if set.NArg() != 0 || configPath == "" || binary == "" {
		return localnet.Config{}, "", usageError{message: "deploy requires --config and --binary"}
	}
	cfg, err := localnet.LoadConfigFile(configPath)
	return cfg, binary, err
}

func runOptions(args []string) (localnet.Config, string, error) {
	set := flag.NewFlagSet("run", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	var configPath, scenario string
	set.StringVar(&configPath, "config", "", "lab config JSON")
	set.StringVar(
		&scenario,
		"scenario",
		"load",
		"scenario name: load, topology-cycle, all, or full-cycle",
	)
	if err := set.Parse(args); err != nil {
		return localnet.Config{}, "", usageError{message: err.Error()}
	}
	if set.NArg() != 0 || configPath == "" {
		return localnet.Config{}, "", usageError{message: "run requires --config"}
	}
	cfg, err := localnet.LoadConfigFile(configPath)
	return cfg, scenario, err
}

func reportOptions(args []string) (string, error) {
	set := flag.NewFlagSet("report", flag.ContinueOnError)
	set.SetOutput(io.Discard)
	var runDirectory string
	set.StringVar(&runDirectory, "run", "", "completed run directory")
	if err := set.Parse(args); err != nil {
		return "", usageError{message: err.Error()}
	}
	if set.NArg() != 0 || runDirectory == "" {
		return "", usageError{message: "report requires --run"}
	}
	absolute, err := filepath.Abs(runDirectory)
	if err != nil {
		return "", fmt.Errorf("resolve run directory: %w", err)
	}
	return absolute, nil
}
