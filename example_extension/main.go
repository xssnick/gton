package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton"
	nodeconfig "github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/gton/example_extension/applylogger"
	"github.com/xssnick/tonutils-go/liteclient"
)

func main() {
	flags := flag.NewFlagSet("gton-node-with-extension", flag.ContinueOnError)
	flags.SetOutput(os.Stderr)
	configPath := flags.String("config", nodeconfig.DefaultPath, "path to node config JSON")
	if err := flags.Parse(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			os.Exit(0)
		}
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	cfg, err := nodeconfig.Load(*configPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	ctx := context.Background()
	globalConfigPath := cfg.GlobalConfigPath()
	if _, err = nodeconfig.EnsureGlobalConfig(ctx, globalConfigPath, nodeconfig.DefaultGlobalConfigURL, false); err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	globalConfig, err := liteclient.GetConfigFromFile(globalConfigPath)
	if err != nil {
		fmt.Fprintf(os.Stderr, "%v\n", err)
		os.Exit(1)
	}

	opts := gton.DefaultNodeOptions()
	opts.Config = cfg
	opts.GlobalConfig = globalConfig
	opts.Logger = zerolog.New(os.Stdout).Level(zerolog.InfoLevel).With().Timestamp().Logger()
	opts.Extension = applylogger.New

	err = gton.RunNode(ctx, opts)
	if err == nil {
		return
	}

	fmt.Fprintf(os.Stderr, "%v\n", err)
	os.Exit(1)
}
