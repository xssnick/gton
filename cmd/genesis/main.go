package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/genesis"
)

func main() {
	if err := run(context.Background(), os.Args[1:], os.Stdout, os.Stderr); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func run(ctx context.Context, args []string, stdout, stderr io.Writer) error {
	paths := genesis.DefaultPaths()
	flags := flag.NewFlagSet("gton-genesis", flag.ContinueOnError)
	flags.SetOutput(stderr)
	flags.StringVar(&paths.Genesis, "genesis", paths.Genesis, "path to genesis JSON")
	flags.StringVar(&paths.Data, "data", paths.Data, "output node data directory")
	flags.StringVar(&paths.GlobalConfig, "global-config", paths.GlobalConfig, "output TON global config JSON")
	flags.StringVar(&paths.Lock, "lock", paths.Lock, "output resolved genesis lock JSON")
	if err := flags.Parse(args); err != nil {
		return err
	}
	if flags.NArg() != 0 {
		return fmt.Errorf("unexpected positional arguments: %v", flags.Args())
	}

	logger := zerolog.New(zerolog.ConsoleWriter{Out: zerolog.SyncWriter(stdout), TimeFormat: time.RFC3339}).With().Timestamp().Logger()
	if _, err := os.Stat(paths.Genesis); errors.Is(err, os.ErrNotExist) {
		if len(args) != 0 {
			return fmt.Errorf("genesis file %s does not exist", paths.Genesis)
		}
		if err = genesis.WriteTemplate(paths.Genesis); err != nil {
			return fmt.Errorf("create genesis template: %w", err)
		}
		logger.Info().Str("genesis", paths.Genesis).Msg("created genesis template; replace placeholders and run gton-genesis again")
		return nil
	} else if err != nil {
		return fmt.Errorf("inspect genesis file: %w", err)
	}

	result, err := genesis.Generate(ctx, paths, logger)
	if err != nil {
		return err
	}
	logger.Info().
		Bool("created", result.Created).
		Str("genesis", paths.Genesis).
		Str("data", paths.Data).
		Str("global_config", paths.GlobalConfig).
		Str("lock", paths.Lock).
		Uint32("genesis_time", result.GenesisTime).
		Hex("masterchain_root_hash", result.Masterchain.RootHash).
		Hex("basechain_root_hash", result.Basechain.RootHash).
		Str("wallet", result.Wallet).
		Msg("genesis ready")
	return nil
}
