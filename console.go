package gton

import (
	"context"
	"errors"
	"fmt"
	"io"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/console"
	service2 "github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/storage/pebblestore"
)

type consoleStatusReader func(context.Context) service2.StatusSnapshot

type consoleStateLifecycleCommands interface {
	CancelPersistentStateSerialization(context.Context) error
	StartPersistentStateSerialization(context.Context, uint32, service2.PersistentStateSerializationScope) error
	StopCellGenerationMigration(context.Context) error
	StartCellGenerationMigration(context.Context, uint32) error
}

func registerConsoleCommands(
	registry *console.Registry,
	readStatus consoleStatusReader,
	stateCommands consoleStateLifecycleCommands,
	dbStatus func(context.Context) (pebblestore.DBStatus, error),
) error {
	statusHandler := func(ctx context.Context, args []string) (string, error) {
		if len(args) != 0 {
			return "", console.ErrNotFound
		}

		return formatStatus(readStatus(ctx), false), nil
	}
	if err := registry.Register("status", statusHandler); err != nil {
		return fmt.Errorf("register status console command: %w", err)
	}
	statusFullHandler := func(ctx context.Context, args []string) (string, error) {
		if len(args) != 0 {
			return "", console.ErrNotFound
		}

		return formatStatus(readStatus(ctx), true), nil
	}
	if err := registry.Register("status full", statusFullHandler); err != nil {
		return fmt.Errorf("register status full console command: %w", err)
	}

	dbStatusHandler := func(ctx context.Context, args []string) (string, error) {
		if len(args) != 0 {
			return "", console.ErrNotFound
		}

		status, err := dbStatus(ctx)
		if err != nil {
			return "", fmt.Errorf("load db status: %w", err)
		}

		return formatDBStatus(status), nil
	}
	if err := registry.Register("status db", dbStatusHandler); err != nil {
		return fmt.Errorf("register status db console command: %w", err)
	}

	serializeHandler := func(ctx context.Context, args []string) (string, error) {
		if len(args) != 1 && len(args) != 2 {
			return "", console.ErrNotFound
		}

		seqno, err := parseMasterchainSeqno(args[0])
		if err != nil {
			return "", fmt.Errorf("invalid serialize command: %w", err)
		}

		scope := service2.PersistentStateSerializationAll
		if len(args) == 2 {
			if !strings.EqualFold(args[1], "basechain") {
				return "", console.ErrNotFound
			}

			scope = service2.PersistentStateSerializationBasechain
		}

		if err = stateCommands.StartPersistentStateSerialization(ctx, seqno, scope); err != nil {
			return "", fmt.Errorf("start persistent state serialization: %w", err)
		}
		if scope == service2.PersistentStateSerializationBasechain {
			return fmt.Sprintf(
				"persistent basechain state serialization started for masterchain seqno %d",
				seqno,
			), nil
		}

		return fmt.Sprintf("persistent state serialization started for masterchain seqno %d", seqno), nil
	}
	if err := registry.Register("serialize", serializeHandler); err != nil {
		return fmt.Errorf("register serialize console command: %w", err)
	}
	serializeCancelHandler := func(ctx context.Context, args []string) (string, error) {
		if len(args) != 0 {
			return "", console.ErrNotFound
		}

		if err := stateCommands.CancelPersistentStateSerialization(ctx); err != nil {
			return "", fmt.Errorf("cancel persistent state serialization: %w", err)
		}

		return "persistent state serialization canceled", nil
	}
	if err := registry.Register("serialize cancel", serializeCancelHandler); err != nil {
		return fmt.Errorf("register serialize cancel console command: %w", err)
	}

	migrateHandler := func(ctx context.Context, args []string) (string, error) {
		if len(args) != 1 {
			return "", console.ErrNotFound
		}

		seqno, err := parseMasterchainSeqno(args[0])
		if err != nil {
			return "", fmt.Errorf("invalid migrate command: %w", err)
		}

		if err = stateCommands.StartCellGenerationMigration(ctx, seqno); err != nil {
			return "", fmt.Errorf("start cell generation migration: %w", err)
		}

		return fmt.Sprintf("cell generation migration started for masterchain seqno %d", seqno), nil
	}
	if err := registry.Register("migrate", migrateHandler); err != nil {
		return fmt.Errorf("register migrate console command: %w", err)
	}
	migrateStopHandler := func(ctx context.Context, args []string) (string, error) {
		if len(args) != 0 {
			return "", console.ErrNotFound
		}

		if err := stateCommands.StopCellGenerationMigration(ctx); err != nil {
			return "", fmt.Errorf("stop cell generation migration: %w", err)
		}

		return "cell generation migration stopped", nil
	}
	if err := registry.Register("migrate stop", migrateStopHandler); err != nil {
		return fmt.Errorf("register migrate stop console command: %w", err)
	}

	return nil
}

func runConsole(
	ctx context.Context,
	logger zerolog.Logger,
	in io.Reader,
	out io.Writer,
	registry *console.Registry,
) {
	err := console.Run(ctx, registry, in, out, func(line string, err error) {
		event := logger.Warn().Err(err).Str("command", strings.TrimSpace(line))
		if errors.Is(err, console.ErrNotFound) {
			event.Msg("unknown console command")

			return
		}

		event.Msg("console command failed")
	})
	if err != nil {
		logger.Warn().Err(err).Msg("console stopped")
	}
}

func parseMasterchainSeqno(value string) (uint32, error) {
	seqno, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, err
	}

	return uint32(seqno), nil
}
