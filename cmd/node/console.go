package main

import (
	"bufio"
	"context"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/rs/zerolog"
	service2 "github.com/xssnick/gton/service"
	"github.com/xssnick/gton/service/storage/pebblestore"
)

func runConsole(ctx context.Context, logger zerolog.Logger, svc *service2.Service, dbStatus func(context.Context) (pebblestore.DBStatus, error)) {
	scanner := bufio.NewScanner(os.Stdin)
	for {
		if !scanner.Scan() {
			return
		}

		select {
		case <-ctx.Done():
			return
		default:
		}

		cmd := strings.Fields(strings.ToLower(strings.TrimSpace(scanner.Text())))
		if len(cmd) == 0 {
			continue
		}

		switch cmd[0] {
		case "status":
			if len(cmd) == 2 && cmd[1] == "db" {
				if dbStatus == nil {
					_, _ = fmt.Fprintln(os.Stdout, formatDBStatus(pebblestore.DBStatus{}))
					continue
				}
				status, err := dbStatus(ctx)
				if err != nil {
					logger.Warn().Err(err).Str("command", strings.Join(cmd, " ")).Msg("failed to load db status")
					continue
				}
				_, _ = fmt.Fprintln(os.Stdout, formatDBStatus(status))
				continue
			}

			showPeers := len(cmd) > 1 && cmd[1] == "full"
			if len(cmd) > 2 {
				logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown console command")
				continue
			}
			if len(cmd) == 2 && cmd[1] != "full" {
				logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown console command")
				continue
			}
			_, _ = fmt.Fprintln(os.Stdout, formatStatus(svc.StatusSnapshot(), showPeers))
		case "serialize":
			if len(cmd) != 2 && len(cmd) != 3 {
				logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown console command")
				continue
			}
			if len(cmd) == 2 && cmd[1] == "cancel" {
				if err := svc.CancelPersistentStateSerialization(ctx); err != nil {
					logger.Warn().Err(err).Str("command", strings.Join(cmd, " ")).Msg("failed to cancel persistent state serialization")
					continue
				}
				_, _ = fmt.Fprintln(os.Stdout, "persistent state serialization canceled")
				continue
			}

			seqno, err := parseMasterchainSeqno(cmd[1])
			if err != nil {
				logger.Warn().Err(err).Str("command", strings.Join(cmd, " ")).Msg("invalid serialize command")
				continue
			}

			scope := service2.PersistentStateSerializationAll
			if len(cmd) == 3 {
				if cmd[2] != "basechain" {
					logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown serialize scope")
					continue
				}
				scope = service2.PersistentStateSerializationBasechain
			}

			if err = svc.StartPersistentStateSerialization(ctx, seqno, scope); err != nil {
				logger.Warn().Err(err).Uint32("masterchain_seqno", seqno).Msg("failed to start persistent state serialization")
				continue
			}
			if scope == service2.PersistentStateSerializationBasechain {
				_, _ = fmt.Fprintf(os.Stdout, "persistent basechain state serialization started for masterchain seqno %d\n", seqno)
			} else {
				_, _ = fmt.Fprintf(os.Stdout, "persistent state serialization started for masterchain seqno %d\n", seqno)
			}
		case "migrate":
			if len(cmd) != 2 {
				logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown console command")
				continue
			}
			if cmd[1] == "stop" {
				if err := svc.StopCellGenerationMigration(ctx); err != nil {
					logger.Warn().Err(err).Str("command", strings.Join(cmd, " ")).Msg("failed to stop cell generation migration")
					continue
				}
				_, _ = fmt.Fprintln(os.Stdout, "cell generation migration stopped")
				continue
			}

			seqno, err := parseMasterchainSeqno(cmd[1])
			if err != nil {
				logger.Warn().Err(err).Str("command", strings.Join(cmd, " ")).Msg("invalid migrate command")
				continue
			}

			if err = svc.StartCellGenerationMigration(ctx, seqno); err != nil {
				logger.Warn().Err(err).Uint32("masterchain_seqno", seqno).Msg("failed to start cell generation migration")
				continue
			}
			_, _ = fmt.Fprintf(os.Stdout, "cell generation migration started for masterchain seqno %d\n", seqno)
		default:
			logger.Warn().Str("command", strings.Join(cmd, " ")).Msg("unknown console command")
		}
	}
}

func parseMasterchainSeqno(value string) (uint32, error) {
	seqno, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return 0, err
	}
	return uint32(seqno), nil
}
