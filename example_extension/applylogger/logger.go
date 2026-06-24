package applylogger

import (
	"context"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

type applyLogger struct {
	log zerolog.Logger
}

func New(n hooks.Node) (hooks.Extension, error) {
	return applyLogger{log: n.Logger}, nil
}

func (l applyLogger) OnExternalMessage(ctx context.Context, e hooks.ExternalMessageEvent) error {
	logEvent := l.log.Info().Bool("is_local", e.IsLocal)
	if e.MessageParsed != nil && e.MessageParsed.DstAddr != nil {
		logEvent.Str("dst", e.MessageParsed.DstAddr.String())
	}
	logEvent.Msg("external message")
	return nil
}

func (l applyLogger) OnBlockReceived(ctx context.Context, event hooks.BlockReceivedEvent) error {
	return nil
}

func (l applyLogger) OnBlockApplied(ctx context.Context, event hooks.BlockAppliedEvent) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	default:
	}

	started := time.Now()
	blockID := event.Meta.ID
	parsed, err := storage.ParseVerifiedBlockCell(blockID, event.BlockRoot)
	if err != nil {
		return fmt.Errorf("parse block: %w", err)
	}

	txCount, err := storage.BlockTransactionCountFromParsed(blockID, parsed)
	if err != nil {
		return err
	}
	elapsed := time.Since(started)

	l.log.Info().
		Int32("workchain", blockID.Workchain).
		Uint64("shard", uint64(blockID.Shard)).
		Uint32("seqno", blockID.SeqNo).
		Dur("processed_in", elapsed).
		Uint32("transactions", txCount).
		Msg("applied block")

	return nil
}
