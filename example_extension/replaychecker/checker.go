package replaychecker

import (
	"context"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"
)

type replayChecker struct {
	log       zerolog.Logger
	validator *Validator
}

func New(n hooks.Node) (hooks.Extension, error) {
	return &replayChecker{
		log:       n.Logger,
		validator: NewValidator(ValidatorOptions{Logger: n.Logger, Store: n.Store}),
	}, nil
}

func (r *replayChecker) Start(context.Context) error {
	return nil
}

func (r *replayChecker) Close(context.Context) error {
	return nil
}

func (r *replayChecker) OnExternalMessage(context.Context, hooks.ExternalMessageEvent) error {
	return nil
}

func (r *replayChecker) OnBlockReceived(context.Context, hooks.BlockReceivedEvent) error {
	return nil
}

func (r *replayChecker) OnBlockApplied(ctx context.Context, event hooks.BlockAppliedEvent) error {
	started := time.Now()
	result, err := r.validator.ValidateAppliedBlock(ctx, event)
	elapsed := time.Since(started)

	block := "<unknown>"
	if event.Meta != nil {
		block = storage.FormatBlockRef(event.Meta.ID)
	}

	logEvent := r.log.Info()
	if err != nil {
		logEvent = r.log.Warn().Err(err)
	}
	logEvent.
		Str("block", block).
		Int("accounts", result.Accounts).
		Int("transactions", result.Transactions).
		Dur("replay_elapsed", elapsed).
		Dur("tx_emulation_elapsed", result.EmulationElapsed).
		Msg("applied block transaction replay checked")

	return err
}
