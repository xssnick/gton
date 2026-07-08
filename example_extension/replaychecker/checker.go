package replaychecker

import (
	"context"
	"errors"
	"fmt"
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

	// A replay mismatch means the checker's re-execution diverged from the
	// applied state — a fundamental correctness failure for the collator
	// blueprint. Returning an error would make the apply-hook runner retry the
	// block flow forever (wedging checkpoint/persist), and returning nil would
	// silently skip the divergence. Neither is acceptable: we LOG the structured
	// details, then PANIC to stop the node hard so the divergence cannot be
	// ignored. Only a transient infrastructure error (store/context) — which a
	// retry can actually fix — is propagated instead.
	if err != nil && errors.Is(err, ErrMismatch) {
		r.log.Error().
			Err(err).
			Str("block", block).
			Int("accounts", result.Accounts).
			Int("transactions", result.Transactions).
			Int("mismatches", result.Mismatches).
			Dur("replay_elapsed", elapsed).
			Dur("tx_emulation_elapsed", result.EmulationElapsed).
			Msg("applied block transaction replay MISMATCH: emulated state diverged from applied state")
		panic(fmt.Sprintf(
			"replaychecker: block %s replay mismatch (%d/%d accounts diverged, %d transactions): %v",
			block, result.Mismatches, result.Accounts, result.Transactions, err,
		))
	}

	if err != nil {
		// Transient infrastructure error: propagate so the apply-hook runner
		// retries this block flow (a re-read of the store/state may succeed).
		r.log.Warn().
			Err(err).
			Str("block", block).
			Int("accounts", result.Accounts).
			Int("transactions", result.Transactions).
			Dur("replay_elapsed", elapsed).
			Msg("applied block transaction replay could not complete; will retry")
		return err
	}

	r.log.Info().
		Str("block", block).
		Int("accounts", result.Accounts).
		Int("transactions", result.Transactions).
		Dur("replay_elapsed", elapsed).
		Dur("tx_emulation_elapsed", result.EmulationElapsed).
		Msg("applied block transaction replay checked")

	return nil
}
