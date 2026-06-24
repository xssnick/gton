package service

import (
	"context"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

const blockApplyHookRetryDelay = 50 * time.Millisecond

type blockApplyHookRunner struct {
	log        zerolog.Logger
	extension  hooks.Extension
	retryDelay time.Duration
}

type blockApplyHookMeta struct {
	MasterchainRef *ton.BlockIDExt
}

func newBlockApplyHookRunner(log zerolog.Logger, extension hooks.Extension) *blockApplyHookRunner {
	if extension == nil {
		return nil
	}

	return &blockApplyHookRunner{
		log:        log,
		extension:  extension,
		retryDelay: blockApplyHookRetryDelay,
	}
}

func (s *Service) applyBlockWithHooks(ctx context.Context, previous []*storage.BlockState, block PreparedBlock, applier stateUpdateApplier, meta *blockApplyHookMeta) (*storage.BlockState, error) {
	next, err := applyBlockWithPreviousStates(previous, block, applier)
	if err != nil {
		return nil, err
	}

	if s.applyHooks != nil && meta != nil {
		if err = s.applyHooks.run(ctx, hooks.BlockAppliedEvent{
			BlockBOC:       block.BlockBOC,
			ProofBOC:       block.ProofBOC,
			BlockRoot:      block.BlockRoot,
			Meta:           block.Meta,
			State:          next.Parsed,
			MasterchainRef: meta.MasterchainRef,
		}); err != nil {
			return nil, err
		}
	}
	return next, nil
}

func (r *blockApplyHookRunner) run(ctx context.Context, event hooks.BlockAppliedEvent) error {
	attempt := 0
	var lastLog time.Time
	for {
		err := r.extension.OnBlockApplied(ctx, event)
		if err == nil {
			return nil
		}
		attempt++
		now := time.Now()
		if attempt == 1 || now.Sub(lastLog) >= 5*time.Second {
			r.log.Warn().
				Err(err).
				Str("block", storage.FormatBlockRef(event.Meta.ID)).
				Str("extension", fmt.Sprintf("%T", r.extension)).
				Int("attempt", attempt).
				Dur("retry_delay", r.retryDelay).
				Msg("apply hook failed, retrying")
			lastLog = now
		}
		if err = waitRetry(ctx, r.retryDelay); err != nil {
			return err
		}
	}
}
