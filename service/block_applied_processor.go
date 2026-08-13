package service

import (
	"context"
	"fmt"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const blockAppliedProcessorRetryDelay = 50 * time.Millisecond

type blockAppliedProcessorRunner struct {
	log        zerolog.Logger
	processor  BlockAppliedProcessor
	retryDelay time.Duration
}

type blockAppliedObserverMeta struct {
	InclusionMasterRef   *ton.BlockIDExt
	InclusionMasterState *cell.Cell
	deferEvent           func(BlockAppliedEvent)
}

func newBlockAppliedProcessorRunner(log zerolog.Logger, processor BlockAppliedProcessor) *blockAppliedProcessorRunner {
	if processor == nil {
		return nil
	}

	return &blockAppliedProcessorRunner{
		log:        log,
		processor:  processor,
		retryDelay: blockAppliedProcessorRetryDelay,
	}
}

func (s *SyncCoordinator) applyBlockAndObserve(ctx context.Context, previous []*storage.BlockState, block PreparedBlock, applier stateUpdateApplier, meta *blockAppliedObserverMeta) (*storage.BlockState, error) {
	applied, err := applyBlockWithPreviousStates(previous, block, applier)
	if err != nil {
		return nil, err
	}
	next := applied.Next

	if s.blockAppliedProcessor != nil && meta != nil {
		event := BlockAppliedEvent{
			BlockBOC:             block.BlockBOC,
			ProofBOC:             block.ProofBOC,
			BlockRoot:            block.BlockRoot,
			Meta:                 block.Meta,
			PreviousState:        applied.PreviousRoot,
			CurrentState:         next.Cell,
			InclusionMasterRef:   meta.InclusionMasterRef,
			InclusionMasterState: meta.InclusionMasterState,
		}
		if meta.deferEvent != nil {
			meta.deferEvent(event)
		} else if err = s.blockAppliedProcessor.run(ctx, event); err != nil {
			return nil, err
		}
	}
	s.status.observeBlock(block.ID, block.BlockRoot, block.Meta.GenUTime, time.Now())
	return next, nil
}

func (r *blockAppliedProcessorRunner) run(ctx context.Context, event BlockAppliedEvent) error {
	attempt := 0
	var lastLog time.Time
	for {
		err := r.processor.ProcessBlockApplied(ctx, event)
		if err == nil {
			return nil
		}
		attempt++
		now := time.Now()
		if attempt == 1 || now.Sub(lastLog) >= 5*time.Second {
			r.log.Warn().
				Err(err).
				Str("block", storage.FormatBlockRef(event.Meta.ID)).
				Str("processor", fmt.Sprintf("%T", r.processor)).
				Int("attempt", attempt).
				Dur("retry_delay", r.retryDelay).
				Msg("block applied processor failed, retrying")
			lastLog = now
		}
		if err = waitRetry(ctx, r.retryDelay); err != nil {
			return err
		}
	}
}
