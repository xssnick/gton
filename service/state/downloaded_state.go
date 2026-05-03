package state

import (
	"context"
	"runtime"
	"time"

	"flexserver/service/storage"

	"github.com/rs/zerolog"
)

type stateImportCoordinator struct {
	log  zerolog.Logger
	slot chan struct{}
}

func newStateImportCoordinator(logger zerolog.Logger) *stateImportCoordinator {
	return &stateImportCoordinator{
		log:  logger,
		slot: make(chan struct{}, 1),
	}
}

func (c *stateImportCoordinator) ImportAndPersist(
	ctx context.Context,
	downloaded storage.DownloadedState,
	store storage.StateStorage,
) (*storage.BlockState, error) {
	block := downloaded.Block()
	blockRef := storage.FormatBlockRef(block)

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case c.slot <- struct{}{}:
	default:
		c.log.Info().
			Str("block", blockRef).
			Msg("state snapshot is ready, waiting for import slot")

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case c.slot <- struct{}{}:
		}
	}
	defer func() { <-c.slot }()

	startedAt := time.Now()
	c.log.Info().
		Str("block", blockRef).
		Msg("decoding staged state snapshot")

	state, err := downloaded.ImportCells(ctx, store)
	if err != nil {
		return nil, err
	}

	c.log.Info().
		Str("block", blockRef).
		Dur("elapsed", time.Since(startedAt)).
		Msg("decoded staged state snapshot")

	if err = store.SaveBlockState(ctx, state); err != nil {
		return nil, err
	}
	runtime.GC()
	return state, nil
}
