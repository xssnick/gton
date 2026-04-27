package state

import (
	"context"
	storage2 "flexserver/service/storage"
	"runtime"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

type DownloadedState interface {
	Block() ton.BlockIDExt
	Decode(ctx context.Context, cells storage2.StateCellTreeImporter) (*storage2.BlockState, error)
	Cleanup() error
}

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
	downloaded DownloadedState,
	store storage2.StateStorage,
) (*storage2.BlockState, error) {
	block := downloaded.Block()
	blockRef := storage2.FormatBlockRef(block)

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

	state, err := downloaded.Decode(ctx, store)
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

type immediateDownloadedState struct {
	state *storage2.BlockState
}

func newImmediateDownloadedState(state *storage2.BlockState) DownloadedState {
	return &immediateDownloadedState{state: storage2.CloneBlockState(state)}
}

func (d *immediateDownloadedState) Block() ton.BlockIDExt {
	if d.state == nil {
		return ton.BlockIDExt{}
	}
	return d.state.Block
}

func (d *immediateDownloadedState) Decode(context.Context, storage2.StateCellTreeImporter) (*storage2.BlockState, error) {
	return storage2.CloneBlockState(d.state), nil
}

func (d *immediateDownloadedState) Cleanup() error {
	d.state = nil
	return nil
}
