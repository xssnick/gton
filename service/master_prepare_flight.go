package service

import (
	"context"
	"sync"
	"time"

	"github.com/xssnick/gton/service/storage"
)

// masterPrepareFlight collapses the state-update cell preparation of one
// masterchain block.
//
// A masterchain broadcast is decoded once in p2p and then reaches two
// independent consumers: the block-sync worker (which validates consensus and
// queues the prepared block) and the next-block probe (which takes the same
// decoded block out of the p2p hot cache). Both walked and re-encoded every
// reachable cell of the same merkle-update target, and the loser's copy was
// thrown away one apply cycle later.
//
// Only the cell preparation is shared. The result is immutable and may be
// aliased by both consumers; the consensus proof is not shareable (its
// signature-check memoization is written in place without a lock) and stays
// per-consumer, as does the metadata clone made by preparedBlockWithStateCells.
//
// Nothing is retained past the last waiter: the entry is removed before the
// result is published, so a failed preparation can never poison a later retry,
// and result reuse over time stays the job of the masterchain queue.
type masterPrepareFlight struct {
	mu    sync.Mutex
	calls map[storage.BlockRootHash]*masterPrepareCall
}

type masterPrepareCall struct {
	done    chan struct{}
	cells   storage.StateCellRecords
	elapsed time.Duration
	err     error
}

// prepareVerifiedMasterchainBlockShared is prepareVerifiedBlockForApply for
// masterchain blocks that more than one pipeline may prepare at the same time.
func (s *Service) prepareVerifiedMasterchainBlockShared(ctx context.Context, block VerifiedBlock) (PreparedBlock, error) {
	cells, elapsed, err := s.preparedMasterchainStateCells(ctx, block)
	if err != nil {
		return PreparedBlock{}, err
	}
	return preparedBlockWithStateCells(block, cells, elapsed), nil
}

func (s *Service) preparedMasterchainStateCells(ctx context.Context, block VerifiedBlock) (storage.StateCellRecords, time.Duration, error) {
	// Keyed on the block, never on its previous ref: two blocks can follow the
	// same prev, and a consumer must never be handed the other one's cells.
	return s.masterPrepare.do(ctx, storage.BlockKey(block.ID), func() (storage.StateCellRecords, error) {
		cells, err := storage.PrepareStateUpdateCells(block.StateUpdate)
		if err != nil {
			return storage.StateCellRecords{}, prepareStateUpdateCellsError(block, err)
		}
		return cells, nil
	})
}

func (g *masterPrepareFlight) do(ctx context.Context, key storage.BlockRootHash, fn func() (storage.StateCellRecords, error)) (storage.StateCellRecords, time.Duration, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = map[storage.BlockRootHash]*masterPrepareCall{}
	}
	if call := g.calls[key]; call != nil {
		done := call.done
		g.mu.Unlock()

		select {
		case <-done:
			return call.cells, call.elapsed, call.err
		case <-ctx.Done():
			// Only the wait is context-aware; the work itself is pure CPU and
			// keeps running for the consumer that started it.
			return storage.StateCellRecords{}, 0, ctx.Err()
		}
	}

	call := &masterPrepareCall{done: make(chan struct{})}
	g.calls[key] = call
	g.mu.Unlock()

	started := time.Now()
	call.cells, call.err = fn()
	call.elapsed = time.Since(started)

	// Removed before the result is published, so nothing is retained past the
	// last waiter and a failure cannot poison a later retry of the same block.
	g.mu.Lock()
	delete(g.calls, key)
	close(call.done)
	g.mu.Unlock()

	return call.cells, call.elapsed, call.err
}
