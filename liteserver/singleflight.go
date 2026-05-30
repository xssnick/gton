package liteserver

import (
	"context"
	"sync"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type liveBlockLoadResult struct {
	root *cell.Cell
	data []byte
}

type liveLoadGroup[K comparable] struct {
	mu    sync.Mutex
	calls map[K]*liveBlockLoadCall
}

type liveBlockLoadCall struct {
	done  chan struct{}
	value any
	err   error
}

func (g *liveLoadGroup[K]) do(ctx context.Context, key K, fn func() (any, error)) (any, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = map[K]*liveBlockLoadCall{}
	}
	if call := g.calls[key]; call != nil {
		done := call.done
		g.mu.Unlock()

		select {
		case <-done:
			return call.value, call.err
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}

	call := &liveBlockLoadCall{done: make(chan struct{})}
	g.calls[key] = call
	g.mu.Unlock()

	call.value, call.err = fn()

	g.mu.Lock()
	delete(g.calls, key)
	close(call.done)
	g.mu.Unlock()

	return call.value, call.err
}
