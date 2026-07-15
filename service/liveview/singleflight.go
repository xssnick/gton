package liveview

import (
	"context"
	"sync"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type liveBlockLoadResult struct {
	root *cell.Cell
	data []byte
}

type liveLoadGroup[K comparable, V any] struct {
	mu    sync.Mutex
	calls map[K]*liveLoadCall[V]
}

type liveLoadCall[V any] struct {
	done  chan struct{}
	value V
	err   error
}

func (g *liveLoadGroup[K, V]) do(ctx context.Context, key K, fn func() (V, error)) (V, error) {
	g.mu.Lock()
	if g.calls == nil {
		g.calls = map[K]*liveLoadCall[V]{}
	}
	if call := g.calls[key]; call != nil {
		done := call.done
		g.mu.Unlock()

		select {
		case <-done:
			return call.value, call.err
		case <-ctx.Done():
			var zero V
			return zero, ctx.Err()
		}
	}

	call := &liveLoadCall[V]{done: make(chan struct{})}
	g.calls[key] = call
	g.mu.Unlock()

	call.value, call.err = fn()

	g.mu.Lock()
	delete(g.calls, key)
	close(call.done)
	g.mu.Unlock()

	return call.value, call.err
}
