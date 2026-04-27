package p2p

import (
	"context"
	"sync"
)

type unboundedQueue[T any] struct {
	mu      sync.Mutex
	items   []T
	notify  chan struct{}
	closeCh chan struct{}
	closed  bool
}

func newUnboundedQueue[T any]() *unboundedQueue[T] {
	return &unboundedQueue[T]{
		notify:  make(chan struct{}, 1),
		closeCh: make(chan struct{}),
	}
}

func (q *unboundedQueue[T]) Push(item T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return false
	}

	q.items = append(q.items, item)
	if len(q.items) == 1 {
		select {
		case q.notify <- struct{}{}:
		default:
		}
	}
	return true
}

func (q *unboundedQueue[T]) Pop(ctx context.Context) (T, bool) {
	var zero T

	for {
		q.mu.Lock()
		if len(q.items) > 0 {
			item := q.items[0]
			q.items[0] = zero
			q.items = q.items[1:]
			if len(q.items) > 0 {
				select {
				case q.notify <- struct{}{}:
				default:
				}
			}
			q.mu.Unlock()
			return item, true
		}
		closed := q.closed
		closeCh := q.closeCh
		notify := q.notify
		q.mu.Unlock()

		if closed {
			return zero, false
		}

		select {
		case <-ctx.Done():
			return zero, false
		case <-closeCh:
			return zero, false
		case <-notify:
		}
	}
}

func (q *unboundedQueue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}
	q.closed = true
	close(q.closeCh)
}
