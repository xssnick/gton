package p2p

import (
	"context"
	"sync"
)

type boundedQueue[T any] struct {
	mu       sync.Mutex
	items    []queuedItem[T]
	bytes    int64
	maxItems int
	maxBytes int64
	dropped  uint64
	size     func(T) int64
	notify   chan struct{}
	closeCh  chan struct{}
	closed   bool
}

type queuedItem[T any] struct {
	value T
	bytes int64
}

func newBoundedQueue[T any](maxItems int, maxBytes int64, size func(T) int64) *boundedQueue[T] {
	return &boundedQueue[T]{
		maxItems: maxItems,
		maxBytes: maxBytes,
		size:     size,
		notify:   make(chan struct{}, 1),
		closeCh:  make(chan struct{}),
	}
}

func (q *boundedQueue[T]) Push(item T) bool {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		q.dropped++
		return false
	}
	if q.maxItems > 0 && len(q.items) >= q.maxItems {
		q.dropped++
		return false
	}

	itemBytes := q.itemBytes(item)
	if q.maxBytes > 0 && (itemBytes > q.maxBytes || q.bytes > q.maxBytes-itemBytes) {
		q.dropped++
		return false
	}

	q.items = append(q.items, queuedItem[T]{value: item, bytes: itemBytes})
	q.bytes += itemBytes
	if len(q.items) == 1 {
		select {
		case q.notify <- struct{}{}:
		default:
		}
	}
	return true
}

func (q *boundedQueue[T]) Pop(ctx context.Context) (T, bool) {
	var zero T

	for {
		q.mu.Lock()
		if item, ok := q.popLocked(); ok {
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

func (q *boundedQueue[T]) TryPop() (T, bool) {
	var zero T
	if q == nil {
		return zero, false
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	return q.popLocked()
}

func (q *boundedQueue[T]) popLocked() (T, bool) {
	var zero T
	if len(q.items) == 0 {
		return zero, false
	}

	item := q.items[0]
	q.items[0] = queuedItem[T]{}
	q.items = q.items[1:]
	q.bytes -= item.bytes
	if len(q.items) > 0 {
		select {
		case q.notify <- struct{}{}:
		default:
		}
	}
	return item.value, true
}

func (q *boundedQueue[T]) StatusSnapshot(name string) QueueStatusSnapshot {
	q.mu.Lock()
	defer q.mu.Unlock()

	return QueueStatusSnapshot{
		Name:     name,
		Items:    len(q.items),
		Bytes:    q.bytes,
		MaxItems: q.maxItems,
		MaxBytes: q.maxBytes,
		Dropped:  q.dropped,
	}
}

func (q *boundedQueue[T]) itemBytes(item T) int64 {
	if q.size == nil {
		return 1
	}
	bytes := q.size(item)
	if bytes <= 0 {
		return 1
	}
	return bytes
}

func (q *boundedQueue[T]) Close() {
	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return
	}
	q.closed = true
	close(q.closeCh)
}

func popPriority[T any](ctx context.Context, priority *boundedQueue[T], fallback *boundedQueue[T]) (T, bool) {
	var zero T

	for {
		if item, ok := priority.TryPop(); ok {
			return item, true
		}
		if item, ok := fallback.TryPop(); ok {
			return item, true
		}

		priorityNotify, priorityClose, priorityClosed := queueWaitState(priority)
		fallbackNotify, fallbackClose, fallbackClosed := queueWaitState(fallback)
		if priorityClosed && fallbackClosed {
			return zero, false
		}

		select {
		case <-ctx.Done():
			return zero, false
		case <-priorityNotify:
		case <-fallbackNotify:
		case <-priorityClose:
		case <-fallbackClose:
		}
	}
}

func queueWaitState[T any](q *boundedQueue[T]) (<-chan struct{}, <-chan struct{}, bool) {
	if q == nil {
		return nil, nil, true
	}

	q.mu.Lock()
	defer q.mu.Unlock()

	if q.closed {
		return nil, nil, true
	}
	return q.notify, q.closeCh, false
}
