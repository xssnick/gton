package p2p

import (
	"sync"

	tnstore "github.com/xssnick/gton/service/storage"
)

type keyedBroadcastWaiters struct {
	mx      sync.Mutex
	waiters map[tnstore.BlockRootHash][]chan struct{}
}

func (w *keyedBroadcastWaiters) watch(key tnstore.BlockRootHash) (<-chan struct{}, func()) {
	ch := make(chan struct{})

	w.mx.Lock()
	if w.waiters == nil {
		w.waiters = map[tnstore.BlockRootHash][]chan struct{}{}
	}
	w.waiters[key] = append(w.waiters[key], ch)
	w.mx.Unlock()

	var once sync.Once
	cancel := func() {
		once.Do(func() {
			w.remove(key, ch)
		})
	}
	return ch, cancel
}

func (w *keyedBroadcastWaiters) remove(key tnstore.BlockRootHash, ch chan struct{}) {
	w.mx.Lock()
	defer w.mx.Unlock()

	waiters := w.waiters[key]
	for i, waiter := range waiters {
		if waiter == ch {
			waiters = append(waiters[:i], waiters[i+1:]...)
			break
		}
	}
	if len(waiters) == 0 {
		delete(w.waiters, key)
		return
	}
	w.waiters[key] = waiters
}

func (w *keyedBroadcastWaiters) notify(key tnstore.BlockRootHash) {
	w.mx.Lock()
	waiters := w.waiters[key]
	delete(w.waiters, key)
	w.mx.Unlock()

	for _, waiter := range waiters {
		close(waiter)
	}
}
