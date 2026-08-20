package pebblestore

import (
	"context"
	"encoding/binary"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type decodedCellFollowerContext struct {
	context.Context
	joined chan struct{}
	once   sync.Once
}

func (c *decodedCellFollowerContext) Done() <-chan struct{} {
	c.once.Do(func() {
		close(c.joined)
	})

	return c.Context.Done()
}

func TestDecodedCellColdMissIsCoalescedAndCanonical(t *testing.T) {
	cache := newDecodedCellCache(decodedCellCacheConfig{enabled: true, shards: 1, entries: 8})
	store := &Store{decodedCells: cache}
	want := cell.BeginCell().MustStoreUInt(0xc011a, 32).EndCell()
	boc := want.ToBOC()
	hash := want.HashKeyAt(0)

	const workers = 32
	start := make(chan struct{})
	calling := make(chan struct{})
	release := make(chan struct{})
	var loads atomic.Int32
	results := make([]*cell.Cell, workers)
	errs := make([]error, workers)
	var group sync.WaitGroup
	group.Add(workers)
	for i := range workers {
		go func() {
			defer group.Done()
			<-start
			calling <- struct{}{}
			results[i], errs[i] = store.loadDecodedCell(
				context.Background(),
				cache,
				activeCellCacheNamespace,
				hash[:],
				func(context.Context) (*cell.Cell, error) {
					loads.Add(1)
					<-release

					return cell.FromBOC(boc)
				},
			)
		}()
	}
	close(start)
	for range workers {
		<-calling
	}
	close(release)
	group.Wait()

	if loads.Load() != 1 {
		t.Fatalf("cold miss loads = %d, want one", loads.Load())
	}
	for i := range workers {
		if errs[i] != nil {
			t.Fatalf("worker %d: %v", i, errs[i])
		}
		if results[i] != results[0] {
			t.Fatalf("worker %d received a non-canonical cell pointer", i)
		}
	}
}

func TestDecodedCellFollowerCancellationDoesNotCancelSharedFill(t *testing.T) {
	cache := newDecodedCellCache(decodedCellCacheConfig{enabled: true, shards: 1, entries: 8})
	store := &Store{decodedCells: cache}
	want := cell.BeginCell().MustStoreUInt(0x51a7e, 32).EndCell()
	hash := want.HashKeyAt(0)

	entered := make(chan struct{})
	release := make(chan struct{})
	var loads atomic.Int32
	load := func(context.Context) (*cell.Cell, error) {
		if loads.Add(1) == 1 {
			close(entered)
		}
		<-release

		return want, nil
	}

	firstDone := make(chan *cell.Cell, 1)
	firstErr := make(chan error, 1)
	go func() {
		loaded, err := store.loadDecodedCell(context.Background(), cache, activeCellCacheNamespace, hash[:], load)
		firstDone <- loaded
		firstErr <- err
	}()
	<-entered

	followerBaseCtx, cancelFollower := context.WithCancel(context.Background())
	joined := make(chan struct{})
	followerCtx := &decodedCellFollowerContext{Context: followerBaseCtx, joined: joined}
	followerErr := make(chan error, 1)
	go func() {
		_, err := store.loadDecodedCell(followerCtx, cache, activeCellCacheNamespace, hash[:], load)
		followerErr <- err
	}()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("second waiter did not join the shared fill")
	}

	cancelFollower()
	if err := <-followerErr; err != context.Canceled {
		t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
	}
	close(release)
	if err := <-firstErr; err != nil {
		t.Fatal(err)
	}
	if loaded := <-firstDone; loaded != want {
		t.Fatal("remaining waiter did not receive the shared fill")
	}
	if loads.Load() != 1 {
		t.Fatalf("shared fill loads = %d, want one", loads.Load())
	}
}

func TestDecodedCellLeaderCancellationDoesNotCancelLiveFollower(t *testing.T) {
	cache := newDecodedCellCache(decodedCellCacheConfig{enabled: true, shards: 1, entries: 8})
	store := &Store{decodedCells: cache}
	want := cell.BeginCell().MustStoreUInt(0x1ead, 32).EndCell()
	hash := want.HashKeyAt(0)

	leaderEntered := make(chan struct{})
	retryEntered := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	var loads atomic.Int32
	releaseLoad := func() {
		releaseOnce.Do(func() {
			close(release)
		})
	}
	defer releaseLoad()

	load := func(loadCtx context.Context) (*cell.Cell, error) {
		switch loads.Add(1) {
		case 1:
			close(leaderEntered)
		case 2:
			close(retryEntered)
		}

		select {
		case <-release:
			return want, nil
		case <-loadCtx.Done():
			return nil, loadCtx.Err()
		}
	}
	type loadResult struct {
		cell *cell.Cell
		err  error
	}

	leaderCtx, cancelLeader := context.WithCancel(context.Background())
	leaderDone := make(chan loadResult, 1)
	go func() {
		loaded, err := store.loadDecodedCell(leaderCtx, cache, activeCellCacheNamespace, hash[:], load)
		leaderDone <- loadResult{cell: loaded, err: err}
	}()
	<-leaderEntered

	joined := make(chan struct{})
	followerCtx := &decodedCellFollowerContext{Context: context.Background(), joined: joined}
	followerDone := make(chan loadResult, 1)
	go func() {
		loaded, err := store.loadDecodedCell(followerCtx, cache, activeCellCacheNamespace, hash[:], load)
		followerDone <- loadResult{cell: loaded, err: err}
	}()
	select {
	case <-joined:
	case <-time.After(time.Second):
		t.Fatal("follower did not join the leader's flight")
	}

	cancelLeader()
	select {
	case result := <-leaderDone:
		if !errors.Is(result.err, context.Canceled) {
			t.Fatalf("canceled leader error = %v, want context.Canceled", result.err)
		}
	case <-time.After(time.Second):
		t.Fatal("synchronous leader did not return after its context was canceled")
	}
	select {
	case <-retryEntered:
	case <-time.After(time.Second):
		t.Fatal("live follower did not retry the canceled leader's fill")
	}

	releaseLoad()
	if result := <-followerDone; result.err != nil || result.cell != want {
		t.Fatalf("live follower result = (%p, %v), want (%p, nil)", result.cell, result.err, want)
	}
	if loads.Load() != 2 {
		t.Fatalf("cancellation-edge loads = %d, want one canceled load plus one retry", loads.Load())
	}
	if cached, err := cache.getHash(activeCellCacheNamespace, hash); err != nil || cached != want {
		t.Fatalf("cached shared result = (%p, %v), want (%p, nil)", cached, err, want)
	}
}

func TestDecodedCellCanceledOnlyWaiterCancelsSharedFill(t *testing.T) {
	cache := newDecodedCellCache(decodedCellCacheConfig{enabled: true, shards: 1, entries: 8})
	store := &Store{decodedCells: cache}
	want := cell.BeginCell().MustStoreUInt(0xca11ce, 32).EndCell()
	hash := want.HashKeyAt(0)

	entered := make(chan struct{})
	stopped := make(chan struct{})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() {
		_, err := store.loadDecodedCell(
			ctx,
			cache,
			activeCellCacheNamespace,
			hash[:],
			func(loadCtx context.Context) (*cell.Cell, error) {
				close(entered)
				<-loadCtx.Done()
				close(stopped)

				return nil, loadCtx.Err()
			},
		)
		done <- err
	}()
	<-entered
	cancel()
	if err := <-done; err != context.Canceled {
		t.Fatalf("canceled waiter error = %v, want context.Canceled", err)
	}
	select {
	case <-stopped:
	case <-time.After(time.Second):
		t.Fatal("shared fill survived after its last waiter canceled")
	}
	if _, err := cache.getHash(activeCellCacheNamespace, hash); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("canceled fill cache lookup = %v, want not found", err)
	}
}

func BenchmarkDecodedCellUniqueMiss(b *testing.B) {
	want := cell.BeginCell().MustStoreUInt(0xc011a, 32).EndCell()

	for _, test := range []struct {
		name string
		ctx  func() (context.Context, context.CancelFunc)
	}{
		{
			name: "background",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.Background(), func() {}
			},
		},
		{
			name: "cancelable",
			ctx: func() (context.Context, context.CancelFunc) {
				return context.WithCancel(context.Background())
			},
		},
	} {
		b.Run(test.name, func(b *testing.B) {
			cache := newDecodedCellCache(decodedCellCacheConfig{enabled: true, shards: 1, entries: 8})
			store := &Store{decodedCells: cache}
			ctx, cancel := test.ctx()
			defer cancel()
			load := func(context.Context) (*cell.Cell, error) {
				return want, nil
			}
			var hash [32]byte
			iteration := uint64(0)

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				binary.LittleEndian.PutUint64(hash[:], iteration)
				iteration++
				loaded, err := store.loadDecodedCell(
					ctx,
					cache,
					activeCellCacheNamespace,
					hash[:],
					load,
				)
				if err != nil || loaded != want {
					b.Fatalf("unique miss result = (%p, %v), want (%p, nil)", loaded, err, want)
				}
			}
		})
	}
}
