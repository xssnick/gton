package service

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	recentTPSWindow        = 10 * time.Second
	recentTPSQueueCapacity = 256
	recentTPSRefreshPeriod = time.Second
)

type recentTPSBlock struct {
	id       ton.BlockIDExt
	key      storage.BlockRootHash
	root     *cell.Cell
	genUTime uint32
}

type recentTPSBucket struct {
	transactions uint64
	masterBlocks int
	blocks       []storage.BlockRootHash
}

type recentTPSWindowState struct {
	window     time.Duration
	buckets    map[uint32]*recentTPSBucket
	seen       map[storage.BlockRootHash]uint32
	freshSince time.Time
}

func newRecentTPSWindowState(window time.Duration) recentTPSWindowState {
	return recentTPSWindowState{
		window:  window,
		buckets: map[uint32]*recentTPSBucket{},
		seen:    map[storage.BlockRootHash]uint32{},
	}
}

func (s *recentTPSWindowState) reset() {
	clear(s.buckets)
	clear(s.seen)
	s.freshSince = time.Time{}
}

func (s *recentTPSWindowState) add(block recentTPSBlock, transactions uint32) {
	if _, ok := s.seen[block.key]; ok {
		return
	}

	bucket := s.buckets[block.genUTime]
	if bucket == nil {
		bucket = &recentTPSBucket{}
		s.buckets[block.genUTime] = bucket
	}
	bucket.transactions += uint64(transactions)
	if block.id.Workchain == -1 {
		bucket.masterBlocks++
	}
	bucket.blocks = append(bucket.blocks, block.key)
	s.seen[block.key] = block.genUTime
}

func (s *recentTPSWindowState) snapshot(now time.Time) StatusTPSSnapshot {
	cutoff := now.Add(-s.window).Unix()
	nowUnix := now.Unix()
	var transactions uint64
	var masterBlocks int
	eligible := false

	for genUTime, bucket := range s.buckets {
		if int64(genUTime) <= cutoff {
			for _, key := range bucket.blocks {
				delete(s.seen, key)
			}
			delete(s.buckets, genUTime)
			continue
		}
		if int64(genUTime) > nowUnix {
			continue
		}

		eligible = true
		transactions += bucket.transactions
		masterBlocks += bucket.masterBlocks
	}

	if eligible && s.freshSince.IsZero() {
		s.freshSince = now
	}
	complete := !s.freshSince.IsZero() && !now.Before(s.freshSince.Add(s.window))

	return StatusTPSSnapshot{
		WindowMasters:   masterBlocks,
		Transactions:    transactions,
		DurationSeconds: int64(s.window / time.Second),
		TPS:             float64(transactions) / s.window.Seconds(),
		Complete:        complete,
	}
}

type recentTPSTracker struct {
	window        time.Duration
	blocks        chan recentTPSBlock
	snapshot      atomic.Pointer[StatusTPSSnapshot]
	invalidations atomic.Uint64
}

func newRecentTPSTracker(window time.Duration) *recentTPSTracker {
	tracker := &recentTPSTracker{
		window: window,
		blocks: make(chan recentTPSBlock, recentTPSQueueCapacity),
	}
	tracker.storeIncomplete()
	return tracker
}

func (t *recentTPSTracker) observe(id ton.BlockIDExt, root *cell.Cell, genUTime uint32, now time.Time) {
	if int64(genUTime) <= now.Add(-t.window).Unix() {
		return
	}
	if genUTime == 0 {
		t.invalidate()
		return
	}

	block := recentTPSBlock{
		id:       id,
		key:      storage.BlockKey(id),
		root:     root,
		genUTime: genUTime,
	}
	select {
	case t.blocks <- block:
	default:
		t.invalidate()
	}
}

func (t *recentTPSTracker) invalidate() {
	t.invalidations.Add(1)
	t.storeIncomplete()
}

func (t *recentTPSTracker) storeIncomplete() {
	windowSeconds := int64(t.window / time.Second)
	t.snapshot.Store(&StatusTPSSnapshot{
		DurationSeconds: windowSeconds,
	})
}

func (t *recentTPSTracker) statusSnapshot() StatusTPSSnapshot {
	return *t.snapshot.Load()
}

func (t *recentTPSTracker) run(ctx context.Context) {
	refresh := time.NewTicker(recentTPSRefreshPeriod)
	defer refresh.Stop()

	state := newRecentTPSWindowState(t.window)
	version := t.invalidations.Load()
	publish := func(now time.Time) {
		currentVersion := t.invalidations.Load()
		if currentVersion != version {
			state.reset()
			version = currentVersion
		}

		snapshot := state.snapshot(now)
		t.snapshot.Store(&snapshot)
		if t.invalidations.Load() != version {
			t.storeIncomplete()
		}
	}

	for {
		select {
		case block := <-t.blocks:
			now := time.Now()
			if int64(block.genUTime) <= now.Add(-t.window).Unix() {
				publish(now)
				continue
			}

			transactions, err := storage.BlockTransactionCountFromBlockCell(block.id, block.root)
			if err != nil {
				t.invalidate()
				publish(now)
				continue
			}
			state.add(block, transactions)
			publish(now)
		case now := <-refresh.C:
			publish(now)
		case <-ctx.Done():
			return
		}
	}
}
