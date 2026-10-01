package service

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type blockMetaStore interface {
	BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error)
}

// StatusStorage is the persisted chain data needed to build status snapshots.
type StatusStorage interface {
	CurrentState(context.Context) (*storage.CurrentState, error)
	BlockMeta(context.Context, ton.BlockIDExt) (*storage.BlockMeta, error)
	BlockData(context.Context, ton.BlockIDExt) ([]byte, error)
}

// StatusTracker owns live chain progress and service-level runtime metrics.
type StatusTracker struct {
	log            zerolog.Logger
	storage        StatusStorage
	liveBlockCache *storage.LiveBlockCache
	recentTPS      *recentTPSTracker
	lazyCellLoads  lazyCellLoadCounters

	mu                 sync.RWMutex
	current            *storage.CurrentState
	appliedMasterchain *storage.BlockState

	startOnce sync.Once
	wg        sync.WaitGroup
}

// NewStatusTracker creates a tracker backed by persisted and optional live block data.
func NewStatusTracker(
	logger zerolog.Logger,
	store StatusStorage,
	liveBlockCache *storage.LiveBlockCache,
) *StatusTracker {
	return &StatusTracker{
		log:            logger,
		storage:        store,
		liveBlockCache: liveBlockCache,
		recentTPS:      newRecentTPSTracker(),
	}
}

// Start begins asynchronous recent-TPS aggregation.
func (t *StatusTracker) Start(ctx context.Context) {
	t.startOnce.Do(func() {
		t.wg.Add(1)
		go func() {
			defer t.wg.Done()
			t.run(ctx)
		}()
	})
}

// Wait waits for the tracker's asynchronous work to stop.
func (t *StatusTracker) Wait() {
	t.wg.Wait()
}

func (t *StatusTracker) run(ctx context.Context) {
	t.recentTPS.run(ctx)
}

func (t *StatusTracker) observeBlock(
	id ton.BlockIDExt,
	root *cell.Cell,
	genUTime uint32,
	now time.Time,
) {
	t.recentTPS.observe(id, root, genUTime, now)
}

// LazyCellLoadMetrics returns a point-in-time copy of lazy-cell load counters.
func (t *StatusTracker) LazyCellLoadMetrics() []storage.LazyCellLoadMetric {
	return t.lazyCellLoads.snapshot()
}

func (t *StatusTracker) publishCurrentState(current *storage.CurrentState) *storage.CurrentState {
	next := storage.CloneCurrentState(current)

	t.mu.Lock()
	if currentStateBehind(next, t.current) {
		t.mu.Unlock()
		return nil
	}
	t.current = next
	t.mu.Unlock()

	return next
}

func (t *StatusTracker) rememberAppliedMasterchain(state *storage.BlockState) {
	next := storage.CloneBlockState(state)

	t.mu.Lock()
	if t.appliedMasterchain != nil && t.appliedMasterchain.Block.SeqNo >= next.Block.SeqNo {
		t.mu.Unlock()
		return
	}
	t.appliedMasterchain = next
	t.mu.Unlock()
}

func (t *StatusTracker) appliedMasterchainSnapshot() *storage.BlockState {
	t.mu.RLock()
	state := storage.CloneBlockState(t.appliedMasterchain)
	t.mu.RUnlock()

	return state
}

func (t *StatusTracker) currentStateSnapshot(ctx context.Context) (*storage.CurrentState, error) {
	t.mu.RLock()
	current := storage.CloneCurrentState(t.current)
	t.mu.RUnlock()
	if current != nil {
		return current, nil
	}

	return t.storage.CurrentState(ctx)
}

// currentMasterchainSeqno reads the published masterchain head without cloning
// the whole current state. It is used for every queued masterchain broadcast.
func (t *StatusTracker) currentMasterchainSeqno() (uint32, bool) {
	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.current == nil {
		return 0, false
	}
	block := t.current.Masterchain.Block
	if block.Workchain != -1 || block.Shard != topShard {
		return 0, false
	}

	return block.SeqNo, true
}

func (t *StatusTracker) currentBlockState(block ton.BlockIDExt) (*storage.BlockState, error) {
	t.mu.RLock()
	state, err := currentStateBlockState(t.current, block)
	t.mu.RUnlock()

	return state, err
}

// SyncLagSeconds returns the largest timestamp lag among tracked chain heads.
func (t *StatusTracker) SyncLagSeconds() (int64, error) {
	return t.syncLagSeconds(time.Now())
}

func (t *StatusTracker) syncLagSeconds(now time.Time) (int64, error) {
	nowUnix := now.Unix()
	var maxLag int64
	hasLag := false

	t.mu.RLock()
	defer t.mu.RUnlock()

	if t.current == nil {
		return 0, fmt.Errorf("current state is missing for sync lag: %w", storage.ErrNotFound)
	}
	if t.current.Masterchain.Parsed != nil && t.current.Masterchain.Parsed.GenUTime != 0 {
		maxLag = nowUnix - int64(t.current.Masterchain.Parsed.GenUTime)
		hasLag = true
	}
	for _, shard := range t.current.Shards {
		if shard.Block.Workchain != 0 || shard.Parsed == nil || shard.Parsed.GenUTime == 0 {
			continue
		}

		lag := nowUnix - int64(shard.Parsed.GenUTime)
		if !hasLag || lag > maxLag {
			maxLag = lag
			hasLag = true
		}
	}
	if !hasLag {
		return 0, fmt.Errorf("current state has no block utime for sync lag: %w", storage.ErrNotFound)
	}

	return maxLag, nil
}

func currentStateBehind(next *storage.CurrentState, current *storage.CurrentState) bool {
	if current == nil {
		return false
	}

	nextShardClientSeqno := next.ShardClientSeqno
	if nextShardClientSeqno == 0 {
		nextShardClientSeqno = next.Masterchain.Block.SeqNo
	}
	currentShardClientSeqno := current.ShardClientSeqno
	if currentShardClientSeqno == 0 {
		currentShardClientSeqno = current.Masterchain.Block.SeqNo
	}
	if nextShardClientSeqno != currentShardClientSeqno {
		return nextShardClientSeqno < currentShardClientSeqno
	}

	return next.Masterchain.Block.SeqNo < current.Masterchain.Block.SeqNo
}
