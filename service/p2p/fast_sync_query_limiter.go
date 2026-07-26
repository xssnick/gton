package p2p

import (
	"errors"
	"sync"
	"time"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	fastSyncQueryRateWindowDuration = time.Second
	fastSyncQueryGlobalLimit        = uint64(96)
	fastSyncQueryHeavyLimit         = uint64(64)
	fastSyncQueryMediumLimit        = uint64(72)
	fastSyncQuerySmallLimit         = uint64(200)

	fastSyncHeavyQueryCostUnit = uint64(2 << 20)
	fastSyncZeroStateMaxSize   = uint64(16 << 20)
)

var errFastSyncQueryRateLimited = errors.New(
	"fast sync inbound query rate limit exceeded",
)

type fastSyncQueryClass uint8

const (
	fastSyncQueryUnlimited fastSyncQueryClass = iota
	fastSyncQueryHeavy
	fastSyncQueryMedium
	fastSyncQuerySmall
)

type fastSyncQueryLimiter struct {
	mu sync.Mutex

	global fastSyncQueryRateWindow
	heavy  fastSyncQueryRateWindow
	medium fastSyncQueryRateWindow
	small  fastSyncQueryRateWindow
}

type fastSyncQueryRateWindow struct {
	duration time.Duration
	limit    uint64
	entries  []fastSyncQueryRateEntry
	head     int
	count    int
	weight   uint64
}

type fastSyncQueryRateEntry struct {
	at     time.Time
	weight uint64
}

func newFastSyncQueryLimiter() fastSyncQueryLimiter {
	return fastSyncQueryLimiter{
		global: newFastSyncQueryRateWindow(fastSyncQueryGlobalLimit),
		heavy:  newFastSyncQueryRateWindow(fastSyncQueryHeavyLimit),
		medium: newFastSyncQueryRateWindow(fastSyncQueryMediumLimit),
		small:  newFastSyncQueryRateWindow(fastSyncQuerySmallLimit),
	}
}

func newFastSyncQueryRateWindow(limit uint64) fastSyncQueryRateWindow {
	return fastSyncQueryRateWindow{
		duration: fastSyncQueryRateWindowDuration,
		limit:    limit,
		entries:  make([]fastSyncQueryRateEntry, int(limit)),
	}
}

func (l *fastSyncQueryLimiter) Allow(
	query tl.Serializable,
	now time.Time,
) bool {
	class, cost := fastSyncQueryCost(query)
	if class == fastSyncQueryUnlimited {
		return true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	category := l.category(class)
	if class == fastSyncQuerySmall {
		if !category.allows(now, cost) {
			return false
		}

		category.insert(now, cost)
		return true
	}

	if !l.global.allows(now, cost) || !category.allows(now, cost) {
		return false
	}

	l.global.insert(now, cost)
	category.insert(now, cost)
	return true
}

func (l *fastSyncQueryLimiter) category(
	class fastSyncQueryClass,
) *fastSyncQueryRateWindow {
	switch class {
	case fastSyncQueryHeavy:
		return &l.heavy
	case fastSyncQueryMedium:
		return &l.medium
	case fastSyncQuerySmall:
		return &l.small
	default:
		panic("invalid fast sync query class")
	}
}

func fastSyncQueryCost(
	query tl.Serializable,
) (fastSyncQueryClass, uint64) {
	switch query := query.(type) {
	case GetArchiveSlice:
		return fastSyncQueryHeavy,
			fastSyncHeavyQueryCost(positiveFastSyncQuerySize(
				int64(query.MaxSize),
			))
	case DownloadPersistentStateSliceV2:
		return fastSyncQueryHeavy,
			fastSyncHeavyQueryCost(positiveFastSyncQuerySize(query.MaxSize))
	case DownloadZeroState:
		return fastSyncQueryHeavy,
			fastSyncHeavyQueryCost(fastSyncZeroStateMaxSize)

	case tonnodeapi.DownloadBlock,
		tonnodeapi.DownloadBlockFull,
		DownloadNextBlockFull,
		DownloadNextBlocksFull,
		DownloadBlockProof,
		DownloadBlockProofLink,
		DownloadKeyBlockProof,
		DownloadKeyBlockProofLink,
		GetOutMsgQueueProof:
		return fastSyncQueryMedium, 1

	case GetNextBlockDescription,
		PrepareBlockProof,
		PrepareKeyBlockProof,
		PrepareBlock,
		PrepareZeroState,
		GetNextKeyBlockIDs,
		GetArchiveInfo,
		GetShardArchiveInfo,
		PreparePersistentState,
		GetPersistentStateSizeV2:
		return fastSyncQuerySmall, 1

	default:
		return fastSyncQueryUnlimited, 0
	}
}

func positiveFastSyncQuerySize(size int64) uint64 {
	if size <= 0 {
		return 0
	}
	return uint64(size)
}

func fastSyncHeavyQueryCost(requestedMaxSize uint64) uint64 {
	cost := requestedMaxSize / fastSyncHeavyQueryCostUnit
	if requestedMaxSize%fastSyncHeavyQueryCostUnit != 0 {
		cost++
	}
	if cost == 0 {
		return 1
	}
	return cost
}

func (w *fastSyncQueryRateWindow) allows(
	now time.Time,
	weight uint64,
) bool {
	w.prune(now)
	return weight <= w.limit && weight <= w.limit-w.weight
}

func (w *fastSyncQueryRateWindow) insert(
	now time.Time,
	weight uint64,
) {
	if w.count > 0 {
		last := (w.head + w.count - 1) % len(w.entries)
		if w.entries[last].at.Equal(now) {
			w.entries[last].weight += weight
			w.weight += weight
			return
		}
	}

	index := (w.head + w.count) % len(w.entries)
	w.entries[index] = fastSyncQueryRateEntry{at: now, weight: weight}
	w.count++
	w.weight += weight
}

func (w *fastSyncQueryRateWindow) prune(now time.Time) {
	for w.count > 0 &&
		now.Sub(w.entries[w.head].at) > w.duration {
		w.weight -= w.entries[w.head].weight
		w.entries[w.head] = fastSyncQueryRateEntry{}
		w.head = (w.head + 1) % len(w.entries)
		w.count--
	}
}
