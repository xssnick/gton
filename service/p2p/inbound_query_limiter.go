package p2p

import (
	"errors"
	"sync"
	"time"

	tonnodeapi "github.com/xssnick/tonutils-go/adnl/node"
	"github.com/xssnick/tonutils-go/tl"
)

const (
	inboundQueryRateWindowDuration = time.Second
	inboundQueryGlobalLimit        = uint64(96)
	inboundQueryHeavyLimit         = uint64(64)
	inboundQueryMediumLimit        = uint64(72)
	inboundQuerySmallLimit         = uint64(200)

	fastSyncHeavyQueryCostUnit = uint64(2 << 20)
	fastSyncZeroStateMaxSize   = uint64(16 << 20)
)

var errInboundQueryRateLimited = errors.New(
	"fast sync inbound query rate limit exceeded",
)

type inboundQueryClass uint8

const (
	inboundQueryUnlimited inboundQueryClass = iota
	inboundQueryHeavy
	inboundQueryMedium
	inboundQuerySmall
)

type inboundQueryLimiter struct {
	mu sync.Mutex

	global inboundQueryRateWindow
	heavy  inboundQueryRateWindow
	medium inboundQueryRateWindow
	small  inboundQueryRateWindow
}

type inboundQueryRateWindow struct {
	duration time.Duration
	limit    uint64
	entries  []inboundQueryRateEntry
	head     int
	count    int
	weight   uint64
}

type inboundQueryRateEntry struct {
	at     time.Time
	weight uint64
}

func newInboundQueryLimiter() inboundQueryLimiter {
	return inboundQueryLimiter{
		global: newInboundQueryRateWindow(inboundQueryGlobalLimit),
		heavy:  newInboundQueryRateWindow(inboundQueryHeavyLimit),
		medium: newInboundQueryRateWindow(inboundQueryMediumLimit),
		small:  newInboundQueryRateWindow(inboundQuerySmallLimit),
	}
}

func newInboundQueryRateWindow(limit uint64) inboundQueryRateWindow {
	return inboundQueryRateWindow{
		duration: inboundQueryRateWindowDuration,
		limit:    limit,
		entries:  make([]inboundQueryRateEntry, int(limit)),
	}
}

func (l *inboundQueryLimiter) Allow(
	query tl.Serializable,
	now time.Time,
) bool {
	class, cost := inboundQueryCost(query)
	if class == inboundQueryUnlimited {
		return true
	}

	l.mu.Lock()
	defer l.mu.Unlock()

	category := l.category(class)
	if class == inboundQuerySmall {
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

func (l *inboundQueryLimiter) category(
	class inboundQueryClass,
) *inboundQueryRateWindow {
	switch class {
	case inboundQueryHeavy:
		return &l.heavy
	case inboundQueryMedium:
		return &l.medium
	case inboundQuerySmall:
		return &l.small
	default:
		panic("invalid fast sync query class")
	}
}

func inboundQueryCost(
	query tl.Serializable,
) (inboundQueryClass, uint64) {
	switch query := query.(type) {
	case GetArchiveSlice:
		return inboundQueryHeavy,
			inboundHeavyQueryCost(positiveInboundQuerySize(
				int64(query.MaxSize),
			))
	case DownloadPersistentStateSliceV2:
		return inboundQueryHeavy,
			inboundHeavyQueryCost(positiveInboundQuerySize(query.MaxSize))
	case DownloadZeroState:
		return inboundQueryHeavy,
			inboundHeavyQueryCost(fastSyncZeroStateMaxSize)

	case tonnodeapi.DownloadBlock,
		tonnodeapi.DownloadBlockFull,
		DownloadNextBlockFull,
		DownloadNextBlocksFull,
		DownloadBlockProof,
		DownloadBlockProofLink,
		DownloadKeyBlockProof,
		DownloadKeyBlockProofLink,
		GetOutMsgQueueProof:
		return inboundQueryMedium, 1

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
		return inboundQuerySmall, 1

	default:
		return inboundQueryUnlimited, 0
	}
}

func positiveInboundQuerySize(size int64) uint64 {
	if size <= 0 {
		return 0
	}
	return uint64(size)
}

func inboundHeavyQueryCost(requestedMaxSize uint64) uint64 {
	cost := requestedMaxSize / fastSyncHeavyQueryCostUnit
	if requestedMaxSize%fastSyncHeavyQueryCostUnit != 0 {
		cost++
	}
	if cost == 0 {
		return 1
	}
	return cost
}

func (w *inboundQueryRateWindow) allows(
	now time.Time,
	weight uint64,
) bool {
	w.prune(now)
	return weight <= w.limit && weight <= w.limit-w.weight
}

func (w *inboundQueryRateWindow) insert(
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
	w.entries[index] = inboundQueryRateEntry{at: now, weight: weight}
	w.count++
	w.weight += weight
}

func (w *inboundQueryRateWindow) prune(now time.Time) {
	for w.count > 0 &&
		now.Sub(w.entries[w.head].at) > w.duration {
		w.weight -= w.entries[w.head].weight
		w.entries[w.head] = inboundQueryRateEntry{}
		w.head = (w.head + 1) % len(w.entries)
		w.count--
	}
}
