package msgpool

import (
	"bytes"
	"container/heap"
	"encoding/binary"
	"fmt"
	"math/rand"
	"slices"
	"sync"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// External message priorities are ordered from lowest to highest. Locally
// submitted messages take precedence over admitted network broadcasts.
const (
	ExternalPriorityBroadcast = 0
	ExternalPriorityLocal     = 1
)

const accountRejectRetrySoftLimit = 1024

type retryReason uint8

const (
	retryNone retryReason = iota
	retryIncluded
	retryAccountRejected
)

// entry is one pooled message.
type entry struct {
	msg         *ExternalMessage
	priority    int
	deleteAt    time.Time
	retryAt     time.Time
	retryReason retryReason
	generation  uint64
	retryCount  uint32
	expiryIndex int
	normIndex   int
	removed     bool
}

func (e *entry) expired(now time.Time) bool {
	return !e.deleteAt.After(now)
}

// prioritySlab holds one priority level.
type prioritySlab struct {
	entries map[[32]byte]*entry
	byAddr  map[addrKey]int
}

type expiryHeap []*entry

func (h expiryHeap) Len() int { return len(h) }
func (h expiryHeap) Less(i, j int) bool {
	if h[i].deleteAt.Equal(h[j].deleteAt) {
		return bytes.Compare(h[i].msg.Hash[:], h[j].msg.Hash[:]) < 0
	}
	return h[i].deleteAt.Before(h[j].deleteAt)
}
func (h expiryHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].expiryIndex = i
	h[j].expiryIndex = j
}
func (h *expiryHeap) Push(x any) {
	e := x.(*entry)
	e.expiryIndex = len(*h)
	*h = append(*h, e)
}
func (h *expiryHeap) Pop() any {
	old := *h
	n := len(old)
	e := old[n-1]
	old[n-1] = nil
	*h = old[:n-1]
	e.expiryIndex = -1
	return e
}

// Pool is the external-message mempool: it accumulates validated messages
// and hands a bounded, correctly ordered batch to the collator when a block
// is being formed. Nothing else — no emulation, no networking, no
// subscriptions. All methods are safe for concurrent use; every operation
// is a single short critical section.
type Pool struct {
	cfg   Config
	log   zerolog.Logger
	clock Clock

	internals *Internals

	mu         sync.Mutex
	closed     bool
	slabs      map[int]*prioritySlab
	prioDesc   []int // slab keys, descending
	byHash     map[[32]byte]*entry
	byNorm     map[[32]byte][]*entry
	expiry     expiryHeap
	rnd        *rand.Rand
	totalCount int
	totalBytes int64
	streams    map[uint64]*ExternalStream
	nextStream uint64

	stats statCounters

	cleanupStop chan struct{}
	cleanupDone chan struct{}
	cleanupOnce sync.Once
}

// Internals exposes the internal-message section of the pool.
func (p *Pool) Internals() *Internals { return p.internals }

// statCounters is mutated and read only under Pool.mu.
type statCounters struct {
	dedupSkipped, priorityBumps               uint64
	added, overflowMempool, overflowBytes     uint64
	overflowAddress, expired                  uint64
	invalidDeleted, includedQuarantined       uint64
	includedReleased, rejectedDelayed         uint64
	rejectedRetried, rejectedExhausted        uint64
	rejectedPressure                          uint64
	staleFeedback, appliedReq, appliedDeleted uint64
}

// New builds a pool.
func New(cfg Config) *Pool {
	cfg.applyDefaults()
	p := &Pool{
		cfg:       cfg,
		log:       *cfg.Logger,
		clock:     cfg.Clock,
		internals: newInternals(*cfg.Logger),
		slabs:     map[int]*prioritySlab{},
		byHash:    map[[32]byte]*entry{},
		byNorm:    map[[32]byte][]*entry{},
		streams:   map[uint64]*ExternalStream{},
		rnd:       rand.New(rand.NewSource(cfg.Clock.Now().UnixNano())),
	}
	switch cfg.Clock.(type) {
	case SystemClock, *SystemClock:
		p.cleanupStop = make(chan struct{})
		p.cleanupDone = make(chan struct{})
		go p.cleanupLoop()
	}
	return p
}

// AddExternal pools a message the ingress layer already deserialized and
// validated (the emulation on broadcast receive / liteserver submit leaves
// the caller with the root cell and usually the parsed header — nothing is
// re-parsed here; parsed may be nil, then the header is decoded from root).
// serializedSize must be the positive, exact received BOC length. The pool
// keeps that number for its byte budget and never retains the ingress buffer.
//
// Duplicates are idempotent no-ops resolved by the cached root-cell hash
// before any other work; a repeat from a higher-priority source re-pools
// the message at the new priority without losing it. A message rejected by
// the caps stays out with ErrExternalCapacity and is also observable through
// Stats.
func (p *Pool) AddExternal(
	serializedSize int,
	root *cell.Cell,
	parsed *tlb.ExternalMessage,
	priority int,
) (ExternalAddResult, error) {
	if serializedSize <= 0 {
		return ExternalAddResult{}, fmt.Errorf("%w: %d", ErrInvalidExternalSize, serializedSize)
	}

	hash := root.HashKey()
	now := p.clock.Now()

	// Fast path: duplicates and priority bumps need no message assembly at
	// all — just the cached root hash.
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return ExternalAddResult{}, ErrClosed
	}
	p.expireLocked(now)
	if e := p.byHash[hash]; e != nil && !e.removed {
		p.dedupOrBumpLocked(e, priority, now)
		result := ExternalAddResult{
			Outcome:     ExternalAddExisting,
			Destination: e.msg.destination(),
		}
		p.mu.Unlock()

		return result, nil
	}
	p.mu.Unlock()

	// Fresh message: assemble routing and the normalized hash outside the
	// lock, then insert (re-checking the dedup race). Only the received length
	// is kept — the raw BOC itself is the ingress layer's buffer and nothing
	// downstream reads it back out of the pool.
	msg, err := newExternalMessage(serializedSize, root, parsed)
	if err != nil {
		return ExternalAddResult{}, err
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return ExternalAddResult{}, ErrClosed
	}
	now = p.clock.Now()
	p.expireLocked(now)
	if e := p.byHash[msg.Hash]; e != nil && !e.removed {
		p.dedupOrBumpLocked(e, priority, now)

		return ExternalAddResult{
			Outcome:     ExternalAddExisting,
			Destination: e.msg.destination(),
		}, nil
	}
	if err = p.insertLocked(msg, priority, now); err != nil {
		return ExternalAddResult{}, err
	}

	return ExternalAddResult{
		Outcome:     ExternalAddInserted,
		Destination: msg.destination(),
	}, nil
}

// dedupOrBumpLocked handles a submission whose raw hash is already pooled.
func (p *Pool) dedupOrBumpLocked(e *entry, priority int, now time.Time) {
	p.reactivateDueLocked(e, now)
	if priority <= e.priority {
		p.stats.dedupSkipped++
		return
	}
	// Same message from a higher-priority source: move it up. The target
	// level caps are checked first so a full level keeps the message at
	// its old priority instead of losing it.
	if p.slabHasRoomLocked(priority, e.msg.key()) {
		p.movePriorityLocked(e, priority, now)
		p.stats.priorityBumps++
	}
}

type selectionCandidate struct {
	msg        *ExternalMessage
	generation uint64
	expiresAt  int64
	rank       uint64
}

type selectionLevel struct {
	start int
	end   int
}

// SelectForBlock returns up to limit collatable messages of the shard in
// collation order: higher priority levels first, fair seeded ordering inside
// each level. limit <= 0 means no limit.
//
// Messages stay pooled: report their outcome with Complete and let the
// applied-block cleanup (EraseApplied) remove imported ones. Returned values
// are snapshots; mutating one cannot alter the pool or another snapshot.
func (p *Pool) SelectForBlock(shard ShardIdent, limit int) []ExternalSnapshot {
	bounded := limit > 0
	if limit <= 0 {
		limit = int(^uint(0) >> 1)
	}
	now := p.clock.Now()

	p.mu.Lock()
	p.expireLocked(now)
	candidateCap := p.totalCount
	if bounded {
		candidateCap = 0
		for _, priority := range p.prioDesc {
			candidateCap += len(p.slabs[priority].entries)
			if candidateCap >= limit {
				break
			}
		}
	}
	candidates := make([]selectionCandidate, 0, candidateCap)
	levels := make([]selectionLevel, 0, len(p.prioDesc))
	for _, prio := range p.prioDesc {
		start := len(candidates)
		for _, e := range p.slabs[prio].entries {
			p.reactivateDueLocked(e, now)
			if e.retryAt.IsZero() && shard.Contains(e.msg.Workchain, e.msg.AddrPrefix) {
				candidates = append(candidates, selectionCandidate{
					msg:        e.msg,
					generation: e.generation,
					expiresAt:  e.deleteAt.UnixNano(),
				})
			}
		}
		if len(candidates) > start {
			levels = append(levels, selectionLevel{start: start, end: len(candidates)})
		}
		if bounded && len(candidates) >= limit {
			break
		}
	}
	seed := p.rnd.Uint64()
	p.mu.Unlock()

	out := make([]ExternalSnapshot, 0, min(limit, len(candidates)))
	for _, bounds := range levels {
		if len(out) >= limit {
			break
		}

		level := candidates[bounds.start:bounds.end]
		for i := range level {
			level[i].rank = selectionRank(level[i].msg.Hash, seed)
		}
		take := min(limit-len(out), len(level))
		selectFirst(level, take)
		for i := 0; i < take; i++ {
			out = append(out, level[i].msg.snapshot(level[i].generation, level[i].expiresAt))
		}
	}
	return out
}

func selectionRank(hash [32]byte, seed uint64) uint64 {
	x := binary.LittleEndian.Uint64(hash[:8]) ^ seed
	x += 0x9e3779b97f4a7c15
	x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
	x = (x ^ (x >> 27)) * 0x94d049bb133111eb
	return x ^ (x >> 31)
}

func selectionLess(a, b selectionCandidate) bool {
	if a.rank != b.rank {
		return a.rank < b.rank
	}
	return bytes.Compare(a.msg.Hash[:], b.msg.Hash[:]) < 0
}

func selectionCompare(a, b selectionCandidate) int {
	if a.rank < b.rank {
		return -1
	}
	if a.rank > b.rank {
		return 1
	}
	return bytes.Compare(a.msg.Hash[:], b.msg.Hash[:])
}

// selectFirst orders the best take candidates by seeded rank. For a bounded
// selection it keeps only a max-heap of size take, avoiding an O(N log N)
// sort of messages the collator cannot consume.
func selectFirst(level []selectionCandidate, take int) {
	if take == len(level) {
		slices.SortFunc(level, selectionCompare)
		return
	}

	selected := level[:take]
	for i := len(selected)/2 - 1; i >= 0; i-- {
		siftDownSelectionMax(selected, i)
	}
	for i := take; i < len(level); i++ {
		if selectionLess(level[i], selected[0]) {
			selected[0] = level[i]
			siftDownSelectionMax(selected, 0)
		}
	}
	slices.SortFunc(selected, selectionCompare)
}

func siftDownSelectionMax(values []selectionCandidate, root int) {
	for {
		left := root*2 + 1
		if left >= len(values) {
			return
		}
		largest := left
		right := left + 1
		if right < len(values) && selectionLess(values[largest], values[right]) {
			largest = right
		}
		if !selectionLess(values[root], values[largest]) {
			return
		}
		values[root], values[largest] = values[largest], values[root]
		root = largest
	}
}

// ---- pool state (p.mu held) ----

func (p *Pool) slab(priority int) *prioritySlab {
	s := p.slabs[priority]
	if s == nil {
		s = &prioritySlab{entries: map[[32]byte]*entry{}, byAddr: map[addrKey]int{}}
		p.slabs[priority] = s
		p.prioDesc = append(p.prioDesc, priority)
		for i := len(p.prioDesc) - 1; i > 0 && p.prioDesc[i] > p.prioDesc[i-1]; i-- {
			p.prioDesc[i], p.prioDesc[i-1] = p.prioDesc[i-1], p.prioDesc[i]
		}
	}
	return s
}

// slabHasRoomLocked reports whether one more message of the address fits
// into the priority level.
func (p *Pool) slabHasRoomLocked(priority int, key addrKey) bool {
	slab := p.slabs[priority]
	if slab == nil {
		return p.cfg.MempoolLimit > 0 && p.cfg.PerAddressLimit > 0
	}
	return len(slab.entries) < p.cfg.MempoolLimit && slab.byAddr[key] < p.cfg.PerAddressLimit
}

func (p *Pool) insertLocked(msg *ExternalMessage, priority int, now time.Time) error {
	key := msg.key()
	slab := p.slabs[priority]
	slabCount := 0
	addressCount := 0
	if slab != nil {
		slabCount = len(slab.entries)
		addressCount = slab.byAddr[key]
	}
	if slabCount >= p.cfg.MempoolLimit {
		p.stats.overflowMempool++
		p.log.Debug().Msgf("cannot add message addr=%d:%x prio=%d: mempool is full (limit=%d)",
			msg.Workchain, msg.Addr[:8], priority, p.cfg.MempoolLimit)
		return ErrExternalCapacity
	}
	if p.cfg.MempoolBytesLimit > 0 && p.totalBytes+int64(msg.Size) > p.cfg.MempoolBytesLimit {
		p.stats.overflowBytes++
		p.log.Debug().Msgf("cannot add message addr=%d:%x prio=%d: mempool bytes budget exceeded",
			msg.Workchain, msg.Addr[:8], priority)
		return ErrExternalCapacity
	}
	if addressCount >= p.cfg.PerAddressLimit {
		p.stats.overflowAddress++
		p.log.Debug().Msgf("cannot add message addr=%d:%x prio=%d: per address limit reached (limit=%d)",
			msg.Workchain, msg.Addr[:8], priority, p.cfg.PerAddressLimit)
		return ErrExternalCapacity
	}
	slab = p.slab(priority)

	e := &entry{
		msg:         msg,
		priority:    priority,
		deleteAt:    now.Add(p.cfg.TTL),
		generation:  1,
		expiryIndex: -1,
	}
	msg.poolEntry = e
	slab.entries[msg.Hash] = e
	slab.byAddr[key]++
	p.byHash[msg.Hash] = e
	norm := p.byNorm[msg.HashNorm]
	e.normIndex = len(norm)
	p.byNorm[msg.HashNorm] = append(norm, e)
	heap.Push(&p.expiry, e)
	p.totalCount++
	p.totalBytes += int64(msg.Size)
	p.stats.added++
	p.offerExternalLocked(e)

	return nil
}

func (p *Pool) movePriorityLocked(e *entry, priority int, now time.Time) {
	p.removeFromSlabLocked(e)
	e.priority = priority

	slab := p.slab(priority)
	slab.entries[e.msg.Hash] = e
	slab.byAddr[e.msg.key()]++

	e.deleteAt = now.Add(p.cfg.TTL)
	heap.Fix(&p.expiry, e.expiryIndex)
}

func (p *Pool) removeFromSlabLocked(e *entry) {
	slab := p.slabs[e.priority]
	delete(slab.entries, e.msg.Hash)
	key := e.msg.key()
	if n := slab.byAddr[key]; n <= 1 {
		delete(slab.byAddr, key)
	} else {
		slab.byAddr[key] = n - 1
	}
	if len(slab.entries) != 0 {
		return
	}

	delete(p.slabs, e.priority)
	for i, priority := range p.prioDesc {
		if priority == e.priority {
			copy(p.prioDesc[i:], p.prioDesc[i+1:])
			p.prioDesc = p.prioDesc[:len(p.prioDesc)-1]
			return
		}
	}
}

func (p *Pool) eraseLocked(e *entry) {
	if e.removed {
		return
	}
	e.removed = true
	if e.expiryIndex >= 0 {
		heap.Remove(&p.expiry, e.expiryIndex)
	}
	p.removeFromSlabLocked(e)
	delete(p.byHash, e.msg.Hash)
	norm := p.byNorm[e.msg.HashNorm]
	last := len(norm) - 1
	if e.normIndex != last {
		moved := norm[last]
		norm[e.normIndex] = moved
		moved.normIndex = e.normIndex
	}
	norm[last] = nil
	norm = norm[:last]
	e.normIndex = -1
	if len(norm) == 0 {
		delete(p.byNorm, e.msg.HashNorm)
	} else {
		p.byNorm[e.msg.HashNorm] = norm
	}
	p.totalCount--
	p.totalBytes -= int64(e.msg.Size)
}

// ---- collation feedback and cleanup ----

// Complete applies collation outcomes to the selection generation that
// produced them. Invalid messages are removed, account-rejected messages are
// delayed for a bounded number of retry generations, included messages are
// quarantined, and skipped messages stay ready. Feedback for an older
// generation is ignored.
func (p *Pool) Complete(feedback []ExternalFeedback) error {
	for i, result := range feedback {
		switch result.Outcome {
		case ExternalIncluded, ExternalInvalid, ExternalNotAccepted, ExternalSkippedLimit:
		default:
			return fmt.Errorf("%w at index %d: %d", ErrInvalidExternalOutcome, i, result.Outcome)
		}
	}

	now := p.clock.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireLocked(now)
	for _, result := range feedback {
		e := p.byHash[result.Ref.Hash]
		if e == nil {
			continue
		}
		if e.generation != result.Ref.Generation {
			p.stats.staleFeedback++
			continue
		}

		switch result.Outcome {
		case ExternalIncluded:
			p.quarantineIncludedLocked(e, now)
		case ExternalInvalid:
			p.eraseLocked(e)
			p.stats.invalidDeleted++
		case ExternalNotAccepted:
			if e.retryCount >= p.cfg.AccountRejectRetryLimit {
				p.eraseLocked(e)
				p.stats.rejectedExhausted++
				continue
			}
			softLimit := min(accountRejectRetrySoftLimit, p.cfg.MempoolLimit)
			if softLimit > 0 && len(p.slabs[e.priority].entries) >= softLimit {
				p.eraseLocked(e)
				p.stats.rejectedPressure++
				continue
			}
			e.generation++
			e.retryCount++
			e.retryAt = now.Add(p.cfg.AccountRejectRetryDelay)
			e.retryReason = retryAccountRejected
			p.stats.rejectedDelayed++
		case ExternalSkippedLimit:
			// No TVM attempt happened, so this feedback must not consume the
			// generation shared with another concurrent collation.
		}
	}
	return nil
}

func (p *Pool) quarantineIncludedLocked(included *entry, now time.Time) {
	retryAt := now.Add(p.cfg.IncludedRetryDelay)
	for _, e := range p.byNorm[included.msg.HashNorm] {
		e.generation++
		e.retryAt = retryAt
		e.retryReason = retryIncluded
		p.stats.includedQuarantined++
	}
}

// EraseApplied removes every message whose normalized hash was imported by
// an applied (finalized) block — the InMsgDescr cleanup path.
func (p *Pool) EraseApplied(normHashes [][32]byte) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.stats.appliedReq += uint64(len(normHashes))
	for _, h := range normHashes {
		// eraseLocked swap-removes from the slice, so drain from the tail.
		for norm := p.byNorm[h]; len(norm) > 0; norm = p.byNorm[h] {
			p.eraseLocked(norm[len(norm)-1])
			p.stats.appliedDeleted++
		}
	}
}

func (p *Pool) reactivateDueLocked(e *entry, now time.Time) {
	if e.retryAt.IsZero() || e.retryAt.After(now) {
		return
	}
	e.retryAt = time.Time{}
	switch e.retryReason {
	case retryIncluded:
		p.stats.includedReleased++
	case retryAccountRejected:
		p.stats.rejectedRetried++
	}
	e.retryReason = retryNone
}

// expireLocked drains every due live entry. Indexed removals keep the heap in
// one-to-one correspondence with the pool, so explicit cleanup and priority
// changes cannot leave stale expiry records behind.
func (p *Pool) expireLocked(now time.Time) {
	for len(p.expiry) > 0 && p.expiry[0].expired(now) {
		e := heap.Pop(&p.expiry).(*entry)
		p.eraseLocked(e)
		p.stats.expired++
	}
}

func (p *Pool) cleanupLoop() {
	interval := p.cfg.TTL / 2
	if interval > 250*time.Second {
		interval = 250 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	defer close(p.cleanupDone)
	for {
		select {
		case <-ticker.C:
			now := p.clock.Now()
			p.mu.Lock()
			p.expireLocked(now)
			p.mu.Unlock()
		case <-p.cleanupStop:
			return
		}
	}
}

// Close rejects further submissions and stops background expiry cleanup.
func (p *Pool) Close() {
	p.mu.Lock()
	p.closed = true
	for _, stream := range p.streams {
		stream.closed = true
		stream.signalLocked()
	}
	clear(p.streams)
	stop := p.cleanupStop
	done := p.cleanupDone
	p.mu.Unlock()
	if stop != nil {
		p.cleanupOnce.Do(func() { close(stop) })
		<-done
	}
}

// Stats returns a counter snapshot.
func (p *Pool) Stats() Stats {
	now := p.clock.Now()
	p.mu.Lock()
	defer p.mu.Unlock()
	p.expireLocked(now)
	return Stats{
		DedupSkipped:        p.stats.dedupSkipped,
		PriorityBumps:       p.stats.priorityBumps,
		Added:               p.stats.added,
		OverflowMempool:     p.stats.overflowMempool,
		OverflowBytes:       p.stats.overflowBytes,
		OverflowAddress:     p.stats.overflowAddress,
		Expired:             p.stats.expired,
		InvalidDeleted:      p.stats.invalidDeleted,
		IncludedQuarantined: p.stats.includedQuarantined,
		IncludedReleased:    p.stats.includedReleased,
		RejectedDelayed:     p.stats.rejectedDelayed,
		RejectedRetried:     p.stats.rejectedRetried,
		RejectedExhausted:   p.stats.rejectedExhausted,
		RejectedPressure:    p.stats.rejectedPressure,
		StaleFeedback:       p.stats.staleFeedback,
		AppliedRequested:    p.stats.appliedReq,
		AppliedDeleted:      p.stats.appliedDeleted,
		Pooled:              p.totalCount,
		PooledBytes:         p.totalBytes,
	}
}
