package msgpool

import (
	"context"
	"fmt"
	"slices"
	"time"
)

// ExternalStream is one collation's bounded stream of ingress-admitted
// external messages. Pool.mu owns every mutable field; Next never keeps the
// lock while waiting and no goroutine is created for the stream.
type ExternalStream struct {
	pool     *Pool
	id       uint64
	shard    ShardIdent
	capacity int
	seed     uint64
	follow   bool

	buffer      []ExternalSnapshot
	head        int
	size        int
	seen        map[[32]byte]struct{}
	excludeNorm map[[32]byte]struct{}
	snapshot    []externalSnapshotCandidate
	snapshotAt  int
	dirty       bool
	closed      bool
	wake        chan struct{}
}

type externalSnapshotCandidate struct {
	msg        *ExternalMessage
	entry      *entry
	generation uint64
	expiresAt  int64
	rank       uint64
}

// OpenExternalStream atomically registers a collation and snapshots the
// messages already ready for its shard. capacity is backpressure capacity,
// not a limit on the total number of messages the collation may consume.
func (p *Pool) OpenExternalStream(shard ShardIdent, capacity int) (*ExternalStream, error) {
	return p.OpenExternalStreamExcluding(shard, capacity, nil)
}

// OpenExternalStreamExcluding opens a stream that will never offer the named
// messages or known normalized variants, neither in its first fill nor in
// anything admitted afterwards.
//
// It exists for a collation that starts before its predecessor has reported what
// it consumed. The pool learns that at the predecessor's commit; a successor
// opened ahead of it would be offered those messages again, execute them a
// second time, and have its feedback dropped as stale — which leaves them
// without a retry stamp, so the same messages come back on every remaining slot
// of the window.
//
// The exclusion is seeded before the first fill: seeding after it would let the
// very first batch through, which is the batch a successor takes. Known raw
// hashes also seed a normalized-hash set so equivalent variants admitted later
// stay excluded.
func (p *Pool) OpenExternalStreamExcluding(
	shard ShardIdent,
	capacity int,
	exclude [][32]byte,
) (*ExternalStream, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("msgpool: external stream capacity must be positive")
	}

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	p.expireLocked(p.clock.Now())
	p.nextStream++
	stream := &ExternalStream{
		pool:     p,
		id:       p.nextStream,
		shard:    shard,
		capacity: capacity,
		seed:     p.rnd.Uint64(),
		follow:   true,
		buffer:   make([]ExternalSnapshot, capacity),
		seen:     make(map[[32]byte]struct{}, capacity+len(exclude)),
		wake:     make(chan struct{}, 1),
	}
	if len(exclude) > 0 {
		stream.excludeNorm = make(map[[32]byte]struct{}, len(exclude))
	}
	for _, hash := range exclude {
		stream.seen[hash] = struct{}{}
		entry := p.byHash[hash]
		if entry == nil {
			continue
		}
		stream.excludeNorm[entry.msg.HashNorm] = struct{}{}
		for _, variant := range p.byNorm[entry.msg.HashNorm] {
			stream.seen[variant.msg.Hash] = struct{}{}
		}
	}
	p.streams[stream.id] = stream
	p.refillExternalStreamLocked(stream)

	return stream, nil
}

// OpenExternalSnapshot returns the messages ready at one atomic pool
// observation. It never follows later admissions. Payload preparation stays
// lazy: the snapshot retains immutable message roots and the collator drains
// at most capacity items at a time, matching the reference masterchain queue.
func (p *Pool) OpenExternalSnapshot(shard ShardIdent, capacity int) (*ExternalStream, error) {
	if capacity <= 0 {
		return nil, fmt.Errorf("msgpool: external snapshot capacity must be positive")
	}

	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil, ErrClosed
	}
	now := p.clock.Now()
	p.expireLocked(now)
	seed := p.rnd.Uint64()
	candidates, levels := p.externalSnapshotCandidatesLocked(shard, now)
	p.mu.Unlock()

	stream := &ExternalStream{
		pool:     p,
		shard:    shard,
		capacity: capacity,
		seed:     seed,
		buffer:   make([]ExternalSnapshot, capacity),
		wake:     make(chan struct{}, 1),
	}
	stream.snapshot = orderExternalSnapshotCandidates(candidates, levels, seed)

	p.mu.Lock()
	defer p.mu.Unlock()
	if p.closed {
		return nil, ErrClosed
	}
	p.refillExternalStreamLocked(stream)

	return stream, nil
}

// Next returns a non-empty batch or an error. Context cancellation and the
// caller's deadline are the only normal way to stop waiting; Close unblocks a
// concurrent Next with ErrClosed.
func (s *ExternalStream) Next(ctx context.Context, limit int) ([]ExternalSnapshot, error) {
	if limit <= 0 {
		return nil, fmt.Errorf("msgpool: external stream batch limit must be positive")
	}

	for {
		s.pool.mu.Lock()
		if batch := s.takeReadyLocked(limit); len(batch) > 0 {
			s.pool.mu.Unlock()

			return batch, nil
		}
		if s.closed || s.pool.closed {
			s.pool.mu.Unlock()
			return nil, ErrClosed
		}
		wake := s.wake
		s.pool.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-wake:
		}
	}
}

// TakeReady returns a non-empty batch already buffered for the collation, or
// nil when the stream is currently empty. It never waits and may be called
// repeatedly to drain an initial snapshot before processing generated
// messages, matching the reference collator's try_pop loop.
func (s *ExternalStream) TakeReady(limit int) []ExternalSnapshot {
	if limit <= 0 {
		return nil
	}

	s.pool.mu.Lock()
	defer s.pool.mu.Unlock()
	return s.takeReadyLocked(limit)
}

func (s *ExternalStream) takeReadyLocked(limit int) []ExternalSnapshot {
	batch := make([]ExternalSnapshot, 0, min(limit, s.size))
	now := s.pool.clock.Now()
	for len(batch) < limit {
		if s.size == 0 {
			if s.dirty {
				s.pool.refillExternalStreamLocked(s)
			}
			if s.size == 0 {
				break
			}
		}
		snapshot := s.buffer[s.head]
		s.buffer[s.head] = ExternalSnapshot{}
		s.head = (s.head + 1) % s.capacity
		s.size--
		entry := snapshot.message.poolEntry
		if entry == nil || entry.removed || entry.generation != snapshot.generation ||
			!entry.retryAt.IsZero() || snapshot.expiresAt <= now.UnixNano() {
			continue
		}
		batch = append(batch, snapshot)
	}
	// Topping the buffer back up after a batch is prefetch, not correctness: the
	// loop above refills the moment it runs dry, so a take never has to stall
	// for want of one. Unconditionally it was a full scan of every slab, under
	// the pool mutex, after every batch — with a buffer of thousands and a batch
	// of a few hundred the buffer never emptied, so the scan ran every time and
	// re-ranked a pool the take had barely moved.
	//
	// Half full is where it becomes worth doing again: below that the next few
	// batches could drain it, and the scan is cheaper here than inside a take.
	// The only thing this delays is a message whose retry backoff expired, which
	// reactivateDueLocked notices during the scan; new arrivals do not wait for
	// it, because offerExternalLocked pushes them into every interested stream
	// as they are admitted.
	if s.dirty && s.size*2 <= s.capacity {
		s.pool.refillExternalStreamLocked(s)
	}

	return batch
}

// Close deregisters the stream and unblocks a concurrent Next. It is
// idempotent; snapshots already returned to the caller remain valid.
func (s *ExternalStream) Close() error {
	s.pool.mu.Lock()
	defer s.pool.mu.Unlock()
	if s.closed {
		return nil
	}
	s.closed = true
	delete(s.pool.streams, s.id)
	s.signalLocked()

	return nil
}

func (p *Pool) offerExternalLocked(e *entry) {
	for _, stream := range p.streams {
		if !stream.shard.Contains(e.msg.Workchain, e.msg.AddrPrefix) {
			continue
		}
		stream.offerLocked(e.msg.snapshot(e.generation, e.deleteAt.UnixNano()))
	}
}

func (s *ExternalStream) offerLocked(snapshot ExternalSnapshot) {
	if s.closed {
		return
	}
	if _, excluded := s.excludeNorm[snapshot.message.HashNorm]; excluded {
		return
	}
	if _, exists := s.seen[snapshot.Hash]; exists {
		return
	}
	if s.size == s.capacity {
		s.dirty = true
		return
	}
	s.enqueueLocked(snapshot)
	s.seen[snapshot.Hash] = struct{}{}
}

func (s *ExternalStream) enqueueLocked(snapshot ExternalSnapshot) {
	tail := (s.head + s.size) % s.capacity
	s.buffer[tail] = snapshot
	s.size++
	s.signalLocked()
}

func (s *ExternalStream) signalLocked() {
	select {
	case s.wake <- struct{}{}:
	default:
	}
}

func (p *Pool) refillExternalStreamLocked(stream *ExternalStream) {
	stream.dirty = false
	free := stream.capacity - stream.size
	if free == 0 || stream.closed {
		return
	}
	snapshotNow := p.clock.Now()
	for free > 0 && stream.snapshotAt < len(stream.snapshot) {
		candidate := stream.snapshot[stream.snapshotAt]
		stream.snapshot[stream.snapshotAt] = externalSnapshotCandidate{}
		stream.snapshotAt++
		if candidate.entry.removed || candidate.entry.generation != candidate.generation ||
			!candidate.entry.retryAt.IsZero() || candidate.entry.expired(snapshotNow) {
			continue
		}
		stream.enqueueLocked(candidate.msg.snapshot(candidate.generation, candidate.entry.deleteAt.UnixNano()))
		free--
	}
	if stream.snapshotAt < len(stream.snapshot) {
		stream.dirty = true
	} else {
		stream.snapshot = nil
	}
	if !stream.follow {
		if stream.snapshot == nil {
			stream.closed = true
		}
		return
	}
	if free == 0 {
		return
	}

	// Sized for what the loop below appends, which is every eligible entry in
	// the pool rather than what the stream has room for: free*2 described the
	// take, not the scan, so a full pool grew this slice from a few hundred to
	// thousands one doubling at a time.
	candidates := make([]selectionCandidate, 0, p.totalCount)
	levels := make([]selectionLevel, 0, len(p.prioDesc))
	now := p.clock.Now()
	p.expireLocked(now)
	for _, priority := range p.prioDesc {
		start := len(candidates)
		for _, e := range p.slabs[priority].entries {
			p.reactivateDueLocked(e, now)
			if !e.retryAt.IsZero() || !stream.shard.Contains(e.msg.Workchain, e.msg.AddrPrefix) {
				continue
			}
			if _, excluded := stream.excludeNorm[e.msg.HashNorm]; excluded {
				continue
			}
			if _, exists := stream.seen[e.msg.Hash]; exists {
				continue
			}
			candidates = append(candidates, selectionCandidate{
				msg:        e.msg,
				generation: e.generation,
				expiresAt:  e.deleteAt.UnixNano(),
			})
		}
		if len(candidates) > start {
			levels = append(levels, selectionLevel{start: start, end: len(candidates)})
		}
	}

	remaining := free
	for _, bounds := range levels {
		if remaining == 0 {
			stream.dirty = true
			break
		}
		level := candidates[bounds.start:bounds.end]
		for i := range level {
			level[i].rank = selectionRank(level[i].msg.Hash, stream.seed)
		}
		take := min(remaining, len(level))
		selectFirst(level, take)
		for i := 0; i < take; i++ {
			stream.offerLocked(level[i].msg.snapshot(level[i].generation, level[i].expiresAt))
		}
		remaining -= take
		if take < len(level) {
			stream.dirty = true
			break
		}
	}
}

func (p *Pool) externalSnapshotCandidatesLocked(
	shard ShardIdent,
	now time.Time,
) ([]externalSnapshotCandidate, []selectionLevel) {
	candidates := make([]externalSnapshotCandidate, 0, p.totalCount)
	levels := make([]selectionLevel, 0, len(p.prioDesc))
	for _, priority := range p.prioDesc {
		start := len(candidates)
		for _, e := range p.slabs[priority].entries {
			p.reactivateDueLocked(e, now)
			if !e.retryAt.IsZero() || !shard.Contains(e.msg.Workchain, e.msg.AddrPrefix) {
				continue
			}
			candidates = append(candidates, externalSnapshotCandidate{
				msg:        e.msg,
				entry:      e,
				generation: e.generation,
				expiresAt:  e.deleteAt.UnixNano(),
			})
		}
		if len(candidates) > start {
			levels = append(levels, selectionLevel{start: start, end: len(candidates)})
		}
	}

	return candidates, levels
}

func orderExternalSnapshotCandidates(
	candidates []externalSnapshotCandidate,
	levels []selectionLevel,
	seed uint64,
) []externalSnapshotCandidate {
	for _, bounds := range levels {
		level := candidates[bounds.start:bounds.end]
		for i := range level {
			level[i].rank = selectionRank(level[i].msg.Hash, seed)
		}
		slices.SortFunc(level, func(a, b externalSnapshotCandidate) int {
			return selectionCompare(
				selectionCandidate{msg: a.msg, rank: a.rank},
				selectionCandidate{msg: b.msg, rank: b.rank},
			)
		})
	}

	return candidates
}
