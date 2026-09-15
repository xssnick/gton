package msgpool

import (
	"fmt"
	"math/rand"
	"testing"
	"time"
)

// legacyRefillExternalStreamLocked is refillExternalStreamLocked as it was when
// every refill allocated a candidate slice sized for the whole pool and selected
// only after scanning every level. It stays as the reference the current refill
// must match message for message.
func legacyRefillExternalStreamLocked(p *Pool, stream *ExternalStream) {
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

// refillTwin is one of two pools driven through the same operations: one
// refills with the current code, the other with the legacy reference. Both are
// built at the same fake instant, so their seeded random sources, and with them
// the stream seeds, agree.
type refillTwin struct {
	pool    *Pool
	clock   *fakeClock
	streams []*ExternalStream
}

func newRefillTwin() *refillTwin {
	clock := newFakeClock()

	return &refillTwin{
		pool: New(Config{
			Clock:                   clock,
			MempoolLimit:            1 << 30,
			PerAddressLimit:         1 << 30,
			MempoolBytesLimit:       1 << 40,
			TTL:                     time.Minute,
			IncludedRetryDelay:      3 * time.Second,
			AccountRejectRetryDelay: time.Second,
		}),
		clock: clock,
	}
}

type refillEquivalenceCoverage struct {
	refills    int
	dirty      int
	multiLevel int
	partial    int
}

// The refill must offer the same messages in the same order, reactivate the
// same retries and reach the same dirty verdict as before, on pools that
// exercise all of it: several priority levels, streams narrower than the pool
// and of other workchains, exclusions by raw and normalized hash, retries that
// fall due during the scan, expiry, and follow admission between refills.
func TestExternalStreamRefillMatchesLegacy(t *testing.T) {
	var coverage refillEquivalenceCoverage
	for seed := range int64(48) {
		t.Run(fmt.Sprintf("seed=%d", seed), func(t *testing.T) {
			runRefillEquivalence(t, rand.New(rand.NewSource(seed)), &coverage)
		})
	}

	// A fixture that never overflows a stream or never spans two levels would
	// pass against any selection; make sure the branches were really reached.
	if coverage.refills == 0 || coverage.dirty == 0 || coverage.multiLevel == 0 || coverage.partial == 0 {
		t.Fatalf("the fixture does not exercise the refill: %+v", coverage)
	}
	t.Logf("refill coverage: %+v", coverage)
}

func runRefillEquivalence(t *testing.T, rng *rand.Rand, coverage *refillEquivalenceCoverage) {
	current := newRefillTwin()
	legacy := newRefillTwin()
	defer current.pool.Close()
	defer legacy.pool.Close()

	shards := []ShardIdent{
		allShard,
		{Workchain: 0, Shard: 0x4000000000000000},
		{Workchain: 0, Shard: 0xa000000000000000},
		{Workchain: -1, Shard: ShardAll},
	}
	type origin struct {
		workchain int32
		addr      [32]byte
		tag       uint64
	}
	var origins []origin
	var hashes [][32]byte

	openStream := func() {
		shard := shards[rng.Intn(len(shards))]
		capacity := 1 + rng.Intn(16)
		var exclude [][32]byte
		for range rng.Intn(3) {
			if len(hashes) > 0 {
				exclude = append(exclude, hashes[rng.Intn(len(hashes))])
			}
		}
		for _, twin := range []*refillTwin{current, legacy} {
			stream, err := twin.pool.OpenExternalStreamExcluding(shard, capacity, exclude)
			if err != nil {
				t.Fatal(err)
			}
			twin.streams = append(twin.streams, stream)
		}
	}
	for range 3 {
		openStream()
	}

	for step := range 600 {
		switch op := rng.Intn(100); {
		case op < 35:
			var msg testMsg
			switch {
			case len(origins) > 0 && rng.Intn(4) == 0:
				// A normalized variant, or with no fee change the same raw message
				// again at a possibly higher priority.
				base := origins[rng.Intn(len(origins))]
				msg = buildExtMsg(t, base.workchain, base.addr, bodyWithTag(base.tag), msgOpts{importFee: uint64(rng.Intn(3))})
			default:
				base := origin{tag: uint64(len(origins))}
				if rng.Intn(10) == 0 {
					base.workchain = -1
				}
				for i := range base.addr {
					base.addr[i] = byte(rng.Intn(256))
				}
				origins = append(origins, base)
				msg = buildExtMsg(t, base.workchain, base.addr, bodyWithTag(base.tag), msgOpts{})
			}
			hashes = append(hashes, msg.root.HashKey())
			priority := rng.Intn(3)
			currentResult, currentErr := current.pool.AddExternal(len(msg.raw), msg.root, nil, priority)
			legacyResult, legacyErr := legacy.pool.AddExternal(len(msg.raw), msg.root, nil, priority)
			if currentResult != legacyResult || fmt.Sprint(currentErr) != fmt.Sprint(legacyErr) {
				t.Fatalf("step %d: add diverged: %+v %v against %+v %v",
					step, currentResult, currentErr, legacyResult, legacyErr)
			}
		case op < 48:
			if len(hashes) == 0 {
				continue
			}
			hash := hashes[rng.Intn(len(hashes))]
			e := current.pool.byHash[hash]
			if e == nil {
				continue
			}
			feedback := []ExternalFeedback{{
				Ref:     ExternalRef{Hash: hash, Generation: e.generation - uint64(rng.Intn(2))},
				Outcome: []ExternalOutcome{ExternalIncluded, ExternalInvalid, ExternalNotAccepted, ExternalSkippedLimit}[rng.Intn(4)],
			}}
			if err := current.pool.Complete(feedback); err != nil {
				t.Fatal(err)
			}
			if err := legacy.pool.Complete(feedback); err != nil {
				t.Fatal(err)
			}
		case op < 58:
			advance := time.Duration(rng.Intn(3000)) * time.Millisecond
			current.clock.advance(advance)
			legacy.clock.advance(advance)
		case op < 61:
			if len(hashes) == 0 {
				continue
			}
			e := current.pool.byHash[hashes[rng.Intn(len(hashes))]]
			if e == nil {
				continue
			}
			norm := [][32]byte{e.msg.HashNorm}
			current.pool.EraseApplied(norm)
			legacy.pool.EraseApplied(norm)
		case op < 80:
			index := rng.Intn(len(current.streams))
			count := 1 + rng.Intn(16)
			for _, twin := range []*refillTwin{current, legacy} {
				twin.pool.mu.Lock()
				stream := twin.streams[index]
				for range min(count, stream.size) {
					stream.buffer[stream.head] = ExternalSnapshot{}
					stream.head = (stream.head + 1) % stream.capacity
					stream.size--
				}
				twin.pool.mu.Unlock()
			}
		case op < 97:
			index := rng.Intn(len(current.streams))
			stream := current.streams[index]
			sizeBefore := stream.size

			current.pool.mu.Lock()
			current.pool.refillExternalStreamLocked(stream)
			current.pool.mu.Unlock()
			legacy.pool.mu.Lock()
			legacyRefillExternalStreamLocked(legacy.pool, legacy.streams[index])
			legacy.pool.mu.Unlock()

			requireSameRefillState(t, step, current, legacy)
			coverage.refills++
			if stream.dirty {
				coverage.dirty++
			}
			priorities := map[int]struct{}{}
			for i := sizeBefore; i < stream.size; i++ {
				offered := stream.buffer[(stream.head+i)%stream.capacity]
				priorities[current.pool.byHash[offered.Hash].priority] = struct{}{}
			}
			if len(priorities) > 1 {
				coverage.multiLevel++
			}
			if stream.dirty && stream.size == stream.capacity && len(priorities) > 0 {
				coverage.partial++
			}
		case op < 99:
			openStream()
		default:
			index := rng.Intn(len(current.streams))
			_ = current.streams[index].Close()
			_ = legacy.streams[index].Close()
		}
	}
	requireSameRefillState(t, -1, current, legacy)
}

func requireSameRefillState(t *testing.T, step int, current, legacy *refillTwin) {
	t.Helper()
	if current.pool.stats != legacy.pool.stats {
		t.Fatalf("step %d: pool stats diverged: %+v against %+v", step, current.pool.stats, legacy.pool.stats)
	}
	requireEqual(t, len(current.pool.byHash), len(legacy.pool.byHash), fmt.Sprintf("step %d: pooled messages", step))
	for hash, e := range current.pool.byHash {
		other := legacy.pool.byHash[hash]
		if other == nil {
			t.Fatalf("step %d: message %x pooled only by the current refill", step, hash[:6])
		}
		if e.priority != other.priority || e.generation != other.generation ||
			e.retryReason != other.retryReason || !e.retryAt.Equal(other.retryAt) || !e.deleteAt.Equal(other.deleteAt) {
			t.Fatalf("step %d: entry %x diverged: %+v against %+v", step, hash[:6], *e, *other)
		}
	}

	for index, stream := range current.streams {
		other := legacy.streams[index]
		if stream.head != other.head || stream.size != other.size ||
			stream.dirty != other.dirty || stream.closed != other.closed {
			t.Fatalf("step %d: stream %d diverged: head %d size %d dirty %v closed %v against head %d size %d dirty %v closed %v",
				step, index, stream.head, stream.size, stream.dirty, stream.closed,
				other.head, other.size, other.dirty, other.closed)
		}
		for slot := range stream.buffer {
			got, want := stream.buffer[slot], other.buffer[slot]
			if got.Hash != want.Hash || got.generation != want.generation || got.expiresAt != want.expiresAt {
				t.Fatalf("step %d: stream %d slot %d holds %x/%d, legacy %x/%d",
					step, index, slot, got.Hash[:6], got.generation, want.Hash[:6], want.generation)
			}
		}
		requireEqual(t, len(stream.seen), len(other.seen), fmt.Sprintf("step %d: stream %d seen", step, index))
		for hash := range stream.seen {
			if _, exists := other.seen[hash]; !exists {
				t.Fatalf("step %d: stream %d has seen %x, legacy has not", step, index, hash[:6])
			}
		}
	}
}
