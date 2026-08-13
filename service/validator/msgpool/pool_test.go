package msgpool

import (
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

var allShard = ShardIdent{Workchain: 0, Shard: ShardAll}

func TestAddAndSelect(t *testing.T) {
	env := newPoolEnv(t)
	msg := env.mustAdd(buildExtMsg(t, 0, testAddr(0x01), bodyWithTag(1), msgOpts{}), 0)

	st := env.pool.Stats()
	requireEqual(t, st.Added, uint64(1), "added")
	requireEqual(t, st.Pooled, 1, "pooled")

	got := env.pool.SelectForBlock(allShard, 0)
	requireEqual(t, len(got), 1, "selected")
	requireEqual(t, got[0].Hash, msg.Hash, "message")
	parsed, err := got[0].Parsed()
	if err != nil {
		t.Fatal(err)
	}
	if got[0].Root() == nil || parsed.DstAddr == nil {
		t.Fatal("selected message must carry root and parsed views")
	}
}

func TestAddRejectsGarbage(t *testing.T) {
	env := newPoolEnv(t)
	// A non ext-in cell must be rejected on assembly.
	intMsg := cell.BeginCell().MustStoreUInt(0, 4).EndCell()
	if err := env.pool.AddExternal(intMsg.ToBOC(), intMsg, nil, 0); err == nil {
		t.Fatal("non ext-in root must fail")
	}
	requireEqual(t, env.pool.Stats().Pooled, 0, "nothing pooled")
}

func TestDedupIdempotent(t *testing.T) {
	env := newPoolEnv(t)
	raw := buildExtMsg(t, 0, testAddr(0x02), bodyWithTag(1), msgOpts{})

	env.mustAdd(raw, 0)
	if err := env.pool.AddExternal(raw.raw, raw.root, nil, 0); err != nil {
		t.Fatalf("duplicate add must be a no-op, got %v", err)
	}

	st := env.pool.Stats()
	requireEqual(t, st.DedupSkipped, uint64(1), "dedup counter")
	requireEqual(t, st.Pooled, 1, "still one pooled")
}

func TestPriorityBump(t *testing.T) {
	env := newPoolEnv(t)
	raw := buildExtMsg(t, 0, testAddr(0x03), bodyWithTag(1), msgOpts{})

	msg := env.mustAdd(raw, 0)
	if err := env.pool.AddExternal(raw.raw, raw.root, nil, 3); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, env.pool.Stats().PriorityBumps, uint64(1), "bump counter")

	// Re-adding at a lower priority afterwards is a plain duplicate.
	if err := env.pool.AddExternal(raw.raw, raw.root, nil, 1); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, env.pool.Stats().PriorityBumps, uint64(1), "no second bump")

	got := env.pool.SelectForBlock(allShard, 0)
	requireEqual(t, len(got), 1, "one message")
	requireEqual(t, got[0].Hash, msg.Hash, "same message")
	requireEqual(t, env.pool.Stats().Pooled, 1, "no duplicates pooled")
}

func TestPriorityBumpKeepsMessageWhenTargetFull(t *testing.T) {
	env := newPoolEnv(t, func(c *Config) { c.MempoolLimit = 1 })
	rawA := buildExtMsg(t, 0, testAddr(0x04), bodyWithTag(1), msgOpts{})
	rawB := buildExtMsg(t, 0, testAddr(0x05), bodyWithTag(2), msgOpts{})

	env.mustAdd(rawA, 2) // fills level 2
	env.mustAdd(rawB, 0)
	// Bumping B into the full level 2 must keep it pooled at level 0.
	if err := env.pool.AddExternal(rawB.raw, rawB.root, nil, 2); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, env.pool.Stats().PriorityBumps, uint64(0), "bump refused")
	requireEqual(t, env.pool.Stats().Pooled, 2, "nothing lost")
	requireEqual(t, len(env.pool.SelectForBlock(allShard, 0)), 2, "both served")
}

func TestCaps(t *testing.T) {
	env := newPoolEnv(t, func(c *Config) {
		c.PerAddressLimit = 2
		c.MempoolLimit = 3
	})
	addr := testAddr(0x06)

	env.mustAdd(buildExtMsg(t, 0, addr, bodyWithTag(1), msgOpts{}), 0)
	env.mustAdd(buildExtMsg(t, 0, addr, bodyWithTag(2), msgOpts{}), 0)
	// Third to the same address is refused.
	env.mustAdd(buildExtMsg(t, 0, addr, bodyWithTag(3), msgOpts{}), 0)
	st := env.pool.Stats()
	requireEqual(t, st.OverflowAddress, uint64(1), "per-address overflow")
	requireEqual(t, st.Pooled, 2, "per-address cap held")

	// Global cap.
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x07), bodyWithTag(4), msgOpts{}), 0)
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x08), bodyWithTag(5), msgOpts{}), 0)
	st = env.pool.Stats()
	requireEqual(t, st.OverflowMempool, uint64(1), "mempool overflow")
	requireEqual(t, st.Pooled, 3, "mempool cap held")
}

func TestBytesBudget(t *testing.T) {
	env := newPoolEnv(t, func(c *Config) { c.MempoolBytesLimit = 150 })
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x09), bodyWithTag(1), msgOpts{}), 0)
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x0a), bodyWithTag(2), msgOpts{}), 0)
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x0b), bodyWithTag(3), msgOpts{}), 0)
	st := env.pool.Stats()
	if st.OverflowBytes == 0 {
		t.Fatal("bytes budget must trip")
	}
	if st.PooledBytes > 150 {
		t.Fatalf("pooled bytes %d exceed the budget", st.PooledBytes)
	}
}

func TestSelectShardFilteringAndPriorityOrder(t *testing.T) {
	env := newPoolEnv(t)
	// Basechain, prefix top bit 0 (left shard) at priority 0.
	lo := env.mustAdd(buildExtMsg(t, 0, testAddr(0x11), bodyWithTag(1), msgOpts{}), 0)
	// Basechain, prefix top bit 1 (right shard) at priority 2.
	hi := env.mustAdd(buildExtMsg(t, 0, testAddr(0x91), bodyWithTag(2), msgOpts{}), 2)
	// Masterchain.
	mc := env.mustAdd(buildExtMsg(t, -1, testAddr(0x22), bodyWithTag(3), msgOpts{}), 0)

	all := env.pool.SelectForBlock(allShard, 0)
	requireEqual(t, len(all), 2, "basechain selection size")
	requireEqual(t, all[0].Hash, hi.Hash, "higher priority first")
	requireEqual(t, all[1].Hash, lo.Hash, "lower priority second")

	left := env.pool.SelectForBlock(ShardIdent{Workchain: 0, Shard: 0x4000000000000000}, 0)
	requireEqual(t, len(left), 1, "left shard size")
	requireEqual(t, left[0].Hash, lo.Hash, "left shard message")

	right := env.pool.SelectForBlock(ShardIdent{Workchain: 0, Shard: 0xC000000000000000}, 0)
	requireEqual(t, len(right), 1, "right shard size")
	requireEqual(t, right[0].Hash, hi.Hash, "right shard message")

	mcGot := env.pool.SelectForBlock(ShardIdent{Workchain: -1, Shard: ShardAll}, 0)
	requireEqual(t, len(mcGot), 1, "masterchain size")
	requireEqual(t, mcGot[0].Hash, mc.Hash, "masterchain message")

	// The limit caps the batch and keeps the priority order.
	limited := env.pool.SelectForBlock(allShard, 1)
	requireEqual(t, len(limited), 1, "limit respected")
	requireEqual(t, limited[0].Hash, hi.Hash, "highest priority within limit")
}

func TestSelectDeterministicWithClock(t *testing.T) {
	order := func(seed int64) [][32]byte {
		env := newPoolEnv(t, func(c *Config) { c.Clock = newFakeClockAt(seed) })
		for i := 0; i < 16; i++ {
			env.mustAdd(buildExtMsg(t, 0, testAddr(byte(i+1)), bodyWithTag(uint64(i)), msgOpts{}), 0)
		}
		var out [][32]byte
		for _, m := range env.pool.SelectForBlock(allShard, 0) {
			out = append(out, m.Hash)
		}
		return out
	}
	a1, a2, b := order(7), order(7), order(8)
	requireEqual(t, len(a1), 16, "size")
	for i := range a1 {
		if a1[i] != a2[i] {
			t.Fatal("same seed must give the same order")
		}
	}
	same := true
	for i := range a1 {
		if a1[i] != b[i] {
			same = false
			break
		}
	}
	if same {
		t.Fatal("different seeds should shuffle differently")
	}
}

func TestLimitedSelectionMatchesFullPrefix(t *testing.T) {
	buildPool := func() *Pool {
		env := newPoolEnv(t, func(c *Config) { c.Clock = newFakeClockAt(77) })
		for i := 0; i < 128; i++ {
			env.mustAdd(buildExtMsg(t, 0, testAddr(byte(i+1)), bodyWithTag(uint64(i)), msgOpts{}), 0)
		}
		return env.pool
	}

	full := buildPool().SelectForBlock(allShard, 0)
	limited := buildPool().SelectForBlock(allShard, 17)
	requireEqual(t, len(limited), 17, "limited size")
	for i := range limited {
		if limited[i].Hash != full[i].Hash {
			t.Fatalf("limited selection differs from full prefix at %d", i)
		}
	}
}

func TestCompleteDeletes(t *testing.T) {
	env := newPoolEnv(t)
	addr := testAddr(0x0c)
	msgA := env.mustAdd(buildExtMsg(t, 0, addr, bodyWithTag(1), msgOpts{}), 0)
	msgB := env.mustAdd(buildExtMsg(t, 0, addr, bodyWithTag(2), msgOpts{}), 0)

	selected := env.pool.SelectForBlock(allShard, 0)
	var refB ExternalRef
	for _, msg := range selected {
		if msg.Hash == msgB.Hash {
			refB = msg.Reference()
		}
	}
	if refB == (ExternalRef{}) {
		t.Fatal("message B was not selected")
	}

	// B is ruled out permanently, A stays pooled and selectable.
	if err := env.pool.Complete([]ExternalFeedback{{Ref: refB, Outcome: ExternalInvalid}}); err != nil {
		t.Fatal(err)
	}
	st := env.pool.Stats()
	requireEqual(t, st.InvalidDeleted, uint64(1), "deleted")
	requireEqual(t, st.Pooled, 1, "one message left")
	got := env.pool.SelectForBlock(allShard, 0)
	requireEqual(t, len(got), 1, "survivor selectable")
	requireEqual(t, got[0].Hash, msgA.Hash, "survivor identity")

	// Feedback for an already removed message is an idempotent no-op.
	if err := env.pool.Complete([]ExternalFeedback{{Ref: refB, Outcome: ExternalInvalid}}); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, env.pool.Stats().InvalidDeleted, uint64(1), "idempotent")
}

func TestCompleteRejectsUnknownOutcomeWithoutMutation(t *testing.T) {
	env := newPoolEnv(t)
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x5a), bodyWithTag(1), msgOpts{}), 0)
	selected := env.pool.SelectForBlock(allShard, 1)

	err := env.pool.Complete([]ExternalFeedback{{
		Ref:     selected[0].Reference(),
		Outcome: ExternalOutcome(255),
	}})
	if !errors.Is(err, ErrInvalidExternalOutcome) {
		t.Fatalf("unknown feedback outcome: %v", err)
	}
	requireEqual(t, env.pool.Stats().Pooled, 1, "invalid batch must not mutate the pool")
}

func TestAccountRejectedDelayedRetriedAndBounded(t *testing.T) {
	env := newPoolEnv(t, func(c *Config) {
		c.AccountRejectRetryDelay = 10 * time.Second
		c.AccountRejectRetryLimit = 1
	})
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x5b), bodyWithTag(1), msgOpts{}), 0)

	first := env.pool.SelectForBlock(allShard, 1)[0]
	if err := env.pool.Complete([]ExternalFeedback{{
		Ref:     first.Reference(),
		Outcome: ExternalNotAccepted,
	}}); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, len(env.pool.SelectForBlock(allShard, 1)), 0, "rejected message delayed")
	requireEqual(t, env.pool.Stats().RejectedDelayed, uint64(1), "delay counted")

	env.clock.advance(10 * time.Second)
	retry := env.pool.SelectForBlock(allShard, 1)
	requireEqual(t, len(retry), 1, "message reactivated")
	if retry[0].Reference().Generation == first.Reference().Generation {
		t.Fatal("retry must use a new generation")
	}

	// A late permanent result from the earlier collation cannot delete the
	// reactivated generation.
	if err := env.pool.Complete([]ExternalFeedback{{
		Ref:     first.Reference(),
		Outcome: ExternalInvalid,
	}}); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, env.pool.Stats().Pooled, 1, "stale feedback ignored")
	requireEqual(t, env.pool.Stats().StaleFeedback, uint64(1), "stale feedback counted")

	// The configured retry generation was already consumed, so a second
	// account rejection removes the message instead of looping forever.
	if err := env.pool.Complete([]ExternalFeedback{{
		Ref:     retry[0].Reference(),
		Outcome: ExternalNotAccepted,
	}}); err != nil {
		t.Fatal(err)
	}
	st := env.pool.Stats()
	requireEqual(t, st.Pooled, 0, "retry budget exhausted")
	requireEqual(t, st.RejectedRetried, uint64(1), "reactivation counted")
	requireEqual(t, st.RejectedExhausted, uint64(1), "exhaustion counted")
}

func TestIncludedAndSkippedFeedbackStayPooled(t *testing.T) {
	env := newPoolEnv(t)
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x5c), bodyWithTag(1), msgOpts{}), 0)

	selected := env.pool.SelectForBlock(allShard, 1)[0]
	if err := env.pool.Complete([]ExternalFeedback{{
		Ref:     selected.Reference(),
		Outcome: ExternalIncluded,
	}}); err != nil {
		t.Fatal(err)
	}
	next := env.pool.SelectForBlock(allShard, 1)[0]
	if next.Reference().Generation == selected.Reference().Generation {
		t.Fatal("completed generation must advance")
	}
	if err := env.pool.Complete([]ExternalFeedback{{
		Ref:     next.Reference(),
		Outcome: ExternalSkippedLimit,
	}}); err != nil {
		t.Fatal(err)
	}
	afterSkip := env.pool.SelectForBlock(allShard, 1)[0]
	if afterSkip.Reference().Generation != next.Reference().Generation {
		t.Fatal("limit-skipped feedback must not consume a selection generation")
	}
	requireEqual(t, env.pool.Stats().Pooled, 1, "retryable outcomes stay pooled")
}

func TestSkippedFeedbackCannotHideConcurrentExecutionResult(t *testing.T) {
	env := newPoolEnv(t)
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x5d), bodyWithTag(1), msgOpts{}), 0)
	ref := env.pool.SelectForBlock(allShard, 1)[0].Reference()

	if err := env.pool.Complete([]ExternalFeedback{{Ref: ref, Outcome: ExternalSkippedLimit}}); err != nil {
		t.Fatal(err)
	}
	if err := env.pool.Complete([]ExternalFeedback{{Ref: ref, Outcome: ExternalInvalid}}); err != nil {
		t.Fatal(err)
	}
	if stats := env.pool.Stats(); stats.Pooled != 0 || stats.InvalidDeleted != 1 || stats.StaleFeedback != 0 {
		t.Fatalf("execution result after skipped feedback was lost: %+v", stats)
	}
}

func TestIncludedFeedbackDoesNotResetAccountRejectBudget(t *testing.T) {
	env := newPoolEnv(t, func(c *Config) {
		c.AccountRejectRetryDelay = time.Second
		c.AccountRejectRetryLimit = 1
	})
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x5f), bodyWithTag(1), msgOpts{}), 0)

	first := env.pool.SelectForBlock(allShard, 1)[0]
	if err := env.pool.Complete([]ExternalFeedback{{Ref: first.Reference(), Outcome: ExternalNotAccepted}}); err != nil {
		t.Fatal(err)
	}
	env.clock.advance(time.Second)
	retry := env.pool.SelectForBlock(allShard, 1)[0]
	if err := env.pool.Complete([]ExternalFeedback{{Ref: retry.Reference(), Outcome: ExternalIncluded}}); err != nil {
		t.Fatal(err)
	}
	afterIncluded := env.pool.SelectForBlock(allShard, 1)[0]
	if err := env.pool.Complete([]ExternalFeedback{{Ref: afterIncluded.Reference(), Outcome: ExternalNotAccepted}}); err != nil {
		t.Fatal(err)
	}
	if stats := env.pool.Stats(); stats.Pooled != 0 || stats.RejectedExhausted != 1 {
		t.Fatalf("included feedback reset the lifetime reject budget: %+v", stats)
	}
}

func TestAccountRejectedMessageIsErasedUnderSlabPressure(t *testing.T) {
	env := newPoolEnv(t, func(c *Config) {
		c.MempoolLimit = 2
		c.PerAddressLimit = 2
	})
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x60), bodyWithTag(1), msgOpts{}), 0)
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x61), bodyWithTag(2), msgOpts{}), 0)
	selected := env.pool.SelectForBlock(allShard, 1)[0]

	if err := env.pool.Complete([]ExternalFeedback{{Ref: selected.Reference(), Outcome: ExternalNotAccepted}}); err != nil {
		t.Fatal(err)
	}
	if stats := env.pool.Stats(); stats.Pooled != 1 || stats.RejectedPressure != 1 || stats.RejectedDelayed != 0 {
		t.Fatalf("rejected message retained under slab pressure: %+v", stats)
	}
}

func TestConcurrentFeedbackConsumesGenerationOnce(t *testing.T) {
	env := newPoolEnv(t, func(c *Config) {
		c.AccountRejectRetryDelay = time.Minute
	})
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x5e), bodyWithTag(1), msgOpts{}), 0)
	ref := env.pool.SelectForBlock(allShard, 1)[0].Reference()

	const workers = 32
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if err := env.pool.Complete([]ExternalFeedback{{
				Ref:     ref,
				Outcome: ExternalNotAccepted,
			}}); err != nil {
				t.Errorf("complete: %v", err)
			}
		}()
	}
	wg.Wait()

	st := env.pool.Stats()
	requireEqual(t, st.RejectedDelayed, uint64(1), "one feedback consumes the generation")
	requireEqual(t, st.StaleFeedback, uint64(workers-1), "remaining feedback is stale")
	requireEqual(t, st.Pooled, 1, "message remains delayed")
}

func TestEraseAppliedByNormalizedHash(t *testing.T) {
	env := newPoolEnv(t)
	addr := testAddr(0x0d)
	body := bodyWithTag(1)
	// Two raw variants of the same logical message: different import fees.
	msgA := env.mustAdd(buildExtMsg(t, 0, addr, body, msgOpts{importFee: 0}), 0)
	msgB := env.mustAdd(buildExtMsg(t, 0, addr, body, msgOpts{importFee: 777}), 0)
	if msgA.Hash == msgB.Hash {
		t.Fatal("raw hashes must differ")
	}
	requireEqual(t, msgA.HashNorm, msgB.HashNorm, "normalized hashes match")
	requireEqual(t, env.pool.Stats().Pooled, 2, "both pooled")

	env.pool.EraseApplied([][32]byte{msgA.HashNorm})
	st := env.pool.Stats()
	requireEqual(t, st.Pooled, 0, "both variants erased")
	requireEqual(t, st.AppliedRequested, uint64(1), "applied requested")
	requireEqual(t, st.AppliedDeleted, uint64(2), "applied deleted")
	requireEqual(t, len(env.pool.expiry), 0, "expiry records removed")
}

func TestEraseAppliedManyNormalizedVariants(t *testing.T) {
	env := newPoolEnv(t)
	addr := testAddr(0x5d)
	body := bodyWithTag(1)

	const variants = 256
	var normalized [32]byte
	for i := 0; i < variants; i++ {
		msg := env.mustAdd(buildExtMsg(t, 0, addr, body, msgOpts{importFee: uint64(i)}), 0)
		if i == 0 {
			normalized = msg.HashNorm
		} else {
			requireEqual(t, msg.HashNorm, normalized, "normalized variant")
		}
	}

	env.pool.EraseApplied([][32]byte{normalized})
	st := env.pool.Stats()
	requireEqual(t, st.Pooled, 0, "all normalized variants removed")
	requireEqual(t, st.AppliedDeleted, uint64(variants), "all variants counted")
	requireEqual(t, len(env.pool.expiry), 0, "no stale expiry records")
}

func TestTTLExpiry(t *testing.T) {
	env := newPoolEnv(t)
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x0e), bodyWithTag(1), msgOpts{}), 0)

	env.clock.advance(599 * time.Second)
	requireEqual(t, len(env.pool.SelectForBlock(allShard, 0)), 1, "alive before TTL")

	env.clock.advance(2 * time.Second)
	requireEqual(t, len(env.pool.SelectForBlock(allShard, 0)), 0, "expired not selected")
	st := env.pool.Stats()
	requireEqual(t, st.Pooled, 0, "selection reclaims expired entry")
	requireEqual(t, st.Expired, uint64(1), "expiry counter")
}

func TestNonPositiveTTLUsesDefault(t *testing.T) {
	for _, ttl := range []time.Duration{0, -time.Second} {
		pool := New(Config{Clock: newFakeClock(), TTL: ttl})
		if pool.cfg.TTL != 10*time.Minute {
			t.Fatalf("TTL %s resolved to %s", ttl, pool.cfg.TTL)
		}
		pool.Close()
	}
}

func TestSystemClockExpiresIdlePoolWithoutStatsPolling(t *testing.T) {
	p := New(Config{TTL: 20 * time.Millisecond})
	t.Cleanup(p.Close)
	message := buildExtMsg(t, 0, testAddr(0x6a), bodyWithTag(1), msgOpts{})
	if err := p.AddExternal(message.raw, message.root, nil, 0); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(time.Second)
	for {
		p.mu.Lock()
		pooled := p.totalCount
		p.mu.Unlock()
		if pooled == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("idle pool retained an external past its TTL")
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func TestExpiredDuplicateIsFreshAdmission(t *testing.T) {
	env := newPoolEnv(t, func(c *Config) { c.TTL = time.Second })
	raw := buildExtMsg(t, 0, testAddr(0x5e), bodyWithTag(1), msgOpts{})
	env.mustAdd(raw, 0)

	env.clock.advance(2 * time.Second)
	if err := env.pool.AddExternal(raw.raw, raw.root, nil, 0); err != nil {
		t.Fatal(err)
	}
	st := env.pool.Stats()
	requireEqual(t, st.Added, uint64(2), "expired duplicate admitted anew")
	requireEqual(t, st.DedupSkipped, uint64(0), "expired entry is not a duplicate")
	requireEqual(t, st.Expired, uint64(1), "old generation expired")
	requireEqual(t, st.Pooled, 1, "fresh generation pooled")
	requireEqual(t, len(env.pool.expiry), 1, "one live expiry record")
}

func TestAddReclaimsAllExpiredEntriesBeforeCaps(t *testing.T) {
	env := newPoolEnv(t, func(c *Config) {
		c.TTL = time.Second
		c.MempoolLimit = 4
	})
	for i := 0; i < 4; i++ {
		env.mustAdd(buildExtMsg(t, 0, testAddr(byte(0x70+i)), bodyWithTag(uint64(i)), msgOpts{}), 0)
	}
	env.clock.advance(2 * time.Second)
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x7f), bodyWithTag(5), msgOpts{}), 0)

	st := env.pool.Stats()
	requireEqual(t, st.Expired, uint64(4), "all due entries reclaimed")
	requireEqual(t, st.Pooled, 1, "expired entries do not occupy the cap")
	requireEqual(t, len(env.pool.expiry), 1, "heap contains only the fresh entry")
}

func TestExpiryHeapTracksOnlyLiveEntries(t *testing.T) {
	env := newPoolEnv(t)
	raw := buildExtMsg(t, 0, testAddr(0x5f), bodyWithTag(1), msgOpts{})
	env.mustAdd(raw, 0)
	for priority := 1; priority <= 64; priority++ {
		if err := env.pool.AddExternal(raw.raw, raw.root, nil, priority); err != nil {
			t.Fatal(err)
		}
	}
	requireEqual(t, len(env.pool.expiry), 1, "priority bumps do not leak expiry records")

	selected := env.pool.SelectForBlock(allShard, 1)[0]
	if err := env.pool.Complete([]ExternalFeedback{{
		Ref:     selected.Reference(),
		Outcome: ExternalInvalid,
	}}); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, len(env.pool.expiry), 0, "explicit removal removes expiry record")
}

func TestSelectReturnsIndependentSnapshots(t *testing.T) {
	env := newPoolEnv(t)
	raw := buildExtMsg(t, 0, testAddr(0x60), bodyWithTag(1), msgOpts{})
	wantBytes := int64(len(raw.raw))
	wantHash := raw.root.HashKey()
	if err := env.pool.AddExternal(raw.raw, raw.root, nil, 0); err != nil {
		t.Fatal(err)
	}
	// The pool retains no copy of the caller's buffer, so scribbling over it
	// must be invisible: only its length ever crossed the boundary.
	raw.raw[0] ^= 0xff

	first := env.pool.SelectForBlock(allShard, 1)[0]
	ref := first.Reference()
	firstParsed, err := first.Parsed()
	if err != nil {
		t.Fatal(err)
	}
	firstParsed.DstAddr = nil
	first.Hash = [32]byte{}

	second := env.pool.SelectForBlock(allShard, 1)[0]
	secondParsed, err := second.Parsed()
	if err != nil {
		t.Fatal(err)
	}
	if second.Hash != wantHash || second.Root() == nil || secondParsed.DstAddr == nil || second.Workchain() != 0 {
		t.Fatal("mutating one selection changed the pooled record")
	}
	if got := env.pool.Stats().PooledBytes; got != wantBytes {
		t.Fatalf("accounted bytes %d, want the received length %d", got, wantBytes)
	}
	if second.Reference() != ref {
		t.Fatal("read-only selection does not advance the generation")
	}

	if err := env.pool.Complete([]ExternalFeedback{{Ref: ref, Outcome: ExternalInvalid}}); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, env.pool.Stats().Pooled, 0, "immutable reference remains usable")
}

func TestPoolClose(t *testing.T) {
	env := newPoolEnv(t)
	env.mustAdd(buildExtMsg(t, 0, testAddr(0x11), bodyWithTag(1), msgOpts{}), 0)

	env.pool.Close()
	closed := buildExtMsg(t, 0, testAddr(0x12), bodyWithTag(2), msgOpts{})
	if err := env.pool.AddExternal(closed.raw, closed.root, nil, 0); !errors.Is(err, ErrClosed) {
		t.Fatalf("closed pool must reject adds, got %v", err)
	}
}

func TestConcurrentPoolCloseWaitsForCleanup(t *testing.T) {
	pool := &Pool{
		cleanupStop: make(chan struct{}),
		cleanupDone: make(chan struct{}),
	}
	cleanupStarted := make(chan struct{})
	releaseCleanup := make(chan struct{})
	go func() {
		<-pool.cleanupStop
		close(cleanupStarted)
		<-releaseCleanup
		close(pool.cleanupDone)
	}()

	firstDone := make(chan struct{})
	go func() {
		pool.Close()
		close(firstDone)
	}()
	<-cleanupStarted

	secondDone := make(chan struct{})
	go func() {
		pool.Close()
		close(secondDone)
	}()
	select {
	case <-secondDone:
		t.Fatal("concurrent Close returned before cleanup stopped")
	case <-time.After(20 * time.Millisecond):
	}

	close(releaseCleanup)
	select {
	case <-firstDone:
	case <-time.After(time.Second):
		t.Fatal("first Close did not return after cleanup stopped")
	}
	select {
	case <-secondDone:
	case <-time.After(time.Second):
		t.Fatal("second Close did not return after cleanup stopped")
	}
}

// TestConcurrentAdds exercises the pool under parallel ingress — distinct
// messages, duplicates and priority bumps racing across goroutines while a
// collator keeps selecting. Run with -race.
func TestConcurrentAdds(t *testing.T) {
	env := newPoolEnv(t)

	const n = 64
	msgs := make([]testMsg, n)
	for i := 0; i < n; i++ {
		msgs[i] = buildExtMsg(t, 0, testAddr(byte(i)), bodyWithTag(uint64(i)), msgOpts{})
	}

	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		for copies := 0; copies < 3; copies++ {
			wg.Add(1)
			go func(m testMsg, prio int) {
				defer wg.Done()
				if err := env.pool.AddExternal(m.raw, m.root, nil, prio); err != nil {
					t.Errorf("add: %v", err)
				}
			}(msgs[i], copies)
		}
	}
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			env.pool.SelectForBlock(allShard, 16)
		}()
	}
	wg.Wait()

	requireEqual(t, env.pool.Stats().Pooled, n, "each message pooled once")
	requireEqual(t, len(env.pool.SelectForBlock(allShard, 0)), n, "selection complete")
}
