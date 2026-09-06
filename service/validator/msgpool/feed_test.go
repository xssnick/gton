package msgpool

import (
	"sync"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var feedTestShard int64 = -1 << 63

var feedTestSource = ShardIdent{Workchain: 0, Shard: uint64(feedTestShard)}

func feedTestBlock(t testing.TB, seqno uint32, genUTime uint32) AppliedBlock {
	t.Helper()

	return AppliedBlock{
		ID: ton.BlockIDExt{
			Workchain: 0,
			Shard:     feedTestShard,
			SeqNo:     seqno,
			RootHash:  make([]byte, 32),
			FileHash:  make([]byte, 32),
		},
		BlockRoot: deltaBlockRoot(t, newOutDescrDict(t).AsCell()),
		GenUTime:  genUTime,
	}
}

type recordedFeedPrewarmer struct {
	mu       sync.Mutex
	accounts []feedPrewarmAccount
	roots    []cell.Hash
}

func (w *recordedFeedPrewarmer) EnqueueRoot(root cell.Hash) bool {
	w.mu.Lock()
	w.roots = append(w.roots, root)
	w.mu.Unlock()

	return true
}

func (w *recordedFeedPrewarmer) EnqueueAccount(workchain int32, account [32]byte) bool {
	w.mu.Lock()
	w.accounts = append(w.accounts, feedPrewarmAccount{workchain: workchain, account: account})
	w.mu.Unlock()

	return true
}

func (w *recordedFeedPrewarmer) snapshot() []feedPrewarmAccount {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]feedPrewarmAccount(nil), w.accounts...)
}

func (w *recordedFeedPrewarmer) rootSnapshot() []cell.Hash {
	w.mu.Lock()
	defer w.mu.Unlock()

	return append([]cell.Hash(nil), w.roots...)
}

func newTestFeed(t *testing.T, prewarmer AccountPrewarmer) *Feed {
	t.Helper()
	pool := New(Config{})
	t.Cleanup(pool.Close)

	return NewFeed(FeedOptions{Pool: pool, Logger: zerolog.Nop(), Prewarmer: prewarmer})
}

func TestFeedPrewarmDeduplicatesDestinationsAcrossOneUpdate(t *testing.T) {
	warmer := &recordedFeedPrewarmer{}
	feed := newTestFeed(t, warmer)
	first := [32]byte{0x11}
	second := [32]byte{0x22}
	seen := feedPrewarmSeen{
		accounts:  make(map[feedPrewarmAccount]struct{}),
		envelopes: make(map[cell.Hash]struct{}),
	}
	firstEnvelope := cell.Hash{0xa1}
	secondEnvelope := cell.Hash{0xa2}
	variableEnvelope := cell.Hash{0xa3}

	feed.prewarmPooled([]*InternalMessage{
		{DestinationWorkchain: 0, DestinationAccount: first, DestinationPrewarmable: true, EnvHash: firstEnvelope},
		{DestinationWorkchain: 0, DestinationAccount: first, DestinationPrewarmable: true, EnvHash: firstEnvelope},
		{DestinationWorkchain: -1, DestinationAccount: second, DestinationPrewarmable: true, EnvHash: secondEnvelope},
		{DestinationWorkchain: 0, DestinationAccount: [32]byte{0x33}, EnvHash: variableEnvelope},
	}, &seen)
	feed.prewarmPooled([]*InternalMessage{
		{DestinationWorkchain: -1, DestinationAccount: second, DestinationPrewarmable: true, EnvHash: secondEnvelope},
	}, &seen)

	got := warmer.snapshot()
	want := []feedPrewarmAccount{
		{workchain: 0, account: first},
		{workchain: -1, account: second},
	}
	if len(got) != len(want) {
		t.Fatalf("prewarmed accounts = %+v, want %+v", got, want)
	}
	for index := range want {
		if got[index] != want[index] {
			t.Fatalf("prewarmed account %d = %+v, want %+v", index, got[index], want[index])
		}
	}

	roots := warmer.rootSnapshot()
	wantRoots := []cell.Hash{firstEnvelope, secondEnvelope, variableEnvelope}
	if len(roots) != len(wantRoots) {
		t.Fatalf("prewarmed envelope roots = %x, want %x", roots, wantRoots)
	}
	for index := range wantRoots {
		if roots[index] != wantRoots[index] {
			t.Fatalf("prewarmed envelope root %d = %x, want %x", index, roots[index], wantRoots[index])
		}
	}
}

// TestFeedDefersAStaleHeadUntilItSettles pins the two halves of the freshness
// gate together: a block older than the window is not fed inline, and the
// deferred head it leaves behind is fed by the sweep once nothing supersedes
// it. Without the second half a standalone collator that started on a halted
// chain would never arm its pool at all.
func TestFeedDefersAStaleHeadUntilItSettles(t *testing.T) {
	pool := New(Config{})
	t.Cleanup(pool.Close)
	feed := NewFeed(FeedOptions{
		Pool:            pool,
		Logger:          zerolog.Nop(),
		FreshnessWindow: time.Minute,
		HeadSettleDelay: time.Millisecond,
	})

	feed.Observe(feedTestBlock(t, 7, 1))

	feed.pendingMu.Lock()
	deferred := len(feed.pending)
	feed.pendingMu.Unlock()
	if deferred != 1 {
		t.Fatalf("deferred stale heads = %d, want 1", deferred)
	}
	if _, tracked := feed.processing[feedTestSource]; tracked {
		t.Fatal("stale head was fed inline")
	}

	time.Sleep(2 * time.Millisecond)
	feed.SweepSettled()

	feed.pendingMu.Lock()
	remaining := len(feed.pending)
	feed.pendingMu.Unlock()
	if remaining != 0 {
		t.Fatalf("settled heads left pending = %d, want 0", remaining)
	}
	tracked := feed.source(feedTestSource)
	tracked.mu.Lock()
	processed, seqno := tracked.processed, tracked.seqno
	tracked.mu.Unlock()
	if !processed || seqno != 7 {
		t.Fatalf("settled head bookkeeping = (%t, %d), want (true, 7)", processed, seqno)
	}
}

// TestFeedRefusesToWalkBackwardsFromASettledHead pins the ordering rule the
// per-source serializer exists for: a stale head that settles after a fresh
// block already advanced the source must not reseed the run behind it.
func TestFeedRefusesToWalkBackwardsFromASettledHead(t *testing.T) {
	pool := New(Config{})
	t.Cleanup(pool.Close)
	feed := NewFeed(FeedOptions{Pool: pool, Logger: zerolog.Nop(), FreshnessWindow: -1})

	if !feed.observeSource(feedTestSource, feedTestBlock(t, 9, 0)) {
		t.Fatal("fresh head was not processed")
	}
	if feed.observeSource(feedTestSource, feedTestBlock(t, 8, 0)) {
		t.Fatal("older head was processed after a newer one")
	}
	if feed.observeSource(feedTestSource, feedTestBlock(t, 9, 0)) {
		t.Fatal("same head was processed twice")
	}
}
