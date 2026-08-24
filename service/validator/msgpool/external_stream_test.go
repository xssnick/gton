package msgpool

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestExternalStreamSnapshotsReadyAndNewMessages(t *testing.T) {
	env := newPoolEnv(t)
	initial := env.mustAdd(buildExtMsg(t, 0, testAddr(0x11), bodyWithTag(1), msgOpts{}), ExternalPriorityBroadcast)

	stream, err := env.pool.OpenExternalStream(allShard, 500)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	first, err := stream.Next(t.Context(), 500)
	if err != nil {
		t.Fatal(err)
	}
	requireEqual(t, len(first), 1, "initial batch size")
	requireEqual(t, first[0].Hash, initial.Hash, "initial message")

	ready := make(chan []ExternalSnapshot, 1)
	errs := make(chan error, 1)
	go func() {
		batch, nextErr := stream.Next(t.Context(), 500)
		if nextErr != nil {
			errs <- nextErr
			return
		}
		ready <- batch
	}()

	added := env.mustAdd(buildExtMsg(t, 0, testAddr(0x22), bodyWithTag(2), msgOpts{}), ExternalPriorityLocal)
	select {
	case nextErr := <-errs:
		t.Fatal(nextErr)
	case batch := <-ready:
		requireEqual(t, len(batch), 1, "new batch size")
		requireEqual(t, batch[0].Hash, added.Hash, "new admitted message")
	case <-time.After(time.Second):
		t.Fatal("ready message did not wake external stream")
	}
}

func TestExternalStreamRoutesShardAndDoesNotRepeatHash(t *testing.T) {
	env := newPoolEnv(t)
	left := ShardIdent{Workchain: 0, Shard: 0x4000000000000000}
	leftMsg := env.mustAdd(buildExtMsg(t, 0, testAddr(0x11), bodyWithTag(1), msgOpts{}), 0)
	env.mustAdd(buildExtMsg(t, 0, testAddr(0xcc), bodyWithTag(2), msgOpts{}), 0)

	stream, err := env.pool.OpenExternalStream(left, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	batch, err := stream.Next(t.Context(), 1)
	if err != nil {
		t.Fatal(err)
	}
	requireEqual(t, len(batch), 1, "left batch size")
	requireEqual(t, batch[0].Hash, leftMsg.Hash, "left message")

	duplicate := buildExtMsg(t, 0, testAddr(0x11), bodyWithTag(1), msgOpts{})
	if _, err = env.pool.AddExternal(len(duplicate.raw), duplicate.root, nil, ExternalPriorityLocal); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(t.Context(), 20*time.Millisecond)
	defer cancel()
	if _, err = stream.Next(ctx, 1); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("duplicate wait error = %v, want deadline", err)
	}
}

func TestExternalStreamCapacityRefillsWithoutDroppingReadyMessages(t *testing.T) {
	env := newPoolEnv(t)
	for i := 0; i < 7; i++ {
		env.mustAdd(buildExtMsg(t, 0, testAddr(byte(i+1)), bodyWithTag(uint64(i+1)), msgOpts{}), 0)
	}

	stream, err := env.pool.OpenExternalStream(allShard, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	seen := make(map[[32]byte]struct{}, 7)
	for len(seen) < 7 {
		batch, nextErr := stream.Next(t.Context(), 2)
		if nextErr != nil {
			t.Fatal(nextErr)
		}
		for _, snapshot := range batch {
			if _, exists := seen[snapshot.Hash]; exists {
				t.Fatalf("external %x repeated", snapshot.Hash)
			}
			seen[snapshot.Hash] = struct{}{}
		}
	}
}

func TestExternalSnapshotDrainsFrozenReadySetWithoutFollowingAdmissions(t *testing.T) {
	env := newPoolEnv(t)
	want := make(map[[32]byte]struct{}, 7)
	for i := 0; i < 7; i++ {
		message := env.mustAdd(
			buildExtMsg(t, 0, testAddr(byte(i+1)), bodyWithTag(uint64(i+1)), msgOpts{}),
			0,
		)
		want[message.Hash] = struct{}{}
	}

	stream, err := env.pool.OpenExternalSnapshot(allShard, 2)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	late := env.mustAdd(buildExtMsg(t, 0, testAddr(0x44), bodyWithTag(44), msgOpts{}), 0)

	seen := make(map[[32]byte]struct{}, len(want))
	for {
		batch := stream.TakeReady(2)
		if len(batch) == 0 {
			break
		}
		for _, snapshot := range batch {
			if _, exists := want[snapshot.Hash]; !exists {
				t.Fatalf("snapshot included post-open message %x", snapshot.Hash)
			}
			if _, exists := seen[snapshot.Hash]; exists {
				t.Fatalf("external %x repeated", snapshot.Hash)
			}
			seen[snapshot.Hash] = struct{}{}
		}
	}
	requireEqual(t, len(seen), len(want), "frozen snapshot size")
	if _, exists := seen[late.Hash]; exists {
		t.Fatalf("late external %x entered frozen snapshot", late.Hash)
	}
	if _, err = stream.Next(t.Context(), 2); !errors.Is(err, ErrClosed) {
		t.Fatalf("drained snapshot Next error = %v, want ErrClosed", err)
	}
}

func TestExternalSnapshotSkipsMessagesThatExpireWhileCollatorPrepares(t *testing.T) {
	env := newPoolEnv(t, func(config *Config) { config.TTL = time.Second })
	env.mustAdd(buildExtMsg(t, 0, testAddr(1), bodyWithTag(1), msgOpts{}), 0)
	stream, err := env.pool.OpenExternalSnapshot(allShard, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()

	env.clock.advance(2 * time.Second)
	if batch := stream.TakeReady(1); len(batch) != 0 {
		t.Fatalf("expired snapshot returned %d messages", len(batch))
	}
	if _, err = stream.Next(t.Context(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("expired snapshot Next error = %v, want ErrClosed", err)
	}
}

func TestExternalSnapshotSkipsMessagesDeactivatedWhileCollatorPrepares(t *testing.T) {
	env := newPoolEnv(t)
	env.mustAdd(buildExtMsg(t, 0, testAddr(1), bodyWithTag(1), msgOpts{}), 0)
	env.mustAdd(buildExtMsg(t, 0, testAddr(2), bodyWithTag(2), msgOpts{}), 0)
	stream, err := env.pool.OpenExternalSnapshot(allShard, 1)
	if err != nil {
		t.Fatal(err)
	}
	defer stream.Close()
	pending := stream.snapshot[stream.snapshotAt]
	ref := ExternalRef{Hash: pending.msg.Hash, Generation: pending.generation}
	if err = env.pool.Complete([]ExternalFeedback{{Ref: ref, Outcome: ExternalInvalid}}); err != nil {
		t.Fatal(err)
	}

	if batch := stream.TakeReady(1); len(batch) != 1 {
		t.Fatalf("already queued snapshot returned %d messages, want one", len(batch))
	}
	if batch := stream.TakeReady(1); len(batch) != 0 {
		t.Fatalf("deactivated pending snapshot returned %d messages", len(batch))
	}
	if _, err = stream.Next(t.Context(), 1); !errors.Is(err, ErrClosed) {
		t.Fatalf("deactivated snapshot Next error = %v, want ErrClosed", err)
	}
}

func TestExternalStreamCloseAndPoolCloseUnblockNext(t *testing.T) {
	tests := []struct {
		name  string
		close func(*Pool, *ExternalStream)
	}{
		{name: "stream", close: func(_ *Pool, stream *ExternalStream) { _ = stream.Close() }},
		{name: "pool", close: func(pool *Pool, _ *ExternalStream) { pool.Close() }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			env := newPoolEnv(t)
			stream, err := env.pool.OpenExternalStream(allShard, 2)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				_, nextErr := stream.Next(t.Context(), 1)
				done <- nextErr
			}()

			test.close(env.pool, stream)
			select {
			case nextErr := <-done:
				if !errors.Is(nextErr, ErrClosed) {
					t.Fatalf("Next error = %v, want ErrClosed", nextErr)
				}
			case <-time.After(time.Second):
				t.Fatal("Next remained blocked after close")
			}
		})
	}
}

// A build that starts before its predecessor has reported what it consumed has
// to be told directly, and the telling has to reach both halves of the stream:
// the first fill and everything admitted afterwards. One map gates both, which
// is why the exclusion is seeded into it rather than filtered at either.
//
// Without this the successor is offered messages its predecessor already
// included, executes them again, and has its feedback for them dropped as
// stale — so they are never stamped with a retry time and come back on every
// remaining slot of the window.
func TestExternalStreamNeverOffersAnExcludedMessage(t *testing.T) {
	env := newPoolEnv(t)
	consumed := env.mustAdd(buildExtMsg(t, 0, testAddr(0x11), bodyWithTag(1), msgOpts{}), ExternalPriorityBroadcast)
	kept := env.mustAdd(buildExtMsg(t, 0, testAddr(0x22), bodyWithTag(2), msgOpts{}), ExternalPriorityBroadcast)

	stream, err := env.pool.OpenExternalStreamExcluding(allShard, 500, [][32]byte{consumed.Hash})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	// The first fill happens inside the open, so an exclusion applied after it
	// would already have leaked the message into the batch a successor takes.
	first, err := stream.Next(t.Context(), 500)
	if err != nil {
		t.Fatal(err)
	}
	requireEqual(t, len(first), 1, "first batch size")
	requireEqual(t, first[0].Hash, kept.Hash, "the first batch carries the message that was not excluded")

	// Follow admission reads the same map: the excluded message is already in the
	// pool and ready, so a stream that only filtered its first fill would offer
	// it the moment anything else woke the stream.
	ready := make(chan []ExternalSnapshot, 1)
	errs := make(chan error, 1)
	go func() {
		batch, nextErr := stream.Next(t.Context(), 500)
		if nextErr != nil {
			errs <- nextErr

			return
		}
		ready <- batch
	}()
	later := env.mustAdd(buildExtMsg(t, 0, testAddr(0x33), bodyWithTag(3), msgOpts{}), ExternalPriorityLocal)
	select {
	case nextErr := <-errs:
		t.Fatal(nextErr)
	case batch := <-ready:
		for _, snapshot := range batch {
			if snapshot.Hash == consumed.Hash {
				t.Fatal("an excluded message was offered by follow admission")
			}
		}
		requireEqual(t, batch[len(batch)-1].Hash, later.Hash, "the later message still arrives")
	case <-time.After(2 * time.Second):
		t.Fatal("the stream never woke")
	}
}

// The plain opener must keep offering everything, or the exclusion becomes a
// property of the pool rather than of one stream.
func TestExternalStreamWithoutExclusionsOffersEverything(t *testing.T) {
	env := newPoolEnv(t)
	first := env.mustAdd(buildExtMsg(t, 0, testAddr(0x11), bodyWithTag(1), msgOpts{}), ExternalPriorityBroadcast)
	second := env.mustAdd(buildExtMsg(t, 0, testAddr(0x22), bodyWithTag(2), msgOpts{}), ExternalPriorityBroadcast)

	stream, err := env.pool.OpenExternalStreamExcluding(allShard, 500, nil)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = stream.Close() })

	batch, err := stream.Next(t.Context(), 500)
	if err != nil {
		t.Fatal(err)
	}
	seen := map[[32]byte]bool{}
	for _, snapshot := range batch {
		seen[snapshot.Hash] = true
	}
	if !seen[first.Hash] || !seen[second.Hash] {
		t.Fatalf("an unexcluded stream offered %d of 2 ready messages", len(seen))
	}
}

// Topping the buffer back up after a batch is prefetch, so it was made
// conditional on the buffer being at most half full — under the pool mutex it is
// a full scan of every slab, and with a buffer of thousands and a batch of
// hundreds it ran after every take on a pool the take had barely moved.
//
// What must not change is what a stream eventually yields. The take loop refills
// the moment it runs dry, so draining a pool deeper than the buffer must still
// hand over every message exactly once, whether the prefetch ran or not.
func TestDeferredRefillStillDrainsTheWholePool(t *testing.T) {
	const (
		pooled   = 600
		capacity = 64
		batch    = 7
	)
	p := New(Config{MempoolLimit: 1 << 30, PerAddressLimit: 1 << 30, MempoolBytesLimit: 1 << 40})
	defer p.Close()

	want := map[[32]byte]struct{}{}
	for i := range pooled {
		var addr [32]byte
		copy(addr[:], fmt.Sprintf("%016d-drain-test", i))
		msg := buildExtMsg(t, 0, addr, cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell(), msgOpts{})
		if _, err := p.AddExternal(len(msg.raw), msg.root, nil, 0); err != nil {
			t.Fatal(err)
		}
		want[msg.root.HashKey()] = struct{}{}
	}

	stream, err := p.OpenExternalStream(allShard, capacity)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = stream.Close() }()

	got := map[[32]byte]struct{}{}
	// Bounded well above what a correct drain needs, so a stream that stops
	// early fails on the count below rather than spinning here.
	for range pooled/batch + capacity + 16 {
		taken := stream.TakeReady(batch)
		if len(taken) == 0 {
			break
		}
		for _, snapshot := range taken {
			if _, repeated := got[snapshot.Hash]; repeated {
				t.Fatalf("message %x was handed over twice", snapshot.Hash[:6])
			}
			got[snapshot.Hash] = struct{}{}
		}
	}
	if len(got) != len(want) {
		t.Fatalf("drained %d of %d pooled messages", len(got), len(want))
	}
	for hash := range want {
		if _, delivered := got[hash]; !delivered {
			t.Fatalf("message %x was never handed over", hash[:6])
		}
	}
}
