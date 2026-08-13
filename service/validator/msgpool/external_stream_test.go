package msgpool

import (
	"context"
	"errors"
	"testing"
	"time"
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
	if err = env.pool.AddExternal(duplicate.raw, duplicate.root, nil, ExternalPriorityLocal); err != nil {
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
