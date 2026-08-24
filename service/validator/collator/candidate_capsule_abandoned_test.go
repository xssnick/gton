package collator

import (
	"errors"
	"runtime"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

// capsuleGoroutineCreator is the frame a broadcast-payload goroutine carries for
// as long as it lives, whichever serializer or compressor it happens to be
// executing inside. Matching on the creator rather than on the work is what
// makes the sighting mean "a payload was started", which is the thing under
// test: the capsule takes no context and exposes no way to stop it, so a
// payload started for a candidate that is never produced runs to completion
// beside whatever the collator does next.
const capsuleGoroutineCreator = "simplex.PrepareCandidateAsync"

// capsulesRunning reports whether any broadcast payload is in flight right now.
// The buffer grows until the dump fits: a truncated dump would answer "no" for
// a payload whose goroutine was cut off the end.
func capsulesRunning(buf *[]byte) bool {
	for {
		n := runtime.Stack(*buf, true)
		if n < len(*buf) {
			return strings.Contains(string((*buf)[:n]), capsuleGoroutineCreator)
		}
		*buf = make([]byte, 2*len(*buf))
	}
}

// watchCapsuleStarts runs collate while sampling every live goroutine, and
// reports how many samples saw a broadcast payload running. A payload takes
// milliseconds and the sampling interval is a fraction of one, so a payload
// that starts anywhere inside collate is seen many times over.
func watchCapsuleStarts(t *testing.T, collate func()) int {
	t.Helper()

	// A payload started by an earlier collation outlives it by several
	// milliseconds, so without this the count would inherit one from the test
	// that ran before this one and fail an arm that started nothing.
	buf := make([]byte, 1<<20)
	quiet := time.Now().Add(5 * time.Second)
	for capsulesRunning(&buf) {
		if time.Now().After(quiet) {
			t.Fatal("a broadcast payload from an earlier collation never finished")
		}
		time.Sleep(time.Millisecond)
	}

	var seen atomic.Int64
	stop, watching := make(chan struct{}), make(chan struct{})
	go func() {
		defer close(watching)
		sampling := make([]byte, 1<<20)
		for {
			if capsulesRunning(&sampling) {
				seen.Add(1)
			}
			select {
			case <-stop:
				return
			case <-time.After(200 * time.Microsecond):
			}
		}
	}()

	collate()
	// The last sample of all: nothing may still be compressing a candidate the
	// collation has already given up on.
	if capsulesRunning(&buf) {
		seen.Add(1)
	}
	close(stop)
	<-watching

	return int(seen.Load())
}

// A collation that ends on a size limit produces no candidate, and the
// broadcast payload is the most expensive thing the collator can be holding
// when that happens: it is a third full serialization of the block plus its
// compression, it cannot be cancelled, and the failure it would run beside is
// precisely the one that rebuilds the block — up to twice, on the largest
// blocks a shard produces and against the tightest part of the slot.
//
// So the payload is started only when nothing already known says the candidate
// is lost. The two limits are known at different moments and the gate treats
// them differently, which is why both are exercised here: the block BOC has
// been measured on the sibling goroutine by the time the payload would start,
// while the collated bytes do not exist yet and the collation's own estimate of
// them stands in.
//
// The third arm is the control. A watcher that sees nothing proves nothing, so
// the same watcher must see a payload on a collation that succeeds.
func TestNoBroadcastPayloadStartsForACandidateASizeLimitRejects(t *testing.T) {
	for _, tc := range []struct {
		name     string
		narrow   func(*Config)
		rejected string
	}{
		{
			name:     "block BOC over the limit",
			narrow:   func(config *Config) { config.maxBlockBytes = 1 },
			rejected: "block BOC",
		},
		{
			name:     "collated data over the limit",
			narrow:   func(config *Config) { config.maxCollatedBytes = 1 },
			rejected: "collated data",
		},
		{
			name:   "control: both limits as configured",
			narrow: func(*Config) {},
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req := benchMainnetCollatedRequest(t, 1)
			// The limits are read off the shared masterchain configuration, so
			// the narrowing goes on a copy of it.
			config := *req.Masterchain.Config
			tc.narrow(&config)
			req.Masterchain.Config = &config

			var err error
			seen := watchCapsuleStarts(t, func() {
				_, err = testBuilder().BuildShard(t.Context(), req)
			})

			if tc.rejected == "" {
				if err != nil {
					t.Fatalf("collate the mainnet fixture: %v", err)
				}
				if seen == 0 {
					t.Fatal("no broadcast payload was seen on a collation that produced a candidate, " +
						"so the sightings the other arms count on mean nothing")
				}
				t.Logf("control: a payload was in flight in %d samples", seen)

				return
			}
			if !errors.Is(err, ErrSizeLimit) || !strings.Contains(err.Error(), tc.rejected) {
				t.Fatalf("collation ended with %v, want the %s size limit", err, tc.rejected)
			}
			if seen != 0 {
				t.Fatalf("a broadcast payload was running in %d samples of a collation "+
					"the %s size limit rejected, for a candidate that is never produced",
					seen, tc.rejected)
			}
		})
	}
}
