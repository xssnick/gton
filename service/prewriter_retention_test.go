package service

import (
	"encoding/binary"
	"testing"

	"github.com/rs/zerolog"
)

func TestPrewriterCompactionReleasesProcessedJobs(t *testing.T) {
	t.Parallel()

	const payloadBytes = 8
	w := newPrewriter[[]byte](zerolog.Nop(), prewriterConfig{maxQueueBytes: 1 << 20})
	w.jobs = make([]prewriteJob[[]byte], 0, prewriteQueueJobsLimit-1)
	var enqueued, popped uint64
	compactions := 0

	checkQueue := func() {
		t.Helper()

		if got, want := len(w.jobs)-w.head, int(enqueued-popped); got != want {
			t.Fatalf("queued jobs = %d, want %d", got, want)
		}
		if got, want := w.bytes, (enqueued-popped)*payloadBytes; got != want {
			t.Fatalf("queued bytes = %d, want %d", got, want)
		}

		// The GC scans the complete backing array, including the compacted tail
		// beyond len. Only unread jobs may retain a payload anywhere in it.
		for i, job := range w.jobs[:cap(w.jobs)] {
			if i < w.head || i >= len(w.jobs) {
				if job.value != nil || job.seq != 0 || job.bytes != 0 {
					t.Fatalf("processed job retained at backing-array index %d", i)
				}
				continue
			}

			want := popped + uint64(i-w.head) + 1
			if job.seq != want || len(job.value) != payloadBytes || job.bytes != payloadBytes {
				t.Fatalf("unread job at index %d: seq=%d len=%d bytes=%d, want %d/%d/%d",
					i, job.seq, len(job.value), job.bytes, want, payloadBytes, payloadBytes)
			}
			if got := binary.LittleEndian.Uint64(job.value); got != want {
				t.Fatalf("unread payload at index %d = %d, want %d", i, got, want)
			}
		}
	}

	enqueue := func(count int) {
		t.Helper()

		for range count {
			payload := make([]byte, payloadBytes)
			binary.LittleEndian.PutUint64(payload, enqueued+1)
			seq, wait, err := w.enqueueDetached(payload, payloadBytes)
			if err != nil {
				t.Fatal(err)
			}
			enqueued++
			if seq != enqueued {
				t.Fatalf("enqueue sequence = %d, want %d", seq, enqueued)
			}
			if err := wait(); err != nil {
				t.Fatal(err)
			}
		}
		checkQueue()
	}

	pop := func(count int) {
		t.Helper()

		for range count {
			previousHead := w.head
			job, ok := w.popJob()
			if !ok {
				t.Fatal("queue emptied before all expected jobs were popped")
			}
			popped++
			if job.seq != popped || len(job.value) != payloadBytes || job.bytes != payloadBytes {
				t.Fatalf("popped job: seq=%d len=%d bytes=%d, want %d/%d/%d",
					job.seq, len(job.value), job.bytes, popped, payloadBytes, payloadBytes)
			}
			if got := binary.LittleEndian.Uint64(job.value); got != popped {
				t.Fatalf("popped payload = %d, want %d", got, popped)
			}
			if previousHead > 0 && w.head == 0 && len(w.jobs) > 0 {
				compactions++
				checkQueue()
			}
		}
		checkQueue()
	}

	enqueue(prewriteQueueJobsLimit - 1)
	pop(prewriteQueueJobsLimit - 2)
	// Refill before emptying, exercising tail reuse and successive compactions.
	enqueue(prewriteQueueJobsLimit - 2)
	pop(prewriteQueueJobsLimit - 1)

	if compactions < 4 {
		t.Fatalf("compactions = %d, want at least 4", compactions)
	}
	if w.jobs != nil || w.head != 0 || w.bytes != 0 {
		t.Fatal("drained queue retains backing storage or accounting")
	}
	if _, ok := w.popJob(); ok {
		t.Fatal("drained queue returned another job")
	}
}
