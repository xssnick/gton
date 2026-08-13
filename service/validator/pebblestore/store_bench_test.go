package pebblestore

import (
	"bytes"
	"testing"

	"github.com/cockroachdb/pebble/v2"
	"github.com/xssnick/gton/service/validator/simplex"
)

// benchLargeWriteBytes approximates one collated candidate value. SaveCandidate
// appends the block BOC and the collated data verbatim into a single batch.Set,
// so a raw write of the same size is a faithful stand-in for the queue and
// fsync behavior under study here.
const benchLargeWriteBytes = 4 << 20

// BenchmarkJournalVoteWrite measures one durable consensus vote on an idle
// store: the floor of the vote path, dominated by the batch fsync.
func BenchmarkJournalVoteWrite(b *testing.B) {
	store := openBenchStore(b)
	defer closeBenchStore(b, store)

	j := store.Validator().Journal(testSession(3), 8)
	result := make(chan error, 1)
	slot := uint32(0)

	b.ReportAllocs()
	for b.Loop() {
		slot++
		j.SaveOurVote(simplex.SkipVote(slot), func(err error) { result <- err })
		if err := <-result; err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkJournalVoteWriteBehindLargeWrite measures the same vote while a
// multi-megabyte write is already in the queue, which is what a validator with
// a local collator does on every leader slot: both workloads share one queue,
// one writer goroutine and one synced batch. The reported latency is the vote's
// only; the large write is submitted asynchronously and drained afterwards.
//
// The comparison against BenchmarkJournalVoteWrite is not purely head-of-line
// cost: the queue is also a group-commit mechanism, so the vote can ride the
// large write's single fsync instead of paying its own.
func BenchmarkJournalVoteWriteBehindLargeWrite(b *testing.B) {
	store := openBenchStore(b)
	defer closeBenchStore(b, store)

	j := store.Validator().Journal(testSession(3), 8)
	payload := bytes.Repeat([]byte{0x31}, benchLargeWriteBytes)
	result := make(chan error, 1)
	large := make(chan error, 1)
	slot := uint32(0)

	b.ReportAllocs()
	for b.Loop() {
		slot++
		key := binaryBenchKey(slot)
		if err := store.submit(writeRequest{
			sizeHint: len(payload),
			apply: func(batch *pebble.Batch) error {
				return batch.Set(key, payload, nil)
			},
			done: func(err error) { large <- err },
		}); err != nil {
			b.Fatal(err)
		}
		j.SaveOurVote(simplex.SkipVote(slot), func(err error) { result <- err })
		if err := <-result; err != nil {
			b.Fatal(err)
		}
		b.StopTimer()
		if err := <-large; err != nil {
			b.Fatal(err)
		}
		b.StartTimer()
	}
}

// BenchmarkLargeWrite is the cost of the large write on its own, for scale.
func BenchmarkLargeWrite(b *testing.B) {
	store := openBenchStore(b)
	defer closeBenchStore(b, store)

	payload := bytes.Repeat([]byte{0x31}, benchLargeWriteBytes)
	large := make(chan error, 1)
	index := uint32(0)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		index++
		key := binaryBenchKey(index)
		if err := store.submit(writeRequest{
			sizeHint: len(payload),
			apply: func(batch *pebble.Batch) error {
				return batch.Set(key, payload, nil)
			},
			done: func(err error) { large <- err },
		}); err != nil {
			b.Fatal(err)
		}
		if err := <-large; err != nil {
			b.Fatal(err)
		}
	}
}

// binaryBenchKey addresses a scratch keyspace that no production key kind uses,
// so benchmark payloads never collide with real records.
func binaryBenchKey(index uint32) []byte {
	return append([]byte{0xfe}, byte(index), byte(index>>8), byte(index>>16), byte(index>>24))
}

func openBenchStore(b *testing.B) *Store {
	b.Helper()

	store, err := Open(Options{Dir: b.TempDir()})
	if err != nil {
		b.Fatalf("Open: %v", err)
	}

	return store
}

func closeBenchStore(b *testing.B, store *Store) {
	b.Helper()

	if err := store.Close(); err != nil {
		b.Fatalf("Close: %v", err)
	}
}

// BenchmarkJournalBootstrap measures the session-start recovery read. Starting
// a session reads it three times with no write in between — recoverReservations,
// the runtime constructor and Engine.Start — so the difference between these
// two benchmarks is what the cached scan saves per session start, twice.
func BenchmarkJournalBootstrap(b *testing.B) {
	benchmarkJournalBootstrap(b, false)
}

func BenchmarkJournalBootstrapUncached(b *testing.B) {
	benchmarkJournalBootstrap(b, true)
}

func benchmarkJournalBootstrap(b *testing.B, rescan bool) {
	b.Helper()

	store := openBenchStore(b)
	defer closeBenchStore(b, store)

	const records = 128
	const signatures = 8
	j := store.Validator().Journal(testSession(3), signatures).(*journal)
	result := make(chan error, 1)
	for slot := uint32(1); slot <= records; slot++ {
		j.SaveOurVote(simplex.SkipVote(slot), func(err error) { result <- err })
		if err := <-result; err != nil {
			b.Fatal(err)
		}
		certificate := &simplex.Certificate{Vote: simplex.SkipVote(slot)}
		for index := range uint32(signatures) {
			certificate.Signatures = append(certificate.Signatures, simplex.VoteSignature{
				ValidatorIndex: index,
				Signature:      bytes.Repeat([]byte{byte(index)}, 64),
			})
		}
		j.SaveCertificate(certificate, func(err error) { result <- err })
		if err := <-result; err != nil {
			b.Fatal(err)
		}
	}

	b.ReportAllocs()
	for b.Loop() {
		if rescan {
			j.mu.Lock()
			j.invalidateBootstrapLocked()
			j.mu.Unlock()
		}
		state, err := j.Bootstrap()
		if err != nil {
			b.Fatal(err)
		}
		if len(state.Certificates) != records {
			b.Fatalf("bootstrap certificates = %d", len(state.Certificates))
		}
	}
}
