package pebblestore

import (
	"bytes"
	"crypto/sha256"
	"testing"

	"github.com/xssnick/gton/service/validator"
	"github.com/xssnick/gton/service/validator/simplex"
)

// benchLargeWriteBytes approximates one candidate wire.
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

// BenchmarkJournalVoteWriteAfterCandidatePack measures the serial store-then-vote
// path used by notarization. The candidate payload is an unsynced pack append;
// only its compact pointer enters Pebble before the durable vote.
func BenchmarkJournalVoteWriteAfterCandidatePack(b *testing.B) {
	store := openBenchStore(b)
	defer closeBenchStore(b, store)

	session := testSession(3)
	j := store.Validator().Journal(session, 8)
	payload := bytes.Repeat([]byte{0x31}, benchLargeWriteBytes)
	payloadHash := sha256.Sum256(payload)
	result := make(chan error, 1)
	slot := uint32(0)

	b.ReportAllocs()
	for b.Loop() {
		slot++
		store.Validator().SaveCandidate(session, validator.CandidateRecord{
			ID:          simplex.CandidateID{Slot: slot},
			Wire:        payload,
			ContentHash: payloadHash,
		}, func(err error) { result <- err })
		if err := <-result; err != nil {
			b.Fatal(err)
		}
		j.SaveOurVote(simplex.SkipVote(slot), func(err error) { result <- err })
		if err := <-result; err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkCandidatePackWrite is the candidate store half on its own.
func BenchmarkCandidatePackWrite(b *testing.B) {
	store := openBenchStore(b)
	defer closeBenchStore(b, store)

	session := testSession(4)
	payload := bytes.Repeat([]byte{0x31}, benchLargeWriteBytes)
	payloadHash := sha256.Sum256(payload)
	result := make(chan error, 1)
	slot := uint32(0)

	b.ReportAllocs()
	b.SetBytes(int64(len(payload)))
	for b.Loop() {
		slot++
		store.Validator().SaveCandidate(session, validator.CandidateRecord{
			ID:          simplex.CandidateID{Slot: slot},
			Wire:        payload,
			ContentHash: payloadHash,
		}, func(err error) { result <- err })
		if err := <-result; err != nil {
			b.Fatal(err)
		}
	}
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
