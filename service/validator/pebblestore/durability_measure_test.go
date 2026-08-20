package pebblestore

import (
	"crypto/sha256"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/cockroachdb/pebble/v2"
	"github.com/cockroachdb/pebble/v2/vfs"
)

// One candidate wire, as measured on the field box: block 424,977 B + collated 334,013 B.
const measureWireBytes = 424977 + 334013

// The measurement behind the durability split, kept reproducible rather than
// retold. It is gated because it is a timing harness rather than a gate: forty
// rounds of a 760 KB write and an fsync each is seconds of disk work, and its
// numbers are properties of the machine it runs on.
//
//	GTON_MEASURE_DURABILITY=1 go test ./service/validator/pebblestore/ \
//	    -run TestMeasureDurabilitySplit -v
//
// The production sequence to a notarize vote, from simplex:
//
//	completeStore(candidate) -> maybeVoteNotar -> castVote -> journal.SaveOurVote
//	  -> journalDone -> finishVoteCast (sign, apply, broadcast)
//
// so the candidate store and the vote journal are SERIAL, never concurrent.
func TestMeasureDurabilitySplit(t *testing.T) {
	if os.Getenv("GTON_MEASURE_DURABILITY") != "1" {
		t.Skip("set GTON_MEASURE_DURABILITY=1 to run the durability split measurement")
	}

	const rounds = 40
	type candidateStore func(
		index int,
		wire []byte,
		hash [sha256.Size]byte,
		db *pebble.DB,
		packs *candidatePackStore,
	) error
	for _, arm := range []struct {
		name  string
		store candidateStore
	}{
		{
			name: "before: candidate in synced Pebble, then vote synced",
			store: func(index int, wire []byte, _ [sha256.Size]byte, db *pebble.DB, _ *candidatePackStore) error {
				return db.Set([]byte(fmt.Sprintf("wire-%04d", index)), wire, pebble.Sync)
			},
		},
		{
			name: "after:  candidate in pack + pointer NoSync, then vote synced",
			store: func(index int, wire []byte, hash [sha256.Size]byte, db *pebble.DB, packs *candidatePackStore) error {
				pointer, err := packs.append(storageNamespace{}, wire, hash)
				if err != nil {
					return err
				}

				return db.Set(
					[]byte(fmt.Sprintf("pointer-%04d", index)),
					encodeCandidatePackPointer(pointer),
					pebble.NoSync,
				)
			},
		},
	} {
		counting := &syncCountingFS{FS: vfs.Default}
		dir := filepath.Join(t.TempDir(), "validator")
		db, err := pebble.Open(dir, &pebble.Options{FS: counting})
		if err != nil {
			t.Fatal(err)
		}
		packs := newCandidatePackStore(dir)

		var payload, commitment, total []time.Duration
		wire := make([]byte, measureWireBytes)
		hash := sha256.Sum256(wire)
		for i := 0; i < rounds; i++ {
			slotStart := time.Now()
			if err = arm.store(i, wire, hash, db, packs); err != nil {
				t.Fatal(err)
			}
			payloadLatency := time.Since(slotStart)

			// Only now, exactly as maybeVoteNotar -> castVote does.
			voteStart := time.Now()
			if err = db.Set([]byte(fmt.Sprintf("vote-%04d", i)), []byte{1}, pebble.Sync); err != nil {
				t.Fatal(err)
			}
			commitmentLatency := time.Since(voteStart)

			payload = append(payload, payloadLatency)
			commitment = append(commitment, commitmentLatency)
			total = append(total, time.Since(slotStart))
		}
		syncs := counting.syncs.Load()
		if err = packs.close(); err != nil {
			t.Fatal(err)
		}
		if err = db.Close(); err != nil {
			t.Fatal(err)
		}

		report := func(label string, samples []time.Duration) {
			sort.Slice(samples, func(i, j int) bool { return samples[i] < samples[j] })
			ms := func(d time.Duration) float64 { return float64(d.Microseconds()) / 1000 }
			t.Logf("%-46s %-22s median=%7.3fms p90=%7.3fms max=%7.3fms",
				arm.name, label, ms(samples[len(samples)/2]), ms(samples[len(samples)*9/10]), ms(samples[len(samples)-1]))
		}
		report("half A: candidate store", payload)
		report("half B: vote journal", commitment)
		report("both: store -> vote cast", total)
		t.Logf("%-46s fsyncs=%d over %d rounds (%.2f per round)", arm.name, syncs, rounds, float64(syncs)/float64(rounds))
	}
}
