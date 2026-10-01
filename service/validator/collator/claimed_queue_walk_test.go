package collator

import (
	"context"
	"errors"
	"maps"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// TestClaimedQueueWalkMatchesTheSemanticWalk pins the two properties the
// claimed-prefix reader owes the full one, both load-bearing for the collated
// proof rather than stylistic:
//
//   - CELLS: the cells its walk reads are the cells the validator's own walk
//     reads. Cleanup's collection of exactly these cells is later recorded as
//     the closure's read set, and a cell the narrow parse stopped opening is a
//     pruned branch on every validator running on proofs — the incident class
//     traceProcessedQueueValidationClosure exists for.
//   - FIELDS: every value cleanup consumes out of an entry — the key, the
//     envelope hash, the current-hop prefix and the whole coverage descriptor —
//     equals what the full parse derives. alreadyProcessed decides dequeues from
//     the descriptor, so a divergence here silently changes which entries the
//     block drains.
//
// Both walks run over the same predecessor queue of a real fixture collation,
// against the same claimed bound, on separately prepared collations so each
// arm's read set attributes its own walk and nothing else.
func TestClaimedQueueWalkMatchesTheSemanticWalk(t *testing.T) {
	const stale = 6
	req := staleOwnQueueRequest(t, stale)
	bound := semanticMessageBound{lt: requestStartLT(t, req), hash: processedInfinityHash}

	type derived struct {
		key          msgpool.QueueKey
		envelopeHash cell.Hash
		current      msgpool.AccountPrefix
		descr        struct {
			curWorkchain  int32
			curPrefix     uint64
			nextWorkchain int32
			nextPrefix    uint64
			lt            uint64
			enqueuedLT    uint64
			hash          [32]byte
		}
	}
	fromClaimed := func(entry claimedQueueEntry) derived {
		var d derived
		d.key = entry.key
		d.envelopeHash = entry.envelopeHash
		d.current = entry.current
		d.descr.curWorkchain = entry.descr.CurWorkchain
		d.descr.curPrefix = entry.descr.CurPrefix
		d.descr.nextWorkchain = entry.descr.NextWorkchain
		d.descr.nextPrefix = entry.descr.NextPrefix
		d.descr.lt = entry.descr.LT
		d.descr.enqueuedLT = entry.descr.EnqueuedLT
		d.descr.hash = entry.descr.Hash
		return d
	}

	// The full walk's view of the same consumption.
	fullArm, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	beforeFull := recordedHashSet(t, fullArm)
	var full []derived
	err = walkSemanticQueuePrefix(fullArm.oldOutQueue, fullArm.shard, bound, func(entry semanticQueueEntry) error {
		full = append(full, fromClaimed(claimedQueueEntry{
			key:          msgpool.MakeQueueKey(entry.envelope.next, entry.envelope.message.HashKey()),
			envelopeHash: entry.enqueued.Msg.HashKey(),
			current:      entry.envelope.current,
			descr:        entry.descr,
		}))
		return nil
	})
	if err != nil {
		t.Fatalf("full walk: %v", err)
	}
	afterFull := recordedHashSet(t, fullArm)

	lightArm, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	beforeLight := recordedHashSet(t, lightArm)
	if !maps.Equal(beforeFull, beforeLight) {
		t.Fatalf("the two collations read different cells before the walks: %d against %d",
			len(beforeFull), len(beforeLight))
	}
	var light []derived
	err = walkClaimedQueuePrefix(lightArm.oldOutQueue, lightArm.shard, bound, func(entry claimedQueueEntry) error {
		light = append(light, fromClaimed(entry))
		return nil
	})
	if err != nil {
		t.Fatalf("claimed walk: %v", err)
	}
	afterLight := recordedHashSet(t, lightArm)

	// The fresh offered message plus every stale entry sit inside the bound, so
	// an empty visit list would mean the fixture stopped exercising the parse.
	if len(full) != stale+1 {
		t.Fatalf("the full walk visited %d entries, want %d — the fixture no longer covers the prefix",
			len(full), stale+1)
	}
	if len(light) != len(full) {
		t.Fatalf("the claimed walk visited %d entries, the full walk %d", len(light), len(full))
	}
	for i := range full {
		if full[i] != light[i] {
			t.Fatalf("entry %d diverges between the parses:\nfull  %+v\nlight %+v", i, full[i], light[i])
		}
	}

	if opened := len(afterFull) - len(beforeFull); opened == 0 {
		t.Fatal("the full walk opened nothing new, so the read-set comparison is vacuous")
	}
	for hash := range afterFull {
		if _, ok := afterLight[hash]; !ok {
			t.Fatalf("the claimed walk did not open %x, which the full walk opens — this under-recording "+
				"is what ships a proof validators reject on a pruned branch", hash)
		}
	}
	for hash := range afterLight {
		if _, ok := afterFull[hash]; !ok {
			t.Fatalf("the claimed walk opened %x, which the full walk does not — the collated proof "+
				"would be wider than the validator's read set", hash)
		}
	}
	t.Logf("parity over %d entries: read set %d -> %d on both arms",
		len(full), len(beforeFull), len(afterFull))
}

// TestClaimedQueueWalkKeepsTheFullParsesVerdicts pins rejection parity on the
// one malformed shape the package already manufactures: msg_envelope_v2#5 with
// neither emitted_lt nor metadata, which the reference's MsgEnvelope::unpack
// rejects. The claimed walk must fail on it the way the full walk does — same
// verdict classification, same ErrInvalidInput, same reason — because cleanup
// now meets inherited content before the closure replay does, and a narrow
// parse that let it through would ship a candidate every validator refuses.
func TestClaimedQueueWalkKeepsTheFullParsesVerdicts(t *testing.T) {
	req := unparseableEnvelopeRequest(t)
	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	bound := semanticMessageBound{lt: requestStartLT(t, req), hash: processedInfinityHash}

	fullErr := walkSemanticQueuePrefix(c.oldOutQueue, c.shard, bound, nil)
	lightErr := walkClaimedQueuePrefix(c.oldOutQueue, c.shard, bound,
		func(claimedQueueEntry) error { return nil })
	for arm, err := range map[string]error{"full": fullErr, "claimed": lightErr} {
		if err == nil {
			t.Fatalf("the %s walk accepted the canonically invalid envelope", arm)
		}
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("the %s walk failed with %v, not an ErrInvalidInput content verdict", arm, err)
		}
		var verdict semanticQueueEntryVerdict
		if !errors.As(err, &verdict) {
			t.Fatalf("the %s walk's failure %v is not classified as a content verdict", arm, err)
		}
		if !strings.Contains(err.Error(), "v2 tag without emitted lt or metadata") {
			t.Fatalf("the %s walk rejected for %q, want the canonical-tag reason", arm, err)
		}
	}
}
