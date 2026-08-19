package collator

import (
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// errInjectedStorageFault stands in for what a store returns when a read it has
// no reason to refuse fails anyway: a bad sector, an unlinked SST, a closed
// database mid-slot. Nothing about it says "this entry is malformed", which is
// the whole point — the classification under test may not depend on the text.
var errInjectedStorageFault = errors.New("injected storage fault")

// faultingLazifier is advLazifier with a fault list: it serves the same trusted
// records, and refuses the hashes it was told to refuse. It counts loads so a
// test can also assert what the fault did NOT cost.
type faultingLazifier struct {
	records map[cell.Hash][]byte
	fail    map[cell.Hash]struct{}
	calls   int
	refused int
}

func newFaultingLazifier() *faultingLazifier {
	return &faultingLazifier{
		records: map[cell.Hash][]byte{},
		fail:    map[cell.Hash]struct{}{},
	}
}

func (l *faultingLazifier) index(tb testing.TB, c *cell.Cell) {
	tb.Helper()
	if c == nil {
		return
	}
	hash := c.HashKey()
	if _, known := l.records[hash]; known {
		return
	}
	record, err := storage.CellRecordFromCell(c)
	if err != nil {
		tb.Fatalf("cell record: %v", err)
	}
	l.records[hash] = storage.EncodeCellRecord(record)
	for i := range int(c.RefsNum()) {
		ref, refErr := c.PeekRef(i)
		if refErr != nil {
			tb.Fatalf("peek ref: %v", refErr)
		}
		l.index(tb, ref)
	}
}

func (l *faultingLazifier) loader() cell.LazyCellLoader {
	var self cell.LazyCellLoader
	self = func(hash cell.Hash) (*cell.Cell, error) {
		l.calls++
		if _, refuse := l.fail[hash]; refuse {
			l.refused++
			return nil, errInjectedStorageFault
		}
		encoded, known := l.records[hash]
		if !known {
			return nil, cell.ErrLazyRefNotFound
		}
		return storage.DecodeLazyCellRecordTrusted(hash[:], encoded, self)
	}
	return self
}

// root returns the store-shaped form of c: the root cell itself present, every
// reference below it an unresolved stub that loads on demand.
func (l *faultingLazifier) root(tb testing.TB, c *cell.Cell) *cell.Cell {
	tb.Helper()

	l.index(tb, c)
	hash := c.HashKey()
	out, err := storage.DecodeLazyCellRecordTrusted(hash[:], l.records[hash], l.loader())
	if err != nil {
		tb.Fatalf("lazy root: %v", err)
	}
	return out
}

// replayOnlyQueueRequest builds a predecessor whose out-queue holds four
// entries at distinct logical times, of which the block imports only the
// HIGHEST. The claimed bound is then above all four, so the walk parses all
// four, while collation itself parses exactly two of them: the imported one,
// and the lowest, which cleanup's own-shard stream always opens before its
// frontier declines it and drops the part.
//
// The returned entry is the second-lowest. Nothing but the proof-closure replay
// opens its cells — it is not imported, so no delete path reaches it, and it is
// unchanged, so the queue diff does not visit it — which is what a fault
// injected into them has to be true of before it can say anything about the
// replay's classification. The assertions do not take that on trust: the two
// failures the replay's loading step raises are worded by
// materialiseSemanticQueueLeaf and by nothing else in the package, so an
// earlier reader reaching the cell first shows up as a different message (it
// did while this test was being written — cleanup's "decode queued envelope"
// on the lowest entry).
//
// corrupt re-encodes that same entry's envelope with the msg_envelope_v2#5 tag,
// which collation accepts and the validator-side parse rejects: the CONTENT
// half of the same experiment, on the same cell.
func replayOnlyQueueRequest(t *testing.T, corrupt bool) (ShardRequest, tiedQueueEntry) {
	t.Helper()

	req, entries := queuedPredecessorRequest(t, 4)
	target := &entries[1]
	if corrupt {
		corruptQueueEnvelopeTag(t, target)
	}
	req = queuedPredecessorRequestWith(t, req, entries, entries[3:])

	return req, *target
}

// replayOnlyEnvelopeAndMessage returns the two cells below the target entry's
// leaf: the envelope, which is the leaf value's only reference, and the
// message, which is the envelope's. They are the complete set the entry parse
// dereferences, and therefore the complete set a storage fault can be injected
// into "below the leaf".
func replayOnlyEnvelopeAndMessage(t *testing.T, entry tiedQueueEntry) (cell.Hash, cell.Hash) {
	t.Helper()

	value, err := entry.enqueued.BeginParse()
	if err != nil {
		t.Fatal(err)
	}
	envelope, err := value.PeekRefCell()
	if err != nil {
		t.Fatal(err)
	}
	envelopeSlice, err := envelope.BeginParse()
	if err != nil {
		t.Fatal(err)
	}
	message, err := envelopeSlice.PeekRefCell()
	if err != nil {
		t.Fatal(err)
	}

	return envelope.HashKey(), message.HashKey()
}

// TestProcessedQueueTraceRefusesToShipOnAStorageFaultBelowTheLeaf is the
// critical half of the classification, and the half that was open: a storage
// fault raised strictly BELOW the leaf — inside the envelope reference the
// entry parse follows, or inside the message reference below that — must not be
// swallowed as a verdict on the entry's content.
//
// The two are told apart by which STEP raises them, not by what they say:
// materialiseSemanticQueueLeaf resolves both references through Cell.Prewarm,
// which does nothing but load, and hands the loaded cells to the parse. So the
// injected error below carries no marker at all — it is a bare
// "injected storage fault", the same shape a Pebble read error has — and it
// still cannot reach the swallow.
//
// The control arms are what make it an experiment rather than an assertion:
//
//   - the same fixture with the same lazified predecessor and no fault builds a
//     block with nothing recorded, so the refusal below is the fault and not
//     the store-shaped parent;
//   - the same entry corrupted instead of refused — the msg_envelope_v2 tag the
//     validator's parse rejects — still ships, with the reason recorded, which
//     is the behaviour the swallow exists for. If a change ever made the two
//     agree, one of these two arms fails whichever way it went.
func TestProcessedQueueTraceRefusesToShipOnAStorageFaultBelowTheLeaf(t *testing.T) {
	for _, tc := range []struct {
		name string
		// message selects the deeper of the two cells: the fault is then
		// raised one reference below the envelope, which is where the residual
		// this test closes used to live.
		message bool
		want    string
	}{
		{name: "envelope reference", want: "load outbound queue envelope"},
		{name: "message reference below the envelope", message: true, want: "load queued message"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, target := replayOnlyQueueRequest(t, false)
			envelopeHash, messageHash := replayOnlyEnvelopeAndMessage(t, target)

			lazifier := newFaultingLazifier()
			faulty := req
			faulty.Previous.State = lazifier.root(t, req.Previous.State)
			if tc.message {
				lazifier.fail[messageHash] = struct{}{}
			} else {
				lazifier.fail[envelopeHash] = struct{}{}
			}

			_, err := testBuilder().BuildShard(context.Background(), faulty)
			if err == nil {
				t.Fatal("a storage fault below a queue leaf shipped the block anyway")
			}
			if !errors.Is(err, errInjectedStorageFault) {
				t.Fatalf("build error = %v, want the injected storage fault", err)
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("build error = %v, want it raised by %q", err, tc.want)
			}
			if lazifier.refused == 0 {
				t.Fatal("the fixture never asked for the refused cell; the fault proves nothing")
			}
			t.Logf("collation refused to ship: %v", err)
		})
	}

	// Control 1: the same store-shaped predecessor with nothing refused.
	t.Run("no fault", func(t *testing.T) {
		req, _ := replayOnlyQueueRequest(t, false)
		lazifier := newFaultingLazifier()
		req.Previous.State = lazifier.root(t, req.Previous.State)

		candidate, err := testBuilder().BuildShard(context.Background(), req)
		if err != nil {
			t.Fatalf("a store-shaped predecessor with no fault failed to collate: %v", err)
		}
		if candidate.Stats.ProcessedScanTraceError != "" {
			t.Fatalf("the replay recorded a failure with nothing wrong: %s",
				candidate.Stats.ProcessedScanTraceError)
		}
		if len(candidate.CollatedData) <= 33 {
			t.Fatalf("collated data is %d bytes, i.e. the bare marker: capFullCollatedData is not in effect",
				len(candidate.CollatedData))
		}
		if lazifier.calls == 0 {
			t.Fatal("the predecessor was not store-shaped: the build read nothing through the loader")
		}
		t.Logf("no fault: block produced, %d loads through the store", lazifier.calls)
	})

	// Control 2: the same entry rejected by the validator-side parse instead of
	// refused by the store. This one must still ship.
	t.Run("content verdict on the same entry", func(t *testing.T) {
		req, _ := replayOnlyQueueRequest(t, true)
		lazifier := newFaultingLazifier()
		req.Previous.State = lazifier.root(t, req.Previous.State)

		candidate, err := testBuilder().BuildShard(context.Background(), req)
		if err != nil {
			t.Fatalf("a verdict on a predecessor entry killed the collation: %v", err)
		}
		if !strings.Contains(candidate.Stats.ProcessedScanTraceError, "v2 tag without emitted lt or metadata") {
			t.Fatalf("recorded reason does not name the entry: %q", candidate.Stats.ProcessedScanTraceError)
		}
		t.Logf("content verdict on the same entry still ships: %s", candidate.Stats.ProcessedScanTraceError)
	})
}

// walkSemanticQueuePrefixUnsplit is walkSemanticQueuePrefix as it stood before
// the load and the parse were separated: the leaf is handed straight to the
// entry parse, which dereferences the envelope and the message itself. It
// exists for one purpose — to count the loads that shape makes — and is never
// called by anything but the measurement below.
func walkSemanticQueuePrefixUnsplit(
	queue *tlb.OutMsgQueueAugDict,
	target msgpool.ShardIdent,
	bound semanticMessageBound,
) error {
	targetPrefix, err := semanticQueueTargetPrefix(target)
	if err != nil {
		return err
	}
	_, _, err = queue.TraverseExtra(func(keyPrefix *cell.Cell, extra, value *cell.Slice) (int, error) {
		direction, prefixErr := semanticQueuePrefixDirection(keyPrefix, targetPrefix)
		if prefixErr != nil || direction == 0 {
			return 0, prefixErr
		}
		if direction != 6 {
			if value != nil {
				return 0, errors.New("outbound queue leaf ends before the target prefix")
			}
			return direction, nil
		}
		minimum := extra.Copy()
		minimumLT, loadErr := minimum.LoadUInt(64)
		if loadErr != nil || minimum.BitsLeft() != 0 || minimum.RefsNum() != 0 {
			return 0, errors.New("invalid outbound queue augmentation")
		}
		if minimumLT > bound.lt {
			return 0, nil
		}
		if value == nil {
			return 6, nil
		}
		entryBound, boundErr := semanticQueueLeafBound(keyPrefix, minimumLT)
		if boundErr != nil {
			return 0, boundErr
		}
		if !entryBound.lessEqual(bound) {
			return 0, nil
		}
		if _, parseErr := parseSemanticNeighborQueueEntry(keyPrefix, value, extra); parseErr != nil {
			return 0, parseErr
		}
		return 0, nil
	})

	return err
}

// TestProcessedQueueTraceAddsNoLoadsOnEitherParentShape is the cost gate on the
// split. Separating the load from the parse must not turn into a second read of
// anything: Prewarm performs the load the parse would have performed on first
// touch, and the loaded cells are then handed to the parse, so the count is the
// same one.
//
// It is measured, not argued, and measured as a difference of loader calls over
// the same walk on a freshly store-shaped tree per arm — never as a share of a
// profile. Both parent shapes are covered because they answer different
// questions: a store-shaped parent is where loads exist at all, and a resident
// one is where Prewarm must be a no-op rather than a copy.
func TestProcessedQueueTraceAddsNoLoadsOnEitherParentShape(t *testing.T) {
	req, _ := replayOnlyQueueRequest(t, false)
	resident := loadPreviousShardState(t, req)
	target := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	bound := semanticMessageBound{lt: requestStartLT(t, req), hash: processedInfinityHash}

	// The store-shaped parent: one fresh lazifier per arm, so neither arm can
	// warm the other's cells.
	loads := func(t *testing.T, split bool) (int, int) {
		t.Helper()

		lazifier := newFaultingLazifier()
		info, err := parseNeighborQueueInfo(lazifier.root(t, resident.OutMsgQueueInfo))
		if err != nil {
			t.Fatal(err)
		}
		visited := 0
		if split {
			err = walkSemanticQueuePrefix(info.OutQueue, target, bound, func(semanticQueueEntry) error {
				visited++
				return nil
			})
		} else {
			err = walkSemanticQueuePrefixUnsplit(info.OutQueue, target, bound)
		}
		if err != nil {
			t.Fatalf("walk a store-shaped predecessor queue: %v", err)
		}
		return lazifier.calls, visited
	}

	before, _ := loads(t, false)
	after, visited := loads(t, true)
	if visited == 0 {
		t.Fatal("the walk visited no entry, so it measured nothing")
	}
	if before == 0 {
		t.Fatal("the store-shaped arm made no loads at all; the measurement is vacuous")
	}
	if after != before {
		t.Fatalf("store-shaped parent: %d loads with the split, %d without — the split added %d",
			after, before, after-before)
	}
	t.Logf("store-shaped parent: %d entries visited, %d loader calls with the split, %d without",
		visited, after, before)

	// The resident parent: nothing below the root is lazy, so Prewarm returns
	// its argument and the loader is never reached. A lazifier armed with the
	// same records and handed the resident tree would count any load that went
	// to the store behind the caller's back.
	armed := newFaultingLazifier()
	armed.index(t, resident.OutMsgQueueInfo)
	residentInfo, err := parseNeighborQueueInfo(resident.OutMsgQueueInfo)
	if err != nil {
		t.Fatal(err)
	}
	residentVisited := 0
	if err = walkSemanticQueuePrefix(residentInfo.OutQueue, target, bound,
		func(semanticQueueEntry) error {
			residentVisited++
			return nil
		}); err != nil {
		t.Fatalf("walk a resident predecessor queue: %v", err)
	}
	if residentVisited != visited {
		t.Fatalf("the resident arm visited %d entries and the store-shaped arm %d; "+
			"the two shapes are not the same walk", residentVisited, visited)
	}
	if armed.calls != 0 {
		t.Fatalf("a resident parent still read %d cells through a loader", armed.calls)
	}
	t.Logf("resident parent: %d entries visited, 0 loader calls", residentVisited)
}
