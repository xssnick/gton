package collator

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// TestFullCollatedProcessedQueueScanVerifiesOnProofs is the regression for the
// third rejection of the same class this node has produced on a live network:
// blocks it collated itself were refused by its own validator with "augmented
// dict has special cells in tree structure", raised inside the inbound-queue
// check, on four scattered slots of one basechain session.
//
// As in both earlier instances, the block is not at fault — the control arm
// below collates the identical block without full collated data and verifies it
// against the resident predecessor tree, where it passes. What was at fault is
// the predecessor state proof, which did not cover a walk the validator makes.
// traceProcessedQueueValidationClosure is the fix, and its comment carries the
// reference citations.
//
// The two sides disagreed by construction, and the disagreement was only ever
// hidden by accident:
//
//   - The validator scans each source queue with walkSemanticQueuePrefix, which
//     descends into every subtree whose augmentation minimum is at or below the
//     candidate's processed bound — and that comparison is on lt alone
//     (semantic_queue_walk.go), so an entry that merely TIES the bound's lt is
//     descended to and only then skipped on the message-hash tie-break. Opening
//     it is what the proof has to cover.
//   - Collation never walked its own predecessor queue that way. Neighbour views
//     for the predecessor are deliberately untraced (loadExpectedNeighbors), so
//     the queue half of the predecessor proof was whatever collation happened to
//     touch: the delete paths of the entries it imported, plus whatever the
//     frontier cleanup's stream expanded.
//
// The fixture therefore builds the one shape where those two sets come apart:
// a queue with a minimum-lt group, which is all cleanup's stream expands, and a
// second group tied on a higher lt, of which the block imports exactly the
// first. Everything left in that second group is inside the walk and was outside
// the proof.
//
// The tie is the discriminator, and the spread control at the end says so: give
// the same entries distinct logical times and the walk stops at each subtree
// top, which the delete rebuild already opened, and the same fixture verified
// even before the fix. Ties are not exotic, but the quantity that ties is the
// EMITTED lt — the enclosed message's created_lt unless the envelope carries an
// explicit emitted_lt — and not EnqueuedMsg's enqueued_lt, which is the
// enqueueing block's start_lt and never enters the augmentation
// (Aug_OutMsgQueue::eval_leaf, block-parse.cpp:2142-2147). Every account
// emitting its first deferred message in one block is emitted at that block's
// start_lt+1 (collator.cpp:4605-4611), so those tie; the fixture below makes
// the tie directly, on created_lt.
//
// What this test does NOT establish, and neither did the two tests it is
// modelled on:
//
//   - It is a synthetic queue, not a replay of the four rejected candidates.
//     Those were never obtained, so that production hit this route into the walk
//     rather than another one is argued from the lt-allocation model, not shown.
//   - It says nothing about a candidate whose queue is genuinely malformed. The
//     special-cells message is a mask: on a short proof it replaces whatever the
//     real verdict would have been, which is why the control arm exists and why
//     a green run here is a statement about the proof, not about the block.
//   - It exercises one unsplit shard. After a split and after a merge the whole
//     predecessor queue is scanned before collation starts, so the walk is
//     covered there for a different reason, which this fixture cannot show.
func TestFullCollatedProcessedQueueScanVerifiesOnProofs(t *testing.T) {
	for _, tc := range []struct {
		name string
		// interleaved gives both groups one next-hop prefix, so their keys
		// intermix in the trie by message hash the way real traffic does.
		// Separate prefixes are the same defect with a cleaner trie.
		interleaved bool
		// payload loads every tied entry with each class of sub-structure a
		// queue entry may carry, so a fix that covered the trie nodes but not
		// what hangs off a leaf would fail here. See assertTiedQueueShape.
		payload bool
	}{
		{name: "separate prefixes"},
		{name: "interleaved prefixes", interleaved: true},
		{name: "entries carrying every payload class", interleaved: true, payload: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			fixture := buildTiedQueueFixture(t, tiedQueueShape{
				minimumCount: 4,
				tiedCount:    92,
				importTied:   1,
				interleaved:  tc.interleaved,
				payload:      tc.payload,
			})

			// The same block, collated without the capability and verified
			// against the resident predecessor tree. It has to pass, or the
			// fixture is exercising a malformed block rather than a short proof.
			if !bytes.Equal(fixture.plain.ID.RootHash, fixture.candidate.ID.RootHash) {
				t.Fatal("full collated data changed the block; the control no longer covers the same block")
			}
			if err := VerifyShardCandidate(context.Background(), fixture.control); err != nil {
				t.Fatalf("the block itself does not verify against the resident predecessor: %v", err)
			}

			if err := VerifyShardCandidate(context.Background(), fixture.proofBacked); err != nil {
				t.Fatalf("verify candidate on its own proofs: %v", err)
			}
		})
	}

	// The negative control: distinct logical times, everything else identical.
	// If this ever fails the defect is wider than the tie and the comment above
	// is wrong.
	t.Run("distinct logical times", func(t *testing.T) {
		fixture := buildTiedQueueFixture(t, tiedQueueShape{
			minimumCount: 4,
			tiedCount:    92,
			importTied:   1,
			spread:       true,
		})
		if err := VerifyShardCandidate(context.Background(), fixture.proofBacked); err != nil {
			t.Fatalf("verify candidate whose surviving queue entries have distinct lts: %v", err)
		}
	})
}

type tiedQueueShape struct {
	// minimumCount is the size of the lowest-lt group. It exists so the tied
	// group is not the minimum: cleanup's stream expands every fork tying the
	// minimum rank before it can emit its first leaf, and that expansion would
	// otherwise put the whole tied group into the proof and hide the defect.
	minimumCount int
	// tiedCount is the size of the group sharing the block's processed bound.
	tiedCount int
	// importTied is how many of that group the block imports; the rest survive
	// strictly above the bound on the message-hash tie-break.
	importTied  int
	interleaved bool
	// spread gives the second group distinct lts instead of one shared lt.
	spread bool
	// payload fills the tied group's messages with every reference-bearing
	// class an EnqueuedMsg can hold.
	payload bool
}

type tiedQueueFixture struct {
	candidate   *Candidate
	plain       *Candidate
	control     ShardVerificationRequest
	proofBacked ShardVerificationRequest
}

func buildTiedQueueFixture(t *testing.T, shape tiedQueueShape) tiedQueueFixture {
	t.Helper()

	req := emptyCandidateRequest(t)
	previous := loadPreviousShardState(t, req)
	minimumLT := previous.GenLT - 400
	tiedLT := previous.GenLT - 300

	tiedTag := byte(0xff)
	if shape.interleaved {
		tiedTag = 0x00
	}
	minimum := queuedTiedGroup(t, 0x00, shape.minimumCount, minimumLT, false)
	var tied []tiedQueueEntry
	if shape.spread {
		for i := range shape.tiedCount {
			tied = append(tied, queuedTiedGroup(t, tiedTag, 1, tiedLT+uint64(i), shape.payload)...)
		}
	} else {
		tied = queuedTiedGroup(t, tiedTag, shape.tiedCount, tiedLT, shape.payload)
	}

	entries := make([]tiedQueueEntry, 0, len(minimum)+len(tied))
	entries = append(entries, minimum...)
	entries = append(entries, tied...)
	req.Previous.State = previousStateWithQueueEntries(t, req.Previous.State, entries)
	queueSize := uint64(len(entries))
	req.Previous.OutQueueSize = &queueSize
	// One idle block over this state gives the candidate a predecessor with a
	// real block root, which full collated data binds its state proof to.
	req.Internals = &msgpool.Cut{}
	req = advanceCandidateRequestApplyingUpdate(t, req)

	imported := make([]*msgpool.InternalMessage, 0, shape.minimumCount+shape.importTied)
	for _, entry := range minimum {
		imported = append(imported, entry.message)
	}
	for i := range shape.importTied {
		imported = append(imported, tied[i].message)
	}
	// More says the pool was truncated, which is what makes stopping mid-queue
	// the ordinary thing it is in production rather than a drained-queue claim.
	req.Internals = &msgpool.Cut{Messages: imported, More: true}
	attachFullCollatedTestNeighbors(t, &req)

	plainReq := req
	config := *req.Masterchain.Config
	plainReq.Masterchain.Config = &config
	plainReq.Masterchain.Config.capabilities &^= capFullCollatedData
	plain, err := testBuilder().BuildShard(context.Background(), plainReq)
	if err != nil {
		t.Fatalf("collate without full collated data: %v", err)
	}

	req.Masterchain.Config.capabilities |= capFullCollatedData
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("collate candidate: %v", err)
	}
	assertTiedQueueShape(t, req, candidate, shape)

	residentState := loadPreviousShardState(t, plainReq)
	control := shardVerificationRequest(plainReq, plain)
	control.NeighborShardEndLT = plainReq.NeighborShardEndLT
	control.Semantics = NewSemanticVerifier(tvm.NewTVM())
	control.Neighbors = append([]Neighbor(nil), plainReq.Neighbors...)
	for i := range control.Neighbors {
		if control.Neighbors[i].Block.Workchain != address.MasterchainID {
			control.Neighbors[i].OutMsgQueueInfo = residentState.OutMsgQueueInfo
		}
	}

	proofBacked := shardVerificationRequest(req, candidate)
	proofBacked.NeighborShardEndLT = req.NeighborShardEndLT
	proofBacked.Semantics = NewSemanticVerifier(tvm.NewTVM())
	proofBacked.Previous.State = narrowedStateRoot(t, req.Previous.State)
	proofBacked.Neighbors = collatedNeighborQueues(t, req, candidate)

	return tiedQueueFixture{candidate: candidate, plain: plain, control: control, proofBacked: proofBacked}
}

// assertTiedQueueShape fails unless the candidate really carries the shape the
// defect needs. These guard against a fixture that passes while proving
// nothing: a block that imported the whole queue deletes every entry and drags
// the trie into the proof, a block that imported none claims no processed bound
// for the walk to run against, and a cleanup that dequeued anything has
// expanded the stream past its first frontier.
func assertTiedQueueShape(t *testing.T, req ShardRequest, candidate *Candidate, shape tiedQueueShape) {
	t.Helper()

	wantImported := uint32(shape.minimumCount + shape.importTied)
	if candidate.Stats.InternalsImported != wantImported {
		t.Fatalf("fixture imported %d internals, want %d", candidate.Stats.InternalsImported, wantImported)
	}
	if candidate.Stats.QueueCleaned != 0 {
		t.Fatalf("fixture cleaned %d queue entries, want 0", candidate.Stats.QueueCleaned)
	}
	survivors := candidate.Stats.OutQueueSize - uint64(candidate.Stats.EnqueuedMessages)
	if want := uint64(shape.tiedCount - shape.importTied); survivors != want {
		t.Fatalf("fixture left %d predecessor queue entries behind, want %d", survivors, want)
	}

	// The predecessor proof must actually prune part of the queue: a proof that
	// carried the whole trie would verify no matter what the walk does.
	data, err := verifyCollatedData(candidate, nil, req.Header.GenUtime)
	if err != nil {
		t.Fatalf("decode collated data: %v", err)
	}
	if !data.full {
		t.Fatal("candidate carries no full collated data")
	}
	stateRoot, err := collatedStateRoot(&data, req.Previous.ID)
	if err != nil {
		t.Fatalf("load collated predecessor state: %v", err)
	}
	var proven tlb.ShardStateUnsplit
	if err = parseProofExact(&proven, stateRoot); err != nil {
		t.Fatalf("decode collated predecessor state: %v", err)
	}
	queue, err := parseNeighborQueueInfo(proven.OutMsgQueueInfo)
	if err != nil {
		t.Fatalf("decode collated predecessor queue: %v", err)
	}
	if pruned := prunedCellCount(queue.OutQueue.AsCell()); pruned == 0 {
		t.Fatal("the predecessor queue proof prunes nothing; the fixture cannot show a short proof")
	}

	// And the walk has to have a bound to run against at all.
	var built tlb.ShardStateUnsplit
	if err = parseExact(&built, candidate.State); err != nil {
		t.Fatalf("decode candidate state: %v", err)
	}
	builtQueue, err := parseNeighborQueueInfo(built.OutMsgQueueInfo)
	if err != nil {
		t.Fatalf("decode candidate queue: %v", err)
	}
	target := msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}
	records, err := tlb.LoadProcessedUptoRecords(builtQueue.ProcInfo, target.Shard)
	if err != nil {
		t.Fatalf("decode candidate processed records: %v", err)
	}
	if len(records) == 0 {
		t.Fatal("candidate claims no processed bound")
	}

	if shape.payload {
		assertSurvivingEntryCarriesEveryPayloadClass(t, built)
	}
}

// assertSurvivingEntryCarriesEveryPayloadClass pins that a queue entry the walk
// visits and the block does not delete really holds each reference-bearing
// class an EnqueuedMsg can carry, so the payload arm cannot decay into a repeat
// of the plain one.
//
// The classes are the complete set, read off the TL-B: the envelope's message
// reference, the extra-currency HashmapE 32, a by-reference StateInit with its
// code, data and library HashmapE 256 roots, and a by-reference body.
// msg_envelope_v2's emitted_lt and MsgMetadata carry no reference at all — which
// is why traceQueueEntryClosure can insist an envelope holds exactly one.
func assertSurvivingEntryCarriesEveryPayloadClass(t *testing.T, state tlb.ShardStateUnsplit) {
	t.Helper()

	queue, err := parseNeighborQueueInfo(state.OutMsgQueueInfo)
	if err != nil {
		t.Fatalf("decode candidate queue: %v", err)
	}
	loaded := 0
	err = queue.OutQueue.ForEachValueExtra(func(value, _ *cell.Slice) (bool, error) {
		var enqueued tlb.EnqueuedMsg
		if err := loadExactSlice(&enqueued, value); err != nil {
			t.Fatalf("decode queue entry: %v", err)
		}
		var envelope tlb.MsgEnvelope
		if err := parseExact(&envelope, enqueued.Msg); err != nil {
			t.Fatalf("decode queued envelope: %v", err)
		}
		var internal tlb.InternalMessage
		if err := parseExact(&internal, envelope.Msg); err != nil {
			t.Fatalf("decode queued message: %v", err)
		}
		if internal.ExtraCurrencies == nil || internal.ExtraCurrencies.IsEmpty() {
			return true, nil
		}
		if internal.StateInit == nil {
			t.Fatal("a payload entry carries extra currencies but no StateInit")
		}
		if internal.StateInit.Code == nil || internal.StateInit.Data == nil {
			t.Fatal("a payload entry's StateInit has no code or data root")
		}
		if internal.StateInit.Lib == nil || internal.StateInit.Lib.IsEmpty() {
			t.Fatal("a payload entry's StateInit carries no library dictionary")
		}
		if internal.Body == nil || internal.Body.RefsNum() == 0 {
			t.Fatal("a payload entry's body reference is empty")
		}
		loaded++
		return true, nil
	})
	if err != nil {
		t.Fatalf("walk candidate queue entries: %v", err)
	}
	if loaded == 0 {
		t.Fatal("no surviving queue entry carries the payload classes; the payload arm proves nothing")
	}
}

type tiedQueueEntry struct {
	message  *msgpool.InternalMessage
	enqueued *cell.Cell
}

// queuedTiedGroup builds count queued internal messages sharing one enqueued lt
// and one next-hop prefix, in canonical (lt, message hash) order. The shared
// next-hop prefix is what makes sorting by queue key the same as sorting by
// message hash, which is the order the collator must import them in.
func queuedTiedGroup(t *testing.T, tag byte, count int, lt uint64, payload bool) []tiedQueueEntry {
	t.Helper()

	entries := make([]tiedQueueEntry, 0, count)
	for i := range count {
		var sourceID, destinationID [32]byte
		sourceID[0], sourceID[29], sourceID[30], sourceID[31] = 0x20, tag, byte(i>>8), byte(i)
		destinationID[0], destinationID[29], destinationID[30], destinationID[31] = tag, tag, byte(i>>8), byte(i)
		message, enqueued := queuedTiedMessage(
			t,
			address.NewAddress(0, 0, sourceID[:]),
			address.NewAddress(0, 0, destinationID[:]),
			lt,
			payload,
		)
		entries = append(entries, tiedQueueEntry{message: message, enqueued: enqueued})
	}
	sort.Slice(entries, func(i, j int) bool {
		return bytes.Compare(entries[i].message.Key[:], entries[j].message.Key[:]) < 0
	})
	return entries
}

// queuedTiedMessage builds one queued internal message. Without payload it is
// the plain shape queuedInternalWithReferencedBody produces; with it, the
// message additionally carries every reference-bearing class listed on
// assertSurvivingEntryCarriesEveryPayloadClass.
func queuedTiedMessage(
	t *testing.T,
	src, dst *address.Address,
	lt uint64,
	payload bool,
) (*msgpool.InternalMessage, *cell.Cell) {
	t.Helper()

	fee := tlb.FromNanoTONU(100_000)
	if !payload {
		return queuedInternalWithReferencedBody(
			t, src, dst, lt, 1_900_000_000, fee, fee, 0,
			msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
		)
	}

	libraries := cell.NewDict(256)
	for i := range 2 {
		key := cell.BeginCell().MustStoreUInt(uint64(0xA0+i), 256).EndCell()
		// SimpleLib: public:Bool root:^Cell.
		value := cell.BeginCell().
			MustStoreBoolBit(true).
			MustStoreRef(cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell()).
			EndCell()
		if err := libraries.Set(key, value); err != nil {
			t.Fatalf("set library %d: %v", i, err)
		}
	}
	extra := extraCurrencyDict(t, map[uint64]*cell.Cell{
		100: canonicalExtraCurrencyValue(),
		200: canonicalExtraCurrencyValue(),
	}).AsDict(32)

	internal := &tlb.InternalMessage{
		IHRDisabled:     true,
		SrcAddr:         src,
		DstAddr:         dst,
		Amount:          tlb.FromNanoTONU(1_000_000_000),
		ExtraCurrencies: extra,
		IHRFee:          fee,
		FwdFee:          fee,
		CreatedLT:       lt,
		CreatedAt:       1_900_000_000,
		StateInit: &tlb.StateInit{
			Code: cell.BeginCell().MustStoreUInt(0xC0DE, 32).EndCell(),
			Data: cell.BeginCell().MustStoreUInt(0xDA7A, 32).EndCell(),
			Lib:  libraries,
		},
		Body: cell.BeginCell().MustStoreRef(cell.BeginCell().MustStoreUInt(0xB0D1, 32).EndCell()).EndCell(),
	}
	builder := cell.BeginCell()
	if err := tlb.StoreMessageWithLayout(builder, &tlb.Message{
		MsgType: tlb.MsgTypeInternal,
		Msg:     internal,
	}, tlb.MessageLayout{StateInitInRef: true, BodyInRef: true}); err != nil {
		t.Fatalf("serialize payload message: %v", err)
	}
	message := builder.EndCell()

	envelopeCell, err := (tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		FwdFeeRemaining: fee,
		Msg:             message,
	}).ToCell()
	if err != nil {
		t.Fatalf("serialize payload envelope: %v", err)
	}
	var envelope tlb.MsgEnvelope
	if err = parseExact(&envelope, envelopeCell); err != nil {
		t.Fatalf("decode payload envelope: %v", err)
	}
	srcPrefix, err := accountPrefixFromAddress(src)
	if err != nil {
		t.Fatal(err)
	}
	dstPrefix, err := accountPrefixFromAddress(dst)
	if err != nil {
		t.Fatal(err)
	}
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: lt, Msg: envelopeCell}).ToCell()
	if err != nil {
		t.Fatalf("serialize payload queue entry: %v", err)
	}

	return &msgpool.InternalMessage{
		Key:          msgpool.MakeQueueKey(msgpool.InterpolatePrefix(srcPrefix, dstPrefix, 96), message.HashKey()),
		EnqueuedLT:   lt,
		QueueLT:      lt,
		EnvHash:      envelopeCell.HashKey(),
		Envelope:     envelope,
		EnvelopeCell: envelopeCell,
		Root:         message,
		Source:       msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
		SourceSeqno:  1,
	}, enqueued
}

func previousStateWithQueueEntries(t *testing.T, root *cell.Cell, entries []tiedQueueEntry) *cell.Cell {
	t.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	for i, entry := range entries {
		key := cell.BeginCell().MustStoreSlice(entry.message.Key[:], 352).EndCell()
		inserted, err := queue.OutQueue.SetWithMode(key, entry.enqueued, cell.DictSetModeAdd)
		if err != nil || !inserted {
			t.Fatalf("insert outbound queue entry %d: inserted=%t err=%v", i, inserted, err)
		}
	}
	var err error
	if state.OutMsgQueueInfo, err = queue.ToCell(); err != nil {
		t.Fatal(err)
	}
	if root, err = tlb.ToCell(&state); err != nil {
		t.Fatal(err)
	}
	return root
}

func prunedCellCount(root *cell.Cell) int {
	seen := make(map[cell.Hash]struct{})
	pruned := 0
	var walk func(*cell.Cell)
	walk = func(c *cell.Cell) {
		if c == nil {
			return
		}
		hash := c.HashKey()
		if _, known := seen[hash]; known {
			return
		}
		seen[hash] = struct{}{}
		if c.IsSpecial() {
			pruned++
			return
		}
		slice, err := c.BeginParse()
		if err != nil {
			return
		}
		for refs := int(slice.RefsNum()); refs > 0; refs-- {
			ref, refErr := slice.LoadRefCell()
			if refErr != nil {
				return
			}
			walk(ref)
		}
	}
	walk(root)
	return pruned
}

// unparseableEnvelopeRequest re-encodes one predecessor queue entry with the
// msg_envelope_v2#5 tag and neither emitted_lt nor metadata. The key is
// untouched — it is the message hash and the next hop, neither of which the
// envelope encoding changes — so the entry sits exactly where it did in the
// trie, strictly below the bound the block goes on to claim.
//
// Collation's own parseQueueEntry accepts the shape. The validator-side
// parseSemanticNeighborQueueEntry does not: requireCanonicalEnvelopeTag
// (semantic_verify_descriptors.go:318-323) rejects it, because
// MsgEnvelope::unpack rejects it. That asymmetry is the whole point — the
// replay is the only place in collation that applies the strict parse, to data
// this node inherited rather than authored.
func unparseableEnvelopeRequest(t *testing.T) ShardRequest {
	t.Helper()

	req, entries := queuedPredecessorRequest(t, 4)
	corrupt := &entries[0]
	corruptQueueEnvelopeTag(t, corrupt)
	req = queuedPredecessorRequestWith(t, req, entries, entries[1:])

	return req
}

// queuedPredecessorRequest lays count queued internal messages at distinct
// logical times into a fresh predecessor, lowest first, and returns them
// unattached: the caller corrupts what it wants to corrupt and then chooses
// which of them the block imports through queuedPredecessorRequestWith.
func queuedPredecessorRequest(t *testing.T, count int) (ShardRequest, []tiedQueueEntry) {
	t.Helper()

	req := emptyCandidateRequest(t)
	previous := loadPreviousShardState(t, req)
	baseLT := previous.GenLT - 400

	var entries []tiedQueueEntry
	for i := range count {
		entries = append(entries, queuedTiedGroup(t, 0x00, 1, baseLT+uint64(i), false)...)
	}

	return req, entries
}

// corruptQueueEnvelopeTag re-encodes an entry with the msg_envelope_v2#5 tag
// and neither emitted_lt nor metadata. The key is untouched — it is the message
// hash and the next hop, neither of which the envelope encoding changes — so
// the entry sits exactly where it did in the trie.
//
// Collation's own parseQueueEntry accepts the shape. The validator-side
// parseSemanticNeighborQueueEntry does not: requireCanonicalEnvelopeTag
// (semantic_verify_descriptors.go) rejects it, because MsgEnvelope::unpack
// rejects it. That asymmetry is the whole point — the replay is the only place
// in collation that applies the strict parse, to data this node inherited
// rather than authored.
func corruptQueueEnvelopeTag(t *testing.T, entry *tiedQueueEntry) {
	t.Helper()

	var envelope tlb.MsgEnvelope
	if err := parseExact(&envelope, entry.message.EnvelopeCell); err != nil {
		t.Fatal(err)
	}
	envelope.V2 = true
	envelope.EmittedLT = nil
	envelope.Metadata = nil
	broken, err := envelope.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	if entry.enqueued, err = (tlb.EnqueuedMsg{
		EnqueuedLT: entry.message.EnqueuedLT,
		Msg:        broken,
	}).ToCell(); err != nil {
		t.Fatal(err)
	}
}

// queuedPredecessorRequestWith seats the entries in the predecessor queue,
// mints one idle block over that state so full collated data has a predecessor
// block to bind its proof to, and offers imported to the collation.
//
// Everything below the block's claimed bound and outside imported is inside the
// walk and outside what collation itself parses — with one exception the
// callers rely on: cleanup's own-shard stream always parses the LOWEST-ranked
// entry before its frontier declines it, so the lowest entry is never
// replay-only.
func queuedPredecessorRequestWith(
	t *testing.T,
	req ShardRequest,
	entries []tiedQueueEntry,
	imported []tiedQueueEntry,
) ShardRequest {
	t.Helper()

	req.Previous.State = previousStateWithQueueEntries(t, req.Previous.State, entries)
	queueSize := uint64(len(entries))
	req.Previous.OutQueueSize = &queueSize
	req.Internals = &msgpool.Cut{}
	req = advanceCandidateRequestApplyingUpdate(t, req)

	messages := make([]*msgpool.InternalMessage, 0, len(imported))
	for _, entry := range imported {
		messages = append(messages, entry.message)
	}
	req.Internals = &msgpool.Cut{Messages: messages, More: true}
	attachFullCollatedTestNeighbors(t, &req)
	req.Masterchain.Config.capabilities |= capFullCollatedData

	return req
}

// TestProcessedQueueTraceRefusesAnUnparseablePredecessorEntry pins acceptance
// parity: the replay uses the same parse as validation, so a content verdict is
// already proof that the candidate cannot be accepted. It must stop before the
// remaining serialization/signing/publication work.
func TestProcessedQueueTraceRefusesAnUnparseablePredecessorEntry(t *testing.T) {
	req := unparseableEnvelopeRequest(t)
	_, err := testBuilder().BuildShard(context.Background(), req)
	if err == nil {
		t.Fatal("an unparseable predecessor queue entry shipped the block")
	}
	if !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "v2 tag without emitted lt or metadata") {
		t.Fatalf("build error = %v, want the validator-side content verdict", err)
	}
	t.Logf("collation stopped on the validator-side verdict: %v", err)
}

// TestProcessedQueueTraceHonoursCancellation covers the one bound this walk is
// allowed to have. It must not take a truncating budget — a short proof
// reproduces the incident by construction — but a cancelled collation ships
// nothing, so ending there truncates nothing that could ever reach a peer. The
// walk is linear in the predecessor's own-prefix queue with no cap, so without
// this an abandoned slot keeps spending the next slot's budget.
func TestProcessedQueueTraceHonoursCancellation(t *testing.T) {
	req := staleOwnQueueRequest(t, 6)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	c, err := testBuilder().prepare(ctx, req)
	if err != nil {
		t.Fatal(err)
	}
	// Stand in for what import would have claimed: a bound above every entry
	// in the predecessor queue, so the walk has the whole prefix to cover.
	c.processedClaimed = true
	c.processedClaim = semanticMessageBound{
		lt:   requestStartLT(t, req),
		hash: processedInfinityHash,
	}
	if err = c.traceProcessedQueueValidationClosure(); err != nil {
		t.Fatalf("a live context must let the walk finish: %v", err)
	}

	cancel()
	err = c.traceProcessedQueueValidationClosure()
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled walk returned %v, want context.Canceled", err)
	}

	// Cancellation is still returned bare even when the same walk would otherwise
	// end in a semantic verdict.
	verdictCtx, cancelVerdict := context.WithCancel(context.Background())
	defer cancelVerdict()
	verdictReq := unparseableEnvelopeRequest(t)
	verdict, err := testBuilder().prepare(verdictCtx, verdictReq)
	if err != nil {
		t.Fatal(err)
	}
	verdict.processedClaimed = true
	verdict.processedClaim = semanticMessageBound{
		lt:   requestStartLT(t, verdictReq),
		hash: processedInfinityHash,
	}
	// Control: with a live context the malformed entry is the returned cause.
	if err = verdict.traceProcessedQueueValidationClosure(); err == nil ||
		!strings.Contains(err.Error(), "v2 tag without emitted lt or metadata") {
		t.Fatalf("control replay error = %v, want the content verdict", err)
	}

	cancelVerdict()
	err = verdict.traceProcessedQueueValidationClosure()
	if err != context.Canceled { //nolint:errorlint // identity is the assertion
		t.Fatalf("a cancelled walk that also met a verdict returned %v, want a bare context.Canceled", err)
	}
}

// prunedPredecessorQueue returns the predecessor outbound queue with everything
// below the OutMsgQueueInfo root replaced by pruned branches. Walking it fails
// the way the live incident failed — TraverseExtra cannot descend a special
// cell — and, decisively for the test below, it fails WITHOUT reaching any
// entry, so the failure is not a verdict on queue content.
func prunedPredecessorQueue(t *testing.T, req ShardRequest) *tlb.OutMsgQueueAugDict {
	t.Helper()

	state := loadPreviousShardState(t, req)
	proof, err := state.OutMsgQueueInfo.CreateHashUsageProof(func(cell.Hash) bool { return false })
	if err != nil {
		t.Fatalf("build pruned queue proof: %v", err)
	}
	narrow, err := unwrapCollatedProof(proof)
	if err != nil {
		t.Fatalf("unwrap pruned queue proof: %v", err)
	}
	info, err := parseNeighborQueueInfo(narrow)
	if err != nil {
		t.Fatalf("decode pruned queue: %v", err)
	}

	return info.OutQueue
}

// TestProcessedQueueTraceRefusesToShipOnANonVerdictFailure covers the storage /
// structural side of the same rule. It must preserve the original cause while
// refusing to ship.
//
// The failure staged here is the incident's own error, raised by TraverseExtra
// on a special cell it must parse to descend, and it is reached before any
// entry: no leaf is opened, so no parse verdict can exist. The candidate's
// configuration is irrelevant to the classification and this drives the closure
// directly, which is also what keeps the assertion on the boundary rather than
// on whatever a full build happens to do around it.
func TestProcessedQueueTraceRefusesToShipOnANonVerdictFailure(t *testing.T) {
	req := staleOwnQueueRequest(t, 4)
	c, err := testBuilder().prepare(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	c.processedClaimed = true
	c.processedClaim = semanticMessageBound{
		lt:   requestStartLT(t, req),
		hash: processedInfinityHash,
	}

	// Control: over the real predecessor queue the same claim replays cleanly,
	// so the failure below is the pruning and nothing else.
	if err = c.traceProcessedQueueValidationClosure(); err != nil {
		t.Fatalf("control replay over the resident queue: %v", err)
	}
	if c.stats.ProcessedScanTraceError != "" {
		t.Fatalf("control replay recorded a failure: %s", c.stats.ProcessedScanTraceError)
	}

	c.oldOutQueue = prunedPredecessorQueue(t, req)
	err = c.traceProcessedQueueValidationClosure()
	if err == nil {
		t.Fatal("a walk that could not open a queue node shipped the block anyway")
	}
	if !strings.Contains(err.Error(), "special cells") {
		t.Fatalf("replay error = %v, want the pruned-branch failure", err)
	}
	if c.stats.ProcessedScanTraceError != "" {
		t.Fatalf("a non-verdict failure was recorded as a shipped short proof: %s",
			c.stats.ProcessedScanTraceError)
	}
	t.Logf("collation refused to ship a knowingly short proof: %v", err)
}
