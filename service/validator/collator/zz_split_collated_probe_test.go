package collator

import (
	"context"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

// deepSplitFixture is a before-split parent whose outbound queue holds depth
// entries with sources spread over both children, and the request for the
// first block of the left child over it. Half of the queued messages carry
// their body in a separate cell, the way jetton wallets build theirs.
type deepSplitFixture struct {
	parent    PreviousBlock
	child     ShardRequest
	envelopes map[cell.Hash]struct{}
	roots     map[cell.Hash]struct{}
	// below holds every cell under a message root: bodies, StateInit cells and
	// their code. The reference split filter never opens any of them.
	below map[cell.Hash]struct{}
}

func newDeepSplitFixture(t *testing.T, depth int) deepSplitFixture {
	t.Helper()

	parentRequest := emptyCandidateRequest(t)
	baseState := predecessorTestState(t, parentRequest.Previous.State)
	fee := tlb.FromNanoTONU(100_000)
	fixture := deepSplitFixture{
		envelopes: make(map[cell.Hash]struct{}, depth),
		roots:     make(map[cell.Hash]struct{}, depth),
		below:     make(map[cell.Hash]struct{}, depth),
	}

	entries := make([]predecessorQueueEntry, 0, depth)
	for i := 0; i < depth; i++ {
		src := predecessorAddress(byte(i*37) | 0x01)
		dst := predecessorAddress(byte(i*91+13) | 0x01)
		lt := baseState.GenLT - uint64(depth+10) + uint64(i)
		var (
			message *msgpool.InternalMessage
			value   *cell.Cell
		)
		switch i % 4 {
		case 0, 2:
			message, value = queuedInternal(t, src, dst, lt, parentRequest.Header.GenUtime-1, fee, fee, 0,
				msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
		case 1:
			message, value = queuedInternalWithDistinctReferencedBody(t, src, dst, lt, parentRequest.Header.GenUtime-1, fee)
		default:
			message, value = queuedInternalWithDistinctReferencedBody(t, src, dst, lt, parentRequest.Header.GenUtime-1, fee)
			message, value = queuedInternalWithStateInitRef(t, message)
		}
		fixture.envelopes[message.EnvelopeCell.HashKey()] = struct{}{}
		fixture.roots[message.Root.HashKey()] = struct{}{}
		for r := 0; r < int(message.Root.RefsNum()); r++ {
			collectCells(message.Root.MustPeekRef(r), fixture.below)
		}
		entries = append(entries, predecessorQueueEntry{key: message.Key, value: value})
	}

	parentRequest.Previous.State = predecessorStateRoot(t, parentRequest.Previous.State, predecessorStateOptions{
		ident:     mustPredecessorIdent(t, parentRequest.Shard),
		seqno:     parentRequest.Previous.ID.SeqNo,
		vertSeqno: baseState.VertSeqno,
		genUtime:  baseState.GenUTime,
		genLT:     baseState.GenLT,
		minRefMC:  baseState.MinRefMCSeqno,
		accounts: activeContracts(t, parentRequest.Header.GenUtime,
			activeContract{address: predecessorAddress(0x11), code: externalAcceptCode(t), balance: 100_000_000_000},
			activeContract{address: predecessorAddress(0x91), code: externalAcceptCode(t), balance: 100_000_000_000}),
		fees:      5,
		masterRef: predecessorTestStats(t, &baseState).MasterRef,
		queue:     entries,
	})
	size := uint64(depth)
	parentRequest.Previous.OutQueueSize = &size
	parentRequest.Internals = &msgpool.Cut{More: true}
	parentSession := &parentRequest.Masterchain.Groups.Active[0]
	parentSession.Registered[0].FSM = groups.ShardFSM{
		Kind:     groups.ShardFSMSplit,
		UTime:    parentRequest.Header.GenUtime,
		Interval: 10,
	}
	parentRequest.BeforeSplit = true

	parentCandidate, err := testBuilder().BuildShard(context.Background(), parentRequest)
	if err != nil {
		t.Fatalf("build before-split parent: %v", err)
	}
	fixture.parent = previousFromCandidate(t, parentCandidate)

	child := parentRequest
	child.Shard = groups.ShardID{
		Workchain: parentRequest.Shard.Workchain,
		Shard:     mustPredecessorChild(t, parentRequest.Shard.Shard, true),
	}
	child.Previous = fixture.parent
	child.BeforeSplit = false
	child.Externals = nil
	child.Internals = &msgpool.Cut{More: true}
	child.StorageStats = parentCandidate.StorageStats
	child.Header.GenUtime++
	child.Header.GenUtimeMS = uint64(child.Header.GenUtime) * 1_000
	child.Masterchain.Config.capabilities |= capFullCollatedData
	child.Masterchain.Groups.Active = []groups.Session{{
		Shard:            child.Shard,
		CatchainSeqno:    parentSession.CatchainSeqno,
		ValidatorSetHash: parentSession.ValidatorSetHash,
		Genesis:          []ton.BlockIDExt{*fixture.parent.ID.Copy()},
		Registered: []groups.ShardDescription{{
			Shard:       parentRequest.Shard,
			Block:       *fixture.parent.ID.Copy(),
			BeforeSplit: true,
		}},
	}}
	parentNeighbor := fullCollatedTestNeighbor(t, fixture.parent)
	child.Neighbors = append([]Neighbor{parentNeighbor}, masterchainNeighbor(child)...)
	child.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return parentNeighbor.EndLT }
	fixture.child = child
	return fixture
}

// queuedInternalWithDistinctReferencedBody is queuedInternalWithReferencedBody
// with a body that differs per message, so the body cells do not collapse into
// one shared cell in a proof.
func queuedInternalWithDistinctReferencedBody(
	t *testing.T,
	src, dst *address.Address,
	createdLT uint64,
	createdAt uint32,
	fee tlb.Coins,
) (*msgpool.InternalMessage, *cell.Cell) {
	t.Helper()

	message, _ := queuedInternal(t, src, dst, createdLT, createdAt, fee, fee, 0,
		msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
	var internal tlb.InternalMessage
	if err := parseExact(&internal, message.Root); err != nil {
		t.Fatal(err)
	}
	internal.Body = cell.BeginCell().MustStoreUInt(0x0f8a7ea5, 32).MustStoreUInt(createdLT, 64).
		MustStoreSlice(dst.Data(), 256).EndCell()
	builder := cell.BeginCell()
	if err := tlb.StoreMessageWithLayout(builder, &tlb.Message{
		MsgType: tlb.MsgTypeInternal,
		Msg:     &internal,
	}, tlb.MessageLayout{BodyInRef: true}); err != nil {
		t.Fatal(err)
	}
	root := builder.EndCell()
	envelope := message.Envelope
	envelope.Msg = root
	envelopeCell, err := envelope.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: message.EnqueuedLT, Msg: envelopeCell}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	result := *message
	result.Key = msgpool.MakeQueueKey(message.Key.NextHop(), root.HashKey())
	result.EnvHash = envelopeCell.HashKey()
	result.Envelope = envelope
	result.EnvelopeCell = envelopeCell
	result.Root = root
	return &result, enqueued
}

func collectCells(c *cell.Cell, into map[cell.Hash]struct{}) {
	into[c.HashKey()] = struct{}{}
	for i := 0; i < int(c.RefsNum()); i++ {
		collectCells(c.MustPeekRef(i), into)
	}
}

type proofCensus struct {
	cells, pruned            int
	bytes                    int
	envelopes, roots, below  int
	envelopeBytes, rootBytes int
	belowBytes               int
	other, otherBytes        int
}

func cellWireBytes(c *cell.Cell) int {
	// descriptor(2) + data bytes + refs(1 byte each at these sizes), the way a
	// BOC without index pays for a cell.
	return 2 + int((c.BitsSize()+7)/8) + int(c.RefsNum())
}

func (f *deepSplitFixture) census(root *cell.Cell) proofCensus {
	var census proofCensus
	seen := make(map[cell.Hash]struct{})
	var walk func(c *cell.Cell)
	walk = func(c *cell.Cell) {
		// Level 0 names the resident cell even when a child of it is pruned in
		// the proof; a pruned branch is counted before it is categorised.
		hash := c.HashKeyAt(0)
		if _, ok := seen[hash]; ok {
			return
		}
		seen[hash] = struct{}{}
		census.cells++
		size := cellWireBytes(c)
		census.bytes += size
		if c.GetType() == cell.PrunedCellType {
			census.pruned++
			return
		}
		switch {
		case has(f.envelopes, hash):
			census.envelopes++
			census.envelopeBytes += size
		case has(f.roots, hash):
			census.roots++
			census.rootBytes += size
		case has(f.below, hash):
			census.below++
			census.belowBytes += size
		default:
			census.other++
			census.otherBytes += size
		}
		for i := 0; i < int(c.RefsNum()); i++ {
			walk(c.MustPeekRef(i))
		}
	}
	walk(root)
	return census
}

func has(set map[cell.Hash]struct{}, hash cell.Hash) bool {
	_, ok := set[hash]
	return ok
}

// TestZZPostSplitCollatedProbe builds the first block after a split over a
// deep parent queue and reports where the block and the collated data bytes
// go. Probe, not a gate.
func TestZZPostSplitCollatedProbe(t *testing.T) {
	if testing.Short() {
		t.Skip("probe")
	}
	for _, depth := range []int{1024, 2048, 8192} {
		t.Run(fmt.Sprint(depth), func(t *testing.T) {
			fixture := newDeepSplitFixture(t, depth)
			var assembly candidateAssemblyDurations
			fixture.child.assembly = &assembly
			candidate, err := testBuilder().BuildShard(context.Background(), fixture.child)
			if err != nil {
				t.Fatalf("build first child block: %v", err)
			}
			verifySerializedFullCollatedCandidate(t, fixture.child, candidate)

			block := candidateBlock(t, candidate)
			update := block.MustPeekRef(2)
			extra := block.MustPeekRef(3)
			t.Logf("DEPTH %d: tx=%d queue_after=%d cleaned=%d block=%dB state_update=%dB extra=%dB collated=%dB root=%x prepare_state=%.2fms cleanup=%.2fms closure=%.2fms",
				depth, candidate.Stats.Transactions, candidate.Stats.OutQueueSize, candidate.Stats.QueueCleaned,
				len(candidate.BlockBOC), len(update.ToBOCWithFlags(false)), len(extra.ToBOCWithFlags(false)),
				len(candidate.CollatedData), candidate.ID.RootHash[:4],
				float64(assembly.stages[CollationStagePrepareState].Microseconds())/1000,
				float64(assembly.stages[CollationStageCleanupOutQueue].Microseconds())/1000,
				float64(assembly.stages[CollationStageValidationClosure].Microseconds())/1000)

			roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
			if err != nil {
				t.Fatal(err)
			}
			parentState := fixture.parent.State.HashKey()
			for i, root := range roots {
				label := fmt.Sprintf("root[%d] type=%d", i, root.GetType())
				if root.GetType() == cell.MerkleProofCellType && root.MustPeekRef(0).HashKeyAt(0) == parentState {
					label += " (previous state proof)"
				}
				census := fixture.census(root)
				t.Logf("  %-40s boc=%7d cells=%6d pruned=%5d | envelopes=%d/%dB roots=%d/%dB below_roots=%d/%dB other=%d/%dB",
					label, len(root.ToBOCWithFlags(false)), census.cells, census.pruned,
					census.envelopes, census.envelopeBytes, census.roots, census.rootBytes,
					census.below, census.belowBytes, census.other, census.otherBytes)
			}
		})
	}
}

// TestPostSplitCollatedProofCarriesOnlyTheReferenceReads pins the collated data
// of the first block after a split to the reference's read set. Splitting reads
// every parent queue entry, so anything read per entry beyond the reference's
// filter_out_msg_queue — the envelope and the message root — and anything the
// proof builder keeps beyond what was read is paid once per queued message.
// Both happened: queuedCurrentPrefix opened the body root and the StateInit
// references through parseQueueEntry, and the proof builder kept every unread
// resident leaf whole. On the stand the two post-split blocks of 2026-09-03
// carried 2,498,713 and 2,461,902 bytes of collated data against 2,168,633 and
// 2,160,356 for the reference's sibling blocks over the same parents.
//
// The block is protocol-inherent and is not what this pins: the reference's
// sibling blocks were 451,971 and 159,775 bytes against our 453,400 and 159,543,
// all of it the rewritten queue skeleton in the state update. The bounds on it
// below only guard the order of magnitude, the way the masterchain regression
// of 2026-09-02 (816 kB blocks against 12.7 kB) would have been caught.
func TestPostSplitCollatedProofCarriesOnlyTheReferenceReads(t *testing.T) {
	const depth = 2048
	fixture := newDeepSplitFixture(t, depth)
	candidate, err := testBuilder().BuildShard(context.Background(), fixture.child)
	if err != nil {
		t.Fatalf("build first child block: %v", err)
	}
	verifySerializedFullCollatedCandidate(t, fixture.child, candidate)
	if candidate.Stats.Transactions != 0 || candidate.Stats.OutQueueSize != depth/2 {
		t.Fatalf("stats = %+v, want an empty block keeping half of the parent queue", candidate.Stats)
	}

	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	parentState := fixture.parent.State.HashKey()
	var previousStateProof *cell.Cell
	for _, root := range roots {
		if root.GetType() == cell.MerkleProofCellType && root.MustPeekRef(0).HashKeyAt(0) == parentState {
			previousStateProof = root
		}
	}
	if previousStateProof == nil {
		t.Fatal("collated data carries no previous-state proof")
	}
	census := fixture.census(previousStateProof)
	if census.envelopes != depth || census.roots != depth {
		t.Fatalf("proof holds %d envelopes and %d message roots, want %d of each: the split filter reads both",
			census.envelopes, census.roots, depth)
	}
	if census.below != 0 {
		t.Fatalf("proof holds %d cells (%d bytes) under message roots; the reference filter never opens one",
			census.below, census.belowBytes)
	}
	// Every referenced body is a boundary the message root exposes, so the
	// proof has at least that many pruned branches and never a whole body.
	if census.pruned < depth/2 {
		t.Fatalf("proof holds %d pruned branches, want at least the %d referenced bodies", census.pruned, depth/2)
	}

	update := candidateBlock(t, candidate).MustPeekRef(2)
	if got := len(update.ToBOCWithFlags(false)); got < 120_000 || got > 200_000 {
		t.Fatalf("state update is %d bytes, want the rewritten skeleton of a %d-entry queue (about 150 kB)", got, depth)
	}
	if got := len(candidate.CollatedData); got > 420_000 {
		t.Fatalf("collated data is %d bytes, want the parent queue's skeleton, envelopes and roots (about 390 kB)", got)
	}
}
