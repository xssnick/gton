package collator

import (
	"bytes"
	"context"
	"errors"
	"math"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// masterchainQueueMessage enqueues one own-queue message routed to the
// masterchain into the previous state and returns its pooled view. The queue
// entry lt may exceed the creation lt, the shape of a transit re-enqueue.
func masterchainQueueMessage(
	t *testing.T,
	req *ShardRequest,
	src, dst *address.Address,
	createdLT, queueLT uint64,
) *msgpool.InternalMessage {
	t.Helper()

	fee := tlb.FromNanoTONU(100_000)
	msg, _ := queuedInternal(t, src, dst, createdLT, req.Header.GenUtime-1,
		fee, fee, 0, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
	msg.QueueLT = queueLT
	enqueued, err := (tlb.EnqueuedMsg{EnqueuedLT: queueLT, Msg: msg.EnvelopeCell}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	req.Previous.State = stateWithQueueMessage(t, req.Previous.State, msg.Key, enqueued)
	return msg
}

func masterchainNeighbor(req ShardRequest, records ...tlb.ProcessedUptoRecord) []Neighbor {
	return []Neighbor{{
		Block: req.Masterchain.ID,
		Shard: msgpool.ShardIdent{
			Workchain: address.MasterchainID,
			Shard:     msgpool.ShardAll,
		},
		EndLT:     req.Masterchain.EndLT,
		Processed: records,
	}}
}

func TestBuildCandidateDequeuesNeighborCoveredEntries(t *testing.T) {
	req := emptyCandidateRequest(t)
	sourceA := address.NewAddress(0, 0, bytes.Repeat([]byte{0xb1}, 32))
	sourceB := address.NewAddress(0, 0, bytes.Repeat([]byte{0xb2}, 32))
	startLT := requestStartLT(t, req)

	// Three masterchain-bound entries in the canonical scan order: the
	// neighbor bound covers all of them, but the second one — a transit-style
	// entry whose queue lt exceeds its creation lt — fails the strict
	// shard-end-lt comparison, ending the scan. The third must stay queued
	// even though it is covered.
	shardEndLT := startLT + 20
	covered := masterchainQueueMessage(t, &req, sourceA,
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0xb3}, 32)), startLT-30, startLT-30)
	unresolved := masterchainQueueMessage(t, &req, sourceB,
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0xb4}, 32)), startLT-20, shardEndLT)
	tail := masterchainQueueMessage(t, &req, sourceA,
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0xb5}, 32)), startLT-10, startLT-10)
	queueSize := uint64(3)
	req.Previous.OutQueueSize = &queueSize

	req.Neighbors = masterchainNeighbor(req, tlb.ProcessedUptoRecord{
		ShardPrefix: 0x8000000000000000,
		MCSeqno:     req.Masterchain.ID.SeqNo,
		LastMsgLT:   startLT - 10,
		LastMsgHash: processedInfinityHash,
	})
	req.NeighborShardEndLT = func(mcSeqno uint32, workchain int32, prefix uint64) uint64 {
		if mcSeqno != req.Masterchain.ID.SeqNo || workchain != 0 {
			t.Fatalf("unexpected shard end lt query (%d, %d, %x)", mcSeqno, workchain, prefix)
		}
		return shardEndLT
	}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 0 || candidate.Stats.OutQueueSize != 2 {
		t.Fatalf("unexpected cleanup stats: %+v", candidate.Stats)
	}

	_, outMessages := candidateMessageDescriptors(t, candidate, req.Masterchain.Config.globalVersion)
	if count, err := outMessages.Count(); err != nil || count != 1 {
		t.Fatalf("out descriptor count = %d, err = %v", count, err)
	}
	out := descriptorByHash(t, outMessages.AugmentedDictionary, covered.Root.HashKey())
	if tag := out.MustLoadUInt(4); tag != 0b1101 {
		t.Fatalf("out descriptor tag = %04b, want msg_export_deq_short$1101", tag)
	}
	envHash, err := out.LoadSlice(256)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(envHash, covered.EnvHash[:]) {
		t.Fatal("msg_export_deq_short does not carry the queued envelope hash")
	}
	workchain, err := out.LoadInt(32)
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := out.LoadUInt(64)
	if err != nil {
		t.Fatal(err)
	}
	next := covered.Key.NextHop()
	if int32(workchain) != next.Workchain || prefix != next.Prefix {
		t.Fatalf("dequeue next hop = (%d, %x), want (%d, %x)", workchain, prefix, next.Workchain, next.Prefix)
	}
	importLT, err := out.LoadUInt(64)
	if err != nil {
		t.Fatal(err)
	}
	if importLT != req.Masterchain.EndLT {
		t.Fatalf("import block lt = %d, want the masterchain reference end lt %d", importLT, req.Masterchain.EndLT)
	}
	if out.BitsLeft() != 0 || out.RefsNum() != 0 {
		t.Fatal("msg_export_deq_short has trailing data")
	}

	queue := candidateQueueInfo(t, candidate)
	if count, err := queue.OutQueue.Count(); err != nil || count != 2 {
		t.Fatalf("outbound queue count = %d, err = %v", count, err)
	}
	for _, keep := range []*msgpool.InternalMessage{unresolved, tail} {
		keyCell := cell.BeginCell().MustStoreSlice(keep.Key[:], 352).EndCell()
		if _, err = queue.OutQueue.LoadValue(keyCell); err != nil {
			t.Fatalf("uncovered queue entry %x was removed: %v", keep.Key, err)
		}
	}

	// The emitted record must round-trip into the pool feed as a removal.
	destination := msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll}
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	if err = pool.Internals().ReconcileDestinations([]msgpool.ShardIdent{destination}); err != nil {
		t.Fatal(err)
	}
	deltas, err := pool.Internals().DeltasFromBlockRoot(
		destination,
		msgpool.SourceRef{Seqno: candidate.ID.SeqNo},
		candidateBlock(t, candidate),
		startLT,
	)
	if err != nil {
		t.Fatal(err)
	}
	delta := deltas[0].Delta
	if len(delta.Added) != 0 || delta.AddedTotal != 0 || delta.RemovedTotal != 1 {
		t.Fatalf("unexpected queue delta: %+v", delta)
	}
	if len(delta.RemovedEnvHashes) != 1 || delta.RemovedEnvHashes[0] != covered.EnvHash {
		t.Fatalf("removed envelope hashes = %x, want %x", delta.RemovedEnvHashes, covered.EnvHash)
	}
}

func TestBuildCandidateBoundsQueueCleanupWork(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xb6}, 32))
	startLT := requestStartLT(t, req)

	first := masterchainQueueMessage(t, &req, source,
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0xb7}, 32)), startLT-20, startLT-20)
	second := masterchainQueueMessage(t, &req, source,
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0xb8}, 32)), startLT-10, startLT-10)
	queueSize := uint64(2)
	req.Previous.OutQueueSize = &queueSize

	req.Neighbors = masterchainNeighbor(req, tlb.ProcessedUptoRecord{
		ShardPrefix: 0x8000000000000000,
		MCSeqno:     req.Masterchain.ID.SeqNo,
		LastMsgLT:   startLT - 10,
		LastMsgHash: processedInfinityHash,
	})
	req.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return math.MaxUint64 }
	// The first dequeue exhausts the normal byte class: the second covered
	// entry stays for a later block.
	req.Masterchain.Config.basechain.limits.bytes = limitThresholds{0, 1, 1 << 40, 1 << 41}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.OutQueueSize != 1 {
		t.Fatalf("unexpected budget stats: %+v", candidate.Stats)
	}
	_, outMessages := candidateMessageDescriptors(t, candidate, req.Masterchain.Config.globalVersion)
	if count, err := outMessages.Count(); err != nil || count != 1 {
		t.Fatalf("out descriptor count = %d, err = %v", count, err)
	}
	descriptorByHash(t, outMessages.AugmentedDictionary, first.Root.HashKey())

	queue := candidateQueueInfo(t, candidate)
	keyCell := cell.BeginCell().MustStoreSlice(second.Key[:], 352).EndCell()
	if _, err = queue.OutQueue.LoadValue(keyCell); err != nil {
		t.Fatalf("budget cutoff removed the second entry: %v", err)
	}
}

func TestBuildCandidateWithoutNeighborRecordsKeepsQueue(t *testing.T) {
	req := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xb9}, 32))
	msg := masterchainQueueMessage(t, &req, source,
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0xba}, 32)), requestStartLT(t, req)-10, requestStartLT(t, req)-10)
	queueSize := uint64(1)
	req.Previous.OutQueueSize = &queueSize

	plain, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	// A resolver without records must not change a single byte.
	req.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return math.MaxUint64 }
	withResolver, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(plain.BlockBOC, withResolver.BlockBOC) {
		t.Fatal("empty neighbor records changed the block")
	}
	if !bytes.Equal(plain.CollatedData, withResolver.CollatedData) {
		t.Fatal("empty neighbor records changed the collated data")
	}

	if plain.Stats.OutQueueSize != 1 {
		t.Fatalf("unexpected stats: %+v", plain.Stats)
	}
	_, outMessages := candidateMessageDescriptors(t, plain, req.Masterchain.Config.globalVersion)
	if count, err := outMessages.Count(); err != nil || count != 0 {
		t.Fatalf("out descriptor count = %d, err = %v", count, err)
	}
	queue := candidateQueueInfo(t, plain)
	keyCell := cell.BeginCell().MustStoreSlice(msg.Key[:], 352).EndCell()
	if _, err = queue.OutQueue.LoadValue(keyCell); err != nil {
		t.Fatalf("queue entry was removed without neighbor records: %v", err)
	}
}

func TestBuildCandidateValidatesQueueCleanupInputs(t *testing.T) {
	base := emptyCandidateRequest(t)
	source := address.NewAddress(0, 0, bytes.Repeat([]byte{0xbb}, 32))
	masterchainQueueMessage(t, &base, source,
		address.NewAddress(0, 0xff, bytes.Repeat([]byte{0xbc}, 32)), requestStartLT(t, base)-10, requestStartLT(t, base)-10)
	queueSize := uint64(1)
	base.Previous.OutQueueSize = &queueSize
	base.Neighbors = masterchainNeighbor(base, tlb.ProcessedUptoRecord{
		ShardPrefix: 0x8000000000000000,
		MCSeqno:     base.Masterchain.ID.SeqNo,
		LastMsgLT:   1,
		LastMsgHash: processedInfinityHash,
	})

	req := base
	if _, err := testBuilder().BuildShard(context.Background(), req); !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "shard end lt") {
		t.Fatalf("error = %v, want a missing shard end lt resolver error", err)
	}

	req = base
	req.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return math.MaxUint64 }
	req.Masterchain.Config.capabilities &^= capShortDequeue
	if _, err := testBuilder().BuildShard(context.Background(), req); !errors.Is(err, ErrUnsupported) {
		t.Fatalf("error = %v, want ErrUnsupported without the short dequeue capability", err)
	}
}
