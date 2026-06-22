package blocksync

import (
	"context"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
)

const topShard = int64(-1 << 63)

func TestServiceFillsGapWithNextBlocks(t *testing.T) {
	node := newStubNode(4)

	block10 := testBlockID(-1, topShard, 10)
	block11 := testBlockID(-1, topShard, 11)
	block12 := testBlockID(-1, topShard, 12)

	node.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}
	node.next[blockKey(block10)] = &p2p.DownloadedBlock{ID: block11}
	node.next[blockKey(block11)] = &p2p.DownloadedBlock{ID: block12}

	service := New(discardLogger(), node)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block12}
	close(node.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 3 {
		t.Fatalf("expected 3 synced blocks, got %d", len(got))
	}

	if !got[0].Downloaded.ID.Equals(&block10) || got[0].CatchUp {
		t.Fatalf("unexpected first block: %+v", got[0])
	}
	if !got[1].Downloaded.ID.Equals(&block11) || !got[1].CatchUp {
		t.Fatalf("unexpected second block: %+v", got[1])
	}
	if !got[2].Downloaded.ID.Equals(&block12) || !got[2].CatchUp {
		t.Fatalf("unexpected third block: %+v", got[2])
	}

	if len(node.exactCalls) != 1 || !node.exactCalls[0].Equals(&block10) {
		t.Fatalf("unexpected exact calls: %+v", node.exactCalls)
	}
	if len(node.nextCalls) != 2 || !node.nextCalls[0].Equals(&block10) || !node.nextCalls[1].Equals(&block11) {
		t.Fatalf("unexpected next calls: %+v", node.nextCalls)
	}
}

func TestServiceIgnoresDuplicatesAndOlderBroadcasts(t *testing.T) {
	node := newStubNode(8)

	block10 := testBlockID(-1, topShard, 10)
	block11 := testBlockID(-1, topShard, 11)
	block09 := testBlockID(-1, topShard, 9)

	node.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}
	node.next[blockKey(block10)] = &p2p.DownloadedBlock{ID: block11}

	service := New(discardLogger(), node)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block09}
	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block11}
	close(node.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 2 {
		t.Fatalf("expected 2 synced blocks, got %d", len(got))
	}
	if !got[0].Downloaded.ID.Equals(&block10) {
		t.Fatalf("unexpected first block: %+v", got[0])
	}
	if !got[1].Downloaded.ID.Equals(&block11) {
		t.Fatalf("unexpected second block: %+v", got[1])
	}

	if len(node.exactCalls) != 1 {
		t.Fatalf("expected 1 exact call, got %d", len(node.exactCalls))
	}
	if len(node.nextCalls) != 1 {
		t.Fatalf("expected 1 next call, got %d", len(node.nextCalls))
	}
}

func TestServiceUsesDecodedBroadcastPayloadWhenPresent(t *testing.T) {
	node := newStubNode(4)

	block10 := testBlockID(-1, topShard, 10)
	block11 := testBlockID(-1, topShard, 11)

	service := New(discardLogger(), node)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10, Downloaded: &p2p.DownloadedBlock{ID: block10}}
	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block11, Downloaded: &p2p.DownloadedBlock{ID: block11}}
	close(node.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 2 {
		t.Fatalf("expected 2 synced blocks, got %d", len(got))
	}
	if !got[0].Downloaded.ID.Equals(&block10) || !got[1].Downloaded.ID.Equals(&block11) {
		t.Fatalf("unexpected synced blocks: %+v", got)
	}
	if len(node.exactCalls) != 0 || len(node.nextCalls) != 0 {
		t.Fatalf("expected broadcast payload to avoid downloads, exact=%d next=%d", len(node.exactCalls), len(node.nextCalls))
	}
}

func TestServiceEmitsDecodedMasterchainBroadcastsWithoutCoalescing(t *testing.T) {
	node := newStubNode(4)

	block10 := testBlockID(-1, topShard, 10)
	block12 := testBlockID(-1, topShard, 12)

	service := New(discardLogger(), node)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "tonNode.blockBroadcastCompressedV2", Block: block12, Downloaded: &p2p.DownloadedBlock{ID: block12}}
	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "tonNode.blockBroadcastCompressedV2", Block: block10, Downloaded: &p2p.DownloadedBlock{ID: block10}}
	close(node.events)

	var got []SyncedBlock
	for block := range service.Blocks() {
		got = append(got, block)
		if block.Downloaded.ID.Equals(&block12) {
			block.Reject()
			continue
		}
		block.Accept()
	}
	wg.Wait()

	if len(got) != 2 {
		t.Fatalf("expected 2 direct broadcasts, got %d", len(got))
	}
	if !got[0].Downloaded.ID.Equals(&block12) || !got[1].Downloaded.ID.Equals(&block10) {
		t.Fatalf("unexpected direct broadcast order: %+v", got)
	}
	if !got[0].Priority || !got[1].Priority {
		t.Fatalf("expected direct masterchain broadcasts to be priority: %+v", got)
	}
	if len(node.exactCalls) != 0 || len(node.nextCalls) != 0 {
		t.Fatalf("expected direct broadcasts to avoid downloads, exact=%d next=%d", len(node.exactCalls), len(node.nextCalls))
	}
}

func TestServiceDoesNotAdvanceAfterRejectedBlock(t *testing.T) {
	node := newStubNode(4)

	block10 := testBlockID(-1, topShard, 10)
	block12 := testBlockID(-1, topShard, 12)

	node.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}
	node.exact[blockKey(block12)] = &p2p.DownloadedBlock{ID: block12}

	service := New(discardLogger(), node)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block12}
	close(node.events)

	var got []SyncedBlock
	for block := range service.Blocks() {
		got = append(got, block)
		if len(got) == 1 {
			block.Reject()
			continue
		}
		block.Accept()
	}
	wg.Wait()

	if len(got) != 2 {
		t.Fatalf("expected 2 emitted blocks, got %d", len(got))
	}
	if !got[0].Downloaded.ID.Equals(&block10) || !got[1].Downloaded.ID.Equals(&block12) {
		t.Fatalf("unexpected synced blocks: %+v", got)
	}
	if len(node.nextCalls) != 0 {
		t.Fatalf("rejected block advanced catch-up state, next calls: %+v", node.nextCalls)
	}
}

func TestServiceDropsFullBlockBroadcastWithoutDecodedPayload(t *testing.T) {
	node := newStubNode(2)

	block10 := testBlockID(-1, topShard, 10)
	node.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}

	service := New(discardLogger(), node)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "tonNode.blockBroadcast", Block: block10}
	close(node.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 0 {
		t.Fatalf("expected full block broadcast without payload to be dropped, got %+v", got)
	}
	if len(node.exactCalls) != 0 || len(node.nextCalls) != 0 {
		t.Fatalf("expected no download fallback, exact=%d next=%d", len(node.exactCalls), len(node.nextCalls))
	}
}

func TestServiceIgnoresNonMasterchainBroadcasts(t *testing.T) {
	node := newStubNode(4)

	base10 := testBlockID(0, topShard, 10)
	base11 := testBlockID(0, topShard, 11)
	node.exact[blockKey(base10)] = &p2p.DownloadedBlock{ID: base10}
	node.next[blockKey(base10)] = &p2p.DownloadedBlock{ID: base11}

	service := New(discardLogger(), node)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	node.events <- p2p.BroadcastEvent{Overlay: "basechain", Kind: "tonNode.newShardBlockBroadcast", Block: base10}
	node.events <- p2p.BroadcastEvent{Overlay: "basechain", Kind: "tonNode.newShardBlockBroadcast", Block: base11}
	close(node.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 0 {
		t.Fatalf("expected non-masterchain broadcasts to be ignored, got %+v", got)
	}
	if len(node.exactCalls) != 0 || len(node.nextCalls) != 0 {
		t.Fatalf("expected no non-masterchain downloads, exact=%d next=%d", len(node.exactCalls), len(node.nextCalls))
	}
}

func TestServiceEmitsShardDescriptionBroadcasts(t *testing.T) {
	node := newStubNode(1)

	base10 := testBlockID(0, topShard, 10)
	service := New(discardLogger(), node)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	node.events <- p2p.BroadcastEvent{
		Overlay: "basechain",
		Kind:    "tonNode.newShardBlockBroadcast",
		Block:   base10,
		ShardDescription: &p2p.ShardBlockDescription{
			CatchainSeqno: 3,
			Data:          []byte{0xAA},
		},
	}
	close(node.events)

	var got []p2p.BroadcastEvent
	for ev := range service.ShardDescriptions() {
		got = append(got, ev)
	}
	wg.Wait()

	if len(got) != 1 {
		t.Fatalf("expected 1 shard description, got %d", len(got))
	}
	if !got[0].Block.Equals(&base10) || got[0].ShardDescription == nil || got[0].ShardDescription.CatchainSeqno != 3 {
		t.Fatalf("unexpected shard description event: %+v", got[0])
	}
	if len(node.exactCalls) != 0 || len(node.nextCalls) != 0 {
		t.Fatalf("expected no shard description downloads, exact=%d next=%d", len(node.exactCalls), len(node.nextCalls))
	}
}

func TestServiceRetriesGapUntilRecovered(t *testing.T) {
	node := newStubNode(4)

	block10 := testBlockID(-1, topShard, 10)
	block11 := testBlockID(-1, topShard, 11)
	block12 := testBlockID(-1, topShard, 12)

	node.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}
	node.nextErrors[blockKey(block10)] = []error{
		errors.New("peer does not have the requested block"),
		errors.New("peer does not have the requested block"),
	}
	node.next[blockKey(block10)] = &p2p.DownloadedBlock{ID: block11}
	node.next[blockKey(block11)] = &p2p.DownloadedBlock{ID: block12}

	service := New(discardLogger(), node)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block12}
	close(node.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 3 {
		t.Fatalf("expected 3 synced blocks, got %d", len(got))
	}
	if !got[1].Downloaded.ID.Equals(&block11) || !got[2].Downloaded.ID.Equals(&block12) {
		t.Fatalf("unexpected synced order: %+v", got)
	}
	if len(node.nextCalls) != 4 {
		t.Fatalf("expected 4 next-block attempts, got %d", len(node.nextCalls))
	}
	if !node.nextCalls[0].Equals(&block10) || !node.nextCalls[1].Equals(&block10) || !node.nextCalls[2].Equals(&block10) || !node.nextCalls[3].Equals(&block11) {
		t.Fatalf("unexpected next-block retry order: %+v", node.nextCalls)
	}
}

func TestServiceCoalescesFutureBroadcastsWithoutSkippingBlocks(t *testing.T) {
	node := newStubNode(8)

	block10 := testBlockID(-1, topShard, 10)
	block11 := testBlockID(-1, topShard, 11)
	block12 := testBlockID(-1, topShard, 12)
	block13 := testBlockID(-1, topShard, 13)
	block14 := testBlockID(-1, topShard, 14)

	node.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}
	node.next[blockKey(block10)] = &p2p.DownloadedBlock{ID: block11}
	node.next[blockKey(block11)] = &p2p.DownloadedBlock{ID: block12}
	node.next[blockKey(block12)] = &p2p.DownloadedBlock{ID: block13}
	node.next[blockKey(block13)] = &p2p.DownloadedBlock{ID: block14}

	service := New(discardLogger(), node)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block12}
	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block13}
	node.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block14}
	close(node.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 5 {
		t.Fatalf("expected 5 synced blocks, got %d", len(got))
	}
	if !got[0].Downloaded.ID.Equals(&block10) || !got[1].Downloaded.ID.Equals(&block11) || !got[2].Downloaded.ID.Equals(&block12) || !got[3].Downloaded.ID.Equals(&block13) || !got[4].Downloaded.ID.Equals(&block14) {
		t.Fatalf("unexpected synced order: %+v", got)
	}
	if got[1].Trigger.Block.SeqNo != 12 || got[2].Trigger.Block.SeqNo != 12 || got[3].Trigger.Block.SeqNo != 13 || got[4].Trigger.Block.SeqNo != 14 {
		t.Fatalf("expected catch-up blocks to use nearest queued targets, got triggers: %d %d %d %d", got[1].Trigger.Block.SeqNo, got[2].Trigger.Block.SeqNo, got[3].Trigger.Block.SeqNo, got[4].Trigger.Block.SeqNo)
	}
	if len(node.nextCalls) != 4 {
		t.Fatalf("expected 4 next-block downloads, got %d", len(node.nextCalls))
	}
}

func TestChainPromotesPendingBroadcastsByNearestSeqno(t *testing.T) {
	chain := &chain{notify: make(chan struct{}, 1)}
	block10 := testBlockID(-1, topShard, 10)
	block11 := testBlockID(-1, topShard, 11)
	block12 := testBlockID(-1, topShard, 12)
	block13 := testBlockID(-1, topShard, 13)

	if !chain.enqueue(p2p.BroadcastEvent{Block: block10}) {
		t.Fatal("enqueue block10 failed")
	}
	got := nextChainEvent(t, chain)
	if !got.Block.Equals(&block10) {
		t.Fatalf("first event = %s, want %s", got.BlockRef(), storage.FormatBlockRef(block10))
	}

	chain.enqueue(p2p.BroadcastEvent{Block: block13})
	chain.enqueue(p2p.BroadcastEvent{Block: block11})
	chain.enqueue(p2p.BroadcastEvent{Block: block12})

	chain.last = &block10
	chain.done()
	got = nextChainEvent(t, chain)
	if !got.Block.Equals(&block11) {
		t.Fatalf("second event = %s, want %s", got.BlockRef(), storage.FormatBlockRef(block11))
	}

	chain.last = &block11
	chain.done()
	got = nextChainEvent(t, chain)
	if !got.Block.Equals(&block12) {
		t.Fatalf("third event = %s, want %s", got.BlockRef(), storage.FormatBlockRef(block12))
	}

	chain.last = &block12
	chain.done()
	got = nextChainEvent(t, chain)
	if !got.Block.Equals(&block13) {
		t.Fatalf("fourth event = %s, want %s", got.BlockRef(), storage.FormatBlockRef(block13))
	}
}

func TestChainPendingBroadcastOverflowKeepsNearestSeqnos(t *testing.T) {
	chain := &chain{notify: make(chan struct{}, defaultPendingBroadcasts+2)}
	block10 := testBlockID(-1, topShard, 10)

	if !chain.enqueue(p2p.BroadcastEvent{Block: block10}) {
		t.Fatal("enqueue block10 failed")
	}
	_ = nextChainEvent(t, chain)

	for seqno := uint32(12); seqno < 12+defaultPendingBroadcasts; seqno++ {
		chain.enqueue(p2p.BroadcastEvent{Block: testBlockID(-1, topShard, seqno)})
	}
	near := testBlockID(-1, topShard, 11)
	far := testBlockID(-1, topShard, 12+defaultPendingBroadcasts)
	chain.enqueue(p2p.BroadcastEvent{Block: far})
	chain.enqueue(p2p.BroadcastEvent{Block: near})

	chain.last = &block10
	chain.done()
	got := nextChainEvent(t, chain)
	if !got.Block.Equals(&near) {
		t.Fatalf("overflow promoted %s, want nearest %s", got.BlockRef(), storage.FormatBlockRef(near))
	}

	if _, ok := chain.pending[far.SeqNo]; ok {
		t.Fatalf("far future block %s stayed queued after overflow", storage.FormatBlockRef(far))
	}
}

func TestChainWaitForNextBlockUsesPendingBroadcastDuringDownload(t *testing.T) {
	node := newBlockingNextNode()
	service := New(discardLogger(), node)
	service.retryCount = 1

	prev := testBlockID(-1, topShard, 10)
	next := testBlockID(-1, topShard, 11)
	target := testBlockID(-1, topShard, 13)
	downloaded := p2p.DownloadedBlock{
		ID:   next,
		Kind: "tonNode.blockBroadcast",
	}
	chain := &chain{
		service: service,
		key:     chainKey(prev),
		notify:  make(chan struct{}, 1),
		busy:    true,
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	done := make(chan nextBlockWaitTestResult, 1)
	go func() {
		block, trigger, ok := chain.waitForNextBlock(ctx, p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: target}, prev)
		done <- nextBlockWaitTestResult{block: block, trigger: trigger, ok: ok}
	}()

	select {
	case got := <-node.nextStarted:
		if !got.Equals(&prev) {
			t.Fatalf("download started from %s, want %s", storage.FormatBlockRef(got), storage.FormatBlockRef(prev))
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for next-block download to start")
	}

	chain.enqueue(p2p.BroadcastEvent{
		Overlay:    "masterchain",
		Kind:       "tonNode.blockBroadcast",
		Block:      next,
		Downloaded: &downloaded,
	})

	select {
	case res := <-done:
		if !res.ok {
			t.Fatal("wait for next block returned false")
		}
		if !res.block.ID.Equals(&next) {
			t.Fatalf("next block = %s, want %s", storage.FormatBlockRef(res.block.ID), storage.FormatBlockRef(next))
		}
		if !res.trigger.Block.Equals(&next) {
			t.Fatalf("trigger block = %s, want %s", res.trigger.BlockRef(), storage.FormatBlockRef(next))
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for pending broadcast")
	}
}

type nextBlockWaitTestResult struct {
	block   p2p.DownloadedBlock
	trigger p2p.BroadcastEvent
	ok      bool
}

func nextChainEvent(t *testing.T, chain *chain) p2p.BroadcastEvent {
	t.Helper()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	ev, ok := chain.next(ctx)
	if !ok {
		t.Fatal("chain did not return next event")
	}
	return ev
}

func collectSyncedBlocks(ch <-chan SyncedBlock) []SyncedBlock {
	var out []SyncedBlock
	for block := range ch {
		out = append(out, block)
		block.Accept()
	}
	return out
}

type stubNode struct {
	events chan p2p.BroadcastEvent

	exact       map[string]*p2p.DownloadedBlock
	next        map[string]*p2p.DownloadedBlock
	exactErrors map[string][]error
	nextErrors  map[string][]error

	exactCalls []ton.BlockIDExt
	nextCalls  []ton.BlockIDExt
}

type blockingNextNode struct {
	events       chan p2p.BroadcastEvent
	nextStarted  chan ton.BlockIDExt
	nextFinished chan ton.BlockIDExt
}

func newBlockingNextNode() *blockingNextNode {
	return &blockingNextNode{
		events:       make(chan p2p.BroadcastEvent),
		nextStarted:  make(chan ton.BlockIDExt, 1),
		nextFinished: make(chan ton.BlockIDExt, 1),
	}
}

func (n *blockingNextNode) Events() <-chan p2p.BroadcastEvent {
	return n.events
}

func (n *blockingNextNode) DownloadBlockFull(ctx context.Context, block ton.BlockIDExt) (*p2p.DownloadedBlock, error) {
	return nil, errors.New("unexpected exact block request")
}

func (n *blockingNextNode) DownloadNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*p2p.DownloadedBlock, error) {
	select {
	case n.nextStarted <- prev:
	default:
	}

	<-ctx.Done()
	n.nextFinished <- prev
	return nil, ctx.Err()
}

func newStubNode(events int) *stubNode {
	return &stubNode{
		events:      make(chan p2p.BroadcastEvent, events),
		exact:       map[string]*p2p.DownloadedBlock{},
		next:        map[string]*p2p.DownloadedBlock{},
		exactErrors: map[string][]error{},
		nextErrors:  map[string][]error{},
	}
}

func (s *stubNode) Events() <-chan p2p.BroadcastEvent {
	return s.events
}

func (s *stubNode) DownloadBlockFull(ctx context.Context, block ton.BlockIDExt) (*p2p.DownloadedBlock, error) {
	s.exactCalls = append(s.exactCalls, block)
	if errs := s.exactErrors[blockKey(block)]; len(errs) > 0 {
		err := errs[0]
		s.exactErrors[blockKey(block)] = errs[1:]
		return nil, err
	}
	if downloaded := s.exact[blockKey(block)]; downloaded != nil {
		block := copyDownloadedBlock(*downloaded)
		return &block, nil
	}
	return nil, errors.New("unexpected exact block request")
}

func (s *stubNode) DownloadNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (*p2p.DownloadedBlock, error) {
	s.nextCalls = append(s.nextCalls, prev)
	if errs := s.nextErrors[blockKey(prev)]; len(errs) > 0 {
		err := errs[0]
		s.nextErrors[blockKey(prev)] = errs[1:]
		return nil, err
	}
	if downloaded := s.next[blockKey(prev)]; downloaded != nil {
		block := copyDownloadedBlock(*downloaded)
		return &block, nil
	}
	return nil, errors.New("unexpected next block request")
}

func copyDownloadedBlock(block p2p.DownloadedBlock) p2p.DownloadedBlock {
	block.ID = ton.BlockIDExt{
		Workchain: block.ID.Workchain,
		Shard:     block.ID.Shard,
		SeqNo:     block.ID.SeqNo,
		RootHash:  append([]byte(nil), block.ID.RootHash...),
		FileHash:  append([]byte(nil), block.ID.FileHash...),
	}
	block.BlockBOC = append([]byte(nil), block.BlockBOC...)
	block.ProofBOC = append([]byte(nil), block.ProofBOC...)
	return block
}

func testBlockID(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  []byte{byte(seqno), 0x01},
		FileHash:  []byte{byte(seqno), 0x02},
	}
}

func blockKey(block ton.BlockIDExt) string {
	return chainKey(block) + fmt.Sprintf(":%d", block.SeqNo)
}
