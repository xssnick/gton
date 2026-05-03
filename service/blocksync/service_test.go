package blocksync

import (
	"context"
	"errors"
	"flexserver/service/p2p"
	"fmt"
	"sync"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

const topShard = int64(-1 << 63)

func TestServiceFillsGapWithNextBlocks(t *testing.T) {
	source := &stubSource{events: make(chan p2p.BroadcastEvent, 4)}
	fetcher := newStubFetcher()

	block10 := testBlockID(-1, topShard, 10)
	block11 := testBlockID(-1, topShard, 11)
	block12 := testBlockID(-1, topShard, 12)

	fetcher.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}
	fetcher.next[blockKey(block10)] = &p2p.DownloadedBlock{ID: block11}
	fetcher.next[blockKey(block11)] = &p2p.DownloadedBlock{ID: block12}

	service := New(discardLogger(), source, fetcher)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block12}
	close(source.events)

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

	if len(fetcher.exactCalls) != 1 || !fetcher.exactCalls[0].Equals(&block10) {
		t.Fatalf("unexpected exact calls: %+v", fetcher.exactCalls)
	}
	if len(fetcher.nextCalls) != 2 || !fetcher.nextCalls[0].Equals(&block10) || !fetcher.nextCalls[1].Equals(&block11) {
		t.Fatalf("unexpected next calls: %+v", fetcher.nextCalls)
	}
}

func TestServiceIgnoresDuplicatesAndOlderBroadcasts(t *testing.T) {
	source := &stubSource{events: make(chan p2p.BroadcastEvent, 8)}
	fetcher := newStubFetcher()

	block10 := testBlockID(-1, topShard, 10)
	block11 := testBlockID(-1, topShard, 11)
	block09 := testBlockID(-1, topShard, 9)

	fetcher.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}
	fetcher.next[blockKey(block10)] = &p2p.DownloadedBlock{ID: block11}

	service := New(discardLogger(), source, fetcher)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block09}
	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block11}
	close(source.events)

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

	if len(fetcher.exactCalls) != 1 {
		t.Fatalf("expected 1 exact call, got %d", len(fetcher.exactCalls))
	}
	if len(fetcher.nextCalls) != 1 {
		t.Fatalf("expected 1 next call, got %d", len(fetcher.nextCalls))
	}
}

func TestServiceUsesDecodedBroadcastPayloadWhenPresent(t *testing.T) {
	source := &stubSource{events: make(chan p2p.BroadcastEvent, 4)}
	fetcher := newStubFetcher()

	block10 := testBlockID(-1, topShard, 10)
	block11 := testBlockID(-1, topShard, 11)

	service := New(discardLogger(), source, fetcher)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10, Downloaded: &p2p.DownloadedBlock{ID: block10}}
	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block11, Downloaded: &p2p.DownloadedBlock{ID: block11}}
	close(source.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 2 {
		t.Fatalf("expected 2 synced blocks, got %d", len(got))
	}
	if !got[0].Downloaded.ID.Equals(&block10) || !got[1].Downloaded.ID.Equals(&block11) {
		t.Fatalf("unexpected synced blocks: %+v", got)
	}
	if len(fetcher.exactCalls) != 0 || len(fetcher.nextCalls) != 0 {
		t.Fatalf("expected broadcast payload to avoid downloads, exact=%d next=%d", len(fetcher.exactCalls), len(fetcher.nextCalls))
	}
}

func TestServiceEmitsDecodedMasterchainBroadcastsWithoutCoalescing(t *testing.T) {
	source := &stubSource{events: make(chan p2p.BroadcastEvent, 4)}
	fetcher := newStubFetcher()

	block10 := testBlockID(-1, topShard, 10)
	block12 := testBlockID(-1, topShard, 12)

	service := New(discardLogger(), source, fetcher)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "tonNode.blockBroadcastCompressedV2", Block: block12, Downloaded: &p2p.DownloadedBlock{ID: block12}}
	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "tonNode.blockBroadcastCompressedV2", Block: block10, Downloaded: &p2p.DownloadedBlock{ID: block10}}
	close(source.events)

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
	if len(fetcher.exactCalls) != 0 || len(fetcher.nextCalls) != 0 {
		t.Fatalf("expected direct broadcasts to avoid downloads, exact=%d next=%d", len(fetcher.exactCalls), len(fetcher.nextCalls))
	}
}

func TestServiceDoesNotAdvanceAfterRejectedBlock(t *testing.T) {
	source := &stubSource{events: make(chan p2p.BroadcastEvent, 4)}
	fetcher := newStubFetcher()

	block10 := testBlockID(-1, topShard, 10)
	block12 := testBlockID(-1, topShard, 12)

	fetcher.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}
	fetcher.exact[blockKey(block12)] = &p2p.DownloadedBlock{ID: block12}

	service := New(discardLogger(), source, fetcher)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block12}
	close(source.events)

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
	if len(fetcher.nextCalls) != 0 {
		t.Fatalf("rejected block advanced catch-up state, next calls: %+v", fetcher.nextCalls)
	}
}

func TestServiceDropsFullBlockBroadcastWithoutDecodedPayload(t *testing.T) {
	source := &stubSource{events: make(chan p2p.BroadcastEvent, 2)}
	fetcher := newStubFetcher()

	block10 := testBlockID(-1, topShard, 10)
	fetcher.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}

	service := New(discardLogger(), source, fetcher)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "tonNode.blockBroadcast", Block: block10}
	close(source.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 0 {
		t.Fatalf("expected full block broadcast without payload to be dropped, got %+v", got)
	}
	if len(fetcher.exactCalls) != 0 || len(fetcher.nextCalls) != 0 {
		t.Fatalf("expected no download fallback, exact=%d next=%d", len(fetcher.exactCalls), len(fetcher.nextCalls))
	}
}

func TestServiceIgnoresNonMasterchainBroadcasts(t *testing.T) {
	source := &stubSource{events: make(chan p2p.BroadcastEvent, 4)}
	fetcher := newStubFetcher()

	base10 := testBlockID(0, topShard, 10)
	base11 := testBlockID(0, topShard, 11)
	fetcher.exact[blockKey(base10)] = &p2p.DownloadedBlock{ID: base10}
	fetcher.next[blockKey(base10)] = &p2p.DownloadedBlock{ID: base11}

	service := New(discardLogger(), source, fetcher)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	source.events <- p2p.BroadcastEvent{Overlay: "basechain", Kind: "tonNode.newShardBlockBroadcast", Block: base10}
	source.events <- p2p.BroadcastEvent{Overlay: "basechain", Kind: "tonNode.newShardBlockBroadcast", Block: base11}
	close(source.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 0 {
		t.Fatalf("expected non-masterchain broadcasts to be ignored, got %+v", got)
	}
	if len(fetcher.exactCalls) != 0 || len(fetcher.nextCalls) != 0 {
		t.Fatalf("expected no non-masterchain downloads, exact=%d next=%d", len(fetcher.exactCalls), len(fetcher.nextCalls))
	}
}

func TestServiceRetriesGapUntilRecovered(t *testing.T) {
	source := &stubSource{events: make(chan p2p.BroadcastEvent, 4)}
	fetcher := newStubFetcher()

	block10 := testBlockID(-1, topShard, 10)
	block11 := testBlockID(-1, topShard, 11)
	block12 := testBlockID(-1, topShard, 12)

	fetcher.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}
	fetcher.nextErrors[blockKey(block10)] = []error{
		errors.New("peer does not have the requested block"),
		errors.New("peer does not have the requested block"),
	}
	fetcher.next[blockKey(block10)] = &p2p.DownloadedBlock{ID: block11}
	fetcher.next[blockKey(block11)] = &p2p.DownloadedBlock{ID: block12}

	service := New(discardLogger(), source, fetcher)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block12}
	close(source.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 3 {
		t.Fatalf("expected 3 synced blocks, got %d", len(got))
	}
	if !got[1].Downloaded.ID.Equals(&block11) || !got[2].Downloaded.ID.Equals(&block12) {
		t.Fatalf("unexpected synced order: %+v", got)
	}
	if len(fetcher.nextCalls) != 4 {
		t.Fatalf("expected 4 next-block attempts, got %d", len(fetcher.nextCalls))
	}
	if !fetcher.nextCalls[0].Equals(&block10) || !fetcher.nextCalls[1].Equals(&block10) || !fetcher.nextCalls[2].Equals(&block10) || !fetcher.nextCalls[3].Equals(&block11) {
		t.Fatalf("unexpected next-block retry order: %+v", fetcher.nextCalls)
	}
}

func TestServiceCoalescesFutureBroadcastsWithoutSkippingBlocks(t *testing.T) {
	source := &stubSource{events: make(chan p2p.BroadcastEvent, 8)}
	fetcher := newStubFetcher()

	block10 := testBlockID(-1, topShard, 10)
	block11 := testBlockID(-1, topShard, 11)
	block12 := testBlockID(-1, topShard, 12)
	block13 := testBlockID(-1, topShard, 13)
	block14 := testBlockID(-1, topShard, 14)

	fetcher.exact[blockKey(block10)] = &p2p.DownloadedBlock{ID: block10}
	fetcher.next[blockKey(block10)] = &p2p.DownloadedBlock{ID: block11}
	fetcher.next[blockKey(block11)] = &p2p.DownloadedBlock{ID: block12}
	fetcher.next[blockKey(block12)] = &p2p.DownloadedBlock{ID: block13}
	fetcher.next[blockKey(block13)] = &p2p.DownloadedBlock{ID: block14}

	service := New(discardLogger(), source, fetcher)
	service.retryCount = 1

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		service.Run(ctx)
	}()

	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block10}
	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block12}
	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block13}
	source.events <- p2p.BroadcastEvent{Overlay: "masterchain", Kind: "block", Block: block14}
	close(source.events)

	got := collectSyncedBlocks(service.Blocks())
	wg.Wait()

	if len(got) != 5 {
		t.Fatalf("expected 5 synced blocks, got %d", len(got))
	}
	if !got[0].Downloaded.ID.Equals(&block10) || !got[1].Downloaded.ID.Equals(&block11) || !got[2].Downloaded.ID.Equals(&block12) || !got[3].Downloaded.ID.Equals(&block13) || !got[4].Downloaded.ID.Equals(&block14) {
		t.Fatalf("unexpected synced order: %+v", got)
	}
	if got[1].Trigger.Block.SeqNo != 14 || got[2].Trigger.Block.SeqNo != 14 || got[3].Trigger.Block.SeqNo != 14 || got[4].Trigger.Block.SeqNo != 14 {
		t.Fatalf("expected catch-up blocks to be coalesced to latest target, got triggers: %d %d %d %d", got[1].Trigger.Block.SeqNo, got[2].Trigger.Block.SeqNo, got[3].Trigger.Block.SeqNo, got[4].Trigger.Block.SeqNo)
	}
	if len(fetcher.nextCalls) != 4 {
		t.Fatalf("expected 4 next-block downloads, got %d", len(fetcher.nextCalls))
	}
}

func collectSyncedBlocks(ch <-chan SyncedBlock) []SyncedBlock {
	var out []SyncedBlock
	for block := range ch {
		out = append(out, block)
		block.Accept()
	}
	return out
}

type stubSource struct {
	events chan p2p.BroadcastEvent
}

func (s *stubSource) Events() <-chan p2p.BroadcastEvent {
	return s.events
}

type stubFetcher struct {
	exact       map[string]*p2p.DownloadedBlock
	next        map[string]*p2p.DownloadedBlock
	exactErrors map[string][]error
	nextErrors  map[string][]error

	exactCalls []ton.BlockIDExt
	nextCalls  []ton.BlockIDExt
}

func newStubFetcher() *stubFetcher {
	return &stubFetcher{
		exact:       map[string]*p2p.DownloadedBlock{},
		next:        map[string]*p2p.DownloadedBlock{},
		exactErrors: map[string][]error{},
		nextErrors:  map[string][]error{},
	}
}

func (s *stubFetcher) DownloadBlockFull(ctx context.Context, block ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	s.exactCalls = append(s.exactCalls, block)
	if errs := s.exactErrors[blockKey(block)]; len(errs) > 0 {
		err := errs[0]
		s.exactErrors[blockKey(block)] = errs[1:]
		return p2p.DownloadedBlock{}, err
	}
	if downloaded := s.exact[blockKey(block)]; downloaded != nil {
		return copyDownloadedBlock(*downloaded), nil
	}
	return p2p.DownloadedBlock{}, errors.New("unexpected exact block request")
}

func (s *stubFetcher) DownloadNextBlockFull(ctx context.Context, prev ton.BlockIDExt) (p2p.DownloadedBlock, error) {
	s.nextCalls = append(s.nextCalls, prev)
	if errs := s.nextErrors[blockKey(prev)]; len(errs) > 0 {
		err := errs[0]
		s.nextErrors[blockKey(prev)] = errs[1:]
		return p2p.DownloadedBlock{}, err
	}
	if downloaded := s.next[blockKey(prev)]; downloaded != nil {
		return copyDownloadedBlock(*downloaded), nil
	}
	return p2p.DownloadedBlock{}, errors.New("unexpected next block request")
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
