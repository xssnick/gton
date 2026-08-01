package storage

import (
	"bytes"
	"context"
	"errors"
	"math/rand"
	"testing"

	"github.com/xssnick/tonutils-go/ton"
)

func TestLiveBlockCachePinsUnflushedArtifactsUntilFlush(t *testing.T) {
	cache := NewLiveBlockCache(1)
	first := testLiveBlockCacheBlockID(1)
	second := testLiveBlockCacheBlockID(2)
	firstData := []byte{0x11}
	firstProof := []byte{0x21}

	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:     first,
		BlockData: firstData,
		Proofs: []LiveBlockProofArtifact{{
			Kind: ServedProofBlock,
			Data: firstProof,
		}},
	}); err != nil {
		t.Fatalf("publish first block: %v", err)
	}
	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:     second,
		BlockData: []byte{0x12},
	}); err != nil {
		t.Fatalf("publish second block: %v", err)
	}

	gotData, err := cache.BlockData(context.Background(), first)
	if err != nil {
		t.Fatalf("unflushed block data was evicted: %v", err)
	}
	if !bytes.Equal(gotData, firstData) {
		t.Fatalf("block data = %x, want %x", gotData, firstData)
	}

	gotProof, err := cache.BlockProof(context.Background(), ServedProofBlock, first)
	if err != nil {
		t.Fatalf("unflushed block proof was evicted: %v", err)
	}
	if !bytes.Equal(gotProof, firstProof) {
		t.Fatalf("block proof = %x, want %x", gotProof, firstProof)
	}

	cache.MarkBlockFlushed(first)
	if _, err = cache.BlockData(context.Background(), first); !errors.Is(err, ErrNotFound) {
		t.Fatalf("flushed block data error = %v, want ErrNotFound", err)
	}
}

func TestLiveBlockCacheEvictsFlushedArtifacts(t *testing.T) {
	cache := NewLiveBlockCache(1)
	first := testLiveBlockCacheBlockID(1)
	second := testLiveBlockCacheBlockID(2)

	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:           first,
		BlockData:       []byte{0x11},
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish first block: %v", err)
	}
	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:           second,
		BlockData:       []byte{0x12},
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish second block: %v", err)
	}

	if _, err := cache.BlockData(context.Background(), first); !errors.Is(err, ErrNotFound) {
		t.Fatalf("old flushed block data error = %v, want ErrNotFound", err)
	}
	if _, err := cache.BlockData(context.Background(), second); err != nil {
		t.Fatalf("latest flushed block data: %v", err)
	}
}

func TestLiveBlockCacheEvictsByOriginalPublishOrderAfterFlushTransition(t *testing.T) {
	cache := NewLiveBlockCache(3)
	first := testLiveBlockCacheBlockID(1)
	second := testLiveBlockCacheBlockID(2)
	third := testLiveBlockCacheBlockID(3)
	fourth := testLiveBlockCacheBlockID(4)

	for _, artifacts := range []LiveBlockCacheArtifacts{
		{Block: first, BlockData: []byte{0x11}},
		{Block: second, BlockData: []byte{0x12}, ArtifactFlushed: true},
		{Block: third, BlockData: []byte{0x13}},
	} {
		if err := cache.PublishLiveBlockArtifacts(artifacts); err != nil {
			t.Fatalf("publish block %d: %v", artifacts.Block.SeqNo, err)
		}
	}

	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:           first,
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("mark first block flushed: %v", err)
	}
	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:     fourth,
		BlockData: []byte{0x14},
	}); err != nil {
		t.Fatalf("publish fourth block: %v", err)
	}

	if _, err := cache.BlockData(t.Context(), first); !errors.Is(err, ErrNotFound) {
		t.Fatalf("oldest evictable block error = %v, want ErrNotFound", err)
	}
	for _, block := range []ton.BlockIDExt{second, third, fourth} {
		if _, err := cache.BlockData(t.Context(), block); err != nil {
			t.Fatalf("retained block %d: %v", block.SeqNo, err)
		}
	}
}

func TestLiveBlockCacheDoesNotEvictBlockThatBecamePinned(t *testing.T) {
	cache := NewLiveBlockCache(3)
	first := testLiveBlockCacheBlockID(1)

	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{Block: first}); err != nil {
		t.Fatalf("publish first metadata: %v", err)
	}
	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:     first,
		BlockData: []byte{0x11},
	}); err != nil {
		t.Fatalf("pin first block: %v", err)
	}

	for seqno := uint32(2); seqno <= 4; seqno++ {
		block := testLiveBlockCacheBlockID(seqno)
		if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
			Block:     block,
			BlockData: []byte{byte(0x10 + seqno)},
		}); err != nil {
			t.Fatalf("publish pinned block %d: %v", seqno, err)
		}
	}

	for seqno := uint32(1); seqno <= 4; seqno++ {
		block := testLiveBlockCacheBlockID(seqno)
		if _, err := cache.BlockData(t.Context(), block); err != nil {
			t.Fatalf("pinned block %d was evicted: %v", seqno, err)
		}
	}

	evictable := testLiveBlockCacheBlockID(5)
	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:           evictable,
		BlockData:       []byte{0x15},
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish evictable block: %v", err)
	}
	if _, err := cache.BlockData(t.Context(), evictable); !errors.Is(err, ErrNotFound) {
		t.Fatalf("new evictable block error = %v, want ErrNotFound", err)
	}
}

func TestLiveBlockCacheFlushRemovesEvictionCandidate(t *testing.T) {
	cache := NewLiveBlockCache(2)
	first := testLiveBlockCacheBlockID(1)
	second := testLiveBlockCacheBlockID(2)
	third := testLiveBlockCacheBlockID(3)

	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:           first,
		BlockData:       []byte{0x11},
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish first block: %v", err)
	}
	cache.MarkBlockFlushed(first)

	for _, block := range []ton.BlockIDExt{second, third} {
		if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
			Block:     block,
			BlockData: []byte{byte(block.SeqNo)},
		}); err != nil {
			t.Fatalf("publish pinned block %d: %v", block.SeqNo, err)
		}
	}

	if len(cache.evictable) != 0 {
		t.Fatalf("evictable entries = %d, want 0", len(cache.evictable))
	}
	for _, block := range []ton.BlockIDExt{second, third} {
		if _, err := cache.BlockData(t.Context(), block); err != nil {
			t.Fatalf("pinned block %d was evicted: %v", block.SeqNo, err)
		}
	}
}

func TestLiveBlockCacheEvictionMatchesPublishOrderModel(t *testing.T) {
	const (
		maxBlocks = 8
		blockIDs  = 32
		steps     = 10000
	)

	cache := NewLiveBlockCache(maxBlocks)
	model := newTestLiveBlockCacheModel(maxBlocks)
	rnd := rand.New(rand.NewSource(1))

	for step := 0; step < steps; step++ {
		seqno := uint32(rnd.Intn(blockIDs) + 1)
		block := testLiveBlockCacheBlockID(seqno)
		if rnd.Intn(5) == 0 {
			cache.MarkBlockFlushed(block)
			model.remove(seqno)
		} else {
			variant := rnd.Intn(8)
			artifacts := LiveBlockCacheArtifacts{
				Block:           block,
				ArtifactFlushed: variant&4 != 0,
			}
			if variant&1 != 0 {
				artifacts.BlockData = []byte{byte(seqno)}
			}
			if variant&2 != 0 {
				artifacts.Proofs = []LiveBlockProofArtifact{{
					Kind: ServedProofBlock,
					Data: []byte{byte(seqno + 1)},
				}}
			}
			if err := cache.PublishLiveBlockArtifacts(artifacts); err != nil {
				t.Fatalf("step %d publish block %d: %v", step, seqno, err)
			}
			model.publish(seqno, artifacts)
		}

		assertLiveBlockCacheMatchesModel(t, step, cache, model)
	}
}

func TestLiveBlockCacheCachedBlockDataReportsArtifactFlushState(t *testing.T) {
	cache := NewLiveBlockCache(1)
	block := testLiveBlockCacheBlockID(1)
	data := []byte{0x11}

	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:     block,
		BlockData: data,
	}); err != nil {
		t.Fatalf("publish unflushed block: %v", err)
	}
	cached, err := cache.CachedBlockData(context.Background(), block)
	if err != nil {
		t.Fatalf("cached unflushed block data: %v", err)
	}
	if cached.ArtifactFlushed {
		t.Fatal("unflushed block data reported as flushed")
	}
	if !bytes.Equal(cached.Data, data) {
		t.Fatalf("cached block data = %x, want %x", cached.Data, data)
	}

	if err = cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:           block,
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish flushed marker: %v", err)
	}
	cached, err = cache.CachedBlockData(context.Background(), block)
	if err != nil {
		t.Fatalf("cached flushed block data: %v", err)
	}
	if !cached.ArtifactFlushed {
		t.Fatal("flushed block data reported as unflushed")
	}
}

func TestLiveBlockCacheRejectsInvalidBlockID(t *testing.T) {
	cache := NewLiveBlockCache(1)

	err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:     ton.BlockIDExt{},
		BlockData: []byte{0x11},
	})
	if !errors.Is(err, ErrInvalidBlockIDHashes) {
		t.Fatalf("publish invalid block error = %v, want ErrInvalidBlockIDHashes", err)
	}
}

func TestLiveBlockCacheRequiresFullBlockIDOnRead(t *testing.T) {
	cache := NewLiveBlockCache(2)
	block := testLiveBlockCacheBlockID(1)
	prev := testLiveBlockCacheBlockID(0)
	data := []byte{0x11}
	proof := []byte{0x21}

	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:     block,
		BlockData: data,
		Meta:      &BlockMeta{ID: block, PrevRefs: []ton.BlockIDExt{prev}},
		Proofs: []LiveBlockProofArtifact{{
			Kind: ServedProofBlock,
			Data: proof,
		}},
	}); err != nil {
		t.Fatalf("publish block: %v", err)
	}

	forgedBlock := block
	forgedBlock.FileHash = bytes.Repeat([]byte{0xff}, 32)
	if _, err := cache.BlockData(t.Context(), forgedBlock); !errors.Is(err, ErrNotFound) {
		t.Fatalf("forged block data error = %v, want ErrNotFound", err)
	}
	if _, err := cache.BlockProof(t.Context(), ServedProofBlock, forgedBlock); !errors.Is(err, ErrNotFound) {
		t.Fatalf("forged block proof error = %v, want ErrNotFound", err)
	}
	if _, err := cache.BlockFull(t.Context(), forgedBlock); !errors.Is(err, ErrNotFound) {
		t.Fatalf("forged full block error = %v, want ErrNotFound", err)
	}
	if cache.HasBlockData(forgedBlock) {
		t.Fatal("forged block reported cached data")
	}

	forgedPrev := prev
	forgedPrev.SeqNo++
	if _, err := cache.NextBlockFull(t.Context(), forgedPrev); !errors.Is(err, ErrNotFound) {
		t.Fatalf("next block for forged previous id error = %v, want ErrNotFound", err)
	}

	gotData, err := cache.BlockData(t.Context(), block)
	if err != nil {
		t.Fatalf("canonical block data: %v", err)
	}
	if !bytes.Equal(gotData, data) {
		t.Fatalf("canonical block data = %x, want %x", gotData, data)
	}
	if _, err = cache.NextBlockFull(t.Context(), prev); err != nil {
		t.Fatalf("next block for canonical previous id: %v", err)
	}
}

func testLiveBlockCacheBlockID(seqno uint32) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(0x4000000000000000),
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{byte(seqno)}, 32),
		FileHash:  bytes.Repeat([]byte{byte(seqno + 100)}, 32),
	}
}

type testLiveBlockCacheModelBlock struct {
	data    bool
	proof   bool
	flushed bool
}

func (b testLiveBlockCacheModelBlock) evictable() bool {
	return b.flushed || !b.data && !b.proof
}

type testLiveBlockCacheModel struct {
	max    int
	blocks map[uint32]testLiveBlockCacheModelBlock
	order  []uint32
}

func newTestLiveBlockCacheModel(max int) *testLiveBlockCacheModel {
	return &testLiveBlockCacheModel{
		max:    max,
		blocks: map[uint32]testLiveBlockCacheModelBlock{},
	}
}

func (m *testLiveBlockCacheModel) publish(seqno uint32, artifacts LiveBlockCacheArtifacts) {
	block, exists := m.blocks[seqno]
	if !exists {
		m.order = append(m.order, seqno)
	}
	block.data = block.data || len(artifacts.BlockData) > 0
	block.proof = block.proof || len(artifacts.Proofs) > 0
	block.flushed = block.flushed || artifacts.ArtifactFlushed
	m.blocks[seqno] = block
	m.evict()
}

func (m *testLiveBlockCacheModel) remove(seqno uint32) {
	if _, exists := m.blocks[seqno]; !exists {
		return
	}
	delete(m.blocks, seqno)
	for idx, current := range m.order {
		if current == seqno {
			m.order = append(m.order[:idx], m.order[idx+1:]...)
			return
		}
	}
}

func (m *testLiveBlockCacheModel) evict() {
	for len(m.blocks) > m.max {
		evicted := false
		for _, seqno := range m.order {
			if m.blocks[seqno].evictable() {
				m.remove(seqno)
				evicted = true
				break
			}
		}
		if !evicted {
			return
		}
	}
}

func assertLiveBlockCacheMatchesModel(t *testing.T, step int, cache *LiveBlockCache, model *testLiveBlockCacheModel) {
	t.Helper()

	if len(cache.entries) != len(model.blocks) {
		t.Fatalf("step %d cache entries = %d, want %d", step, len(cache.entries), len(model.blocks))
	}
	for seqno := uint32(1); seqno <= 32; seqno++ {
		key := BlockKey(testLiveBlockCacheBlockID(seqno))
		entry := cache.entries[key]
		_, exists := model.blocks[seqno]
		if (entry != nil) != exists {
			t.Fatalf("step %d block %d presence = %t, want %t", step, seqno, entry != nil, exists)
		}
		if entry == nil {
			continue
		}

		loaded, ok := cache.blocks.Load(key)
		if !ok {
			t.Fatalf("step %d block %d is missing from read map", step, seqno)
		}
		evictable := liveBlockCacheBlockEvictable(loaded.(*LiveBlockCacheBlock))
		if (entry.heapIndex >= 0) != evictable {
			t.Fatalf("step %d block %d heap membership = %t, want %t", step, seqno, entry.heapIndex >= 0, evictable)
		}
		if entry.heapIndex >= 0 && cache.evictable[entry.heapIndex] != entry {
			t.Fatalf("step %d block %d heap index points to another entry", step, seqno)
		}
	}
	for idx, entry := range cache.evictable {
		if entry.heapIndex != idx {
			t.Fatalf("step %d heap entry %d index = %d", step, idx, entry.heapIndex)
		}
		if idx > 0 {
			parent := (idx - 1) / 2
			if cache.evictable[parent].order > entry.order {
				t.Fatalf("step %d heap order is invalid at index %d", step, idx)
			}
		}
	}
}
