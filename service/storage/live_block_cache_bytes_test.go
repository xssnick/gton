package storage

import (
	"bytes"
	"math/rand"
	"testing"
)

func TestLiveBlockCacheBoundsFlushedBytes(t *testing.T) {
	cache := NewLiveBlockCache(DefaultLiveBlockCacheMaxBlocks, DefaultLiveBlockCacheMaxBytes)
	data := make([]byte, 16<<20)
	for seqno := uint32(1); seqno <= 64; seqno++ {
		if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
			Block:           testLiveBlockCacheBlockID(seqno),
			BlockData:       data,
			ArtifactFlushed: true,
		}); err != nil {
			t.Fatalf("publish block %d: %v", seqno, err)
		}
	}

	retained := int64(0)
	for seqno := uint32(1); seqno <= 64; seqno++ {
		if cache.HasBlockData(testLiveBlockCacheBlockID(seqno)) {
			retained += int64(len(data))
		}
	}
	if retained > DefaultLiveBlockCacheMaxBytes {
		t.Fatalf("retained flushed bytes = %d, want <= %d", retained, DefaultLiveBlockCacheMaxBytes)
	}
	if retained != cache.flushedBytes {
		t.Fatalf("flushed bytes = %d, want %d", cache.flushedBytes, retained)
	}
	if !cache.HasBlockData(testLiveBlockCacheBlockID(64)) {
		t.Fatal("newest flushed block was evicted")
	}
}

func TestLiveBlockCacheByteBoundKeepsTransientProofs(t *testing.T) {
	cache := NewLiveBlockCache(DefaultLiveBlockCacheMaxBlocks, DefaultLiveBlockCacheMaxBytes)
	transient := testLiveBlockCacheBlockID(1)
	data := make([]byte, 16<<20)

	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:     transient,
		BlockData: data,
		Proofs:    []LiveBlockProofArtifact{{Kind: ServedProofBlockLink, Data: []byte{0x21}}},
		Transient: true,
	}); err != nil {
		t.Fatalf("publish transient block: %v", err)
	}
	for seqno := uint32(2); seqno <= 65; seqno++ {
		if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
			Block:           testLiveBlockCacheBlockID(seqno),
			BlockData:       data,
			ArtifactFlushed: true,
		}); err != nil {
			t.Fatalf("publish flushed block %d: %v", seqno, err)
		}
	}

	if _, err := cache.BlockProof(t.Context(), ServedProofBlockLink, transient); err != nil {
		t.Fatalf("transient proof link was pushed out by flushed bytes: %v", err)
	}
	if cache.HasBlockData(testLiveBlockCacheBlockID(2)) {
		t.Fatal("oldest flushed block was retained over the byte bound")
	}
	if cache.flushedBytes > DefaultLiveBlockCacheMaxBytes {
		t.Fatalf("flushed bytes = %d, want <= %d", cache.flushedBytes, DefaultLiveBlockCacheMaxBytes)
	}
}

func TestLiveBlockCacheByteBoundSkipsPinnedArtifacts(t *testing.T) {
	cache := NewLiveBlockCache(8, 4)
	pinned := testLiveBlockCacheBlockID(1)
	flushed := testLiveBlockCacheBlockID(2)

	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:     pinned,
		BlockData: bytes.Repeat([]byte{0x11}, 8),
	}); err != nil {
		t.Fatalf("publish pinned block: %v", err)
	}
	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:           flushed,
		BlockData:       bytes.Repeat([]byte{0x12}, 4),
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("publish flushed block: %v", err)
	}
	if !cache.HasBlockData(pinned) || !cache.HasBlockData(flushed) {
		t.Fatal("pinned bytes pushed out a flushed block within the byte bound")
	}
	if cache.flushedBytes != 4 {
		t.Fatalf("flushed bytes = %d, want 4", cache.flushedBytes)
	}

	if err := cache.PublishLiveBlockArtifacts(LiveBlockCacheArtifacts{
		Block:           pinned,
		ArtifactFlushed: true,
	}); err != nil {
		t.Fatalf("mark pinned block flushed: %v", err)
	}
	if cache.HasBlockData(pinned) {
		t.Fatal("oldest flushed block was retained over the byte bound")
	}
	if !cache.HasBlockData(flushed) {
		t.Fatal("newer flushed block was evicted within the byte bound")
	}
	if cache.flushedBytes != 4 {
		t.Fatalf("flushed bytes after eviction = %d, want 4", cache.flushedBytes)
	}

	cache.MarkBlockFlushed(flushed)
	if cache.flushedBytes != 0 {
		t.Fatalf("flushed bytes after flush = %d, want 0", cache.flushedBytes)
	}
}

func TestLiveBlockCacheByteEvictionMatchesPublishOrderModel(t *testing.T) {
	const (
		maxBlocks = 8
		maxBytes  = 24
		blockIDs  = 32
		steps     = 10000
	)

	cache := NewLiveBlockCache(maxBlocks, maxBytes)
	model := &testLiveBlockCacheBytesModel{
		maxBlocks: maxBlocks,
		maxBytes:  maxBytes,
		blocks:    map[uint32]testLiveBlockCacheBytesBlock{},
	}
	rnd := rand.New(rand.NewSource(1))

	for step := 0; step < steps; step++ {
		seqno := uint32(rnd.Intn(blockIDs) + 1)
		block := testLiveBlockCacheBlockID(seqno)
		if rnd.Intn(5) == 0 {
			cache.MarkBlockFlushed(block)
			model.remove(seqno)
		} else {
			artifacts := LiveBlockCacheArtifacts{
				Block:           block,
				ArtifactFlushed: rnd.Intn(3) == 0,
				Transient:       rnd.Intn(3) == 0,
			}
			if rnd.Intn(2) == 0 {
				artifacts.BlockData = bytes.Repeat([]byte{byte(seqno)}, rnd.Intn(8)+1)
			}
			if rnd.Intn(2) == 0 {
				artifacts.Proofs = []LiveBlockProofArtifact{{
					Kind: ServedProofBlock,
					Data: bytes.Repeat([]byte{byte(seqno + 1)}, rnd.Intn(8)+1),
				}}
			}
			if err := cache.PublishLiveBlockArtifacts(artifacts); err != nil {
				t.Fatalf("step %d publish block %d: %v", step, seqno, err)
			}
			model.publish(seqno, artifacts)
		}

		if len(cache.entries) != len(model.blocks) {
			t.Fatalf("step %d cache entries = %d, want %d", step, len(cache.entries), len(model.blocks))
		}
		if cache.flushedBytes != model.flushedBytes() {
			t.Fatalf("step %d flushed bytes = %d, want %d", step, cache.flushedBytes, model.flushedBytes())
		}
		for seqno := uint32(1); seqno <= blockIDs; seqno++ {
			want, exists := model.blocks[seqno]
			loaded, ok := cache.blocks.Load(BlockKey(testLiveBlockCacheBlockID(seqno)))
			if ok != exists {
				t.Fatalf("step %d block %d presence = %t, want %t", step, seqno, ok, exists)
			}
			if !ok {
				continue
			}
			got := loaded.(*LiveBlockCacheBlock)
			if len(got.data) != want.data || len(got.proofs[ServedProofBlock]) != want.proof {
				t.Fatalf("step %d block %d sizes = %d/%d, want %d/%d", step, seqno, len(got.data), len(got.proofs[ServedProofBlock]), want.data, want.proof)
			}
			if got.transient != want.transient {
				t.Fatalf("step %d block %d transient = %t, want %t", step, seqno, got.transient, want.transient)
			}
		}
	}
}

type testLiveBlockCacheBytesBlock struct {
	data      int
	proof     int
	flushed   bool
	transient bool
}

func (b testLiveBlockCacheBytesBlock) evictable() bool {
	return b.flushed || b.transient || b.data == 0 && b.proof == 0
}

type testLiveBlockCacheBytesModel struct {
	maxBlocks int
	maxBytes  int64
	blocks    map[uint32]testLiveBlockCacheBytesBlock
	order     []uint32
}

func (m *testLiveBlockCacheBytesModel) publish(seqno uint32, artifacts LiveBlockCacheArtifacts) {
	block, exists := m.blocks[seqno]
	if !exists {
		m.order = append(m.order, seqno)
		block.transient = true
	}
	if len(artifacts.BlockData) > 0 {
		block.data = len(artifacts.BlockData)
	}
	if len(artifacts.Proofs) > 0 {
		block.proof = len(artifacts.Proofs[0].Data)
	}
	block.flushed = block.flushed || artifacts.ArtifactFlushed
	block.transient = block.transient && artifacts.Transient
	m.blocks[seqno] = block

	for m.flushedBytes() > m.maxBytes {
		m.removeOldest(func(block testLiveBlockCacheBytesBlock) bool { return block.flushed })
	}
	for len(m.blocks) > m.maxBlocks {
		if !m.removeOldest(testLiveBlockCacheBytesBlock.evictable) {
			return
		}
	}
}

func (m *testLiveBlockCacheBytesModel) removeOldest(match func(testLiveBlockCacheBytesBlock) bool) bool {
	for _, seqno := range m.order {
		if match(m.blocks[seqno]) {
			m.remove(seqno)
			return true
		}
	}
	return false
}

func (m *testLiveBlockCacheBytesModel) remove(seqno uint32) {
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

func (m *testLiveBlockCacheBytesModel) flushedBytes() int64 {
	size := int64(0)
	for _, block := range m.blocks {
		if block.flushed {
			size += int64(block.data + block.proof)
		}
	}
	return size
}
