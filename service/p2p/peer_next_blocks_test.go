package p2p

import (
	"context"
	"crypto/sha256"
	"errors"
	"sync/atomic"
	"testing"

	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type countingNextBlockStore struct {
	PeerStorage
	calls atomic.Int32
}

func (s *countingNextBlockStore) NextBlockFull(
	ctx context.Context,
	prev ton.BlockIDExt,
) (*tnstore.ServedBlockFull, error) {
	s.calls.Add(1)
	return s.PeerStorage.NextBlockFull(ctx, prev)
}

func TestNextBlocksFullTLValuesRoundTrip(t *testing.T) {
	query := DownloadNextBlocksFull{
		PrevBlock: testStoredMasterBlockID(10),
		MaxBlocks: 7,
	}
	encoded, err := tl.Serialize(query, true)
	if err != nil {
		t.Fatalf("serialize query: %v", err)
	}

	var parsedQuery any
	rest, err := tl.Parse(&parsedQuery, encoded, true)
	if err != nil {
		t.Fatalf("parse query: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("query has %d trailing bytes", len(rest))
	}
	gotQuery, ok := parsedQuery.(DownloadNextBlocksFull)
	if !ok {
		t.Fatalf("query type = %T, want DownloadNextBlocksFull value", parsedQuery)
	}
	if gotQuery.MaxBlocks != query.MaxBlocks || !gotQuery.PrevBlock.Equals(&query.PrevBlock) {
		t.Fatalf("query = %+v, want %+v", gotQuery, query)
	}

	response := NextBlocksFull{Blocks: []any{
		DataFullCompressed{
			ID:         testStoredMasterBlockID(11),
			Compressed: []byte{0x01, 0x02, 0x03},
		},
	}}
	encoded, err = tl.Serialize(response, true)
	if err != nil {
		t.Fatalf("serialize response: %v", err)
	}

	var parsedResponse any
	rest, err = tl.Parse(&parsedResponse, encoded, true)
	if err != nil {
		t.Fatalf("parse response: %v", err)
	}
	if len(rest) != 0 {
		t.Fatalf("response has %d trailing bytes", len(rest))
	}
	gotResponse, ok := parsedResponse.(NextBlocksFull)
	if !ok {
		t.Fatalf("response type = %T, want NextBlocksFull value", parsedResponse)
	}
	if len(gotResponse.Blocks) != 1 {
		t.Fatalf("response block count = %d, want 1", len(gotResponse.Blocks))
	}
	if _, ok = gotResponse.Blocks[0].(DataFullCompressed); !ok {
		t.Fatalf("response block type = %T, want DataFullCompressed value", gotResponse.Blocks[0])
	}
}

func TestNextBlocksFullBuilderMatchesCppAggregateLimit(t *testing.T) {
	blockID := testStoredMasterBlockID(1)

	t.Run("later block stops before limit", func(t *testing.T) {
		builder := newNextBlocksFullBuilder(2)
		first := DataFullCompressed{
			ID:         blockID,
			Compressed: make([]byte, nextBlocksFullMaxAggregateBytes-256),
		}
		if err := builder.add(first); err != nil {
			t.Fatalf("add first block: %v", err)
		}
		sizeAfterFirst := builder.totalSize

		second := DataFullCompressed{
			ID:         testStoredMasterBlockID(2),
			Compressed: make([]byte, 512),
		}
		if err := builder.add(second); !errors.Is(err, errNextBlocksFullSizeLimit) {
			t.Fatalf("add second block error = %v, want size limit", err)
		}
		if len(builder.blocks) != 1 {
			t.Fatalf("block count = %d, want 1", len(builder.blocks))
		}
		if builder.totalSize != sizeAfterFirst {
			t.Fatalf("serialized size = %d, want unchanged %d", builder.totalSize, sizeAfterFirst)
		}
	})

	t.Run("oversized first block is retained", func(t *testing.T) {
		builder := newNextBlocksFullBuilder(1)
		first := DataFullCompressed{
			ID:         blockID,
			Compressed: make([]byte, nextBlocksFullMaxAggregateBytes+1),
		}
		if err := builder.add(first); err != nil {
			t.Fatalf("add oversized first block: %v", err)
		}
		if len(builder.blocks) != 1 {
			t.Fatalf("block count = %d, want 1", len(builder.blocks))
		}
		if !builder.sizeLimitReached() {
			t.Fatal("oversized first block did not stop the batch")
		}
	})
}

func TestDataFullCompressedWireSize(t *testing.T) {
	for _, compressedSize := range []int{0, 1, 0xfd, 0xfe, 0xff, 256, 1024} {
		block := DataFullCompressed{
			ID:         testStoredMasterBlockID(1),
			Compressed: make([]byte, compressedSize),
		}
		serialized, err := tl.Serialize(block, true)
		if err != nil {
			t.Fatalf("serialize %d-byte block: %v", compressedSize, err)
		}
		if got := dataFullCompressedWireSize(block); got != len(serialized) {
			t.Fatalf(
				"%d-byte block wire size = %d, want %d",
				compressedSize,
				got,
				len(serialized),
			)
		}
	}
}

func TestServeNextBlocksFullCapsCountAndStorageIO(t *testing.T) {
	tests := []struct {
		name      string
		maxBlocks int32
		want      int
		wantCalls int32
	}{
		{
			name:      "large positive count",
			maxBlocks: 100,
			want:      int(nextBlocksFullMaxCount),
			wantCalls: int32(nextBlocksFullMaxCount),
		},
		{
			name:      "negative count uses unsigned C++ semantics",
			maxBlocks: -1,
			want:      int(nextBlocksFullMaxCount),
			wantCalls: int32(nextBlocksFullMaxCount),
		},
		{
			name:      "zero count",
			maxBlocks: 0,
			want:      0,
			wantCalls: 0,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sub, store, prev, ids := nextBlocksTestSubscription(
				t,
				int(nextBlocksFullMaxCount)+2,
			)

			response, err := sub.serveNextBlocksFull(
				context.Background(),
				prev,
				tt.maxBlocks,
			)
			if err != nil {
				t.Fatalf("serve next blocks: %v", err)
			}
			batch, ok := response.(NextBlocksFull)
			if !ok {
				t.Fatalf("response type = %T, want NextBlocksFull", response)
			}
			if len(batch.Blocks) != tt.want {
				t.Fatalf("block count = %d, want %d", len(batch.Blocks), tt.want)
			}
			if calls := store.calls.Load(); calls != tt.wantCalls {
				t.Fatalf("NextBlockFull calls = %d, want %d", calls, tt.wantCalls)
			}

			for i, item := range batch.Blocks {
				block, ok := item.(DataFullCompressed)
				if !ok {
					t.Fatalf("block %d type = %T, want DataFullCompressed value", i, item)
				}
				if !block.ID.Equals(&ids[i]) {
					t.Fatalf("block %d id = %v, want %v", i, block.ID, ids[i])
				}
				decoded, err := decodeCompressedBlock(block)
				if err != nil {
					t.Fatalf("decode block %d: %v", i, err)
				}
				if !decoded.ID.Equals(&ids[i]) {
					t.Fatalf("decoded block %d id = %v, want %v", i, decoded.ID, ids[i])
				}
			}
		})
	}
}

func TestServeNextBlocksFullStopsLikeCpp(t *testing.T) {
	t.Run("non-masterchain anchor", func(t *testing.T) {
		sub, store, _, _ := nextBlocksTestSubscription(t, 1)
		response, err := sub.serveNextBlocksFull(
			context.Background(),
			testStoredBlockID(1),
			1,
		)
		if err != nil {
			t.Fatalf("serve next blocks: %v", err)
		}
		batch := response.(NextBlocksFull)
		if len(batch.Blocks) != 0 {
			t.Fatalf("block count = %d, want 0", len(batch.Blocks))
		}
		if calls := store.calls.Load(); calls != 0 {
			t.Fatalf("NextBlockFull calls = %d, want 0", calls)
		}
	})

	t.Run("not found returns accumulated blocks", func(t *testing.T) {
		sub, store, prev, _ := nextBlocksTestSubscription(t, 2)
		response, err := sub.serveNextBlocksFull(
			context.Background(),
			prev,
			10,
		)
		if err != nil {
			t.Fatalf("serve next blocks: %v", err)
		}
		batch := response.(NextBlocksFull)
		if len(batch.Blocks) != 2 {
			t.Fatalf("block count = %d, want 2", len(batch.Blocks))
		}
		if calls := store.calls.Load(); calls != 3 {
			t.Fatalf("NextBlockFull calls = %d, want 3", calls)
		}
	})

	t.Run("proof link is not returned", func(t *testing.T) {
		sub, store, prev, ids := nextBlocksTestSubscription(t, 1)
		base := store.PeerStorage.(*testPeerStore)
		base.mu.Lock()
		base.blocks[tnstore.BlockKey(ids[0])].IsLink = true
		base.mu.Unlock()

		response, err := sub.serveNextBlocksFull(
			context.Background(),
			prev,
			1,
		)
		if err != nil {
			t.Fatalf("serve next blocks: %v", err)
		}
		batch := response.(NextBlocksFull)
		if len(batch.Blocks) != 0 {
			t.Fatalf("block count = %d, want 0", len(batch.Blocks))
		}
		if calls := store.calls.Load(); calls != 1 {
			t.Fatalf("NextBlockFull calls = %d, want 1", calls)
		}
	})
}

func nextBlocksTestSubscription(
	t *testing.T,
	blockCount int,
) (*overlaySubscription, *countingNextBlockStore, ton.BlockIDExt, []ton.BlockIDExt) {
	t.Helper()

	base := newTestPeerStore()
	store := &countingNextBlockStore{PeerStorage: base}
	logger := discardLogger()
	node, err := New(Options{
		Logger:        &logger,
		PeerStorage:   store,
		StateFilesDir: t.TempDir(),
	})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}
	t.Cleanup(node.closeSubscriptions)

	anchor := testStoredMasterBlockID(0)
	prev := anchor
	ids := make([]ton.BlockIDExt, 0, blockCount)
	for i := 1; i <= blockCount; i++ {
		root := testPeerBlockRoot(t, -1, uint32(i))
		blockData := serializeCompressedBlockRoot(root)
		rootHash := root.HashKey()
		fileHash := sha256.Sum256(blockData)
		id := ton.BlockIDExt{
			Workchain: -1,
			Shard:     topShard,
			SeqNo:     uint32(i),
			RootHash:  append([]byte(nil), rootHash[:]...),
			FileHash:  append([]byte(nil), fileHash[:]...),
		}
		if err = base.SaveBlockFull(&tnstore.ServedBlockFull{
			ID:    id,
			Proof: testBlockProofCell(t, id, testProofSignatureSet()).ToBOC(),
			Block: blockData,
		}); err != nil {
			t.Fatalf("save block %d: %v", i, err)
		}
		if err = base.LinkNextBlock(prev, id); err != nil {
			t.Fatalf("link block %d: %v", i, err)
		}
		ids = append(ids, id)
		prev = id
	}

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{Name: "masterchain"},
		log:  discardLogger(),
	})
	return sub, store, anchor, ids
}
