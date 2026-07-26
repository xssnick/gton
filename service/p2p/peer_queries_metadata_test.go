package p2p

import (
	"context"
	"errors"
	"reflect"
	"testing"

	tnstore "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type metadataQueryBlobCalls struct {
	blockFull     int
	nextBlockFull int
	blockData     int
	blockProof    int
	zeroState     int
}

type metadataQueryCountingStore struct {
	tnstore.PeerServingStorage

	blockMetaCalls     int
	zeroStateSizeCalls int
	blobCalls          metadataQueryBlobCalls
	blockMetaErr       error
	zeroStateSizeErr   error
}

func (s *metadataQueryCountingStore) BlockMeta(
	ctx context.Context,
	block ton.BlockIDExt,
) (*tnstore.BlockMeta, error) {
	s.blockMetaCalls++
	if s.blockMetaErr != nil {
		return nil, s.blockMetaErr
	}

	return s.PeerServingStorage.BlockMeta(ctx, block)
}

func (s *metadataQueryCountingStore) ZeroStateSize(
	ctx context.Context,
	block ton.BlockIDExt,
) (int64, error) {
	s.zeroStateSizeCalls++
	if s.zeroStateSizeErr != nil {
		return 0, s.zeroStateSizeErr
	}

	return s.PeerServingStorage.ZeroStateSize(ctx, block)
}

func (s *metadataQueryCountingStore) BlockFull(
	context.Context,
	ton.BlockIDExt,
) (*tnstore.ServedBlockFull, error) {
	s.blobCalls.blockFull++
	return nil, errors.New("block full payload accessor called")
}

func (s *metadataQueryCountingStore) NextBlockFull(
	context.Context,
	ton.BlockIDExt,
) (*tnstore.ServedBlockFull, error) {
	s.blobCalls.nextBlockFull++
	return nil, errors.New("next block full payload accessor called")
}

func (s *metadataQueryCountingStore) BlockData(
	context.Context,
	ton.BlockIDExt,
) ([]byte, error) {
	s.blobCalls.blockData++
	return nil, errors.New("block data payload accessor called")
}

func (s *metadataQueryCountingStore) BlockProof(
	context.Context,
	tnstore.ServedProofKind,
	ton.BlockIDExt,
) ([]byte, error) {
	s.blobCalls.blockProof++
	return nil, errors.New("block proof payload accessor called")
}

func (s *metadataQueryCountingStore) ZeroState(
	context.Context,
	ton.BlockIDExt,
) ([]byte, error) {
	s.blobCalls.zeroState++
	return nil, errors.New("zerostate payload accessor called")
}

type metadataPeerQueryCase struct {
	name     string
	query    tl.Serializable
	response tl.Serializable
}

type metadataPeerQueryErrorCase struct {
	name  string
	query tl.Serializable
}

func TestPrepareQueriesUseMetadataWithoutReadingPayloads(t *testing.T) {
	sub, store, cases := newMetadataQueryTestSubscription(t)

	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			response, err := sub.dispatchPeerQuery(context.Background(), test.query)
			if err != nil {
				t.Fatalf("dispatch %T: %v", test.query, err)
			}
			if reflect.TypeOf(response) != reflect.TypeOf(test.response) {
				t.Fatalf("response = %T, want %T", response, test.response)
			}
		})
	}

	if store.blockMetaCalls != 5 {
		t.Fatalf("block metadata calls = %d, want 5", store.blockMetaCalls)
	}
	if store.zeroStateSizeCalls != 1 {
		t.Fatalf("zerostate size calls = %d, want 1", store.zeroStateSizeCalls)
	}
	if store.blobCalls != (metadataQueryBlobCalls{}) {
		t.Fatalf("payload accessor calls = %+v, want none", store.blobCalls)
	}
}

func TestPrepareQueriesPropagateMetadataErrorsWithoutReadingPayloads(t *testing.T) {
	metadataFailure := errors.New("metadata is corrupt")
	block := testStoredMasterBlockID(200)
	blockCases := []metadataPeerQueryErrorCase{
		{
			name:  "next block description",
			query: GetNextBlockDescription{PrevBlock: block},
		},
		{
			name:  "prepare block",
			query: PrepareBlock{Block: block},
		},
		{
			name: "prepare block proof",
			query: PrepareBlockProof{
				Block: block,
			},
		},
		{
			name: "prepare key block proof",
			query: PrepareKeyBlockProof{
				Block: block,
			},
		},
	}
	for _, test := range blockCases {
		t.Run(test.name, func(t *testing.T) {
			store := &metadataQueryCountingStore{
				PeerServingStorage: newTestPeerStore(),
				blockMetaErr:       metadataFailure,
			}
			sub := metadataQueryErrorTestSubscription(store)

			response, err := sub.dispatchPeerQuery(context.Background(), test.query)
			if !errors.Is(err, metadataFailure) {
				t.Fatalf("dispatch %T error = %v, want metadata failure", test.query, err)
			}
			if response != nil {
				t.Fatalf("dispatch %T response = %T, want nil", test.query, response)
			}
			if store.blobCalls != (metadataQueryBlobCalls{}) {
				t.Fatalf("payload accessor calls = %+v, want none", store.blobCalls)
			}
		})
	}

	t.Run("prepare zerostate", func(t *testing.T) {
		store := &metadataQueryCountingStore{
			PeerServingStorage: newTestPeerStore(),
			zeroStateSizeErr:   metadataFailure,
		}
		sub := metadataQueryErrorTestSubscription(store)

		response, err := sub.dispatchPeerQuery(
			context.Background(),
			PrepareZeroState{Block: testStoredMasterBlockID(0)},
		)
		if !errors.Is(err, metadataFailure) {
			t.Fatalf("prepare zerostate error = %v, want metadata failure", err)
		}
		if response != nil {
			t.Fatalf("prepare zerostate response = %T, want nil", response)
		}
		if store.blobCalls != (metadataQueryBlobCalls{}) {
			t.Fatalf("payload accessor calls = %+v, want none", store.blobCalls)
		}
	})
}

func BenchmarkPrepareQueriesMetadataOnly(b *testing.B) {
	sub, store, cases := newMetadataQueryTestSubscription(b)

	for _, test := range cases {
		b.Run(test.name, func(b *testing.B) {
			ctx := context.Background()
			var response tl.Serializable

			b.ReportAllocs()
			for b.Loop() {
				var err error
				response, err = sub.dispatchPeerQuery(ctx, test.query)
				if err != nil {
					b.Fatalf("dispatch %T: %v", test.query, err)
				}
			}

			_ = response
		})
	}

	if store.blobCalls != (metadataQueryBlobCalls{}) {
		b.Fatalf("payload accessor calls = %+v, want none", store.blobCalls)
	}
}

func newMetadataQueryTestSubscription(
	tb testing.TB,
) (*overlaySubscription, *metadataQueryCountingStore, []metadataPeerQueryCase) {
	tb.Helper()

	base := newTestPeerStore()
	prev := testStoredMasterBlockID(100)
	next := testStoredMasterBlockID(101)
	keyBlock := testStoredMasterBlockID(102)
	zeroState := testStoredMasterBlockID(0)

	if err := base.SaveBlockFull(&tnstore.ServedBlockFull{
		ID:    next,
		Block: []byte{0x01},
		Proof: []byte{0x02},
	}); err != nil {
		tb.Fatalf("save next block: %v", err)
	}
	if err := base.LinkNextBlock(prev, next); err != nil {
		tb.Fatalf("link next block: %v", err)
	}
	if err := base.SaveBlockProof(
		tnstore.ServedProofKeyBlock,
		keyBlock,
		[]byte{0x03},
		nil,
	); err != nil {
		tb.Fatalf("save key block proof: %v", err)
	}
	if err := base.SaveZeroState(zeroState, []byte{0x04}, nil); err != nil {
		tb.Fatalf("save zerostate: %v", err)
	}

	store := &metadataQueryCountingStore{PeerServingStorage: base}
	logger := discardLogger()
	node, err := New(Options{
		Logger:             &logger,
		PeerServingStorage: store,
		StateFilesDir:      tb.TempDir(),
	})
	if err != nil {
		tb.Fatalf("create node: %v", err)
	}
	tb.Cleanup(node.closeSubscriptions)

	sub := testOverlaySubscription(&overlaySubscription{
		node: node,
		spec: overlaySpec{Name: "masterchain"},
		log:  discardLogger(),
	})
	cases := []metadataPeerQueryCase{
		{
			name:     "next block description",
			query:    GetNextBlockDescription{PrevBlock: prev},
			response: BlockDescription{},
		},
		{
			name:     "prepare block",
			query:    PrepareBlock{Block: next},
			response: Prepared{},
		},
		{
			name: "prepare block proof",
			query: PrepareBlockProof{
				Block: next,
			},
			response: PreparedProof{},
		},
		{
			name: "prepare key block proof",
			query: PrepareKeyBlockProof{
				Block: keyBlock,
			},
			response: PreparedProof{},
		},
		{
			name:     "prepare zerostate",
			query:    PrepareZeroState{Block: zeroState},
			response: PreparedState{},
		},
	}

	return sub, store, cases
}

func metadataQueryErrorTestSubscription(
	store tnstore.PeerServingStorage,
) *overlaySubscription {
	return testOverlaySubscription(&overlaySubscription{
		node: &Node{
			peerStorage:    store,
			liveBlockCache: tnstore.NewLiveBlockCache(1),
		},
		spec: overlaySpec{Name: "masterchain"},
		log:  discardLogger(),
	})
}
