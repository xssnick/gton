package p2p

import (
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

type sliceCallCountingPeerStore struct {
	PeerStorage
	persistentStateSliceCalls int
	archiveSliceCalls         int
}

func (s *sliceCallCountingPeerStore) PersistentStateSlice(
	context.Context,
	ton.BlockIDExt,
	ton.BlockIDExt,
	int64,
	int64,
	int64,
) ([]byte, error) {
	s.persistentStateSliceCalls++
	return nil, nil
}

func (s *sliceCallCountingPeerStore) ArchiveSlice(
	context.Context,
	int64,
	int64,
	int32,
) ([]byte, error) {
	s.archiveSliceCalls++
	return nil, nil
}

func TestSliceQueriesRejectInvalidCPlusPlusSizesBeforeStorage(t *testing.T) {
	state := PersistentStateIDV2{
		Block:            testStoredBlockID(1),
		MasterchainBlock: testStoredMasterBlockID(1),
	}
	tests := []struct {
		name string
		req  tl.Serializable
	}{
		{
			name: "persistent state negative",
			req: DownloadPersistentStateSliceV2{
				State:   state,
				MaxSize: -1,
			},
		},
		{
			name: "persistent state above TL boundary",
			req: DownloadPersistentStateSliceV2{
				State:   state,
				MaxSize: maxPeerSliceRequestSize + 1,
			},
		},
		{
			name: "archive negative",
			req: GetArchiveSlice{
				MaxSize: -1,
			},
		},
		{
			name: "archive above TL boundary",
			req: GetArchiveSlice{
				MaxSize: maxPeerSliceRequestSize + 1,
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			store := &sliceCallCountingPeerStore{
				PeerStorage: newTestPeerStore(),
			}
			sub := &overlaySubscription{
				node: &Node{peerStorage: store},
			}

			response, err := sub.dispatchPeerQuery(context.Background(), tt.req)
			if err == nil {
				t.Fatal("query succeeded")
			}
			if response != nil {
				t.Fatalf("query returned response %T", response)
			}
			if store.persistentStateSliceCalls != 0 || store.archiveSliceCalls != 0 {
				t.Fatalf(
					"storage calls = (persistent state %d, archive %d), want zero",
					store.persistentStateSliceCalls,
					store.archiveSliceCalls,
				)
			}
		})
	}
}

func TestSliceQueriesAcceptExactCPlusPlusBoundary(t *testing.T) {
	state := PersistentStateIDV2{
		Block:            testStoredBlockID(1),
		MasterchainBlock: testStoredMasterBlockID(1),
	}
	tests := []struct {
		name string
		req  tl.Serializable
	}{
		{
			name: "persistent state",
			req: DownloadPersistentStateSliceV2{
				State:   state,
				MaxSize: maxPeerSliceRequestSize,
			},
		},
		{
			name: "archive",
			req: GetArchiveSlice{
				MaxSize: maxPeerSliceRequestSize,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			store := &sliceCallCountingPeerStore{
				PeerStorage: newTestPeerStore(),
			}
			sub := &overlaySubscription{
				node: &Node{peerStorage: store},
			}

			if _, err := sub.dispatchPeerQuery(context.Background(), test.req); err != nil {
				t.Fatalf("query failed: %v", err)
			}
			if store.persistentStateSliceCalls+store.archiveSliceCalls != 1 {
				t.Fatalf(
					"storage calls = (persistent state %d, archive %d), want one",
					store.persistentStateSliceCalls,
					store.archiveSliceCalls,
				)
			}
		})
	}
}
