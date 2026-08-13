package p2p

import (
	"context"
	"crypto/sha256"
	"errors"
	"testing"
	"time"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestDownloadNextFromOverlayOrMasterBroadcastUsesBroadcastDuringDownload(t *testing.T) {
	node := newTestNode(t)
	prev := testStoredMasterBlockID(200)
	broadcast := testMasterchainBroadcastDownloadedBlock(t, prev, 201, 0x201)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	started := make(chan context.Context, 1)
	got := make(chan *DownloadedBlock, 1)
	errCh := make(chan error, 1)
	go func() {
		block, err := node.downloadNextFromOverlayOrMasterBroadcast(ctx, prev, func(queryCtx context.Context) (*DownloadedBlock, error) {
			started <- queryCtx
			<-queryCtx.Done()
			return nil, queryCtx.Err()
		})
		if err != nil {
			errCh <- err
			return
		}
		got <- block
	}()

	var downloadCtx context.Context
	select {
	case downloadCtx = <-started:
	case <-ctx.Done():
		t.Fatal("download did not start")
	}

	if !node.rememberMasterchainNextBroadcastBlock(&broadcast) {
		t.Fatal("masterchain broadcast was not cached")
	}

	select {
	case err := <-errCh:
		t.Fatalf("download next race: %v", err)
	case block := <-got:
		if !block.ID.Equals(&broadcast.ID) {
			t.Fatalf("block = %s, want %s", storage.FormatBlockRef(block.ID), storage.FormatBlockRef(broadcast.ID))
		}
		if block.Kind != broadcast.Kind {
			t.Fatalf("kind = %q, want %q", block.Kind, broadcast.Kind)
		}
	case <-ctx.Done():
		t.Fatal("timed out waiting for broadcast result")
	}

	select {
	case <-downloadCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("download context was not canceled")
	}
}

func TestDownloadNextFromOverlayOrMasterBroadcastPrefersReadyBroadcastOverPeerResult(t *testing.T) {
	node := newTestNode(t)
	prev := testStoredMasterBlockID(210)
	broadcast := testMasterchainBroadcastDownloadedBlock(t, prev, 211, 0x211)
	peer := testMasterchainBroadcastDownloadedBlock(t, prev, 211, 0x212)
	peer.Kind = "tonNode.dataFull"

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	got, err := node.downloadNextFromOverlayOrMasterBroadcast(ctx, prev, func(context.Context) (*DownloadedBlock, error) {
		if !node.rememberMasterchainNextBroadcastBlock(&broadcast) {
			return nil, errors.New("masterchain broadcast was not cached")
		}
		return &peer, nil
	})
	if err != nil {
		t.Fatalf("download next race: %v", err)
	}
	if !got.ID.Equals(&broadcast.ID) {
		t.Fatalf("block = %s, want %s", storage.FormatBlockRef(got.ID), storage.FormatBlockRef(broadcast.ID))
	}
	if got.Kind != broadcast.Kind {
		t.Fatalf("kind = %q, want %q", got.Kind, broadcast.Kind)
	}
}

func TestDownloadNextBlockFullUsesMasterchainBroadcastCacheBeforeOverlay(t *testing.T) {
	node := newTestNode(t)
	prev := testStoredMasterBlockID(220)
	broadcast := testMasterchainBroadcastDownloadedBlock(t, prev, 221, 0x221)
	if !node.rememberMasterchainNextBroadcastBlock(&broadcast) {
		t.Fatal("masterchain broadcast was not cached")
	}

	got, err := node.DownloadNextBlockFull(context.Background(), prev)
	if err != nil {
		t.Fatalf("download next block full: %v", err)
	}
	if !got.ID.Equals(&broadcast.ID) {
		t.Fatalf("block = %s, want %s", storage.FormatBlockRef(got.ID), storage.FormatBlockRef(broadcast.ID))
	}
	desc, err := node.NextBlockDescription(context.Background(), prev)
	if err != nil {
		t.Fatalf("next block description: %v", err)
	}
	if !desc.Equals(&broadcast.ID) {
		t.Fatalf("description = %s, want %s", storage.FormatBlockRef(desc), storage.FormatBlockRef(broadcast.ID))
	}
}

func TestAcceptBroadcastCachesMasterchainNextBroadcast(t *testing.T) {
	node := newTestNode(t)
	prev := testStoredMasterBlockID(230)
	broadcast := testMasterchainBroadcastDownloadedBlock(t, prev, 231, 0x231)

	node.acceptBroadcast(acceptedBroadcast{
		fingerprint: "masterchain-next-broadcast",
		event: &BroadcastEvent{
			Kind:       broadcast.Kind,
			Block:      broadcast.ID,
			Downloaded: &broadcast,
		},
	})

	got, err := node.masterchainNextBroadcastCache.BlockAfter(prev)
	if err != nil {
		t.Fatalf("load cached masterchain broadcast: %v", err)
	}
	if !got.ID.Equals(&broadcast.ID) {
		t.Fatalf("block = %s, want %s", storage.FormatBlockRef(got.ID), storage.FormatBlockRef(broadcast.ID))
	}
}

func TestAcceptBroadcastObservesSignedMasterchainBlockReceived(t *testing.T) {
	node := newTestNode(t)
	observer := &testBlockReceivedObserver{}
	node.blockReceivedObserver = observer
	prev := testStoredMasterBlockID(240)
	broadcast := testMasterchainBroadcastDownloadedBlock(t, prev, 241, 0x241)

	node.acceptBroadcast(acceptedBroadcast{
		fingerprint: "masterchain-received-hook",
		event: &BroadcastEvent{
			Kind:       broadcast.Kind,
			Block:      broadcast.ID,
			Downloaded: &broadcast,
		},
	})

	if len(observer.events) != 1 {
		t.Fatalf("block received events = %d, want 1", len(observer.events))
	}
	event := observer.events[0]
	if !event.IsSigned || event.Downloaded != &broadcast {
		t.Fatalf("unexpected block received event: %+v", event)
	}
}

func TestWatchMasterchainNextBroadcastBlockFiresOnStore(t *testing.T) {
	node := newTestNode(t)
	prev := testStoredMasterBlockID(250)
	broadcast := testMasterchainBroadcastDownloadedBlock(t, prev, 251, 0x251)

	wake, unwatch := node.WatchMasterchainNextBroadcastBlock(prev)
	defer unwatch()
	if wake == nil {
		t.Fatal("watch returned nil channel for masterchain prev")
	}
	if node.HasMasterchainNextBroadcastBlock(prev) {
		t.Fatal("cache reports next broadcast before store")
	}

	if !node.rememberMasterchainNextBroadcastBlock(&broadcast) {
		t.Fatal("masterchain broadcast was not cached")
	}

	select {
	case <-wake:
	case <-time.After(time.Second):
		t.Fatal("watch did not fire after the next broadcast was cached")
	}
	if !node.HasMasterchainNextBroadcastBlock(prev) {
		t.Fatal("cache does not report the stored next broadcast")
	}

	if wake, _ := node.WatchMasterchainNextBroadcastBlock(testBlockID(0, topShard, 1)); wake != nil {
		t.Fatal("watch returned a channel for non-masterchain prev")
	}
	if node.HasMasterchainNextBroadcastBlock(testBlockID(0, topShard, 1)) {
		t.Fatal("cache reports next broadcast for non-masterchain prev")
	}
}

func testMasterchainBroadcastDownloadedBlock(t *testing.T, prev ton.BlockIDExt, seqno uint32, payload uint64) DownloadedBlock {
	t.Helper()

	root := cell.BeginCell().MustStoreUInt(payload, 16).EndCell()
	blockBOC := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	rootHash := root.HashKey()
	fileHash := sha256.Sum256(blockBOC)

	block := testBlockID(-1, topShard, seqno)
	block.RootHash = append([]byte(nil), rootHash[:]...)
	block.FileHash = append([]byte(nil), fileHash[:]...)

	proof := testBlockProofCell(t, block, nil)
	proofBOC := proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	return DownloadedBlock{
		ID:       block,
		Kind:     "tonNode.blockBroadcast",
		Block:    root,
		Proof:    proof,
		BlockBOC: blockBOC,
		ProofBOC: proofBOC,
		Meta: &storage.BlockMeta{
			ID:       block,
			PrevRefs: []ton.BlockIDExt{prev},
		},
		StateUpdate:      cell.BeginCell().EndCell(),
		IsLink:           true,
		VerifiedRootHash: true,
	}
}
