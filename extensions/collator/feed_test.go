package collator

import (
	"context"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

var feedTestShard = msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll}

// TestAppliedShardBlockArmsTheInternalsRun is the standalone half of the rule a
// validator has always obeyed: the out-queue run of every neighbour is advanced
// by the applied-block stream, so a leader window opens on runs already at the
// head instead of walking each neighbour's whole queue out of state.
//
// Before the pool feed reached this extension, a non-masterchain applied block
// only had its externals erased and this source stayed unseen.
func TestAppliedShardBlockArmsTheInternalsRun(t *testing.T) {
	messages := msgpool.New(msgpool.Config{})
	t.Cleanup(messages.Close)
	feed := msgpool.NewFeed(msgpool.FeedOptions{Pool: messages})
	extension, err := New(Options{
		Controller: &testController{},
		ShardTops:  &testShardTopSink{},
		Messages:   messages,
		Feed:       feed,
	})(hooks.Node{Store: &testNodeStore{}})
	if err != nil {
		t.Fatal(err)
	}

	// The destination projection a masterchain apply publishes. The controller
	// does this from its own reconcile; here it is the precondition, not the
	// subject.
	if err = feed.Reconcile(msgpool.NewTopology(&groups.Snapshot{Active: []groups.Session{{
		Shard: groups.ShardID{Workchain: feedTestShard.Workchain, Shard: int64(feedTestShard.Shard)},
		Registered: []groups.ShardDescription{{
			Shard: groups.ShardID{Workchain: feedTestShard.Workchain, Shard: int64(feedTestShard.Shard)},
		}},
	}}})); err != nil {
		t.Fatal(err)
	}

	message := testInternalMessage(t, 0x22, 1000)
	blockRoot := testAppliedBlockRoot(t)
	applied := hooks.BlockAppliedEvent{
		Meta: &storage.BlockMeta{
			ID: ton.BlockIDExt{
				Workchain: feedTestShard.Workchain,
				Shard:     int64(feedTestShard.Shard),
				SeqNo:     7,
				RootHash:  blockRoot.Hash(),
				FileHash:  make([]byte, 32),
			},
			GenUTime: uint32(time.Now().Unix()),
		},
		BlockRoot: blockRoot,
		CurrentState: testQueueStateRoot(t, map[msgpool.QueueKey]tlb.EnqueuedMsg{
			testQueueKey(t, message, 0x22): {EnqueuedLT: 1000, Msg: testMessageEnvelope(t, message)},
		}),
	}
	if err = extension.OnBlockApplied(context.Background(), applied); err != nil {
		t.Fatal(err)
	}

	top, err := messages.Internals().SourceTop(feedTestShard, feedTestShard)
	if err != nil {
		t.Fatalf("applied shard block did not arm the internals run: %v", err)
	}
	want := msgpool.SourceRef{Seqno: 7}
	copy(want.RootHash[:], blockRoot.Hash())
	if top != want {
		t.Fatalf("internals run position = %+v, want %+v", top, want)
	}

	cut, err := messages.Internals().Cut(feedTestShard, msgpool.CutRequest{
		Sources: map[msgpool.ShardIdent]msgpool.CutSource{feedTestShard: {Visible: top}},
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(cut.Messages) != 1 {
		t.Fatalf("armed run serves %d messages, want 1", len(cut.Messages))
	}
}

func testInternalMessage(t *testing.T, destinationFill byte, lt uint64) *cell.Cell {
	t.Helper()

	source := [32]byte{0x0e}
	destination := [32]byte{destinationFill}

	return cell.BeginCell().
		MustStoreUInt(0, 1).
		MustStoreBoolBit(true).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		MustStoreAddr(address.NewAddress(0, 0, source[:])).
		MustStoreAddr(address.NewAddress(0, 0, destination[:])).
		MustStoreCoins(1_000_000).
		MustStoreBoolBit(false).
		MustStoreCoins(0).
		MustStoreCoins(1_000).
		MustStoreUInt(lt, 64).
		MustStoreUInt(0, 32).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		EndCell()
}

func testMessageEnvelope(t *testing.T, message *cell.Cell) *cell.Cell {
	t.Helper()

	envelope, err := tlb.MsgEnvelope{
		CurAddr:         tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		NextAddr:        tlb.IntermediateAddress{Type: tlb.IntermediateAddressRegular, UseDestBits: 96},
		FwdFeeRemaining: tlb.MustFromTON("0.001"),
		Msg:             message,
	}.ToCell()
	if err != nil {
		t.Fatal(err)
	}

	return envelope
}

func testQueueKey(t *testing.T, message *cell.Cell, destinationFill byte) msgpool.QueueKey {
	t.Helper()

	destination := [32]byte{destinationFill}
	hop, err := msgpool.AccountPrefixFromAddress(address.NewAddress(0, 0, destination[:]))
	if err != nil {
		t.Fatal(err)
	}

	return msgpool.MakeQueueKey(hop, message.HashKey())
}

// testQueueStateRoot wraps queued envelopes into the minimal ShardStateUnsplit
// shape the feed reads: the out-queue dictionary and the stored queue size the
// reseed cross-checks itself against.
func testQueueStateRoot(t *testing.T, queued map[msgpool.QueueKey]tlb.EnqueuedMsg) *cell.Cell {
	t.Helper()

	dictionary, err := cell.NewAugDict(352, tlb.AugOutMsgQueue{})
	if err != nil {
		t.Fatal(err)
	}
	for key, value := range queued {
		valueCell, cellErr := value.ToCell()
		if cellErr != nil {
			t.Fatal(cellErr)
		}
		keyCell := cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
		if _, cellErr = dictionary.SetWithMode(keyCell, valueCell, cell.DictSetModeAdd); cellErr != nil {
			t.Fatal(cellErr)
		}
	}
	size := uint64(len(queued))
	dispatchQueue, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	extra, err := tlb.OutMsgQueueExtra{DispatchQueue: dispatchQueue, OutQueueSize: &size}.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	queueInfo := cell.BeginCell().
		MustStoreBuilder(dictionary.AsCell().ToBuilder()).
		MustStoreBoolBit(false).
		MustStoreBoolBit(true).
		MustStoreBuilder(extra.ToBuilder()).
		EndCell()

	return cell.BeginCell().
		MustStoreUInt(0x9023afe2, 32).
		MustStoreRef(queueInfo).
		EndCell()
}
