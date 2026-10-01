package liveview

import (
	"bytes"
	"context"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// TestAcceptedStatePublicationNotifiesObserversOnceReadable pins the contract
// the validator's message pool is built on: an observer hears about a
// publication exactly once, strictly after the store can serve the block and its
// state — reading them back from inside the observer must succeed and must return
// the very cell that was published — and never about a publication the store
// refused. A stopped registration hears nothing more.
func TestAcceptedStatePublicationNotifiesObserversOnceReadable(t *testing.T) {
	live, _ := acceptedStateStore(t)

	var seen []storage.LiveBlockArtifacts
	var readBack []*cell.Cell
	stop := live.ObserveAcceptedBlockStates(func(artifacts storage.LiveBlockArtifacts) {
		seen = append(seen, artifacts)
		state, err := live.BlockState(context.Background(), artifacts.Block)
		if err != nil {
			t.Errorf("the observer could not read the published state back: %v", err)

			return
		}
		readBack = append(readBack, state.Cell)
	})

	// A refused publication notifies nobody: a masterchain block is never
	// published ahead of its apply.
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+3, 0x71)
	refused := fixture.artifacts()
	refused.Block.Workchain = masterchainID
	refused.Block.Shard = masterchainShard
	refused.State.Block = refused.Block
	if err := live.PublishAcceptedBlockState(refused); err == nil {
		t.Fatal("a masterchain state was published")
	}
	if len(seen) != 0 {
		t.Fatalf("a refused publication notified %d observers", len(seen))
	}

	if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
		t.Fatalf("publish the accepted block state: %v", err)
	}
	if len(seen) != 1 {
		t.Fatalf("one publication notified %d times", len(seen))
	}
	got := seen[0]
	if !got.Block.Equals(&fixture.block) {
		t.Error("the observer was told about another block")
	}
	if got.Root != fixture.root || !bytes.Equal(got.BlockData, fixture.blockData) {
		t.Error("the observer did not receive the published block root and BOC")
	}
	if got.State == nil || got.State.Cell != fixture.state {
		t.Error("the observer did not receive the very state cell that was published")
	}
	if got.Meta == nil || got.Meta.StartLT != 100 || got.Meta.GenUTime != 1_720_000_000 {
		t.Errorf("the observer did not receive the block metadata: %+v", got.Meta)
	}
	if len(readBack) != 1 || readBack[0] != fixture.state {
		t.Error("the state read back from inside the observer is not the published cell")
	}

	// A second publication of the same block is a new publication to the store
	// and to the observer alike; the observer's own bookkeeping decides what a
	// repeat means.
	if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
		t.Fatalf("republish the accepted block state: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("a republication notified %d times in total, want 2", len(seen))
	}

	stop()
	if err := live.PublishAcceptedBlockState(newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+4, 0x72).artifacts()); err != nil {
		t.Fatalf("publish after the registration was stopped: %v", err)
	}
	if len(seen) != 2 {
		t.Fatalf("a stopped registration was notified: %d notifications", len(seen))
	}
	// Stopping twice, or stopping one registration while another stays, must
	// leave the others untouched.
	stop()
}

// TestAcceptedStateObserverMayStopFromInsideTheCallback covers the observer
// that unregisters itself while being notified, which is how a consumer that is
// closing behaves: the registration list is snapshotted before the callbacks
// run, so the removal neither deadlocks nor skips the other observers of the
// same publication.
func TestAcceptedStateObserverMayStopFromInsideTheCallback(t *testing.T) {
	live, _ := acceptedStateStore(t)

	first, second := 0, 0
	var stopFirst func()
	stopFirst = live.ObserveAcceptedBlockStates(func(storage.LiveBlockArtifacts) {
		first++
		stopFirst()
	})
	stopSecond := live.ObserveAcceptedBlockStates(func(storage.LiveBlockArtifacts) {
		second++
	})
	defer stopSecond()

	for seqno := uint32(1); seqno <= 2; seqno++ {
		fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+seqno, 0x80+byte(seqno))
		if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
			t.Fatalf("publish accepted block %d: %v", seqno, err)
		}
	}
	if first != 1 {
		t.Fatalf("the self-stopping observer ran %d times, want once", first)
	}
	if second != 2 {
		t.Fatalf("the remaining observer ran %d times, want twice", second)
	}
}
