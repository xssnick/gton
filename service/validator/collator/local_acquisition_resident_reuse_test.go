package collator

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// A masterchain block this node collated itself put every shard top it
// registers into the block cache while it was being built, so the publish
// that installs it has nothing left to read. The neighbour pins must still
// happen: they are what keeps the slot from snapshotting those positions under
// the session mutex.
//
// A pin is invisible while the committed state still holds the position, so the
// committed history is pushed past it afterwards: only a branch that pinned the
// run keeps answering for it.
func TestPublishPinsNeighborsWhenEveryTopIsCached(t *testing.T) {
	fixture := newViewRaceAcquisition(t, 2)
	acquisition := fixture.acquisition
	managed, err := acquisition.session(fixture.session.ID)
	if err != nil {
		t.Fatal(err)
	}
	managed.mu.Lock()
	branch := managed.branch
	managed.mu.Unlock()

	destination := targetShardIdent(fixture.session.Shard)
	next := fixture.blocks[1].ID
	source := blockShardIdent(next)
	ref, err := localSourceRef(next)
	if err != nil {
		t.Fatal(err)
	}
	internals := acquisition.messages.Internals()
	if err = internals.Seed(destination, source, ref, nil, 0); err != nil {
		t.Fatalf("commit masterchain source %d: %v", next.SeqNo, err)
	}

	for _, description := range fixture.update.Registered {
		if description.Block.Workchain == masterchainWorkchainID {
			continue
		}
		if _, err = acquisition.blockSource(context.Background(), description.Block, acquisitionReadImmediate); err != nil {
			t.Fatalf("cache registered top %d: %v", description.Block.SeqNo, err)
		}
	}

	synctest.Test(t, func(t *testing.T) {
		if err := acquisition.PublishMasterchainView(
			context.Background(),
			fixture.snapshots[1],
			fixture.blocks[1].Block,
			fixture.blocks[1].State,
		); err != nil {
			t.Fatalf("publish masterchain view %d: %v", next.SeqNo, err)
		}
		synctest.Wait()
	})

	for seqno := ref.Seqno + 1; !errors.Is(internals.ValidateSourceRef(destination, source, ref), msgpool.ErrCutStale); seqno++ {
		if seqno > ref.Seqno+1024 {
			t.Fatal("committed history never moved past the pinned position")
		}
		later := msgpool.SourceRef{Seqno: seqno, RootHash: [32]byte{0xa5, byte(seqno), byte(seqno >> 8)}}
		if err = internals.ApplyBlock(destination, source, later, &msgpool.InternalsDelta{}); err != nil {
			t.Fatalf("apply masterchain source %d: %v", seqno, err)
		}
	}
	if !branch.SourcePinnable(source, ref) {
		t.Fatal("publishing a view whose tops were all cached pinned no neighbour position")
	}
}

// Once the next masterchain block demotes the resident view, every historical
// frontier lookup for this seqno reads its block through the block cache. The
// publish already holds that exact verified pair and its shard registry, so the
// lookup must not go back to storage for them. The store here is empty, so any
// storage read fails.
func TestPublishMasterchainViewCachesResidentBlock(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder:   testBuilder(),
		Store:     &localValidationStore{},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = acquisition.PublishMasterchainView(
		context.Background(),
		fixture.request.Groups,
		fixture.request.Previous.Block,
		fixture.request.Previous.State,
	); err != nil {
		t.Fatalf("publish resident view: %v", err)
	}
	resident := acquisition.master.resident()

	// Stands in for the install of the next masterchain block.
	acquisition.blocks.advance()

	source, err := acquisition.blockSource(context.Background(), fixture.request.Previous.ID, acquisitionReadImmediate)
	if err != nil {
		t.Fatalf("read the published masterchain block with nothing in storage: %v", err)
	}
	if source.previous.State != fixture.request.Previous.State || source.previous.Block != fixture.request.Previous.Block {
		t.Fatal("cached masterchain block is not the published block/state pair")
	}
	genLT, registry, err := source.masterShardRegistry()
	if err != nil {
		t.Fatal(err)
	}
	if registry != resident.registry || genLT != resident.state.GenLT {
		t.Fatal("cached masterchain block re-derived the shard registry the published view already holds")
	}
}

// A non-resident cached view already holds the verified block/state pair, and
// the store here is empty: both the hit and the rebuild for a new group
// snapshot must come from that view instead of reading the block again.
func TestProjectedMasterViewReusesCachedPairWithoutStorage(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	groupSource := &localAcquisitionTestGroups{snapshot: fixture.request.Groups}
	acquisition := &LocalAcquisition{
		store:   &localValidationStore{},
		groups:  groupSource,
		configs: localConfigCache{entries: make(map[localConfigKey]localPreparedConfig)},
	}
	cached, err := acquisition.masterView(fixture.request.Previous, fixture.oldState, fixture.request.Groups)
	if err != nil {
		t.Fatal(err)
	}
	acquisition.master.store(cached)

	id := fixture.request.Previous.ID
	projected, err := acquisition.projectedMasterView(context.Background(), id, time.Now(), acquisitionReadImmediate)
	if err != nil {
		t.Fatalf("project a cached masterchain view with nothing in storage: %v", err)
	}
	if projected != cached {
		t.Fatal("projection rebuilt a cached view bound to the same snapshot")
	}

	rebound := *fixture.request.Groups
	groupSource.projected = &rebound
	rebuilt, err := acquisition.projectedMasterView(context.Background(), id, time.Now(), acquisitionReadImmediate)
	if err != nil {
		t.Fatalf("rebuild a cached masterchain view for a new snapshot with nothing in storage: %v", err)
	}
	if rebuilt == cached || rebuilt.context.Groups != &rebound || rebuilt.previous.State != cached.previous.State {
		t.Fatal("rebuilt view is not the cached pair bound to the new snapshot")
	}
}
