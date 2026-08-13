package service

import (
	"context"
	"testing"

	"github.com/xssnick/gton/service/blocksync"
	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/storage"

	"github.com/rs/zerolog"
)

type internalNonfinalPublisher struct {
	testLiveCheckpointFlusher
	artifacts []storage.LiveBlockArtifacts
	kinds     []storage.LiveBlockNonfinalKind
}

func (p *internalNonfinalPublisher) NonfinalBlockCacheEnabled() bool {
	return true
}

func (p *internalNonfinalPublisher) PublishNonfinalBlockArtifacts(
	artifacts storage.LiveBlockArtifacts,
	kind storage.LiveBlockNonfinalKind,
) error {
	p.artifacts = append(p.artifacts, artifacts)
	p.kinds = append(p.kinds, kind)
	return nil
}

func TestNextSyncedBlockProcessorJobDrainsPriorityFirst(t *testing.T) {
	priorityJobs := make(chan blocksync.SyncedBlock, 1)
	jobs := make(chan blocksync.SyncedBlock, 1)
	jobs <- blocksync.SyncedBlock{CatchUp: true}
	priorityJobs <- blocksync.SyncedBlock{Priority: true}

	got, ok := nextSyncedBlockProcessorJob(context.Background(), priorityJobs, jobs)
	if !ok {
		t.Fatal("expected synced block job")
	}
	if !got.Priority {
		t.Fatalf("got regular job before priority job: %+v", got)
	}
}

func TestNextSyncedBlockProcessorJobUsesRegularAfterPriorityClosed(t *testing.T) {
	priorityJobs := make(chan blocksync.SyncedBlock)
	close(priorityJobs)
	jobs := make(chan blocksync.SyncedBlock, 1)
	jobs <- blocksync.SyncedBlock{CatchUp: true}

	got, ok := nextSyncedBlockProcessorJob(context.Background(), priorityJobs, jobs)
	if !ok {
		t.Fatal("expected regular synced block job")
	}
	if !got.CatchUp {
		t.Fatalf("got unexpected job after priority close: %+v", got)
	}
}

func TestIsHotMasterchainSyncedBlockRequiresPriorityLiveMaster(t *testing.T) {
	master := testMasterBlockID(10)
	if !isHotMasterchainSyncedBlock(blocksync.SyncedBlock{
		Priority:   true,
		Downloaded: p2p.DownloadedBlock{ID: master},
	}) {
		t.Fatal("priority live master block should use hot queue")
	}

	if isHotMasterchainSyncedBlock(blocksync.SyncedBlock{
		Priority:   true,
		CatchUp:    true,
		Downloaded: p2p.DownloadedBlock{ID: master},
	}) {
		t.Fatal("catch-up master block should not use hot queue")
	}

	base := master
	base.Workchain = 0
	if isHotMasterchainSyncedBlock(blocksync.SyncedBlock{
		Priority:   true,
		Downloaded: p2p.DownloadedBlock{ID: base},
	}) {
		t.Fatal("basechain block should not use master hot queue")
	}
}

func TestSyncedBlockSourcePrefersInternalIngress(t *testing.T) {
	got := syncedBlockSource(blocksync.SyncedBlock{Internal: true, CatchUp: true})
	if got != SyncBlockSourceInternal {
		t.Fatalf("source = %q, want internal", got)
	}
}

func TestProcessInternalShardBlockPublishesSignedNonfinalState(t *testing.T) {
	downloaded := testBroadcastShardBlock(t, 401)
	publisher := &internalNonfinalPublisher{}
	coordinator := &SyncCoordinator{
		log:               zerolog.Nop(),
		liveState:         publisher,
		shardPrepareQueue: make(chan shardPrepareRequest, 1),
	}

	err := coordinator.processSyncedBlock(context.Background(), blocksync.SyncedBlock{
		Downloaded: *downloaded,
		Internal:   true,
	})
	if err != nil {
		t.Fatalf("process internal shard block: %v", err)
	}
	if len(publisher.artifacts) != 1 {
		t.Fatalf("non-final publications = %d, want 1", len(publisher.artifacts))
	}
	got := publisher.artifacts[0]
	if !got.Block.Equals(&downloaded.ID) || got.Root != downloaded.Block || got.StateUpdate != downloaded.StateUpdate {
		t.Fatalf("published non-final artifact does not preserve the verified block: %+v", got)
	}
	if len(publisher.kinds) != 1 || publisher.kinds[0] != storage.LiveBlockNonfinalSigned {
		t.Fatalf("non-final publication kinds = %v, want signed", publisher.kinds)
	}
}
