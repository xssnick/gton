package service

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/service/archive"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestNextArchiveMasterDownloadSeqnoUsesActualSequenceBoundary(t *testing.T) {
	tests := []struct {
		name     string
		sequence []PreparedBlock
		target   uint32
		want     uint32
		ok       bool
	}{
		{name: "empty", target: 20},
		{
			name: "next actual block",
			sequence: []PreparedBlock{
				{ID: ton.BlockIDExt{SeqNo: 11}},
				{ID: ton.BlockIDExt{SeqNo: 12}},
			},
			target: 20,
			want:   13,
			ok:     true,
		},
		{
			name:     "target reached",
			sequence: []PreparedBlock{{ID: ton.BlockIDExt{SeqNo: 20}}},
			target:   20,
		},
		{
			name:     "target passed",
			sequence: []PreparedBlock{{ID: ton.BlockIDExt{SeqNo: 21}}},
			target:   20,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, ok := nextArchiveMasterDownloadSeqno(tt.sequence, tt.target)
			if got != tt.want || ok != tt.ok {
				t.Fatalf("next master download = %d,%v want %d,%v", got, ok, tt.want, tt.ok)
			}
		})
	}
}

func TestArchiveMasterDownloadTaskIsSingleConsumer(t *testing.T) {
	downloaded := &archive.Downloaded{Data: []byte{1, 2, 3}}
	taskCtx, cancelTask := context.WithCancel(context.Background())
	finished := make(chan struct{})
	task := &archiveMasterDownloadTask{
		startSeqno: 10,
		cancel:     cancelTask,
		done:       make(chan archiveMasterDownloadResult),
		joined:     finished,
	}
	go func() {
		defer close(finished)
		defer cancelTask()
		task.complete(taskCtx, archiveMasterDownloadResult{downloaded: downloaded})
	}()

	got, err := task.take(context.Background())
	if err != nil || got != downloaded {
		t.Fatalf("take result = %p,%v want %p,nil", got, err, downloaded)
	}
	waitArchiveMasterDownloadProducer(t, finished)
	select {
	case <-taskCtx.Done():
	default:
		t.Fatal("successful master archive prefetch retained its child context")
	}
	if _, err = task.take(context.Background()); err == nil {
		t.Fatal("master archive prefetch was consumed twice")
	}
}

func TestArchiveMasterDownloadTaskDiscardReleasesData(t *testing.T) {
	downloaded := &archive.Downloaded{Data: make([]byte, 4<<20)}
	taskCtx, cancelTask := context.WithCancel(context.Background())
	finished := make(chan struct{})
	task := &archiveMasterDownloadTask{
		startSeqno: 10,
		cancel:     cancelTask,
		done:       make(chan archiveMasterDownloadResult),
		joined:     finished,
	}
	go func() {
		defer close(finished)
		defer cancelTask()
		task.complete(taskCtx, archiveMasterDownloadResult{downloaded: downloaded})
	}()

	task.discard()
	waitArchiveMasterDownloadProducer(t, finished)
	if downloaded.Data != nil {
		t.Fatal("discarded master archive prefetch retained raw bytes")
	}
}

func TestArchiveMasterDownloadTaskCancellationLetsProducerReleaseResult(t *testing.T) {
	downloaded := &archive.Downloaded{Data: make([]byte, 1<<20)}
	taskCtx, cancelTask := context.WithCancel(context.Background())
	finished := make(chan struct{})
	task := &archiveMasterDownloadTask{
		startSeqno: 10,
		cancel:     cancelTask,
		done:       make(chan archiveMasterDownloadResult),
		joined:     finished,
	}
	go func() {
		defer close(finished)
		defer cancelTask()
		<-taskCtx.Done()
		task.complete(taskCtx, archiveMasterDownloadResult{downloaded: downloaded, err: context.Canceled})
	}()

	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	if _, err := task.take(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled prefetch error = %v, want context.Canceled", err)
	}
	waitArchiveMasterDownloadProducer(t, finished)
	if downloaded.Data != nil {
		t.Fatal("canceled master archive prefetch retained late raw bytes")
	}
}

func TestArchiveMasterImportDropsInvalidCacheAndRetriesWhenPeerIsGone(t *testing.T) {
	tests := []struct {
		name   string
		blocks map[storage.BlockRootHash]PreparedBlock
	}{
		{name: "missing next block", blocks: map[storage.BlockRootHash]PreparedBlock{}},
		{
			name: "invalid consensus proof",
			blocks: func() map[storage.BlockRootHash]PreparedBlock {
				block := testArchiveMasterBlock(11, false)
				return map[storage.BlockRootHash]PreparedBlock{storage.BlockKey(block.ID): block}
			}(),
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()

			start := testArchiveStrategyMasterStateWithWorkchains(t, cell.NewDict(32))
			masterShard := archive.ShardID{Workchain: -1, Shard: topShard}
			key := archiveImportCacheKey{masterchainSeqno: 11, shard: masterShard}
			cache := newArchiveImportCache()
			bad := &archiveImportResult{
				stats:  &archive.ImportStats{Peer: "bad-peer", ArchiveID: 1},
				blocks: tt.blocks,
			}
			if _, err := cache.load(ctx, key, func(context.Context) (*archiveImportResult, error) {
				return bad, nil
			}); err != nil {
				t.Fatalf("seed invalid cache: %v", err)
			}

			goodBlock := testArchiveMasterBlock(11, true)
			good := &archiveImportResult{
				stats: &archive.ImportStats{Peer: "good-peer", ArchiveID: 2},
				blocks: map[storage.BlockRootHash]PreparedBlock{
					storage.BlockKey(goodBlock.ID): goodBlock,
				},
			}
			queue := &archiveImportQueue{downloadHot: make(chan archiveDownloadJob, 1)}
			queueUsed := make(chan struct{})
			go func() {
				select {
				case job := <-queue.downloadHot:
					job.done <- archiveImportQueueResult{imported: good}
					close(queueUsed)
				case <-ctx.Done():
				}
			}()

			runner := &archiveCatchUpRun{
				archive:        &ArchiveRunner{log: zerolog.Nop()},
				archiveSession: newTestArchiveSession(t),
				importCache:    cache,
			}
			result, _, err := runner.startArchiveMasterImport(ctx, queue, start.Block, start, 11, nil).wait(ctx)
			if err != nil {
				t.Fatalf("master import retry: %v", err)
			}
			if len(result.masterSequence) != 1 || result.masterSequence[0].ID.SeqNo != 11 {
				t.Fatalf("master sequence after retry = %+v", result.masterSequence)
			}
			select {
			case <-queueUsed:
			case <-ctx.Done():
				t.Fatal("semantic failure did not retry through the archive queue")
			}

			loads := 0
			cached, err := cache.load(ctx, key, func(context.Context) (*archiveImportResult, error) {
				loads++
				return nil, errors.New("unexpected cache miss")
			})
			if err != nil {
				t.Fatalf("load replacement cache entry: %v", err)
			}
			if !cached.cached || loads != 0 || cached.imported.stats.Peer != "good-peer" {
				t.Fatalf("replacement cache = cached:%v loads:%d peer:%q", cached.cached, loads, cached.imported.stats.Peer)
			}
		})
	}
}

func TestArchiveMasterImportDoesNotPrecheckBlockAfterSyncUntil(t *testing.T) {
	const syncUntil = uint32(1_000)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := testArchiveStrategyMasterStateWithWorkchains(t, cell.NewDict(32))
	block := testArchiveMasterBlock(11, false)
	block.Meta.GenUTime = syncUntil + 1
	masterShard := archive.ShardID{Workchain: -1, Shard: topShard}
	key := archiveImportCacheKey{masterchainSeqno: 11, shard: masterShard}
	cache := newArchiveImportCache()
	if _, err := cache.load(ctx, key, func(context.Context) (*archiveImportResult, error) {
		return &archiveImportResult{
			stats: &archive.ImportStats{Peer: "cutoff-peer", ArchiveID: 1},
			blocks: map[storage.BlockRootHash]PreparedBlock{
				storage.BlockKey(block.ID): block,
			},
		}, nil
	}); err != nil {
		t.Fatalf("seed sync_until cache: %v", err)
	}

	runner := &archiveCatchUpRun{
		archive:     &ArchiveRunner{log: zerolog.Nop(), syncUntil: syncUntil},
		importCache: cache,
	}
	result, _, err := runner.startArchiveMasterImport(ctx, nil, start.Block, start, 11, nil).wait(ctx)
	if err != nil {
		t.Fatalf("master import at sync_until: %v", err)
	}
	if len(result.masterSequence) != 1 || result.masterSequence[0].ID.SeqNo != 11 {
		t.Fatalf("sync_until master sequence = %+v", result.masterSequence)
	}
	if len(result.consensusProofs) != 0 {
		t.Fatalf("sync_until block after cutoff produced %d consensus prechecks", len(result.consensusProofs))
	}
}

func waitArchiveMasterDownloadProducer(t *testing.T, finished <-chan struct{}) {
	t.Helper()

	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("master archive download producer did not stop")
	}
}

func TestArchiveMasterOverlapStartRequiresFullyPrecheckedSequence(t *testing.T) {
	sequence := []PreparedBlock{testArchiveMasterBlock(11, false), testArchiveMasterBlock(12, false)}
	proofs := map[uint32]*masterchainConsensusProof{11: {}, 12: {}}

	start, ok := archiveMasterOverlapStart(archiveMasterImportResult{masterSequence: sequence, consensusProofs: proofs})
	if !ok || !start.Equals(&sequence[1].ID) {
		t.Fatalf("overlap start = %s,%v want %s,true", storage.FormatBlockRef(start), ok, storage.FormatBlockRef(sequence[1].ID))
	}

	if _, ok = archiveMasterOverlapStart(archiveMasterImportResult{masterSequence: sequence}); ok {
		t.Fatal("window without prechecked proofs (key-block start) must not overlap the next master import")
	}
	trimmed := map[uint32]*masterchainConsensusProof{11: {}}
	if _, ok = archiveMasterOverlapStart(archiveMasterImportResult{masterSequence: sequence, consensusProofs: trimmed}); ok {
		t.Fatal("partially prechecked sequence must not overlap the next master import")
	}
	if _, ok = archiveMasterOverlapStart(archiveMasterImportResult{}); ok {
		t.Fatal("empty master sequence must not overlap the next master import")
	}
}
