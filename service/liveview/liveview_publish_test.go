package liveview

import (
	"bytes"
	"context"
	"encoding/binary"
	"testing"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// An accepted publication can arrive for a block the applied current state has
// already passed: acceptance runs behind the sync pipeline, which applied the
// block, flushed it and let the ordinary cache limit evict it. Nothing enrols
// such a block in the accepted-state bound, and no flush will ever come for it
// again, so whatever that publication installs is pinned for good — the live
// block with its full state, and the unflushed block data in the block cache.
// The observers still hear about it.
func TestAcceptedPublicationOfACoveredBlockPinsNothing(t *testing.T) {
	live, _ := acceptedStateStore(t, Options{MasterBlockCache: 4, ShardBlockCache: 1})
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno-1, 0x3c)

	var notified []storage.LiveBlockArtifacts
	stop := live.ObserveAcceptedBlockStates(func(artifacts storage.LiveBlockArtifacts) {
		notified = append(notified, artifacts)
	})
	defer stop()

	if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
		t.Fatalf("publish the accepted state of a covered block: %v", err)
	}
	if len(notified) != 1 || notified[0].State == nil || notified[0].State.Cell != fixture.state {
		t.Fatalf("observers were notified %d times, want once with the published state", len(notified))
	}

	for step := uint32(1); step <= 5; step++ {
		advanceAppliedShardTop(t, live, acceptedStateAppliedSeqno+step, 900+step, byte(0x60+2*step))
	}

	live.mu.RLock()
	cached := live.blocks[storage.BlockKey(fixture.block)]
	pinned := cached != nil && !live.liveBlockEvictableLocked(cached)
	live.mu.RUnlock()
	if pinned {
		t.Fatal("the covered accepted publication left a live block the cache limit can never evict")
	}
	if data, err := live.liveBlockCache.CachedBlockData(context.Background(), fixture.block); err == nil && !data.ArtifactFlushed {
		t.Fatal("the covered accepted publication left unflushed block data nothing will flush")
	}
	if blocks := live.AcceptedStateBlocks(); len(blocks) != 0 {
		t.Fatalf("accepted publications = %d, want none for a covered block", len(blocks))
	}
}

// The other shape of a covered block: the sync pipeline's own copy is still
// resident and waiting for its checkpoint flush. It serves the block, so the
// late accepted publication must neither take the entry over nor swap in its
// own materialization of the state.
func TestAcceptedPublicationOfACoveredBlockLeavesThePipelineCopy(t *testing.T) {
	live, _ := acceptedStateStore(t, Options{MasterBlockCache: 4, ShardBlockCache: 64})
	fixture := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+1, 0x3e)

	pipeline := fixture.artifacts()
	pipelineState := cell.BeginCell().
		MustStoreUInt(uint64(0x3e)+0x100, 32).
		MustStoreRef(cell.BeginCell().MustStoreUInt(uint64(fixture.block.SeqNo), 32).EndCell()).
		EndCell()
	if !bytes.Equal(pipelineState.Hash(), fixture.state.Hash()) {
		t.Fatal("the pipeline fixture is not the same state, so it does not model the same block")
	}
	pipeline.State = &storage.BlockState{
		Block:         fixture.block,
		StateRootHash: pipelineState.Hash(),
		Cell:          pipelineState,
	}
	pipeline.AvailabilityOnly = true
	if err := live.PublishLiveBlockArtifacts(pipeline); err != nil {
		t.Fatalf("publish the pipeline copy of the block: %v", err)
	}
	advanceAppliedShardTop(t, live, fixture.block.SeqNo+2, 910, 0x70)

	if err := live.PublishAcceptedBlockState(fixture.artifacts()); err != nil {
		t.Fatalf("publish the accepted state of a covered block: %v", err)
	}

	live.mu.RLock()
	cached := live.blocks[storage.BlockKey(fixture.block)]
	live.mu.RUnlock()
	if cached == nil || cached.acceptedOwner {
		t.Fatal("the covered accepted publication took over the pipeline's live block")
	}
	state, err := live.BlockState(context.Background(), fixture.block)
	if err != nil {
		t.Fatalf("state of the pipeline's block: %v", err)
	}
	if state.Cell != pipelineState {
		t.Fatal("the covered accepted publication replaced the pipeline's state materialization")
	}
}

// The observers of an accepted publication are the message pool, which exists to
// move at acceptance rather than a second later. The non-final promotion the same
// publication unblocks rebuilds whole waiting blocks, so it runs after them — and
// still before the publication returns.
func TestAcceptedPublicationNotifiesObserversBeforeTheNonfinalPromotion(t *testing.T) {
	live, _ := acceptedStateStore(t, Options{MasterBlockCache: 4, ShardBlockCache: 64, NonFinalEnabled: true})
	accepted := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+1, 0x42)
	successor := newAcceptedBlockFixture(t, acceptedStateAppliedSeqno+2, 0x44)

	ingest := successor.ingestArtifacts(t, accepted.state, accepted.block)
	if err := live.PublishNonfinalBlockArtifacts(ingest, storage.LiveBlockNonfinalSigned); err != nil {
		t.Fatalf("publish the successor through the ingest: %v", err)
	}
	if waiting := live.nonfinalWaitingLen(); waiting != 1 {
		t.Fatalf("waiting non-final blocks = %d, want the successor waiting for its parent", waiting)
	}

	waitingDuringObserver := -1
	stop := live.ObserveAcceptedBlockStates(func(storage.LiveBlockArtifacts) {
		waitingDuringObserver = live.nonfinalWaitingLen()
	})
	defer stop()

	if err := live.PublishAcceptedBlockState(accepted.artifacts()); err != nil {
		t.Fatalf("publish the accepted parent: %v", err)
	}
	if waitingDuringObserver != 1 {
		t.Fatalf("waiting non-final blocks seen by the observer = %d, want the successor not yet promoted", waitingDuringObserver)
	}
	if waiting := live.nonfinalWaitingLen(); waiting != 0 {
		t.Fatalf("waiting non-final blocks after the publication = %d, want the successor promoted", waiting)
	}
	if signed, _ := live.NonfinalPendingShardBlocks(nil); len(signed) != 1 || !blockIDEqual(signed[0], successor.block) {
		t.Fatalf("pending non-final signed blocks = %v, want the promoted successor", signed)
	}
}

// The non-final cell index keeps one entry per record of a block, without a
// dedupe of its own, because both producers of those records already emit each
// cell hash once. The fixture reaches one new cell through two references so a
// producer that stopped deduplicating would show up here.
func TestNonfinalCellIndexHoldsOneEntryPerRecord(t *testing.T) {
	leaf := cell.BeginCell().MustStoreUInt(0x51, 16).EndCell()
	shared := cell.BeginCell().MustStoreUInt(0x52, 16).MustStoreRef(leaf).EndCell()
	from := cell.BeginCell().MustStoreUInt(0x53, 16).MustStoreRef(leaf).EndCell()
	to := cell.BeginCell().MustStoreUInt(0x54, 16).MustStoreRef(shared).MustStoreRef(shared).EndCell()
	update, err := cell.CreateMerkleUpdate(from, to)
	if err != nil {
		t.Fatalf("create merkle update: %v", err)
	}
	updateRecords, err := storage.PrepareStateUpdateCells(update)
	if err != nil {
		t.Fatalf("prepare state update cells: %v", err)
	}
	snapshotRecords, err := nonfinalSnapshotStateRecords(to)
	if err != nil {
		t.Fatalf("prepare snapshot records: %v", err)
	}

	for name, records := range map[string]storage.StateCellRecords{
		"state update": updateRecords,
		"snapshot":     snapshotRecords,
	} {
		t.Run(name, func(t *testing.T) {
			live := New(noopBacking{}, Options{NonFinalEnabled: true})
			block := testNonfinalIndexBlock(12, masterchainShard)
			key := storage.BlockKey(block)

			live.mu.Lock()
			defer live.mu.Unlock()

			live.putNonfinalPendingLocked(key, liveNonfinalPending{block: block, cells: records})
			if len(live.nonFinalCellIndex) != records.Len() {
				t.Fatalf("indexed hashes = %d, want one per record (%d)", len(live.nonFinalCellIndex), records.Len())
			}
			_ = records.ForEach(func(record storage.EncodedCellRecord) error {
				if entries := live.nonFinalCellIndex[record.Hash]; len(entries) != 1 || entries[0].block != key {
					t.Errorf("index entries for %x = %d, want one for the block", record.Hash[:4], len(entries))
				}
				return nil
			})

			live.deleteNonfinalPendingLocked(key)
			if len(live.nonFinalCellIndex) != 0 {
				t.Fatalf("indexed hashes after the delete = %d, want none", len(live.nonFinalCellIndex))
			}
		})
	}
}

func BenchmarkNonfinalCellIndexPutDelete(b *testing.B) {
	records := make([]storage.EncodedCellRecord, 4096)
	for i := range records {
		binary.BigEndian.PutUint32(records[i].Hash[:], uint32(i))
		records[i].Data = []byte{byte(i), 0x01}
	}
	cells := storage.NewStateCellRecords(records)
	live := New(noopBacking{}, Options{NonFinalEnabled: true})
	block := testNonfinalIndexBlock(12, masterchainShard)
	key := storage.BlockKey(block)

	b.ReportAllocs()
	for b.Loop() {
		live.mu.Lock()
		live.putNonfinalPendingLocked(key, liveNonfinalPending{block: block, cells: cells})
		live.deleteNonfinalPendingLocked(key)
		live.mu.Unlock()
	}
}
