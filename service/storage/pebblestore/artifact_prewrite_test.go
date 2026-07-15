package pebblestore

import (
	"bytes"
	"context"
	"os"
	"path/filepath"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/ton"
)

func testPrewriteServedBlock(block ton.BlockIDExt, masterSeqno uint32, blockData []byte, proofData []byte) *storage.ServedBlockFull {
	meta := &storage.BlockMeta{
		ID:       block,
		GenUTime: block.SeqNo * 10,
		StartLT:  uint64(block.SeqNo),
		EndLT:    uint64(block.SeqNo + 1),
	}
	if block.Workchain != -1 || block.Shard != topShard {
		meta.MasterchainRefSeqno = masterSeqno
	}
	return &storage.ServedBlockFull{
		ID:    block,
		Block: blockData,
		Proof: proofData,
		Meta:  meta,
	}
}

func testArchivePackagesSize(t *testing.T, store *Store) int64 {
	t.Helper()
	var total int64
	root := filepath.Join(store.dir, "archive", "packages")
	err := filepath.Walk(root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	if err != nil && !os.IsNotExist(err) {
		t.Fatalf("walk archive packages: %v", err)
	}
	return total
}

func TestPrewriteCheckpointArtifactsConsumedByCheckpoint(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	master := testServedBlockID(-1, topShard, 500, 0x51)
	shard := testServedBlockID(0, int64(0x4000000000000000), 700, 0x52)
	entries := []storage.StateCheckpointBlock{
		{Artifact: testPrewriteServedBlock(master, 0, []byte{0x11, 0x12}, []byte{0x21})},
		{Artifact: testPrewriteServedBlock(shard, master.SeqNo, []byte{0x31, 0x32, 0x33}, []byte{0x41})},
	}

	if err = store.PrewriteCheckpointArtifacts(entries); err != nil {
		t.Fatalf("prewrite checkpoint artifacts: %v", err)
	}
	if got := len(store.prewrittenArtifacts); got != 2 {
		t.Fatalf("prewritten artifact records = %d, want 2", got)
	}
	if len(store.prewrittenPackRegs) == 0 {
		t.Fatal("prewrite produced no pack registrations")
	}
	sizeAfterPrewrite := testArchivePackagesSize(t, store)
	if sizeAfterPrewrite == 0 {
		t.Fatal("prewrite did not append pack data")
	}

	if _, err = store.SaveStateCheckpointEntries(context.Background(), entries, storage.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save state checkpoint entries: %v", err)
	}

	if got := testArchivePackagesSize(t, store); got != sizeAfterPrewrite {
		t.Fatalf("archive packages size after checkpoint = %d, want unchanged %d (checkpoint must not re-append prewritten artifacts)", got, sizeAfterPrewrite)
	}
	if got := len(store.prewrittenArtifacts); got != 0 {
		t.Fatalf("prewritten artifact records after checkpoint = %d, want 0", got)
	}
	if got := len(store.prewrittenPackRegs); got != 0 {
		t.Fatalf("prewritten pack registrations after checkpoint = %d, want 0", got)
	}
	if got := len(store.pendingArchiveSync); got != 0 {
		t.Fatalf("pending archive sync entries after checkpoint = %d, want 0", got)
	}

	for _, entry := range entries {
		full, err := store.BlockFull(context.Background(), entry.Artifact.ID)
		if err != nil {
			t.Fatalf("read block full %s: %v", storage.FormatBlockRef(entry.Artifact.ID), err)
		}
		if !bytes.Equal(full.Block, entry.Artifact.Block) {
			t.Fatalf("block data %s = %x, want %x", storage.FormatBlockRef(entry.Artifact.ID), full.Block, entry.Artifact.Block)
		}
		if !bytes.Equal(full.Proof, entry.Artifact.Proof) {
			t.Fatalf("block proof %s = %x, want %x", storage.FormatBlockRef(entry.Artifact.ID), full.Proof, entry.Artifact.Proof)
		}
	}

	if _, err = store.ArchiveInfo(context.Background(), int32(master.SeqNo), -1, topShard); err != nil {
		t.Fatalf("archive info for prewritten master pack: %v", err)
	}
}

func TestPrewriteCheckpointArtifactsMismatchFallsBackToInlineAppend(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	master := testServedBlockID(-1, topShard, 600, 0x61)
	staged := testPrewriteServedBlock(master, 0, []byte{0x11}, []byte{0x21})
	if err = store.PrewriteCheckpointArtifacts([]storage.StateCheckpointBlock{{Artifact: staged}}); err != nil {
		t.Fatalf("prewrite checkpoint artifacts: %v", err)
	}
	sizeAfterPrewrite := testArchivePackagesSize(t, store)

	replaced := testPrewriteServedBlock(master, 0, []byte{0x11}, []byte{0x22, 0x23})
	if _, err = store.SaveStateCheckpointEntries(context.Background(), []storage.StateCheckpointBlock{{Artifact: replaced}}, storage.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save state checkpoint entries: %v", err)
	}

	if got := testArchivePackagesSize(t, store); got <= sizeAfterPrewrite {
		t.Fatalf("archive packages size after mismatch checkpoint = %d, want > %d (inline re-append expected)", got, sizeAfterPrewrite)
	}
	proof, err := store.BlockProof(context.Background(), storage.ServedProofBlock, master)
	if err != nil {
		t.Fatalf("read block proof: %v", err)
	}
	if !bytes.Equal(proof, replaced.Proof) {
		t.Fatalf("block proof = %x, want replaced proof %x", proof, replaced.Proof)
	}
	if got := len(store.prewrittenArtifacts); got != 0 {
		t.Fatalf("prewritten artifact records after mismatch consume = %d, want 0", got)
	}
}

func TestPrewriteCheckpointArtifactsAbandonDropsPrewrittenState(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	master := testServedBlockID(-1, topShard, 700, 0x71)
	entries := []storage.StateCheckpointBlock{
		{Artifact: testPrewriteServedBlock(master, 0, []byte{0x11, 0x12}, []byte{0x21})},
	}
	if err = store.PrewriteCheckpointArtifacts(entries); err != nil {
		t.Fatalf("prewrite checkpoint artifacts: %v", err)
	}
	if testArchivePackagesSize(t, store) == 0 {
		t.Fatal("prewrite did not append pack data")
	}

	// A failed checkpoint truncates every pending pack tail; the streamed refs
	// must die with it so nothing consumes refs into truncated space. Only the
	// pack header written at file creation survives the truncation.
	store.artifactPublishMu.Lock()
	store.abandonPendingArtifactPacks()
	store.artifactPublishMu.Unlock()

	sizeAfterAbandon := testArchivePackagesSize(t, store)
	if sizeAfterAbandon > 4 {
		t.Fatalf("archive packages size after abandon = %d, want at most the pack header", sizeAfterAbandon)
	}
	if store.prewrittenArtifacts != nil {
		t.Fatal("prewritten artifact records survived abandon")
	}
	if store.prewrittenPackRegs != nil {
		t.Fatal("prewritten pack registrations survived abandon")
	}

	if _, err = store.SaveStateCheckpointEntries(context.Background(), entries, storage.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save state checkpoint entries after abandon: %v", err)
	}
	data, err := store.BlockData(context.Background(), master)
	if err != nil {
		t.Fatalf("read block data after abandon: %v", err)
	}
	if !bytes.Equal(data, entries[0].Artifact.Block) {
		t.Fatalf("block data = %x, want %x", data, entries[0].Artifact.Block)
	}
}

func TestPrewriteCheckpointArtifactsWithStateUsesMasterchainRef(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	master := testServedBlockID(-1, topShard, 20150, 0x4a)
	shard := testServedBlockID(0, int64(0x4000000000000000), 1000, 0x4b)
	artifact := testPrewriteServedBlock(shard, 0, []byte{0x01}, []byte{0x02})
	// The artifact meta carries no masterchain ref; it must come from the state
	// exactly like on the inline checkpoint path.
	artifact.Meta.MasterchainRefSeqno = 0
	entry := storage.StateCheckpointBlock{
		State: &storage.BlockState{
			Block:          shard,
			MasterchainRef: &master,
			StateRootHash:  bytes.Repeat([]byte{0x4c}, 32),
		},
		Artifact: artifact,
	}

	if err = store.PrewriteCheckpointArtifacts([]storage.StateCheckpointBlock{entry}); err != nil {
		t.Fatalf("prewrite checkpoint artifacts: %v", err)
	}

	store.artifactPublishMu.Lock()
	registrations := store.drainPrewrittenPackRegistrations()
	store.artifactPublishMu.Unlock()
	if len(registrations) != 1 {
		t.Fatalf("prewritten registrations = %d, want 1", len(registrations))
	}
	reg := registrations[0]
	if reg.baseSeq != 20000 || reg.startSeq != 20100 {
		t.Fatalf("prewritten registration base/start = %d/%d, want 20000/20100", reg.baseSeq, reg.startSeq)
	}
	if reg.workchain != shard.Workchain || reg.shard != shard.Shard {
		t.Fatalf("prewritten registration shard = %d/%016x, want %d/%016x", reg.workchain, uint64(reg.shard), shard.Workchain, uint64(shard.Shard))
	}
}

// Regression: two prewrite calls in one checkpoint window each open a new
// dynamic pack of the same slice (the shard pack and the master pack of slice
// 20100). Allocation reads committed state only, so the second call must see
// the first call's staged registration or both packs get the same archive id
// ("archive package descriptor mismatch").
func TestPrewriteSeparateCallsAllocateDistinctPackIDs(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	master := testServedBlockID(-1, topShard, 20150, 0x81)
	shard := testServedBlockID(0, int64(0x4000000000000000), 900, 0x82)
	entries := []storage.StateCheckpointBlock{
		{Artifact: testPrewriteServedBlock(shard, master.SeqNo, []byte{0x11, 0x12}, []byte{0x13})},
		{Artifact: testPrewriteServedBlock(master, 0, []byte{0x21, 0x22}, []byte{0x23})},
	}

	if err = store.PrewriteCheckpointArtifacts(entries[:1]); err != nil {
		t.Fatalf("prewrite shard artifact: %v", err)
	}
	if err = store.PrewriteCheckpointArtifacts(entries[1:]); err != nil {
		t.Fatalf("prewrite master artifact: %v", err)
	}
	if got := len(store.prewrittenPackRegs); got != 2 {
		t.Fatalf("staged pack registrations = %d, want 2 distinct ids", got)
	}

	if _, err = store.SaveStateCheckpointEntries(context.Background(), entries, storage.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save state checkpoint entries: %v", err)
	}
	for _, entry := range entries {
		full, err := store.BlockFull(context.Background(), entry.Artifact.ID)
		if err != nil {
			t.Fatalf("read block full %s: %v", storage.FormatBlockRef(entry.Artifact.ID), err)
		}
		if !bytes.Equal(full.Block, entry.Artifact.Block) {
			t.Fatalf("block data %s = %x, want %x", storage.FormatBlockRef(entry.Artifact.ID), full.Block, entry.Artifact.Block)
		}
	}
}

// Regression: a checkpoint whose inline (miss-path) append opens a new pack
// must allocate around the still-staged prewritten registrations; draining
// them before the build used to let the inline append reuse their indexes.
func TestCheckpointInlineAppendSeesStagedPackRegistrations(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	master := testServedBlockID(-1, topShard, 20150, 0x91)
	shard := testServedBlockID(0, int64(0x4000000000000000), 950, 0x92)
	entries := []storage.StateCheckpointBlock{
		{Artifact: testPrewriteServedBlock(shard, master.SeqNo, []byte{0x31, 0x32}, []byte{0x33})},
		{Artifact: testPrewriteServedBlock(master, 0, []byte{0x41, 0x42}, []byte{0x43})},
	}

	// Only the shard block is prewritten; the master block reaches the
	// checkpoint inline and its pack is created by the miss-path append.
	if err = store.PrewriteCheckpointArtifacts(entries[:1]); err != nil {
		t.Fatalf("prewrite shard artifact: %v", err)
	}
	if _, err = store.SaveStateCheckpointEntries(context.Background(), entries, storage.StateCellRecords{}, nil); err != nil {
		t.Fatalf("save state checkpoint entries: %v", err)
	}
	if got := len(store.prewrittenPackRegs); got != 0 {
		t.Fatalf("prewritten pack registrations after checkpoint = %d, want 0", got)
	}
	for _, entry := range entries {
		full, err := store.BlockFull(context.Background(), entry.Artifact.ID)
		if err != nil {
			t.Fatalf("read block full %s: %v", storage.FormatBlockRef(entry.Artifact.ID), err)
		}
		if !bytes.Equal(full.Block, entry.Artifact.Block) {
			t.Fatalf("block data %s = %x, want %x", storage.FormatBlockRef(entry.Artifact.ID), full.Block, entry.Artifact.Block)
		}
	}
}

func TestPackWritebackDueAdvancesWatermark(t *testing.T) {
	store, err := Open(Options{Dir: t.TempDir()})
	if err != nil {
		t.Fatalf("open store: %v", err)
	}
	defer func() { _ = store.Close() }()

	store.artifactMu.Lock()
	store.pendingArchiveSync["small.pack"] = pendingPackWrite{size: packWritebackBytesThreshold - 1}
	store.pendingArchiveSync["large.pack"] = pendingPackWrite{size: packWritebackBytesThreshold + 5}
	store.pendingArchiveSync["synced.pack"] = pendingPackWrite{
		size:          2*packWritebackBytesThreshold + 10,
		writebackSize: 5,
	}
	store.artifactMu.Unlock()

	due := store.packWritebackDue()
	if len(due) != 2 || due[0] != "large.pack" || due[1] != "synced.pack" {
		t.Fatalf("writeback due = %v, want [large.pack synced.pack]", due)
	}

	store.artifactMu.Lock()
	defer store.artifactMu.Unlock()
	if got := store.pendingArchiveSync["large.pack"].writebackSize; got != packWritebackBytesThreshold+5 {
		t.Fatalf("large.pack writeback watermark = %d, want %d", got, packWritebackBytesThreshold+5)
	}
	if got := store.pendingArchiveSync["synced.pack"].writebackSize; got != 2*packWritebackBytesThreshold+10 {
		t.Fatalf("synced.pack writeback watermark = %d, want %d", got, 2*packWritebackBytesThreshold+10)
	}
	if got := store.pendingArchiveSync["small.pack"].writebackSize; got != 0 {
		t.Fatalf("small.pack writeback watermark = %d, want 0", got)
	}
}
