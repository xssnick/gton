package storage

import (
	"bytes"
	"context"
	"encoding/binary"
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestBuildBlockMetaFromParsedShardBlockDoesNotUseHeaderMasterRef(t *testing.T) {
	id := ton.BlockIDExt{
		Workchain: 0,
		Shard:     masterchainShard,
		SeqNo:     21,
		RootHash:  bytes.Repeat([]byte{0x55}, 32),
		FileHash:  bytes.Repeat([]byte{0x66}, 32),
	}
	masterRef := tlb.ExtBlkRef{
		EndLt:    100,
		SeqNo:    19,
		RootHash: bytes.Repeat([]byte{0x11}, 32),
		FileHash: bytes.Repeat([]byte{0x22}, 32),
	}
	prevRef := tlb.ExtBlkRef{
		EndLt:    90,
		SeqNo:    20,
		RootHash: bytes.Repeat([]byte{0x33}, 32),
		FileHash: bytes.Repeat([]byte{0x44}, 32),
	}

	var header tlb.BlockHeader
	header.NotMaster = true
	header.MasterRef = &masterRef
	header.PrevRef = tlb.BlkPrevInfo{Prev1: prevRef}

	meta, err := BuildBlockMetaFromParsedBlock(id, &tlb.Block{BlockInfo: header})
	if err != nil {
		t.Fatalf("build meta: %v", err)
	}
	// Keep this aligned with cppnode: BlockInfo.MasterRef is validated as part
	// of the shard header, but BlockHandle::masterchain_ref_block is filled later
	// from the masterchain block that actually applies/includes the shard block.
	if meta.MasterchainRefKnown() {
		t.Fatalf("header master ref leaked into block meta: %d", meta.MasterchainRefSeqno)
	}
}

func TestMergeBlockMetaOverwritesKnownZeroMasterchainRef(t *testing.T) {
	shardZero := ton.BlockIDExt{Workchain: 0, Shard: masterchainShard, SeqNo: 0}
	base := &BlockMeta{ID: shardZero, MasterchainRefSeqno: 10}
	next := &BlockMeta{ID: shardZero}

	merged := MergeBlockMeta(base, next)
	if !merged.MasterchainRefKnown() {
		t.Fatal("zero-state shard master ref is not known")
	}
	if merged.MasterchainRefSeqno != 0 {
		t.Fatalf("masterchain ref seqno = %d, want 0", merged.MasterchainRefSeqno)
	}
}

func TestCloneBlockStateCopiesBlockIDHashes(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     masterchainShard,
		SeqNo:     20,
		RootHash:  bytes.Repeat([]byte{0x33}, 32),
		FileHash:  bytes.Repeat([]byte{0x44}, 32),
	}
	ref := ton.BlockIDExt{
		Workchain: -1,
		Shard:     masterchainShard,
		SeqNo:     21,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	state := &BlockState{Block: block, MasterchainRef: &ref}

	cloned := CloneBlockState(state)
	cloned.Block.RootHash[0] = 0xCC
	cloned.Block.FileHash[0] = 0xDD
	cloned.MasterchainRef.RootHash[0] = 0xAA
	cloned.MasterchainRef.FileHash[0] = 0xBB

	if state.Block.RootHash[0] == 0xCC {
		t.Fatal("clone shares block root hash backing array")
	}
	if state.Block.FileHash[0] == 0xDD {
		t.Fatal("clone shares block file hash backing array")
	}
	if state.MasterchainRef.RootHash[0] == 0xAA {
		t.Fatal("clone shares masterchain ref root hash backing array")
	}
	if state.MasterchainRef.FileHash[0] == 0xBB {
		t.Fatal("clone shares masterchain ref file hash backing array")
	}

	withoutCells := BlockStateWithoutCells(state)
	withoutCells.Block.RootHash[0] = 0xEE
	if state.Block.RootHash[0] == 0xEE {
		t.Fatal("metadata clone shares block root hash backing array")
	}
}

func TestBlockMetaCloneCopiesBlockIDHashes(t *testing.T) {
	id := ton.BlockIDExt{
		Workchain: 0,
		Shard:     masterchainShard,
		SeqNo:     20,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x12}, 32),
	}
	prev := ton.BlockIDExt{
		Workchain: 0,
		Shard:     masterchainShard,
		SeqNo:     19,
		RootHash:  bytes.Repeat([]byte{0x21}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
	next := ton.BlockIDExt{
		Workchain: 0,
		Shard:     masterchainShard,
		SeqNo:     21,
		RootHash:  bytes.Repeat([]byte{0x31}, 32),
		FileHash:  bytes.Repeat([]byte{0x32}, 32),
	}
	meta := &BlockMeta{ID: id, PrevRefs: []ton.BlockIDExt{prev}, NextRefs: []ton.BlockIDExt{next}}

	cloned := meta.Clone()
	cloned.ID.RootHash[0] = 0xAA
	cloned.ID.FileHash[0] = 0xAB
	cloned.PrevRefs[0].RootHash[0] = 0xBA
	cloned.PrevRefs[0].FileHash[0] = 0xBB
	cloned.NextRefs[0].RootHash[0] = 0xCA
	cloned.NextRefs[0].FileHash[0] = 0xCB

	if meta.ID.RootHash[0] == 0xAA || meta.ID.FileHash[0] == 0xAB {
		t.Fatal("clone shares meta id hash backing arrays")
	}
	if meta.PrevRefs[0].RootHash[0] == 0xBA || meta.PrevRefs[0].FileHash[0] == 0xBB {
		t.Fatal("clone shares prev ref hash backing arrays")
	}
	if meta.NextRefs[0].RootHash[0] == 0xCA || meta.NextRefs[0].FileHash[0] == 0xCB {
		t.Fatal("clone shares next ref hash backing arrays")
	}
}

func TestStoredProofKindsForServedBlockTreatsNonMasterAsProofLink(t *testing.T) {
	shard := ton.BlockIDExt{Workchain: 0, Shard: masterchainShard, SeqNo: 21}
	got := StoredProofKindsForServedBlock(shard, false, false)
	if len(got) != 1 || got[0] != ServedProofBlockLink {
		t.Fatalf("proof kinds = %#v, want shard proof link", got)
	}

	master := ton.BlockIDExt{Workchain: masterchainID, Shard: masterchainShard, SeqNo: 21}
	got = StoredProofKindsForServedBlock(master, false, true)
	if len(got) != 2 || got[0] != ServedProofBlock || got[1] != ServedProofKeyBlock {
		t.Fatalf("proof kinds = %#v, want master full proof kinds", got)
	}
}

func TestLoadCellGraphBuildsScheduledSharedRefs(t *testing.T) {
	shared := cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell()
	left := cell.BeginCell().MustStoreUInt(0xBB, 8).MustStoreRef(shared).EndCell()
	root := cell.BeginCell().MustStoreRef(shared).MustStoreRef(left).EndCell()

	records, err := CollectCellRecords(root)
	if err != nil {
		t.Fatalf("collect cell records: %v", err)
	}
	byHash := make(map[cell.Hash]*CellRecord, len(records))
	for _, record := range records {
		var hash cell.Hash
		copy(hash[:], record.Hash)
		byHash[hash] = record
	}

	rootHash := root.HashKey()
	loaded, err := LoadCellGraph(context.Background(), rootHash[:], func(hash []byte) (*CellRecord, error) {
		var key cell.Hash
		copy(key[:], hash)
		record := byHash[key]
		if record == nil {
			t.Fatalf("unexpected missing record %x", hash)
		}
		return record, nil
	})
	if err != nil {
		t.Fatalf("load cell graph: %v", err)
	}
	if !bytes.Equal(loaded.ToBOC(), root.ToBOC()) {
		t.Fatalf("loaded cell mismatch")
	}
}

func TestLargeBOCRecordsFromCellRecords(t *testing.T) {
	partial := cell.BeginCell().MustStoreUInt(0b10101, 5).EndCell()
	shared := cell.BeginCell().MustStoreUInt(0xAA, 8).MustStoreRef(partial).EndCell()
	root := cell.BeginCell().MustStoreUInt(0b111, 3).MustStoreRef(shared).MustStoreRef(shared).EndCell()

	records, err := CollectCellRecords(root)
	if err != nil {
		t.Fatalf("collect cell records: %v", err)
	}
	loader := largeBOCTestLoader{records: make(map[cell.Hash]*CellRecord, len(records))}
	for _, record := range records {
		var hash cell.Hash
		copy(hash[:], record.Hash)
		loader.records[hash] = record
	}

	var buf bytes.Buffer
	rootHash := root.HashKey()
	err = cell.ToLargeBOC(&buf, []cell.Hash{rootHash}, cell.BOCSerializeOptions{
		WithCRC32C:    true,
		WithIndex:     true,
		WithCacheBits: true,
		WithTopHash:   true,
		WithIntHashes: true,
	}, loader, uint64(len(records)), 1<<17)
	if err != nil {
		t.Fatalf("serialize large boc: %v", err)
	}

	loaded, err := cell.FromBOC(buf.Bytes())
	if err != nil {
		t.Fatalf("parse large boc: %v", err)
	}
	if loaded.HashKey() != rootHash {
		t.Fatalf("root hash mismatch: got=%x want=%x", loaded.Hash(), root.Hash())
	}
}

func TestAppendLargeBOCPayloadRecordFromCellRecordUsesArena(t *testing.T) {
	root := cell.BeginCell().MustStoreUInt(0b10101, 5).EndCell()
	records, err := CollectCellRecords(root)
	if err != nil {
		t.Fatalf("collect cell records: %v", err)
	}
	if len(records) != 1 {
		t.Fatalf("records = %d, want 1", len(records))
	}

	record := records[0]
	originalData := bytes.Clone(record.Data)
	arena := []byte{0xAA}
	payload, arena, err := AppendLargeBOCPayloadRecordFromCellRecord(record, arena)
	if err != nil {
		t.Fatalf("append payload record: %v", err)
	}
	if !bytes.Equal(record.Data, originalData) {
		t.Fatal("source record data was mutated")
	}
	if len(payload.Data) == 0 {
		t.Fatal("payload data is empty")
	}
	if &payload.Data[0] == &record.Data[0] {
		t.Fatal("payload data aliases source record data")
	}
	if len(arena) != 1+len(payload.Data) {
		t.Fatalf("arena size = %d, want %d", len(arena), 1+len(payload.Data))
	}

	want, err := LargeBOCPayloadRecordFromCellRecord(record)
	if err != nil {
		t.Fatalf("payload record: %v", err)
	}
	if !bytes.Equal(payload.Data, want.Data) {
		t.Fatal("arena payload diverges from normal payload")
	}
}

func TestLargeBOCEncodedRecordsMatchCellRecords(t *testing.T) {
	leaf := cell.BeginCell().MustStoreUInt(0b10101, 5).EndCell()
	shared := cell.BeginCell().MustStoreUInt(0xAA, 8).MustStoreRef(leaf).EndCell()
	root := cell.BeginCell().MustStoreUInt(0b111, 3).MustStoreRef(shared).MustStoreRef(shared).EndCell()
	pruned := mustStoragePrunedBranch(t, root)
	update, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.MerkleUpdateCellType), 8).
		MustStoreSlice(pruned.Hash(0), 256).
		MustStoreSlice(root.Hash(0), 256).
		MustStoreUInt(uint64(pruned.Depth(0)), 16).
		MustStoreUInt(uint64(root.Depth(0)), 16).
		MustStoreRef(pruned).
		MustStoreRef(root).
		EndCellSpecial(true)
	if err != nil {
		t.Fatalf("create merkle update: %v", err)
	}

	for _, cl := range []*cell.Cell{leaf, shared, root, pruned, update} {
		record, err := CellRecordFromCell(cl)
		if err != nil {
			t.Fatalf("cell record: %v", err)
		}
		encoded, err := PrepareEncodedCellRecordFromCellMetadata(cl, cl.GetMetadata())
		if err != nil {
			t.Fatalf("encoded cell record: %v", err)
		}

		wantMeta, err := LargeBOCMetaRecordFromCellRecord(record)
		if err != nil {
			t.Fatalf("meta from record: %v", err)
		}
		gotMeta, err := LargeBOCMetaRecordFromEncodedCellRecord(record.Hash, encoded.Data)
		if err != nil {
			t.Fatalf("meta from encoded: %v", err)
		}
		if gotMeta != wantMeta {
			t.Fatalf("encoded meta mismatch for %x", record.Hash)
		}

		wantPayload, err := LargeBOCPayloadRecordFromCellRecord(record)
		if err != nil {
			t.Fatalf("payload from record: %v", err)
		}
		arena := []byte{0xAA}
		gotPayload, arena, err := AppendLargeBOCPayloadRecordFromEncodedCellRecord(encoded.Data, arena)
		if err != nil {
			t.Fatalf("payload from encoded: %v", err)
		}
		if len(arena) != 1+len(gotPayload.Data) {
			t.Fatalf("arena size = %d, want %d", len(arena), 1+len(gotPayload.Data))
		}
		if !bytes.Equal(gotPayload.Data, wantPayload.Data) {
			t.Fatalf("encoded payload mismatch for %x", record.Hash)
		}
	}
}

func TestPrepareReachableStateCellsUsesExactRecordBacking(t *testing.T) {
	leaf := cell.BeginCell().MustStoreUInt(0b10101, 5).EndCell()
	shared := cell.BeginCell().MustStoreUInt(0xAA, 8).MustStoreRef(leaf).EndCell()
	root := cell.BeginCell().MustStoreUInt(0b111, 3).MustStoreRef(shared).MustStoreRef(shared).EndCell()

	records, err := PrepareReachableStateCells(root)
	if err != nil {
		t.Fatalf("prepare reachable state cells: %v", err)
	}

	checked := 0
	err = records.ForEach(func(record EncodedCellRecord) error {
		checked++
		if cap(record.Data) != len(record.Data) {
			t.Fatalf("record %x backing capacity = %d, want exact %d", record.Hash[:], cap(record.Data), len(record.Data))
		}
		return nil
	})
	if err != nil {
		t.Fatalf("iterate records: %v", err)
	}
	if checked == 0 {
		t.Fatal("prepared no records")
	}
}

func TestPrepareStateUpdateCellsArenaGrowthPreservesRecords(t *testing.T) {
	root, cells := benchmarkCellGraph(t, 512, 4)
	update, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.MerkleUpdateCellType), 8).
		MustStoreSlice(root.Hash(0), 256).
		MustStoreSlice(root.Hash(0), 256).
		MustStoreUInt(uint64(root.Depth(0)), 16).
		MustStoreUInt(uint64(root.Depth(0)), 16).
		MustStoreRef(root).
		MustStoreRef(root).
		EndCellSpecial(true)
	if err != nil {
		t.Fatalf("create merkle update: %v", err)
	}

	records, err := PrepareStateUpdateCells(update)
	if err != nil {
		t.Fatalf("prepare state update cells: %v", err)
	}
	if records.Len() != cells {
		t.Fatalf("prepared records = %d, want %d", records.Len(), cells)
	}

	checked := 0
	err = WalkReachableStateCells(root, func(current *cell.Cell, meta cell.Metadata) error {
		want, err := PrepareEncodedCellRecordFromCellMetadata(current, meta)
		if err != nil {
			return err
		}
		got := records.Data(meta.Hash)
		if !bytes.Equal(got, want.Data) {
			t.Fatalf("record %x changed while growing arena", meta.Hash[:])
		}
		if cap(got) != len(got) {
			t.Fatalf("record %x backing capacity = %d, want exact %d", meta.Hash[:], cap(got), len(got))
		}
		checked++
		return nil
	})
	if err != nil {
		t.Fatalf("walk expected state cells: %v", err)
	}
	if checked != cells {
		t.Fatalf("checked records = %d, want %d", checked, cells)
	}
}

func TestPrepareReachableStateUpdateCellsMatchesMetadataEncoder(t *testing.T) {
	leaf := cell.BeginCell().MustStoreUInt(0xBEEF, 16).EndCell()
	shared := cell.BeginCell().MustStoreUInt(0xA7, 8).MustStoreRef(leaf).EndCell()
	root := cell.BeginCell().MustStoreUInt(0x55, 8).MustStoreRef(shared).MustStoreRef(shared).EndCell()
	library, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.LibraryCellType), 8).
		MustStoreSlice(root.Hash(0), 256).
		EndCellSpecial(true)
	if err != nil {
		t.Fatalf("create library cell: %v", err)
	}

	levelOnePruned := mustStoragePrunedBranchAtLevel(t, root, 1)
	levelThreePruned := mustStoragePrunedBranchAtLevel(t, root, 3)
	nonVirtualProof := mustStorageMerkleProofBody(t, levelOnePruned)
	virtualProof := mustStorageMerkleProofBody(t, levelThreePruned)
	nonVirtualUpdate := mustStorageMerkleUpdateBody(t, levelOnePruned, root)
	virtualUpdate := mustStorageMerkleUpdateBody(t, levelThreePruned, root)
	if nonVirtualProof.Level() != 0 || nonVirtualUpdate.Level() != 0 {
		t.Fatal("non-virtual Merkle cases must have level zero")
	}
	if virtualProof.Level() == 0 || virtualUpdate.Level() == 0 {
		t.Fatal("virtualized Merkle cases must have a non-zero level")
	}

	tests := []struct {
		name string
		root *cell.Cell
	}{
		{
			name: "ordinary shared graph",
			root: root,
		},
		{
			name: "library cell",
			root: library,
		},
		{
			name: "non-virtual Merkle proof",
			root: nonVirtualProof,
		},
		{
			name: "virtualized Merkle proof",
			root: virtualProof,
		},
		{
			name: "non-virtual Merkle update with mixed refs",
			root: nonVirtualUpdate,
		},
		{
			name: "virtualized Merkle update with mixed refs",
			root: virtualUpdate,
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := prepareReachableStateUpdateCells(tt.root)
			if err != nil {
				t.Fatalf("prepare state update cells: %v", err)
			}
			want, err := prepareReachableStateUpdateCellsWithMetadata(tt.root)
			if err != nil {
				t.Fatalf("prepare reference state update cells: %v", err)
			}

			gotRecords := got.AppendTo(nil)
			wantRecords := want.AppendTo(nil)
			if len(gotRecords) != len(wantRecords) {
				t.Fatalf("records = %d, want %d", len(gotRecords), len(wantRecords))
			}
			for i := range wantRecords {
				if gotRecords[i].Hash != wantRecords[i].Hash {
					t.Fatalf(
						"record %d hash = %x, want %x",
						i,
						gotRecords[i].Hash[:],
						wantRecords[i].Hash[:],
					)
				}
				if !bytes.Equal(gotRecords[i].Data, wantRecords[i].Data) {
					t.Fatalf("record %d data for %x differs from metadata encoder", i, gotRecords[i].Hash[:])
				}
			}
			if got.ByteSize() != want.ByteSize() {
				t.Fatalf("record bytes = %d, want %d", got.ByteSize(), want.ByteSize())
			}
		})
	}
}

func prepareReachableStateUpdateCellsWithMetadata(root *cell.Cell) (StateCellRecords, error) {
	records := make([]EncodedCellRecord, 0)
	seen := make(map[cell.Hash]struct{})
	stack := []*cell.Cell{root.Virtualize(0)}
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		if current.IsLazy() {
			loader, err := current.BeginParse()
			if err != nil {
				return StateCellRecords{}, err
			}
			current = loader.BaseCell()
		}

		if current.GetType() == cell.PrunedCellType && current.ActualLevel() == current.EffectiveLevel()+1 {
			continue
		}

		meta := current.GetMetadata()
		if _, ok := seen[meta.Hash]; ok {
			continue
		}
		seen[meta.Hash] = struct{}{}

		recordCell := current
		if current.GetType() == cell.PrunedCellType {
			var err error
			recordCell, err = materializePrunedStateCell(current)
			if err != nil {
				return StateCellRecords{}, err
			}
		}
		record, err := PrepareEncodedCellRecordFromCellMetadata(recordCell, meta)
		if err != nil {
			return StateCellRecords{}, err
		}
		records = append(records, record)

		if current.GetType() == cell.PrunedCellType {
			continue
		}
		for i, ref := range meta.Refs {
			if ref.Lazy {
				return StateCellRecords{}, fmt.Errorf("ref %d from %x is lazy", i, meta.Hash[:])
			}
			refCell, err := current.PeekRef(i)
			if err != nil {
				return StateCellRecords{}, err
			}
			stack = append(stack, refCell)
		}
	}
	return NewStateCellRecords(records), nil
}

func TestLargeBOCPrunedMetadataUsesPrunedHashesDepths(t *testing.T) {
	leaf := cell.BeginCell().MustStoreUInt(0xBEEF, 16).EndCell()
	hidden := cell.BeginCell().MustStoreUInt(0xA7, 8).MustStoreRef(leaf).EndCell()
	pruned := mustStoragePrunedBranchAtLevel(t, hidden, 1)

	record, err := CellRecordFromCell(pruned)
	if err != nil {
		t.Fatalf("cell record: %v", err)
	}

	meta, err := LargeBOCMetaRecordFromCellRecord(record)
	if err != nil {
		t.Fatalf("meta from record: %v", err)
	}
	assertLargeBOCMetaMatchesCellMetadata(t, meta, pruned.GetMetadata())

	encoded, err := PrepareEncodedCellRecordFromCellMetadata(pruned, pruned.GetMetadata())
	if err != nil {
		t.Fatalf("encoded cell record: %v", err)
	}
	encodedMeta, err := LargeBOCMetaRecordFromEncodedCellRecord(record.Hash, encoded.Data)
	if err != nil {
		t.Fatalf("meta from encoded record: %v", err)
	}
	assertLargeBOCMetaMatchesCellMetadata(t, encodedMeta, pruned.GetMetadata())
}

func TestPrepareReachableStateUpdateCellsUsesCppPrunedBoundaryRule(t *testing.T) {
	represented := cell.BeginCell().MustStoreUInt(0xA7, 8).EndCell()

	nonBoundary := mustStoragePrunedBranchAtLevel(t, represented, 3)
	nonBoundaryView := nonBoundary.Virtualize(1)
	nonBoundaryParent := mustStorageMerkleProofBody(t, nonBoundary)
	nonBoundaryRecords, err := prepareReachableStateUpdateCells(nonBoundaryParent)
	if err != nil {
		t.Fatalf("prepare non-boundary state update cells: %v", err)
	}
	nonBoundaryHash := nonBoundary.Virtualize(1).HashKey()
	nonBoundaryData := nonBoundaryRecords.Data(nonBoundaryHash)
	if len(nonBoundaryData) == 0 {
		t.Fatalf("non-boundary pruned branch %x was not prepared", nonBoundaryHash[:])
	}

	record := DecodeCellRecordTrusted(nonBoundaryHash[:], nonBoundaryData)
	if got, want := record.D1>>5, nonBoundaryView.LevelMask().Mask; got != want {
		t.Fatalf("non-boundary pruned descriptor level mask = %d, want %d", got, want)
	}
	loaded, err := LazyCellRecord(record, nil)
	if err != nil {
		t.Fatalf("load prepared non-boundary pruned branch: %v", err)
	}
	if loaded.GetType() != cell.PrunedCellType {
		t.Fatalf("loaded non-boundary branch type = %d, want pruned", loaded.GetType())
	}
	if loaded.HashKey() != nonBoundaryHash {
		t.Fatalf("loaded non-boundary branch hash mismatch: got=%x want=%x", loaded.HashKey(), nonBoundaryHash)
	}
	assertLargeBOCMetaMatchesCellMetadataFromEncoded(t, nonBoundaryHash[:], nonBoundaryData, nonBoundaryView.GetMetadata())

	boundary := mustStoragePrunedBranchAtLevel(t, represented, 2)
	boundaryParent := mustStorageMerkleProofBody(t, boundary)
	boundaryRecords, err := prepareReachableStateUpdateCells(boundaryParent)
	if err != nil {
		t.Fatalf("prepare boundary state update cells: %v", err)
	}
	boundaryHash := boundary.Virtualize(1).HashKey()
	if boundaryRecords.Has(boundaryHash) {
		t.Fatalf("boundary pruned branch %x was prepared, want existing-state boundary", boundaryHash[:])
	}
}

func assertLargeBOCMetaMatchesCellMetadataFromEncoded(tb testing.TB, hash []byte, data []byte, want cell.Metadata) {
	tb.Helper()

	meta, err := LargeBOCMetaRecordFromEncodedCellRecord(hash, data)
	if err != nil {
		tb.Fatalf("large boc meta from encoded record: %v", err)
	}
	assertLargeBOCMetaMatchesCellMetadata(tb, meta, want)
}

func assertLargeBOCMetaMatchesCellMetadata(tb testing.TB, got cell.LargeBOCMetaRecord, want cell.Metadata) {
	tb.Helper()

	if got.D1>>5 != want.LevelMask.Mask {
		tb.Fatalf("large boc level mask = %d, want %d", got.D1>>5, want.LevelMask.Mask)
	}

	count := CellRefHashesCount(want.LevelMask.Mask)
	if len(want.Hashes) != count || len(want.Depths) != count {
		tb.Fatalf("test metadata size mismatch: hashes=%d depths=%d want=%d", len(want.Hashes), len(want.Depths), count)
	}
	for i := 0; i < count; i++ {
		if got.Hashes[i] != want.Hashes[i] {
			tb.Fatalf("large boc hash %d mismatch: got=%x want=%x", i, got.Hashes[i], want.Hashes[i])
		}
		if got.Depths[i] != want.Depths[i] {
			tb.Fatalf("large boc depth %d mismatch: got=%d want=%d", i, got.Depths[i], want.Depths[i])
		}
	}
}

type largeBOCTestLoader struct {
	records map[cell.Hash]*CellRecord
}

func (l largeBOCTestLoader) LoadMeta(hashes []cell.Hash, dst []cell.LargeBOCMetaRecord) ([]cell.LargeBOCMetaRecord, error) {
	for _, hash := range hashes {
		record := l.records[hash]
		if record == nil {
			return dst, ErrNotFound
		}
		meta, err := LargeBOCMetaRecordFromCellRecord(record)
		if err != nil {
			return dst, err
		}
		dst = append(dst, meta)
	}
	return dst, nil
}

func mustStoragePrunedBranch(tb testing.TB, hidden *cell.Cell) *cell.Cell {
	tb.Helper()

	hiddenHash := hidden.Hash(0)
	prunedData := append([]byte{byte(cell.PrunedCellType), 0x01}, hiddenHash...)
	prunedData = append(prunedData, 0, 0)
	pruned, err := cell.BeginCell().MustStoreSlice(prunedData, uint(len(prunedData))*8).EndCellSpecial(true)
	if err != nil {
		tb.Fatalf("create pruned branch: %v", err)
	}
	return pruned
}

func mustStoragePrunedBranchAtLevel(tb testing.TB, hidden *cell.Cell, level int) *cell.Cell {
	tb.Helper()

	if level < 1 || level > 3 {
		tb.Fatalf("pruned level = %d, want 1..3", level)
	}

	mask := byte(1 << uint(level-1))
	hiddenHash := hidden.Hash(0)
	prunedData := append([]byte{byte(cell.PrunedCellType), mask}, hiddenHash...)
	var depth [2]byte
	binary.BigEndian.PutUint16(depth[:], hidden.Depth(0))
	prunedData = append(prunedData, depth[:]...)

	pruned, err := cell.BeginCell().MustStoreSlice(prunedData, uint(len(prunedData))*8).EndCellSpecial(true)
	if err != nil {
		tb.Fatalf("create pruned branch: %v", err)
	}
	if pruned.Level() != level {
		tb.Fatalf("pruned level = %d, want %d", pruned.Level(), level)
	}
	return pruned
}

func mustStorageMerkleProofBody(tb testing.TB, body *cell.Cell) *cell.Cell {
	tb.Helper()

	proof, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.MerkleProofCellType), 8).
		MustStoreSlice(body.Hash(0), 256).
		MustStoreUInt(uint64(body.Depth(0)), 16).
		MustStoreRef(body).
		EndCellSpecial(true)
	if err != nil {
		tb.Fatalf("create merkle proof body: %v", err)
	}
	return proof
}

func mustStorageMerkleUpdateBody(tb testing.TB, from *cell.Cell, to *cell.Cell) *cell.Cell {
	tb.Helper()

	update, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.MerkleUpdateCellType), 8).
		MustStoreSlice(from.Hash(0), 256).
		MustStoreSlice(to.Hash(0), 256).
		MustStoreUInt(uint64(from.Depth(0)), 16).
		MustStoreUInt(uint64(to.Depth(0)), 16).
		MustStoreRef(from).
		MustStoreRef(to).
		EndCellSpecial(true)
	if err != nil {
		tb.Fatalf("create merkle update: %v", err)
	}
	return update
}

func (l largeBOCTestLoader) LoadPayload(hashes []cell.Hash, dst []cell.LargeBOCPayloadRecord) ([]cell.LargeBOCPayloadRecord, error) {
	for _, hash := range hashes {
		record := l.records[hash]
		if record == nil {
			return dst, ErrNotFound
		}
		payload, err := LargeBOCPayloadRecordFromCellRecord(record)
		if err != nil {
			return dst, err
		}
		dst = append(dst, payload)
	}
	return dst, nil
}
