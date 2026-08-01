package storage

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestDecodeLazyCellRecordTrustedMatchesLegacyAndOwnsEncodedData(t *testing.T) {
	leaves := make([]*cell.Cell, 4)
	ordinaryBuilder := cell.BeginCell().MustStoreUInt(0x5a, 8)
	for i := range leaves {
		leaves[i] = cell.BeginCell().MustStoreUInt(uint64(i+1), 64).EndCell()
		ordinaryBuilder.MustStoreRef(leaves[i])
	}
	ordinary := ordinaryBuilder.EndCell()

	churnBuilder := cell.BeginCell().MustStoreUInt(0xc3, 8)
	for i := range leaves {
		churnBuilder.MustStoreRef(
			cell.BeginCell().MustStoreUInt(uint64(0x100+i), 64).EndCell(),
		)
	}
	churn := churnBuilder.EndCell()
	churnEncoded, err := PrepareEncodedCellRecordFromCellMetadata(churn, churn.GetMetadata())
	if err != nil {
		t.Fatalf("encode scratch-churn cell record: %v", err)
	}

	library, err := cell.BeginCell().
		MustStoreUInt(uint64(cell.LibraryCellType), 8).
		MustStoreSlice(ordinary.Hash(0), 256).
		EndCellSpecial(true)
	if err != nil {
		t.Fatalf("create library cell: %v", err)
	}

	prunedLevelOne := mustStoragePrunedBranchAtLevel(t, ordinary, 1)
	prunedLevelThree := mustStoragePrunedBranchAtLevel(t, ordinary, 3)
	merkleProof := mustStorageMerkleProofBody(t, prunedLevelThree)
	merkleUpdate := mustStorageMerkleUpdateBody(t, prunedLevelOne, ordinary)

	// Lazy loaders resolve the represented hash. A Merkle proof virtualizes its
	// higher-level pruned ref, so that hash resolves to the hidden ordinary cell.
	tests := []struct {
		name       string
		root       *cell.Cell
		loadedRefs []*cell.Cell
	}{
		{name: "ordinary-four-refs", root: ordinary, loadedRefs: leaves},
		{name: "library", root: library},
		{name: "pruned-level-one", root: prunedLevelOne},
		{name: "pruned-level-three", root: prunedLevelThree},
		{name: "merkle-proof", root: merkleProof, loadedRefs: []*cell.Cell{ordinary}},
		{name: "merkle-update", root: merkleUpdate, loadedRefs: []*cell.Cell{prunedLevelOne, ordinary}},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			encoded, err := PrepareEncodedCellRecordFromCellMetadata(tt.root, tt.root.GetMetadata())
			if err != nil {
				t.Fatalf("encode cell record: %v", err)
			}
			legacyEncoded := bytes.Clone(encoded.Data)
			legacyRecord := DecodeCellRecordTrusted(encoded.Hash[:], legacyEncoded)
			if len(tt.loadedRefs) != len(legacyRecord.Refs) {
				t.Fatalf("loader refs count: got=%d want=%d", len(tt.loadedRefs), len(legacyRecord.Refs))
			}

			loaderCells := make(map[cell.Hash]*cell.Cell, len(legacyRecord.Refs))
			for i, refRecord := range legacyRecord.Refs {
				hash, err := CellRefHash(refRecord)
				if err != nil {
					t.Fatalf("ref %d hash: %v", i, err)
				}
				var key cell.Hash
				copy(key[:], hash)

				loaderCells[key] = tt.loadedRefs[i]
			}
			loader := func(hash cell.Hash) (*cell.Cell, error) {
				loaded := loaderCells[hash]
				if loaded == nil {
					return nil, ErrNotFound
				}
				return loaded, nil
			}

			legacy, err := LazyCellRecord(legacyRecord, loader)
			if err != nil {
				t.Fatalf("legacy decode: %v", err)
			}

			hash := bytes.Clone(encoded.Hash[:])
			got, err := DecodeLazyCellRecordTrusted(hash, encoded.Data, loader)
			if err != nil {
				t.Fatalf("fused decode: %v", err)
			}

			clear(hash)
			for i := range encoded.Data {
				encoded.Data[i] = 0xa5
			}
			for i := 0; i < 16; i++ {
				_, err = DecodeLazyCellRecordTrusted(churnEncoded.Hash[:], churnEncoded.Data, loader)
				if err != nil {
					t.Fatalf("reuse scratch %d: %v", i, err)
				}
			}

			assertDecodedLazyCellMatches(t, got, legacy)
			for i := 0; i < int(got.RefsNum()); i++ {
				gotRef, err := got.PeekRef(i)
				if err != nil {
					t.Fatalf("peek fused ref %d: %v", i, err)
				}
				legacyRef, err := legacy.PeekRef(i)
				if err != nil {
					t.Fatalf("peek legacy ref %d: %v", i, err)
				}
				assertDecodedLazyCellMatches(t, gotRef, legacyRef)

				gotLoaded, err := gotRef.BeginParse()
				if err != nil {
					t.Fatalf("load fused ref %d: %v", i, err)
				}
				legacyLoaded, err := legacyRef.BeginParse()
				if err != nil {
					t.Fatalf("load legacy ref %d: %v", i, err)
				}
				if gotLoaded.BaseCell().HashKey() != legacyLoaded.BaseCell().HashKey() {
					t.Fatalf("loaded ref %d hash mismatch", i)
				}
			}
		})
	}
}

func assertDecodedLazyCellMatches(tb testing.TB, got *cell.Cell, want *cell.Cell) {
	tb.Helper()

	if got.HashKey() != want.HashKey() {
		tb.Fatalf("hash mismatch: got=%x want=%x", got.Hash(), want.Hash())
	}
	if got.GetType() != want.GetType() {
		tb.Fatalf("type mismatch: got=%d want=%d", got.GetType(), want.GetType())
	}
	if got.LevelMask() != want.LevelMask() {
		tb.Fatalf("level mask mismatch: got=%d want=%d", got.LevelMask().Mask, want.LevelMask().Mask)
	}
	if got.BitsSize() != want.BitsSize() {
		tb.Fatalf("bits size mismatch: got=%d want=%d", got.BitsSize(), want.BitsSize())
	}
	if got.RefsNum() != want.RefsNum() {
		tb.Fatalf("refs mismatch: got=%d want=%d", got.RefsNum(), want.RefsNum())
	}
	for level := 0; level <= got.Level(); level++ {
		if got.HashKey(level) != want.HashKey(level) {
			tb.Fatalf("level %d hash mismatch: got=%x want=%x", level, got.Hash(level), want.Hash(level))
		}
		if got.Depth(level) != want.Depth(level) {
			tb.Fatalf("level %d depth mismatch: got=%d want=%d", level, got.Depth(level), want.Depth(level))
		}
	}

	gotSlice, err := got.BeginParse()
	if err != nil {
		tb.Fatalf("parse fused cell: %v", err)
	}
	wantSlice, err := want.BeginParse()
	if err != nil {
		tb.Fatalf("parse legacy cell: %v", err)
	}
	gotBits, gotData, err := gotSlice.RestBits()
	if err != nil {
		tb.Fatalf("read fused body: %v", err)
	}
	wantBits, wantData, err := wantSlice.RestBits()
	if err != nil {
		tb.Fatalf("read legacy body: %v", err)
	}
	if gotBits != wantBits || !bytes.Equal(gotData, wantData) {
		tb.Fatalf(
			"body mismatch: got_bits=%d want_bits=%d got=%x want=%x",
			gotBits,
			wantBits,
			gotData,
			wantData,
		)
	}
}
