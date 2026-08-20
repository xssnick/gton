package storage

import (
	"bytes"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestDecodeCellRecordRefHashesMixedCompactLayout(t *testing.T) {
	commonHash := bytes.Repeat([]byte{0x11}, encodedCellRecordHashSize)
	levelHashes := append(bytes.Repeat([]byte{0x21}, encodedCellRecordHashSize),
		bytes.Repeat([]byte{0x22}, encodedCellRecordHashSize)...)
	levelHashes = append(levelHashes, bytes.Repeat([]byte{0x23}, encodedCellRecordHashSize)...)

	record := &CellRecord{
		D1: 2,
		D2: 0,
		Refs: []CellRefRecord{
			{
				Hashes: commonHash,
				Depths: make([]byte, encodedCellRecordDepthSize),
			},
			{
				LevelMask: 0b011,
				Hashes:    levelHashes,
				Depths:    make([]byte, 3*encodedCellRecordDepthSize),
			},
		},
	}
	encoded := EncodeCellRecord(record)
	if encoded[0]&encodedCellRecordCompactRefsFlag == 0 {
		t.Fatal("mixed record did not use the compact ref layout")
	}

	var refs [4]cell.Hash
	count, err := DecodeCellRecordRefHashes(encoded, &refs)
	if err != nil {
		t.Fatalf("decode ref hashes: %v", err)
	}
	if count != 2 {
		t.Fatalf("ref count = %d, want 2", count)
	}
	if !bytes.Equal(refs[0][:], commonHash) {
		t.Fatalf("common ref hash = %x, want %x", refs[0], commonHash)
	}
	wantLevelHash := levelHashes[len(levelHashes)-encodedCellRecordHashSize:]
	if !bytes.Equal(refs[1][:], wantLevelHash) {
		t.Fatalf("levelled ref hash = %x, want %x", refs[1], wantLevelHash)
	}

	allocs := testing.AllocsPerRun(1000, func() {
		if _, decodeErr := DecodeCellRecordRefHashes(encoded, &refs); decodeErr != nil {
			t.Fatalf("decode ref hashes: %v", decodeErr)
		}
	})
	if allocs != 0 {
		t.Fatalf("decode ref hashes allocates %.1f objects per call, want 0", allocs)
	}
}

func TestDecodeCellRecordRefHashesRejectsMalformedRecords(t *testing.T) {
	valid := EncodeCellRecord(&CellRecord{
		D1: 1,
		D2: 0,
		Refs: []CellRefRecord{{
			Hashes: bytes.Repeat([]byte{0x31}, encodedCellRecordHashSize),
			Depths: make([]byte, encodedCellRecordDepthSize),
		}},
	})

	tests := map[string][]byte{
		"empty":                nil,
		"descriptor_only":      {0},
		"too_many_refs":        {5, 0},
		"compact_mask_missing": {encodedCellRecordCompactRefsFlag | 1, 0},
		"compact_mask_invalid": {encodedCellRecordCompactRefsFlag | 1, 0, 2},
		"ref_truncated":        valid[:len(valid)-1],
		"trailing_bytes":       append(bytes.Clone(valid), 0),
	}
	for name, encoded := range tests {
		t.Run(name, func(t *testing.T) {
			var refs [4]cell.Hash
			if _, err := DecodeCellRecordRefHashes(encoded, &refs); err == nil {
				t.Fatal("malformed cell record was accepted")
			}
		})
	}
}
