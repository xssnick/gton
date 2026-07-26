package service

import (
	"encoding/binary"
	"testing"

	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func stageTestRecords(offset, count int) storage.StateCellRecords {
	records := make([]storage.EncodedCellRecord, count)
	for i := range records {
		id := offset + i
		binary.BigEndian.PutUint64(records[i].Hash[24:], uint64(id))
		// Payload must be a function of the hash, so re-staging the same cell
		// is a no-op rather than a replacement.
		records[i].Data = []byte{0x01, byte(id), byte(id >> 8)}
	}
	return storage.NewStateCellRecords(records)
}

// TestStageRecordsWithoutPrewriterKeepsWindowState pins that skipping the
// prewrite slice when no prewriter is attached changes nothing observable: the
// window still absorbs every record and the request stays empty.
func TestStageRecordsWithoutPrewriterKeepsWindowState(t *testing.T) {
	cache := newStateCellEncodedCache(16)

	request := cache.stageRecords(stageTestRecords(0, 500), nil)
	if request.cache != nil || request.prewriter != nil || !request.records.Empty() {
		t.Fatalf("request without prewriter carries %d records, want an empty request", request.records.Len())
	}
	if cache.len() != 500 {
		t.Fatalf("window records = %d, want 500", cache.len())
	}
	if cache.prewritePending != 0 {
		t.Fatalf("prewritePending = %d, want 0", cache.prewritePending)
	}

	var hash cell.Hash
	binary.BigEndian.PutUint64(hash[24:], 499)
	chunks, _ := cache.appendRecordChunks(nil)
	if !storage.NewStateCellRecordChunks(chunks, 0).Has(hash) {
		t.Fatal("window lost a staged record on the nil-prewriter path")
	}
}

// TestStageRecordsPrewritesOnlyChangedRecords pins that the presized slice
// still carries exactly the records that changed the window, not the whole
// input.
func TestStageRecordsPrewritesOnlyChangedRecords(t *testing.T) {
	cache := newStateCellEncodedCache(16)
	prewriter := &stateCellPrewriter{}

	first := cache.stageRecords(stageTestRecords(0, 300), prewriter)
	if first.records.Len() != 300 {
		t.Fatalf("first prewrite = %d records, want 300", first.records.Len())
	}
	if cache.prewritePending != 1 {
		t.Fatalf("prewritePending after first stage = %d, want 1", cache.prewritePending)
	}
	cache.completePrewrite(0, nil)

	// Re-staging identical records must produce no prewrite at all.
	repeat := cache.stageRecords(stageTestRecords(0, 300), prewriter)
	if !repeat.records.Empty() {
		t.Fatalf("repeat prewrite = %d records, want none", repeat.records.Len())
	}
	if cache.prewritePending != 0 {
		t.Fatalf("prewritePending after repeat stage = %d, want 0", cache.prewritePending)
	}

	// A mixed set prewrites only the new tail.
	mixed := cache.stageRecords(stageTestRecords(200, 300), prewriter)
	if mixed.records.Len() != 200 {
		t.Fatalf("mixed prewrite = %d records, want 200", mixed.records.Len())
	}
	if cache.len() != 500 {
		t.Fatalf("window records = %d, want 500", cache.len())
	}
	cache.completePrewrite(0, nil)
}

// TestStageRecordsPrewriteCapacityIsBounded pins the presize bound: a large
// input whose records are already in the window must not reserve a slice far
// beyond what it appends.
func TestStageRecordsPrewriteCapacityIsBounded(t *testing.T) {
	const count = stateCellPrewriteCapacityHint * 4

	cache := newStateCellEncodedCache(16)
	prewriter := &stateCellPrewriter{}

	seed := cache.stageRecords(stageTestRecords(0, count), prewriter)
	if seed.records.Len() != count {
		t.Fatalf("seed prewrite = %d records, want %d", seed.records.Len(), count)
	}
	cache.completePrewrite(0, nil)

	// Same input again: one changed record among count, so the reservation must
	// stay at the hint rather than scaling with the input.
	changed := stageTestRecords(0, count)
	replaced := changed.Records[count-1]
	replaced.Data = []byte{0x02, 0x02, 0x02}
	changed.Records[count-1] = replaced

	mostlyKnown := cache.stageRecords(changed, prewriter)
	if mostlyKnown.records.Len() != 1 {
		t.Fatalf("prewrite = %d records, want 1", mostlyKnown.records.Len())
	}
	// The reservation must stay at the hint instead of scaling with the input,
	// which is the whole point of bounding it.
	if got := cap(mostlyKnown.records.Records); got > stateCellPrewriteCapacityHint {
		t.Fatalf("prewrite capacity = %d, want at most the hint %d", got, stateCellPrewriteCapacityHint)
	}
	cache.completePrewrite(0, nil)
}
