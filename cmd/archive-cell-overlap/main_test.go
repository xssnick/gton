package main

import (
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCalculateOverlap(t *testing.T) {
	t.Parallel()

	known := []cellStat{
		{hash: testHash(1), size: 10, occurrences: 2},
		{hash: testHash(2), size: 20, occurrences: 1},
	}
	target := completedWindow{
		report: windowReport{
			LogicalCellWrites:      4,
			LogicalCellValueBytes:  60,
			LogicalCellRecordBytes: 60 + 4*cellKeyBytes,
		},
		cells: []cellStat{
			{hash: testHash(1), size: 10, occurrences: 3},
			{hash: testHash(3), size: 30, occurrences: 1},
		},
	}

	overlap, err := calculateOverlap([]int{1}, 2, known, target)
	if err != nil {
		t.Fatal(err)
	}
	if overlap.DistinctIntersectionCells != 1 {
		t.Fatalf("distinct intersection cells = %d, want 1", overlap.DistinctIntersectionCells)
	}
	if overlap.AvoidableTargetWrites != 3 {
		t.Fatalf("avoidable writes = %d, want 3", overlap.AvoidableTargetWrites)
	}
	if overlap.AvoidableTargetValueBytes != 30 {
		t.Fatalf("avoidable value bytes = %d, want 30", overlap.AvoidableTargetValueBytes)
	}
	if overlap.TargetValueBytePercent != 50 {
		t.Fatalf("avoidable value bytes = %.2f%%, want 50%%", overlap.TargetValueBytePercent)
	}
}

func TestUnionCells(t *testing.T) {
	t.Parallel()

	union, err := unionCells(
		[]cellStat{
			{hash: testHash(1), size: 10},
			{hash: testHash(3), size: 30},
		},
		[]cellStat{
			{hash: testHash(2), size: 20},
			{hash: testHash(3), size: 30},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(union) != 3 {
		t.Fatalf("union cells = %d, want 3", len(union))
	}
	for i, value := range []byte{1, 2, 3} {
		if union[i].hash[0] != value {
			t.Fatalf("union cell %d hash prefix = %d, want %d", i, union[i].hash[0], value)
		}
	}
}

func testHash(value byte) cell.Hash {
	var hash cell.Hash
	hash[0] = value
	return hash
}
