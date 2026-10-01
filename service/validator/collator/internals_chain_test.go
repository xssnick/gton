package collator

import (
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// A chained successor speculates through the same lane tracer as its still
// unretired predecessor, so the tracer's buffer holds two plans' reads before
// the first replay runs. Retirement must forward exactly the retired plan's
// own reads: a successor read leaking into the predecessor's replay reaches
// the shared record — and through it the size estimate and the collated proof
// — one admission decision too early, which is the defect the wave arms of
// TestInternalWavesProduceTheSequentialBlock would report as a diverged block.
// This test pins the mechanism itself: detached segments keep the plans
// apart, in order, with the tracer still buffering in between.
func TestLaneTracerSegmentsKeepChainedPlansApart(t *testing.T) {
	leaf := func(tag uint64) *cell.Cell {
		return cell.BeginCell().MustStoreUInt(tag, 32).EndCell()
	}
	root := leaf(0xffff)
	usage := cell.NewReadSet(root)
	c := &collation{usage: usage}
	lane := &accountLane{}
	lane.tracer = newLaneTracer(c, lane)

	parse := func(tag uint64) *cell.Cell {
		read := leaf(tag).WithTrace(lane.tracer.trace)
		if _, err := read.BeginParse(); err != nil {
			t.Fatalf("parse %#x: %v", tag, err)
		}
		return read
	}
	recorded := func(read *cell.Cell) bool {
		_, ok := usage.Contains(read.HashKey())
		return ok
	}

	// The predecessor's emulation, then the worker's handoff: its reads are
	// detached before its wg would be released, and the successor's begin
	// against the same tracer.
	lane.tracer.speculate()
	head := []*cell.Cell{parse(0xa1), parse(0xa2)}
	headEvents := lane.tracer.detachEvents(nil)
	successor := []*cell.Cell{parse(0xb1), parse(0xb2)}
	successorEvents := lane.tracer.detachEvents(headEvents[:0:0])

	for _, read := range append(append([]*cell.Cell{}, head...), successor...) {
		if recorded(read) {
			t.Fatalf("cell %x reached the shared record while buffered", read.HashKey())
		}
	}

	// The predecessor retires: exactly its own segment lands in the record.
	lane.tracer.replaySegment(headEvents)
	for _, read := range head {
		if !recorded(read) {
			t.Fatalf("predecessor read %x was not replayed with its plan", read.HashKey())
		}
	}
	for _, read := range successor {
		if recorded(read) {
			t.Fatalf("successor read %x leaked into the predecessor's replay", read.HashKey())
		}
	}

	// The successor retires; the chain is over, so the tracer returns to
	// pass-through and a later phase's read of the same lane forwards
	// synchronously again.
	lane.tracer.replaySegment(successorEvents)
	lane.tracer.discard()
	for _, read := range successor {
		if !recorded(read) {
			t.Fatalf("successor read %x was not replayed at its own retirement", read.HashKey())
		}
	}
	if late := parse(0xc1); !recorded(late) {
		t.Fatal("a read after the chain retired did not pass through to the shared record")
	}
}
