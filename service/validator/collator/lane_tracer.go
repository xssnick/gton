package collator

import (
	"sync/atomic"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

// laneTracer is the one listener every read made on behalf of an account lane
// goes through: the ShardAccounts descent that finds the account, the account
// state the transaction parses, the cells the machine reports loading, and the
// storage-stat dictionary the transaction walks. It exists so that a lane can
// execute speculatively — ahead of the point where the collation has decided
// whether the message is admitted at all — without touching the shared record.
//
// It has two modes. Buffering, while the lane's message is executing off the
// main goroutine: every event is appended, in the order it happened, and
// nothing reaches the shared recorders. Pass-through, otherwise: every event is
// forwarded synchronously to exactly the recorder it would have reached had
// the lane's cells carried that recorder's trace directly, which is what they
// carried before this type existed.
//
// replay turns a buffered lane into a retired one: the events are forwarded in
// recorded order and the tracer drops to pass-through. Because lanes retire in
// message order and a lane's own events are in execution order, the shared
// record after retiring message k is exactly the record a sequential collation
// has after executing message k — the same cells, first-recorded by the same
// instance, charged to the size estimate in the same sequence. A lane that is
// discarded instead — the block filled before its message — is simply never
// replayed, and its reads never happened as far as the block is concerned.
//
// The trace is per lane, not per message, and the tracer is never detached:
// cells loaded through it keep its trace for as long as they live, so a later
// phase reading the same account state — the externals phase, immediate
// delivery, finalization — still reaches the shared recorders, through
// pass-through, with no event lost to a listener that has stopped listening.
//
// Like the block's own recorder it can be sealed. A produced block embeds cells
// read out of the predecessor, and those cells carry the trace of whichever
// lane read them; an unsealed tracer would keep the whole collation reachable
// for as long as the candidate lives and keep forwarding every later descent
// into a record nobody reads. seal drops the collation and, through the
// recorder's own sealed flag, stops child traces from propagating, which is
// the property TestBuiltBlockRootCarriesNoLiveRecorder pins.
type laneTracer struct {
	// c is the collation the events are forwarded to. It is a pointer the
	// tracer lets go of when sealed, so a retained block does not retain the
	// collation through its cells.
	c atomic.Pointer[collation]
	// usage is the block record, kept beside c because the sealed check has to
	// keep working after c is gone. The record itself is small once sealed.
	usage *cell.ReadSet
	lane  atomic.Pointer[accountLane]

	// trace is what the lane's predecessor cells carry.
	trace *cell.Trace
	// statTrace is what the lane's storage-stat dictionary carries. It is a
	// separate listener because its events have a different destination: the
	// shared storage proof the lane is bound to, not the block record.
	statTrace *cell.Trace

	buffering bool
	events    []laneTraceEvent
}

type laneTraceKind uint8

const (
	// laneTraceTree is a read of a predecessor cell, destined for the block's
	// read set, where it is billed to the collated size estimate.
	laneTraceTree laneTraceKind = iota
	// laneTraceExecution is a cell the machine reported loading, destined for
	// recordExecutionRead: unbilled in the read set and counted by the collated
	// proof estimate.
	laneTraceExecution
	// laneTraceStat is a read of the account's storage-stat dictionary, destined
	// for the proof builder shared by every account bound to that dictionary.
	laneTraceStat
)

type laneTraceEvent struct {
	cell *cell.Cell
	kind laneTraceKind
}

// laneStatListener is the storage-stat half of a laneTracer, split into its own
// type only because the two halves need two distinct Trace objects.
type laneStatListener laneTracer

func newLaneTracer(c *collation, lane *accountLane) *laneTracer {
	t := &laneTracer{usage: c.usage}
	t.c.Store(c)
	t.lane.Store(lane)
	t.trace = cell.NewTraceForListener(t)
	t.statTrace = cell.NewTraceForListener((*laneStatListener)(t))
	return t
}

// OnLoad implements TraceListener for the lane's predecessor cells.
func (t *laneTracer) OnLoad(loaded *cell.Cell) {
	if t.buffering {
		t.events = append(t.events, laneTraceEvent{cell: loaded, kind: laneTraceTree})
		return
	}
	t.usage.OnLoad(loaded)
}

func (*laneTracer) OnCreate() {}

// ChildTrace implements TraceListener. Returning the same trace for every
// reference is what keeps descent allocation-free, exactly as ReadSet does —
// and returning none once the record is closed is what makes a retained block
// inert, exactly as ReadSet does.
//
// Closed, not sealed. The record is detached at the fence partway through
// finishing the block, while its table is still being read to select the proof;
// a lane tracer that kept propagating until the later Seal would stand in front
// of the record on every cell its lane touched and go on recording through them
// for the whole of that window — which is exactly the window a successor
// collation runs in.
func (t *laneTracer) ChildTrace(int) *cell.Trace {
	if t.usage.Inert() {
		return nil
	}
	return t.trace
}

func (*laneTracer) PendingError() error { return nil }

// onExecutionRead is the machine's OnCellLoad hook for this lane.
func (t *laneTracer) onExecutionRead(loaded *cell.Cell) {
	if t.buffering {
		t.events = append(t.events, laneTraceEvent{cell: loaded, kind: laneTraceExecution})
		return
	}
	if c := t.c.Load(); c != nil {
		c.recordExecutionRead(loaded)
	}
}

// OnLoad implements TraceListener for the lane's storage-stat dictionary.
func (l *laneStatListener) OnLoad(loaded *cell.Cell) {
	t := (*laneTracer)(l)
	if t.buffering {
		t.events = append(t.events, laneTraceEvent{cell: loaded, kind: laneTraceStat})
		return
	}
	t.forwardStat(loaded)
}

func (*laneStatListener) OnCreate() {}

func (l *laneStatListener) ChildTrace(int) *cell.Trace {
	t := (*laneTracer)(l)
	if t.usage.Inert() {
		return nil
	}
	return t.statTrace
}

func (*laneStatListener) PendingError() error { return nil }

// forwardStat delivers one storage-stat read to the shared proof builder the
// lane is bound to. A lane that was resolved but never registered has no
// builder yet, and that is not a dropped event: registration always precedes
// replay, and pass-through only begins once replay has run.
func (t *laneTracer) forwardStat(loaded *cell.Cell) {
	lane := t.lane.Load()
	if lane == nil {
		return
	}
	if builder := lane.storageProof; builder != nil {
		builder.ReadSet().OnLoad(loaded)
	}
}

// seal lets go of the collation and the lane. It is called when the block
// record is sealed; ChildTrace consults that seal, so after this the tracer
// neither forwards nor propagates, and a cell carrying it retains nothing but
// the tracer itself.
func (t *laneTracer) seal() {
	t.c.Store(nil)
	t.lane.Store(nil)
	t.events = nil
}

// speculate switches the tracer to buffering. It is called on the main
// goroutine before the lane's message is handed to a worker, so the flip is
// ordered before every read the worker makes.
func (t *laneTracer) speculate() {
	t.buffering = true
}

// replay forwards the buffered events in order and returns the tracer to
// pass-through. It runs on the main goroutine after the worker has finished,
// which orders it after every append.
func (t *laneTracer) replay() {
	t.replaySegment(t.events)
	t.discard()
}

// replaySegment forwards one detached segment in order. Unlike replay it
// leaves the tracer's buffer and mode alone: the events belong to the caller,
// and the lane may still be buffering for a chained successor whose reads are
// not part of this segment.
func (t *laneTracer) replaySegment(events []laneTraceEvent) {
	c := t.c.Load()
	for _, event := range events {
		switch event.kind {
		case laneTraceTree:
			t.usage.OnLoad(event.cell)
		case laneTraceExecution:
			if c != nil {
				c.recordExecutionRead(event.cell)
			}
		case laneTraceStat:
			t.forwardStat(event.cell)
		}
	}
}

// detachEvents hands the buffered events to the caller and installs recycle,
// emptied, as the new buffer. It exists for chained speculation: two
// emulations of one lane may be in flight against the tracer before the first
// retires, and the buffer they share would otherwise mix their reads — replay
// at retirement must forward exactly the retired plan's own prefix. It runs on
// the goroutine that produced the events, between two emulations, so no append
// is concurrent with the swap; the segment it returns is immutable from that
// point on, which is what lets the main goroutine replay it while the same
// goroutine buffers the successor.
func (t *laneTracer) detachEvents(recycle []laneTraceEvent) []laneTraceEvent {
	events := t.events
	t.events = recycle[:0]

	return events
}

// discard drops the buffered events unreplayed and returns the tracer to
// pass-through. This is what happens to a lane whose message the block could
// not take: the work is thrown away, and so is every trace of it.
func (t *laneTracer) discard() {
	clear(t.events)
	t.events = t.events[:0]
	t.buffering = false
}
