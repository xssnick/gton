package msgpool

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func deltaFromBlockRoot(root *cell.Cell, startLT uint64, owner ShardIdent) (*InternalsDelta, error) {
	deltas, err := deltasFromBlockRoot(root, startLT, owner)
	if err != nil {
		return nil, err
	}

	return deltas[0], nil
}

func deltasFromBlockRoot(root *cell.Cell, startLT uint64, owners ...ShardIdent) ([]*InternalsDelta, error) {
	snapshot, err := routingSnapshot(owners)
	if err != nil {
		return nil, err
	}
	routed, err := routedDeltasFromBlockRoot(root, startLT, ShardIdent{}, SourceRef{}, snapshot)
	if err != nil {
		return nil, err
	}

	deltas := make([]*InternalsDelta, len(routed))
	for index := range routed {
		deltas[index] = routed[index].Delta
	}

	return deltas, nil
}

func seedFromStateRoot(stateRoot *cell.Cell, owner ShardIdent) ([]*InternalMessage, uint64, error) {
	snapshot, err := routingSnapshot([]ShardIdent{owner})
	if err != nil {
		return nil, 0, err
	}
	seeds, total, err := routedSeedsFromStateRoot(stateRoot, ShardIdent{}, SourceRef{}, snapshot)
	if err != nil {
		return nil, 0, err
	}

	return seeds[0].Messages, total, nil
}

func routingSnapshot(destinations []ShardIdent) (*internalsSnapshot, error) {
	snapshot := &internalsSnapshot{
		destinations: make([]destinationEntry, len(destinations)),
		byShard:      make(map[ShardIdent]*destinationState, len(destinations)),
		router:       destinationRouter{workchains: map[int32]*routeNode{}},
	}
	seen := make(map[ShardIdent]struct{}, len(destinations))
	for index, destination := range destinations {
		if err := validateDestination(destination); err != nil {
			return nil, err
		}
		if _, exists := seen[destination]; exists {
			return nil, fmt.Errorf("msgpool: duplicate destination %d:%016x", destination.Workchain, destination.Shard)
		}

		seen[destination] = struct{}{}
		snapshot.destinations[index] = destinationEntry{shard: destination}
		snapshot.router.insert(destination, index)
	}

	return snapshot, nil
}

// ---- clock ----

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_800_000_000, 0)}
}

func newFakeClockAt(seed int64) *fakeClock {
	return &fakeClock{now: time.Unix(1_800_000_000, seed)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(d)
	c.mu.Unlock()
}

// ---- message builders ----

func testAddr(fill byte) [32]byte {
	var a [32]byte
	for i := range a {
		a[i] = fill
	}
	return a
}

func addrOf(wc int32, addr [32]byte) *address.Address {
	return address.NewAddress(0, byte(wc), addr[:])
}

func bodyWithTag(tag uint64) *cell.Cell {
	return cell.BeginCell().MustStoreUInt(tag, 64).EndCell()
}

type msgOpts struct {
	importFee  uint64
	inlineBody bool
}

// testMsg mimics what the ingress layer holds after deserializing and
// emulating a message: the original BOC bytes and the root cell.
type testMsg struct {
	raw  []byte
	root *cell.Cell
}

func buildExtMsg(t *testing.T, wc int32, addr [32]byte, body *cell.Cell, o msgOpts) testMsg {
	t.Helper()
	b := cell.BeginCell().
		MustStoreUInt(0b10, 2).
		MustStoreUInt(0b00, 2).
		MustStoreAddr(addrOf(wc, addr)).
		MustStoreCoins(o.importFee).
		MustStoreUInt(0, 1) // init: nothing
	if o.inlineBody {
		s, err := body.BeginParse()
		if err != nil {
			t.Fatal(err)
		}
		b.MustStoreUInt(0, 1).MustStoreBuilder(s.ToBuilder())
	} else {
		b.MustStoreUInt(1, 1).MustStoreRef(body)
	}
	c := b.EndCell()
	return testMsg{raw: c.ToBOC(), root: c}
}

// ---- pool env ----

type poolEnv struct {
	t     *testing.T
	pool  *Pool
	clock *fakeClock
}

func newPoolEnv(t *testing.T, mutate ...func(*Config)) *poolEnv {
	t.Helper()
	clock := newFakeClock()
	cfg := Config{Clock: clock}
	for _, m := range mutate {
		m(&cfg)
	}
	return &poolEnv{t: t, pool: New(cfg), clock: clock}
}

func (n *destinationState) queueSize(source ShardIdent) (uint64, error) {
	n.mu.Lock()
	defer n.mu.Unlock()

	run := n.runs[source]
	if run == nil {
		return 0, ErrNotFound
	}

	return uint64(run.queueSize), nil
}

func bindTestMessages(source ShardIdent, seqno uint32, messages []*InternalMessage) {
	for _, message := range messages {
		message.Source = source
		if message.SourceSeqno == 0 || message.SourceSeqno > seqno {
			message.SourceSeqno = seqno
		}
	}
}

func (n *destinationState) Seed(
	source ShardIdent,
	top SourceRef,
	messages []*InternalMessage,
	queueTotal uint64,
) error {
	bindTestMessages(source, top.Seqno, messages)
	return n.seed(source, top, messages, queueTotal)
}

func (n *destinationState) ApplyBlock(source ShardIdent, ref SourceRef, delta *InternalsDelta) error {
	bindTestMessages(source, ref.Seqno, delta.Added)
	return n.applyBlock(source, ref, delta)
}

func (n *destinationState) AddCandidate(
	source ShardIdent,
	id, parent [32]byte,
	seqno uint32,
	delta *InternalsDelta,
) error {
	bindTestMessages(n.owner, seqno, delta.Added)
	n.mu.Lock()
	_, continued := n.candidates[parent]
	n.mu.Unlock()
	request := CandidateRequest{ID: id, Seqno: seqno, Delta: delta}
	if continued {
		request.Parent = &parent
	} else {
		request.Base = []CandidateSource{{
			Source: source,
			Visible: SourceRef{
				Seqno:    seqno - 1,
				RootHash: parent,
			},
		}}
	}

	return n.addCandidate(request)
}

func (n *destinationState) DropCandidate(id [32]byte) {
	n.dropCandidate(id)
}

func (n *destinationState) SourceTop(source ShardIdent) (SourceRef, error) {
	return n.sourceTop(source)
}

func (n *destinationState) Cut(request CutRequest) (*Cut, error) {
	return n.cut(request)
}

func (n *destinationState) PruneSources(keep func(ShardIdent) bool) {
	n.pruneSources(keep)
}

func (n *destinationState) Stats() InternalsStats {
	return n.statsSnapshot()
}

// mustAdd pools a message and returns its assembled view for assertions.
func (e *poolEnv) mustAdd(m testMsg, priority int) *ExternalMessage {
	e.t.Helper()
	msg, err := newExternalMessage(len(m.raw), m.root, nil)
	if err != nil {
		e.t.Fatalf("assemble: %v", err)
	}
	result, err := e.pool.AddExternal(len(m.raw), m.root, nil, priority)
	if err != nil {
		e.t.Fatalf("add: %v", err)
	}
	if result.Outcome != ExternalAddInserted || result.Destination != msg.destination() {
		e.t.Fatalf("add result = %+v, want inserted destination %+v", result, msg.destination())
	}
	return msg
}

// assembleMsg builds the pool view of a test message, failing the test on
// malformed input.
func assembleMsg(t *testing.T, m testMsg) *ExternalMessage {
	t.Helper()
	msg, err := newExternalMessage(len(m.raw), m.root, nil)
	if err != nil {
		t.Fatal(err)
	}
	return msg
}

func requireEqual[T comparable](t *testing.T, got, want T, msg string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", msg, got, want)
	}
}
