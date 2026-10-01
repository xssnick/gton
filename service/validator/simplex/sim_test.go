package simplex

import (
	"container/heap"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

// The deterministic N-node simulator: real engines, virtual time, a scripted
// lossy network. Everything runs on one goroutine ordered by a (time, seq)
// event heap, so a fixed seed reproduces the run bit for bit.

type simEvent struct {
	at  time.Time
	seq uint64
	fn  func()
}

type simEventHeap []*simEvent

func (h simEventHeap) Len() int { return len(h) }
func (h simEventHeap) Less(i, j int) bool {
	if !h[i].at.Equal(h[j].at) {
		return h[i].at.Before(h[j].at)
	}
	return h[i].seq < h[j].seq
}
func (h simEventHeap) Swap(i, j int) { h[i], h[j] = h[j], h[i] }
func (h *simEventHeap) Push(x any)   { *h = append(*h, x.(*simEvent)) }
func (h *simEventHeap) Pop() any {
	old := *h
	n := len(old)
	ev := old[n-1]
	*h = old[:n-1]
	return ev
}

type simConfig struct {
	nodes           int
	spw             uint32
	seed            int64
	lossPct         int
	dupPct          int
	baseLatency     time.Duration
	jitter          time.Duration
	validationDelay map[int]time.Duration
	partitioned     func(at time.Time, from, to int) bool
	byzantine       map[int]bool // scripted nodes without engines
	params          *Params
}

type simNode struct {
	s       *sim
	idx     int
	eng     *Engine
	journal *MemoryJournal
	alive   bool

	finalized  []CandidateID
	notarized  map[uint32]CandidateID
	misreports map[uint32]int
	fatal      error
}

type sim struct {
	t       *testing.T
	cfg     simConfig
	rng     *rand.Rand
	clock   *fakeClock
	session [32]byte
	vals    []Validator
	keys    []ed25519.PrivateKey
	nodes   []*simNode

	events simEventHeap
	seq    uint64
	trace  []string
}

func newSim(t *testing.T, cfg simConfig) *sim {
	t.Helper()
	vals, keys := testValidators(cfg.nodes)
	s := &sim{
		t: t, cfg: cfg,
		rng:   rand.New(rand.NewSource(cfg.seed)),
		clock: newFakeClock(),
		vals:  vals, keys: keys,
	}
	for i := range s.session {
		s.session[i] = byte(0x33 + i)
	}
	if cfg.baseLatency == 0 {
		cfg.baseLatency = 20 * time.Millisecond
		s.cfg.baseLatency = cfg.baseLatency
	}
	for i := 0; i < cfg.nodes; i++ {
		n := &simNode{s: s, idx: i, notarized: map[uint32]CandidateID{}, misreports: map[uint32]int{}}
		n.journal = NewMemoryJournal(cfg.nodes)
		s.nodes = append(s.nodes, n)
	}
	for _, n := range s.nodes {
		if cfg.byzantine[n.idx] {
			continue
		}
		s.buildEngine(n)
		n.alive = true
	}
	return s
}

func (s *sim) buildEngine(n *simNode) {
	params := DefaultParams()
	if s.cfg.params != nil {
		params = *s.cfg.params
	}
	hooks := &simHooks{node: n}
	cfg := Config{
		SessionID:            s.session,
		ProtocolVersion:      3,
		Validators:           s.vals,
		LocalIndex:           n.idx,
		SlotsPerLeaderWindow: s.cfg.spw,
		Params:               &params,
		Journal:              n.journal,
		Transport:            &simTransport{node: n},
		Hooks:                hooks,
		Signer:               newTestSigner(s.keys[n.idx]),
		Clock:                s.clock,
	}
	eng, err := NewEngine(cfg)
	if err != nil {
		s.t.Fatal(err)
	}
	n.eng = eng
}

func (s *sim) schedule(d time.Duration, tag string, fn func()) {
	at := s.clock.Now().Add(d)
	s.seq++
	s.trace = append(s.trace, fmt.Sprintf("%d %s @%d", s.seq, tag, at.UnixMilli()))
	heap.Push(&s.events, &simEvent{at: at, seq: s.seq, fn: fn})
}

func (s *sim) wake(n *simNode) {
	if !n.alive {
		return
	}
	at := n.eng.NextWakeup()
	if at.IsZero() {
		return
	}
	d := at.Sub(s.clock.Now())
	if d < 0 {
		d = 0
	}
	idx := n.idx
	s.schedule(d, fmt.Sprintf("wake:%d", idx), func() {
		node := s.nodes[idx]
		if !node.alive {
			return
		}
		node.eng.Advance()
		s.wake(node)
	})
}

// run processes events until the horizon or until done returns true.
func (s *sim) run(horizon time.Duration, done func() bool) {
	end := s.clock.Now().Add(horizon)
	for len(s.events) > 0 {
		ev := heap.Pop(&s.events).(*simEvent)
		if ev.at.After(end) {
			return
		}
		if ev.at.After(s.clock.Now()) {
			s.clock.set(ev.at)
		}
		ev.fn()
		if done != nil && done() {
			return
		}
	}
}

func (s *sim) start() {
	for _, n := range s.nodes {
		if n.eng == nil {
			continue
		}
		if err := n.eng.Start(); err != nil {
			s.t.Fatalf("node %d start: %v", n.idx, err)
		}
		s.wake(n)
	}
}

// ---- network ----

func (s *sim) deliverToAll(from int, data []byte, tag string) {
	for _, dst := range s.nodes {
		if dst.idx == from {
			continue
		}
		s.deliverTo(from, dst.idx, data, tag)
	}
}

func (s *sim) deliverTo(from, to int, data []byte, tag string) {
	if s.cfg.partitioned != nil && s.cfg.partitioned(s.clock.Now(), from, to) {
		return
	}
	if s.cfg.lossPct > 0 && s.rng.Intn(100) < s.cfg.lossPct {
		return
	}
	copies := 1
	if s.cfg.dupPct > 0 && s.rng.Intn(100) < s.cfg.dupPct {
		copies = 2
	}
	msg := make([]byte, len(data))
	copy(msg, data)
	for c := 0; c < copies; c++ {
		lat := s.cfg.baseLatency
		if s.cfg.jitter > 0 {
			lat += time.Duration(s.rng.Int63n(int64(s.cfg.jitter)))
		}
		src, dst := from, to
		s.schedule(lat, fmt.Sprintf("%s:%d->%d", tag, from, to), func() {
			node := s.nodes[dst]
			if !node.alive {
				return
			}
			node.eng.HandleMessage(peer(src), src, msg)
			s.wake(node)
		})
	}
}

func (s *sim) deliverCandidate(from int, c *Candidate) {
	for _, dst := range s.nodes {
		if dst.idx == from || s.cfg.byzantine[dst.idx] {
			continue
		}
		if s.cfg.partitioned != nil && s.cfg.partitioned(s.clock.Now(), from, dst.idx) {
			continue
		}
		if s.cfg.lossPct > 0 && s.rng.Intn(100) < s.cfg.lossPct {
			continue
		}
		lat := s.cfg.baseLatency
		if s.cfg.jitter > 0 {
			lat += time.Duration(s.rng.Int63n(int64(s.cfg.jitter)))
		}
		dstIdx := dst.idx
		cc := *c
		s.schedule(lat, fmt.Sprintf("cand:%d->%d", from, dstIdx), func() {
			node := s.nodes[dstIdx]
			if !node.alive {
				return
			}
			_ = node.eng.SubmitCandidate(&cc)
			s.wake(node)
		})
	}
}

type simTransport struct {
	node *simNode
}

func (t *simTransport) BroadcastToAll(msg []byte) {
	t.node.s.deliverToAll(t.node.idx, msg, "msg")
}

func (t *simTransport) BroadcastToValidators(msg []byte) { t.BroadcastToAll(msg) }

func (t *simTransport) BroadcastToRandom(count uint32, msg []byte) {
	s := t.node.s
	others := make([]int, 0, len(s.nodes)-1)
	for _, n := range s.nodes {
		if n.idx != t.node.idx {
			others = append(others, n.idx)
		}
	}
	s.rng.Shuffle(len(others), func(i, j int) { others[i], others[j] = others[j], others[i] })
	if int(count) < len(others) {
		others = others[:count]
	}
	for _, to := range others {
		s.deliverTo(t.node.idx, to, msg, "gossip")
	}
}

// ---- hooks: candidate production + validation stubs ----

type simHooks struct {
	node *simNode
}

func (h *simHooks) ValidateCandidate(c *Candidate, done func(error)) {
	s := h.node.s
	delay := 5 * time.Millisecond
	if d, ok := s.cfg.validationDelay[h.node.idx]; ok {
		delay = d
	}
	idx := h.node.idx
	s.schedule(delay, fmt.Sprintf("validate:%d", idx), func() {
		node := s.nodes[idx]
		if !node.alive {
			return
		}
		done(nil)
		s.wake(node)
	})
}

func (h *simHooks) StoreCandidate(_ *Candidate, done func(error)) { done(nil) }

// HandleWindow produces a chain of candidates for a local leader window,
// each linking to the previous one, starting from the given base.
func (h *simHooks) HandleWindow(w Window) {
	if !w.LocalLeader || w.ObservedSlot != w.StartSlot {
		return
	}

	s := h.node.s
	idx := h.node.idx
	parent := w.Base
	for slot := w.StartSlot; slot < w.EndSlot; slot++ {
		c := s.makeCandidate(idx, slot, parent)
		parent = Parent(c.ID)
		delay := time.Duration(slot-w.StartSlot)*50*time.Millisecond + time.Millisecond
		s.schedule(delay, fmt.Sprintf("produce:%d/%d", idx, slot), func() {
			node := s.nodes[idx]
			if !node.alive {
				return
			}
			if err := node.eng.SubmitCandidate(c); err != nil {
				s.t.Errorf("node %d self candidate rejected: %v", idx, err)
			}
			s.deliverCandidate(idx, c)
			s.wake(node)
		})
	}
}

func (h *simHooks) OnNotarized(id CandidateID, cert VerifiedCertificate) {
	prev, ok := h.node.notarized[id.Slot]
	if ok && prev != id {
		h.node.s.t.Errorf("node %d: conflicting notarizations at slot %d: %s vs %s",
			h.node.idx, id.Slot, fmtSlotID(prev), fmtSlotID(id))
	}
	h.node.notarized[id.Slot] = id
}

func (h *simHooks) OnFinalized(id CandidateID, cert VerifiedCertificate) {
	h.node.finalized = append(h.node.finalized, id)
}

func (h *simHooks) OnMisbehavior(v uint32, m Misbehavior) {
	h.node.misreports[v]++
}

func (h *simHooks) OnFatal(err error) {
	h.node.fatal = err
}

// makeCandidate builds a deterministic signed candidate for a leader/slot.
func (s *sim) makeCandidate(leader int, slot uint32, parent ParentID) *Candidate {
	c := &Candidate{
		ID:     CandidateID{Slot: slot},
		Parent: parent,
		Leader: uint32(leader),
		Block:  ton.BlockIDExt{Workchain: 0, Shard: -0x8000000000000000, SeqNo: slot + 1},
	}
	seedHash := sha256.Sum256([]byte(fmt.Sprintf("blk-%d-%d-%v", leader, slot, parent)))
	fileHash := sha256.Sum256(seedHash[:])
	c.Block.RootHash = seedHash[:]
	c.Block.FileHash = fileHash[:]
	c.CollatedFileHash = sha256.Sum256(c.Block.FileHash)
	c.ID = c.ComputeID(slot)
	sig, err := SignCandidate(newTestSigner(s.keys[leader]), s.session, c.ID)
	if err != nil {
		s.t.Fatal(err)
	}
	c.Signature = sig
	return c
}

// ---- assertions ----

func (s *sim) requireNoFatal() {
	s.t.Helper()
	for _, n := range s.nodes {
		if n.fatal != nil {
			s.t.Fatalf("node %d fatal: %v", n.idx, n.fatal)
		}
	}
}

// requireConsistency checks that no two nodes disagree about any notarized
// or finalized slot.
func (s *sim) requireConsistency() {
	s.t.Helper()
	notar := map[uint32]CandidateID{}
	final := map[uint32]CandidateID{}
	for _, n := range s.nodes {
		for slot, id := range n.notarized {
			if prev, ok := notar[slot]; ok && prev != id {
				s.t.Fatalf("cross-node notarization conflict at slot %d", slot)
			}
			notar[slot] = id
		}
		for _, id := range n.finalized {
			if prev, ok := final[id.Slot]; ok && prev != id {
				s.t.Fatalf("cross-node finalization conflict at slot %d", id.Slot)
			}
			final[id.Slot] = id
			if nid, ok := notar[id.Slot]; ok && nid != id {
				s.t.Fatalf("finalized %s but notarized %s at slot %d", fmtSlotID(id), fmtSlotID(nid), id.Slot)
			}
		}
	}
}

func (s *sim) minFinalizedSlots(nodes ...int) int {
	min := -1
	for _, i := range nodes {
		n := len(s.nodes[i].finalized)
		if min == -1 || n < min {
			min = n
		}
	}
	return min
}

func allNodes() []int {
	out := make([]int, testValidatorCount)
	for i := range out {
		out[i] = i
	}
	return out
}

// ---- scenarios ----

func TestSimHappyPath(t *testing.T) {
	s := newSim(t, simConfig{nodes: 4, spw: 4, seed: 1})
	s.start()
	s.run(2*time.Minute, func() bool { return s.minFinalizedSlots(allNodes()...) >= 12 })

	if got := s.minFinalizedSlots(allNodes()...); got < 12 {
		t.Fatalf("insufficient progress: %d finalized slots", got)
	}
	s.requireConsistency()
	s.requireNoFatal()
	for _, n := range s.nodes {
		if len(n.misreports) != 0 {
			t.Fatalf("unexpected misbehavior reports on node %d", n.idx)
		}
	}
}

func TestSimLossReorderDuplicates(t *testing.T) {
	s := newSim(t, simConfig{
		nodes: 4, spw: 4, seed: 7,
		lossPct: 20, dupPct: 25, jitter: 150 * time.Millisecond,
	})
	s.start()
	s.run(10*time.Minute, func() bool { return s.minFinalizedSlots(allNodes()...) >= 6 })

	if got := s.minFinalizedSlots(allNodes()...); got < 6 {
		t.Fatalf("insufficient progress under loss: %d finalized slots", got)
	}
	s.requireConsistency()
	s.requireNoFatal()
}

func TestSimLeaderCrashIsSkipped(t *testing.T) {
	s := newSim(t, simConfig{nodes: 4, spw: 4, seed: 3})
	s.start()
	// Kill validator 1 immediately: its windows (1, 5, ...) must be skipped
	// through by timeouts while the chain keeps growing.
	s.nodes[1].alive = false

	s.run(5*time.Minute, func() bool { return s.minFinalizedSlots(0, 2, 3) >= 8 })
	if got := s.minFinalizedSlots(0, 2, 3); got < 8 {
		t.Fatalf("insufficient progress with a dead leader: %d", got)
	}
	for _, n := range []*simNode{s.nodes[0], s.nodes[2], s.nodes[3]} {
		for _, id := range n.finalized {
			window := id.Slot / s.cfg.spw
			if window%4 == 1 {
				t.Fatalf("slot %d from the dead leader's window was finalized", id.Slot)
			}
		}
	}
	s.requireConsistency()
	s.requireNoFatal()
}

func TestSimPartitionHealsViaStandstill(t *testing.T) {
	partStart := time.Unix(1_700_000_000, 0).Add(3 * time.Second)
	partEnd := partStart.Add(45 * time.Second)
	s := newSim(t, simConfig{
		nodes: 4, spw: 4, seed: 5,
		partitioned: func(at time.Time, from, to int) bool {
			if at.Before(partStart) || at.After(partEnd) {
				return false
			}
			return (from < 2) != (to < 2) // {0,1} | {2,3}
		},
	})
	s.start()

	// Run into the partition and measure progress at its end.
	s.run(55*time.Second, nil)
	atHeal := s.minFinalizedSlots(allNodes()...)

	// After healing, the standstill resolution must revive the session.
	s.run(4*time.Minute, func() bool { return s.minFinalizedSlots(allNodes()...) >= atHeal+6 })
	if got := s.minFinalizedSlots(allNodes()...); got < atHeal+6 {
		t.Fatalf("no recovery after partition: %d -> %d", atHeal, got)
	}
	s.requireConsistency()
	s.requireNoFatal()
}

// TestSimByzantineEquivocator: a scripted validator signs two different
// candidates per slot and shows different votes to different peers. With
// less than 1/3 faulty weight the chain stays consistent, and certificates
// eventually expose the equivocation as provable misbehavior.
func TestSimByzantineEquivocator(t *testing.T) {
	s := newSim(t, simConfig{
		nodes: 4, spw: 4, seed: 11,
		byzantine: map[int]bool{3: true},
	})
	s.start()

	// The byzantine script reacts to nothing: it just spams conflicting
	// notarize votes for every slot in flight.
	var byzVote func(slot uint32)
	byzVote = func(slot uint32) {
		if slot > 40 {
			return
		}
		idA := candID(slot, byte(0xa0+slot%16))
		idB := candID(slot, byte(0xb0+slot%16))
		for to := 0; to < 3; to++ {
			target := idA
			if to == 2 {
				target = idB
			}
			sig := ed25519.Sign(s.keys[3], DataToSign(s.session, VoteBytes(NotarizeVote(target))))
			sv := SignedVote{ValidatorIndex: 3, Vote: NotarizeVote(target), Signature: sig}
			s.deliverTo(3, to, sv.Serialize(), "byz")
		}
		s.schedule(500*time.Millisecond, "byz-next", func() { byzVote(slot + 1) })
	}
	byzVote(0)

	s.run(5*time.Minute, func() bool { return s.minFinalizedSlots(0, 1, 2) >= 8 })
	if got := s.minFinalizedSlots(0, 1, 2); got < 8 {
		t.Fatalf("insufficient progress with a byzantine validator: %d", got)
	}
	s.requireConsistency()
	s.requireNoFatal()
}

// TestSimRestartCatchesUp: a validator crashes mid-run and restarts from its
// journal; it must rejoin, never equivocate, and catch up with finality.
func TestSimRestartCatchesUp(t *testing.T) {
	s := newSim(t, simConfig{nodes: 4, spw: 4, seed: 13})
	s.start()

	s.run(20*time.Second, nil)
	// Crash node 2: drop it from the network (its queued deliveries fizzle
	// on the alive check).
	s.nodes[2].alive = false
	crashFinalized := len(s.nodes[2].finalized)

	s.run(30*time.Second, nil)

	// Restart from the same journal.
	n := s.nodes[2]
	s.buildEngine(n)
	if err := n.eng.Start(); err != nil {
		t.Fatalf("restart: %v", err)
	}
	n.alive = true
	s.wake(n)

	target := s.minFinalizedSlots(0, 1, 3) + 8
	s.run(5*time.Minute, func() bool { return s.minFinalizedSlots(allNodes()...) >= target })
	if got := len(s.nodes[2].finalized); got <= crashFinalized {
		t.Fatalf("restarted node did not catch up: %d -> %d", crashFinalized, got)
	}
	s.requireConsistency()
	s.requireNoFatal()
	for _, n := range s.nodes {
		if n.misreports[2] > 0 {
			t.Fatalf("restarted node was reported for misbehavior on node %d", n.idx)
		}
	}
}

// TestSimSlowValidation: one validator's validation gate is slower than the
// slot budget; it skips, votes notarize late, and the session still makes
// progress with the remaining quorum.
func TestSimSlowValidation(t *testing.T) {
	s := newSim(t, simConfig{
		nodes: 4, spw: 4, seed: 17,
		validationDelay: map[int]time.Duration{1: 5 * time.Second},
	})
	s.start()
	s.run(5*time.Minute, func() bool { return s.minFinalizedSlots(allNodes()...) >= 8 })
	if got := s.minFinalizedSlots(allNodes()...); got < 8 {
		t.Fatalf("insufficient progress with slow validation: %d", got)
	}
	s.requireConsistency()
	s.requireNoFatal()
}

// TestSimDeterminism: identical seeds produce identical event traces.
func TestSimDeterminism(t *testing.T) {
	runOnce := func() []string {
		s := newSim(t, simConfig{nodes: 4, spw: 4, seed: 42, lossPct: 10, dupPct: 10, jitter: 80 * time.Millisecond})
		s.start()
		s.run(1*time.Minute, func() bool { return s.minFinalizedSlots(allNodes()...) >= 6 })
		s.requireConsistency()
		s.requireNoFatal()
		return s.trace
	}
	t1 := runOnce()
	t2 := runOnce()
	if len(t1) != len(t2) {
		t.Fatalf("trace lengths differ: %d vs %d", len(t1), len(t2))
	}
	for i := range t1 {
		if t1[i] != t2[i] {
			t.Fatalf("traces diverge at %d:\n%s\n%s", i, t1[i], t2[i])
		}
	}
}
