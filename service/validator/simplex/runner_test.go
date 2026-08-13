package simplex

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

// The Runner smoke test wires several production Runners over a goroutine-safe
// in-memory network and lets them reach finality on the wall clock. Run with
// -race: it exercises the real concurrency envelope of the wrapper.

type runnerNet struct {
	mu    sync.Mutex
	nodes map[int]*runnerNode
}

type runnerNode struct {
	idx    int
	net    *runnerNet
	runner *Runner
	hooks  *runnerHooks
}

type runnerTransport struct {
	net *runnerNet
	idx int
}

type signalTransport struct {
	once sync.Once
	sent chan struct{}
}

func (t *signalTransport) BroadcastToAll([]byte) {
	t.once.Do(func() { close(t.sent) })
}

func (t *signalTransport) BroadcastToValidators(msg []byte) { t.BroadcastToAll(msg) }

func (t *signalTransport) BroadcastToRandom(uint32, []byte) {}

func (t *runnerTransport) BroadcastToAll(msg []byte) {
	t.net.mu.Lock()
	defer t.net.mu.Unlock()
	for idx, n := range t.net.nodes {
		if idx == t.idx {
			continue
		}
		n.runner.HandleMessage(peer(t.idx), t.idx, msg)
	}
}

func (t *runnerTransport) BroadcastToValidators(msg []byte) { t.BroadcastToAll(msg) }

func (t *runnerTransport) BroadcastToRandom(count uint32, msg []byte) {
	t.BroadcastToAll(msg)
}

type runnerHooks struct {
	node *runnerNode

	mu        sync.Mutex
	finalized []CandidateID
	fatal     error

	session [32]byte
	keys    []ed25519.PrivateKey
	spw     uint32
}

func (h *runnerHooks) ValidateCandidate(_ *Candidate, done func(error)) {
	go done(nil)
}

func (h *runnerHooks) StoreCandidate(_ *Candidate, done func(error)) {
	go done(nil)
}

func (h *runnerHooks) HandleWindow(w Window) {
	if !w.LocalLeader || w.ObservedSlot != w.StartSlot {
		return
	}

	// Produce a chained window of candidates and hand them to everyone.
	parent := w.Base
	cands := make([]*Candidate, 0, w.EndSlot-w.StartSlot)
	for slot := w.StartSlot; slot < w.EndSlot; slot++ {
		c := &Candidate{
			ID:     CandidateID{Slot: slot},
			Parent: parent,
			Leader: uint32(h.node.idx),
			Block:  ton.BlockIDExt{Workchain: 0, Shard: -0x8000000000000000, SeqNo: slot + 1},
		}
		rootHash := sha256.Sum256([]byte(fmt.Sprintf("rblk-%d-%d-%v", h.node.idx, slot, parent)))
		fileHash := sha256.Sum256(rootHash[:])
		c.Block.RootHash = rootHash[:]
		c.Block.FileHash = fileHash[:]
		c.CollatedFileHash = sha256.Sum256(c.Block.FileHash)
		c.ID = c.ComputeID(slot)
		sig, err := SignCandidate(newTestSigner(h.keys[h.node.idx]), h.session, c.ID)
		if err != nil {
			panic(err)
		}
		c.Signature = sig
		parent = Parent(c.ID)
		cands = append(cands, c)
	}
	go func() {
		h.node.net.mu.Lock()
		nodes := make([]*runnerNode, 0, len(h.node.net.nodes))
		for _, n := range h.node.net.nodes {
			nodes = append(nodes, n)
		}
		h.node.net.mu.Unlock()
		for _, c := range cands {
			for _, n := range nodes {
				n.runner.SubmitCandidate(c)
			}
		}
	}()
}

func (h *runnerHooks) OnNotarized(CandidateID, *Certificate) {}

func (h *runnerHooks) OnFinalized(id CandidateID, cert *Certificate) {
	h.mu.Lock()
	h.finalized = append(h.finalized, id)
	h.mu.Unlock()
}

func (h *runnerHooks) OnMisbehavior(uint32, Misbehavior) {}

func (h *runnerHooks) OnFatal(err error) {
	h.mu.Lock()
	h.fatal = err
	h.mu.Unlock()
}

func (h *runnerHooks) finalizedCount() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.finalized)
}

func TestRunnerRealTimeSession(t *testing.T) {
	runRunnerSession(t, func(n int) Journal { return NewMemoryJournal(n) })
}

// TestRunnerRealTimeSessionAsyncJournal runs the same session over a journal
// that completes saves from its own goroutine after a delay — the production
// shape of a WAL-backed journal. Run with -race.
func TestRunnerRealTimeSessionAsyncJournal(t *testing.T) {
	runRunnerSession(t, func(n int) Journal { return &goroutineJournal{inner: NewMemoryJournal(n)} })
}

// TestRunnerCancellationUnderContinuousPosts guards the event-loop fairness
// boundary. A task that replenishes the queue forever must not prevent the
// runner from observing cancellation.
func TestRunnerCancellationUnderContinuousPosts(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	// newTestEnv uses a fixed consensus clock. The production runner compares
	// deadlines against wall time, so this event-loop fairness test must use
	// the production clock as well.
	env.eng.clock = SystemClock{}
	runner := NewRunner(env.eng)

	var executed atomic.Uint64
	var flood func()
	flood = func() {
		executed.Add(1)
		runner.post(flood)
	}
	runner.post(flood)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	deadline := time.After(3 * time.Second)
	for executed.Load() < runnerBatchSize*2 {
		select {
		case err := <-done:
			t.Fatalf("runner exited before cancellation: %v", err)
		case <-deadline:
			t.Fatal("continuous task did not run")
		case <-time.After(time.Millisecond):
		}
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runner cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("continuous queue starved cancellation")
	}
}

func TestRunnerPrecheckCandidateBroadcast(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.eng.clock = SystemClock{}
	runner := NewRunner(env.eng)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	first := [32]byte{0x11}
	second := [32]byte{0x22}
	if err := runner.PrecheckCandidateBroadcast(0, first, false); err != nil {
		t.Fatal(err)
	}
	if err := runner.PrecheckCandidateBroadcast(0, first, true); err != nil {
		t.Fatal(err)
	}
	if err := runner.PrecheckCandidateBroadcast(0, second, true); err == nil {
		t.Fatal("conflicting broadcast id was accepted")
	}

	cancel()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestRunnerPrioritizesCandidateWorkOverDirectMessageBacklog(t *testing.T) {
	runner := NewRunner(&Engine{})
	var order []string
	for i := 0; i < runnerBatchSize-1; i++ {
		runner.post(func() { order = append(order, "message") })
	}
	runner.postPriority(func() { order = append(order, "candidate") })

	batch, stopped, more := runner.takeBatch()
	if stopped || more {
		t.Fatalf("take batch state stopped=%t more=%t", stopped, more)
	}
	for _, work := range batch {
		work()
	}
	if len(order) != runnerBatchSize || order[0] != "candidate" {
		t.Fatalf("priority batch order = %q", order)
	}
}

// TestRunnerTimerUnderContinuousPosts verifies the same fairness property for
// consensus timers. The first voter timeout must cast a skip even while every
// processed task immediately posts another one.
func TestRunnerTimerUnderContinuousPosts(t *testing.T) {
	validators, keys := testValidators(1)
	params := DefaultParams()
	params.TargetRate = 5 * time.Millisecond
	params.FirstBlockTimeout = 10 * time.Millisecond
	params.StandstillTimeout = time.Hour
	transport := &signalTransport{sent: make(chan struct{})}
	eng, err := NewEngine(Config{
		ProtocolVersion:      3,
		Validators:           validators,
		LocalIndex:           0,
		SlotsPerLeaderWindow: 4,
		Params:               &params,
		Journal:              NewMemoryJournal(1),
		Transport:            transport,
		Hooks:                acceptingHooks{},
		Signer:               newTestSigner(keys[0]),
	})
	if err != nil {
		t.Fatal(err)
	}
	runner := NewRunner(eng)

	var flood func()
	flood = func() { runner.post(flood) }
	runner.post(flood)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	select {
	case <-transport.sent:
	case err := <-done:
		t.Fatalf("runner exited before timeout: %v", err)
	case <-time.After(time.Second):
		t.Fatal("continuous queue starved consensus timer")
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runner cancellation: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("runner did not stop")
	}
}

// goroutineJournal defers every completion to a fresh goroutine with a small
// delay, forcing the engines to keep running while records are "in flight".
type goroutineJournal struct{ inner *MemoryJournal }

func (g *goroutineJournal) Bootstrap() (*BootstrapState, error) { return g.inner.Bootstrap() }

func (g *goroutineJournal) defer2(done func(error)) func(error) {
	return func(err error) {
		go func() {
			time.Sleep(200 * time.Microsecond)
			done(err)
		}()
	}
}

func (g *goroutineJournal) SaveOurVote(v Vote, done func(error)) {
	g.inner.SaveOurVote(v, g.defer2(done))
}

func (g *goroutineJournal) SaveCertificate(c *Certificate, done func(error)) {
	g.inner.SaveCertificate(c, g.defer2(done))
}

func (g *goroutineJournal) SaveFirstNonAnnouncedWindow(w uint32, done func(error)) {
	g.inner.SaveFirstNonAnnouncedWindow(w, g.defer2(done))
}

func runRunnerSession(t *testing.T, mkJournal func(n int) Journal) {
	const n = 4
	vals, keys := testValidators(n)
	var session [32]byte
	session[0] = 0x99

	params := DefaultParams()
	params.TargetRate = 60 * time.Millisecond
	params.FirstBlockTimeout = 120 * time.Millisecond
	params.StandstillTimeout = 500 * time.Millisecond

	net := &runnerNet{nodes: map[int]*runnerNode{}}
	var runners []*Runner
	var hooks []*runnerHooks

	for i := 0; i < n; i++ {
		node := &runnerNode{idx: i, net: net}
		h := &runnerHooks{node: node, session: session, keys: keys, spw: 4}
		p := params
		eng, err := NewEngine(Config{
			SessionID:            session,
			ProtocolVersion:      3,
			Validators:           vals,
			LocalIndex:           i,
			SlotsPerLeaderWindow: 4,
			Params:               &p,
			Journal:              mkJournal(n),
			Transport:            &runnerTransport{net: net, idx: i},
			Hooks:                h,
			Signer:               newTestSigner(keys[i]),
		})
		if err != nil {
			t.Fatal(err)
		}
		node.runner = NewRunner(eng)
		node.hooks = h
		net.mu.Lock()
		net.nodes[i] = node
		net.mu.Unlock()
		runners = append(runners, node.runner)
		hooks = append(hooks, h)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	errCh := make(chan error, n)
	for _, r := range runners {
		go func(r *Runner) { errCh <- r.Run(ctx) }(r)
	}

	deadline := time.After(15 * time.Second)
	tick := time.NewTicker(50 * time.Millisecond)
	defer tick.Stop()
waitLoop:
	for {
		select {
		case <-deadline:
			break waitLoop
		case err := <-errCh:
			t.Fatalf("runner exited early: %v", err)
		case <-tick.C:
			min := 1 << 30
			for _, h := range hooks {
				if c := h.finalizedCount(); c < min {
					min = c
				}
			}
			if min >= 8 {
				break waitLoop
			}
		}
	}

	cancel()
	for range runners {
		select {
		case err := <-errCh:
			if err != nil {
				t.Fatalf("runner error: %v", err)
			}
		case <-time.After(5 * time.Second):
			t.Fatal("runner did not stop")
		}
	}

	for i, h := range hooks {
		if h.fatal != nil {
			t.Fatalf("node %d fatal: %v", i, h.fatal)
		}
		if c := h.finalizedCount(); c < 8 {
			t.Fatalf("node %d finalized only %d slots", i, c)
		}
	}
}
