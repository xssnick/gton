package simplex

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

// TestRunnerSurfacesTimerFatal: a session that fails fatally from a
// timer-driven path (the journal write of a timeout skip vote) must make
// Runner.Run return, not sleep until the next input. NextWakeup reports "no
// deadline" once the engine failed, so without the explicit error check the
// loop would idle for an hour.
func TestRunnerSurfacesTimerFatal(t *testing.T) {
	vals, keys := testValidators(4)
	var session [32]byte
	session[0] = 0x42

	params := DefaultParams()
	params.FirstBlockTimeout = 50 * time.Millisecond
	params.TargetRate = 30 * time.Millisecond
	params.StandstillTimeout = time.Hour

	j := NewMemoryJournal(4)
	j.OnBeforeWrite(func(kind string) error {
		if kind == "vote" {
			return errors.New("disk on fire")
		}
		return nil
	})
	hooks := acceptingHooks{}

	eng, err := NewEngine(Config{
		SessionID:            session,
		ProtocolVersion:      3,
		Validators:           vals,
		LocalIndex:           0,
		SlotsPerLeaderWindow: 4,
		Params:               &params,
		Journal:              j,
		Transport:            &fakeTransport{},
		Hooks:                hooks,
		Signer:               newTestSigner(keys[0]),
	})
	if err != nil {
		t.Fatal(err)
	}

	r := NewRunner(eng)
	runErr := make(chan error, 1)
	go func() { runErr <- r.Run(context.Background()) }()

	select {
	case e := <-runErr:
		if e == nil {
			t.Fatal("Run must return the fatal session error, got nil")
		}
		if !bytes.Contains([]byte(e.Error()), []byte("disk on fire")) {
			t.Fatalf("unexpected error: %v", e)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after a timer-driven fatal error")
	}
}

// TestRunnerAccessors covers the loop-safe state accessors.
func TestRunnerAccessors(t *testing.T) {
	vals, keys := testValidators(4)
	var session [32]byte
	hooks := acceptingHooks{}

	eng, err := NewEngine(Config{
		SessionID:            session,
		ProtocolVersion:      3,
		Validators:           vals,
		LocalIndex:           0,
		SlotsPerLeaderWindow: 4,
		Journal:              NewMemoryJournal(4),
		Transport:            &fakeTransport{},
		Hooks:                hooks,
		Signer:               newTestSigner(keys[0]),
	})
	if err != nil {
		t.Fatal(err)
	}

	r := NewRunner(eng)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- r.Run(ctx) }()

	// Slot 0 exists from Start, so the dump is never empty once the loop runs.
	deadline := time.After(3 * time.Second)
	for r.DebugDump() == "" {
		select {
		case <-deadline:
			t.Fatal("engine never produced pool state")
		case <-time.After(5 * time.Millisecond):
		}
	}
	requireEqual(t, r.Stats().CertificatesStored, uint64(0), "no certificates yet")
	if err := r.Err(); err != nil {
		t.Fatalf("unexpected session error: %v", err)
	}

	p := DefaultParams()
	p.TargetRate = 5 * time.Second
	if err := r.UpdateParams(p); err != nil {
		t.Fatal(err)
	}

	bad := DefaultParams()
	bad.TargetRate = 0
	if err := r.UpdateParams(bad); err == nil {
		t.Fatal("a zero target rate must be rejected")
	}

	cancel()
	<-done
	// Accessors on a stopped loop must not hang.
	_ = r.Stats()
	if err := r.Err(); err != nil {
		t.Fatalf("unexpected error after stop: %v", err)
	}
	if err := r.UpdateParams(p); !errors.Is(err, ErrStopped) {
		t.Fatalf("update after stop = %v, want %v", err, ErrStopped)
	}
	r.mu.Lock()
	queued := len(r.queue)
	r.mu.Unlock()
	requireEqual(t, queued, 0, "stopped runner queue")
}

func TestNewEngineRequiresHooks(t *testing.T) {
	validators, keys := testValidators(4)
	hooks := acceptingHooks{}
	base := Config{
		ProtocolVersion:      3,
		Validators:           validators,
		LocalIndex:           0,
		SlotsPerLeaderWindow: 4,
		Journal:              NewMemoryJournal(4),
		Transport:            &fakeTransport{},
		Hooks:                hooks,
		Signer:               newTestSigner(keys[0]),
	}

	withoutHooks := base
	withoutHooks.Hooks = nil
	if _, err := NewEngine(withoutHooks); err == nil {
		t.Fatal("validator without hooks must be rejected")
	}

	for protocolVersion := uint8(0); protocolVersion <= MaxProtocolVersion; protocolVersion++ {
		supported := base
		supported.ProtocolVersion = protocolVersion
		if _, err := NewEngine(supported); err != nil {
			t.Fatalf("protocol version %d: %v", protocolVersion, err)
		}
	}

	unsupported := base
	unsupported.ProtocolVersion = MaxProtocolVersion + 1
	if _, err := NewEngine(unsupported); err == nil {
		t.Fatal("unrepresentable consensus protocol must be rejected")
	}

	observer := base
	observer.LocalIndex = ObserverIndex
	observer.Signer = nil
	if _, err := NewEngine(observer); err != nil {
		t.Fatalf("observer with hooks: %v", err)
	}
	observer.Hooks = nil
	if _, err := NewEngine(observer); err == nil {
		t.Fatal("observer without outcome hooks must be rejected")
	}
}

// TestParamsValidation: parameter sets that would wedge the engine (a zero
// pacing, a zero standstill timeout, a zero egress budget) are refused both at
// construction and on a live update.
func TestParamsValidation(t *testing.T) {
	base := DefaultParams()
	cases := map[string]func(*Params){
		"zero target rate":                      func(p *Params) { p.TargetRate = 0 },
		"zero first block":                      func(p *Params) { p.FirstBlockTimeout = 0 },
		"zero first block multiplier":           func(p *Params) { p.FirstBlockTimeoutMultiplier = 0 },
		"negative first block multiplier":       func(p *Params) { p.FirstBlockTimeoutMultiplier = -1 },
		"nan first block multiplier":            func(p *Params) { p.FirstBlockTimeoutMultiplier = math.NaN() },
		"infinite first block multiplier":       func(p *Params) { p.FirstBlockTimeoutMultiplier = math.Inf(1) },
		"zero candidate resolve multiplier":     func(p *Params) { p.CandidateResolveTimeoutMultiplier = 0 },
		"negative candidate resolve multiplier": func(p *Params) { p.CandidateResolveTimeoutMultiplier = -1 },
		"nan candidate resolve multiplier":      func(p *Params) { p.CandidateResolveTimeoutMultiplier = math.NaN() },
		"infinite candidate resolve multiplier": func(p *Params) { p.CandidateResolveTimeoutMultiplier = math.Inf(1) },
		"zero standstill timeout":               func(p *Params) { p.StandstillTimeout = 0 },
		"zero egress budget": func(p *Params) {
			p.StandstillMaxEgressBytesPerS = 0
			p.StandstillMinEgressBytesPerS = 0
		},
	}
	vals, keys := testValidators(4)
	hooks := acceptingHooks{}
	for name, mutate := range cases {
		p := base
		mutate(&p)
		if err := p.Validate(); err == nil {
			t.Errorf("%s: validate must fail", name)
		}
		_, err := NewEngine(Config{
			ProtocolVersion: 3, Validators: vals, LocalIndex: 0, SlotsPerLeaderWindow: 4, Params: &p,
			Journal: NewMemoryJournal(4), Transport: &fakeTransport{},
			Hooks:  hooks,
			Signer: newTestSigner(keys[0]),
		})
		if err == nil {
			t.Errorf("%s: NewEngine must fail", name)
		}
	}

	env := newTestEnv(t, withLocal(1))
	env.start()
	p := base
	p.TargetRate = 7 * time.Second
	if err := env.eng.UpdateParams(p); err != nil {
		t.Fatal(err)
	}
	requireEqual(t, env.eng.params.TargetRate, 7*time.Second, "params updated")
	bad := base
	bad.StandstillTimeout = 0
	if err := env.eng.UpdateParams(bad); err == nil {
		t.Fatal("invalid update must be rejected")
	}
	requireEqual(t, env.eng.params.TargetRate, 7*time.Second, "old params kept on rejection")
}

// TestJournalWrappedAlreadySaved: a Journal that wraps ErrAlreadySaved (the
// documented sentinel contract allows %w) must be treated as a duplicate on
// both the vote and the certificate path, not as a fatal write failure.
func TestJournalWrappedAlreadySaved(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.journal.OnBeforeWrite(func(kind string) error {
		if kind == "cert" {
			return fmt.Errorf("pebble: %w", ErrAlreadySaved)
		}
		return nil
	})
	env.start()

	id := candID(0, 0x31)
	env.deliverVote(0, NotarizeVote(id))
	env.deliverVote(2, NotarizeVote(id))
	env.deliverVote(3, NotarizeVote(id))

	env.requireNoFatal()
	requireEqual(t, len(env.hooks.notarized), 1, "certificate applied despite wrapped duplicate")
}

// outOfRangeSchedule is a broken embedder-supplied schedule.
type outOfRangeSchedule struct{ n uint32 }

func (s outOfRangeSchedule) ExpectedLeader(uint32) uint32 { return s.n + 5 }

// TestOutOfRangeLeaderRejected: a leader index outside the validator set must
// produce an error, never an index panic on the untrusted candidate path.
func TestOutOfRangeLeaderRejected(t *testing.T) {
	vals, keys := testValidators(4)
	var session [32]byte
	hooks := acceptingHooks{}
	eng, err := NewEngine(Config{
		SessionID:            session,
		ProtocolVersion:      3,
		Validators:           vals,
		LocalIndex:           1,
		SlotsPerLeaderWindow: 4,
		Schedule:             outOfRangeSchedule{n: 4},
		Journal:              NewMemoryJournal(4),
		Transport:            &fakeTransport{},
		Hooks:                hooks,
		Signer:               newTestSigner(keys[1]),
		Clock:                newFakeClock(),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = eng.Start(); err != nil {
		t.Fatal(err)
	}
	c := &Candidate{
		ID:     CandidateID{Slot: 0},
		Parent: Genesis(),
		Leader: 9,
		Block: ton.BlockIDExt{
			Workchain: 0, Shard: -0x8000000000000000, SeqNo: 1,
			RootHash: make([]byte, 32), FileHash: make([]byte, 32),
		},
		Signature: make([]byte, 64),
	}
	c.ID = c.ComputeID(0)
	if err = eng.SubmitCandidate(c); err == nil {
		t.Fatal("an out-of-range leader must be rejected")
	}
}

// TestStoreOverlapsValidation: the durable store is started when the candidate
// is accepted and runs concurrently with the parent wait and the validation
// gate; the notarize vote waits for both.
func TestStoreOverlapsValidation(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.hooks.storeDefer = true
	env.start()

	cand := env.makeCandidate(0, Genesis(), 0x55)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	// The store starts before the validation verdict.
	requireEqual(t, len(env.hooks.stored), 1, "store started on acceptance")
	requireEqual(t, len(env.hooks.validations), 1, "validation started")

	// Validation alone is not enough.
	env.completeValidation(cand.ID, nil)
	requireEqual(t, env.trans.countVotes(VoteNotarize), 0, "no vote before the store lands")

	env.completeStores()
	requireEqual(t, env.trans.countVotes(VoteNotarize), 1, "vote after the store lands")
	env.requireNoFatal()
}

// TestStoreCompletesBeforeValidation covers the opposite completion order.
func TestStoreCompletesBeforeValidation(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.hooks.storeDefer = true
	env.start()

	cand := env.makeCandidate(0, Genesis(), 0x56)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	env.completeStores()
	requireEqual(t, env.trans.countVotes(VoteNotarize), 0, "no vote before validation")

	env.completeValidation(cand.ID, nil)
	requireEqual(t, env.trans.countVotes(VoteNotarize), 1, "vote after validation")
	env.requireNoFatal()
}

// TestStoreFailureIsFatal: a store we cannot complete invalidates the vote it
// would stand for.
func TestStoreFailureIsFatal(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.hooks.storeErr = errors.New("no space left")
	env.start()

	cand := env.makeCandidate(0, Genesis(), 0x57)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	if len(env.hooks.fatals) != 1 {
		t.Fatalf("store failure must be fatal, got %d fatals", len(env.hooks.fatals))
	}
	requireEqual(t, env.trans.countVotes(VoteNotarize), 0, "no vote after a failed store")
}

// TestCertificateWireIsCanonical: certificates are re-serialized from the
// decoded object, never echoed from the received frame. TL bytes fields carry
// unchecked alignment padding, so echoing would let a peer pick the content
// hash we journal and re-gossip.
func TestCertificateWireIsCanonical(t *testing.T) {
	env := newTestEnv(t, withLocal(1))
	env.start()

	cert := env.buildCert(SkipVote(0), 0, 2, 3)
	canonical := cert.Serialize()

	// Flip the alignment padding of the last signature: the frame stays
	// decodable and is accepted, but it is not canonical.
	dirty := append([]byte(nil), canonical...)
	dirty[len(dirty)-1] = 0xff
	if bytes.Equal(dirty, canonical) {
		t.Fatal("test vector has no padding to dirty")
	}
	parsed, err := parseCertificate(dirty, 4)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = env.eng.verifyCertificate(parsed); err != nil {
		t.Fatalf("dirty padding must still verify: %v", err)
	}
	if !bytes.Equal(parsed.Serialize(), canonical) {
		t.Fatal("Serialize must return the canonical encoding, not the received frame")
	}
	requireEqual(t, CertificateRecordHash(parsed), CertificateRecordHash(cert), "journal dedup hash")

	// The engine must gossip the canonical bytes too.
	env.eng.HandleMessage(peer(2), 2, dirty)
	requireEqual(t, len(env.trans.gossips), 1, "certificate gossiped")
	if !bytes.Equal(env.trans.gossips[0].msg, canonical) {
		t.Fatal("gossiped bytes must be canonical")
	}
}

// ---- tracing ----

type recTracer struct {
	events []TraceEvent
	wire   [][]byte
	at     []time.Time
}

func (r *recTracer) Trace(at time.Time, ev TraceEvent) {
	r.events = append(r.events, ev)
	r.wire = append(r.wire, ev.Serialize())
	r.at = append(r.at, at)
}

func (r *recTracer) count() map[string]int {
	out := map[string]int{}
	for _, ev := range r.events {
		switch ev.(type) {
		case TraceSessionStarted:
			out["id"]++
		case TraceVoted:
			out["voted"]++
		case TraceCertObserved:
			out["cert"]++
		case TraceCandidateReceived:
			out["candidate"]++
		}
	}
	return out
}

// TestTraceEvents: the engine emits the canonical trace events at the canonical
// points — startup, own vote, saved certificate, candidate received.
func TestTraceEvents(t *testing.T) {
	tr := &recTracer{}
	env := newTestEnv(t, withLocal(1), func(_ *testEnv, cfg *Config) {
		cfg.Tracer = tr
		cfg.Workchain = -1
		cfg.Shard = 0x8000000000000000
		cfg.CCSeqno = 77
	})
	env.start()

	got := tr.count()
	requireEqual(t, got["id"], 1, "session id event")
	id0 := tr.events[0].(TraceSessionStarted)
	requireEqual(t, id0.Workchain, int32(-1), "workchain")
	requireEqual(t, id0.CCSeqno, uint32(77), "cc seqno")
	requireEqual(t, id0.LocalIndex, 1, "local index")
	requireEqual(t, id0.TotalValidators, 4, "validator count")

	cand := env.makeCandidate(0, Genesis(), 0x77)
	if err := env.eng.SubmitCandidate(cand); err != nil {
		t.Fatal(err)
	}
	env.completeValidation(cand.ID, nil)
	env.deliverVote(2, NotarizeVote(cand.ID))
	env.deliverVote(3, NotarizeVote(cand.ID))

	got = tr.count()
	// The candidate came from validator 0, not from us.
	requireEqual(t, got["candidate"], 1, "candidate received event")
	// Our notarize plus the finalize that follows the certificate.
	requireEqual(t, got["voted"], 2, "voted events")
	requireEqual(t, got["cert"], 1, "cert observed event")

	// Every event must be a decodable boxed consensus.stats.Event, and the
	// timestamped wrapper must carry the engine clock reading.
	for i, w := range tr.wire {
		if len(w) < 4 {
			t.Fatalf("event %d: empty wire", i)
		}
		stamped := EncodeTimestampedTraceEvent(tr.at[i], tr.events[i])
		if got := binary.LittleEndian.Uint32(stamped[:4]); got != idStatsTimestampedEvent {
			t.Fatalf("event %d: wrong wrapper ctor %#08x", i, got)
		}
		ts := math.Float64frombits(binary.LittleEndian.Uint64(stamped[4:12]))
		want := float64(tr.at[i].UnixNano()) / float64(time.Second)
		if ts != want {
			t.Fatalf("event %d: ts %v, want %v", i, ts, want)
		}
		if !bytes.Equal(stamped[12:], w) {
			t.Fatalf("event %d: wrapper body mismatch", i)
		}
	}
}

// TestTraceCandidateReceivedWire checks the candidateReceived payload against
// a hand-decoded layout: bare candidateId, boxed parent, boxed block, Bool.
func TestTraceCandidateReceivedWire(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: 0, Shard: -0x8000000000000000, SeqNo: 5,
		RootHash: bytes.Repeat([]byte{1}, 32), FileHash: bytes.Repeat([]byte{2}, 32),
	}
	ev := TraceCandidateReceived{
		ID:     candID(3, 0x44),
		Parent: Parent(candID(2, 0x33)),
		Block:  &block,
	}
	wire := ev.Serialize()
	if got := binary.LittleEndian.Uint32(wire[:4]); got != tl.CRC(schemeStatsCandidateReceived) {
		t.Fatalf("wrong ctor %#08x", got)
	}
	// id: slot(4) + hash(32) bare, then the boxed parent constructor.
	requireEqual(t, binary.LittleEndian.Uint32(wire[4:8]), uint32(3), "slot")
	if got := binary.LittleEndian.Uint32(wire[40:44]); got != tl.CRC(
		"consensus.candidateParent id:consensus.CandidateId = consensus.CandidateParent") {
		t.Fatalf("wrong parent ctor %#08x", got)
	}
	// An empty candidate switches the block union to consensus.stats.empty.
	ev.Block = nil
	ev.Parent = Genesis()
	wire = ev.Serialize()
	if got := binary.LittleEndian.Uint32(wire[40:44]); got != tl.CRC(
		"consensus.candidateWithoutParents = consensus.CandidateParent") {
		t.Fatalf("wrong genesis parent ctor %#08x", got)
	}
	if got := binary.LittleEndian.Uint32(wire[44:48]); got != tl.CRC(schemeStatsCandidateEmpty) {
		t.Fatalf("wrong empty block ctor %#08x", got)
	}
}
