package simplex

import (
	"crypto/ed25519"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/ton"
)

// ---- deterministic keys ----

func testKey(i int) ed25519.PrivateKey {
	seed := make([]byte, ed25519.SeedSize)
	seed[0] = byte(i + 1)
	seed[1] = byte(i >> 8)
	seed[31] = 0x5a
	return ed25519.NewKeyFromSeed(seed)
}

type testSigner struct {
	key ed25519.PrivateKey
}

func (s testSigner) Sign(data []byte) ([]byte, error) {
	return ed25519.Sign(s.key, data), nil
}

func newTestSigner(key ed25519.PrivateKey) Signer {
	return testSigner{key: key}
}

func testValidators(n int, weights ...uint64) ([]Validator, []ed25519.PrivateKey) {
	vals := make([]Validator, n)
	keys := make([]ed25519.PrivateKey, n)
	for i := 0; i < n; i++ {
		keys[i] = testKey(i)
		w := uint64(1)
		if i < len(weights) {
			w = weights[i]
		}
		vals[i] = Validator{PublicKey: keys[i].Public().(ed25519.PublicKey), Weight: w}
	}
	return vals, keys
}

// ---- fake clock ----

type fakeClock struct {
	mu  sync.Mutex
	now time.Time
}

func newFakeClock() *fakeClock {
	return &fakeClock{now: time.Unix(1_700_000_000, 0)}
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

func (c *fakeClock) set(t time.Time) {
	c.mu.Lock()
	c.now = t
	c.mu.Unlock()
}

// ---- fake transport ----

type sentGossip struct {
	count uint32
	msg   []byte
}

type fakeTransport struct {
	broadcasts          [][]byte
	validatorBroadcasts [][]byte
	gossips             []sentGossip
}

func (t *fakeTransport) BroadcastToAll(msg []byte) {
	t.broadcasts = append(t.broadcasts, msg)
}

func (t *fakeTransport) BroadcastToValidators(msg []byte) {
	t.broadcasts = append(t.broadcasts, msg)
	t.validatorBroadcasts = append(t.validatorBroadcasts, msg)
}

func (t *fakeTransport) BroadcastToRandom(count uint32, msg []byte) {
	t.gossips = append(t.gossips, sentGossip{count: count, msg: msg})
}

func (t *fakeTransport) reset() {
	t.broadcasts = nil
	t.validatorBroadcasts = nil
	t.gossips = nil
}

// countVotes returns how many broadcast messages are votes of the given kind.
func (t *fakeTransport) countVotes(kind VoteKind) int {
	n := 0
	for _, msg := range t.broadcasts {
		if v, _, err := parseSignedVote(msg); err == nil && v.Kind == kind {
			n++
		}
	}
	return n
}

// ---- recording hooks ----

type recHooks struct {
	t *testing.T

	validations    []*Candidate
	validationDone map[CandidateID]func(error)
	stored         []*Candidate
	// storeDeferred collects candidates whose store completion the test drives
	// by hand (env.completeStores); by default stores complete inline.
	storeDefer     bool
	storeDeferred  []candidateCompletion
	windows        []Window
	notarized      []CandidateID
	notarizedCerts []*Certificate
	notarizedSeals []VerifiedCertificate
	finalized      []CandidateID
	finalizedCerts []*Certificate
	finalizedSeals []VerifiedCertificate
	misbehaviors   []struct {
		Validator uint32
		M         Misbehavior
	}
	fatals []error

	storeErr error
}

func newRecHooks(t *testing.T) *recHooks {
	t.Helper()
	return &recHooks{t: t, validationDone: make(map[CandidateID]func(error))}
}

type candidateCompletion struct {
	candidate *Candidate
	done      func(error)
}

type acceptingHooks struct{}

func (acceptingHooks) ValidateCandidate(_ *Candidate, done func(error)) { done(nil) }
func (acceptingHooks) StoreCandidate(_ *Candidate, done func(error))    { done(nil) }
func (acceptingHooks) HandleWindow(Window)                              {}
func (acceptingHooks) OnNotarized(CandidateID, VerifiedCertificate)     {}
func (acceptingHooks) OnFinalized(CandidateID, VerifiedCertificate)     {}
func (acceptingHooks) OnMisbehavior(uint32, Misbehavior)                {}
func (acceptingHooks) OnFatal(error)                                    {}

func (h *recHooks) ValidateCandidate(c *Candidate, done func(error)) {
	h.validations = append(h.validations, c)
	h.validationDone[c.ID] = done
}

func (h *recHooks) StoreCandidate(c *Candidate, done func(error)) {
	h.stored = append(h.stored, c)
	if h.storeDefer {
		h.storeDeferred = append(h.storeDeferred, candidateCompletion{candidate: c, done: done})
		return
	}
	done(h.storeErr)
}
func (h *recHooks) HandleWindow(w Window) { h.windows = append(h.windows, w) }
func (h *recHooks) OnNotarized(id CandidateID, cert VerifiedCertificate) {
	h.notarized = append(h.notarized, id)
	h.notarizedCerts = append(h.notarizedCerts, cert.Certificate())
	h.notarizedSeals = append(h.notarizedSeals, cert)
}
func (h *recHooks) OnFinalized(id CandidateID, cert VerifiedCertificate) {
	h.finalized = append(h.finalized, id)
	h.finalizedCerts = append(h.finalizedCerts, cert.Certificate())
	h.finalizedSeals = append(h.finalizedSeals, cert)
}
func (h *recHooks) OnMisbehavior(v uint32, m Misbehavior) {
	h.misbehaviors = append(h.misbehaviors, struct {
		Validator uint32
		M         Misbehavior
	}{v, m})
}
func (h *recHooks) OnFatal(err error) { h.fatals = append(h.fatals, err) }

// ---- single-engine test env ----

type testEnv struct {
	t       *testing.T
	vals    []Validator
	keys    []ed25519.PrivateKey
	session [32]byte
	spw     uint32

	eng     *Engine
	clock   *fakeClock
	trans   *fakeTransport
	hooks   *recHooks
	journal *MemoryJournal
	params  Params
}

type envOpt func(*testEnv, *Config)

const testValidatorCount = 4

func withLocal(idx int) envOpt {
	return func(_ *testEnv, cfg *Config) { cfg.LocalIndex = idx }
}

func withParams(p Params) envOpt {
	return func(env *testEnv, cfg *Config) { env.params = p; cfg.Params = &env.params }
}

func withShardPrefix(l uint32) envOpt {
	return func(_ *testEnv, cfg *Config) { cfg.ShardPrefixLen = l }
}

func withProtocolVersion(version uint8) envOpt {
	return func(_ *testEnv, cfg *Config) { cfg.ProtocolVersion = version }
}

func newTestEnv(t *testing.T, opts ...envOpt) *testEnv {
	t.Helper()
	vals, keys := testValidators(testValidatorCount)
	env := &testEnv{
		t:       t,
		vals:    vals,
		keys:    keys,
		spw:     4,
		clock:   newFakeClock(),
		trans:   &fakeTransport{},
		hooks:   newRecHooks(t),
		journal: NewMemoryJournal(testValidatorCount),
		params:  DefaultParams(),
	}
	for i := range env.session {
		env.session[i] = byte(0xd0 + i%16)
	}
	cfg := Config{
		SessionID:            env.session,
		ProtocolVersion:      3,
		Validators:           vals,
		LocalIndex:           0,
		SlotsPerLeaderWindow: env.spw,
		Params:               &env.params,
		Journal:              env.journal,
		Transport:            env.trans,
		Hooks:                env.hooks,
		Clock:                env.clock,
	}
	for _, o := range opts {
		o(env, &cfg)
	}
	if cfg.LocalIndex != ObserverIndex {
		cfg.Signer = newTestSigner(env.keys[cfg.LocalIndex])
	}
	eng, err := NewEngine(cfg)
	if err != nil {
		t.Fatal(err)
	}
	env.eng = eng
	return env
}

// completeStores drains the deferred candidate stores in arrival order.
func (env *testEnv) completeStores() {
	pending := env.hooks.storeDeferred
	env.hooks.storeDeferred = nil
	for _, completion := range pending {
		completion.done(env.hooks.storeErr)
	}
}

func (env *testEnv) completeValidation(id CandidateID, verdict error) {
	env.t.Helper()

	done := env.hooks.validationDone[id]
	if done == nil {
		env.t.Fatalf("candidate %s has no pending validation", id)
	}
	delete(env.hooks.validationDone, id)
	done(verdict)
}

type testCandidateRejection struct {
	message string
}

func (e testCandidateRejection) Error() string    { return e.message }
func (testCandidateRejection) CandidateRejected() {}

func (env *testEnv) start() {
	env.t.Helper()
	if err := env.eng.Start(); err != nil {
		env.t.Fatal(err)
	}
}

// signedVoteWire builds a signed vote message from validator i.
func (env *testEnv) signedVoteWire(i int, v Vote) []byte {
	sig := ed25519.Sign(env.keys[i], DataToSign(env.session, VoteBytes(v)))
	sv := SignedVote{ValidatorIndex: uint32(i), Vote: v, Signature: sig}
	return sv.Serialize()
}

func peer(i int) PeerID {
	var p PeerID
	p[0] = byte(i + 1)
	return p
}

// deliverVote feeds a signed vote from validator i into the engine.
func (env *testEnv) deliverVote(i int, v Vote) {
	env.eng.HandleMessage(peer(i), i, env.signedVoteWire(i, v))
}

// buildCert assembles and signs a certificate over the given validators.
func (env *testEnv) buildCert(v Vote, signers ...int) *Certificate {
	cert := &Certificate{Vote: v}
	for _, i := range signers {
		sig := ed25519.Sign(env.keys[i], DataToSign(env.session, VoteBytes(v)))
		cert.Signatures = append(cert.Signatures, VoteSignature{ValidatorIndex: uint32(i), Signature: sig})
	}
	return cert
}

// makeCandidate builds a signed ordinary candidate for a slot.
func (env *testEnv) makeCandidate(slot uint32, parent ParentID, fill byte) *Candidate {
	leader := env.eng.schedule.ExpectedLeader(slot)
	c := &Candidate{
		ID:     CandidateID{Slot: slot},
		Parent: parent,
		Leader: leader,
		Block: ton.BlockIDExt{
			Workchain: 0, Shard: -0x8000000000000000, SeqNo: slot + 1,
			RootHash: make([]byte, 32), FileHash: make([]byte, 32),
		},
	}
	for i := 0; i < 32; i++ {
		c.Block.RootHash[i] = fill
		c.Block.FileHash[i] = fill ^ 0xff
		c.CollatedFileHash[i] = fill ^ 0x0f
	}
	c.ID = c.ComputeID(slot)
	sig, err := SignCandidate(newTestSigner(env.keys[leader]), env.session, c.ID)
	if err != nil {
		env.t.Fatal(err)
	}
	c.Signature = sig
	return c
}

func (env *testEnv) requireNoFatal() {
	env.t.Helper()
	if len(env.hooks.fatals) > 0 {
		env.t.Fatalf("unexpected fatal: %v", env.hooks.fatals[0])
	}
}

func requireEqual[T comparable](t *testing.T, got, want T, msg string) {
	t.Helper()
	if got != want {
		t.Fatalf("%s: got %v, want %v", msg, got, want)
	}
}

func fmtSlotID(id CandidateID) string { return fmt.Sprintf("%d/%x", id.Slot, id.Hash[:4]) }
