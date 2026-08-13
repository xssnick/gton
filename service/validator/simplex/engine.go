package simplex

import (
	"crypto/ed25519"
	"encoding/binary"
	"errors"
	"fmt"
	"runtime"
	"sync"
	"time"

	"github.com/rs/zerolog"
)

// Stats are engine counters; retrieve a snapshot via Engine.Stats.
type Stats struct {
	VotesApplied       uint64
	OwnVotesCast       uint64
	MessagesDropped    uint64
	CertificatesBuilt  uint64
	CertificatesStored uint64
	CandidatesAccepted uint64
	CandidatesRejected uint64
	SlotsFinalized     uint64
	Misbehaviors       uint64
	Bans               uint64
	Standstills        uint64
}

// Engine is the headless Simplex consensus state machine for one validator
// session.
//
// The engine is deliberately single-threaded and I/O-free except for the
// injected Journal (asynchronous durable writes), Transport (fire-and-forget
// sends), candidate services and event sink. It is NOT safe for concurrent
// use: every method must be called from one goroutine. Runner provides that
// loop for production; deterministic tests drive the engine directly.
type Engine struct {
	sessionID   [32]byte
	validators  []Validator
	totalWeight uint64
	threshold   uint64
	localIndex  int
	protocol    uint8
	spw         uint32
	shardPfxLen uint32
	maxCertWire int
	params      Params
	schedule    LeaderSchedule
	journal     Journal
	bootstrap   ValidatedBootstrap
	transport   Transport
	hooks       Hooks
	tracer      Tracer
	signer      Signer
	clock       Clock
	log         zerolog.Logger

	// post re-enters the engine loop from another goroutine; set by NewRunner.
	// When nil (bare engine, deterministic tests) journal completions run
	// inline on the caller's stack.
	post func(func())

	workchain int32
	shard     uint64
	ccSeqno   uint32

	started  bool
	stopped  bool
	fatalErr error

	// pool state
	slots                   *slotMap[poolSlot]
	now                     uint32
	firstNonAnnouncedWindow uint32
	lastFinalized           ParentID
	lastFinalCert           *Certificate
	skipRuns                sortedU32
	// Candidates whose parent chain has not resolved yet.
	parentWaits           []*Candidate
	windowAnnounces       []windowAnnounce
	suppressCertBroadcast bool
	seenBroadcasts        map[uint32][32]byte
	bans                  map[PeerID]time.Time

	// standstill
	standstillAt time.Time
	drain        *drainState

	voter *voterState

	// payloadCache memoizes the consensus.dataToSign bytes of the last vote
	// per kind: within a slot every validator signs the same notarize (and
	// then finalize) payload, so the ~99% repeat rate makes signature checks
	// allocation-free. Indexed by VoteKind; safe because the engine is
	// single-goroutine and the payload is never mutated, only replaced.
	payloadCache [4]struct {
		set     bool
		vote    Vote
		payload []byte
	}

	pending  []func()
	draining bool

	// pendingVerify holds inbound votes that passed the cheap admission gates
	// and still need their signature checked; see FlushVerify.
	pendingVerify []pendingVote

	stats Stats
}

// pendingVote is one admitted inbound vote awaiting its signature check. The
// payload is captured on the engine goroutine because votePayload memoizes
// into engine state, which the parallel verifiers must not touch; every other
// field is immutable for the lifetime of the batch.
type pendingVote struct {
	src     PeerID
	idx     uint32
	vote    Vote
	sig     []byte
	payload []byte
	ok      bool
}

// votePayload returns the memoized dataToSign bytes for a vote.
func (e *Engine) votePayload(v Vote) []byte {
	c := &e.payloadCache[v.Kind%4]
	if !c.set || c.vote != v {
		c.set = true
		c.vote = v
		c.payload = DataToSign(e.sessionID, VoteBytes(v))
	}
	return c.payload
}

// NewEngine validates the configuration and builds an engine. Call Start to
// run recovery and begin the session.
func NewEngine(cfg Config) (*Engine, error) {
	if len(cfg.Validators) == 0 {
		return nil, fmt.Errorf("simplex: empty validator set")
	}
	if cfg.ProtocolVersion < 3 {
		return nil, fmt.Errorf("simplex: protocol version %d is unsupported, require 3 or newer", cfg.ProtocolVersion)
	}
	if cfg.SlotsPerLeaderWindow == 0 {
		return nil, fmt.Errorf("simplex: slots per leader window must be positive")
	}
	if cfg.ShardPrefixLen > 60 {
		return nil, fmt.Errorf("simplex: shard prefix length %d exceeds the TON maximum", cfg.ShardPrefixLen)
	}
	if cfg.LocalIndex != ObserverIndex && (cfg.LocalIndex < 0 || cfg.LocalIndex >= len(cfg.Validators)) {
		return nil, fmt.Errorf("simplex: local index %d out of range", cfg.LocalIndex)
	}
	if cfg.LocalIndex != ObserverIndex && cfg.Signer == nil {
		return nil, fmt.Errorf("simplex: validators require a signer")
	}
	if cfg.Journal == nil || cfg.Transport == nil || cfg.Hooks == nil {
		return nil, fmt.Errorf("simplex: journal, transport and hooks are required")
	}

	total, err := totalValidatorWeight(cfg.Validators)
	if err != nil {
		return nil, err
	}

	params := DefaultParams()
	if cfg.Params != nil {
		params = *cfg.Params
	}
	if err := params.Validate(); err != nil {
		return nil, err
	}
	schedule := cfg.Schedule
	if schedule == nil {
		schedule = RoundRobinSchedule{SlotsPerLeaderWindow: cfg.SlotsPerLeaderWindow, Validators: uint32(len(cfg.Validators))}
	}
	clock := cfg.Clock
	if clock == nil {
		clock = SystemClock{}
	}
	log := zerolog.Nop()
	if cfg.Logger != nil {
		log = *cfg.Logger
	}

	nValidators := len(cfg.Validators)
	e := &Engine{
		sessionID:             cfg.SessionID,
		validators:            cfg.Validators,
		totalWeight:           total,
		threshold:             total*2/3 + 1,
		localIndex:            cfg.LocalIndex,
		protocol:              cfg.ProtocolVersion,
		spw:                   cfg.SlotsPerLeaderWindow,
		shardPfxLen:           cfg.ShardPrefixLen,
		params:                params,
		schedule:              schedule,
		journal:               cfg.Journal,
		bootstrap:             cfg.Bootstrap,
		transport:             cfg.Transport,
		hooks:                 cfg.Hooks,
		tracer:                cfg.Tracer,
		signer:                cfg.Signer,
		clock:                 clock,
		log:                   log,
		workchain:             cfg.Workchain,
		shard:                 cfg.Shard,
		ccSeqno:               cfg.CCSeqno,
		slots:                 newSlotMap(func() *poolSlot { return newPoolSlot(nValidators) }),
		seenBroadcasts:        map[uint32][32]byte{},
		bans:                  map[PeerID]time.Time{},
		suppressCertBroadcast: true,
		// Upper bound of a well-formed certificate wire: constructor ids,
		// a boxed vote and one 64-byte signature entry per validator.
		maxCertWire: 64 + nValidators*80,
	}
	if cfg.LocalIndex != ObserverIndex {
		e.voter = newVoterState(params.FirstBlockTimeout)
	}
	return e, nil
}

// ---- lifecycle ----

// Start loads the journal, replays certificates and own votes, re-casts the
// startup skip votes for the last announced window and begins the session.
func (e *Engine) Start() error {
	if e.started {
		return fmt.Errorf("simplex: engine already started")
	}
	if e.stopped {
		return ErrStopped
	}

	bs, err := e.journal.Bootstrap()
	if err != nil {
		return fmt.Errorf("simplex: journal bootstrap: %w", err)
	}
	// A caller that had to read the journal to build this engine hands the
	// state it read over already verified. Re-reading it here is two point
	// reads against a journal that caches its bootstrap and invalidates the
	// cache on every write, so the same state comes back as the same value —
	// and verifying that state again would pay a quorum of ed25519
	// verifications per saved certificate for nothing. Anything else, including
	// a state that grew since, is verified here as before.
	if e.bootstrap.State() != bs {
		if _, err = ValidateBootstrap(e.sessionID, e.validators, bs); err != nil {
			return err
		}
	}
	e.firstNonAnnouncedWindow = bs.FirstNonAnnouncedWindow

	if e.voter != nil {
		e.bootstrapVoter(bs.OurVotes)
	}

	// Slot 0 always builds on genesis.
	e.slots.at(0).addAvailableBase(Genesis())

	// Certificates first, without re-gossip; downstream notifications
	// (OnNotarized/OnFinalized) are re-emitted and must be consumed
	// idempotently.
	for _, cert := range bs.Certificates {
		slot := e.slots.at(cert.Vote.Slot())
		if slot == nil {
			continue
		}
		if !slot.needs(cert.Vote) {
			return fmt.Errorf("simplex: journal holds duplicate %s certificate for slot %d", cert.Vote.Kind, cert.Vote.Slot())
		}
		e.storeSavedCertificate(slot, cert)
		if e.fatalErr != nil {
			return e.fatalErr
		}
	}
	e.suppressCertBroadcast = false
	e.drainPending()

	// Re-sign and re-apply our own votes in the original order; they are
	// already durable and were possibly already delivered, so no broadcast.
	for _, v := range bs.OurVotes {
		e.castVote(v, true)
	}

	if e.voter != nil {
		e.castStartupWindowSkips()
	}

	e.started = true
	e.standstillAt = e.clock.Now().Add(e.params.StandstillTimeout)
	e.trace(TraceSessionStarted{
		Workchain:            e.workchain,
		Shard:                e.shard,
		CCSeqno:              e.ccSeqno,
		LocalIndex:           e.localIndex,
		TotalValidators:      len(e.validators),
		Weight:               e.localWeight(),
		TotalWeight:          e.totalWeight,
		SlotsPerLeaderWindow: e.spw,
	})
	e.advancePresent()
	e.drainPending()

	if e.localIndex >= 0 {
		e.log.Info().Msgf("validator group started, we are validator %d with weight %d out of %d",
			e.localIndex, e.validators[e.localIndex].Weight, e.totalWeight)
	} else {
		e.log.Info().Msgf("validator group started, we are an observer")
	}
	return e.fatalErr
}

// Stop halts the engine; all further inputs are ignored. Journal writes are
// synchronous, so there is nothing to flush.
func (e *Engine) Stop() {
	e.stopped = true
	e.parentWaits = nil
	e.windowAnnounces = nil
	e.drain = nil
	e.pending = nil
	e.pendingVerify = nil
}

// Err returns the fatal session error, if any.
func (e *Engine) Err() error { return e.fatalErr }

// Stats returns a snapshot of the engine counters.
func (e *Engine) Stats() Stats { return e.stats }

// UpdateParams installs a new noncritical parameter set mid-session. Timers
// already armed keep their deadline and pick up the new pacing on the next
// rearm. An invalid set is rejected and the old one stays in force.
func (e *Engine) UpdateParams(p Params) error {
	if err := p.Validate(); err != nil {
		return err
	}
	e.params = p
	return nil
}

func (e *Engine) localWeight() uint64 {
	if e.localIndex < 0 {
		return 0
	}
	return e.validators[e.localIndex].Weight
}

// trace hands one event to the optional Tracer.
func (e *Engine) trace(ev TraceEvent) {
	if e.tracer != nil {
		e.tracer.Trace(e.clock.Now(), ev)
	}
}

func (e *Engine) failed() bool { return e.fatalErr != nil || e.stopped }

func (e *Engine) fatal(err error) {
	if e.failed() {
		return
	}
	e.fatalErr = err
	e.log.Warn().Msgf("fatal session error: %v", err)
	// Fail closed: drop everything queued, deliver only the fatal signal.
	e.pending = nil
	e.drain = nil
	e.enqueue(func() { e.hooks.OnFatal(err) })
}

// enqueue defers a notification until the current transition completes,
// modelling bus delivery: handlers never observe a state mid-update and never
// re-enter the pool.
func (e *Engine) enqueue(f func()) {
	e.pending = append(e.pending, f)
}

// journalDone wraps a journal completion continuation. The returned callback
// is handed to the Journal and may be invoked from any goroutine when the
// engine runs under Runner; a bare engine runs the continuation inline, which
// keeps deterministic tests fully synchronous (an in-memory journal completing
// inline reproduces the old blocking behavior exactly).
func (e *Engine) journalDone(f func(error)) func(error) {
	return func(err error) {
		if e.post != nil {
			e.post(func() { e.completeAsync(func() { f(err) }) })
			return
		}
		e.completeAsync(func() { f(err) })
	}
}

// candidateDone turns an asynchronous candidate-service completion into an
// engine-loop input. The callback is single-use even if a faulty service calls
// it more than once. With Runner it may be invoked from any goroutine; a bare
// Engine is intentionally single-threaded and expects inline completions.
func (e *Engine) candidateDone(id CandidateID, complete func(CandidateID, error)) func(error) {
	var once sync.Once

	return func(err error) {
		once.Do(func() {
			apply := func() { e.run(func() { complete(id, err) }) }
			if e.post != nil {
				e.post(apply)
				return
			}
			apply()
		})
	}
}

// completeAsync runs a journal continuation on the engine goroutine. It is
// dropped after a stop or fatal error; unlike run it has no started gate,
// because startup skip votes legitimately complete while Start is on the
// stack (or, under Runner, right after it returns).
func (e *Engine) completeAsync(action func()) {
	if e.failed() {
		return
	}
	action()
	e.drainPending()
}

func (e *Engine) drainPending() {
	if e.draining {
		return
	}
	e.draining = true
	for len(e.pending) > 0 {
		f := e.pending[0]
		e.pending = e.pending[1:]
		f()
	}
	e.draining = false
}

// run executes a top-level input action and then delivers all deferred
// notifications.
func (e *Engine) run(action func()) {
	if !e.started || e.failed() {
		return
	}
	action()
	e.drainPending()
}

// ---- voting ----

// castVote casts one own vote. replay drives all three differences between a
// fresh cast and a bootstrap replay: the intent is already durable so it is not
// journalled again, a conflict with the pool is tolerated because the vote was
// already applied once, and it is not re-broadcast.
func (e *Engine) castVote(v Vote, replay bool) {
	if e.failed() || e.voter == nil {
		return
	}
	e.trace(TraceVoted{Vote: v})
	if replay {
		e.finishVoteCast(v, replay)
		return
	}
	e.journal.SaveOurVote(v, e.journalDone(func(err error) {
		if errors.Is(err, ErrAlreadySaved) {
			// The intent is already durable; it is applied by the original
			// cast (possibly still completing) or was applied by the
			// bootstrap replay.
			return
		}
		if err != nil {
			e.fatal(fmt.Errorf("simplex: vote journal write for %s: %w", v, err))
			return
		}
		e.finishVoteCast(v, replay)
	}))
}

// finishVoteCast signs, applies and broadcasts an own vote whose intent is
// durable (or durability-exempt during replay). The strict order
// journal -> sign -> apply -> send is preserved per vote; the engine merely
// keeps serving other inputs while the journal write is in flight.
func (e *Engine) finishVoteCast(v Vote, replay bool) {
	if e.voter == nil {
		return
	}
	sig, err := e.signer.Sign(e.votePayload(v))
	if err != nil {
		e.fatal(fmt.Errorf("simplex: signing %s: %w", v, err))
		return
	}
	sv := SignedVote{ValidatorIndex: uint32(e.localIndex), Vote: v, Signature: sig}
	wire := sv.Serialize()
	e.stats.OwnVotesCast++
	if e.applyVote(uint32(e.localIndex), v, sig, replay) && !replay {
		e.transport.BroadcastToAll(wire)
	}
}

func (e *Engine) reportMisbehavior(idx uint32, m Misbehavior) {
	e.stats.Misbehaviors++
	e.enqueue(func() { e.hooks.OnMisbehavior(idx, m) })
}

// ---- network ingestion ----

// HandleMessage ingests one protocol message (vote or certificate) received
// from the session overlay. srcValidator is the validator index the
// transport authenticated the sender as, or ObserverIndex when the sender is
// not a session validator. src is used only for temporary bad-signature bans.
//
// It verifies inline, so a directly driven engine (deterministic tests) stays
// fully serial: one call in, all of its effects out. Runner instead admits a
// whole batch through handleMessageBatched and flushes once, which is what
// spreads the vote signature checks over the cores.
func (e *Engine) HandleMessage(src PeerID, srcValidator int, data []byte) {
	e.run(func() {
		e.handleMessage(src, srcValidator, data)
		e.flushVerify()
	})
}

// handleMessageBatched admits a message without flushing the deferred vote
// verification; the caller must call FlushVerify once the batch is drained.
func (e *Engine) handleMessageBatched(src PeerID, srcValidator int, data []byte) {
	e.run(func() { e.handleMessage(src, srcValidator, data) })
}

func (e *Engine) handleMessage(src PeerID, srcValidator int, data []byte) {
	if e.isBanned(src) {
		e.log.Debug().Msgf("dropping message from temporarily banned peer")
		e.stats.MessagesDropped++
		return
	}
	if len(data) < 4 {
		e.stats.MessagesDropped++
		return
	}
	firstTooNewSlot := (e.now/e.spw + e.params.MaxLeaderWindowDesync + 1) * e.spw

	switch binary.LittleEndian.Uint32(data) {
	case idSignedVote:
		if srcValidator < 0 || srcValidator >= len(e.validators) {
			e.log.Warn().Msgf("dropping vote from a peer that is not a validator")
			e.ban(src)
			return
		}
		v, sig, err := parseSignedVote(data)
		if err != nil {
			e.stats.MessagesDropped++
			return
		}
		if v.Slot() >= firstTooNewSlot {
			e.log.Warn().Msgf("dropping too new vote from validator %d: slot=%d, current_slot=%d", srcValidator, v.Slot(), e.now)
			e.stats.MessagesDropped++
			return
		}
		if v.Slot() < e.slots.firstNonFinalized {
			return
		}
		slot := e.slots.at(v.Slot())
		if !slot.votes[srcValidator].wants(v) {
			return
		}
		// Everything that can reject the message without cryptography has run;
		// the ed25519 check is deferred so that a whole arrival burst — every
		// validator broadcasts its vote for a slot at the same moment — is
		// checked on all cores at once instead of one at a time on this
		// goroutine. FlushVerify re-runs the gates above before applying.
		e.pendingVerify = append(e.pendingVerify, pendingVote{
			src:     src,
			idx:     uint32(srcValidator),
			vote:    v,
			sig:     sig,
			payload: e.votePayload(v),
		})

	case idCertificate:
		if len(data) > e.maxCertWire {
			e.stats.MessagesDropped++
			return
		}
		cert, err := parseCertificate(data, len(e.validators))
		if err != nil {
			e.stats.MessagesDropped++
			return
		}
		tooNew := cert.Vote.Slot() >= firstTooNewSlot
		if !tooNew {
			// Cheap dedup before the expensive signature checks.
			slot := e.slots.at(cert.Vote.Slot())
			if slot == nil || !slot.needs(cert.Vote) {
				return
			}
		}
		if err = e.verifyCertificate(cert); err != nil {
			e.log.Warn().Msgf("dropping bad certificate: %v", err)
			e.ban(src)
			return
		}
		// Unlike votes, valid certificates from far ahead are accepted: they
		// are proof of session progress and drive catch-up. The slot state is
		// allocated only after the certificate checks out.
		if tooNew {
			slot := e.slots.at(cert.Vote.Slot())
			if slot == nil || !slot.needs(cert.Vote) {
				return
			}
		}
		e.handleCertificate(cert)

	default:
		e.stats.MessagesDropped++
	}
}

// FlushVerify checks the signatures of the votes admitted since the last flush
// and applies them. Runner calls it once per drained batch; Engine.Advance
// calls it too, so nothing can sit unverified across a timer sleep.
func (e *Engine) FlushVerify() {
	if len(e.pendingVerify) == 0 {
		return
	}
	if !e.started || e.failed() {
		e.pendingVerify = e.pendingVerify[:0]
		return
	}
	e.run(func() { e.flushVerify() })
}

func (e *Engine) flushVerify() {
	batch := e.pendingVerify
	if len(batch) == 0 {
		return
	}

	verifyPendingVotes(e.validators, batch)

	// The gates the admission path ran are re-run here against current state:
	// applying an earlier entry of this very batch can ban the sender, finalize
	// the slot out from under a later one (slots.at then returns nil) or make
	// its vote a duplicate. Arrival order is preserved so the applied set is
	// exactly the one serial verification would have produced.
	firstTooNewSlot := (e.now/e.spw + e.params.MaxLeaderWindowDesync + 1) * e.spw
	for i := range batch {
		if e.failed() {
			break
		}
		p := &batch[i]
		if e.isBanned(p.src) {
			continue
		}
		// e.now only moves forward, so this can no longer fire for an entry
		// that passed at admission; it stays as the invariant of the gate.
		if p.vote.Slot() >= firstTooNewSlot || p.vote.Slot() < e.slots.firstNonFinalized {
			continue
		}
		slot := e.slots.at(p.vote.Slot())
		if slot == nil || !slot.votes[p.idx].wants(p.vote) {
			continue
		}
		if !p.ok {
			e.log.Warn().Msgf("dropping bad vote from validator %d: invalid signature", p.idx)
			e.ban(p.src)
			continue
		}
		if e.applyVote(p.idx, p.vote, p.sig, false) {
			e.stats.VotesApplied++
		}
	}
	e.pendingVerify = e.pendingVerify[:0]
}

// verifyPendingVotes records the signature verdict of every buffered vote. The
// check is a pure predicate over immutable state — the validator set is fixed
// at NewEngine and the payload was captured at admission — which is what makes
// it safe to run off the engine goroutine.
func verifyPendingVotes(validators []Validator, batch []pendingVote) {
	// The predicate is a plain function, not a shared closure: a closure
	// referenced from the fan-out body would be heap-allocated on every call,
	// including the single-vote flush of the inline path.
	if len(batch) < parallelVerifyMinSigs || runtime.GOMAXPROCS(0) < 2 {
		for i := range batch {
			batch[i].ok = verifyPendingVote(validators, &batch[i])
		}
		return
	}
	// Unlike a certificate quorum, every vote stands on its own: a bad one
	// must not stop the batch, so the bound is never lowered.
	verifyFanOut(len(batch), func(i int) int {
		batch[i].ok = verifyPendingVote(validators, &batch[i])
		return len(batch)
	})
}

func verifyPendingVote(validators []Validator, p *pendingVote) bool {
	return len(p.sig) == ed25519.SignatureSize &&
		ed25519.Verify(validators[p.idx].PublicKey, p.payload, p.sig)
}

// verifyCertificate checks a certificate against the session validator set:
// the vote shape, index bounds, per-validator uniqueness, the 2/3+1 weighted
// quorum and every signature. The cheap structural checks run before any
// cryptography; large signature sets are verified on all cores.
//
// It is the in-session twin of the free VerifyCertificate — kept separate to
// reuse the memoized dataToSign payload and the precomputed threshold — and
// enforces the same precondition: the vote shape check keeps votePayload from
// panicking on a kind voteToTL cannot encode.
func (e *Engine) verifyCertificate(cert *Certificate) error {
	if cert == nil {
		return fmt.Errorf("simplex: nil certificate")
	}
	if err := validateCertificateVote(cert.Vote); err != nil {
		return err
	}
	payload := e.votePayload(cert.Vote)

	return verifyCertificatePayload(e.validators, e.threshold, payload, cert)
}

// ---- candidates ----

// SubmitCandidate ingests a candidate proposal (own or received from the
// candidate transport). The engine recomputes the candidate id from its hash
// data and verifies the leader assignment and signature; the error reports a
// forged or malformed candidate, while stale/duplicate candidates are
// silently ignored.
func (e *Engine) SubmitCandidate(c *Candidate) error {
	if !e.started || e.failed() {
		return ErrStopped
	}
	if e.voter == nil {
		return nil
	}

	cc := *c
	cc.Signature = append([]byte(nil), c.Signature...)
	cc.Block.RootHash = append([]byte(nil), c.Block.RootHash...)
	cc.Block.FileHash = append([]byte(nil), c.Block.FileHash...)
	if c.Delegation != nil {
		cc.Delegation = &Delegation{
			CollatorKey: append(ed25519.PublicKey(nil), c.Delegation.CollatorKey...),
			Signature:   append([]byte(nil), c.Delegation.Signature...),
		}
	}
	if err := cc.ValidateShape(); err != nil {
		return err
	}
	cc.ID = cc.ComputeID(c.ID.Slot)
	leader := e.schedule.ExpectedLeader(cc.ID.Slot)
	// A custom LeaderSchedule is embedder-supplied while the candidate itself
	// is untrusted input; refuse an out-of-range leader instead of indexing
	// the validator set with it.
	if int(leader) >= len(e.validators) {
		return fmt.Errorf("simplex: leader schedule returned out-of-range validator %d for slot %d", leader, cc.ID.Slot)
	}
	if cc.Leader != leader {
		return fmt.Errorf("simplex: candidate source %d does not match expected leader %d", cc.Leader, leader)
	}
	if cc.Delegation != nil {
		if err := verifyDelegatedCandidate(
			&cc,
			e.validators[leader].PublicKey,
			e.sessionID,
			e.spw,
			e.protocol,
		); err != nil {
			return err
		}
	} else if !VerifyCandidateSignature(e.validators[leader].PublicKey, e.sessionID, cc.ID, cc.Signature) {
		return fmt.Errorf("simplex: candidate signature is not valid")
	}

	e.run(func() { e.acceptCandidate(&cc) })
	return nil
}

// PrecheckCandidateBroadcast lets the candidate transport reject wrong-slot
// or duplicate broadcasts before reassembly.
// signatureChecked commits the broadcast id for the slot.
func (e *Engine) PrecheckCandidateBroadcast(slot uint32, broadcastID [32]byte, signatureChecked bool) error {
	if !e.started || e.failed() {
		return ErrStopped
	}
	if slot < e.slots.firstNonFinalized {
		return fmt.Errorf("simplex: slot is already finalized")
	}
	if slot > e.now+e.params.MaxLeaderWindowDesync*e.spw {
		return fmt.Errorf("simplex: slot is too far in the future")
	}
	if existing, ok := e.seenBroadcasts[slot]; ok && existing != broadcastID {
		return fmt.Errorf("simplex: duplicate broadcast")
	}
	if signatureChecked {
		e.seenBroadcasts[slot] = broadcastID
	}
	return nil
}

// ---- timers ----

// NextWakeup returns the earliest pending deadline, or the zero time when no
// timer is armed. Callers re-arm their timer after every engine interaction.
func (e *Engine) NextWakeup() time.Time {
	if !e.started || e.failed() {
		return time.Time{}
	}
	var at time.Time
	consider := func(t time.Time) {
		if t.IsZero() {
			return
		}
		if at.IsZero() || t.Before(at) {
			at = t
		}
	}
	consider(e.standstillAt)
	if e.voter != nil && e.voter.alarmArmed {
		consider(e.voter.alarmAt)
	}
	if e.drain != nil {
		consider(e.drain.nextAt)
	}
	return at
}

// Advance fires all timers that are due at the current clock reading.
func (e *Engine) Advance() {
	e.run(func() {
		// A timer must never fire on state that is missing votes already
		// admitted; the timeouts are exactly the decisions those votes change.
		e.flushVerify()
		now := e.clock.Now()
		if !e.failed() && e.voter != nil && e.voter.alarmArmed && !e.voter.alarmAt.After(now) {
			e.fireVoterAlarm()
		}
		if !e.failed() && !e.standstillAt.IsZero() && !e.standstillAt.After(now) {
			e.fireStandstill(now)
		}
		if !e.failed() && e.drain != nil && !e.drain.nextAt.After(now) {
			e.stepDrain(now)
		}
	})
}

// ---- bans ----

func (e *Engine) ban(peer PeerID) {
	e.stats.Bans++
	e.bans[peer] = e.clock.Now().Add(e.params.BadSignatureBanDuration)
}

func (e *Engine) isBanned(peer PeerID) bool {
	until, ok := e.bans[peer]
	if !ok {
		return false
	}
	if e.clock.Now().After(until) {
		delete(e.bans, peer)
		return false
	}
	return true
}
