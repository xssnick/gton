package validator

import (
	"bytes"
	"cmp"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"slices"
	"sync"
	"time"

	corestorage "github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/simplex"
)

var (
	// ErrCandidateUnavailable is returned when a one-node overlay is missing
	// data the protocol requires to be local already.
	ErrCandidateUnavailable = errors.New("validator runtime: candidate is unavailable")
	// ErrResolverClosed reports work cancelled by session shutdown.
	ErrResolverClosed = errors.New("validator runtime: resolver closed")
	// ErrCandidateRequestRateLimited reports the per-source sliding window
	// applied to incoming requestCandidate queries.
	ErrCandidateRequestRateLimited = errors.New("validator runtime: candidate request rate limit exceeded")
)

// CandidateRequest is one consensus.simplex.requestCandidate operation. A
// network implementation must enforce MaxReplySize before buffering a reply.
type CandidateRequest struct {
	SessionID         [32]byte
	ID                simplex.CandidateID
	WantCandidate     bool
	WantNotarization  bool
	MaximumReplyBytes uint32
}

// CandidateResponse is the decoded response to CandidateRequest. Empty wire
// and a nil certificate mean that the selected peer did not have that part.
// Ownership of CandidateWire transfers to the resolver.
type CandidateResponse struct {
	CandidateWire []byte
	Notarization  *simplex.Certificate
}

// CandidateProvider is the only network boundary used by candidate
// resolution. The implementation is deliberately outside the validator core
// and must return promptly when ctx is cancelled: a locally arriving missing
// part cancels an in-flight peer request immediately.
type CandidateProvider interface {
	RequestCandidate(context.Context, CandidateRequest) (CandidateResponse, error)
}

// CandidateResolution is complete only when both the candidate and its
// notarization certificate are present and verified. Notarization carries the
// proof of that verification: the resolver only ever stores certificates it
// sealed through its own simplex.CertificateVerifier, or that arrived sealed
// from the engine or the validated journal.
type CandidateResolution struct {
	Candidate    *CandidateArtifact
	Notarization simplex.VerifiedCertificate
}

type candidateResolver struct {
	session   SessionStorageID
	sessionID [32]byte
	storage   ValidatorStorage
	provider  CandidateProvider
	codec     *candidateCodec
	// certificates verifies notarizations peers send us and seals them. It is
	// held for the session so the roster digest and the quorum threshold are
	// derived once rather than per certificate.
	certificates       *simplex.CertificateVerifier
	peerCount          int
	maxReply           uint32
	validateCandidates bool

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu      sync.Mutex
	params  simplex.Params
	entries map[simplex.CandidateID]*candidateEntry
	// retained holds exactly the entries the finalization sweep can act on: the
	// ones carrying a payload worth dropping, whether or not the store holds a
	// copy of it. entries is never pruned — a released candidate keeps its
	// identity there for the life of the session — so sweeping it would make
	// every finalization cost a walk of everything the session has ever seen, on
	// the Simplex engine goroutine.
	retained map[simplex.CandidateID]*candidateEntry
	// cache is the running projection of that retention, maintained where the
	// payloads themselves change. cacheStats recomputes it from scratch and the
	// two must agree.
	cache candidateCacheStats
	// budget is what this session may hold back for the local producer beyond
	// the fixed margin, in bytes and payloads rather than in slots. It is the
	// bound the retention floor is derived against; see retention.go.
	budget retentionBudget
	// retentionScratch is the slot/size list retentionCapFloor sorts. It is
	// reused because that walk runs on the Simplex engine goroutine for every
	// finalization and its length is bounded by the budget itself.
	retentionScratch []retainedPayload
	requests         map[simplex.PeerID]*candidateRequestWindow
	closed           bool
}

// retainedPayload is one retained candidate that still costs memory: its slot
// and the bytes it holds. An empty candidate and a released one are both absent
// from this list, which is the whole point — neither is production this node is
// behind on, and neither is memory it can free.
type retainedPayload struct {
	slot  uint32
	bytes int64
}

type candidateEntry struct {
	// durable is the entry's claim that the candidate store holds a readable
	// copy of this candidate's canonical wire. It is set where that becomes
	// true — the durable index restored at startup, a completed SaveCandidate,
	// a completed load — and cleared where it turns out not to be, which is a
	// read of that copy that fails. It is the only thing that makes releasing a
	// payload recoverable, so every path that asks "can this be read back?"
	// asks this field and nothing else.
	durable   bool
	candidate *CandidateArtifact
	// validationRoots is an at-most-once handoff from network decode to the
	// semantic validator. It is kept outside candidate so every other resolver
	// consumer sees and retains only the canonical bytes.
	validationRoots *candidateValidationRoots
	wire            []byte
	// wireHash is the canonical wire identity, kept after the payload itself is
	// released. It is the same sha256 the store indexes the candidate by, and it
	// is what keeps a conflicting duplicate detectable once the bytes are gone.
	// It says what this candidate is, never where its bytes can be found.
	wireHash     [32]byte
	hasWireHash  bool
	notarization simplex.VerifiedCertificate
	load         *resolverFlight
	resolve      *resolverFlight
	store        *resolverFlight
	serve        *resolverFlight
}

// releasable reports whether the payload of an already finalized candidate can
// be dropped from memory. The resolver lock is held by the caller.
//
// Durability is deliberately not a condition. A payload with a durable copy is
// released because it can be read back; a payload without one is released
// because consensus never accepted it — nothing stores a candidate that was
// only ever fetched from a peer, so requiring durability here would pin those
// bytes for the whole session and hide them from this sweep entirely. What the
// entry keeps in both cases is its identity and its certificate; what differs
// is only where the bytes come from if they are ever needed again, and that
// question is answered by durable rather than by this predicate.
//
// An empty candidate carries no BOCs, so releasing one frees nothing measurable
// and only costs a lineage walk one storage read per step.
//
// A load or a store flight is left to itself: each is bounded by one storage
// operation and each is installing or writing out the very bytes this would
// drop. A resolve flight is not a claim on the payload either. It has its own
// waiter/owner lifetime, and resolveInner holds no pointer to the bytes while
// waiting for a certificate; it re-reads the durable copy when it finally has
// both parts.
func (e *candidateEntry) releasable(slot uint32, watermark uint32) bool {
	return e.candidate != nil && slot < watermark &&
		!e.candidate.Candidate.Empty &&
		e.load == nil && e.store == nil
}

type resolverFlight struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    error
	// waiters are active resolve callers and owners are the subset performing a
	// finalization/recovery operation. Ownership keeps an incidental caller from
	// cancelling shared work, but it never exempts that work from expires: a node
	// which sees final certificates without their payloads must not accumulate one
	// session-lifetime retry loop per finalized slot.
	waiters  int
	owners   int
	finished bool
	expires  time.Time
	timer    *time.Timer
	// result is what this flight produced, captured under the same lock that
	// completed it. Waiters must not re-read the entry afterwards: the
	// finalization sweep can release its payload in between. A load flight fills
	// only Candidate.
	result CandidateResolution
	// wire is what a serve flight produced: the durable copy of one released
	// candidate, held only until the requests sharing this flight have taken
	// their own copy of it. It is never stored back on the entry.
	wire []byte
	// notify holds the callbacks of the store flight's non-joining submitters,
	// invoked once each from the storage callback after the flight completes.
	// A caller that waits on done takes its outcome from err instead.
	notify []func(error)
	// request cancels the peer request a resolve flight currently has
	// outstanding. It lets a part that arrives locally end that request even
	// when the flight cannot be completed from memory, which is the case for a
	// candidate whose payload the finalization sweep has released.
	request context.CancelFunc
}

type candidateRequestWindow struct {
	times []time.Time
	head  int
}

type candidateResolverOptions struct {
	Session            SessionStorageID
	SessionID          [32]byte
	Storage            ValidatorStorage
	Provider           CandidateProvider
	Codec              *candidateCodec
	Validators         []simplex.Validator
	PeerCount          int
	ValidateCandidates bool
	Limits             CandidateLimits
	Params             simplex.Params
	Stored             StoredSessionState
	Bootstrap          simplex.ValidatedBootstrap
}

func newCandidateResolver(options candidateResolverOptions) (*candidateResolver, error) {
	maxReply := uint64(options.Limits.MaxBlockBytes) +
		uint64(options.Limits.MaxCollatedDataBytes) +
		1<<20
	if maxReply > math.MaxUint32 {
		return nil, errors.New("validator runtime: maximum candidate reply size overflows uint32")
	}

	certificates, err := simplex.NewCertificateVerifier(options.SessionID, options.Validators)
	if err != nil {
		return nil, fmt.Errorf("validator runtime: %w", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &candidateResolver{
		session:            options.Session,
		sessionID:          options.SessionID,
		storage:            options.Storage,
		provider:           options.Provider,
		codec:              options.Codec,
		certificates:       certificates,
		peerCount:          options.PeerCount,
		maxReply:           uint32(maxReply),
		validateCandidates: options.ValidateCandidates,
		ctx:                ctx,
		cancel:             cancel,
		params:             options.Params,
		budget:             defaultRetentionBudget(options.Session.Shard.IsMasterchain()),
		entries:            make(map[simplex.CandidateID]*candidateEntry, len(options.Stored.CandidateIDs)),
		retained:           make(map[simplex.CandidateID]*candidateEntry),
	}
	// The durable index is the whole truth a restarted session starts with: the
	// candidates below are in the store, and nothing about this process not
	// having written them itself makes them any less so.
	for _, id := range options.Stored.CandidateIDs {
		r.markDurableLocked(r.entry(id))
	}
	if options.Bootstrap.State() == nil {
		cancel()

		return nil, errors.New("validator runtime: bootstrap state was not validated")
	}
	// These signatures were verified when the bootstrap state was validated.
	// The binding check is what makes that statement about *this* session and
	// this roster rather than about whichever pair the journal was validated
	// against; the certificates travel on to block acceptance, which drops its
	// own Ed25519 pass because of them.
	for _, certificate := range options.Bootstrap.Certificates() {
		if certificate.Vote().Kind != simplex.VoteNotarize {
			continue
		}
		if err = certificates.Binding().Check(certificate); err != nil {
			cancel()

			return nil, fmt.Errorf("validator runtime: journaled notarization: %w", err)
		}
		r.entry(certificate.Vote().ID).notarization = certificate
	}

	return r, nil
}

func (r *candidateResolver) entry(id simplex.CandidateID) *candidateEntry {
	entry := r.entries[id]
	if entry == nil {
		entry = &candidateEntry{}
		r.entries[id] = entry
	}

	return entry
}

func (r *candidateResolver) updateParams(params simplex.Params) {
	r.mu.Lock()
	if r.params.CandidateResolveRateLimit != params.CandidateResolveRateLimit {
		// Every per-source rate window is cleared when this noncritical parameter
		// changes; keeping old samples would apply two limits to one window.
		r.requests = nil
	}
	r.params = params
	r.mu.Unlock()
}

func (r *candidateResolver) observeNotarization(id simplex.CandidateID, certificate simplex.VerifiedCertificate) {
	r.mu.Lock()
	entry := r.entry(id)
	entry.notarization = certificate
	r.completeResolveLocked(id, entry)
	r.mu.Unlock()
}

func (r *candidateResolver) stage(artifact *CandidateArtifact, wire []byte) error {
	id := artifact.Candidate.ID
	// Hashing a whole candidate wire takes milliseconds; it never runs under the
	// resolver lock.
	hash := sha256.Sum256(wire)

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrResolverClosed
	}

	entry := r.entry(id)
	if entry.hasWireHash && hash != entry.wireHash {
		return ErrCandidateConflict
	}
	if entry.candidate != nil {
		r.completeResolveLocked(id, entry)

		return nil
	}
	if entry.durable {
		// The store already holds these bytes — this candidate was released after
		// it became durable, or the session restarted onto a candidate it had
		// written before the crash — so a rebroadcast must not pin them again.
		// Everything that still needs them reaches storage before the network.
		return nil
	}
	r.attachPayloadLocked(id, entry, artifact, wire, hash)
	r.completeResolveLocked(id, entry)

	return nil
}

// attachPayloadLocked installs the bytes of one candidate. Every payload the
// resolver retains passes through here and through releasePayloadLocked, which
// is what keeps the projection exact without a walk and what keeps the release
// queue complete.
func (r *candidateResolver) attachPayloadLocked(
	id simplex.CandidateID,
	entry *candidateEntry,
	artifact *CandidateArtifact,
	wire []byte,
	hash [32]byte,
) {
	// Both capsules retain parsed DAGs. Resolver state needs only the canonical
	// bytes, so every retention route strips them in one place. A voting runtime
	// keeps the validation capsule separately until the first validation claims
	// it; an observer discards it immediately.
	entry.candidate = artifact
	if artifact.preparedBlock != nil || artifact.validationRoots != nil {
		retained := *artifact
		retained.preparedBlock = nil
		retained.validationRoots = nil
		entry.candidate = &retained
	}
	if r.validateCandidates {
		entry.validationRoots = artifact.validationRoots
	}
	entry.wire = wire
	entry.wireHash = hash
	entry.hasWireHash = true
	r.cache.Candidates++
	r.cache.Bytes += candidatePayloadBytes(artifact, wire)
	r.queueReleaseLocked(id, entry)
}

// markDurableLocked records that the store holds this candidate. Every place
// that learns it goes through here, so the running projection stays exact.
func (r *candidateResolver) markDurableLocked(entry *candidateEntry) {
	if entry.durable {
		return
	}
	entry.durable = true
	r.cache.Stored++
}

// clearDurableLocked withdraws that claim after a read of the durable copy
// failed. The entry keeps its identity and its certificate; what it loses is
// the assertion that the bytes can be read back, which is what lets every
// consumer go to a peer instead and lets a later store write the copy again.
func (r *candidateResolver) clearDurableLocked(entry *candidateEntry) {
	if !entry.durable {
		return
	}
	entry.durable = false
	r.cache.Stored--
}

func (r *candidateResolver) queueReleaseLocked(id simplex.CandidateID, entry *candidateEntry) {
	if entry.candidate == nil || entry.candidate.Candidate.Empty {
		return
	}
	r.retained[id] = entry
}

func (r *candidateResolver) releasePayloadLocked(id simplex.CandidateID, entry *candidateEntry) {
	if entry.candidate != nil {
		r.cache.Candidates--
		r.cache.Bytes -= candidatePayloadBytes(entry.candidate, entry.wire)
		entry.candidate = nil
		entry.validationRoots = nil
		entry.wire = nil
	}
	delete(r.retained, id)
}

func candidatePayloadBytes(artifact *CandidateArtifact, wire []byte) int64 {
	return int64(len(wire)) + int64(len(artifact.BlockBOC)) + int64(len(artifact.CollatedData))
}

// completeResolveLocked hands a waiting resolve flight the parts that have just
// become locally available.
//
// A flight whose payload the finalization sweep released cannot be completed
// from memory, but nothing about it is lost: the bytes are durable, and its own
// loop finishes from storage. Cancelling the peer request it has outstanding is
// what makes it do so now instead of after that request times out.
func (r *candidateResolver) completeResolveLocked(id simplex.CandidateID, entry *candidateEntry) {
	flight := entry.resolve
	if flight == nil || entry.notarization.IsZero() {
		return
	}
	if entry.candidate == nil {
		if entry.durable && flight.request != nil {
			flight.request()
			flight.request = nil
		}

		return
	}

	r.finishResolveLocked(
		id,
		entry,
		flight,
		CandidateResolution{Candidate: entry.candidate, Notarization: entry.notarization},
		nil,
	)
}

func (r *candidateResolver) finishResolveLocked(
	id simplex.CandidateID,
	entry *candidateEntry,
	flight *resolverFlight,
	result CandidateResolution,
	err error,
) {
	if flight.finished {
		return
	}

	flight.finished = true
	flight.err = err
	flight.result = result
	if flight.timer != nil {
		flight.timer.Stop()
		flight.timer = nil
	}
	if flight.request != nil {
		flight.request()
		flight.request = nil
	}
	if entry.resolve == flight {
		entry.resolve = nil
	}
	close(flight.done)
	flight.cancel()
}

func (r *candidateResolver) armResolveExpiryLocked(id simplex.CandidateID, flight *resolverFlight) {
	if flight.expires.IsZero() {
		flight.expires = time.Now().Add(resolverFlightTTL(r.params, r.session.Protocol.SlotsPerLeaderWindow))
	}
	delay := time.Until(flight.expires)
	if delay <= 0 {
		delay = time.Nanosecond
	}
	flight.timer = time.AfterFunc(delay, func() {
		r.mu.Lock()
		defer r.mu.Unlock()

		flight.timer = nil
		entry := r.entries[id]
		if entry == nil || entry.resolve != flight || flight.finished {
			return
		}

		r.finishResolveLocked(id, entry, flight, CandidateResolution{}, context.DeadlineExceeded)
	})
}

func (r *candidateResolver) releaseResolveWaiter(
	id simplex.CandidateID,
	flight *resolverFlight,
	owned bool,
) {
	r.mu.Lock()
	defer r.mu.Unlock()

	if flight.waiters > 0 {
		flight.waiters--
	}
	if owned && flight.owners > 0 {
		flight.owners--
	}
	entry := r.entries[id]
	if entry == nil || entry.resolve != flight || flight.finished {
		return
	}
	if flight.waiters == 0 {
		r.finishResolveLocked(id, entry, flight, CandidateResolution{}, context.Canceled)

		return
	}
}

func resolverFlightTTL(params simplex.Params, slotsPerLeaderWindow uint32) time.Duration {
	windows := max(uint64(params.MaxLeaderWindowDesync), 1)
	slots := windows * uint64(max(slotsPerLeaderWindow, 1))
	if windows != 0 && slots/windows != uint64(max(slotsPerLeaderWindow, 1)) {
		return time.Duration(math.MaxInt64)
	}
	if params.TargetRate <= 0 || slots > uint64(math.MaxInt64)/uint64(params.TargetRate) {
		return time.Duration(math.MaxInt64)
	}

	ttl := time.Duration(slots) * params.TargetRate
	if ttl < params.CandidateResolveTimeoutCap {
		ttl = params.CandidateResolveTimeoutCap
	}
	if ttl <= 0 {
		return time.Second
	}

	return ttl
}

func (w *candidateRequestWindow) allow(now time.Time, limit uint32) bool {
	for w.head < len(w.times) && now.Sub(w.times[w.head]) > time.Second {
		w.head++
	}
	if uint64(len(w.times)-w.head) >= uint64(limit) {
		return false
	}

	if w.head == len(w.times) {
		w.times = w.times[:0]
		w.head = 0
	} else if w.head > 64 && w.head*2 >= len(w.times) {
		copy(w.times, w.times[w.head:])
		w.times = w.times[:len(w.times)-w.head]
		w.head = 0
	}
	w.times = append(w.times, now)

	return true
}

func (r *candidateResolver) allowRequestLocked(source simplex.PeerID, now time.Time) bool {
	limit := r.params.CandidateResolveRateLimit
	if limit == 0 {
		return false
	}
	if r.requests == nil {
		r.requests = make(map[simplex.PeerID]*candidateRequestWindow)
	}
	window := r.requests[source]
	if window == nil {
		window = &candidateRequestWindow{}
		r.requests[source] = window
	}

	return window.allow(now, limit)
}

// candidateCacheStats is a projection of the session-scoped candidate cache.
// Bytes is the payload actually retained in memory: for a staged candidate that
// is the canonical wire plus the decoded block and collated data, all three of
// which survive the durable write to storage. An entry whose payload has been
// released counts in Entries but not in Candidates or Bytes, which is the
// intended reading.
//
// Stored counts the entries the candidate store holds a copy of, which is not
// the same set: a restart knows its durable candidates before it has any of
// their payloads, and a candidate fetched from a peer that consensus never
// accepted has a payload no store was ever asked to keep.
type candidateCacheStats struct {
	Entries    int
	Candidates int
	Stored     int
	Bytes      int64
}

func (s *candidateCacheStats) add(entry *candidateEntry) {
	if entry.durable {
		s.Stored++
	}
	if entry.candidate == nil {
		return
	}
	s.Candidates++
	s.Bytes += candidatePayloadBytes(entry.candidate, entry.wire)
}

// cacheStats recomputes the projection by walking every entry. It is the audit
// form of the running one: the sweep publishes r.cache, and these two agreeing
// is the invariant that keeps the incremental accounting honest.
func (r *candidateResolver) cacheStats() candidateCacheStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	stats := candidateCacheStats{Entries: len(r.entries)}
	for _, entry := range r.entries {
		stats.add(entry)
	}

	return stats
}

// cacheProjection is the running projection as of right now, from the running
// counters rather than from a walk. The finalization sweeps publish the same
// numbers, but only when a finalization happens: a metric that refreshes on
// finalization is blind for exactly as long as a session is not finalizing,
// which is the condition it exists to describe. Candidates keep arriving and
// keep being retained through a standstill, so this is where the memory a
// stalled session is accumulating actually shows.
func (r *candidateResolver) cacheProjection() candidateCacheStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.statsLocked()
}

func (r *candidateResolver) statsLocked() candidateCacheStats {
	stats := r.cache
	stats.Entries = len(r.entries)

	return stats
}

// retainedSlots is the margin of finalized candidates whose payload stays in
// memory. It is deliberately wider than the resolved-parent margin: that one
// only has to cover a leader window which opened just before the finalization
// it now trails, while a candidate payload has a second consumer. A lineage
// walk goes back to the masterchain-visible finalized block, which lags this
// session's own finalized slot by the shard-top inclusion delay rather than by
// a leader window — a wall-clock delay, which is why the margin is derived from
// one (see candidateRetentionMarginDuration) instead of being a slot count
// carried over from whatever rate it was first written for.
func (r *candidateResolver) retainedSlots() uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return candidateRetainedSlots(r.params.TargetRate)
}

// payloadResidency reports where resolving id will have to get its payload:
// out of memory, out of the candidate store, or off the network. It is what
// turns a lineage walk from a duration into an explanation — a walk that is
// suddenly reading storage is the retention floor having given up, and that
// distinction was previously only inferable from the log.
func (r *candidateResolver) payloadResidency(id simplex.CandidateID) LineageStepSource {
	r.mu.Lock()
	defer r.mu.Unlock()

	entry := r.entries[id]
	switch {
	case entry == nil:
		return LineageStepPeer
	case entry.candidate != nil:
		return LineageStepMemory
	case entry.durable:
		return LineageStepStorage
	default:
		return LineageStepPeer
	}
}

// retentionBudgetBytes is the payload budget this session's floor is derived
// against. It is exported as a gauge so a dashboard can draw the threshold
// under the retained bytes.
func (r *candidateResolver) retentionBudgetBytes() int64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	return r.budget.Bytes
}

// retentionCapFloor reports the oldest slot the retention budget still allows
// the finalization sweeps to hold, walking back from the finalized tip. It is
// the bound on what the local producer may ask them to keep, and it is
// deliberately not a slot distance.
//
// The quantity that must be bounded is memory, and slot distance is not
// proportional to it: a skipped slot advances the slot number and produces no
// candidate, so a run of skips — a stalled session, which is exactly when the
// producer needs its lineage most — used to consume the whole allowance without
// a single byte being retained. What this walks instead is the retained
// payloads themselves. Empty candidates and already-released ones carry no
// bytes and appear nowhere in it.
//
// It runs on the Simplex engine goroutine for every finalization, and its cost
// is the number of payload-carrying retained entries, which the budget bounds:
// once the budget is exhausted the floor rises and the sweep releases the rest.
//
// Crossing the budget is a degradation point and not an allocation limit.
// Nothing here refuses a candidate, allocates a smaller one or fails a
// validation: the floor rises, notifyFinalized releases the payloads below it,
// and the next consumer that needs one reads it back through loadCandidate —
// out of the candidate store if consensus accepted it, off the network if it
// did not. The visible cost is a lineage walk paying storage reads, which the
// walk's own step-source metric reports.
//
// What the budget bounds is therefore narrower than "this session's payload
// memory", and the honest ceiling has five terms, only the first of which
// retentionFloorCapBytes names:
//
//   - budget.Bytes of finalized payloads held below the fixed margin, which is
//     what this walk decides;
//   - the fixed margin itself — candidateRetainedSlots of finalized payloads
//     that notifyFinalized keeps unconditionally. The margin is configured as a
//     DURATION, candidateRetentionMarginDuration = 8 s, and then converted to a
//     slot count that is rounded up and floored at candidateCacheRetainedSlots;
//     the wall-clock span it actually covers is that slot count times the
//     session's TargetRate and is therefore never below 8 s and usually above
//     it. At the protocol default rate of 2400 ms that is 4 SLOTS = 9.6 s of
//     wall clock, about 3.4 MiB at mainnet's ~880 KiB candidates. On the 200 ms
//     test network the same 8 s gives 40 SLOTS = 8.0 s exactly, 13.1 MiB at that
//     run's 336 KiB p90. Slot counts and wall-clock spans are different units and
//     are labelled here: writing "8 s of them: 4 slots" conflated the configured
//     duration with the span the rounded slot count really covers;
//   - payloads the sweep skipped because a flight was in progress over them.
//     releasable() requires load == nil && store == nil, so a payload being read
//     back or written out survives its watermark and is reconsidered at the next
//     finalization. It is bounded by concurrency and not by bytes: at most one
//     load and one store flight per entry, each holding one payload for the
//     duration of one storage operation;
//   - every candidate at or above the finalized tip. The loop below skips
//     id.Slot > tip and the sweep's watermark never reaches them, deliberately:
//     consensus may still need any of them, so their number is what the session
//     admits for undecided slots and not something a memory budget may decide;
//   - artifacts pinned by a holder outside this cache. release() drops the
//     entry's reference, not the object: a resolution handed to a validation, a
//     builtCandidate capsule on the local produce path, or a wire copy inside a
//     serve flight keeps its payload alive for as long as that holder does.
//     Those lifetimes are bounded by the validation deadline and by session
//     retirement, which is the honest answer, and not by anything in this file.
//
// Nothing here is a reason to widen budget.Bytes: the point of enumerating them
// is that the number this file publishes is a component of the ceiling and not
// the ceiling, so a session's measured RSS exceeding it is expected rather than
// evidence of a leak.
func (r *candidateResolver) retentionCapFloor(tip uint32) uint32 {
	r.mu.Lock()
	defer r.mu.Unlock()

	backstop := r.budget.capSlots(r.params.TargetRate)
	floor := uint32(0)
	if tip > backstop {
		floor = tip - backstop
	}

	payloads := r.retentionScratch[:0]
	for id, entry := range r.retained {
		if id.Slot > tip || id.Slot < floor || entry.candidate == nil {
			continue
		}
		bytes := candidatePayloadBytes(entry.candidate, entry.wire)
		if bytes <= 0 {
			continue
		}
		payloads = append(payloads, retainedPayload{slot: id.Slot, bytes: bytes})
	}
	r.retentionScratch = payloads
	slices.SortFunc(payloads, func(a, b retainedPayload) int {
		return cmp.Compare(b.slot, a.slot)
	})

	var (
		bytes int64
		count int
	)
	for _, payload := range payloads {
		if bytes+payload.bytes > r.budget.Bytes || count+1 > r.budget.Payloads {
			// This one no longer fits, so it and everything below it are what the
			// sweeps are free to release; the floor is the slot above it. Never
			// above the tip itself: a single payload larger than the whole budget
			// still leaves the fixed margin in charge rather than inventing a
			// floor consensus has not reached.
			return min(payload.slot+1, tip)
		}
		bytes += payload.bytes
		count++
	}

	return floor
}

// notifyFinalized releases the payload of candidates consensus can no longer
// need and returns what the cache retains afterwards.
//
// floor is the oldest slot the local producer can still be asked to walk back
// to; retentionFloorNone means no consumer constrains this sweep. The fixed
// margin above is a distance below the finalized slot, and the lineage walk
// that consumes these payloads is not: it stops at the applied anchor, which
// falls arbitrarily far behind finalization when the node's apply pipeline
// lags. Releasing on the margin alone therefore drops exactly the payloads the
// next leader window is about to walk, and reloads them from storage once per
// window forever. The sweep releases below whichever of the two is lower.
//
// This runs on the Simplex engine goroutine for every finalization, so it costs
// what it retains and not what it remembers: only entries still holding a
// droppable payload are visited, which in a healthy session is the retention
// margin itself. An entry the sweep cannot release yet stays queued and is
// reconsidered at the next finalization.
//
// A released entry stays in the map. Its durability claim, notarization
// certificate and wire identity are the whole point of keeping it, and dropping
// it would make a pruned candidate indistinguishable from an unknown one. A
// released entry that is durable is read back through loadCandidate before any
// consumer asks the network; one that is not durable was never accepted by
// consensus, and the network is exactly where an unexpected need for it should
// go.
func (r *candidateResolver) notifyFinalized(slot uint32, floor uint32) candidateCacheStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	if margin := candidateRetainedSlots(r.params.TargetRate); !r.closed && slot >= margin {
		watermark := min(slot-margin, floor)
		for id, entry := range r.retained {
			if entry.releasable(id.Slot, watermark) {
				r.releasePayloadLocked(id, entry)
			}
		}
	}

	return r.statsLocked()
}

func (r *candidateResolver) candidate(
	ctx context.Context,
	id simplex.CandidateID,
) (*CandidateArtifact, error) {
	artifact, err := r.loadCandidate(ctx, id)
	if err != nil {
		return nil, err
	}
	if artifact == nil {
		return nil, ErrCandidateUnavailable
	}

	r.mu.Lock()
	entry := r.entries[id]
	roots := entry.validationRoots
	entry.validationRoots = nil
	r.mu.Unlock()
	if roots != nil {
		prepared := *artifact
		prepared.validationRoots = roots

		return &prepared, nil
	}

	return artifact, nil
}

func (r *candidateResolver) response(
	ctx context.Context,
	source simplex.PeerID,
	request CandidateRequest,
) (CandidateResponse, error) {
	if request.SessionID != r.sessionID {
		return CandidateResponse{}, errors.New("validator runtime: candidate request session mismatch")
	}
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return CandidateResponse{}, ErrResolverClosed
	}
	if !r.allowRequestLocked(source, time.Now()) {
		r.mu.Unlock()

		return CandidateResponse{}, ErrCandidateRequestRateLimited
	}
	entry := r.entries[request.ID]
	r.mu.Unlock()
	if entry == nil {
		// A miss is answered without inserting anything: arbitrary remote IDs
		// must neither grow resolver memory nor trigger a storage lookup.
		return CandidateResponse{}, nil
	}

	var response CandidateResponse
	if request.WantCandidate {
		r.mu.Lock()
		// A retained wire is immutable once installed — release clears the field,
		// never the bytes — so the copy below is taken outside this lock. It is a
		// real external ownership boundary, the transport may retain or mutate its
		// response buffer, but a whole overlay asking at once must not queue
		// multi-megabyte copies behind the lock the consensus goroutine takes.
		wire := entry.wire
		released := wire == nil && entry.durable
		r.mu.Unlock()
		if released {
			loaded, err := r.storedWire(ctx, request.ID)
			if err != nil {
				return CandidateResponse{}, err
			}
			// Already an owned copy, read for this reply and retained by nothing.
			wire = loaded
		} else {
			wire = bytes.Clone(wire)
		}
		response.CandidateWire = wire
	}
	if request.WantNotarization {
		r.mu.Lock()
		// Peers get the wire certificate; the seal is a local proof and does
		// not travel.
		response.Notarization = entry.notarization.Certificate()
		r.mu.Unlock()
	}

	return response, nil
}

// storedWire answers peers from the durable copy without pinning it. A
// candidate whose payload was released stays released: peers sweeping old slots
// must not rebuild the cache the finalization sweep just dropped.
//
// The read is shared. This is the one resolver path an entire overlay can enter
// at once — the rate limit is per source, so it bounds a peer, not the fanout —
// and every one of those requests would otherwise be its own storage read of
// the same multi-megabyte candidate. Sharing it retains nothing: the flight
// leaves the entry when it completes, and its bytes live only as long as the
// requests that were waiting on it.
func (r *candidateResolver) storedWire(
	ctx context.Context,
	id simplex.CandidateID,
) ([]byte, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return nil, ErrResolverClosed
	}
	entry := r.entries[id]
	if entry == nil {
		r.mu.Unlock()

		return nil, nil
	}
	flight := entry.serve
	if flight == nil {
		flight = &resolverFlight{done: make(chan struct{})}
		entry.serve = flight
		r.wg.Add(1)
		go r.serveLoop(id, flight)
	}
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-flight.done:
	}
	if flight.err != nil {
		return nil, flight.err
	}
	if flight.wire == nil {
		return nil, nil
	}

	// Each request owns its reply: the transport may retain or mutate the buffer
	// it is handed. What sharing removed is the storage read behind it, not this
	// copy.
	return bytes.Clone(flight.wire), nil
}

func (r *candidateResolver) serveLoop(id simplex.CandidateID, flight *resolverFlight) {
	defer r.wg.Done()

	wire, err := r.loadStoredWire(r.ctx, id)
	r.forgetUnreadableDurableCopy(id, err)

	r.mu.Lock()
	flight.wire = wire
	flight.err = err
	// The payload is not stored back on the entry: serving a released candidate
	// must not rebuild the cache the finalization sweep dropped.
	if entry := r.entries[id]; entry != nil && entry.serve == flight {
		entry.serve = nil
	}
	close(flight.done)
	r.mu.Unlock()
}

func (r *candidateResolver) loadStoredWire(
	ctx context.Context,
	id simplex.CandidateID,
) ([]byte, error) {
	record, err := r.storage.Candidate(ctx, r.session, id)
	if err != nil {
		if errors.Is(err, corestorage.ErrNotFound) {
			return nil, errors.New("validator runtime: candidate index points to missing data")
		}

		return nil, fmt.Errorf("validator runtime: load candidate: %w", err)
	}
	if record.ID != id {
		return nil, errors.New("validator runtime: candidate storage returned another id")
	}

	r.mu.Lock()
	entry := r.entries[id]
	if entry == nil {
		r.mu.Unlock()

		return nil, nil
	}
	known := entry.hasWireHash
	// The store is content-addressed, so this compares the identity it indexes
	// this candidate under against the one this session saw on the wire. That is
	// the part the store cannot check for us; that its content really hashes to
	// its own key is the store's own business, and re-deriving it here would only
	// hash megabytes to repeat it.
	conflict := known && record.ContentHash != entry.wireHash
	r.mu.Unlock()
	if conflict {
		return nil, ErrCandidateConflict
	}
	if known {
		return record.Wire, nil
	}

	// A restart stub knows the id but has never seen its bytes. Verifying them
	// once is exactly what the retaining load path did, id binding and leader
	// signature included; only the decoded artifact is discarded.
	if _, err = r.codec.decode(record.Wire, &id); err != nil {
		return nil, fmt.Errorf("validator runtime: decode stored candidate: %w", err)
	}

	r.mu.Lock()
	if entry = r.entries[id]; entry != nil && !entry.hasWireHash {
		entry.wireHash = record.ContentHash
		entry.hasWireHash = true
	}
	r.mu.Unlock()

	return record.Wire, nil
}

func (r *candidateResolver) resolve(ctx context.Context, id simplex.CandidateID) (CandidateResolution, error) {
	return r.resolveWithOwnership(ctx, id, false)
}

// resolveFinalization keeps a flight alive while durable consensus recovery or
// finalization owns it. Incidental validation callers may leave without
// cancelling that work; the resolver epoch deadline still bounds the owner so
// missing payloads cannot retain a peer-retry loop for the whole session.
func (r *candidateResolver) resolveFinalization(
	ctx context.Context,
	id simplex.CandidateID,
) (CandidateResolution, error) {
	return r.resolveWithOwnership(ctx, id, true)
}

func (r *candidateResolver) resolveWithOwnership(
	ctx context.Context,
	id simplex.CandidateID,
	owned bool,
) (CandidateResolution, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return CandidateResolution{}, ErrResolverClosed
	}
	entry := r.entry(id)
	if entry.candidate != nil && !entry.notarization.IsZero() {
		result := CandidateResolution{Candidate: entry.candidate, Notarization: entry.notarization}
		r.mu.Unlock()

		return result, nil
	}
	flight := entry.resolve
	if flight != nil {
		select {
		case <-flight.done:
			result, err := flight.result, flight.err
			r.mu.Unlock()
			if err != nil {
				return CandidateResolution{}, err
			}
			if result.Candidate == nil || result.Notarization.IsZero() {
				return CandidateResolution{}, ErrCandidateUnavailable
			}

			return result, nil
		default:
		}
	}
	if flight == nil {
		flightCtx, cancel := context.WithCancel(r.ctx)
		flight = &resolverFlight{done: make(chan struct{}), cancel: cancel}
		entry.resolve = flight
		r.armResolveExpiryLocked(id, flight)
		r.wg.Add(1)
		go r.resolveLoop(flightCtx, id, flight)
	}
	flight.waiters++
	if owned {
		flight.owners++
	}
	r.mu.Unlock()
	defer r.releaseResolveWaiter(id, flight, owned)

	select {
	case <-ctx.Done():
		return CandidateResolution{}, ctx.Err()
	case <-flight.done:
	}

	if flight.err != nil {
		return CandidateResolution{}, flight.err
	}
	if flight.result.Candidate == nil || flight.result.Notarization.IsZero() {
		return CandidateResolution{}, ErrCandidateUnavailable
	}

	return flight.result, nil
}

func (r *candidateResolver) resolveLoop(
	ctx context.Context,
	id simplex.CandidateID,
	flight *resolverFlight,
) {
	defer r.wg.Done()

	result, err := r.resolveInner(ctx, id, flight)
	r.mu.Lock()
	entry := r.entry(id)
	if entry.resolve == flight {
		r.finishResolveLocked(id, entry, flight, result, err)
	}
	r.mu.Unlock()
}

// localResolution completes a resolve from what is available without the
// network: the notarization certificate the entry holds, and the payload, which
// comes back from storage when the finalization sweep released it while this
// flight was waiting. A zero resolution and a nil error mean "not both parts
// yet", which is the loop's cue to keep asking peers.
func (r *candidateResolver) localResolution(
	ctx context.Context,
	id simplex.CandidateID,
) (CandidateResolution, error) {
	r.mu.Lock()
	entry := r.entry(id)
	notarization := entry.notarization
	available := entry.candidate != nil || entry.durable
	r.mu.Unlock()
	if notarization.IsZero() || !available {
		return CandidateResolution{}, nil
	}

	artifact, err := r.loadCandidate(ctx, id)
	if err != nil {
		return CandidateResolution{}, unrecoverableResolveError(ctx, err)
	}
	if artifact == nil {
		return CandidateResolution{}, nil
	}

	return CandidateResolution{Candidate: artifact, Notarization: notarization}, nil
}

// unrecoverableResolveError keeps the read failures a resolve must give up on
// and discards the ones it can still work around. A durable copy that would not
// read has already lost its claim by the time this is reached, so the caller's
// loop asks peers for the bytes; a conflict, a closed resolver or a cancelled
// caller end it instead.
func unrecoverableResolveError(ctx context.Context, err error) error {
	if err == nil {
		return nil
	}
	if ctx.Err() != nil || errors.Is(err, ErrCandidateConflict) || errors.Is(err, ErrResolverClosed) {
		return err
	}

	return nil
}

func (r *candidateResolver) resolveInner(
	ctx context.Context,
	id simplex.CandidateID,
	flight *resolverFlight,
) (CandidateResolution, error) {
	// The payload is read here, before any waiting: this flight is going to need
	// it, and the read belongs ahead of the wait rather than inside it. A read
	// that fails has already withdrawn this entry's durability claim, so the loop
	// below asks a peer for the bytes instead of failing the resolve on a file
	// that cannot be read.
	_, loadErr := r.loadCandidate(ctx, id)
	if fatal := unrecoverableResolveError(ctx, loadErr); fatal != nil {
		return CandidateResolution{}, fatal
	}
	resolution, err := r.localResolution(ctx, id)
	if err != nil {
		return CandidateResolution{}, err
	}
	if resolution.Candidate != nil {
		return resolution, nil
	}

	r.mu.Lock()
	single := r.peerCount == 1
	timeout := r.params.CandidateResolveTimeout
	r.mu.Unlock()
	if single {
		if loadErr != nil {
			// There is no peer to work around an unreadable durable copy with, so
			// this overlay is told what actually went wrong with it.
			return CandidateResolution{}, loadErr
		}

		return CandidateResolution{}, ErrCandidateUnavailable
	}

	// From here on this flight holds no pointer to the payload it just loaded.
	// What it is waiting for is a certificate, and pinning a finalized
	// candidate's bytes for however long a peer takes to send one is exactly the
	// retention the finalization sweep exists to bound.
	for {
		if err = ctx.Err(); err != nil {
			return CandidateResolution{}, ErrResolverClosed
		}

		started := time.Now()
		r.mu.Lock()
		entry := r.entry(id)
		// A payload released with a durable copy behind it is still locally
		// available and must not be asked of a peer. One released without a
		// readable copy — never accepted by consensus, or stored and since found
		// unreadable — has to come from the network.
		wantCandidate := entry.candidate == nil && !entry.durable
		wantNotarization := entry.notarization.IsZero()
		params := r.params
		r.mu.Unlock()
		if !wantCandidate && !wantNotarization {
			if resolution, err = r.localResolution(ctx, id); err != nil {
				return CandidateResolution{}, err
			}
			if resolution.Candidate != nil {
				return resolution, nil
			}
			// The durable copy this entry claims is unreadable; ask a peer again.
			wantCandidate = true
		}

		request := CandidateRequest{
			SessionID:         r.sessionID,
			ID:                id,
			WantCandidate:     wantCandidate,
			WantNotarization:  wantNotarization,
			MaximumReplyBytes: r.maxReply,
		}
		requestCtx, cancel := context.WithTimeout(ctx, timeout)
		r.mu.Lock()
		flight.request = cancel
		r.mu.Unlock()
		response, requestErr := r.provider.RequestCandidate(requestCtx, request)
		r.mu.Lock()
		flight.request = nil
		r.mu.Unlock()
		cancel()
		if requestErr == nil {
			_ = r.mergeResponse(request, response)
		}

		timeout = multiplyResolveTimeout(
			timeout,
			params.CandidateResolveTimeoutMultiplier,
			params.CandidateResolveTimeoutCap,
		)
		if resolution, err = r.localResolution(ctx, id); err != nil {
			return CandidateResolution{}, err
		}
		if resolution.Candidate != nil {
			return resolution, nil
		}

		remaining := params.CandidateResolveCooldown - time.Since(started)
		if remaining <= 0 {
			continue
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()

			return CandidateResolution{}, ErrResolverClosed
		case <-timer.C:
		}
	}
}

func multiplyResolveTimeout(timeout time.Duration, multiplier float64, cap time.Duration) time.Duration {
	next := float64(timeout) * multiplier
	if next >= float64(cap) {
		return cap
	}
	if next > float64(math.MaxInt64) {
		return cap
	}

	return time.Duration(next)
}

// loadCandidate returns the artifact this entry ends up holding, and a nil
// artifact for a candidate the session has never seen. The pointer is taken
// from the flight rather than re-read from the entry: the finalization sweep
// may release the payload again between the flight completing and this return.
func (r *candidateResolver) loadCandidate(
	ctx context.Context,
	id simplex.CandidateID,
) (*CandidateArtifact, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return nil, ErrResolverClosed
	}
	entry := r.entry(id)
	if entry.candidate != nil {
		artifact := entry.candidate
		r.mu.Unlock()

		return artifact, nil
	}
	if !entry.durable {
		r.mu.Unlock()

		return nil, nil
	}
	flight := entry.load
	if flight == nil {
		flight = &resolverFlight{done: make(chan struct{})}
		entry.load = flight
		r.wg.Add(1)
		go r.loadCandidateLoop(id, flight)
	}
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-flight.done:
		return flight.result.Candidate, flight.err
	}
}

func (r *candidateResolver) loadCandidateLoop(id simplex.CandidateID, flight *resolverFlight) {
	defer r.wg.Done()

	loaded, err := r.loadDurableCandidate(id)
	r.forgetUnreadableDurableCopy(id, err)

	r.mu.Lock()
	flight.err = err
	flight.result.Candidate = loaded
	entry := r.entry(id)
	if entry.load == flight {
		entry.load = nil
	}
	close(flight.done)
	r.mu.Unlock()
}

func (r *candidateResolver) loadDurableCandidate(id simplex.CandidateID) (*CandidateArtifact, error) {
	record, err := r.storage.Candidate(r.ctx, r.session, id)
	if err != nil {
		if errors.Is(err, corestorage.ErrNotFound) {
			return nil, errors.New("validator runtime: candidate index points to missing data")
		}

		return nil, fmt.Errorf("validator runtime: load candidate: %w", err)
	}
	if record.ID != id {
		return nil, errors.New("validator runtime: candidate storage returned another id")
	}
	artifact, err := r.codec.decode(record.Wire, &id)
	if err != nil {
		return nil, fmt.Errorf("validator runtime: decode stored candidate: %w", err)
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	entry := r.entry(id)
	// The store's own identity for these bytes, not a second hash of them: this
	// is the loading path of a lineage walk, and the megabytes it would
	// re-derive are the ones it just read.
	if entry.hasWireHash && record.ContentHash != entry.wireHash {
		return nil, ErrCandidateConflict
	}
	if entry.candidate == nil {
		r.attachPayloadLocked(id, entry, artifact, record.Wire, record.ContentHash)
	}
	r.markDurableLocked(entry)
	r.completeResolveLocked(id, entry)

	return entry.candidate, nil
}

// forgetUnreadableDurableCopy drops the entry's durability claim after a read
// of that copy failed. Missing, truncated or no longer decodable bytes are all
// the same thing to every consumer: the claim promised a read that does not
// work, so it must stop standing in for one. Peers can supply the candidate,
// and a later store writes the copy again instead of trusting an index entry
// that lies.
//
// A conflict is not such a failure. The store holds other bytes under this id,
// which no peer can resolve and a retry would only re-read; that stays the hard
// error it is. Neither is a read cut short by session shutdown: the copy is
// nothing this resolver has learned anything about.
func (r *candidateResolver) forgetUnreadableDurableCopy(id simplex.CandidateID, err error) {
	if err == nil || errors.Is(err, ErrCandidateConflict) || r.ctx.Err() != nil {
		return
	}

	r.mu.Lock()
	if entry := r.entries[id]; entry != nil {
		r.clearDurableLocked(entry)
	}
	r.mu.Unlock()
}

func (r *candidateResolver) mergeResponse(request CandidateRequest, response CandidateResponse) error {
	if uint64(len(response.CandidateWire)) > uint64(request.MaximumReplyBytes) {
		return errors.New("validator runtime: candidate response exceeds requested limit")
	}
	if !request.WantCandidate && len(response.CandidateWire) != 0 {
		return errors.New("validator runtime: unrequested candidate in response")
	}
	if !request.WantNotarization && response.Notarization != nil {
		return errors.New("validator runtime: unrequested notarization in response")
	}

	var artifact *CandidateArtifact
	var canonicalWire []byte
	var wireHash [32]byte
	var err error
	if len(response.CandidateWire) != 0 {
		// The canonical wire comes from the roots the decode parsed: the
		// signature is verified once, and neither BOC is parsed or
		// re-serialized a second time to prove it canonical.
		artifact, canonicalWire, err = r.codec.decodeCanonical(response.CandidateWire, &request.ID)
		if err != nil {
			return err
		}
		wireHash = sha256.Sum256(canonicalWire)
	}
	var notarization simplex.VerifiedCertificate
	if response.Notarization != nil {
		if response.Notarization.Vote != simplex.NotarizeVote(request.ID) {
			return errors.New("validator runtime: notarization vote mismatch")
		}
		if notarization, err = r.certificates.Verify(response.Notarization); err != nil {
			return fmt.Errorf("validator runtime: invalid notarization certificate: %w", err)
		}
	}

	r.mu.Lock()
	entry := r.entry(request.ID)
	if artifact != nil {
		if entry.hasWireHash && wireHash != entry.wireHash {
			r.mu.Unlock()

			return ErrCandidateConflict
		}
		// A peer answering for a candidate the store already holds must not put
		// the payload back into memory: the sweep released it because it can be
		// read, and every consumer reaches storage before the network. Without a
		// readable copy the peer's bytes are the only bytes there are, and the
		// identity check above is what keeps them from being different ones.
		if entry.candidate == nil && !entry.durable {
			r.attachPayloadLocked(request.ID, entry, artifact, canonicalWire, wireHash)
		}
	}
	if !notarization.IsZero() && entry.notarization.IsZero() {
		entry.notarization = notarization
	}
	r.completeResolveLocked(request.ID, entry)
	r.mu.Unlock()

	return nil
}

// storeAsync submits the durable write of one candidate wire and returns the
// flight that completes when that write does. Admission is synchronous and so
// is the submission itself: the record enters the store's single durable FIFO
// on the calling goroutine, at exactly the point in the caller's sequence where
// it was submitted. Only the wait on the fsync is left to the caller.
//
// A nil flight with a nil error means the candidate is already durable and
// there is nothing to wait for.
//
// notify, when non-nil, is invoked once with the write's outcome from the
// storage callback. It exists for callers that submit the write and do not join
// it — an observer runtime has no voter to report a failed write to — and it
// runs on the storage callback goroutine, never on a goroutine this spawns.
func (r *candidateResolver) storeAsync(id simplex.CandidateID, notify func(error)) (*resolverFlight, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return nil, ErrResolverClosed
	}
	entry := r.entry(id)
	// The durable copy is checked first, and it is checked on the claim rather
	// than on this process having written it: a candidate whose payload the
	// sweep released, and one this session restored from its own durable index
	// after a restart, are both stored and neither is unavailable. Answering
	// otherwise reports a write failure for a write that already happened, and
	// Simplex ends the session on it.
	if entry.durable {
		r.mu.Unlock()
		if notify != nil {
			notify(nil)
		}

		return nil, nil
	}
	if entry.candidate == nil || len(entry.wire) == 0 {
		r.mu.Unlock()

		return nil, ErrCandidateUnavailable
	}
	flight := entry.store
	var wire []byte
	var wireHash [32]byte
	isOwner := false
	if flight == nil {
		flight = &resolverFlight{done: make(chan struct{})}
		entry.store = flight
		wire = entry.wire
		// The identity this candidate was admitted under. attachPayloadLocked is
		// the only writer of entry.wire and it writes this hash of those very
		// bytes in the same statement, so handing it to the store spares a second
		// sha256 over a multi-megabyte payload on the producer's own goroutine.
		wireHash = entry.wireHash
		isOwner = true
		r.wg.Add(1)
	}
	if notify != nil {
		// The flight is still in flight while entry.store points at it: the
		// callback clears the field under this same lock before it completes.
		flight.notify = append(flight.notify, notify)
	}
	r.mu.Unlock()

	if isOwner {
		record := CandidateRecord{ID: id, Wire: wire, ContentHash: wireHash}
		r.storage.SaveCandidate(r.session, record, func(err error) {
			r.mu.Lock()
			flight.err = err
			entry := r.entry(id)
			if err == nil {
				r.markDurableLocked(entry)
			}
			if entry.store == flight {
				entry.store = nil
			}
			notifiers := flight.notify
			flight.notify = nil
			close(flight.done)
			r.mu.Unlock()
			for _, done := range notifiers {
				done(err)
			}
			r.wg.Done()
		})
	}

	return flight, nil
}

func (r *candidateResolver) store(ctx context.Context, id simplex.CandidateID) error {
	flight, err := r.storeAsync(id, nil)
	if err != nil {
		return err
	}
	if flight == nil {
		return nil
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-flight.done:
		return flight.err
	}
}

func (r *candidateResolver) close() {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()
		r.wg.Wait()

		return
	}
	r.closed = true
	r.cancel()
	r.mu.Unlock()

	r.wg.Wait()
}
