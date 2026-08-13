package validator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
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
// notarization certificate are present and verified.
type CandidateResolution struct {
	Candidate    *CandidateArtifact
	Notarization *simplex.Certificate
}

type candidateResolver struct {
	session    SessionStorageID
	sessionID  [32]byte
	storage    ValidatorStorage
	provider   CandidateProvider
	codec      *candidateCodec
	validators []simplex.Validator
	peerCount  int
	maxReply   uint32

	ctx    context.Context
	cancel context.CancelFunc
	wg     sync.WaitGroup

	mu       sync.Mutex
	params   simplex.Params
	entries  map[simplex.CandidateID]*candidateEntry
	requests map[simplex.PeerID]*candidateRequestWindow
	closed   bool
}

type candidateEntry struct {
	candidateInDB bool
	candidate     *CandidateArtifact
	wire          []byte
	notarization  *simplex.Certificate
	isStored      bool
	load          *resolverFlight
	resolve       *resolverFlight
	store         *resolverFlight
}

type resolverFlight struct {
	done   chan struct{}
	cancel context.CancelFunc
	err    error
}

type candidateRequestWindow struct {
	times []time.Time
	head  int
}

type candidateResolverOptions struct {
	Session    SessionStorageID
	SessionID  [32]byte
	Storage    ValidatorStorage
	Provider   CandidateProvider
	Codec      *candidateCodec
	Validators []simplex.Validator
	PeerCount  int
	Limits     CandidateLimits
	Params     simplex.Params
	Stored     StoredSessionState
	Bootstrap  simplex.ValidatedBootstrap
}

func newCandidateResolver(options candidateResolverOptions) (*candidateResolver, error) {
	maxReply := uint64(options.Limits.MaxBlockBytes) +
		uint64(options.Limits.MaxCollatedDataBytes) +
		1<<20
	if maxReply > math.MaxUint32 {
		return nil, errors.New("validator runtime: maximum candidate reply size overflows uint32")
	}

	ctx, cancel := context.WithCancel(context.Background())
	r := &candidateResolver{
		session:    options.Session,
		sessionID:  options.SessionID,
		storage:    options.Storage,
		provider:   options.Provider,
		codec:      options.Codec,
		validators: options.Validators,
		peerCount:  options.PeerCount,
		maxReply:   uint32(maxReply),
		ctx:        ctx,
		cancel:     cancel,
		params:     options.Params,
		entries:    make(map[simplex.CandidateID]*candidateEntry, len(options.Stored.CandidateIDs)),
	}
	for _, id := range options.Stored.CandidateIDs {
		r.entry(id).candidateInDB = true
	}
	bootstrap := options.Bootstrap.State()
	if bootstrap == nil {
		cancel()

		return nil, errors.New("validator runtime: bootstrap state was not validated")
	}
	// These signatures were verified when the bootstrap state was validated.
	for _, certificate := range bootstrap.Certificates {
		if certificate.Vote.Kind != simplex.VoteNotarize {
			continue
		}
		r.entry(certificate.Vote.ID).notarization = certificate
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

func (r *candidateResolver) observeNotarization(id simplex.CandidateID, certificate *simplex.Certificate) {
	r.mu.Lock()
	entry := r.entry(id)
	entry.notarization = certificate
	r.completeResolveLocked(entry)
	r.mu.Unlock()
}

func (r *candidateResolver) stage(artifact *CandidateArtifact, wire []byte) error {
	id := artifact.Candidate.ID

	r.mu.Lock()
	defer r.mu.Unlock()
	if r.closed {
		return ErrResolverClosed
	}

	entry := r.entry(id)
	if entry.candidate != nil {
		if !bytes.Equal(entry.wire, wire) {
			return ErrCandidateConflict
		}
		r.completeResolveLocked(entry)

		return nil
	}
	entry.candidate = artifact
	entry.wire = wire
	r.completeResolveLocked(entry)

	return nil
}

func (r *candidateResolver) completeResolveLocked(entry *candidateEntry) {
	if entry.candidate == nil || entry.notarization == nil || entry.resolve == nil {
		return
	}

	flight := entry.resolve
	entry.resolve = nil
	flight.err = nil
	close(flight.done)
	flight.cancel()
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

// candidateCacheStats is a debug projection of the session-scoped candidate
// cache. Bytes is the payload actually retained in memory: for a staged
// candidate that is the canonical wire plus the decoded block and collated
// data, all three of which survive the durable write to storage.
type candidateCacheStats struct {
	Entries    int
	Candidates int
	Stored     int
	Bytes      int64
}

func (r *candidateResolver) cacheStats() candidateCacheStats {
	r.mu.Lock()
	defer r.mu.Unlock()

	stats := candidateCacheStats{Entries: len(r.entries)}
	for _, entry := range r.entries {
		if entry.isStored {
			stats.Stored++
		}
		if entry.candidate == nil {
			continue
		}
		stats.Candidates++
		stats.Bytes += int64(len(entry.wire)) +
			int64(len(entry.candidate.BlockBOC)) +
			int64(len(entry.candidate.CollatedData))
	}

	return stats
}

func (r *candidateResolver) candidate(
	ctx context.Context,
	id simplex.CandidateID,
) (*CandidateArtifact, error) {
	if err := r.loadCandidate(ctx, id); err != nil {
		return nil, err
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	artifact := r.entry(id).candidate
	if artifact == nil {
		return nil, ErrCandidateUnavailable
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
		mustLoad := entry.candidate == nil && entry.candidateInDB
		r.mu.Unlock()
		if mustLoad {
			if err := r.loadCandidate(ctx, request.ID); err != nil {
				return CandidateResponse{}, err
			}
		}

		r.mu.Lock()
		if entry.candidate != nil {
			// This is a real external ownership boundary: the transport may retain
			// or mutate its response buffer after this call returns.
			response.CandidateWire = bytes.Clone(entry.wire)
		}
		r.mu.Unlock()
	}
	if request.WantNotarization {
		r.mu.Lock()
		response.Notarization = entry.notarization
		r.mu.Unlock()
	}

	return response, nil
}

func (r *candidateResolver) resolve(ctx context.Context, id simplex.CandidateID) (CandidateResolution, error) {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return CandidateResolution{}, ErrResolverClosed
	}
	entry := r.entry(id)
	if entry.candidate != nil && entry.notarization != nil {
		result := CandidateResolution{Candidate: entry.candidate, Notarization: entry.notarization}
		r.mu.Unlock()

		return result, nil
	}
	flight := entry.resolve
	if flight == nil {
		flightCtx, cancel := context.WithCancel(r.ctx)
		flight = &resolverFlight{done: make(chan struct{}), cancel: cancel}
		entry.resolve = flight
		r.wg.Add(1)
		go r.resolveLoop(flightCtx, id, flight)
	}
	r.mu.Unlock()

	select {
	case <-ctx.Done():
		return CandidateResolution{}, ctx.Err()
	case <-flight.done:
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	entry = r.entry(id)
	if flight.err != nil {
		return CandidateResolution{}, flight.err
	}
	if entry.candidate == nil || entry.notarization == nil {
		return CandidateResolution{}, ErrCandidateUnavailable
	}

	return CandidateResolution{Candidate: entry.candidate, Notarization: entry.notarization}, nil
}

func (r *candidateResolver) resolveLoop(
	ctx context.Context,
	id simplex.CandidateID,
	flight *resolverFlight,
) {
	defer r.wg.Done()

	err := r.resolveInner(ctx, id)
	r.mu.Lock()
	entry := r.entry(id)
	if entry.resolve == flight {
		flight.err = err
		entry.resolve = nil
		close(flight.done)
		flight.cancel()
	}
	r.mu.Unlock()
}

func (r *candidateResolver) resolveInner(ctx context.Context, id simplex.CandidateID) error {
	if err := r.loadCandidate(ctx, id); err != nil {
		return err
	}

	r.mu.Lock()
	entry := r.entry(id)
	if entry.candidate != nil && entry.notarization != nil {
		r.mu.Unlock()

		return nil
	}
	if r.peerCount == 1 {
		r.mu.Unlock()

		return ErrCandidateUnavailable
	}
	timeout := r.params.CandidateResolveTimeout
	r.mu.Unlock()

	for {
		if err := ctx.Err(); err != nil {
			return ErrResolverClosed
		}

		started := time.Now()
		r.mu.Lock()
		entry = r.entry(id)
		wantCandidate := entry.candidate == nil
		wantNotarization := entry.notarization == nil
		params := r.params
		r.mu.Unlock()
		if !wantCandidate && !wantNotarization {
			return nil
		}

		request := CandidateRequest{
			SessionID:         r.sessionID,
			ID:                id,
			WantCandidate:     wantCandidate,
			WantNotarization:  wantNotarization,
			MaximumReplyBytes: r.maxReply,
		}
		requestCtx, cancel := context.WithTimeout(ctx, timeout)
		response, err := r.provider.RequestCandidate(requestCtx, request)
		cancel()
		if err == nil {
			_ = r.mergeResponse(request, response)
		}

		timeout = multiplyResolveTimeout(
			timeout,
			params.CandidateResolveTimeoutMultiplier,
			params.CandidateResolveTimeoutCap,
		)
		r.mu.Lock()
		entry = r.entry(id)
		complete := entry.candidate != nil && entry.notarization != nil
		r.mu.Unlock()
		if complete {
			return nil
		}

		remaining := params.CandidateResolveCooldown - time.Since(started)
		if remaining <= 0 {
			continue
		}
		timer := time.NewTimer(remaining)
		select {
		case <-ctx.Done():
			timer.Stop()

			return ErrResolverClosed
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

func (r *candidateResolver) loadCandidate(ctx context.Context, id simplex.CandidateID) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return ErrResolverClosed
	}
	entry := r.entry(id)
	if entry.candidate != nil || !entry.candidateInDB {
		r.mu.Unlock()

		return nil
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
		return ctx.Err()
	case <-flight.done:
		return flight.err
	}
}

func (r *candidateResolver) loadCandidateLoop(id simplex.CandidateID, flight *resolverFlight) {
	defer r.wg.Done()

	record, err := r.storage.Candidate(r.ctx, r.session, id)
	if err != nil {
		if errors.Is(err, corestorage.ErrNotFound) {
			err = errors.New("validator runtime: candidate index points to missing data")
		} else {
			err = fmt.Errorf("validator runtime: load candidate: %w", err)
		}
	} else {
		if record.ID != id {
			err = errors.New("validator runtime: candidate storage returned another id")
		}
		var artifact *CandidateArtifact
		if err == nil {
			artifact, err = r.codec.decode(record.Wire, &id)
		}
		if err != nil {
			err = fmt.Errorf("validator runtime: decode stored candidate: %w", err)
		} else {
			r.mu.Lock()
			entry := r.entry(id)
			if entry.candidate != nil && !bytes.Equal(entry.wire, record.Wire) {
				err = ErrCandidateConflict
			} else {
				if entry.candidate == nil {
					entry.candidate = artifact
					entry.wire = record.Wire
				}
				entry.isStored = true
				r.completeResolveLocked(entry)
			}
			r.mu.Unlock()
		}
	}

	r.mu.Lock()
	flight.err = err
	entry := r.entry(id)
	if entry.load == flight {
		entry.load = nil
	}
	close(flight.done)
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
	var err error
	if len(response.CandidateWire) != 0 {
		// The canonical wire comes from the roots the decode parsed: the
		// signature is verified once, and neither BOC is parsed or
		// re-serialized a second time to prove it canonical.
		artifact, canonicalWire, err = r.codec.decodeCanonical(response.CandidateWire, &request.ID)
		if err != nil {
			return err
		}
	}
	if response.Notarization != nil {
		certificate := response.Notarization
		if certificate.Vote != simplex.NotarizeVote(request.ID) {
			return errors.New("validator runtime: notarization vote mismatch")
		}
		if err = simplex.VerifyCertificate(r.sessionID, r.validators, certificate); err != nil {
			return fmt.Errorf("validator runtime: invalid notarization certificate: %w", err)
		}
	}

	r.mu.Lock()
	entry := r.entry(request.ID)
	if artifact != nil {
		if entry.candidate != nil && !bytes.Equal(entry.wire, canonicalWire) {
			r.mu.Unlock()

			return ErrCandidateConflict
		}
		if entry.candidate == nil {
			entry.candidate = artifact
			entry.wire = canonicalWire
		}
	}
	if response.Notarization != nil && entry.notarization == nil {
		entry.notarization = response.Notarization
	}
	r.completeResolveLocked(entry)
	r.mu.Unlock()

	return nil
}

func (r *candidateResolver) store(ctx context.Context, id simplex.CandidateID) error {
	r.mu.Lock()
	if r.closed {
		r.mu.Unlock()

		return ErrResolverClosed
	}
	entry := r.entry(id)
	if entry.candidate == nil || len(entry.wire) == 0 {
		r.mu.Unlock()

		return ErrCandidateUnavailable
	}
	if entry.isStored {
		r.mu.Unlock()

		return nil
	}
	flight := entry.store
	var wire []byte
	isOwner := false
	if flight == nil {
		flight = &resolverFlight{done: make(chan struct{})}
		entry.store = flight
		wire = entry.wire
		isOwner = true
		r.wg.Add(1)
	}
	r.mu.Unlock()

	if isOwner {
		r.storage.SaveCandidate(r.session, CandidateRecord{ID: id, Wire: wire}, func(err error) {
			r.mu.Lock()
			flight.err = err
			entry := r.entry(id)
			if err == nil {
				entry.isStored = true
				entry.candidateInDB = true
			}
			if entry.store == flight {
				entry.store = nil
			}
			close(flight.done)
			r.mu.Unlock()
			r.wg.Done()
		})
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
