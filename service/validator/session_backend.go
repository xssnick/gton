package validator

import (
	"context"
	"errors"
	"time"

	"github.com/xssnick/gton/service/validator/simplex"
)

var (
	// ErrBlockNotReady asks the state resolver to retry final acceptance after
	// the canonical one-second delay.
	ErrBlockNotReady = errors.New("validator runtime: block is not ready for acceptance")
	// ErrCandidateRejected is a semantic invalid-candidate verdict. It rejects
	// only that proposal. Infrastructure and cancellation errors remain distinct
	// so they are not misclassified as proof of Byzantine input; validation
	// failures of either kind make this validator abstain on that candidate.
	ErrCandidateRejected = candidateRejectedError{}
)

type candidateRejectedError struct{}

func (candidateRejectedError) Error() string      { return "validator runtime: candidate rejected" }
func (candidateRejectedError) CandidateRejected() {}

// CandidateValidation is the semantic verifier result. State is the exact
// immutable post-candidate state and is ready for descendants before the
// ordinary node pipeline applies the finalized block. ValidAfter mirrors
// ValidateCandidateResult.ok_from_utime and delays notarization without
// blocking the Simplex event loop.
type CandidateValidation struct {
	ValidAfter time.Time
	State      *ChainState
}

// CandidateValidationRequest is one candidate offered for validation against
// one parent.
//
// It is a struct rather than two arguments because successor belongs to this
// exact parent/candidate operation. Validation is one cancellable task: waits
// for masterchain and neighbour inputs resume inside the acquisition stage,
// while this handle lets its one successor apply overlap the semantic replay.
type CandidateValidationRequest struct {
	Parent   *ChainState
	Artifact *CandidateArtifact
	// successor is unexported because only this package can produce one that
	// belongs to Parent, and a handle belonging to another state is refused
	// rather than silently ignored. An implementation outside it simply
	// validates without the overlap, which is what every non-announcing backend
	// does anyway.
	successor *liveSuccessorApply
}

// CandidateSubmitter is the command callback attached to one leader window.
// It is valid only for that window and serializes candidates in slot order.
// A successful call transfers ownership of the immutable artifact to the
// runtime; the producer must not mutate its fields or backing buffers later.
type CandidateSubmitter func(context.Context, *CandidateArtifact) error

// BlockAcceptance contains both the ordinary block being accepted and the
// candidate certified by Certificate. They differ when a final certificate
// crosses one or more empty candidates to their ordinary ancestor.
type BlockAcceptance struct {
	Candidate *CandidateArtifact
	// Certificate is the quorum proof, sealed. Its zero value means "no
	// certificate": a shard finalization crossing empty candidates accepts the
	// ordinary ancestor under the notarization the resolver already holds.
	//
	// The seal is what lets BlockAccepter skip the second Ed25519 quorum pass.
	// It is only a proof relative to the (session id, roster) pair it was
	// verified under, so the accepter checks that binding before trusting it.
	Certificate        simplex.VerifiedCertificate
	CertifiedCandidate *CandidateArtifact
	// Retry means the same in-process acceptance is waiting on a newer chain
	// view. Local ingress is retried because its asynchronous attempt may have
	// been rejected with the same stale view; network publication is not, since
	// acceptance publishes before it ever consults that view.
	Retry bool
	// state is the chain state whose single tip IS the accepted block: the
	// successor this session already computed by applying the block's update to
	// the full parent it holds. It is carried here so acceptance can publish it
	// into the live view at the moment the block is accepted, instead of every
	// later reader waiting for the shard client to apply the block and the store
	// to flush it.
	//
	// It is unexported because only this package may produce one. A ChainState
	// root is by construction a full apply over a full parent; a state that is
	// merely good enough to validate — the proof-backed root the collator
	// executes against on the full-collated path — is narrow and must never be
	// published as a lineage state. Making this a runtime-supplied field would be
	// the way that distinction gets lost.
	//
	// Nil is normal and never an error: a replayed finalization after a restart
	// has no resolved state, and the reader falls back to waiting exactly as it
	// does today.
	state *ChainState
	// Replay means the finalization was restored from the consensus journal.
	// Local ingress must see it again because the process may have crashed after
	// persisting consensus state but before the asynchronously queued block was
	// applied. Network publication has already happened and is not repeated.
	Replay bool
}

// LeaderWindow is the resolved production input emitted after the window
// journal record and its base state are both ready.
type LeaderWindow struct {
	Window  simplex.Window
	StartAt time.Time
	Submit  CandidateSubmitter
}

// SessionBackend is the cohesive node/collator boundary of a consensus
// runtime. Implementations load authenticated chain tips, perform semantic
// validation and acceptance, and consume production/misbehavior events.
// UpdateSession is atomic: when it returns an error, the previously accepted
// SessionState must remain active and observable by every other method.
type SessionBackend interface {
	ActivateSession(context.Context, SessionStart) error
	UpdateSession(context.Context, SessionState) error
	LoadChainState(context.Context, ChainStateRequest) (ChainStateData, error)
	ValidateCandidate(context.Context, CandidateValidationRequest) (CandidateValidation, error)
	AcceptBlock(context.Context, BlockAcceptance) error
	HandleLeaderWindow(context.Context, LeaderWindow) error
	HandleMisbehavior(context.Context, uint32, simplex.Misbehavior) error
	// Close releases this runtime attachment but preserves durable collator
	// state so a still-desired consensus group can restart.
	Close() error
	// Retire permanently removes the collator state after the group leaves the
	// desired topology. It includes Close and is idempotent.
	Retire() error
}
