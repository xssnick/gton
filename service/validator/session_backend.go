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
	ErrCandidateRejected = errors.New("validator runtime: candidate rejected")
)

// CandidateValidation is the semantic verifier result. ValidAfter mirrors
// ValidateCandidateResult.ok_from_utime and delays notarization without
// blocking the Simplex event loop.
type CandidateValidation struct {
	ValidAfter time.Time
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
	Candidate          *CandidateArtifact
	Certificate        *simplex.Certificate
	CertifiedCandidate *CandidateArtifact
	// Retry means the same in-process acceptance is waiting on a newer chain
	// view. Local ingress is retried because its asynchronous attempt may have
	// been rejected with the same stale view; network publication is not, since
	// acceptance publishes before it ever consults that view.
	Retry bool
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
	ValidateCandidate(context.Context, *ChainState, *CandidateArtifact) (CandidateValidation, error)
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
