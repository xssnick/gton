package simplex

import (
	"errors"
	"fmt"
)

// BootstrapState is everything the engine needs to recover a session after a
// restart.
type BootstrapState struct {
	FirstNonAnnouncedWindow uint32
	// OurVotes are the locally cast vote intents in seqno (cast) order.
	OurVotes []Vote
	// Certificates are all durably saved certificates, order-insensitive.
	Certificates []*Certificate
}

// ValidatedBootstrap is a BootstrapState whose every certificate has been
// verified against one session id and validator set.
//
// A recovering session has three consumers of the journal — the engine, the
// candidate resolver and the masterchain replay of durable finalizations — and
// each of them used to re-verify the certificates it cared about, so every
// saved certificate paid its quorum of ed25519 verifications twice. This type
// is the single check they share: the state is only reachable through State(),
// and the only way to construct one is ValidateBootstrap.
//
// Certificates() hands the same certificates out already sealed, so a consumer
// that must prove the quorum further downstream — block acceptance does —
// carries the proof in the type instead of repeating the work.
type ValidatedBootstrap struct {
	state   *BootstrapState
	binding CertificateBinding
	sealed  []VerifiedCertificate
}

// ValidateBootstrap verifies every certificate in state against sessionID and
// validators. The quorum threshold is derived once for the whole set.
func ValidateBootstrap(
	sessionID [32]byte,
	validators []Validator,
	state *BootstrapState,
) (ValidatedBootstrap, error) {
	if state == nil {
		return ValidatedBootstrap{}, errors.New("simplex: bootstrap state is absent")
	}
	if len(state.Certificates) == 0 {
		// Nothing to verify, so an unusable roster is not an error here — it
		// never was, and a caller reading an empty journal legitimately has one.
		// The binding is still derived when the roster allows it, so that a
		// consumer comparing bindings sees the real one rather than the zero
		// value; with no certificates there is nothing sealed to leak either way.
		binding, err := NewCertificateBinding(sessionID, validators)
		if err != nil {
			return ValidatedBootstrap{state: state}, nil
		}

		return ValidatedBootstrap{state: state, binding: binding}, nil
	}
	if len(validators) == 0 {
		return ValidatedBootstrap{}, errors.New("simplex: empty validator set")
	}

	totalWeight, err := totalValidatorWeight(validators)
	if err != nil {
		return ValidatedBootstrap{}, err
	}
	threshold := totalWeight*2/3 + 1
	binding, err := NewCertificateBinding(sessionID, validators)
	if err != nil {
		return ValidatedBootstrap{}, err
	}

	sealed := make([]VerifiedCertificate, 0, len(state.Certificates))
	for _, cert := range state.Certificates {
		if cert == nil {
			return ValidatedBootstrap{}, errors.New("simplex: nil journaled certificate")
		}
		if err = validateCertificateVote(cert.Vote); err != nil {
			return ValidatedBootstrap{}, fmt.Errorf("simplex: corrupt journaled certificate for %s: %w", cert.Vote, err)
		}
		payload := DataToSign(sessionID, VoteBytes(cert.Vote))
		if err = verifyCertificatePayload(validators, threshold, payload, cert); err != nil {
			return ValidatedBootstrap{}, fmt.Errorf("simplex: corrupt journaled certificate for %s: %w", cert.Vote, err)
		}
		sealed = append(sealed, sealCertificate(cert, binding))
	}

	return ValidatedBootstrap{state: state, binding: binding, sealed: sealed}, nil
}

// State is the verified journal state, or nil for the zero value.
func (b ValidatedBootstrap) State() *BootstrapState {
	return b.state
}

// Certificates are the journaled certificates, sealed under Binding(). The
// order matches State().Certificates.
func (b ValidatedBootstrap) Certificates() []VerifiedCertificate {
	return b.sealed
}

// Binding is the (session id, roster) pair the certificates were verified
// under. It is the zero binding only when nothing was sealed.
func (b ValidatedBootstrap) Binding() CertificateBinding {
	return b.binding
}

// Journal is the durable intent log of the session.
//
// Saves are asynchronous: the engine keeps
// serving messages while a record persists and resumes the dependent work
// (sign+apply+send for votes, application for certificates, the window
// announcement) only from the completion callback.
//
// Contract, enforced by the engine and required from implementations:
//   - done must be invoked exactly once per save: with nil once the record is
//     durable (fsync or equivalent), with ErrAlreadySaved for a duplicate, or
//     with a write error;
//   - done may be invoked synchronously before the save returns (in-memory
//     journals, dedup hits) or later from any goroutine. The callback is
//     engine-provided and re-enters the engine loop by itself when the engine
//     runs under Runner; an engine driven directly (deterministic tests) must
//     receive completions on its goroutine;
//   - records deduplicate by content hash (OurVoteRecordHash /
//     CertificateRecordHash). A duplicate completes with ErrAlreadySaved even
//     when the original save is still in flight; the engine applies the
//     record through the original completion only;
//   - completion order is free: the engine tolerates saves completing out of
//     submission order;
//   - Bootstrap returns own votes ordered by their original save order.
//
// Any error other than ErrAlreadySaved is treated by the engine as fatal for
// the session (fail-closed).
type Journal interface {
	Bootstrap() (*BootstrapState, error)
	SaveOurVote(v Vote, done func(error))
	SaveCertificate(c *Certificate, done func(error))
	SaveFirstNonAnnouncedWindow(w uint32, done func(error))
}
