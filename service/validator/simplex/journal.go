package simplex

import (
	"errors"
	"fmt"
	"slices"
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
// is the single check they share, and the only way to construct one is
// ValidateBootstrap.
//
// Certificates() hands the same certificates out already sealed, so a consumer
// that must prove the quorum further downstream — block acceptance does —
// carries the proof in the type instead of repeating the work.
//
// The state and every certificate are owned snapshots. Public accessors return
// copies, so a journal cache or a consumer cannot mutate already verified
// recovery evidence.
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
		owned := cloneBootstrapState(state)
		// Nothing to verify, so an unusable roster is not an error here — it
		// never was, and a caller reading an empty journal legitimately has one.
		// The binding is still derived when the roster allows it, so that a
		// consumer comparing bindings sees the real one rather than the zero
		// value; with no certificates there is nothing sealed to leak either way.
		binding, err := NewCertificateBinding(sessionID, validators)
		if err != nil {
			return ValidatedBootstrap{state: owned}, nil
		}

		return ValidatedBootstrap{state: owned, binding: binding}, nil
	}

	verifier, err := NewCertificateVerifier(sessionID, validators)
	if err != nil {
		return ValidatedBootstrap{}, err
	}

	owned := &BootstrapState{
		FirstNonAnnouncedWindow: state.FirstNonAnnouncedWindow,
		OurVotes:                slices.Clone(state.OurVotes),
	}
	if state.Certificates != nil {
		owned.Certificates = make([]*Certificate, len(state.Certificates))
	}

	sealed := make([]VerifiedCertificate, 0, len(state.Certificates))
	for i, cert := range state.Certificates {
		if cert == nil {
			return ValidatedBootstrap{}, errors.New("simplex: nil journaled certificate")
		}
		verified, verifyErr := verifier.Verify(cert)
		if verifyErr != nil {
			return ValidatedBootstrap{}, fmt.Errorf("simplex: corrupt journaled certificate for %s: %w", cert.Vote, verifyErr)
		}
		owned.Certificates[i] = verified.certificate()
		sealed = append(sealed, verified)
	}

	return ValidatedBootstrap{state: owned, binding: verifier.Binding(), sealed: sealed}, nil
}

// State returns a copy of the verified journal state, or nil for the zero
// value. Mutating the result cannot change this validated snapshot.
func (b ValidatedBootstrap) State() *BootstrapState {
	return cloneBootstrapState(b.state)
}

// Certificates returns the journaled certificates sealed under Binding(). The
// returned slice is a copy and its order matches State().Certificates.
func (b ValidatedBootstrap) Certificates() []VerifiedCertificate {
	return slices.Clone(b.sealed)
}

// Binding is the (session id, roster) pair the certificates were verified
// under. It is the zero binding only when nothing was sealed.
func (b ValidatedBootstrap) Binding() CertificateBinding {
	return b.binding
}

// stateSnapshot is the package-owned immutable state used by Engine during
// recovery. It must never cross an injected interface or package boundary.
func (b ValidatedBootstrap) stateSnapshot() *BootstrapState {
	return b.state
}

// matches reports whether state has the same consensus contents as the owned
// snapshot. Engine uses it to retain the prevalidated fast path without
// retaining the journal's mutable pointer as an identity token.
func (b ValidatedBootstrap) matches(state *BootstrapState) bool {
	if b.state == nil || state == nil {
		return b.state == nil && state == nil
	}
	if b.state.FirstNonAnnouncedWindow != state.FirstNonAnnouncedWindow ||
		!slices.Equal(b.state.OurVotes, state.OurVotes) ||
		len(b.state.Certificates) != len(state.Certificates) {
		return false
	}
	for i := range b.state.Certificates {
		if !certificatesEqual(b.state.Certificates[i], state.Certificates[i]) {
			return false
		}
	}

	return true
}

func cloneBootstrapState(state *BootstrapState) *BootstrapState {
	if state == nil {
		return nil
	}

	cloned := &BootstrapState{
		FirstNonAnnouncedWindow: state.FirstNonAnnouncedWindow,
		OurVotes:                slices.Clone(state.OurVotes),
	}
	if state.Certificates != nil {
		cloned.Certificates = make([]*Certificate, len(state.Certificates))
		for i := range state.Certificates {
			cloned.Certificates[i] = cloneCertificate(state.Certificates[i])
		}
	}

	return cloned
}

// Journal is the consensus persistence log of the session.
//
// Saves are asynchronous: the engine keeps
// serving messages while a record persists and resumes the dependent work
// (sign+apply+send for votes and application for certificates) from the
// completion callback. Window announcement does not wait for its cursor.
//
// Contract, enforced by the engine and required from implementations:
//   - done must be invoked exactly once per save: with nil once the record has
//     crossed that method's persistence boundary, with ErrAlreadySaved for a
//     duplicate, or with a write error. SaveOurVote must be durable (fsync or
//     equivalent) before nil because the callback permits a local signature.
//     Certificates and the pool cursor are authenticated/reconstructible and
//     may use a WAL-only boundary;
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
