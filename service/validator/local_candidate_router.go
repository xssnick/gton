package validator

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/validator/collator"
)

var (
	// ErrCandidateRouteNotFound reports a collator artifact for a session that
	// has no active local validator runtime. The collator may retry delivery
	// while a recovered session is being prepared.
	ErrCandidateRouteNotFound = errors.New("validator local backend: candidate route not found")
	// ErrCandidateRouteConflict prevents two local voting runtimes from owning
	// the same consensus session and accepting the same collator output.
	ErrCandidateRouteConflict = errors.New("validator local backend: candidate route conflict")
)

type localCandidateRoute struct {
	generation uint64
	deliver    func(context.Context, collator.CandidateArtifact) error
}

// LocalCandidateRouter is the composition-root-owned emission boundary shared
// by a local collator and all local validator session backends. A session has
// at most one voting runtime; observers never register routes.
type LocalCandidateRouter struct {
	mu       sync.RWMutex
	next     uint64
	sessions map[[32]byte]localCandidateRoute
}

func NewLocalCandidateRouter() *LocalCandidateRouter {
	return &LocalCandidateRouter{sessions: make(map[[32]byte]localCandidateRoute)}
}

// Register binds one voting runtime to its session. The returned release
// function is idempotent and removes only this exact registration.
func (r *LocalCandidateRouter) Register(
	sessionID [32]byte,
	deliver func(context.Context, collator.CandidateArtifact) error,
) (func(), error) {
	if sessionID == ([32]byte{}) {
		return nil, errors.New("validator local backend: candidate route session id is zero")
	}
	if deliver == nil {
		return nil, errors.New("validator local backend: candidate route delivery is required")
	}

	r.mu.Lock()
	defer r.mu.Unlock()
	if _, exists := r.sessions[sessionID]; exists {
		return nil, ErrCandidateRouteConflict
	}
	r.next++
	generation := r.next
	r.sessions[sessionID] = localCandidateRoute{generation: generation, deliver: deliver}

	return func() {
		r.mu.Lock()
		if current, exists := r.sessions[sessionID]; exists && current.generation == generation {
			delete(r.sessions, sessionID)
		}
		r.mu.Unlock()
	}, nil
}

// EmitCandidate implements collator.EmitCandidate without copying the block
// or collated-data payloads. The selected backend and runtime treat the
// artifact as immutable for the rest of its lifetime.
func (r *LocalCandidateRouter) EmitCandidate(
	ctx context.Context,
	artifact collator.CandidateArtifact,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if artifact.SessionID != artifact.WindowID.SessionID {
		return errors.New("validator local backend: collator artifact session differs from its window")
	}

	r.mu.RLock()
	route, exists := r.sessions[artifact.SessionID]
	r.mu.RUnlock()
	if !exists {
		return fmt.Errorf("%w for session %x", ErrCandidateRouteNotFound, artifact.SessionID)
	}

	err := route.deliver(ctx, artifact)
	if errors.Is(err, ErrLeaderWindowClosed) {
		// The same window can no longer reopen in the validator runtime. C++
		// publishes CandidateReceived once and cancels a superseded producer;
		// retrying this durable artifact would only hold the collator production
		// barrier and block the next authenticated session view.
		return collator.ErrStaleWindow
	}

	return err
}
