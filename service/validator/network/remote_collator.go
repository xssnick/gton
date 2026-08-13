package network

import (
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/simplex"
)

const remoteCollatorMaximumAnswerBytes = 1024

// RemoteCollatorClient sends Delegated-v3 control queries over a Manager's
// already prepared private consensus overlays.
type RemoteCollatorClient struct {
	manager    *Manager
	collatorID [32]byte

	mu      sync.Mutex
	started bool
	closed  bool
}

var _ collator.RemoteCollatorTransport = (*RemoteCollatorClient)(nil)

// NewRemoteCollatorClient binds one authenticated remote collator ADNL ID.
func NewRemoteCollatorClient(manager *Manager, collatorID [32]byte) (*RemoteCollatorClient, error) {
	if manager == nil {
		return nil, errors.New("validator network: manager is required")
	}
	if collatorID == ([32]byte{}) {
		return nil, errors.New("validator network: remote collator ID is zero")
	}

	return &RemoteCollatorClient{manager: manager, collatorID: collatorID}, nil
}

func (c *RemoteCollatorClient) CollatorID() [32]byte {
	return c.collatorID
}

func (c *RemoteCollatorClient) Start(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.manager.mu.RLock()
	managerClosed := c.manager.closed
	c.manager.mu.RUnlock()
	if managerClosed {
		return ErrManagerClosed
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		return collator.ErrClosed
	}
	c.started = true
	return nil
}

func (c *RemoteCollatorClient) Close(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	return nil
}

func (c *RemoteCollatorClient) Probe(
	ctx context.Context,
	query collator.AuthenticatedQuery,
	request simplex.ConsensusPleaseCollatePrepare,
) error {
	wire, err := EncodePleaseCollatePrepare(request)
	if err != nil {
		return err
	}
	return c.query(ctx, query, wire)
}

func (c *RemoteCollatorClient) Commit(
	ctx context.Context,
	query collator.AuthenticatedQuery,
	request simplex.ConsensusPleaseCollate,
) error {
	wire, err := EncodePleaseCollate(request)
	if err != nil {
		return err
	}
	return c.query(ctx, query, wire)
}

func (c *RemoteCollatorClient) query(
	ctx context.Context,
	query collator.AuthenticatedQuery,
	wire []byte,
) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if query.SourceADNL != c.manager.localADNLID {
		return errors.New("validator network: outgoing authenticated query has another source ADNL ID")
	}
	c.mu.Lock()
	started := c.started
	closed := c.closed
	c.mu.Unlock()
	if closed {
		return collator.ErrClosed
	}
	if !started {
		return collator.ErrNotStarted
	}

	hub, err := c.manager.session(query.SessionID)
	if err != nil {
		return errors.Join(collator.ErrUnavailable, err)
	}
	validatorEndpoint := hub.endpoint(sessionKindValidator)
	if validatorEndpoint == nil || !validatorEndpoint.active() {
		return errors.Join(collator.ErrUnavailable, ErrSessionInactive)
	}
	spec, err := hub.contribution(sessionKindValidator)
	if err != nil || spec.role == collator.OverlayRoleObserver || spec.signer == nil {
		return errors.Join(collator.ErrUnavailable, ErrSessionInactive)
	}
	target := p2p.PeerID(c.collatorID)
	if expected, ok := spec.candidateADNL[target]; !ok || expected != target {
		return errors.Join(collator.ErrUnauthorized, errors.New("validator network: remote collator is not authorized for session"))
	}
	answer, err := hub.queryRaw(
		ctx,
		target,
		remoteCollatorMaximumAnswerBytes,
		wire,
	)
	if err != nil {
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("%w: query remote collator: %w", collator.ErrUnavailable, err)
	}
	if IsRequestError(answer) {
		return ErrRequestRejected
	}

	// C++ treats every delivered non-requestError answer as success. Some
	// deployed peers return constructors other than tonNode.success.
	return nil
}
