package collator

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"

	"github.com/xssnick/gton/service/validator/simplex"
)

// RemoteCollator implements Collator for a validator which delegates block
// production to a standalone collator. Candidate broadcasts still arrive via
// the validator's consensus SessionNetwork; this client owns only session
// control and the two Delegated-v3 queries.
type RemoteCollator struct {
	transport RemoteCollatorTransport
	id        [32]byte

	lifecycle chan struct{}
	mu        sync.Mutex
	started   bool
	closing   bool
	closed    bool
	active    uint64
	drain     chan struct{}
	drained   bool
	sessions  map[[32]byte]*remoteCollatorSession
	failed    uint64
	lastError string
}

type remoteCollatorSession struct {
	control chan struct{}
	known   bool
	record  SessionRecord
}

func newRemoteCollatorSession() *remoteCollatorSession {
	session := &remoteCollatorSession{control: make(chan struct{}, 1)}
	session.control <- struct{}{}

	return session
}

func (s *remoteCollatorSession) lock(ctx context.Context) error {
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-s.control:
		return nil
	}
}

func (s *remoteCollatorSession) unlock() {
	s.control <- struct{}{}
}

var _ Collator = (*RemoteCollator)(nil)

func NewRemoteCollator(transport RemoteCollatorTransport) (*RemoteCollator, error) {
	if transport == nil {
		return nil, errors.New("collator remote: transport is required")
	}
	id := transport.CollatorID()
	if id == ([32]byte{}) {
		return nil, errors.New("collator remote: collator id is zero")
	}

	client := &RemoteCollator{
		transport: transport,
		id:        id,
		lifecycle: make(chan struct{}, 1),
		sessions:  make(map[[32]byte]*remoteCollatorSession),
	}
	client.lifecycle <- struct{}{}

	return client, nil
}

func (c *RemoteCollator) CollatorID() [32]byte {
	return c.id
}

func (c *RemoteCollator) lockLifecycle(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.lifecycle:
		if err := ctx.Err(); err != nil {
			c.lifecycle <- struct{}{}

			return err
		}

		return nil
	}
}

func (c *RemoteCollator) unlockLifecycle() {
	c.lifecycle <- struct{}{}
}

func (c *RemoteCollator) Start(ctx context.Context) error {
	if err := c.lockLifecycle(ctx); err != nil {
		return err
	}
	defer c.unlockLifecycle()

	c.mu.Lock()
	if c.started {
		c.mu.Unlock()
		return nil
	}
	if c.closing || c.closed {
		c.mu.Unlock()
		return ErrClosed
	}
	c.mu.Unlock()

	if err := c.transport.Start(ctx); err != nil {
		c.recordError(err)
		return err
	}

	c.mu.Lock()
	c.started = true
	c.mu.Unlock()

	return nil
}

func (c *RemoteCollator) Close(ctx context.Context) error {
	if err := c.lockLifecycle(ctx); err != nil {
		return err
	}
	defer c.unlockLifecycle()

	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	if !c.closing {
		c.closing = true
		c.drain = make(chan struct{})
		if c.active == 0 {
			close(c.drain)
			c.drained = true
		}
	}
	drain := c.drain
	c.mu.Unlock()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-drain:
	}

	if err := c.transport.Close(ctx); err != nil {
		c.recordError(err)
		return err
	}

	c.mu.Lock()
	c.closed = true
	c.closing = false
	c.started = false
	c.drain = nil
	c.drained = false
	clear(c.sessions)
	c.mu.Unlock()

	return nil
}

func (c *RemoteCollator) Session(ctx context.Context, sessionID [32]byte) (SessionRecord, error) {
	done, err := c.beginOperation(ctx)
	if err != nil {
		return SessionRecord{}, err
	}
	defer done()

	session, err := c.lockSession(ctx, sessionID, false, true)
	if err != nil {
		return SessionRecord{}, err
	}
	defer session.unlock()

	return cloneSessionRecord(session.record), nil
}

func (c *RemoteCollator) PrepareSession(
	ctx context.Context,
	session Session,
	update SessionUpdate,
) error {
	done, err := c.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()

	managed, err := c.lockSession(ctx, session.ID, true, false)
	if err != nil {
		return err
	}
	defer managed.unlock()

	record := cloneSessionRecord(SessionRecord{Session: session, Update: update})
	if err = validateSessionRecord(record); err != nil {
		c.removeUnknownSession(session.ID, managed)
		return err
	}

	c.mu.Lock()
	if managed.known {
		if managed.record.Session.Equal(record.Session) &&
			managed.record.Update.Equal(record.Update) {
			c.mu.Unlock()
			return nil
		}
		c.mu.Unlock()
		return ErrSessionConflict
	}
	managed.record = record
	managed.known = true
	c.mu.Unlock()

	return nil
}

func (c *RemoteCollator) ActivateSession(ctx context.Context, activation SessionActivation) error {
	done, err := c.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()

	managed, err := c.lockSession(ctx, activation.SessionID, false, true)
	if err != nil {
		return err
	}
	defer managed.unlock()

	record := cloneSessionRecord(managed.record)
	if err = validateSessionActivation(record.Session, activation); err != nil {
		return err
	}
	if record.Activation != nil {
		if record.Activation.Equal(activation) {
			return nil
		}
		return ErrSessionConflict
	}

	activation = cloneSessionActivation(activation)
	record.Activation = &activation
	managed.record = record

	return nil
}

func (c *RemoteCollator) UpdateSession(ctx context.Context, update SessionUpdate) error {
	done, err := c.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()

	session, err := c.lockSession(ctx, update.SessionID, false, true)
	if err != nil {
		return err
	}
	defer session.unlock()

	record := cloneSessionRecord(session.record)
	if err = validateSessionUpdate(record.Session, update); err != nil {
		return err
	}
	if record.Update.Equal(update) {
		return nil
	}
	if err = ValidateSessionUpdateAdvance(record.Update, update); err != nil {
		return err
	}
	record.Update = cloneSessionUpdate(update)
	session.record = record

	return nil
}

func (c *RemoteCollator) RetireSession(ctx context.Context, sessionID [32]byte) error {
	done, err := c.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()

	session, err := c.lockSession(ctx, sessionID, true, false)
	if err != nil {
		return err
	}
	defer session.unlock()

	c.removeSession(sessionID, session)

	return nil
}

func (c *RemoteCollator) Probe(ctx context.Context, request WindowPreparation) error {
	done, err := c.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()

	session, err := c.lockSession(ctx, request.SessionID, false, true)
	if err != nil {
		return err
	}
	defer session.unlock()

	query := AuthenticatedQuery{
		SessionID:  request.SessionID,
		SourceADNL: request.SourceADNL,
	}
	wire := simplex.ConsensusPleaseCollatePrepare{WindowStartSlot: int32(request.StartSlot)}
	if err := c.transport.Probe(ctx, query, wire); err != nil {
		c.recordError(err)

		return classifyRemoteWindowError(ctx, "probe", err)
	}

	return nil
}

func (c *RemoteCollator) CommitDelegation(ctx context.Context, request WindowRequest) error {
	done, err := c.beginOperation(ctx)
	if err != nil {
		return err
	}
	defer done()

	session, err := c.lockSession(ctx, request.SessionID, false, true)
	if err != nil {
		return err
	}
	defer session.unlock()

	query := AuthenticatedQuery{
		SessionID:  request.SessionID,
		SourceADNL: request.SourceADNL,
	}
	wire := request.PleaseCollate
	wire.Signature = bytes.Clone(wire.Signature)
	if err := c.transport.Commit(ctx, query, wire); err != nil {
		c.recordError(err)

		return classifyRemoteWindowError(ctx, "commit delegation", err)
	}

	return nil
}

func (c *RemoteCollator) Status(ctx context.Context) (Status, error) {
	if err := ctx.Err(); err != nil {
		return Status{}, err
	}

	c.mu.Lock()
	defer c.mu.Unlock()
	sessions := uint64(0)
	for _, session := range c.sessions {
		if session.known {
			sessions++
		}
	}

	return Status{
		Started:       c.started,
		Closing:       c.closing,
		Closed:        c.closed,
		FailedWindows: c.failed,
		LastError:     c.lastError,
		Storage: StorageStatus{
			Sessions: sessions,
		},
	}, nil
}

// beginOperation admits work before it waits on a session lock. Close rejects
// new admissions and drains this count, so a successful transport.Close never
// overlaps either an active request or a same-session waiter.
func (c *RemoteCollator) beginOperation(ctx context.Context) (func(), error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	c.mu.Lock()
	if c.closed || c.closing {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if !c.started {
		c.mu.Unlock()
		return nil, ErrNotStarted
	}
	c.active++
	c.mu.Unlock()

	return c.finishOperation, nil
}

func (c *RemoteCollator) finishOperation() {
	c.mu.Lock()
	c.active--
	if c.closing && c.active == 0 && !c.drained {
		close(c.drain)
		c.drained = true
	}
	c.mu.Unlock()
}

func (c *RemoteCollator) lockSession(
	ctx context.Context,
	sessionID [32]byte,
	create bool,
	requireKnown bool,
) (*remoteCollatorSession, error) {
	c.mu.Lock()
	session := c.sessions[sessionID]
	if session == nil && create {
		session = newRemoteCollatorSession()
		c.sessions[sessionID] = session
	}
	c.mu.Unlock()
	if session == nil {
		return nil, ErrNotFound
	}
	if err := session.lock(ctx); err != nil {
		return nil, err
	}

	c.mu.Lock()
	current := c.sessions[sessionID]
	known := current == session && session.known
	c.mu.Unlock()
	if current != session || requireKnown && !known {
		session.unlock()
		return nil, ErrNotFound
	}

	return session, nil
}

func (c *RemoteCollator) removeUnknownSession(sessionID [32]byte, session *remoteCollatorSession) {
	c.mu.Lock()
	if c.sessions[sessionID] == session && !session.known {
		delete(c.sessions, sessionID)
	}
	c.mu.Unlock()
}

func (c *RemoteCollator) removeSession(sessionID [32]byte, session *remoteCollatorSession) {
	c.mu.Lock()
	if c.sessions[sessionID] == session {
		delete(c.sessions, sessionID)
	}
	c.mu.Unlock()
}

func classifyRemoteWindowError(ctx context.Context, operation string, err error) error {
	if ctxErr := ctx.Err(); ctxErr != nil {
		return ctxErr
	}
	if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
		return err
	}
	if errors.Is(err, ErrUnavailable) {
		return fmt.Errorf("collator remote: %s unavailable: %w", operation, err)
	}

	return fmt.Errorf("collator remote: %s: %w", operation, err)
}

func (c *RemoteCollator) recordError(err error) {
	c.mu.Lock()
	c.failed++
	c.lastError = err.Error()
	c.mu.Unlock()
}
