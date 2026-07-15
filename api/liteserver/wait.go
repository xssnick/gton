package liteserver

import (
	"context"
	"errors"
	"time"

	"github.com/xssnick/gton/service/liveview"

	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

const errCodeTimeout int32 = 652

type queryLogTiming struct {
	// query holds the raw query payload; queryName resolves the debug label
	// lazily so the reflection-based naming runs only when a log event or the
	// query observer actually consumes it. Cold error paths may store an
	// already-resolved string instead.
	query any
	// sequence holds the raw sequence items for the same reason; the joined
	// name is rendered on demand by sequenceName.
	sequence     []tl.Serializable
	errorReason  string
	duration     time.Duration
	waitDuration time.Duration
}

func (t queryLogTiming) queryName() string {
	switch q := t.query.(type) {
	case nil:
		return "unknown"
	case string:
		if q == "" {
			return "unknown"
		}
		return q
	default:
		return liteserverQueryLogName(q)
	}
}

// sequenceName renders the sequence debug label. It reflects over every item,
// so call it only when the emitting log event is enabled.
func (t queryLogTiming) sequenceName() string {
	if len(t.sequence) == 0 {
		return ""
	}
	return liteserverQuerySequenceLogName(t.sequence)
}

// handleQueryDataWithTiming processes a query synchronously. A nil response is
// returned only when the gate fails to admit the query because the client
// disconnected.
func (s *Server) handleQueryDataWithTiming(ctx context.Context, data any, gate *executorGate) (tl.Serializable, queryLogTiming) {
	switch q := data.(type) {
	case liteclient.LiteServerQuery:
		return s.handleQueryDataWithTiming(ctx, q.Data, gate)
	case liteclient.LiteServerQueryPrefix:
		started := time.Now()
		resp := ton.LSError{Code: errCodeProtoViolation, Text: "missing liteserver function after queryPrefix"}
		return resp, queryLogTiming{query: q, duration: time.Since(started)}
	case []tl.Serializable:
		return s.handleQuerySequenceWithTiming(ctx, q, gate)
	default:
		if !gate.enter(ctx) {
			return nil, queryLogTiming{query: q}
		}
		return s.handleStandaloneQueryWithTiming(ctx, q)
	}
}

func (s *Server) handleStandaloneQueryWithTiming(ctx context.Context, query any) (tl.Serializable, queryLogTiming) {
	started := time.Now()
	if sendMessage, ok := query.(ton.SendMessage); ok {
		resp, reason := s.handleSendMessageWithReason(ctx, sendMessage)
		return resp, queryLogTiming{
			query:       query,
			errorReason: reason,
			duration:    time.Since(started),
		}
	}

	resp := s.handleQuery(ctx, query)
	return resp, queryLogTiming{
		query:    query,
		duration: time.Since(started),
	}
}

func (s *Server) handleQuerySequenceWithTiming(ctx context.Context, items []tl.Serializable, gate *executorGate) (tl.Serializable, queryLogTiming) {
	if len(items) == 0 {
		resp := ton.LSError{Code: errCodeProtoViolation, Text: "empty liteserver query"}
		return resp, queryLogTiming{query: "empty", sequence: items}
	}

	var waitDuration time.Duration
	for i, item := range items {
		switch q := item.(type) {
		case liteclient.LiteServerQueryPrefix:
			continue
		case ton.WaitMasterchainSeqno:
			started := time.Now()
			errResp := s.waitMasterchainSeqno(ctx, q)
			waitDuration += time.Since(started)
			if errResp != nil {
				return *errResp, queryLogTiming{
					query:        q,
					sequence:     items,
					waitDuration: waitDuration,
				}
			}
		case liteclient.LiteServerQuery:
			if i != len(items)-1 {
				resp := ton.LSError{Code: errCodeProtoViolation, Text: "unexpected query after liteServer.query"}
				// Resolve the name eagerly: the lazy path would unwrap the
				// wrapper and report the inner query instead.
				return resp, queryLogTiming{query: liteserverTypeName(q), sequence: items, waitDuration: waitDuration}
			}

			resp, timing := s.handleQueryDataWithTiming(ctx, q.Data, gate)
			timing.sequence = items
			timing.waitDuration += waitDuration
			return resp, timing
		default:
			if i != len(items)-1 {
				resp := ton.LSError{Code: errCodeProtoViolation, Text: "unexpected query after liteserver function"}
				return resp, queryLogTiming{query: item, sequence: items, waitDuration: waitDuration}
			}

			if !gate.enter(ctx) {
				return nil, queryLogTiming{query: item, sequence: items, waitDuration: waitDuration}
			}
			resp, timing := s.handleStandaloneQueryWithTiming(ctx, item)
			timing.sequence = items
			timing.waitDuration += waitDuration
			return resp, timing
		}
	}

	resp := ton.LSError{Code: errCodeProtoViolation, Text: "missing liteserver function after waitMasterchainSeqno"}
	return resp, queryLogTiming{query: "missing", sequence: items, waitDuration: waitDuration}
}

func (s *Server) waitMasterchainSeqno(ctx context.Context, query ton.WaitMasterchainSeqno) *ton.LSError {
	if query.Seqno < 0 {
		return &ton.LSError{Code: errCodeProtoViolation, Text: "invalid masterchain seqno"}
	}

	seqno := uint32(query.Seqno)
	timeout := time.Duration(query.Timeout) * time.Millisecond
	if s.requestLimits.MaxKeepAlive > 0 && timeout > s.requestLimits.MaxKeepAlive {
		timeout = s.requestLimits.MaxKeepAlive
	}
	return waitMasterchainError(s.store.WaitMasterchainSeqno(ctx, seqno, timeout))
}

func waitMasterchainError(err error) *ton.LSError {
	switch {
	case err == nil:
		return nil
	case errors.Is(err, liveview.ErrWaitMasterchainTimeout), errors.Is(err, context.DeadlineExceeded):
		return &ton.LSError{Code: errCodeTimeout, Text: "timeout"}
	case errors.Is(err, liveview.ErrWaitMasterchainTooFar):
		return &ton.LSError{Code: errCodeNotReady, Text: "too big masterchain block seqno"}
	case errors.Is(err, context.Canceled):
		return &ton.LSError{Code: errCodeTimeout, Text: "timeout"}
	default:
		resp := errorResponse(err, "cannot wait masterchain seqno")
		return &resp
	}
}
