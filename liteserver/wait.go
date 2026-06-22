package liteserver

import (
	"context"
	"errors"
	"time"

	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

const errCodeTimeout int32 = 652

type queryLogTiming struct {
	query        string
	sequence     string
	errorReason  string
	duration     time.Duration
	waitDuration time.Duration
}

func (t queryLogTiming) queryName() string {
	if t.query == "" {
		return "unknown"
	}
	return t.query
}

func (s *Server) handleQueryData(ctx context.Context, data any) tl.Serializable {
	switch q := data.(type) {
	case liteclient.LiteServerQuery:
		return s.handleQueryData(ctx, q.Data)
	case liteclient.LiteServerQueryPrefix:
		return ton.LSError{Code: errCodeProtoViolation, Text: "missing liteserver function after queryPrefix"}
	case []tl.Serializable:
		return s.handleQuerySequence(ctx, q)
	default:
		return s.handleQuery(ctx, q)
	}
}

func (s *Server) handleQueryDataWithTiming(ctx context.Context, data any) (tl.Serializable, queryLogTiming) {
	switch q := data.(type) {
	case liteclient.LiteServerQuery:
		return s.handleQueryDataWithTiming(ctx, q.Data)
	case liteclient.LiteServerQueryPrefix:
		started := time.Now()
		resp := ton.LSError{Code: errCodeProtoViolation, Text: "missing liteserver function after queryPrefix"}
		return resp, queryLogTiming{query: liteserverTypeName(q), duration: time.Since(started)}
	case []tl.Serializable:
		return s.handleQuerySequenceWithTiming(ctx, q)
	default:
		return s.handleStandaloneQueryWithTiming(ctx, q)
	}
}

func (s *Server) handleStandaloneQueryWithTiming(ctx context.Context, query any) (tl.Serializable, queryLogTiming) {
	started := time.Now()
	if sendMessage, ok := query.(ton.SendMessage); ok {
		resp, reason := s.handleSendMessageWithReason(ctx, sendMessage)
		return resp, queryLogTiming{
			query:       liteserverQueryLogName(query),
			errorReason: reason,
			duration:    time.Since(started),
		}
	}

	resp := s.handleQuery(ctx, query)
	return resp, queryLogTiming{
		query:    liteserverQueryLogName(query),
		duration: time.Since(started),
	}
}

func (s *Server) handleQuerySequenceWithTiming(ctx context.Context, items []tl.Serializable) (tl.Serializable, queryLogTiming) {
	sequence := liteserverQuerySequenceLogName(items)
	if len(items) == 0 {
		resp := ton.LSError{Code: errCodeProtoViolation, Text: "empty liteserver query"}
		return resp, queryLogTiming{query: "empty", sequence: sequence}
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
					query:        liteserverTypeName(q),
					sequence:     sequence,
					waitDuration: waitDuration,
				}
			}
		case liteclient.LiteServerQuery:
			if i != len(items)-1 {
				resp := ton.LSError{Code: errCodeProtoViolation, Text: "unexpected query after liteServer.query"}
				return resp, queryLogTiming{query: liteserverTypeName(q), sequence: sequence, waitDuration: waitDuration}
			}

			resp, timing := s.handleQueryDataWithTiming(ctx, q.Data)
			timing.sequence = sequence
			timing.waitDuration += waitDuration
			return resp, timing
		default:
			if i != len(items)-1 {
				resp := ton.LSError{Code: errCodeProtoViolation, Text: "unexpected query after liteserver function"}
				return resp, queryLogTiming{query: liteserverQueryLogName(item), sequence: sequence, waitDuration: waitDuration}
			}

			resp, timing := s.handleStandaloneQueryWithTiming(ctx, item)
			timing.sequence = sequence
			timing.waitDuration += waitDuration
			return resp, timing
		}
	}

	resp := ton.LSError{Code: errCodeProtoViolation, Text: "missing liteserver function after waitMasterchainSeqno"}
	return resp, queryLogTiming{query: "missing", sequence: sequence, waitDuration: waitDuration}
}

func (s *Server) handleQuerySequence(ctx context.Context, items []tl.Serializable) tl.Serializable {
	if len(items) == 0 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "empty liteserver query"}
	}

	for i, item := range items {
		switch q := item.(type) {
		case liteclient.LiteServerQueryPrefix:
			continue
		case ton.WaitMasterchainSeqno:
			if errResp := s.waitMasterchainSeqno(ctx, q); errResp != nil {
				return *errResp
			}
		case liteclient.LiteServerQuery:
			if i != len(items)-1 {
				return ton.LSError{Code: errCodeProtoViolation, Text: "unexpected query after liteServer.query"}
			}
			return s.handleQueryData(ctx, q.Data)
		default:
			if i != len(items)-1 {
				return ton.LSError{Code: errCodeProtoViolation, Text: "unexpected query after liteserver function"}
			}
			return s.handleQuery(ctx, item)
		}
	}

	return ton.LSError{Code: errCodeProtoViolation, Text: "missing liteserver function after waitMasterchainSeqno"}
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
	case errors.Is(err, errWaitMasterchainTimeout), errors.Is(err, context.DeadlineExceeded):
		return &ton.LSError{Code: errCodeTimeout, Text: "timeout"}
	case errors.Is(err, errWaitMasterchainTooFar):
		return &ton.LSError{Code: errCodeNotReady, Text: "too big masterchain block seqno"}
	case errors.Is(err, context.Canceled):
		return &ton.LSError{Code: errCodeTimeout, Text: "timeout"}
	default:
		resp := errorResponse(err, "cannot wait masterchain seqno")
		return &resp
	}
}
