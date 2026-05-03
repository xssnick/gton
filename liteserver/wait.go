package liteserver

import (
	"context"
	"errors"
	"fmt"
	"time"

	"flexserver/service/storage"

	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/ton"
)

const errCodeTimeout int32 = 652

type masterchainSeqnoWaiter interface {
	WaitMasterchainSeqno(ctx context.Context, seqno uint32, timeout time.Duration) error
}

func (s *Server) handleQueryData(ctx context.Context, data any) tl.Serializable {
	switch q := data.(type) {
	case liteclient.LiteServerQuery:
		return s.handleQueryData(ctx, q.Data)
	case liteclient.LiteServerQueryPrefix:
		return ton.LSError{Code: errCodeProtoViolation, Text: "missing liteserver function after queryPrefix"}
	case tl.Raw:
		items, err := parseQuerySequence(q)
		if err != nil {
			return ton.LSError{Code: errCodeProtoViolation, Text: err.Error()}
		}
		return s.handleQuerySequence(ctx, items)
	case []tl.Serializable:
		return s.handleQuerySequence(ctx, q)
	default:
		return s.handleQuery(ctx, q)
	}
}

func parseQuerySequence(raw tl.Raw) ([]tl.Serializable, error) {
	data := []byte(raw)
	items := make([]tl.Serializable, 0, 2)
	for len(data) > 0 {
		var item tl.Serializable
		rest, err := tl.Parse(&item, data, true)
		if err != nil {
			return nil, fmt.Errorf("cannot parse liteserver query: %w", err)
		}
		items = append(items, item)
		data = rest
	}
	if len(items) == 0 {
		return nil, fmt.Errorf("empty liteserver query")
	}
	return items, nil
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
	if waiter, ok := s.store.(masterchainSeqnoWaiter); ok {
		return waitMasterchainError(waiter.WaitMasterchainSeqno(ctx, seqno, timeout))
	}

	return waitMasterchainError(s.pollMasterchainSeqno(ctx, seqno, timeout))
}

func (s *Server) pollMasterchainSeqno(ctx context.Context, seqno uint32, timeout time.Duration) error {
	if timeout > 10*time.Second {
		timeout = 10 * time.Second
	}
	if timeout < 0 {
		timeout = 0
	}

	waitCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		current, err := s.store.CurrentState(waitCtx)
		if err == nil {
			currentSeqno := currentMasterchainSeqno(current)
			if currentSeqno >= seqno {
				return nil
			}
			if currentSeqno > 0 && seqno > currentSeqno+100 {
				return errWaitMasterchainTooFar
			}
		} else if !errors.Is(err, storage.ErrNotFound) && !errors.Is(err, context.Canceled) && !errors.Is(err, context.DeadlineExceeded) {
			return err
		}

		select {
		case <-ticker.C:
		case <-waitCtx.Done():
			if errors.Is(waitCtx.Err(), context.DeadlineExceeded) {
				return errWaitMasterchainTimeout
			}
			return waitCtx.Err()
		}
	}
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
