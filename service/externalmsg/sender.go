package externalmsg

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/xssnick/gton/internal/extmsg"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const MaxBroadcastDataSize = 16 << 20

var (
	ErrMessageTooLarge = errors.New("external message is too large")
	ErrParseFailed     = errors.New("external message parse failed")
	ErrBroadcastFailed = errors.New("external message broadcast failed")
)

type CheckFunc func(ctx context.Context, body []byte, root *cell.Cell, msg *tlb.ExternalMessage) (CheckResult, error)

type BroadcastFunc func(ctx context.Context, body []byte, dst *address.Address, root *cell.Cell, msg *tlb.ExternalMessage) error

type SenderOptions struct {
	Check          CheckFunc
	Broadcast      BroadcastFunc
	AllowDuplicate bool
	Now            func() time.Time
	MaxBodyBytes   int
	Cache          *MessageCache
	Limiter        *extmsg.AddressLimiter
}

type Sender struct {
	check          CheckFunc
	broadcast      BroadcastFunc
	allowDuplicate bool
	now            func() time.Time
	maxBodyBytes   int
	cache          *MessageCache
	limiter        *extmsg.AddressLimiter
}

type sendAttempt struct {
	cacheKey     uint64
	cacheMessage bool
	duplicate    bool
}

type senderStageError struct {
	stage error
	err   error
}

func (e senderStageError) Error() string {
	return e.err.Error()
}

func (e senderStageError) Unwrap() error {
	return errors.Join(e.stage, e.err)
}

func NewSender(opts SenderOptions) (*Sender, error) {
	if opts.Check == nil {
		return nil, fmt.Errorf("external message checker is required")
	}
	if opts.Broadcast == nil {
		return nil, fmt.Errorf("external message broadcaster is required")
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	maxBodyBytes := opts.MaxBodyBytes
	if maxBodyBytes == 0 {
		maxBodyBytes = MaxBroadcastDataSize
	}
	if maxBodyBytes < 0 {
		return nil, fmt.Errorf("external message max body bytes cannot be negative")
	}

	cache := opts.Cache
	if cache == nil {
		cache = NewMessageCache()
	}

	limiter := opts.Limiter
	if limiter == nil {
		limiter = extmsg.NewDefaultAddressLimiter()
	}

	return &Sender{
		check:          opts.Check,
		broadcast:      opts.Broadcast,
		allowDuplicate: opts.AllowDuplicate,
		now:            now,
		maxBodyBytes:   maxBodyBytes,
		cache:          cache,
		limiter:        limiter,
	}, nil
}

func (s *Sender) Send(ctx context.Context, body []byte) error {
	attempt, err := s.beginSend(body)
	if err != nil {
		return err
	}
	if attempt.duplicate {
		return nil
	}

	_, err = s.send(ctx, body, attempt)
	return err
}

func (s *Sender) SendForHash(ctx context.Context, body []byte) (*cell.Cell, error) {
	attempt, err := s.beginSend(body)
	if err != nil {
		return nil, err
	}
	if attempt.duplicate {
		root, _, err := extmsg.ParseMessage(body)
		if err != nil {
			return nil, senderStageError{stage: ErrParseFailed, err: err}
		}
		return root, nil
	}

	return s.send(ctx, body, attempt)
}

func (s *Sender) beginSend(body []byte) (sendAttempt, error) {
	if s.maxBodyBytes > 0 && len(body) > s.maxBodyBytes {
		return sendAttempt{}, fmt.Errorf("%w: %d", ErrMessageTooLarge, len(body))
	}

	if s.allowDuplicate {
		return sendAttempt{}, nil
	}

	cacheKey := MessageCacheKey(body)
	return sendAttempt{
		cacheKey:     cacheKey,
		cacheMessage: true,
		duplicate:    !s.cache.Mark(cacheKey, s.now()),
	}, nil
}

func (s *Sender) send(ctx context.Context, body []byte, attempt sendAttempt) (*cell.Cell, error) {
	dropCached := func() {
		if attempt.cacheMessage {
			s.cache.Drop(attempt.cacheKey)
		}
	}

	root, msg, err := extmsg.ParseMessage(body)
	if err != nil {
		dropCached()
		return nil, senderStageError{stage: ErrParseFailed, err: err}
	}

	addrKey := extmsg.AddressKeyFor(msg.DstAddr)
	if err = externalMessageAddressLimitError(addrKey, s.limiter.Check(addrKey, s.now())); err != nil {
		dropCached()
		return nil, err
	}

	check, err := s.check(ctx, body, root, msg)
	if err != nil {
		dropCached()
		return nil, err
	}
	if check.Root == nil || check.Message == nil || check.Message.DstAddr == nil {
		dropCached()
		return nil, fmt.Errorf("external message checker returned incomplete result")
	}

	if err = externalMessageAddressLimitError(addrKey, s.limiter.Add(addrKey, s.now())); err != nil {
		dropCached()
		return nil, err
	}
	if err = s.broadcast(ctx, body, msg.DstAddr, check.Root, check.Message); err != nil {
		s.limiter.Remove(addrKey, s.now())
		dropCached()
		return nil, senderStageError{stage: ErrBroadcastFailed, err: err}
	}

	return check.Root, nil
}

func externalMessageAddressLimitError(key extmsg.AddressKey, err error) error {
	if err == nil {
		return nil
	}
	return fmt.Errorf("%w %d:%x", err, key.Workchain, key.Account[:])
}
