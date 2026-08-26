package service

import (
	"context"
	"errors"
	"fmt"

	"github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var _ p2p.ExternalMessageAdmission = (*Service)(nil)

var errExternalMessageCheckerNotConfigured = errors.New("external message checker is not configured")

type externalMessageHookRunner struct {
	log       zerolog.Logger
	extension hooks.Extension
}

func newExternalMessageHookRunner(log zerolog.Logger, extension hooks.Extension) *externalMessageHookRunner {
	if extension == nil {
		return nil
	}

	return &externalMessageHookRunner{
		log:       log,
		extension: extension,
	}
}

func (s *Service) AcceptExternalMessage(ctx context.Context, event p2p.ExternalMessageEvent) error {
	if s.externalMessageChecker == nil {
		return errExternalMessageCheckerNotConfigured
	}

	result, err := s.checkExternalMessage(ctx, event)
	if err != nil {
		return err
	}
	return s.runExternalMessageHooks(ctx, event, result.Root, result.Message)
}

func (s *Service) AcceptCheckedExternalMessage(ctx context.Context, event p2p.ExternalMessageEvent) error {
	return s.runExternalMessageHooks(ctx, event, event.Root, event.Message)
}

func (s *Service) checkExternalMessage(ctx context.Context, event p2p.ExternalMessageEvent) (externalmsg.CheckResult, error) {
	if event.Root != nil && event.Message != nil {
		return s.externalMessageChecker.Check(ctx, event.Body, event.Root, event.Message)
	}
	return s.externalMessageChecker.CheckBOC(ctx, event.Body)
}

func (s *Service) runExternalMessageHooks(ctx context.Context, source p2p.ExternalMessageEvent, root *cell.Cell, msg *tlb.ExternalMessage) error {
	if s.externalMessageHooks == nil {
		return nil
	}
	return s.externalMessageHooks.run(ctx, hooks.ExternalMessageEvent{
		IsLocal:       source.IsLocal,
		Priority:      source.Priority,
		MessageRoot:   root,
		MessageParsed: msg,
	})
}

func (r *externalMessageHookRunner) run(ctx context.Context, event hooks.ExternalMessageEvent) error {
	if err := r.extension.OnExternalMessage(ctx, event); err != nil {
		r.log.Warn().
			Err(err).
			Str("extension", fmt.Sprintf("%T", r.extension)).
			Msg("external message hook failed, dropping message")
		return err
	}
	return nil
}
