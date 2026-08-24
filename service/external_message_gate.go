package service

import (
	"context"
	"errors"

	"github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/p2p"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

var _ p2p.ExternalMessageAdmission = (*ExternalMessageAdmission)(nil)

var errExternalMessageCheckerNotConfigured = errors.New("external message checker is not configured")

// ExternalMessageChecker validates an external message before it enters the
// broadcast pipeline.
type ExternalMessageChecker interface {
	Check(context.Context, []byte, *cell.Cell, *tlb.ExternalMessage) (externalmsg.CheckResult, error)
	CheckBOC(context.Context, []byte) (externalmsg.CheckResult, error)
}

// ExternalMessageAdmission owns external-message validation and the optional
// core gate invoked after validation.
type ExternalMessageAdmission struct {
	checker ExternalMessageChecker
	gate    *externalMessageGateRunner
}

type externalMessageGateRunner struct {
	log  zerolog.Logger
	gate ExternalMessageGate
}

func NewExternalMessageAdmission(log zerolog.Logger, checker ExternalMessageChecker, gate ExternalMessageGate) *ExternalMessageAdmission {
	return &ExternalMessageAdmission{
		checker: checker,
		gate:    newExternalMessageGateRunner(log, gate),
	}
}

func newExternalMessageGateRunner(log zerolog.Logger, gate ExternalMessageGate) *externalMessageGateRunner {
	if gate == nil {
		return nil
	}

	return &externalMessageGateRunner{
		log:  log,
		gate: gate,
	}
}

func (a *ExternalMessageAdmission) AcceptExternalMessage(ctx context.Context, event p2p.ExternalMessageEvent) error {
	return acceptExternalMessage(ctx, event, a.checker, a.gate)
}

func (a *ExternalMessageAdmission) AcceptCheckedExternalMessage(ctx context.Context, event p2p.ExternalMessageEvent) error {
	return runExternalMessageGate(ctx, a.gate, ExternalMessageEvent{
		IsLocal:        event.IsLocal,
		SerializedSize: len(event.Body),
		MessageRoot:    event.Root,
		MessageParsed:  event.Message,
	})
}

func acceptExternalMessage(ctx context.Context, event p2p.ExternalMessageEvent, checker ExternalMessageChecker, gate *externalMessageGateRunner) error {
	if checker == nil {
		return errExternalMessageCheckerNotConfigured
	}

	result, err := checkExternalMessage(ctx, event, checker)
	if err != nil {
		return err
	}

	return runExternalMessageGate(ctx, gate, ExternalMessageEvent{
		IsLocal:        event.IsLocal,
		SerializedSize: len(event.Body),
		MessageRoot:    result.Root,
		MessageParsed:  result.Message,
	})
}

func checkExternalMessage(ctx context.Context, event p2p.ExternalMessageEvent, checker ExternalMessageChecker) (externalmsg.CheckResult, error) {
	if event.Root != nil && event.Message != nil {
		return checker.Check(ctx, event.Body, event.Root, event.Message)
	}

	return checker.CheckBOC(ctx, event.Body)
}

func runExternalMessageGate(ctx context.Context, gate *externalMessageGateRunner, event ExternalMessageEvent) error {
	if gate == nil {
		return nil
	}

	return gate.run(ctx, event)
}

func (r *externalMessageGateRunner) run(ctx context.Context, event ExternalMessageEvent) error {
	if err := r.gate.AcceptExternalMessage(ctx, event); err != nil {
		r.log.Warn().
			Err(err).
			Msg("external message gate rejected message")
		return err
	}

	return nil
}
