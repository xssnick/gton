package service

import (
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/p2p"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type externalMessageCheckerStub struct {
	checkResult    externalmsg.CheckResult
	checkErr       error
	checkBOCResult externalmsg.CheckResult
	checkBOCErr    error

	checkCalls    int
	checkBOCCalls int
	ctx           context.Context
	body          []byte
	root          *cell.Cell
	message       *tlb.ExternalMessage
}

func (s *externalMessageCheckerStub) Check(ctx context.Context, body []byte, root *cell.Cell, message *tlb.ExternalMessage) (externalmsg.CheckResult, error) {
	s.checkCalls++
	s.ctx = ctx
	s.body = body
	s.root = root
	s.message = message

	return s.checkResult, s.checkErr
}

func (s *externalMessageCheckerStub) CheckBOC(ctx context.Context, body []byte) (externalmsg.CheckResult, error) {
	s.checkBOCCalls++
	s.ctx = ctx
	s.body = body

	return s.checkBOCResult, s.checkBOCErr
}

type externalMessageGateStub struct {
	err   error
	calls int
	ctx   context.Context
	event ExternalMessageEvent
}

func (g *externalMessageGateStub) AcceptExternalMessage(ctx context.Context, event ExternalMessageEvent) error {
	g.calls++
	g.ctx = ctx
	g.event = event

	return g.err
}

func TestExternalMessageAdmissionChecksParsedMessageWithoutCopies(t *testing.T) {
	t.Parallel()

	body := []byte{1, 2, 3}
	inputRoot := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	inputMessage := &tlb.ExternalMessage{}
	checkedRoot := cell.BeginCell().MustStoreUInt(2, 2).EndCell()
	checkedMessage := &tlb.ExternalMessage{}
	checker := &externalMessageCheckerStub{
		checkResult: externalmsg.CheckResult{
			Root:    checkedRoot,
			Message: checkedMessage,
		},
	}
	gate := &externalMessageGateStub{}
	admission := NewExternalMessageAdmission(zerolog.Nop(), checker, gate)
	ctx := t.Context()

	err := admission.AcceptExternalMessage(ctx, p2p.ExternalMessageEvent{
		IsLocal: true,
		Body:    body,
		Root:    inputRoot,
		Message: inputMessage,
	})
	if err != nil {
		t.Fatalf("accept external message: %v", err)
	}
	if checker.checkCalls != 1 || checker.checkBOCCalls != 0 {
		t.Fatalf("checker calls: Check=%d CheckBOC=%d, want 1 and 0", checker.checkCalls, checker.checkBOCCalls)
	}
	if checker.ctx != ctx {
		t.Fatal("checker received a different context")
	}
	if len(checker.body) != len(body) || &checker.body[0] != &body[0] {
		t.Fatal("checker received a copied body")
	}
	if checker.root != inputRoot || checker.message != inputMessage {
		t.Fatal("checker received different parsed message pointers")
	}
	if gate.calls != 1 || gate.ctx != ctx {
		t.Fatalf("gate calls/context: calls=%d same_context=%t, want 1 and true", gate.calls, gate.ctx == ctx)
	}
	if !gate.event.IsLocal || gate.event.MessageRoot != checkedRoot || gate.event.MessageParsed != checkedMessage {
		t.Fatal("gate did not receive the checker result unchanged")
	}
}

func TestExternalMessageAdmissionChecksBOCWithoutCopy(t *testing.T) {
	t.Parallel()

	body := []byte{4, 5, 6}
	checkedRoot := cell.BeginCell().MustStoreUInt(3, 2).EndCell()
	checkedMessage := &tlb.ExternalMessage{}
	checker := &externalMessageCheckerStub{
		checkBOCResult: externalmsg.CheckResult{
			Root:    checkedRoot,
			Message: checkedMessage,
		},
	}
	gate := &externalMessageGateStub{}
	admission := NewExternalMessageAdmission(zerolog.Nop(), checker, gate)

	if err := admission.AcceptExternalMessage(t.Context(), p2p.ExternalMessageEvent{Body: body}); err != nil {
		t.Fatalf("accept external message BOC: %v", err)
	}
	if checker.checkCalls != 0 || checker.checkBOCCalls != 1 {
		t.Fatalf("checker calls: Check=%d CheckBOC=%d, want 0 and 1", checker.checkCalls, checker.checkBOCCalls)
	}
	if len(checker.body) != len(body) || &checker.body[0] != &body[0] {
		t.Fatal("checker received a copied body")
	}
	if gate.event.MessageRoot != checkedRoot || gate.event.MessageParsed != checkedMessage {
		t.Fatal("gate did not receive the parsed BOC result unchanged")
	}
}

func TestExternalMessageAdmissionAcceptsCheckedMessageWithoutChecker(t *testing.T) {
	t.Parallel()

	root := cell.BeginCell().MustStoreUInt(1, 2).EndCell()
	message := &tlb.ExternalMessage{}
	checker := &externalMessageCheckerStub{checkErr: errors.New("checker must not run")}
	gate := &externalMessageGateStub{}
	admission := NewExternalMessageAdmission(zerolog.Nop(), checker, gate)
	ctx := t.Context()

	err := admission.AcceptCheckedExternalMessage(ctx, p2p.ExternalMessageEvent{
		IsLocal: true,
		Body:    []byte{7, 8, 9},
		Root:    root,
		Message: message,
	})
	if err != nil {
		t.Fatalf("accept checked external message: %v", err)
	}
	if checker.checkCalls != 0 || checker.checkBOCCalls != 0 {
		t.Fatalf("checked message invoked checker: Check=%d CheckBOC=%d", checker.checkCalls, checker.checkBOCCalls)
	}
	if gate.calls != 1 || gate.ctx != ctx {
		t.Fatalf("gate calls/context: calls=%d same_context=%t, want 1 and true", gate.calls, gate.ctx == ctx)
	}
	if !gate.event.IsLocal || gate.event.MessageRoot != root || gate.event.MessageParsed != message {
		t.Fatal("gate received different checked message pointers")
	}
}

func TestExternalMessageAdmissionRequiresChecker(t *testing.T) {
	t.Parallel()

	gate := &externalMessageGateStub{}
	admission := NewExternalMessageAdmission(zerolog.Nop(), nil, gate)

	err := admission.AcceptExternalMessage(t.Context(), p2p.ExternalMessageEvent{Body: []byte{1}})
	if !errors.Is(err, errExternalMessageCheckerNotConfigured) {
		t.Fatalf("AcceptExternalMessage error = %v, want %v", err, errExternalMessageCheckerNotConfigured)
	}
	if gate.calls != 0 {
		t.Fatalf("gate calls = %d, want 0", gate.calls)
	}
}

func TestExternalMessageAdmissionPropagatesCheckerError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("check failed")
	checker := &externalMessageCheckerStub{checkBOCErr: wantErr}
	gate := &externalMessageGateStub{}
	admission := NewExternalMessageAdmission(zerolog.Nop(), checker, gate)

	err := admission.AcceptExternalMessage(t.Context(), p2p.ExternalMessageEvent{Body: []byte{1}})
	if !errors.Is(err, wantErr) {
		t.Fatalf("AcceptExternalMessage error = %v, want %v", err, wantErr)
	}
	if gate.calls != 0 {
		t.Fatalf("gate calls = %d, want 0", gate.calls)
	}
}

func TestExternalMessageAdmissionPropagatesGateError(t *testing.T) {
	t.Parallel()

	wantErr := errors.New("gate rejected message")
	root := cell.BeginCell().MustStoreUInt(2, 2).EndCell()
	message := &tlb.ExternalMessage{}
	gate := &externalMessageGateStub{err: wantErr}
	admission := NewExternalMessageAdmission(zerolog.Nop(), &externalMessageCheckerStub{}, gate)

	err := admission.AcceptCheckedExternalMessage(t.Context(), p2p.ExternalMessageEvent{
		IsLocal: true,
		Root:    root,
		Message: message,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("AcceptCheckedExternalMessage error = %v, want %v", err, wantErr)
	}
	if gate.calls != 1 {
		t.Fatalf("gate calls = %d, want 1", gate.calls)
	}
	if !gate.event.IsLocal || gate.event.MessageRoot != root || gate.event.MessageParsed != message {
		t.Fatal("gate error path changed event pointers")
	}
}

func TestExternalMessageAdmissionAllowsMissingGate(t *testing.T) {
	t.Parallel()

	admission := NewExternalMessageAdmission(zerolog.Nop(), nil, nil)
	if err := admission.AcceptCheckedExternalMessage(t.Context(), p2p.ExternalMessageEvent{}); err != nil {
		t.Fatalf("accept checked message without gate: %v", err)
	}
}
