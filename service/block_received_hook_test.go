package service

import (
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/hooks"
	"github.com/xssnick/gton/service/p2p"

	"github.com/rs/zerolog"
)

var errTestBlockReceivedHook = errors.New("block received hook")

type testBlockReceivedExtension struct {
	calls  int
	err    error
	events []hooks.BlockReceivedEvent
}

func (e *testBlockReceivedExtension) OnBlockApplied(context.Context, hooks.BlockAppliedEvent) error {
	return nil
}

func (e *testBlockReceivedExtension) OnExternalMessage(context.Context, hooks.ExternalMessageEvent) error {
	return nil
}

func (e *testBlockReceivedExtension) OnBlockReceived(_ context.Context, event hooks.BlockReceivedEvent) error {
	e.calls++
	e.events = append(e.events, event)
	return e.err
}

func TestBlockReceivedHookLogsAndContinuesOnError(t *testing.T) {
	extension := &testBlockReceivedExtension{err: errTestBlockReceivedHook}
	svc := &Service{
		blockReceivedHooks: &blockReceivedHookRunner{
			log:       zerolog.Nop(),
			extension: extension,
		},
	}

	svc.ObserveBlockReceived(context.Background(), p2p.BlockReceivedEvent{
		IsSigned:   true,
		Downloaded: &p2p.DownloadedBlock{ID: testBlockID(0, topShard, 1)},
	})

	if extension.calls != 1 {
		t.Fatalf("hook calls = %d, want 1", extension.calls)
	}
}
