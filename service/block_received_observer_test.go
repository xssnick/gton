package service

import (
	"context"
	"errors"
	"testing"

	"github.com/xssnick/gton/service/p2p"

	"github.com/rs/zerolog"
)

var errTestBlockReceivedObserver = errors.New("block received observer")

type testBlockReceivedObserver struct {
	calls  int
	err    error
	events []BlockReceivedEvent
}

func (e *testBlockReceivedObserver) ObserveBlockReceived(_ context.Context, event BlockReceivedEvent) error {
	e.calls++
	e.events = append(e.events, event)
	return e.err
}

func TestBlockReceivedObserverLogsAndContinuesOnError(t *testing.T) {
	observer := &testBlockReceivedObserver{err: errTestBlockReceivedObserver}
	svc := &SyncCoordinator{
		blockReceivedObserver: &blockReceivedObserverRunner{
			log:      zerolog.Nop(),
			observer: observer,
		},
	}

	svc.ObserveBlockReceived(context.Background(), p2p.BlockReceivedEvent{
		IsSigned:   true,
		Downloaded: &p2p.DownloadedBlock{ID: testBlockID(0, topShard, 1)},
	})

	if observer.calls != 1 {
		t.Fatalf("observer calls = %d, want 1", observer.calls)
	}
}
