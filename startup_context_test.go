package gton

import (
	"context"
	"testing"
)

func TestSignalContextsKeepGraceContextAfterRunCancellation(t *testing.T) {
	runCtx, shutdownCtx, cancelRun, stop := signalContexts(context.Background())
	t.Cleanup(stop)

	cancelRun()
	select {
	case <-runCtx.Done():
	default:
		t.Fatal("run context was not canceled")
	}
	select {
	case <-shutdownCtx.Done():
		t.Fatal("shutdown context was canceled with the run context")
	default:
	}

	stop()
	select {
	case <-shutdownCtx.Done():
	default:
		t.Fatal("shutdown context was not canceled by final stop")
	}
}
