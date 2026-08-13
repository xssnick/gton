package gton

import (
	"errors"
	"fmt"
	"testing"

	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/gton/service/p2p"
)

func TestExternalMessageNetworkErrorHidesTransportOfflineError(t *testing.T) {
	err := externalMessageNetworkError(fmt.Errorf("send through p2p: %w", p2p.ErrOffline))
	if !errors.Is(err, extmsg.ErrNetworkOffline) {
		t.Fatalf("network error = %v, want external-message offline", err)
	}
	if errors.Is(err, p2p.ErrOffline) {
		t.Fatalf("network error exposes p2p error: %v", err)
	}
}

func TestExternalMessageNetworkErrorPreservesUnrelatedError(t *testing.T) {
	want := errors.New("broadcast failed")
	if got := externalMessageNetworkError(want); got != want {
		t.Fatalf("network error = %v, want original %v", got, want)
	}
}
