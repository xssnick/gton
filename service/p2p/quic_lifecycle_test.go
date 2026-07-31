package p2p

import (
	"context"
	"net"
	"runtime"
	"testing"
	"time"
)

func TestStartQUICServerCanRetryAfterBusyPort(t *testing.T) {
	adnlEndpoint, quicEndpoint := testFreeQUICDerivedEndpoint(t)
	busyConn, err := net.ListenPacket("udp", quicEndpoint.String())
	if err != nil {
		t.Fatalf("occupy QUIC port: %v", err)
	}
	t.Cleanup(func() {
		_ = busyConn.Close()
	})

	node := newTestNode(t)
	node.listenAddr = adnlEndpoint.String()

	if err := node.startQUICServer(); err == nil {
		t.Cleanup(func() {
			_ = node.closeQUICGateway()
		})
		t.Fatal("start succeeded while QUIC port was occupied")
	}
	if err := busyConn.Close(); err != nil {
		t.Fatalf("release occupied QUIC port: %v", err)
	}

	if err := node.startQUICServer(); err != nil {
		t.Fatalf("retry QUIC server start: %v", err)
	}
	t.Cleanup(func() {
		if err := node.closeQUICGateway(); err != nil {
			t.Errorf("close QUIC gateway: %v", err)
		}
	})
}

func TestCloseQUICGatewayReleasesPortAndCompletesServe(t *testing.T) {
	adnlEndpoint, quicEndpoint := testFreeQUICDerivedEndpoint(t)
	node := newTestNode(t)
	node.listenAddr = adnlEndpoint.String()

	if err := node.startQUICServer(); err != nil {
		t.Fatalf("start QUIC server: %v", err)
	}
	t.Cleanup(func() {
		_ = node.closeQUICGateway()
	})
	done := node.quicServeDone

	if err := node.closeQUICGateway(); err != nil {
		t.Fatalf("close QUIC gateway: %v", err)
	}
	select {
	case <-done:
	default:
		t.Fatal("QUIC serve loop did not complete before close returned")
	}

	packetConn, err := net.ListenPacket("udp", quicEndpoint.String())
	if err != nil {
		t.Fatalf("bind released QUIC port: %v", err)
	}
	if err := packetConn.Close(); err != nil {
		t.Fatalf("close rebound QUIC port: %v", err)
	}
}

func TestQUICMonitorEntersOfflineWithoutDeadlock(t *testing.T) {
	adnlEndpoint, _ := testFreeQUICDerivedEndpoint(t)
	node := newTestNode(t)
	node.listenAddr = adnlEndpoint.String()

	if err := node.startQUICServer(); err != nil {
		t.Fatalf("start QUIC server: %v", err)
	}
	t.Cleanup(func() {
		_ = node.closeQUICGateway()
	})
	ctx, cancel := context.WithCancel(context.Background())
	t.Cleanup(cancel)
	node.startQUICMonitor(ctx)

	if err := node.quicPacketConn.Close(); err != nil {
		t.Fatalf("close QUIC packet listener: %v", err)
	}

	timer := time.NewTimer(3 * time.Second)
	defer timer.Stop()
	select {
	case <-node.stopped:
	case <-timer.C:
		t.Fatal("node did not stop after unexpected QUIC listener failure")
	}
	if !node.IsOffline() {
		t.Fatal("node remained online after unexpected QUIC listener failure")
	}
	// The incident must be distinguishable from the deliberate ton.sync_until
	// stop: the service treats that one as a finished sync, and RunNode only
	// takes the process down (non-zero exit, so a supervisor restarts) on this.
	if !node.OfflineFailed() {
		t.Fatal("unexpected QUIC failure was not marked as a failure")
	}
	select {
	case <-node.Failed():
	default:
		t.Fatal("Failed was not closed after unexpected QUIC listener failure")
	}
}

// The other side of the same coin: a deliberate offline is not an incident, so
// it must neither raise the flag nor take the process down.
func TestEnterOfflineIsNotMarkedAsFailure(t *testing.T) {
	node := newTestNode(t)

	node.EnterOffline("ton.sync_until=1 reached")

	if !node.IsOffline() {
		t.Fatal("node did not enter offline mode")
	}
	if node.OfflineFailed() {
		t.Fatal("deliberate offline was marked as a failure")
	}
	select {
	case <-node.Failed():
		t.Fatal("deliberate offline closed Failed")
	default:
	}
}

func TestOfflineFailurePublishesReasonBeforeSignal(t *testing.T) {
	node := newTestNode(t)
	const reason = "QUIC gateway stopped unexpectedly"

	node.offlineReasonMu.Lock()
	reasonLocked := true
	defer func() {
		if reasonLocked {
			node.offlineReasonMu.Unlock()
		}
	}()

	go node.enterOfflineFailure(reason)

	for !node.IsOffline() {
		select {
		case <-node.Failed():
			t.Fatal("Failed was closed before the offline reason could be published")
		default:
			runtime.Gosched()
		}
	}
	select {
	case <-node.Failed():
		t.Fatal("Failed was closed while publishing the offline reason was blocked")
	default:
	}

	node.offlineReasonMu.Unlock()
	reasonLocked = false

	select {
	case <-node.Failed():
	case <-time.After(time.Second):
		t.Fatal("failure signal was not published after the reason lock was released")
	}
	if got := node.OfflineReason(); got != reason {
		t.Fatalf("offline reason = %q, want %q", got, reason)
	}
	node.Wait()
}

func TestQUICMonitorIgnoresNormalCancellation(t *testing.T) {
	adnlEndpoint, _ := testFreeQUICDerivedEndpoint(t)
	node := newTestNode(t)
	node.listenAddr = adnlEndpoint.String()

	if err := node.startQUICServer(); err != nil {
		t.Fatalf("start QUIC server: %v", err)
	}
	t.Cleanup(func() {
		_ = node.closeQUICGateway()
	})
	ctx, cancel := context.WithCancel(context.Background())
	node.startQUICMonitor(ctx)

	cancel()
	if err := node.closeQUICGateway(); err != nil {
		t.Fatalf("close QUIC gateway: %v", err)
	}
	node.wg.Wait()

	if node.IsOffline() {
		t.Fatalf("normal cancellation entered offline mode: %s", node.OfflineReason())
	}
}
