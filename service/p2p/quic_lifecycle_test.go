package p2p

import (
	"context"
	"net"
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
