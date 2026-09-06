package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/adnl"
	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

// preparedFrameOverlayADNL is the test transport with the prepared ADNL send
// path production peers have (*adnl.ADNL through the gateway's peerConn); it
// records the prepared messages it was handed.
type preparedFrameOverlayADNL struct {
	*testOverlayADNL
	prepared []*adnl.PreparedCustomMessage
}

func (m *preparedFrameOverlayADNL) SendPreparedCustomMessage(_ context.Context, msg *adnl.PreparedCustomMessage) error {
	m.prepared = append(m.prepared, msg)
	return nil
}

func newPreparedFrameOverlayPeer(t *testing.T, overlayID []byte) (*overlay.ADNLOverlayWrapper, *preparedFrameOverlayADNL) {
	t.Helper()

	transport := &preparedFrameOverlayADNL{testOverlayADNL: newTestOverlayADNL()}
	receiver, err := overlay.NewBroadcastReceiver(overlayID, maxOverlayPayloadSize, true, false)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(receiver.Close)
	wrapper, err := overlay.CreateExtendedADNL(transport).AttachOverlay(receiver)
	if err != nil {
		t.Fatal(err)
	}
	return wrapper, transport
}

// TestSendFastFECToPeerSharesADNLFrameAcrossPeers runs the queued FEC
// rebroadcast for one sender against the fanout of ADNL peers the way the
// per-peer workers do, and checks that every peer was handed the same
// prepared ADNL message per part -- serialized once, framed once -- and that
// those bytes are exactly what the per-peer serialization used to produce.
func TestSendFastFECToPeerSharesADNLFrameAcrossPeers(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	sender, err := overlay.NewBroadcastFECSender(
		priv,
		overlay.CertificateEmpty{},
		bytes.Repeat([]byte{0xAB}, 2000),
		overlay.BroadcastFlagAnySender,
		overlay.WithBroadcastFECSymbolSize(rebroadcastFECSymbolSize),
		overlay.WithBroadcastFECDate(100),
	)
	if err != nil {
		t.Fatalf("create fec sender: %v", err)
	}

	overlayID := testPeerID("fec-frame-overlay")
	transports := make([]*preparedFrameOverlayADNL, 0, rebroadcastFanout)
	for i := 0; i < rebroadcastFanout; i++ {
		peer, transport := newPreparedFrameOverlayPeer(t, overlayID[:])
		if err = sendFastFECToPeer(context.Background(), sender, peer, time.Second, 0); err != nil {
			t.Fatalf("send to peer %d: %v", i, err)
		}
		transports = append(transports, transport)
	}

	total := int(sender.TotalParts())
	for i, transport := range transports {
		if len(transport.prepared) != total {
			t.Fatalf("peer %d received %d prepared parts, want %d", i, len(transport.prepared), total)
		}
		if len(transport.sent) != 0 {
			t.Fatalf("peer %d took the reflective path %d times", i, len(transport.sent))
		}
	}

	for seqno := 0; seqno < total; seqno++ {
		part, err := sender.Part(uint32(seqno))
		if err != nil {
			t.Fatal(err)
		}
		want, err := tl.Serialize(&adnl.MessageCustom{Data: []tl.Serializable{
			overlay.Message{Overlay: overlayID[:]},
			tl.Raw(part.FullWire),
		}}, true)
		if err != nil {
			t.Fatal(err)
		}

		first := transports[0].prepared[seqno]
		if !bytes.Equal(first.Wire(), want) {
			t.Fatalf("part %d: prepared frame differs from the per-peer serialization", seqno)
		}
		for i, transport := range transports[1:] {
			if transport.prepared[seqno] != first {
				t.Fatalf("part %d: peer %d was handed a different prepared frame", seqno, i+1)
			}
		}
	}
}
