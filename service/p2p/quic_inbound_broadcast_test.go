package p2p

import (
	"bytes"
	"testing"
)

func TestQUICBroadcastSourceIgnoresFeedback(t *testing.T) {
	id := testPeerID("inbound-quic-broadcast-source")
	source := quicBroadcastSource{id: &id}

	if got := source.ID(); !bytes.Equal(got, id[:]) {
		t.Fatalf("source id = %x, want %x", got, id)
	}
	if err := source.SendCustomMessage(t.Context(), ForgetPeer{}); err != nil {
		t.Fatalf("ignored feedback returned an error: %v", err)
	}
}
