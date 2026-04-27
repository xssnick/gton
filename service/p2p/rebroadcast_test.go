package p2p

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"testing"

	"github.com/xssnick/tonutils-go/adnl/overlay"
	"github.com/xssnick/tonutils-go/tl"
)

type mockRebroadcastPeer struct {
	id   []byte
	sent []tl.Serializable
}

func (m *mockRebroadcastPeer) ID() []byte {
	return append([]byte(nil), m.id...)
}

func (m *mockRebroadcastPeer) SendCustomMessage(_ context.Context, req tl.Serializable) error {
	m.sent = append(m.sent, req)
	return nil
}

func TestPlanRebroadcastMatchesCppNodeRouting(t *testing.T) {
	tests := []struct {
		name       string
		kind       string
		payloadLen int
		mode       rebroadcastMode
		flags      int32
	}{
		{name: "block broadcast always fec", kind: "tonNode.blockBroadcast", payloadLen: 32, mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender},
		{name: "compressed block broadcast always fec", kind: "tonNode.blockBroadcastCompressedV2", payloadLen: 256, mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender},
		{name: "small shard info stays simple", kind: "tonNode.newShardBlockBroadcast", payloadLen: ordinarySimpleBroadcastMaxSize, mode: rebroadcastModeSimple},
		{name: "large shard info switches to fec anysender", kind: "tonNode.newShardBlockBroadcast", payloadLen: ordinarySimpleBroadcastMaxSize + 1, mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender},
		{name: "small external stays simple", kind: "tonNode.externalMessageBroadcast", payloadLen: 128, mode: rebroadcastModeSimple},
		{name: "large external switches to fec", kind: "tonNode.externalMessageBroadcast", payloadLen: ordinarySimpleBroadcastMaxSize + 1, mode: rebroadcastModeFEC},
		{name: "small ihr stays simple", kind: "tonNode.ihrMessageBroadcast", payloadLen: 128, mode: rebroadcastModeSimple},
		{name: "large ihr switches to fec", kind: "tonNode.ihrMessageBroadcast", payloadLen: ordinarySimpleBroadcastMaxSize + 1, mode: rebroadcastModeFEC},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			plan := planRebroadcast(tt.kind, tt.payloadLen)
			if plan.mode != tt.mode || plan.flags != tt.flags {
				t.Fatalf("unexpected plan: got mode=%d flags=%d want mode=%d flags=%d", plan.mode, plan.flags, tt.mode, tt.flags)
			}
		})
	}
}

func TestCalcFECRebroadcastPartsMatchesCppNodeFormula(t *testing.T) {
	tests := []struct {
		payload int
		want    uint32
	}{
		{payload: 1, want: 2},
		{payload: 768, want: 4},
		{payload: 769, want: 4},
		{payload: 1536, want: 6},
		{payload: 2000, want: 6},
	}

	for _, tt := range tests {
		if got := calcFECRebroadcastParts(tt.payload, rebroadcastFECSymbolSize); got != tt.want {
			t.Fatalf("payload=%d: got %d parts want %d", tt.payload, got, tt.want)
		}
	}
}

func TestBuildSimpleBroadcastSupportsAnySender(t *testing.T) {
	logger := discardLogger()
	node, err := New(Options{Logger: &logger})
	if err != nil {
		t.Fatalf("create node: %v", err)
	}

	payload := []byte{0xAA, 0xBB, 0xCC}
	msg, err := node.buildSimpleBroadcast(payload, overlay.BroadcastFlagAnySender)
	if err != nil {
		t.Fatalf("build simple broadcast: %v", err)
	}

	if msg.Flags != overlay.BroadcastFlagAnySender {
		t.Fatalf("unexpected flags: %d", msg.Flags)
	}

	toSign, err := tl.Serialize(overlay.BroadcastToSign{
		Hash: mustHashSimpleBroadcastID(t, msg.Data, msg.Flags),
		Date: uint32(msg.Date),
	}, true)
	if err != nil {
		t.Fatalf("serialize toSign: %v", err)
	}

	pub := node.privKey.Public().(ed25519.PublicKey)
	if !ed25519.Verify(pub, toSign, msg.Signature) {
		t.Fatalf("signature should verify with zero source hash when any-sender is enabled")
	}
}

func TestRebroadcastFECToPeerSetUsesCppNodeBurstCount(t *testing.T) {
	_, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatalf("generate key: %v", err)
	}

	peer := &mockRebroadcastPeer{id: bytes.Repeat([]byte{0x11}, 32)}
	payload := bytes.Repeat([]byte{0xAB}, 2000)
	req := rebroadcastRequest{
		kind:    "tonNode.blockBroadcast",
		payload: payload,
	}

	_, _, _ = rebroadcastFECToPeers(
		context.Background(),
		discardLogger(),
		priv,
		[]overlay.BroadcastPeer{peer},
		req,
		rebroadcastPlan{mode: rebroadcastModeFEC, flags: overlay.BroadcastFlagAnySender},
	)

	want := int(calcFECRebroadcastParts(len(payload), rebroadcastFECSymbolSize))
	if len(peer.sent) != want {
		t.Fatalf("unexpected sent part count: got %d want %d", len(peer.sent), want)
	}

	for i, msg := range peer.sent {
		if _, ok := msg.(*overlay.BroadcastFEC); !ok {
			t.Fatalf("message %d has unexpected type %T", i, msg)
		}
	}
}

func mustHashSimpleBroadcastID(t *testing.T, payload []byte, flags int32) []byte {
	t.Helper()

	hash, err := tl.Hash(OverlayBroadcastID{
		Source:   make([]byte, 32),
		DataHash: hashSimpleBroadcastPayload(payload),
		Flags:    flags,
	})
	if err != nil {
		t.Fatalf("hash broadcast id: %v", err)
	}
	return hash
}
