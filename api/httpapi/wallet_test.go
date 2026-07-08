package httpapi

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	"github.com/xssnick/tonutils-go/ton/wallet"
)

func TestWalletTypeNameMatchesHTTPAPI(t *testing.T) {
	tests := []struct {
		version wallet.Version
		want    string
	}{
		{version: wallet.V3R2, want: "wallet v3 r2"},
		{version: wallet.V4R2, want: "wallet v4 r2"},
		{version: wallet.V5R1Final, want: "wallet v5 r1"},
	}

	for _, tt := range tests {
		if got := walletTypeName(tt.version); got != tt.want {
			t.Fatalf("walletTypeName(%v) = %q, want %q", tt.version, got, tt.want)
		}
	}
}

func TestWalletIDFromData(t *testing.T) {
	pubKey := ed25519.PublicKey(bytes.Repeat([]byte{0x12}, ed25519.PublicKeySize))

	v4, err := wallet.GetStateInit(pubKey, wallet.V4R2, wallet.DefaultSubwallet)
	if err != nil {
		t.Fatalf("v4 state init: %v", err)
	}
	v4ID, err := walletIDFromData(wallet.V4R2, v4.Data)
	if err != nil {
		t.Fatalf("v4 wallet id: %v", err)
	}
	if v4ID == nil || *v4ID != int64(wallet.DefaultSubwallet) {
		t.Fatalf("v4 wallet id = %v, want %d", v4ID, wallet.DefaultSubwallet)
	}

	const subwallet = 17
	v5Config := wallet.ConfigV5R1Final{NetworkGlobalID: wallet.MainnetGlobalID, Workchain: 0}
	v5, err := wallet.GetStateInit(pubKey, v5Config, subwallet)
	if err != nil {
		t.Fatalf("v5 state init: %v", err)
	}
	v5ID, err := walletIDFromData(wallet.V5R1Final, v5.Data)
	if err != nil {
		t.Fatalf("v5 wallet id: %v", err)
	}

	want := wallet.V5R1ID{
		NetworkGlobalID: wallet.MainnetGlobalID,
		WorkChain:       0,
		SubwalletNumber: subwallet,
		WalletVersion:   0,
	}.Serialized()
	if v5ID == nil || *v5ID != int64(want) {
		t.Fatalf("v5 wallet id = %v, want %d", v5ID, want)
	}
}
