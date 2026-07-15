package httpapi

import (
	"bytes"
	"crypto/ed25519"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton/wallet"
	"github.com/xssnick/tonutils-go/tvm/cell"
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

func TestWalletSeqnoFromData(t *testing.T) {
	const seqno = uint64(0x10203040)

	tests := []struct {
		name    string
		version wallet.Version
		data    *cell.Cell
	}{
		{name: "v1", version: wallet.V1R1, data: cell.BeginCell().MustStoreUInt(seqno, 32).EndCell()},
		{name: "v4", version: wallet.V4R2, data: cell.BeginCell().MustStoreUInt(seqno, 32).EndCell()},
		{name: "v5 beta", version: wallet.V5R1Beta, data: cell.BeginCell().MustStoreBoolBit(false).MustStoreUInt(seqno, 32).EndCell()},
		{name: "v5 final", version: wallet.V5R1Final, data: cell.BeginCell().MustStoreBoolBit(true).MustStoreUInt(seqno, 32).EndCell()},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := walletSeqnoFromData(test.version, test.data)
			if err != nil {
				t.Fatalf("wallet seqno: %v", err)
			}
			if got != seqno {
				t.Fatalf("wallet seqno = %d, want %d", got, seqno)
			}
		})
	}

	if _, err := walletSeqnoFromData(wallet.V5R1Final, cell.BeginCell().MustStoreBoolBit(true).EndCell()); err == nil {
		t.Fatal("truncated v5 wallet data was accepted")
	}
}

func TestSuspendedAddressCacheUsesConfigRoot(t *testing.T) {
	firstRoot := suspendedAddressConfigRoot(t, 100)
	secondRoot := suspendedAddressConfigRoot(t, 200)
	var cache suspendedAddressCache

	first, err := cache.load(firstRoot)
	if err != nil {
		t.Fatalf("load first suspended list: %v", err)
	}
	reused, err := cache.load(firstRoot)
	if err != nil {
		t.Fatalf("reuse first suspended list: %v", err)
	}
	if reused != first {
		t.Fatal("same config root did not reuse suspended list")
	}

	second, err := cache.load(secondRoot)
	if err != nil {
		t.Fatalf("load second suspended list: %v", err)
	}
	if second == first || second.SuspendedUntil != 200 {
		t.Fatal("new config root reused stale suspended list")
	}

	absent, err := cache.load(nil)
	if err != nil || absent != nil {
		t.Fatalf("nil config root = (%v, %v), want (nil, nil)", absent, err)
	}
}

func suspendedAddressConfigRoot(t *testing.T, until uint32) *cell.Cell {
	t.Helper()

	list, err := tlb.ToCell(&tlb.SuspendedAddressList{SuspendedUntil: until})
	if err != nil {
		t.Fatalf("serialize suspended list: %v", err)
	}
	dict := cell.NewDict(32)
	key := cell.BeginCell().MustStoreUInt(uint64(tlb.ConfigParamSuspendedAddressList), 32).EndCell()
	if err = dict.SetBuilder(key, cell.BeginCell().MustStoreRef(list)); err != nil {
		t.Fatalf("store suspended list config: %v", err)
	}
	return dict.AsCell()
}
