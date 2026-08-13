package collator

import (
	"bytes"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestValidateOracleBridgeParameter(t *testing.T) {
	oracles := cell.NewDict(256)
	if err := oracles.Set(
		cell.BeginCell().MustStoreUInt(1, 256).EndCell(),
		cell.BeginCell().MustStoreUInt(2, 256).EndCell(),
	); err != nil {
		t.Fatal(err)
	}
	parameter := cell.BeginCell().
		MustStoreUInt(3, 256).
		MustStoreUInt(4, 256)
	if err := parameter.StoreDict(oracles); err != nil {
		t.Fatal(err)
	}
	parameter.MustStoreUInt(5, 256)
	raw := masterConfigTestRaw(t, 71, parameter.EndCell())
	if err := validateKnownConfigParameter(raw, 71); err != nil {
		t.Fatal(err)
	}

	malformed := cell.NewDict(256)
	if err := malformed.Set(
		cell.BeginCell().MustStoreUInt(1, 256).EndCell(),
		cell.BeginCell().MustStoreUInt(2, 255).EndCell(),
	); err != nil {
		t.Fatal(err)
	}
	parameter = cell.BeginCell().MustStoreUInt(3, 256).MustStoreUInt(4, 256)
	if err := parameter.StoreDict(malformed); err != nil {
		t.Fatal(err)
	}
	parameter.MustStoreUInt(5, 256)
	raw = masterConfigTestRaw(t, 71, parameter.EndCell())
	if err := validateKnownConfigParameter(raw, 71); err == nil {
		t.Fatal("malformed oracle value was accepted")
	}
}

func TestValidateJettonBridgeParameters(t *testing.T) {
	oracles := cell.NewDict(256)
	v0 := cell.BeginCell().
		MustStoreUInt(0, 8).
		MustStoreUInt(1, 256).
		MustStoreUInt(2, 256)
	if err := v0.StoreDict(oracles); err != nil {
		t.Fatal(err)
	}
	v0.MustStoreUInt(3, 8).MustStoreCoins(4)
	if err := validateKnownConfigParameter(masterConfigTestRaw(t, 79, v0.EndCell()), 79); err != nil {
		t.Fatal(err)
	}

	prices := cell.BeginCell()
	for amount := uint64(1); amount <= 6; amount++ {
		prices.MustStoreCoins(amount)
	}
	v1 := cell.BeginCell().
		MustStoreUInt(1, 8).
		MustStoreUInt(1, 256).
		MustStoreUInt(2, 256)
	if err := v1.StoreDict(oracles); err != nil {
		t.Fatal(err)
	}
	v1.MustStoreUInt(3, 8).MustStoreRef(prices.EndCell()).MustStoreUInt(4, 256)
	if err := validateKnownConfigParameter(masterConfigTestRaw(t, 81, v1.EndCell()), 81); err != nil {
		t.Fatal(err)
	}

	unknown := cell.BeginCell().MustStoreUInt(2, 8).MustStoreSlice(bytes.Repeat([]byte{0}, 64), 512)
	if err := unknown.StoreDict(oracles); err != nil {
		t.Fatal(err)
	}
	unknown.MustStoreUInt(0, 8)
	if err := validateKnownConfigParameter(masterConfigTestRaw(t, 82, unknown.EndCell()), 82); err == nil {
		t.Fatal("unknown jetton bridge version was accepted")
	}
}

func TestValidateKnownConfigParameterRejectsUnknownPositive(t *testing.T) {
	raw := masterConfigTestRaw(t, 70, cell.BeginCell().EndCell())
	if err := validateKnownConfigParameter(raw, 70); err == nil {
		t.Fatal("unknown positive parameter was accepted")
	}
}

// TestValidateKnownConfigParameterAcceptedSet pins the exact set of positive
// parameter ids the switch recognises. An id missing from it stops the node:
// validateMasterConfigData runs over the CURRENT config, so one unrecognised
// live parameter fails every masterchain block, both collated and validated.
// The reference set is ConfigParam::get_tag of ton c7da81d4 (2023-03-23) —
// {0..4, 6..18, 20..25, 28,29, 31..37, 39,40, 43,44, 71,72,73, 79,80,81} —
// widened by the ids that appeared upstream after that checkout and are already
// carried by mainnet: 5, 19, 30, 45, 82. Do not narrow it back to the pinned
// tree's set.
func TestValidateKnownConfigParameterAcceptedSet(t *testing.T) {
	want := map[uint32]struct{}{}
	for _, id := range []uint32{
		1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19,
		20, 21, 22, 23, 24, 25, 28, 29, 30, 31, 32, 33, 34, 35, 36, 37, 39, 40,
		43, 44, 45, 71, 72, 73, 79, 80, 81, 82,
	} {
		want[id] = struct{}{}
	}

	// Parameter 0 is handled by validateMasterConfigData itself and never
	// reaches the switch, so it is deliberately outside the pinned set here.
	for id := uint32(1); id <= 100; id++ {
		raw := masterConfigTestRaw(t, id, cell.BeginCell().EndCell())
		err := validateKnownConfigParameter(raw, id)
		// Every branch but the default one either accepts the parameter or
		// fails while decoding it; only an unlisted id reports the switch miss.
		known := err == nil || !strings.Contains(err.Error(), "unsupported positive parameter")
		if _, expected := want[id]; known != expected {
			t.Fatalf("parameter %d: known=%v, want %v (err %v)", id, known, expected, err)
		}
	}
}

func masterConfigTestRaw(t *testing.T, id uint32, parameter *cell.Cell) tlb.BlockchainConfig {
	t.Helper()
	dict := cell.NewDict(32)
	value := cell.BeginCell().MustStoreRef(parameter).EndCell()
	if err := dict.SetIntKey(new(big.Int).SetUint64(uint64(id)), value); err != nil {
		t.Fatal(err)
	}
	return tlb.BlockchainConfig{Root: dict.AsCell()}
}
