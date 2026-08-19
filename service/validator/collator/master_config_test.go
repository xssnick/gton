package collator

import (
	"bytes"
	"math/big"
	"slices"
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

func TestValidateValidatorRegistryConfigParameter(t *testing.T) {
	for _, withNewCodeHash := range []bool{false, true} {
		parameter := cell.BeginCell().
			MustStoreUInt(validatorRegistryConfigConstructor, 32).
			MustStoreUInt(1, 256).
			MustStoreUInt(10, 32).
			MustStoreBoolBit(withNewCodeHash)
		if withNewCodeHash {
			parameter.MustStoreUInt(2, 256)
		}

		raw := masterConfigTestRaw(t, 46, parameter.EndCell())
		if err := validateKnownConfigParameter(raw, 46); err != nil {
			t.Fatalf("new_code_hash=%v: %v", withNewCodeHash, err)
		}
	}
}

func TestValidateValidatorRegistryConfigParameterRejectsMalformed(t *testing.T) {
	tests := map[string]*cell.Cell{
		"constructor": cell.BeginCell().
			MustStoreUInt(0, 32).
			MustStoreUInt(1, 256).
			MustStoreUInt(10, 32).
			MustStoreBoolBit(false).
			EndCell(),
		"missing code hash": cell.BeginCell().
			MustStoreUInt(validatorRegistryConfigConstructor, 32).
			MustStoreUInt(1, 256).
			MustStoreUInt(10, 32).
			MustStoreBoolBit(true).
			EndCell(),
		"trailing data": cell.BeginCell().
			MustStoreUInt(validatorRegistryConfigConstructor, 32).
			MustStoreUInt(1, 256).
			MustStoreUInt(10, 32).
			MustStoreBoolBit(false).
			MustStoreBoolBit(true).
			EndCell(),
	}

	for name, parameter := range tests {
		t.Run(name, func(t *testing.T) {
			raw := masterConfigTestRaw(t, 46, parameter)
			if err := validateKnownConfigParameter(raw, 46); err == nil {
				t.Fatal("malformed validator registry parameter was accepted")
			}
		})
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
		43, 44, 45, 46, 71, 72, 73, 79, 80, 81, 82,
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

// TestDeriveMasterConfigTransitionReuseMatchesFreshParse is the differential
// guard for reusing the predecessor's prepared configuration.
//
// The outputs must match because they feed the candidate. The read sets must
// match too, and that is the less obvious half: on the collation path this
// function runs under the block's read set, the Merkle update descends only
// through cells that set recorded, and the collated-size estimate answers
// membership out of the same record — so reuse is sound exactly while the
// replayed footprint is the set the skipped parses would have read.
//
// This half only bites while a wrong footprint is rejected, which is what
// TestMasterConfigFootprintMutationsAreDetected keeps true.
func TestDeriveMasterConfigTransitionReuseMatchesFreshParse(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)

	fresh, freshReads := masterConfigTransitionReads(t, fixture, nil, nil)
	reused, reusedReads := masterConfigTransitionReads(t, fixture,
		fixture.request.Config, fixture.request.Groups.Config)

	// Without this the test would pass vacuously if the gate ever stopped firing.
	if reused.config != fixture.request.Config || reused.groups != fixture.request.Groups.Config {
		t.Fatal("the reuse gate did not fire for an unchanged configuration")
	}
	if fresh.config == fixture.request.Config {
		t.Fatal("the fresh branch returned the predecessor's prepared config")
	}

	if fresh.keyBlock != reused.keyBlock {
		t.Fatalf("key block flag = %v reused, %v fresh", reused.keyBlock, fresh.keyBlock)
	}
	if !bytes.Equal(fresh.params.ConfigAddr, reused.params.ConfigAddr) ||
		fresh.params.Config.Params.AsCell().HashKey() != reused.params.Config.Params.AsCell().HashKey() {
		t.Fatal("reused transition installed different configuration parameters")
	}
	if fresh.groups.Catchain != reused.groups.Catchain {
		t.Fatalf("catchain config = %+v reused, %+v fresh", reused.groups.Catchain, fresh.groups.Catchain)
	}
	if len(fresh.config.workchains) != len(reused.config.workchains) {
		t.Fatalf("workchains = %d reused, %d fresh", len(reused.config.workchains), len(fresh.config.workchains))
	}

	// Set equality, not a count: the recorded set is what the update descends
	// through and what the size estimate resolves membership against, so any
	// difference here is a difference in the block the collator produces.
	if !slices.Equal(freshReads, reusedReads) {
		t.Fatalf("replaying the footprint recorded %d cells and parsing recorded %d; "+
			"the two must be the same set or the two paths emit different blocks",
			len(reusedReads), len(freshReads))
	}

	// Stripping the footprint must fall back to parsing rather than reuse. This
	// is what keeps the equality above from passing vacuously the day capture
	// silently starts returning nil, and it preserves the original measurement:
	// the parses really do read thousands of cells nothing else on this path
	// touches.
	stripped := withFootprint(fixture.request.Config, nil)
	fallback, fallbackReads := masterConfigTransitionReads(t, fixture, stripped, fixture.request.Groups.Config)
	if fallback.config == stripped {
		t.Fatal("a configuration without a footprint was reused on the recording path")
	}
	if !slices.Equal(freshReads, fallbackReads) {
		t.Fatal("the footprint-less fallback did not read what a fresh parse reads")
	}
	t.Logf("configuration parse records %d cells, of which %d are the config footprint",
		len(freshReads), len(fixture.request.Config.footprint.cells))
}
