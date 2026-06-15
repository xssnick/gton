package blockproof

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestCurrentValidatorsForBlockPrefersTemporaryValidators(t *testing.T) {
	currentSet := testValidatorSetConfigCell(t, 0x11, 100)
	temporarySet := testValidatorSetConfigCell(t, 0x22, 200)
	cfg := testValidatorBlockchainConfig(t, map[uint32]*cell.Cell{
		tlb.ConfigParamCatchainConfig:        testCatchainConfigCell(),
		tlb.ConfigParamCurrentValidators:     currentSet,
		tlb.ConfigParamCurrentTempValidators: temporarySet,
	})

	block := testBlockID(-1, 1)
	validators, err := CurrentValidatorsForBlock(cfg, &block, 7)
	if err != nil {
		t.Fatalf("current validators: %v", err)
	}

	testRequireValidator(t, validators, 0x22, 200)
}

func TestCurrentValidatorsForBlockFallsBackToCurrentValidators(t *testing.T) {
	currentSet := testValidatorSetConfigCell(t, 0x11, 100)
	cfg := testValidatorBlockchainConfig(t, map[uint32]*cell.Cell{
		tlb.ConfigParamCatchainConfig:    testCatchainConfigCell(),
		tlb.ConfigParamCurrentValidators: currentSet,
	})

	block := testBlockID(-1, 1)
	validators, err := CurrentValidatorsForBlock(cfg, &block, 7)
	if err != nil {
		t.Fatalf("current validators: %v", err)
	}

	testRequireValidator(t, validators, 0x11, 100)
}

func TestCurrentValidatorsForBlockLoadsLegacyValidatorSet(t *testing.T) {
	currentSet := testLegacyValidatorSetConfigCell(t, 0x33, 300)
	cfg := testValidatorBlockchainConfig(t, map[uint32]*cell.Cell{
		tlb.ConfigParamCatchainConfig:    testCatchainConfigCell(),
		tlb.ConfigParamCurrentValidators: currentSet,
	})

	block := testBlockID(-1, 1)
	validators, err := CurrentValidatorsForBlock(cfg, &block, 7)
	if err != nil {
		t.Fatalf("current validators: %v", err)
	}

	testRequireValidator(t, validators, 0x33, 300)
}

func TestPrevValidatorsForBlockPrefersTemporaryValidators(t *testing.T) {
	prevSet := testValidatorSetConfigCell(t, 0x11, 100)
	temporarySet := testValidatorSetConfigCell(t, 0x22, 200)
	cfg := testValidatorBlockchainConfig(t, map[uint32]*cell.Cell{
		tlb.ConfigParamCatchainConfig:     testCatchainConfigCell(),
		tlb.ConfigParamPrevValidators:     prevSet,
		tlb.ConfigParamPrevTempValidators: temporarySet,
	})

	block := testBlockID(-1, 1)
	validators, err := PrevValidatorsForBlock(cfg, &block, 7)
	if err != nil {
		t.Fatalf("previous validators: %v", err)
	}

	testRequireValidator(t, validators, 0x22, 200)
}

func TestPrevValidatorsForBlockFallsBackToPreviousValidators(t *testing.T) {
	prevSet := testValidatorSetConfigCell(t, 0x11, 100)
	cfg := testValidatorBlockchainConfig(t, map[uint32]*cell.Cell{
		tlb.ConfigParamCatchainConfig: testCatchainConfigCell(),
		tlb.ConfigParamPrevValidators: prevSet,
	})

	block := testBlockID(-1, 1)
	validators, err := PrevValidatorsForBlock(cfg, &block, 7)
	if err != nil {
		t.Fatalf("previous validators: %v", err)
	}

	testRequireValidator(t, validators, 0x11, 100)
}

func TestNextValidatorsForBlockPrefersTemporaryValidators(t *testing.T) {
	nextSet := testValidatorSetConfigCell(t, 0x11, 100)
	temporarySet := testValidatorSetConfigCell(t, 0x22, 200)
	cfg := testValidatorBlockchainConfig(t, map[uint32]*cell.Cell{
		tlb.ConfigParamCatchainConfig:     testCatchainConfigCell(),
		tlb.ConfigParamNextValidators:     nextSet,
		tlb.ConfigParamNextTempValidators: temporarySet,
	})

	block := testBlockID(-1, 1)
	validators, err := NextValidatorsForBlock(cfg, &block, 7)
	if err != nil {
		t.Fatalf("next validators: %v", err)
	}

	testRequireValidator(t, validators, 0x22, 200)
}

func TestNextValidatorsForBlockFallsBackToNextValidators(t *testing.T) {
	nextSet := testValidatorSetConfigCell(t, 0x11, 100)
	cfg := testValidatorBlockchainConfig(t, map[uint32]*cell.Cell{
		tlb.ConfigParamCatchainConfig: testCatchainConfigCell(),
		tlb.ConfigParamNextValidators: nextSet,
	})

	block := testBlockID(-1, 1)
	validators, err := NextValidatorsForBlock(cfg, &block, 7)
	if err != nil {
		t.Fatalf("next validators: %v", err)
	}

	testRequireValidator(t, validators, 0x11, 100)
}

func testRequireValidator(t *testing.T, validators []*tlb.ValidatorAddr, keyByte byte, weight uint64) {
	t.Helper()

	if len(validators) != 1 {
		t.Fatalf("got %d validators, want 1", len(validators))
	}
	if validators[0].Weight != weight {
		t.Fatalf("validator weight = %d, want %d", validators[0].Weight, weight)
	}
	if got := validators[0].PublicKey.Key[0]; got != keyByte {
		t.Fatalf("validator key starts with 0x%02x, want 0x%02x", got, keyByte)
	}
}

func testValidatorBlockchainConfig(t *testing.T, params map[uint32]*cell.Cell) *tlb.BlockchainConfig {
	t.Helper()

	dict := cell.NewDict(32)
	for id, value := range params {
		wrapped := cell.BeginCell().MustStoreRef(value).EndCell()
		if err := dict.SetIntKey(new(big.Int).SetUint64(uint64(id)), wrapped); err != nil {
			t.Fatalf("set config param %d: %v", id, err)
		}
	}
	return &tlb.BlockchainConfig{Root: dict.AsCell()}
}

func testValidatorSetConfigCell(t *testing.T, keyByte byte, weight uint64) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(16)
	validator := testValidatorAddrCell(keyByte, weight)
	if err := dict.SetIntKey(big.NewInt(0), validator); err != nil {
		t.Fatalf("set validator: %v", err)
	}

	set, err := tlb.ToCell(&tlb.ValidatorSetExt{
		UTimeSince:  1,
		UTimeUntil:  2,
		Total:       1,
		Main:        1,
		TotalWeight: weight,
		List:        dict,
	})
	if err != nil {
		t.Fatalf("build validator set: %v", err)
	}
	return set
}

func testLegacyValidatorSetConfigCell(t *testing.T, keyByte byte, weight uint64) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(16)
	validator := testValidatorAddrCell(keyByte, weight)
	if err := dict.SetIntKey(big.NewInt(0), validator); err != nil {
		t.Fatalf("set validator: %v", err)
	}

	return cell.BeginCell().
		MustStoreUInt(0x11, 8).
		MustStoreUInt(1, 32).
		MustStoreUInt(2, 32).
		MustStoreUInt(1, 16).
		MustStoreUInt(1, 16).
		MustStoreBuilder(dict.AsCell().ToBuilder()).
		EndCell()
}

func testValidatorAddrCell(keyByte byte, weight uint64) *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0x73, 8).
		MustStoreUInt(0x8e81278a, 32).
		MustStoreSlice(bytes.Repeat([]byte{keyByte}, 32), 256).
		MustStoreUInt(weight, 64).
		MustStoreSlice(bytes.Repeat([]byte{0xaa}, 32), 256).
		EndCell()
}

func testCatchainConfigCell() *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0xc1, 8).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(1, 32).
		EndCell()
}
