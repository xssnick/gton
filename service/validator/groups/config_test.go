package groups

import (
	"errors"
	"fmt"
	"math"
	"math/big"
	"reflect"
	"strconv"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

type testValidatorWire struct {
	index    uint16
	key      [32]byte
	adnl     [32]byte
	weight   uint64
	withADNL bool
	trailing bool
}

type testConfigParamsCase struct {
	name   string
	params map[uint32]*cell.Cell
}

type testInvalidValidatorSetCase struct {
	name string
	set  *cell.Cell
	want string
}

type testConsensusLimitsCase struct {
	name         string
	constructor  uint64
	wantBlock    uint32
	wantCollated uint32
}

func TestParseConfigDefaultsAndValidatorSetFallbacks(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{
		{index: 0, key: groupTestBytes(1), weight: 10},
		{index: 1, key: groupTestBytes(2), adnl: groupTestBytes(70), weight: 20, withADNL: true},
	}, true, 30)

	config, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: current,
	}))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}

	wantCatchain := CatchainConfig{
		MasterchainLifetime:     200,
		ShardLifetime:           200,
		ShardValidatorsLifetime: 3000,
		ShardValidators:         7,
	}
	if config.Catchain != wantCatchain {
		t.Fatalf("default catchain config = %+v, want %+v", config.Catchain, wantCatchain)
	}
	if config.NewConsensus.Masterchain != nil || config.NewConsensus.Shard != nil {
		t.Fatalf("absent config 30 decoded as present: %+v", config.NewConsensus)
	}
	if config.MaxBlockSize != defaultCandidateSizeLimit || config.MaxCollatedDataSize != defaultCandidateSizeLimit {
		t.Fatalf("default candidate limits = %d/%d", config.MaxBlockSize, config.MaxCollatedDataSize)
	}
	if config.NextValidators != nil {
		t.Fatal("next validator set reported present")
	}
	if got := config.ActiveValidators.Validators[0].PublicKey; got != groupTestBytes(1) {
		t.Fatalf("active validator key = %x", got)
	}
	if config.ActiveValidators.Validators[0].ADNL != ([32]byte{}) {
		t.Fatal("legacy validator descriptor did not preserve zero ADNL")
	}
	if members := config.PersistentOverlayMembers(); len(members) != 2 {
		t.Fatalf("cached persistent overlay member count = %d, want 2", len(members))
	}
	if config.ActiveValidators.TotalWeight != 30 || len(config.ActiveValidators.Validators) != 2 {
		t.Fatalf("current validator set = %+v", config.ActiveValidators)
	}
}

func TestParseConfigConsensusCandidateLimits(t *testing.T) {
	tests := []testConsensusLimitsCase{
		{name: "v4", constructor: 0xd9, wantBlock: 104, wantCollated: 204},
	}
	current := buildTestValidatorSet(t, []testValidatorWire{{
		index:  0,
		key:    groupTestBytes(1),
		weight: 1,
	}}, false, 0)

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			limits := buildTestConsensusLimits(test.constructor, test.wantBlock, test.wantCollated)
			config, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
				configParamConsensus:         limits,
				configParamCurrentValidators: current,
			}))
			if err != nil {
				t.Fatal(err)
			}
			if config.MaxBlockSize != test.wantBlock || config.MaxCollatedDataSize != test.wantCollated {
				t.Fatalf("candidate limits = %d/%d, want %d/%d",
					config.MaxBlockSize,
					config.MaxCollatedDataSize,
					test.wantBlock,
					test.wantCollated,
				)
			}
		})
	}
}

// Parameter 29 survives only as the carrier of the two candidate size limits;
// the catchain-era constructors that preceded consensus_config_v4 are refused
// outright rather than parsed for their limits, so a chain this node cannot
// validate fails at the config rather than somewhere further in.
func TestParseConfigRejectsPreV4ConsensusConstructors(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(1), weight: 1}}, false, 0)
	for _, constructor := range []uint64{0xd6, 0xd7, 0xd8} {
		t.Run(fmt.Sprintf("%02x", constructor), func(t *testing.T) {
			_, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
				configParamConsensus:         buildTestConsensusLimits(constructor, 101, 201),
				configParamCurrentValidators: current,
			}))
			want := fmt.Sprintf("unsupported constructor #%02x", constructor)
			if err == nil || !strings.Contains(err.Error(), want) {
				t.Fatalf("ParseConfig error = %v, want substring %q", err, want)
			}
		})
	}
}

func TestParseConfigRejectsMalformedConsensusLimits(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(1), weight: 1}}, false, 0)
	invalidFlags := cell.BeginCell().
		MustStoreUInt(0xd9, 8).
		MustStoreUInt(1, 6).
		EndCell()

	_, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamConsensus:         invalidFlags,
		configParamCurrentValidators: current,
	}))
	if err == nil || !strings.Contains(err.Error(), "flags must be zero") {
		t.Fatalf("invalid consensus flags error = %v", err)
	}

	zeroRoundCell := cell.BeginCell().
		MustStoreUInt(0xd9, 8).
		MustStoreUInt(0, 6).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		MustStoreUInt(0, 8).
		EndCell()
	_, err = ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamConsensus:         zeroRoundCell,
		configParamCurrentValidators: current,
	}))
	if err == nil || !strings.Contains(err.Error(), "round_candidates must be positive") {
		t.Fatalf("zero round candidates error = %v", err)
	}
}

func buildTestConsensusLimits(constructor uint64, maxBlockSize, maxCollatedDataSize uint32) *cell.Cell {
	b := cell.BeginCell().MustStoreUInt(constructor, 8)
	switch constructor {
	case 0xd6:
		b.MustStoreUInt(1, 32)
	case 0xd7, 0xd8:
		b.MustStoreUInt(0, 7).MustStoreBoolBit(false).MustStoreUInt(1, 8)
	case 0xd9:
		b.MustStoreUInt(0, 6).MustStoreBoolBit(false).MustStoreBoolBit(false).MustStoreUInt(1, 8)
	}

	for range 5 {
		b.MustStoreUInt(0, 32)
	}
	b.MustStoreUInt(uint64(maxBlockSize), 32)
	b.MustStoreUInt(uint64(maxCollatedDataSize), 32)
	switch constructor {
	case 0xd8:
		b.MustStoreUInt(3, 16)
	case 0xd9:
		b.MustStoreUInt(3, 16).MustStoreUInt(0, 32)
	}

	return b.EndCell()
}

func TestParseConfigPrefersTemporaryValidatorSets(t *testing.T) {
	validatorSet := func(seed byte) *cell.Cell {
		return buildTestValidatorSet(t, []testValidatorWire{{
			index: 0, key: groupTestBytes(seed), adnl: groupTestBytes(seed + 40), weight: 1, withADNL: true,
		}}, true, 1)
	}

	config, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamPreviousValidators: validatorSet(5),
		configParamPreviousTemporary:  validatorSet(6),
		configParamCurrentValidators:  validatorSet(1),
		configParamCurrentTemporary:   validatorSet(2),
		configParamNextValidators:     validatorSet(3),
		configParamNextTemporary:      validatorSet(4),
	}))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}

	if got := config.ActiveValidators.Validators[0].PublicKey; got != groupTestBytes(2) {
		t.Fatalf("active set key = %x, want temporary current", got)
	}
	if got := config.NextValidators.Validators[0].PublicKey; got != groupTestBytes(4) {
		t.Fatalf("next set key = %x, want temporary next", got)
	}

	// The persistent overlay union is built from parameters 32, 34, and 36,
	// so parsing every persistent set stays observable through it.
	members := config.PersistentOverlayMembers()
	want := map[[32]byte]struct{}{
		groupTestBytes(5 + 40): {},
		groupTestBytes(1 + 40): {},
		groupTestBytes(3 + 40): {},
	}
	if len(members) != len(want) {
		t.Fatalf("persistent overlay member count = %d, want %d", len(members), len(want))
	}
	for _, member := range members {
		if _, expected := want[member.ADNL]; !expected {
			t.Fatalf("unexpected persistent overlay member %x", member)
		}
	}
}

func TestParseConfigRequiresCurrentValidatorSet(t *testing.T) {
	_, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamPreviousValidators: buildTestValidatorSet(t, []testValidatorWire{{
			index: 0, key: groupTestBytes(1), weight: 1,
		}}, false, 0),
	}))
	if !errors.Is(err, ErrNotFound) {
		t.Fatalf("missing current set error = %v, want ErrNotFound", err)
	}
}

func TestParseConfigUsesPersistentNextFallback(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(1), weight: 1}}, false, 0)
	next := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(2), weight: 1}}, false, 0)

	config, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: current,
		configParamNextValidators:    next,
	}))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	if got := config.NextValidators.Validators[0].PublicKey; got != groupTestBytes(2) {
		t.Fatalf("persistent next fallback = %x", got)
	}
}

func TestParseConfig28Strict(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(1), weight: 1}}, false, 0)
	valid := cell.BeginCell().
		MustStoreUInt(0xc2, 8).
		MustStoreUInt(0, 7).
		MustStoreBoolBit(true).
		MustStoreUInt(101, 32).
		MustStoreUInt(102, 32).
		MustStoreUInt(103, 32).
		MustStoreUInt(8, 32).
		EndCell()

	config, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCatchain:          valid,
		configParamCurrentValidators: current,
	}))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}
	want := CatchainConfig{
		MasterchainLifetime:     101,
		ShardLifetime:           102,
		ShardValidatorsLifetime: 103,
		ShardValidators:         8,
		ShuffleMasterchain:      true,
	}
	if config.Catchain != want {
		t.Fatalf("catchain config = %+v, want %+v", config.Catchain, want)
	}

	invalidFlags := cell.BeginCell().
		MustStoreUInt(0xc2, 8).
		MustStoreUInt(1, 7).
		MustStoreBoolBit(false).
		MustStoreUInt(1, 32).
		MustStoreUInt(1, 32).
		MustStoreUInt(1, 32).
		MustStoreUInt(1, 32).
		EndCell()
	zeroLifetime := cell.BeginCell().
		MustStoreUInt(0xc1, 8).
		MustStoreUInt(0, 32).
		MustStoreUInt(1, 32).
		MustStoreUInt(1, 32).
		MustStoreUInt(1, 32).
		EndCell()
	for name, parameter := range map[string]*cell.Cell{
		"nonzero flags": invalidFlags,
		"zero lifetime": zeroLifetime,
	} {
		t.Run(name, func(t *testing.T) {
			_, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
				configParamCatchain:          parameter,
				configParamCurrentValidators: current,
			}))
			if err == nil {
				t.Fatal("ParseConfig accepted malformed config 28")
			}
		})
	}
}

func TestParseConfigRejectsMalformedOptionalValidatorSet(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(1), weight: 1}}, false, 0)
	malformed := cell.BeginCell().MustStoreUInt(0xff, 8).EndCell()

	for _, parameter := range []uint32{
		configParamPreviousValidators,
		configParamCurrentTemporary,
		configParamNextValidators,
		configParamNextTemporary,
	} {
		t.Run(strconv.FormatUint(uint64(parameter), 10), func(t *testing.T) {
			_, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
				configParamCurrentValidators: current,
				parameter:                    malformed,
			}))
			if err == nil || !strings.Contains(err.Error(), "config parameter") {
				t.Fatalf("malformed optional parameter %d error = %v", parameter, err)
			}
		})
	}
}

func TestParseConfigIgnoresPreviousTemporaryValidatorSet(t *testing.T) {
	current := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(1), weight: 1}}, false, 0)
	malformed := cell.BeginCell().MustStoreUInt(0xff, 8).EndCell()

	// Nothing reads parameter 33, and the C++ node never looks it up, so an odd
	// value there must not stop this node from applying, collating or
	// validating masterchain blocks.
	config, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCurrentValidators: current,
		configParamPreviousTemporary: malformed,
	}))
	if err != nil {
		t.Fatalf("parse config with malformed parameter %d: %v", configParamPreviousTemporary, err)
	}
	if got := config.ActiveValidators.Validators[0].PublicKey; got != groupTestBytes(1) {
		t.Fatalf("active set key = %x, want current", got)
	}
}

func TestParseConfigRejectsTrailingData(t *testing.T) {
	validCurrent := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(1), weight: 1}}, false, 0)
	catchainWithTrailingBit := cell.BeginCell().
		MustStoreUInt(0xc1, 8).
		MustStoreUInt(200, 32).
		MustStoreUInt(200, 32).
		MustStoreUInt(3000, 32).
		MustStoreUInt(7, 32).
		MustStoreUInt(1, 1).
		EndCell()
	descriptorWithTrailingBit := buildTestValidatorSet(t, []testValidatorWire{{
		index: 0, key: groupTestBytes(1), weight: 1, trailing: true,
	}}, true, 1)

	tests := []testConfigParamsCase{
		{
			name: "catchain value",
			params: map[uint32]*cell.Cell{
				configParamCatchain:          catchainWithTrailingBit,
				configParamCurrentValidators: validCurrent,
			},
		},
		{
			name: "validator descriptor",
			params: map[uint32]*cell.Cell{
				configParamCurrentValidators: descriptorWithTrailingBit,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := ParseConfig(buildTestConfig(t, test.params)); err == nil || !strings.Contains(err.Error(), "trailing") {
				t.Fatalf("ParseConfig error = %v, want trailing-data rejection", err)
			}
		})
	}

	root := buildTestConfigWithMalformedWrapper(t, configParamCurrentValidators, validCurrent)
	if _, err := ParseConfig(root); err == nil || !strings.Contains(err.Error(), "wrapper") {
		t.Fatalf("malformed wrapper error = %v", err)
	}

	validDictionary := buildTestConfig(t, map[uint32]*cell.Cell{
		configParamCatchain:          catchainWithTrailingBit,
		configParamCurrentValidators: validCurrent,
	})
	malformedFork := validDictionary.MustBeginParse().ToBuilder().MustStoreUInt(0, 1).EndCell()
	if _, err := ParseConfig(malformedFork); err == nil || !strings.Contains(err.Error(), "dictionary fork") {
		t.Fatalf("malformed dictionary fork error = %v", err)
	}
}

func TestParseValidatorSetRejectsInvalidWeightsAndKeys(t *testing.T) {
	duplicateKey := groupTestBytes(9)
	tests := []testInvalidValidatorSetCase{
		{
			name: "zero weight",
			set: buildTestValidatorSet(t, []testValidatorWire{
				{index: 0, key: groupTestBytes(1), weight: 0},
			}, true, 1),
			want: "weight must be positive",
		},
		{
			name: "zero declared total",
			set: buildTestValidatorSet(t, []testValidatorWire{
				{index: 0, key: groupTestBytes(1), weight: 1},
			}, true, 0),
			want: "declared total weight must be positive",
		},
		{
			name: "declared mismatch",
			set: buildTestValidatorSet(t, []testValidatorWire{
				{index: 0, key: groupTestBytes(1), weight: 10},
			}, true, 11),
			want: "declared total weight",
		},
		{
			name: "total above protocol maximum",
			set: buildTestValidatorSet(t, []testValidatorWire{
				{index: 0, key: groupTestBytes(1), weight: maxTotalValidatorWeight},
				{index: 1, key: groupTestBytes(2), weight: 1},
			}, true, maxTotalValidatorWeight+1),
			want: "exceeds 2^61",
		},
		{
			name: "uint64 overflow",
			set: buildTestValidatorSet(t, []testValidatorWire{
				{index: 0, key: groupTestBytes(1), weight: math.MaxUint64},
				{index: 1, key: groupTestBytes(2), weight: 1},
			}, false, 0),
			want: "overflows uint64",
		},
		{
			name: "duplicate public key",
			set: buildTestValidatorSet(t, []testValidatorWire{
				{index: 0, key: duplicateKey, weight: 1},
				{index: 1, key: duplicateKey, adnl: groupTestBytes(80), weight: 1, withADNL: true},
			}, true, 2),
			want: "duplicates public key",
		},
		{
			name: "nonconsecutive dictionary index",
			set: buildTestValidatorSet(t, []testValidatorWire{
				{index: 0, key: groupTestBytes(1), weight: 1},
				{index: 2, key: groupTestBytes(2), weight: 1},
			}, true, 2),
			want: "dictionary index 2, want 1",
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			_, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
				configParamCurrentValidators: test.set,
			}))
			if err == nil || !strings.Contains(err.Error(), test.want) {
				t.Fatalf("ParseConfig error = %v, want substring %q", err, test.want)
			}
		})
	}
}

func TestParseConfig30SimplexVersions(t *testing.T) {
	noncritical := cell.NewDict(8)
	setTestDictionaryValue(t, noncritical, 1, cell.BeginCell().MustStoreUInt(42, 32).EndCell())
	setTestDictionaryValue(t, noncritical, 250, cell.BeginCell().MustStoreUInt(99, 32).EndCell())

	masterchain := cell.BeginCell().
		MustStoreUInt(0x22, 8).
		MustStoreUInt(0x55&0x1f, 5).
		MustStoreUInt(3, 2).
		MustStoreBoolBit(true).
		MustStoreUInt(4, 32).
		MustStoreDict(nil).
		EndCell()
	shard := cell.BeginCell().
		MustStoreUInt(0x22, 8).
		MustStoreUInt(0x15, 5).
		MustStoreUInt(3, 2).
		MustStoreBoolBit(true).
		MustStoreUInt(9, 32).
		MustStoreDict(noncritical).
		EndCell()
	config30 := cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreMaybeRef(masterchain).
		MustStoreMaybeRef(shard).
		EndCell()
	current := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(1), weight: 1}}, false, 0)

	config, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamNewConsensus:      config30,
		configParamCurrentValidators: current,
	}))
	if err != nil {
		t.Fatalf("parse config: %v", err)
	}

	if config.NewConsensus.Masterchain == nil || config.NewConsensus.Shard == nil {
		t.Fatalf("config 30 presence = %+v", config.NewConsensus)
	}
	if got := config.NewConsensus.Masterchain; got.Version != 2 || got.Flags != 0x55&0x1f ||
		got.ProtocolVersion != 3 || !got.UseQUIC || got.SlotsPerLeaderWindow != 4 {
		t.Fatalf("masterchain simplex config = %+v", got)
	}
	wantParams := []NoncriticalParam{{ID: 1, Value: 42}, {ID: 250, Value: 99}}
	gotShard := config.NewConsensus.Shard
	if gotShard.Version != 2 || gotShard.Flags != 0x15 || gotShard.ProtocolVersion != 3 ||
		!gotShard.UseQUIC || gotShard.SlotsPerLeaderWindow != 9 {
		t.Fatalf("shard simplex v2 config = %+v", gotShard)
	}
	if len(gotShard.NoncriticalParams) != len(wantParams) {
		t.Fatalf("noncritical params = %+v", gotShard.NoncriticalParams)
	}
	for i := range wantParams {
		if gotShard.NoncriticalParams[i] != wantParams[i] {
			t.Fatalf("noncritical param %d = %+v, want %+v", i, gotShard.NoncriticalParams[i], wantParams[i])
		}
	}
}

// The pre-Delegated-v3 #21 constructor is not parsed at all: this node only
// joins simplex v2 with protocol version 3 or newer, so a #21 configuration
// must be refused where it is read rather than accepted and then declined by
// the supervisor, which left the failure to surface as an idle validator.
func TestParseConfig30RejectsLegacySimplexV1(t *testing.T) {
	legacy := cell.BeginCell().
		MustStoreUInt(0x21, 8).
		MustStoreUInt(0x55, 7).
		MustStoreBoolBit(true).
		MustStoreUInt(100, 32).
		MustStoreUInt(4, 32).
		MustStoreUInt(500, 32).
		MustStoreUInt(6, 32).
		EndCell()
	config30 := cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreMaybeRef(legacy).
		MustStoreMaybeRef(nil).
		EndCell()
	current := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(1), weight: 1}}, false, 0)

	_, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamNewConsensus:      config30,
		configParamCurrentValidators: current,
	}))
	if err == nil || !strings.Contains(err.Error(), "unsupported constructor #21") {
		t.Fatalf("ParseConfig error = %v, want unsupported constructor #21", err)
	}
}

func TestParseConfig30RejectsZeroSlots(t *testing.T) {
	invalid := cell.BeginCell().
		MustStoreUInt(0x22, 8).
		MustStoreUInt(0, 5).
		MustStoreUInt(0, 2).
		MustStoreBoolBit(false).
		MustStoreUInt(0, 32).
		MustStoreDict(nil).
		EndCell()
	config30 := cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreMaybeRef(invalid).
		MustStoreMaybeRef(nil).
		EndCell()
	current := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(1), weight: 1}}, false, 0)

	_, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamNewConsensus:      config30,
		configParamCurrentValidators: current,
	}))
	if err == nil || !strings.Contains(err.Error(), "slots_per_leader_window must be positive") {
		t.Fatalf("zero slots error = %v", err)
	}
}

func buildTestConfig(t testing.TB, params map[uint32]*cell.Cell) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(32)
	for parameter, parameterCell := range params {
		wrapper := cell.BeginCell().MustStoreRef(parameterCell).EndCell()
		setTestDictionaryValue(t, dict, uint64(parameter), wrapper)
	}

	return dict.AsCell()
}

func buildTestConfigWithMalformedWrapper(t *testing.T, parameter uint32, parameterCell *cell.Cell) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(32)
	wrapper := cell.BeginCell().MustStoreUInt(1, 1).MustStoreRef(parameterCell).EndCell()
	setTestDictionaryValue(t, dict, uint64(parameter), wrapper)

	return dict.AsCell()
}

func buildTestValidatorSet(t testing.TB, validators []testValidatorWire, extended bool, declaredWeight uint64) *cell.Cell {
	t.Helper()

	return buildTestValidatorSetSince(t, validators, extended, declaredWeight, 100)
}

// buildTestValidatorSetSince exposes utime_since, which decides whether a next
// set is already eligible for the following catchain session.
func buildTestValidatorSetSince(
	t testing.TB,
	validators []testValidatorWire,
	extended bool,
	declaredWeight uint64,
	since uint32,
) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(16)
	for _, validator := range validators {
		constructor := validatorConstructor
		if validator.withADNL {
			constructor = validatorAddressConstructor
		}
		descriptor := cell.BeginCell().MustStoreUInt(constructor, 8)
		storeTestPublicKey(descriptor, validator.key)
		descriptor.MustStoreUInt(validator.weight, 64)
		if validator.withADNL {
			descriptor.MustStoreSlice(validator.adnl[:], 256)
		}
		if validator.trailing {
			descriptor.MustStoreUInt(1, 1)
		}
		setTestDictionaryValue(t, dict, uint64(validator.index), descriptor.EndCell())
	}

	constructor := validatorSetConstructor
	if extended {
		constructor = validatorSetExtendedConstructor
	}
	b := cell.BeginCell().
		MustStoreUInt(constructor, 8).
		MustStoreUInt(uint64(since), 32).
		MustStoreUInt(uint64(since)+100, 32).
		MustStoreUInt(uint64(len(validators)), 16).
		MustStoreUInt(1, 16)
	if extended {
		b.MustStoreUInt(declaredWeight, 64).MustStoreDict(dict)
	} else {
		b.MustStoreBuilder(dict.AsCell().MustBeginParse().ToBuilder())
	}

	return b.EndCell()
}

func storeTestPublicKey(b *cell.Builder, publicKey [32]byte) {
	b.MustStoreUInt(ed25519PublicKeyConstructor, 32).MustStoreSlice(publicKey[:], 256)
}

func storeTestSimpleSignature(b *cell.Builder, seed byte) {
	r := groupTestBytes(seed)
	s := groupTestBytes(seed + 1)
	b.MustStoreUInt(5, 4).MustStoreSlice(r[:], 256).MustStoreSlice(s[:], 256)
}

func setTestDictionaryValue(t testing.TB, dict *cell.Dictionary, key uint64, value *cell.Cell) {
	t.Helper()

	if err := dict.SetIntKey(new(big.Int).SetUint64(key), value); err != nil {
		t.Fatalf("set dictionary key %d: %v", key, err)
	}
}

func setTestDictionaryBytesKey(t *testing.T, dict *cell.Dictionary, key [32]byte, value *cell.Cell) {
	t.Helper()

	keyCell := cell.BeginCell().MustStoreSlice(key[:], 256).EndCell()
	if err := dict.Set(keyCell, value); err != nil {
		t.Fatalf("set dictionary key %x: %v", key, err)
	}
}

func groupTestBytes(seed byte) [32]byte {
	var result [32]byte
	for i := range result {
		result[i] = seed + byte(i)
	}

	return result
}

// TestSimplexProtocolCopiesEveryConfigField is the guard Protocol needs: it is
// read by two independent session projections which cannot see each other, so a
// field added to SimplexProtocol and forgotten in Protocol becomes a zero value
// on both sides rather than a compile error.
func TestSimplexProtocolCopiesEveryConfigField(t *testing.T) {
	config := SimplexConfig{
		Version:              2,
		Flags:                5,
		ProtocolVersion:      4,
		UseQUIC:              true,
		SlotsPerLeaderWindow: 6,
	}
	projected := reflect.ValueOf(config.Protocol())
	source := reflect.ValueOf(config)

	for i := range projected.NumField() {
		field := projected.Type().Field(i)
		origin := source.FieldByName(field.Name)
		if !origin.IsValid() {
			t.Fatalf("SimplexProtocol.%s has no SimplexConfig field of the same name", field.Name)
		}
		if projected.Field(i).IsZero() {
			t.Fatalf("the fixture leaves %s zero, so its copy is not actually observed", field.Name)
		}
		if projected.Field(i).Interface() != origin.Interface() {
			t.Fatalf("Protocol().%s = %v, want %v", field.Name, projected.Field(i), origin)
		}
	}
}

func TestSupportedProtocolAdmitsV2FromProtocolVersionThree(t *testing.T) {
	tests := []struct {
		version         uint8
		protocolVersion uint8
		want            bool
	}{
		{version: 2, protocolVersion: 3, want: true},
		{version: 2, protocolVersion: 4, want: true},
		{version: 2, protocolVersion: 2, want: false},
		{version: 2, protocolVersion: 0, want: false},
		{version: 1, protocolVersion: 3, want: false},
		{version: 3, protocolVersion: 3, want: false},
	}
	for _, test := range tests {
		config := SimplexConfig{Version: test.version, ProtocolVersion: test.protocolVersion}
		if got := config.SupportedProtocol(); got != test.want {
			t.Fatalf("SupportedProtocol(v%d, protocol %d) = %v, want %v",
				test.version, test.protocolVersion, got, test.want)
		}
	}
}
