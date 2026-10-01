package groups

import (
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestConfig30ZeroFirstBlockGraceIsValid(t *testing.T) {
	// Explicit zero values in config 30 remove first-block grace in C++;
	// they must survive decoding and remain usable by the consensus engine.
	noncritical := cell.NewDict(8)
	setTestDictionaryValue(t, noncritical, 1, cell.BeginCell().MustStoreUInt(0, 32).EndCell())
	setTestDictionaryValue(t, noncritical, 2, cell.BeginCell().MustStoreUInt(0, 32).EndCell())
	simplexConfig := cell.BeginCell().
		MustStoreUInt(0x22, 8).
		MustStoreUInt(0, 5).
		MustStoreUInt(2, 2).
		MustStoreBoolBit(true).
		MustStoreUInt(4, 32).
		MustStoreDict(noncritical).
		EndCell()
	config30 := cell.BeginCell().
		MustStoreUInt(0x10, 8).
		MustStoreMaybeRef(simplexConfig).
		MustStoreMaybeRef(simplexConfig).
		EndCell()
	current := buildTestValidatorSet(t, []testValidatorWire{{index: 0, key: groupTestBytes(1), weight: 1}}, false, 0)
	config, err := ParseConfig(buildTestConfig(t, map[uint32]*cell.Cell{
		configParamNewConsensus:      config30,
		configParamCurrentValidators: current,
	}))
	if err != nil {
		t.Fatal(err)
	}

	for _, chain := range []*SimplexConfig{config.NewConsensus.Masterchain, config.NewConsensus.Shard} {
		params := chain.SimplexParams()
		if params.FirstBlockTimeout != 0 || params.FirstBlockTimeoutMultiplier != 0 {
			t.Fatal("explicit zero grace parameters were replaced by defaults")
		}
		if err := params.Validate(); err != nil {
			t.Fatalf("valid config 30 grace parameters rejected: %v", err)
		}
	}
}
