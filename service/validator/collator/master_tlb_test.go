package collator

import (
	"errors"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestSerializeMcBlockExtraRoundTrip(t *testing.T) {
	for _, keyBlock := range []bool{false, true} {
		name := "ordinary"
		if keyBlock {
			name = "key"
		}

		t.Run(name, func(t *testing.T) {
			extra := testMcBlockExtra(t, keyBlock)
			extra.Details.RecoverCreateMsg = cell.BeginCell().MustStoreUInt(0x31, 8).EndCell()
			extra.Details.MintMsg = cell.BeginCell().MustStoreUInt(0x72, 8).EndCell()

			root, err := serializeMcBlockExtra(extra)
			if err != nil {
				t.Fatal(err)
			}

			loader := root.MustBeginParse()
			var decoded tlb.McBlockExtra
			if err = tlb.LoadFromCell(&decoded, loader); err != nil {
				t.Fatalf("decode serialized masterchain block extra: %v", err)
			}
			if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
				t.Fatalf("serialized masterchain block extra has %d trailing bits and %d trailing refs",
					loader.BitsLeft(), loader.RefsNum())
			}
			if decoded.KeyBlock != keyBlock {
				t.Fatalf("key block = %v, want %v", decoded.KeyBlock, keyBlock)
			}
			if decoded.ShardHashes == nil || decoded.ShardHashes.GetKeySize() != 32 {
				t.Fatalf("decoded shard hashes = %#v", decoded.ShardHashes)
			}
			if decoded.ShardFees == nil || decoded.ShardFees.GetKeySize() != 96 {
				t.Fatalf("decoded shard fees = %#v", decoded.ShardFees)
			}
			if decoded.Details.PrevBlockSignatures == nil || decoded.Details.PrevBlockSignatures.GetKeySize() != 16 {
				t.Fatalf("decoded previous signatures = %#v", decoded.Details.PrevBlockSignatures)
			}
			if decoded.Details.RecoverCreateMsg == nil ||
				decoded.Details.RecoverCreateMsg.HashKey() != extra.Details.RecoverCreateMsg.HashKey() {
				t.Fatal("recover message changed after round trip")
			}
			if decoded.Details.MintMsg == nil || decoded.Details.MintMsg.HashKey() != extra.Details.MintMsg.HashKey() {
				t.Fatal("mint message changed after round trip")
			}
			if (decoded.ConfigParams != nil) != keyBlock {
				t.Fatalf("decoded configuration presence = %v, key block = %v",
					decoded.ConfigParams != nil, keyBlock)
			}

			reencoded, err := serializeMcBlockExtra(&decoded)
			if err != nil {
				t.Fatalf("re-encode decoded masterchain block extra: %v", err)
			}
			if reencoded.HashKey() != root.HashKey() {
				t.Fatal("masterchain block extra changed after round trip")
			}
		})
	}
}

func TestSerializeMcBlockExtraMatchesEmptyOrdinaryTLB(t *testing.T) {
	got, err := serializeMcBlockExtra(testEmptyMcBlockExtra(t))
	if err != nil {
		t.Fatal(err)
	}

	// masterchain_block_extra#cca5, an empty ShardHashes HashmapE, an empty
	// ShardFees HashmapAugE with two zero CurrencyCollections in its root extra,
	// and a details reference containing three absent HashmapE/Maybe values.
	details := cell.BeginCell().
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		EndCell()
	want := cell.BeginCell().
		MustStoreUInt(0xcca5, 16).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		MustStoreUInt(0, 4).
		MustStoreBoolBit(false).
		MustStoreUInt(0, 4).
		MustStoreBoolBit(false).
		MustStoreRef(details).
		EndCell()

	if got.HashKey() != want.HashKey() {
		t.Fatalf("masterchain block extra hash = %x, want %x", got.Hash(), want.Hash())
	}
}

func TestSerializeMcBlockExtraRejectsMalformedContract(t *testing.T) {
	tests := []struct {
		name  string
		build func(t *testing.T) *tlb.McBlockExtra
	}{
		{
			name:  "nil extra",
			build: func(*testing.T) *tlb.McBlockExtra { return nil },
		},
		{
			name: "nil shard hashes",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testEmptyMcBlockExtra(t)
				extra.ShardHashes = nil
				return extra
			},
		},
		{
			name: "wrong shard hashes key size",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testEmptyMcBlockExtra(t)
				extra.ShardHashes = cell.NewDict(31)
				return extra
			},
		},
		{
			name: "nil shard fees wrapper",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testEmptyMcBlockExtra(t)
				extra.ShardFees = nil
				return extra
			},
		},
		{
			name: "nil shard fees dictionary",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testEmptyMcBlockExtra(t)
				extra.ShardFees = &tlb.ShardFeesAugDict{}
				return extra
			},
		},
		{
			name: "wrong shard fees key size",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testEmptyMcBlockExtra(t)
				fees, err := cell.NewAugDict(95, tlb.AugShardFees{})
				if err != nil {
					t.Fatal(err)
				}
				extra.ShardFees = &tlb.ShardFeesAugDict{AugmentedDictionary: fees}
				return extra
			},
		},
		{
			name: "nil previous signatures",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testEmptyMcBlockExtra(t)
				extra.Details.PrevBlockSignatures = nil
				return extra
			},
		},
		{
			name: "wrong previous signatures key size",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testEmptyMcBlockExtra(t)
				extra.Details.PrevBlockSignatures = cell.NewDict(15)
				return extra
			},
		},
		{
			name: "non-key block configuration",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testEmptyMcBlockExtra(t)
				extra.ConfigParams = testConfigParams(t)
				return extra
			},
		},
		{
			name: "key block without configuration",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testEmptyMcBlockExtra(t)
				extra.KeyBlock = true
				return extra
			},
		},
		{
			name: "short configuration address",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testMcBlockExtra(t, true)
				extra.ConfigParams.ConfigAddr = make([]byte, 31)
				return extra
			},
		},
		{
			name: "nil configuration dictionary",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testMcBlockExtra(t, true)
				extra.ConfigParams.Config.Params = nil
				return extra
			},
		},
		{
			name: "wrong configuration key size",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testMcBlockExtra(t, true)
				extra.ConfigParams.Config.Params = cell.NewDict(31)
				return extra
			},
		},
		{
			name: "empty configuration dictionary",
			build: func(t *testing.T) *tlb.McBlockExtra {
				extra := testMcBlockExtra(t, true)
				extra.ConfigParams.Config.Params = cell.NewDict(32)
				return extra
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := serializeMcBlockExtra(test.build(t)); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

func testEmptyMcBlockExtra(t *testing.T) *tlb.McBlockExtra {
	t.Helper()

	fees, err := tlb.NewShardFeesAugDict()
	if err != nil {
		t.Fatal(err)
	}
	extra := &tlb.McBlockExtra{
		ShardHashes: cell.NewDict(32),
		ShardFees:   fees,
	}
	extra.Details.PrevBlockSignatures = cell.NewDict(16)
	return extra
}

func testMcBlockExtra(t *testing.T, keyBlock bool) *tlb.McBlockExtra {
	t.Helper()

	extra := testEmptyMcBlockExtra(t)
	extra.KeyBlock = keyBlock
	if keyBlock {
		extra.ConfigParams = testConfigParams(t)
	}

	key := cell.BeginCell().MustStoreInt(0, 32).MustStoreUInt(0x8000000000000000, 64).EndCell()
	value := cell.BeginCell().
		MustStoreBigCoins(big.NewInt(17)).
		MustStoreBoolBit(false).
		MustStoreBigCoins(big.NewInt(3)).
		MustStoreBoolBit(false).
		EndCell()
	if err := extra.ShardFees.Set(key, value); err != nil {
		t.Fatal(err)
	}

	return extra
}

func testConfigParams(t *testing.T) *tlb.ConfigParams {
	t.Helper()

	params := cell.NewDict(32)
	key := cell.BeginCell().MustStoreUInt(0, 32).EndCell()
	value := cell.BeginCell().MustStoreRef(cell.BeginCell().MustStoreSlice(make([]byte, 32), 256).EndCell()).EndCell()
	if err := params.Set(key, value); err != nil {
		t.Fatal(err)
	}

	config := &tlb.ConfigParams{ConfigAddr: make([]byte, 32)}
	config.Config.Params = params
	return config
}
