package blockproof

import (
	"bytes"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestShardTopValidatorContextAcceptsOnlyExactCatchainRosterPairs(t *testing.T) {
	context := testShardTopValidatorContext(
		t,
		50,
		map[string]uint32{"": 7},
		map[uint32]*cell.Cell{
			tlb.ConfigParamCatchainConfig:    testShardTopCatchainConfigCell(100),
			tlb.ConfigParamCurrentValidators: testShardTopValidatorSetCell(t, 0x11, 1),
			tlb.ConfigParamNextValidators:    testShardTopValidatorSetCell(t, 0x22, 100),
		},
	)
	block := ton.BlockIDExt{Workchain: 0, Shard: sharddomain.Root, SeqNo: 1}

	current, err := context.ValidatorsForCatchain(block, 7)
	if err != nil {
		t.Fatalf("current validators: %v", err)
	}
	testShardTopRequireValidatorKey(t, current, 0x11)

	next, err := context.ValidatorsForCatchain(block, 8)
	if err != nil {
		t.Fatalf("next validators: %v", err)
	}
	testShardTopRequireValidatorKey(t, next, 0x22)

	if _, err = context.ValidatorsForCatchain(block, 9); !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("arbitrary catchain error = %v, want ErrNotFound", err)
	}
	if errors.Is(err, ErrShardTopValidatorContextNotReady) {
		t.Fatalf("arbitrary catchain error was classified not-ready: %v", err)
	}
}

func TestShardTopValidatorContextDerivesAncestorAndImmediateMergeCatchain(t *testing.T) {
	left, err := sharddomain.Child(sharddomain.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	leftRight, err := sharddomain.Child(left, false)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name       string
		leaves     map[string]uint32
		shard      int64
		declaredCC uint32
	}{
		{
			name:       "ancestor leaf",
			leaves:     map[string]uint32{"": 7},
			shard:      leftRight,
			declaredCC: 7,
		},
		{
			name:       "immediate child pair",
			leaves:     map[string]uint32{"0": 7, "1": 9},
			shard:      sharddomain.Root,
			declaredCC: 10,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			context := testShardTopValidatorContext(
				t,
				1,
				test.leaves,
				map[uint32]*cell.Cell{
					tlb.ConfigParamCatchainConfig:    testShardTopCatchainConfigCell(100),
					tlb.ConfigParamCurrentValidators: testShardTopValidatorSetCell(t, 0x33, 1),
				},
			)
			block := ton.BlockIDExt{Workchain: 0, Shard: test.shard, SeqNo: 1}

			validators, err := context.ValidatorsForCatchain(block, test.declaredCC)
			if err != nil {
				t.Fatal(err)
			}
			testShardTopRequireValidatorKey(t, validators, 0x33)
		})
	}
}

func TestShardTopValidatorContextResolvesImmutableRegistryAnchors(t *testing.T) {
	left, err := sharddomain.Child(sharddomain.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := sharddomain.Child(sharddomain.Root, false)
	if err != nil {
		t.Fatal(err)
	}

	tests := []struct {
		name   string
		leaves map[string]uint32
		target int64
		count  uint8
		left   int64
		right  int64
	}{
		{name: "ancestor leaf", leaves: map[string]uint32{"": 7}, target: left, count: 1, left: sharddomain.Root},
		{name: "merge pair", leaves: map[string]uint32{"0": 7, "1": 8}, target: sharddomain.Root, count: 2, left: left, right: right},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			context := testShardTopValidatorContext(
				t,
				1,
				test.leaves,
				map[uint32]*cell.Cell{tlb.ConfigParamCurrentValidators: testShardTopValidatorSetCell(t, 0x11, 1)},
			)
			block := ton.BlockIDExt{Workchain: 0, Shard: test.target, SeqNo: 2}

			anchors, err := context.ShardTopAnchors(block)
			if err != nil {
				t.Fatal(err)
			}
			if anchors.Count != test.count || anchors.Left.Shard != test.left ||
				(test.count == 2 && anchors.Right.Shard != test.right) {
				t.Fatalf("anchors = %+v", anchors)
			}
			expectedLeft := ton.BlockIDExt{
				Workchain: 0,
				Shard:     test.left,
				SeqNo:     1,
				RootHash:  bytes.Repeat([]byte{0x11}, 32),
				FileHash:  bytes.Repeat([]byte{0x22}, 32),
			}
			if !anchors.Left.Matches(expectedLeft) {
				t.Fatalf("left anchor does not match registry block: %+v", anchors.Left)
			}
		})
	}
}

func TestShardTopValidatorContextChecksExactMasterchainAncestry(t *testing.T) {
	ancestor := testBlockID(-1, 99)
	ancestor.RootHash = bytes.Repeat([]byte{0x31}, 32)
	ancestor.FileHash = bytes.Repeat([]byte{0x32}, 32)
	head := testBlockID(-1, 100)
	head.RootHash = bytes.Repeat([]byte{0x41}, 32)
	head.FileHash = bytes.Repeat([]byte{0x42}, 32)
	info, err := LoadMasterStateInfo(testMasterStateInfoCell(t, false, nil, []testOldMasterBlock{{id: ancestor}}))
	if err != nil {
		t.Fatal(err)
	}
	context := &ShardTopValidatorContext{masterchain: head, prevBlocks: info.PrevBlocks}

	valid, err := context.IsMasterchainAncestor(ancestor)
	if err != nil || !valid {
		t.Fatalf("known ancestor: valid=%t err=%v", valid, err)
	}
	fork := *ancestor.Copy()
	fork.RootHash[0] ^= 0xff
	valid, err = context.IsMasterchainAncestor(fork)
	if err != nil || valid {
		t.Fatalf("fork ancestor: valid=%t err=%v", valid, err)
	}
	valid, err = context.IsMasterchainAncestor(head)
	if err != nil || !valid {
		t.Fatalf("current head: valid=%t err=%v", valid, err)
	}
}

func TestShardTopValidatorContextRejectsMoreThanOneMergeAhead(t *testing.T) {
	tests := []struct {
		name   string
		leaves map[string]uint32
	}{
		{
			name: "four grandchildren",
			leaves: map[string]uint32{
				"00": 7,
				"01": 7,
				"10": 7,
				"11": 7,
			},
		},
		{
			name: "one leaf and two opposite grandchildren",
			leaves: map[string]uint32{
				"0":  7,
				"10": 7,
				"11": 7,
			},
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			context := testShardTopValidatorContext(
				t,
				1,
				test.leaves,
				map[uint32]*cell.Cell{
					tlb.ConfigParamCurrentValidators: testShardTopValidatorSetCell(t, 0x11, 1),
				},
			)
			block := ton.BlockIDExt{Workchain: 0, Shard: sharddomain.Root, SeqNo: 1}

			if _, err := context.ValidatorsForCatchain(block, 7); !errors.Is(err, ErrShardTopValidatorContextNotReady) {
				t.Fatalf("error = %v, want ErrShardTopValidatorContextNotReady", err)
			} else if !errors.Is(err, storage.ErrNotFound) {
				t.Fatalf("error = %v, want ErrNotFound", err)
			}
		})
	}
}

func TestShardTopValidatorContextSelectsTentativeRosterAtBoundary(t *testing.T) {
	tests := []struct {
		name        string
		nextSince   uint32
		includeNext bool
		lifetime    uint32
		expectedKey byte
	}{
		{
			name:        "next starts at boundary",
			nextSince:   200,
			includeNext: true,
			lifetime:    100,
			expectedKey: 0x22,
		},
		{
			name:        "next starts after boundary",
			nextSince:   201,
			includeNext: true,
			lifetime:    100,
			expectedKey: 0x11,
		},
		{
			name:        "missing next falls back before lifetime lookup",
			includeNext: false,
			lifetime:    0,
			expectedKey: 0x11,
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := map[uint32]*cell.Cell{
				tlb.ConfigParamCatchainConfig:    testShardTopCatchainConfigCell(test.lifetime),
				tlb.ConfigParamCurrentValidators: testShardTopValidatorSetCell(t, 0x11, 1),
			}
			if test.includeNext {
				params[tlb.ConfigParamNextValidators] = testShardTopValidatorSetCell(t, 0x22, test.nextSince)
			}
			context := testShardTopValidatorContext(t, 150, map[string]uint32{"": 7}, params)
			block := ton.BlockIDExt{Workchain: 0, Shard: sharddomain.Root, SeqNo: 1}

			validators, err := context.ValidatorsForCatchain(block, 8)
			if err != nil {
				t.Fatal(err)
			}
			testShardTopRequireValidatorKey(t, validators, test.expectedKey)
		})
	}
}

func TestShardTopValidatorContextPrefersTemporaryRosters(t *testing.T) {
	context := testShardTopValidatorContext(
		t,
		50,
		map[string]uint32{"": 7},
		map[uint32]*cell.Cell{
			tlb.ConfigParamCatchainConfig:        testShardTopCatchainConfigCell(100),
			tlb.ConfigParamCurrentValidators:     testShardTopValidatorSetCell(t, 0x11, 1),
			tlb.ConfigParamCurrentTempValidators: testShardTopValidatorSetCell(t, 0x12, 1),
			tlb.ConfigParamNextValidators:        testShardTopValidatorSetCell(t, 0x21, 1),
			tlb.ConfigParamNextTempValidators:    testShardTopValidatorSetCell(t, 0x22, 1),
		},
	)
	block := ton.BlockIDExt{Workchain: 0, Shard: sharddomain.Root, SeqNo: 1}

	current, err := context.ValidatorsForCatchain(block, 7)
	if err != nil {
		t.Fatal(err)
	}
	testShardTopRequireValidatorKey(t, current, 0x12)

	next, err := context.ValidatorsForCatchain(block, 8)
	if err != nil {
		t.Fatal(err)
	}
	testShardTopRequireValidatorKey(t, next, 0x22)
}

func TestShardTopValidatorContextAllowsEmptyShardRegistry(t *testing.T) {
	context := testShardTopValidatorContext(
		t,
		1,
		nil,
		map[uint32]*cell.Cell{
			tlb.ConfigParamCurrentValidators: testShardTopValidatorSetCell(t, 0x11, 1),
		},
	)
	if context.BlockchainConfig() == nil || context.BlockchainConfig().Root == nil {
		t.Fatal("context has no blockchain config")
	}

	block := ton.BlockIDExt{Workchain: 0, Shard: sharddomain.Root, SeqNo: 1}
	if _, err := context.ValidatorsForCatchain(block, 7); !errors.Is(err, ErrShardTopValidatorContextNotReady) {
		t.Fatalf("empty registry error = %v, want ErrShardTopValidatorContextNotReady", err)
	} else if !errors.Is(err, storage.ErrNotFound) {
		t.Fatalf("empty registry error = %v, want ErrNotFound", err)
	}
}

func TestShardTopValidatorContextRejectsUnsafeLifetimeArithmetic(t *testing.T) {
	tests := []struct {
		name     string
		genUTime uint32
		lifetime uint32
		contains string
	}{
		{
			name:     "zero lifetime",
			genUTime: 1,
			lifetime: 0,
			contains: "must be positive",
		},
		{
			name:     "boundary overflow",
			genUTime: math.MaxUint32,
			lifetime: 1,
			contains: "overflows uint32",
		},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			context := testShardTopValidatorContext(
				t,
				test.genUTime,
				map[string]uint32{"": 7},
				map[uint32]*cell.Cell{
					tlb.ConfigParamCatchainConfig:    testShardTopCatchainConfigCell(test.lifetime),
					tlb.ConfigParamCurrentValidators: testShardTopValidatorSetCell(t, 0x11, 1),
					tlb.ConfigParamNextValidators:    testShardTopValidatorSetCell(t, 0x22, 1),
				},
			)
			block := ton.BlockIDExt{Workchain: 0, Shard: sharddomain.Root, SeqNo: 1}

			if _, err := context.ValidatorsForCatchain(block, 8); err == nil || !strings.Contains(err.Error(), test.contains) {
				t.Fatalf("error = %v, want substring %q", err, test.contains)
			}
		})
	}
}

func TestShardTopValidatorContextRejectsUnavailableAndOverflowingCatchainBases(t *testing.T) {
	t.Run("sentinel leaf", func(t *testing.T) {
		context := testShardTopValidatorContext(
			t,
			1,
			map[string]uint32{"": math.MaxUint32},
			map[uint32]*cell.Cell{
				tlb.ConfigParamCurrentValidators: testShardTopValidatorSetCell(t, 0x11, 1),
			},
		)
		block := ton.BlockIDExt{Workchain: 0, Shard: sharddomain.Root, SeqNo: 1}

		if _, err := context.ValidatorsForCatchain(block, math.MaxUint32); !errors.Is(err, ErrShardTopValidatorContextNotReady) {
			t.Fatalf("sentinel catchain error = %v, want ErrShardTopValidatorContextNotReady", err)
		} else if !errors.Is(err, storage.ErrNotFound) {
			t.Fatalf("sentinel catchain error = %v, want ErrNotFound", err)
		}
	})

	t.Run("merge increment", func(t *testing.T) {
		context := testShardTopValidatorContext(
			t,
			1,
			map[string]uint32{"0": math.MaxUint32, "1": 7},
			map[uint32]*cell.Cell{
				tlb.ConfigParamCurrentValidators: testShardTopValidatorSetCell(t, 0x11, 1),
			},
		)
		block := ton.BlockIDExt{Workchain: 0, Shard: sharddomain.Root, SeqNo: 1}

		if _, err := context.ValidatorsForCatchain(block, 0); !errors.Is(err, ErrShardTopValidatorContextNotReady) {
			t.Fatalf("merge overflow error = %v, want ErrShardTopValidatorContextNotReady", err)
		} else if !strings.Contains(err.Error(), "overflows uint32") {
			t.Fatalf("merge overflow error = %v", err)
		}
	})
}

func TestShardTopValidatorContextClassifiesOnlyLocalContextGapsAsNotReady(t *testing.T) {
	block := ton.BlockIDExt{Workchain: 0, Shard: sharddomain.Root, SeqNo: 1}

	t.Run("missing current roster", func(t *testing.T) {
		context := testShardTopValidatorContext(
			t,
			1,
			map[string]uint32{"": 7},
			map[uint32]*cell.Cell{0: cell.BeginCell().EndCell()},
		)
		if _, err := context.ValidatorsForCatchain(block, 7); !errors.Is(err, ErrShardTopValidatorContextNotReady) {
			t.Fatalf("error = %v, want ErrShardTopValidatorContextNotReady", err)
		}
	})

	t.Run("malformed current roster", func(t *testing.T) {
		context := testShardTopValidatorContext(
			t,
			1,
			map[string]uint32{"": 7},
			map[uint32]*cell.Cell{tlb.ConfigParamCurrentValidators: cell.BeginCell().EndCell()},
		)
		if _, err := context.ValidatorsForCatchain(block, 7); !errors.Is(err, ErrShardTopValidatorContextNotReady) {
			t.Fatalf("error = %v, want ErrShardTopValidatorContextNotReady", err)
		}
	})

	t.Run("wrong declaration stays permanent", func(t *testing.T) {
		context := testShardTopValidatorContext(
			t,
			1,
			map[string]uint32{"": 7},
			map[uint32]*cell.Cell{0: cell.BeginCell().EndCell()},
		)
		_, err := context.ValidatorsForCatchain(block, 9)
		if err == nil {
			t.Fatal("wrong catchain declaration was accepted")
		}
		if errors.Is(err, ErrShardTopValidatorContextNotReady) {
			t.Fatalf("wrong catchain declaration was classified not-ready: %v", err)
		}
	})
}

func TestShardTopValidatorContextDefaultsAbsentAndMalformedCatchainConfigOnce(t *testing.T) {
	tests := []struct {
		name     string
		catchain *cell.Cell
	}{
		{name: "absent"},
		{name: "malformed", catchain: cell.BeginCell().EndCell()},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			params := map[uint32]*cell.Cell{
				tlb.ConfigParamCurrentValidators: testShardTopValidatorSetWithCountCell(t, 0x10, 1, 8),
				tlb.ConfigParamNextValidators:    testShardTopValidatorSetWithCountCell(t, 0x80, 201, 8),
			}
			if test.catchain != nil {
				params[tlb.ConfigParamCatchainConfig] = test.catchain
			}
			context := testShardTopValidatorContext(t, 199, map[string]uint32{"": 7}, params)
			block := ton.BlockIDExt{Workchain: 0, Shard: sharddomain.Root, SeqNo: 1}

			validators, err := context.ValidatorsForCatchain(block, 8)
			if err != nil {
				t.Fatalf("defaulted catchain config validators: %v", err)
			}
			// The C++ defaults select seven shard validators. Its shard lifetime
			// is 200, so the next boundary from 199 is 200 and the next set
			// starting at 201 is not active yet.
			if len(validators) != 7 {
				t.Fatalf("validator count = %d, want default 7", len(validators))
			}
			for _, validator := range validators {
				if validator.PublicKey.Key[0] >= 0x80 {
					t.Fatalf("selected next validator key 0x%02x before default boundary", validator.PublicKey.Key[0])
				}
			}
		})
	}
}

func TestShardTopValidatorContextStrictlyParsesPresentShardDescriptions(t *testing.T) {
	description := testShardTopDescriptionCell(t, 7)
	malformed := cell.BeginCell().
		MustStoreBuilder(description.ToBuilder()).
		MustStoreUInt(0, 1).
		EndCell()
	tree := cell.BeginCell().
		MustStoreUInt(0, 1).
		MustStoreBuilder(malformed.ToBuilder()).
		EndCell()
	state := testShardTopValidatorState(
		t,
		1,
		tree,
		testValidatorBlockchainConfig(t, map[uint32]*cell.Cell{
			tlb.ConfigParamCurrentValidators: testShardTopValidatorSetCell(t, 0x11, 1),
		}),
	)

	if _, err := NewShardTopValidatorContext(state); err == nil || !strings.Contains(err.Error(), "trailing bits") {
		t.Fatalf("malformed shard description error = %v", err)
	} else if errors.Is(err, ErrShardTopValidatorContextNotReady) {
		t.Fatalf("malformed shard description was classified not-ready: %v", err)
	}
}

func testShardTopValidatorContext(
	t *testing.T,
	genUTime uint32,
	leaves map[string]uint32,
	params map[uint32]*cell.Cell,
) *ShardTopValidatorContext {
	t.Helper()

	var tree *cell.Cell
	if leaves != nil {
		tree = testShardTopBinTree(t, "", leaves)
	}
	state := testShardTopValidatorState(t, genUTime, tree, testValidatorBlockchainConfig(t, params))
	context, err := NewShardTopValidatorContext(state)
	if err != nil {
		t.Fatalf("build shard top validator context: %v", err)
	}
	return context
}

func testShardTopValidatorState(
	t *testing.T,
	genUTime uint32,
	tree *cell.Cell,
	config *tlb.BlockchainConfig,
) *storage.BlockState {
	t.Helper()

	var shardHashes *cell.Dictionary
	if tree != nil {
		shardHashes = cell.NewDict(32)
		key := cell.BeginCell().MustStoreInt(0, 32).EndCell()
		value := cell.BeginCell().MustStoreRef(tree).EndCell()
		if err := shardHashes.Set(key, value); err != nil {
			t.Fatalf("store shard hashes: %v", err)
		}
	}

	extra := cell.BeginCell().
		MustStoreUInt(0xcc26, 16).
		MustStoreDict(shardHashes).
		MustStoreSlice(make([]byte, 32), 256).
		MustStoreRef(config.Root).
		MustStoreRef(testShardTopMasterchainInfo(t)).
		MustStoreCoins(0).
		MustStoreDict(nil).
		EndCell()

	return &storage.BlockState{
		Parsed: &tlb.ShardStateUnsplit{
			GenUTime:     genUTime,
			McStateExtra: extra,
		},
	}
}

type testShardTopOldBlocksAugmentation struct{}

func (testShardTopOldBlocksAugmentation) SkipExtra(loader *cell.Slice) error {
	var extra tlb.KeyMaxLt
	return tlb.LoadFromCell(&extra, loader)
}

func (testShardTopOldBlocksAugmentation) EmptyExtra(dst *cell.Builder) error {
	if err := dst.StoreBoolBit(false); err != nil {
		return err
	}
	return dst.StoreUInt(0, 64)
}

func (testShardTopOldBlocksAugmentation) LeafExtra(*cell.Slice, *cell.Builder) error {
	return errors.New("unexpected old masterchain block")
}

func (testShardTopOldBlocksAugmentation) CombineExtra(*cell.Slice, *cell.Slice, *cell.Builder) error {
	return errors.New("unexpected old masterchain subtree")
}

func testShardTopMasterchainInfo(t *testing.T) *cell.Cell {
	t.Helper()

	history, err := cell.NewAugDict(32, testShardTopOldBlocksAugmentation{})
	if err != nil {
		t.Fatal(err)
	}
	historyCell, err := history.ToCell()
	if err != nil {
		t.Fatal(err)
	}

	return cell.BeginCell().
		MustStoreUInt(0, 16).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreBoolBit(false).
		MustStoreBuilder(historyCell.ToBuilder()).
		MustStoreBoolBit(false).
		MustStoreBoolBit(false).
		EndCell()
}

func testShardTopBinTree(t *testing.T, path string, leaves map[string]uint32) *cell.Cell {
	t.Helper()

	if catchainSeqno, found := leaves[path]; found {
		for candidate := range leaves {
			if candidate != path && strings.HasPrefix(candidate, path) {
				t.Fatalf("shard tree leaf %q also has descendant %q", path, candidate)
			}
		}
		return cell.BeginCell().
			MustStoreUInt(0, 1).
			MustStoreBuilder(testShardTopDescriptionCell(t, catchainSeqno).ToBuilder()).
			EndCell()
	}

	leftPath := path + "0"
	rightPath := path + "1"
	if !testShardTopHasPath(leaves, leftPath) || !testShardTopHasPath(leaves, rightPath) {
		t.Fatalf("shard tree fork %q does not have both branches", path)
	}
	return cell.BeginCell().
		MustStoreUInt(1, 1).
		MustStoreRef(testShardTopBinTree(t, leftPath, leaves)).
		MustStoreRef(testShardTopBinTree(t, rightPath, leaves)).
		EndCell()
}

func testShardTopHasPath(leaves map[string]uint32, path string) bool {
	for candidate := range leaves {
		if strings.HasPrefix(candidate, path) {
			return true
		}
	}
	return false
}

func testShardTopDescriptionCell(t *testing.T, catchainSeqno uint32) *cell.Cell {
	t.Helper()

	description, err := tlb.ToCell(&tlb.ShardDesc{
		SeqNo:              1,
		RootHash:           bytes.Repeat([]byte{0x11}, 32),
		FileHash:           bytes.Repeat([]byte{0x22}, 32),
		NextCatchainSeqNo:  catchainSeqno,
		NextValidatorShard: sharddomain.Root,
		SplitMergeAt:       tlb.FutureSplitMergeNone{},
	})
	if err != nil {
		t.Fatalf("build shard description: %v", err)
	}
	return description
}

func testShardTopValidatorSetCell(t *testing.T, keyByte byte, since uint32) *cell.Cell {
	t.Helper()

	return testShardTopValidatorSetWithCountCell(t, keyByte, since, 1)
}

func testShardTopValidatorSetWithCountCell(t *testing.T, firstKeyByte byte, since uint32, count int) *cell.Cell {
	t.Helper()

	validators := cell.NewDict(16)
	for i := range count {
		keyByte := firstKeyByte + byte(i)
		if err := validators.SetIntKey(big.NewInt(int64(i)), testValidatorAddrCell(keyByte, 100)); err != nil {
			t.Fatalf("store validator %d: %v", i, err)
		}
	}

	set, err := tlb.ToCell(&tlb.ValidatorSetExt{
		UTimeSince:  since,
		UTimeUntil:  math.MaxUint32,
		Total:       uint16(count),
		Main:        uint16(count),
		TotalWeight: uint64(count) * 100,
		List:        validators,
	})
	if err != nil {
		t.Fatalf("build validator set: %v", err)
	}
	return set
}

func testShardTopCatchainConfigCell(shardLifetime uint32) *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(0xc1, 8).
		MustStoreUInt(100, 32).
		MustStoreUInt(uint64(shardLifetime), 32).
		MustStoreUInt(100, 32).
		MustStoreUInt(1, 32).
		EndCell()
}

func testShardTopRequireValidatorKey(t *testing.T, validators []*tlb.ValidatorAddr, expected byte) {
	t.Helper()

	if len(validators) != 1 {
		t.Fatalf("validator count = %d, want 1", len(validators))
	}
	if got := validators[0].PublicKey.Key[0]; got != expected {
		t.Fatalf("validator key starts with 0x%02x, want 0x%02x", got, expected)
	}
}
