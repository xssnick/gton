package groups

import (
	"bytes"
	"math/big"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestResolveCollatorsByValidatorMatchesRegistrySelection(t *testing.T) {
	t.Parallel()

	contract := groupTestBytes(90)
	occupiedADNL := groupTestBytes(1)
	firstCollator := groupTestBytes(10)
	ignoredByLimit := groupTestBytes(20)
	sharedCollator := groupTestBytes(30)

	previousValidator := testValidatorWire{
		index: 0, key: groupTestBytes(43), adnl: groupTestBytes(3), weight: 1, withADNL: true,
	}
	currentValidator := testValidatorWire{
		index: 0, key: groupTestBytes(41), adnl: occupiedADNL, weight: 1, withADNL: true,
	}
	malformedValidator := testValidatorWire{
		index: 1, key: groupTestBytes(42), adnl: groupTestBytes(2), weight: 1, withADNL: true,
	}
	nextValidator := testValidatorWire{
		index: 0, key: groupTestBytes(44), adnl: groupTestBytes(4), weight: 1, withADNL: true,
	}
	unrelatedKey := groupTestBytes(99)
	currentInPrevious := currentValidator
	currentInPrevious.index = 1
	currentInNext := currentValidator
	currentInNext.index = 1

	storage := registryTestStorage(t, map[[32]byte]*cell.Cell{
		currentValidator.key: registryTestValidator(registryTestEntry(t,
			ignoredByLimit,
			firstCollator,
			occupiedADNL,
		)),
		malformedValidator.key: cell.BeginCell().MustStoreUInt(0xdeadbeef, 32).EndCell(),
		previousValidator.key:  registryTestValidator(registryTestEntry(t, sharedCollator)),
		nextValidator.key:      registryTestValidator(registryTestEntry(t, sharedCollator)),
		unrelatedKey:           registryTestValidator(registryTestEntry(t, groupTestBytes(40))),
	})
	configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
		configParamFundamentalContracts: registryTestFundamentalContracts(t, contract),
		configParamPreviousValidators: buildTestValidatorSet(t, []testValidatorWire{
			previousValidator,
			currentInPrevious,
		}, true, 2),
		configParamCurrentValidators: buildTestValidatorSet(t, []testValidatorWire{
			currentValidator,
			malformedValidator,
		}, true, 2),
		configParamNextValidators: buildTestValidatorSet(t, []testValidatorWire{
			nextValidator,
			currentInNext,
		}, true, 2),
		configParamValidatorRegistry: registryTestConfig(contract, 2),
	})
	input := registryTestState(t, 100, true, false, configRoot, [32]byte{}, registryTestAccounts(t, contract, storage))
	state, err := ParseState(input)
	if err != nil {
		t.Fatal(err)
	}
	config, err := ParseConfig(state.ConfigRoot)
	if err != nil {
		t.Fatal(err)
	}

	registry, issue := resolveCollatorsByValidator(state, config)
	if issue != nil {
		t.Fatalf("usable registry reported an issue: %v", issue)
	}
	publicRegistry, err := ResolveCollatorsByValidator(input)
	if err != nil {
		t.Fatal(err)
	}
	registryTestRequireEqual(t, publicRegistry, registry)
	want := []CollatorRegistryEntry{
		registryTestProjection(t, previousValidator.key, sharedCollator),
		registryTestProjection(t, currentValidator.key, firstCollator),
		registryTestProjection(t, nextValidator.key, sharedCollator),
	}
	sortRegistryTestEntries(want)
	registryTestRequireEqual(t, registry, want)

	for _, entry := range registry {
		if entry.ValidatorKeyID == registryTestKeyID(t, currentValidator.key) {
			if len(entry.CollatorADNLIDs) != 1 || entry.CollatorADNLIDs[0] != firstCollator {
				t.Fatalf("limit/filter order = %x, want only %x", entry.CollatorADNLIDs, firstCollator)
			}
			if entry.CollatorADNLIDs[0] == ignoredByLimit {
				t.Fatal("collator after the max-collators-per-validator prefix was backfilled")
			}
		}
	}

	overlay := persistentOverlayWithCollators(config.PersistentOverlayMembers(), registry)
	if registryTestOverlayCount(overlay, sharedCollator) != 1 {
		t.Fatalf("shared collator occurs more than once in overlay: %+v", overlay)
	}
	if registryTestOverlayCount(overlay, occupiedADNL) != 1 {
		t.Fatalf("validator ADNL was duplicated by registry: %+v", overlay)
	}
}

func TestResolveCollatorsByValidatorRequiresSpecialActiveContract(t *testing.T) {
	t.Parallel()

	contract := groupTestBytes(90)
	validator := testValidatorWire{index: 0, key: groupTestBytes(41), weight: 1}
	collatorID := groupTestBytes(10)
	storage := registryTestStorage(t, map[[32]byte]*cell.Cell{
		validator.key: registryTestValidator(registryTestEntry(t, collatorID)),
	})
	accounts := registryTestAccounts(t, contract, storage)

	t.Run("not special", func(t *testing.T) {
		configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
			configParamCurrentValidators: buildTestValidatorSet(t, []testValidatorWire{validator}, true, 1),
			configParamValidatorRegistry: registryTestConfig(contract, 1),
		})
		state, config := registryTestParsedStateAndConfig(
			t,
			registryTestState(t, 100, true, false, configRoot, groupTestBytes(91), accounts),
		)
		registry, issue := resolveCollatorsByValidator(state, config)
		if len(registry) != 0 {
			t.Fatalf("non-special registry accepted: %+v", registry)
		}
		if issue == nil {
			t.Fatal("non-special registry contract disabled delegation without reporting a reason")
		}
	})

	t.Run("config contract is implicitly special", func(t *testing.T) {
		configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
			configParamCurrentValidators: buildTestValidatorSet(t, []testValidatorWire{validator}, true, 1),
			configParamValidatorRegistry: registryTestConfig(contract, 1),
		})
		state, config := registryTestParsedStateAndConfig(
			t,
			registryTestState(t, 100, true, false, configRoot, contract, accounts),
		)
		registry, issue := resolveCollatorsByValidator(state, config)
		if issue != nil {
			t.Fatalf("usable registry reported an issue: %v", issue)
		}
		if len(registry) != 1 || registry[0].CollatorADNLIDs[0] != collatorID {
			t.Fatalf("config contract registry = %+v", registry)
		}
	})

	t.Run("missing account", func(t *testing.T) {
		configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
			configParamFundamentalContracts: registryTestFundamentalContracts(t, contract),
			configParamCurrentValidators:    buildTestValidatorSet(t, []testValidatorWire{validator}, true, 1),
			configParamValidatorRegistry:    registryTestConfig(contract, 1),
		})
		state, config := registryTestParsedStateAndConfig(
			t,
			registryTestState(t, 100, true, false, configRoot, [32]byte{}, nil),
		)
		registry, issue := resolveCollatorsByValidator(state, config)
		if len(registry) != 0 {
			t.Fatalf("missing registry account accepted: %+v", registry)
		}
		if issue == nil {
			t.Fatal("missing registry account disabled delegation without reporting a reason")
		}
	})

	t.Run("account without data", func(t *testing.T) {
		configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
			configParamFundamentalContracts: registryTestFundamentalContracts(t, contract),
			configParamCurrentValidators:    buildTestValidatorSet(t, []testValidatorWire{validator}, true, 1),
			configParamValidatorRegistry:    registryTestConfig(contract, 1),
		})
		state, config := registryTestParsedStateAndConfig(
			t,
			registryTestState(t, 100, true, false, configRoot, [32]byte{}, registryTestAccounts(t, contract, nil)),
		)
		registry, issue := resolveCollatorsByValidator(state, config)
		if len(registry) != 0 {
			t.Fatalf("registry account without data accepted: %+v", registry)
		}
		if issue == nil {
			t.Fatal("registry account without data disabled delegation without reporting a reason")
		}
	})

	t.Run("malformed parameter 46", func(t *testing.T) {
		configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
			configParamFundamentalContracts: registryTestFundamentalContracts(t, contract),
			configParamCurrentValidators:    buildTestValidatorSet(t, []testValidatorWire{validator}, true, 1),
			configParamValidatorRegistry:    cell.BeginCell().MustStoreUInt(0, 32).EndCell(),
		})
		state, config := registryTestParsedStateAndConfig(
			t,
			registryTestState(t, 100, true, false, configRoot, [32]byte{}, accounts),
		)
		registry, issue := resolveCollatorsByValidator(state, config)
		if len(registry) != 0 {
			t.Fatalf("malformed registry config accepted: %+v", registry)
		}
		if issue == nil {
			t.Fatal("malformed registry config disabled delegation without reporting a reason")
		}
	})
}

func TestTrackerColdStartPinsRegistryAtKeyAndNonKeyRotations(t *testing.T) {
	t.Parallel()

	contract := groupTestBytes(90)
	validator := testValidatorWire{index: 0, key: groupTestBytes(41), weight: 1}
	validatorKeyID := registryTestKeyID(t, validator.key)
	oldCollator := groupTestBytes(10)
	nonKeyCollator := groupTestBytes(20)
	keyCollator := groupTestBytes(30)
	configRoot := buildTestConfig(t, map[uint32]*cell.Cell{
		configParamFundamentalContracts: registryTestFundamentalContracts(t, contract),
		configParamCurrentValidators:    buildTestValidatorSet(t, []testValidatorWire{validator}, true, 1),
		configParamValidatorRegistry:    registryTestConfig(contract, 1),
	})
	registryAccounts := func(id [32]byte) *tlb.ShardAccountsAugDict {
		return registryTestAccounts(t, contract, registryTestStorage(t, map[[32]byte]*cell.Cell{
			validator.key: registryTestValidator(registryTestEntry(t, id)),
		}))
	}

	initial := []CollatorRegistryEntry{{
		ValidatorKeyID:  validatorKeyID,
		CollatorADNLIDs: [][32]byte{oldCollator},
	}}
	tracker, err := NewTracker(TrackerOptions{InitialCollators: initial})
	if err != nil {
		t.Fatal(err)
	}
	initial[0].CollatorADNLIDs[0] = groupTestBytes(99)

	first := registryTestState(t, 100, true, false, configRoot, [32]byte{}, registryAccounts(nonKeyCollator))
	result, err := tracker.Apply(ApplyInput{Block: first.Block, Root: first.Root, AsOf: time.Unix(1_700_000_000, 0)})
	if err != nil {
		t.Fatal(err)
	}
	registryTestRequireEqual(t, result.Snapshot.CollatorsByValidator, []CollatorRegistryEntry{{
		ValidatorKeyID: validatorKeyID, CollatorADNLIDs: [][32]byte{oldCollator},
	}})
	if len(result.Snapshot.Future) != 0 {
		t.Fatalf("non-key rotation retained future sessions: %+v", result.Snapshot.Future)
	}
	if registryTestOverlayCount(result.Snapshot.PersistentOverlay, oldCollator) != 1 ||
		registryTestOverlayCount(result.Snapshot.PersistentOverlay, nonKeyCollator) != 0 {
		t.Fatalf("non-key rotation overlay switched too early: %+v", result.Snapshot.PersistentOverlay)
	}

	second := registryTestState(t, 101, false, false, configRoot, [32]byte{}, registryAccounts(keyCollator))
	result, err = tracker.Apply(ApplyInput{Block: second.Block, Root: second.Root, AsOf: time.Unix(1_700_000_001, 0)})
	if err != nil {
		t.Fatal(err)
	}
	registryTestRequireEqual(t, result.Snapshot.CollatorsByValidator, []CollatorRegistryEntry{{
		ValidatorKeyID: validatorKeyID, CollatorADNLIDs: [][32]byte{nonKeyCollator},
	}})
	if registryTestOverlayCount(result.Snapshot.PersistentOverlay, nonKeyCollator) != 1 ||
		registryTestOverlayCount(result.Snapshot.PersistentOverlay, keyCollator) != 0 {
		t.Fatalf("registry refreshed without rotation: %+v", result.Snapshot.PersistentOverlay)
	}

	third := registryTestState(t, 102, true, true, configRoot, [32]byte{}, registryAccounts(keyCollator))
	result, err = tracker.Apply(ApplyInput{Block: third.Block, Root: third.Root, AsOf: time.Unix(1_700_000_002, 0)})
	if err != nil {
		t.Fatal(err)
	}
	registryTestRequireEqual(t, result.Snapshot.CollatorsByValidator, []CollatorRegistryEntry{{
		ValidatorKeyID: validatorKeyID, CollatorADNLIDs: [][32]byte{keyCollator},
	}})
	if len(result.Snapshot.Future) != 0 {
		t.Fatalf("key rotation retained future sessions: %+v", result.Snapshot.Future)
	}
	if registryTestOverlayCount(result.Snapshot.PersistentOverlay, keyCollator) != 1 {
		t.Fatalf("key rotation did not switch registry immediately: %+v", result.Snapshot.PersistentOverlay)
	}
}

func TestNewTrackerRejectsDuplicateInitialRegistry(t *testing.T) {
	t.Parallel()

	keyID := groupTestBytes(1)
	id := groupTestBytes(2)
	_, err := NewTracker(TrackerOptions{InitialCollators: []CollatorRegistryEntry{
		{ValidatorKeyID: keyID, CollatorADNLIDs: [][32]byte{id}},
		{ValidatorKeyID: keyID, CollatorADNLIDs: [][32]byte{groupTestBytes(3)}},
	}})
	if err == nil {
		t.Fatal("NewTracker accepted duplicate initial validator registry entry")
	}
}

func registryTestState(
	t *testing.T,
	seqno uint32,
	rotated bool,
	keyState bool,
	configRoot *cell.Cell,
	configAddress [32]byte,
	accounts *tlb.ShardAccountsAugDict,
) StateInput {
	t.Helper()

	shard := ShardID{Workchain: 0, Shard: masterchainShard}
	return buildStateFixture(t, stateFixtureOptions{
		Seqno:            seqno,
		GenUTime:         1_700_000_000 + seqno,
		CatchainSeqno:    44,
		RotatedAllShards: rotated,
		AfterKeyBlock:    keyState,
		ConfigRoot:       configRoot,
		ConfigAddress:    configAddress,
		Accounts:         accounts,
		ShardHashes: testShardHashes(t, testBinTreeLeaf(
			testParsedShardDescription(t, shard, seqno-1, 91, tlb.FutureSplitMergeNone{}),
		)),
	})
}

func registryTestParsedStateAndConfig(t *testing.T, input StateInput) (*State, *Config) {
	t.Helper()

	state, err := ParseState(input)
	if err != nil {
		t.Fatal(err)
	}
	config, err := ParseConfig(state.ConfigRoot)
	if err != nil {
		t.Fatal(err)
	}

	return state, config
}

func registryTestConfig(contract [32]byte, limit uint32) *cell.Cell {
	return cell.BeginCell().
		MustStoreUInt(validatorRegistryConfigConstructor, 32).
		MustStoreSlice(contract[:], 256).
		MustStoreUInt(uint64(limit), 32).
		MustStoreBoolBit(false).
		EndCell()
}

func registryTestFundamentalContracts(t *testing.T, contracts ...[32]byte) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(256)
	for _, contract := range contracts {
		setTestDictionaryBytesKey(t, dict, contract, cell.BeginCell().EndCell())
	}

	return cell.BeginCell().MustStoreDict(dict).EndCell()
}

func registryTestStorage(t *testing.T, validators map[[32]byte]*cell.Cell) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(256)
	for key, value := range validators {
		setTestDictionaryBytesKey(t, dict, key, value)
	}

	return cell.BeginCell().
		MustStoreUInt(validatorRegistryStorageConstructor, 32).
		MustStoreDict(dict).
		MustStoreUInt(0, 32).
		EndCell()
}

func registryTestValidator(entry *cell.Cell) *cell.Cell {
	builder := cell.BeginCell().
		MustStoreUInt(validatorRegistryValidatorConstructor, 32).
		MustStoreUInt(0, 32)
	if entry == nil {
		return builder.MustStoreBoolBit(false).EndCell()
	}

	return builder.MustStoreBoolBit(true).MustStoreRef(entry).EndCell()
}

func registryTestEntry(t *testing.T, collators ...[32]byte) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(256)
	for _, id := range collators {
		value := cell.BeginCell().
			MustStoreUInt(0x10364494, 32).
			MustStoreUInt(0, 4).
			MustStoreBoolBit(true).
			EndCell()
		setTestDictionaryBytesKey(t, dict, id, value)
	}

	return cell.BeginCell().
		MustStoreUInt(validatorRegistryEntryConstructor, 32).
		MustStoreDict(dict).
		MustStoreUInt(0, 4).
		MustStoreBoolBit(true).
		EndCell()
}

func registryTestAccounts(t *testing.T, contract [32]byte, data *cell.Cell) *tlb.ShardAccountsAugDict {
	t.Helper()

	accountCell, err := tlb.ToCell(&tlb.AccountState{
		IsValid: true,
		Address: address.NewAddress(0, 0xff, append([]byte(nil), contract[:]...)),
		StorageInfo: tlb.StorageInfo{
			StorageUsed: tlb.StorageUsed{
				CellsUsed: big.NewInt(0),
				BitsUsed:  big.NewInt(0),
			},
			StorageExtra: tlb.StorageExtraNone{},
		},
		AccountStorage: tlb.AccountStorage{
			Status:  tlb.AccountStatusActive,
			Balance: tlb.ZeroCoins,
			StateInit: &tlb.StateInit{
				Data: data,
				Lib:  cell.NewDict(256),
			},
		},
	})
	if err != nil {
		t.Fatalf("build registry account: %v", err)
	}
	shardAccount, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       accountCell,
		LastTransHash: make([]byte, 32),
	})
	if err != nil {
		t.Fatalf("build registry shard account: %v", err)
	}

	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatalf("create registry accounts: %v", err)
	}
	key := cell.BeginCell().MustStoreSlice(contract[:], 256).EndCell()
	if err = accounts.Set(key, shardAccount); err != nil {
		t.Fatalf("store registry account: %v", err)
	}

	return accounts
}

func registryTestProjection(t *testing.T, validatorKey [32]byte, collators ...[32]byte) CollatorRegistryEntry {
	t.Helper()

	return CollatorRegistryEntry{
		ValidatorKeyID:  registryTestKeyID(t, validatorKey),
		CollatorADNLIDs: collators,
	}
}

func registryTestKeyID(t *testing.T, publicKey [32]byte) [32]byte {
	t.Helper()

	keyID, err := PublicKeyHash(publicKey)
	if err != nil {
		t.Fatal(err)
	}

	return keyID
}

func sortRegistryTestEntries(entries []CollatorRegistryEntry) {
	for i := 1; i < len(entries); i++ {
		for j := i; j > 0 && bytes.Compare(entries[j].ValidatorKeyID[:], entries[j-1].ValidatorKeyID[:]) < 0; j-- {
			entries[j], entries[j-1] = entries[j-1], entries[j]
		}
	}
}

func registryTestRequireEqual(t *testing.T, got, want []CollatorRegistryEntry) {
	t.Helper()

	if len(got) != len(want) {
		t.Fatalf("registry length = %d, want %d: got %+v", len(got), len(want), got)
	}
	for i := range want {
		if got[i].ValidatorKeyID != want[i].ValidatorKeyID || len(got[i].CollatorADNLIDs) != len(want[i].CollatorADNLIDs) {
			t.Fatalf("registry[%d] = %+v, want %+v", i, got[i], want[i])
		}
		for j := range want[i].CollatorADNLIDs {
			if got[i].CollatorADNLIDs[j] != want[i].CollatorADNLIDs[j] {
				t.Fatalf("registry[%d].collators[%d] = %x, want %x", i, j, got[i].CollatorADNLIDs[j], want[i].CollatorADNLIDs[j])
			}
		}
	}
}

func registryTestOverlayCount(overlay []PersistentOverlayMember, id [32]byte) int {
	count := 0
	for i := range overlay {
		if overlay[i].ADNL == id {
			count++
		}
	}

	return count
}
