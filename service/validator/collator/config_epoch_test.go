package collator

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"math/big"
	"slices"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// TestPrepareConfigAddsNoReadsToTheMasterConfigTransition pins what the
// configuration parses of one masterchain block read.
//
// deriveMasterConfigTransition runs under the block's read set on the collation
// path: the Merkle update descends only through cells that set recorded, and the
// collated-size estimate answers membership out of the same record. So this
// count and this digest ARE the produced masterchain block. A change in either
// number means the collator emits different bytes than it emitted before, and
// the only acceptable reason to move the golden is a deliberate, reviewed change
// to what a masterchain block commits to.
//
// The golden was taken from the tree before Config held any epoch-derived
// configuration data, which is what makes this a differential rather than a
// tautology. Every value PrepareConfig now precomputes is derived either from the
// already-prepared execution config (no cell touched at all) or from parameters
// validateMasterConfigData has read on this path before PrepareConfig runs.
func TestPrepareConfigAddsNoReadsToTheMasterConfigTransition(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	_, reads := masterConfigTransitionReads(t, fixture, nil, nil)

	digest := sha256.New()
	for _, hash := range reads {
		digest.Write(hash[:])
	}
	got := hex.EncodeToString(digest.Sum(nil))

	const (
		wantCount  = 2868
		wantDigest = "bb62eb5021630c3034e03a70a0ae82fa97d9109f426f272d08ca930fb202fb3f"
	)
	if len(reads) != wantCount || got != wantDigest {
		t.Fatalf("configuration transition recorded %d cells (%s), want %d (%s)",
			len(reads), got, wantCount, wantDigest)
	}
}

// epochConfigOf prepares one configuration root the way a live epoch is
// prepared, minus the footprint capture the read-set tests need.
func epochConfigOf(t *testing.T, root *cell.Cell) *Config {
	t.Helper()

	execution, err := tvm.PrepareBlockchainConfig(root)
	if err != nil {
		t.Fatal(err)
	}
	config, err := PrepareConfig(execution)
	if err != nil {
		t.Fatal(err)
	}
	return config
}

func epochConfigWithoutParam(t *testing.T, base *cell.Cell, id uint32) *cell.Cell {
	t.Helper()

	dict := base.AsDict(32)
	if err := dict.DeleteIntKey(new(big.Int).SetUint64(uint64(id))); err != nil {
		t.Fatal(err)
	}
	return dict.AsCell()
}

// freshMasterSpecialAccounts is the algorithm masterSpecialAccounts ran before
// the identities became epoch data, kept here verbatim as an independent oracle.
// It is the tick/tock order proof: the sequence it produces is the sequence the
// masterchain collator executes, and a permutation reassigns logical times.
func freshMasterSpecialAccounts(t *testing.T, root *cell.Cell) [][32]byte {
	t.Helper()

	config := tlb.BlockchainConfig{Root: root}
	fundamental, err := config.GetFundamentalSmartContractAddresses()
	if err != nil {
		t.Fatal(err)
	}
	if fundamental.Addresses == nil || fundamental.Addresses.GetKeySize() != 256 {
		t.Fatal("fundamental smart contract dictionary is malformed")
	}
	items, err := fundamental.Addresses.LoadAll()
	if err != nil {
		t.Fatal(err)
	}

	addresses := make([][32]byte, 0, len(items)+1)
	for i := range items {
		key, loadErr := items[i].Key.LoadSlice(256)
		if loadErr != nil || items[i].Key.BitsLeft() != 0 ||
			items[i].Value.BitsLeft() != 0 || items[i].Value.RefsNum() != 0 {
			t.Fatalf("fundamental smart contract entry %d is malformed", i)
		}
		addresses = append(addresses, [32]byte(key))
	}

	configAddress, err := config.GetConfigAddress()
	if err != nil || len(configAddress) != 32 {
		t.Fatalf("config smart contract address is malformed: %v", err)
	}
	listed := false
	for i := range addresses {
		if bytes.Equal(addresses[i][:], configAddress) {
			listed = true
			break
		}
	}
	if !listed {
		addresses = append(addresses, [32]byte(configAddress))
	}
	return addresses
}

// TestPrepareConfigEpochValuesMatchFreshParse is the differential guard for
// hoisting per-block parses into the config epoch: every value Config now
// precomputes must equal what the parse it replaced would have produced from the
// same root.
func TestPrepareConfigEpochValuesMatchFreshParse(t *testing.T) {
	root := loadMainnetConfig(t).execution.Root()
	config := epochConfigOf(t, root)
	raw := tlb.BlockchainConfig{Root: root}

	t.Run("gas accounting", func(t *testing.T) {
		for _, tc := range []struct {
			masterchain bool
			got         gasAccounting
			limits      blockLimits
		}{
			{masterchain: false, got: config.gas[0], limits: config.basechain.limits},
			{masterchain: true, got: config.gas[1], limits: config.masterchain.limits},
		} {
			prices, err := raw.GetGasPrices(tc.masterchain)
			if err != nil {
				t.Fatal(err)
			}
			normal, err := semanticGasLimit(tc.limits.gas[3], prices.GasLimit)
			if err != nil {
				t.Fatal(err)
			}
			special, err := semanticGasLimit(tc.limits.gas[3], prices.SpecialGasLimit)
			if err != nil {
				t.Fatal(err)
			}
			if tc.got.err != nil {
				t.Fatalf("%s gas accounting: %v", chainName(tc.masterchain), tc.got.err)
			}
			if tc.got.normal != normal || tc.got.special != special {
				t.Fatalf("%s gas limits = %d/%d, want %d/%d",
					chainName(tc.masterchain), tc.got.normal, tc.got.special, normal, special)
			}
			if tc.got.normal == 0 || tc.got.special == 0 {
				t.Fatalf("%s gas limits are zero, the comparison proves nothing",
					chainName(tc.masterchain))
			}
		}
	})

	t.Run("special accounts", func(t *testing.T) {
		want := freshMasterSpecialAccounts(t, root)
		if len(want) < 2 {
			t.Fatalf("mainnet lists %d special accounts, too few to prove an order", len(want))
		}
		if config.specials.err != nil {
			t.Fatal(config.specials.err)
		}
		if !slices.Equal(config.specials.ordered, want) {
			t.Fatalf("special accounts %x, want %x", config.specials.ordered, want)
		}

		sorted := slices.Clone(want)
		slices.SortFunc(sorted, func(left, right [32]byte) int {
			return bytes.Compare(left[:], right[:])
		})
		if !slices.Equal(config.specials.sorted, sorted) {
			t.Fatalf("sorted special accounts %x, want %x", config.specials.sorted, sorted)
		}
		if slices.Equal(config.specials.ordered, config.specials.sorted) {
			t.Fatal("the mainnet fixture is already sorted, so the order assertion is vacuous")
		}
		if len(config.specials.set) != len(want) {
			t.Fatalf("special account set holds %d, want %d", len(config.specials.set), len(want))
		}
		for _, accountID := range want {
			if _, ok := config.specials.set[accountID]; !ok {
				t.Fatalf("special account %x is missing from the set", accountID)
			}
		}
	})

	t.Run("fee destinations", func(t *testing.T) {
		collector, err := raw.GetFeeCollectorAddress()
		if err != nil {
			t.Fatal(err)
		}
		minter, err := raw.GetMinterAddress()
		if err != nil {
			t.Fatal(err)
		}
		if !config.fees.collector.ok || !bytes.Equal(config.fees.collector.addr[:], collector) {
			t.Fatalf("fee collector %x/%v, want %x",
				config.fees.collector.addr, config.fees.collector.ok, collector)
		}
		if !config.fees.minter.ok || !bytes.Equal(config.fees.minter.addr[:], minter) {
			t.Fatalf("minter %x/%v, want %x", config.fees.minter.addr, config.fees.minter.ok, minter)
		}
	})

	// The fallbacks are the half a single mainnet root cannot exercise, and it
	// cannot exercise them by deletion alone either: mainnet's parameter 1 already
	// holds the same address as its parameter 3, so dropping 3 yields the same
	// answer for the wrong reason. The fallback parameter is therefore rewritten
	// to a value that appears nowhere else in the configuration.
	for _, tc := range []struct {
		name     string
		primary  uint32
		fallback uint32
		got      func(feeDestinations) feeDestination
	}{
		{
			name:     "fee collector falls back to the elector",
			primary:  tlb.ConfigParamFeeCollectorAddress,
			fallback: tlb.ConfigParamElectorAddress,
			got:      func(fees feeDestinations) feeDestination { return fees.collector },
		},
		{
			name:     "minter falls back to the config address",
			primary:  tlb.ConfigParamMinterAddress,
			fallback: tlb.ConfigParamConfigAddress,
			got:      func(fees feeDestinations) feeDestination { return fees.minter },
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var want [32]byte
			want[0], want[31] = 0xfa, byte(tc.fallback)
			rewritten := masterBuildConfigWithParam(t, root, int64(tc.fallback),
				cell.BeginCell().MustStoreSlice(want[:], 256).EndCell())
			rewritten = epochConfigWithoutParam(t, rewritten, tc.primary)

			got := tc.got(epochConfigOf(t, rewritten).fees)
			if !got.ok || got.addr != want {
				t.Fatalf("destination %x/%v, want the fallback %x", got.addr, got.ok, want)
			}
			if tc.got(config.fees).addr == want {
				t.Fatal("the fallback address collides with the primary one")
			}
		})
	}
}

// TestPrepareConfigWithoutConfigAddressDefersRejectionToMasterPaths pins the
// timing of the one rejection that moved into the epoch derivation.
//
// PrepareConfig must still succeed: it also runs from localConfigCache.prepare,
// so failing here would make the whole epoch unpreparable and take SHARD
// collation down over a masterchain-shaped defect. The rejection has to keep
// arriving where it arrives today — at the master consumers.
func TestPrepareConfigWithoutConfigAddressDefersRejectionToMasterPaths(t *testing.T) {
	root := epochConfigWithoutParam(t, loadMainnetConfig(t).execution.Root(), tlb.ConfigParamConfigAddress)
	config := epochConfigOf(t, root)

	if config.specials.err == nil {
		t.Fatal("a configuration without parameter 0 produced a special-account list")
	}
	if !errors.Is(config.specials.err, ErrInvalidInput) ||
		!strings.Contains(config.specials.err.Error(), "config smart contract address is malformed") {
		t.Fatalf("carried rejection = %v", config.specials.err)
	}
	// Nothing else in the epoch is collateral damage: a shard block never asks
	// about special accounts, and its gas allowance still resolves.
	if config.gas[0].err != nil || config.gas[0].normal == 0 {
		t.Fatalf("basechain gas accounting = %+v", config.gas[0])
	}

	collation := &collation{config: config}
	if _, err := collation.masterSpecialAccounts(); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("masterSpecialAccounts error = %v", err)
	}
	if _, err := collation.masterSpecialAccountIDs(); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("masterSpecialAccountIDs error = %v", err)
	}

	replay := &semanticReplay{
		transition: CandidateTransition{Config: config},
		candidate:  &verifiedCandidate{},
	}
	if err := replay.prepareGasAccounting(); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("masterchain gas accounting error = %v", err)
	}
	// The shard path must not see it at all.
	replay.candidate.block.BlockInfo.NotMaster = true
	if err := replay.prepareGasAccounting(); err != nil {
		t.Fatalf("shard gas accounting rejected a masterchain-only defect: %v", err)
	}
	if replay.specials.set != nil {
		t.Fatal("the shard path bound a special-account set")
	}
}

// mintConfigRoot returns the mainnet configuration with parameter 7 replaced by
// the given dictionary, or removed when the dictionary is nil.
func mintConfigRoot(t *testing.T, toMint *cell.Dictionary) *cell.Cell {
	t.Helper()

	base := loadMainnetConfig(t).execution.Root()
	if toMint == nil {
		return epochConfigWithoutParam(t, base, tlb.ConfigParamExtraCurrencyToMint)
	}
	param, err := tlb.ToCell(&tlb.ExtraCurrencyToMintConfig{ToMint: toMint})
	if err != nil {
		t.Fatal(err)
	}
	return masterBuildConfigWithParam(t, base, int64(tlb.ConfigParamExtraCurrencyToMint), param)
}

// TestComputeMintedDisabledWhenParameterAbsent and its malformed-entry twin pin
// the classification computeMinted must keep, and the reason both are here is
// that computeMinted is the one configuration-derived value deliberately left
// per-block: see its doc comment for why hoisting its dictionary walk into
// PrepareConfig would change masterchain block bytes.
//
// Collapsing "absent" into "error" would make every masterchain block on a
// network without parameter 7 uncollatable, matching neither
// Config::get_extra_currency_config nor this node's own shard path.
func TestComputeMintedDisabledWhenParameterAbsent(t *testing.T) {
	root := mintConfigRoot(t, nil)
	// The epoch still prepares: minting disabled is a configuration state, not a
	// defect.
	epochConfigOf(t, root)

	minted, err := computeMinted(tlb.BlockchainConfig{Root: root}, tlb.CurrencyCollection{})
	if err != nil {
		t.Fatalf("absent parameter 7: %v", err)
	}
	if !currencyZero(minted) {
		t.Fatalf("absent parameter 7 minted %+v", minted)
	}
}

// Collapsing "present but malformed inner amount" into "disabled" is the
// opposite protocol bug: we would mint nothing where the reference throws,
// produce a block C++ rejects, and accept such a block as a validator.
func TestComputeMintedRejectsMalformedEntry(t *testing.T) {
	toMint := cell.NewDict(32)
	// A value that is not a var-uint amount: the outer collection decodes, the
	// entry does not.
	if err := toMint.SetIntKey(big.NewInt(7), cell.BeginCell().MustStoreUInt(0xff, 8).EndCell()); err != nil {
		t.Fatal(err)
	}
	root := mintConfigRoot(t, toMint)

	// The epoch prepares, because the defect is masterchain-only. This half is
	// what keeps a shard-only path from dying on it.
	epochConfigOf(t, root)

	_, err := computeMinted(tlb.BlockchainConfig{Root: root}, tlb.CurrencyCollection{})
	if !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("malformed extra currency amount error = %v", err)
	}
}

// masterchainTickTockOrderMainnet is the sequence the masterchain collator
// executes tick and tock in, for the mainnet configuration fixture, written out
// instead of derived.
//
// The reference is C++, in two places:
//
//   - cppnode/ton/crypto/block/mc-config.cpp, Config::get_special_smartcontracts:
//     it walks the parameter-31 dictionary with check_for_each, which visits a
//     vm::Dictionary in ascending key order, appending each key; then, and only
//     when the walk did not already see it, it pushes config_addr at the END.
//   - cppnode/ton/validator/impl/collator.cpp,
//     Collator::create_ticktock_transactions: `for (auto smc_addr : special_smcs)`
//     — that vector, in that order, is the execution order, and the execution
//     order assigns transactions to accounts inside one masterchain block.
//
// Both properties are visible in the literal below and neither survives a
// permutation: entries 0..6 are the seven fundamental contracts in strictly
// ascending order (00 < 0e < 33 < 3b < 4d < dd < ea), and 5555…, the mainnet
// configuration contract, is LAST rather than between 4d5c… and dd24… where
// sorting would put it — because parameter 31 does not list it and C++ appends
// it. Sorting the sequence, reversing it, or moving the appended entry anywhere
// but the end is a masterchain consensus divergence, and this literal is what
// says so without restating the algorithm that produced it.
var masterchainTickTockOrderMainnet = []string{
	"0000000000000000000000000000000000000000000000000000000000000000",
	"0ebd7ff9ca70e06e9e22a8922f5ae75211a9d6a34a8094e8e1587b606bdbb662",
	"3333333333333333333333333333333333333333333333333333333333333333",
	"3b9bbfd0ad5338b9700f0833380ee17d463e51c1ae671ee6f08901bde899b202",
	"4d5c0210b35daddaa219fac459dba0fdefb1fae4e97a0d0797739fe050d694ca",
	"dd24c4a1f2b88f8b7053513b5cc6c5a31bc44b2a72dcb4d8c0338af0f0d37ec5",
	"ead7da389bde317c5fb285807ce507baad31c35fe5534b3c418785e901f64c68",
	"5555555555555555555555555555555555555555555555555555555555555555",
}

// TestMasterSpecialAccountOrderIsTheReferenceOrder is the non-circular half of
// the tick/tock order proof. TestPrepareConfigEpochValuesMatchFreshParse
// compares the order against freshMasterSpecialAccounts, which is the same
// algorithm written twice: it catches the collator losing the order, but it
// cannot catch the order itself being wrong, because both sides would be wrong
// together. This one has no algorithm on the expectation side at all.
func TestMasterSpecialAccountOrderIsTheReferenceOrder(t *testing.T) {
	specials := epochConfigOf(t, loadMainnetConfig(t).execution.Root()).specials
	if specials.err != nil {
		t.Fatalf("prepare mainnet special accounts: %v", specials.err)
	}

	want := make([][32]byte, len(masterchainTickTockOrderMainnet))
	for i, encoded := range masterchainTickTockOrderMainnet {
		raw, err := hex.DecodeString(encoded)
		if err != nil || len(raw) != 32 {
			t.Fatalf("reference entry %d is not a 256-bit account id: %v", i, err)
		}
		want[i] = [32]byte(raw)
	}

	if len(specials.ordered) != len(want) {
		t.Fatalf("tick/tock order has %d accounts, the reference has %d: %x",
			len(specials.ordered), len(want), specials.ordered)
	}
	for i := range want {
		if specials.ordered[i] != want[i] {
			t.Fatalf("tick/tock account %d = %x, the reference order has %x",
				i, specials.ordered[i], want[i])
		}
	}

	// The appended configuration contract is what separates "the C++ order" from
	// "any order over the same set", so it is asserted as a property too: sorting
	// the sequence would move it, and every set-shaped assertion in the package
	// would stay green while the collator executed a different masterchain block.
	if slices.Equal(specials.ordered, specials.sorted) {
		t.Fatal("the tick/tock order coincides with ascending account order, " +
			"so this fixture cannot tell the executed order from a sorted one")
	}
	last := specials.ordered[len(specials.ordered)-1]
	if configured, ok := loadMainnetConfig(t).execution.ConfigAddress(); !ok || configured != last {
		t.Fatalf("tick/tock order ends with %x, want the configuration contract %x", last, configured)
	}
}
