package collator

import (
	"bytes"
	"context"
	"errors"
	"math/big"
	"testing"
	"testing/synctest"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type configAddressTransitionCase struct {
	name string
	root *cell.Cell
}

type cachedConfigAddressCase struct {
	name     string
	prepared localPreparedConfig
	actual   [32]byte
	other    [32]byte
}

func TestMasterConfigStateAddressSurvivesKeyBlock(t *testing.T) {
	base := loadMainnetConfig(t).execution.Root()
	mandatory, err := (tlb.BlockchainConfig{Root: base}).GetParam(9)
	if err != nil {
		t.Fatal(err)
	}
	// Parameter 0 may be removed only when neither the previous nor the new
	// mandatory set requires it.
	required := mandatory.AsDict(32).Copy()
	if err = required.DeleteIntKey(big.NewInt(0)); err != nil {
		t.Fatal(err)
	}
	base = masterBuildConfigWithParam(t, base, 9, required.AsCell())
	requested := [32]byte{0x67, 0x10}

	for _, tc := range []configAddressTransitionCase{
		{
			name: "optional parameter zero removed",
			root: epochConfigWithoutParam(t, base, 0),
		},
		{
			name: "requested contract is absent",
			root: masterBuildConfigWithParam(t, base, 0,
				cell.BeginCell().MustStoreSlice(requested[:], 256).EndCell()),
		},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{
					configRoot:        base,
					accountConfigRoot: tc.root,
				})
				actual := [32]byte(fixture.oldExtra.ConfigParams.ConfigAddr)
				time.Sleep(time.Unix(int64(fixture.request.Header.GenUtime), 0).Sub(time.Now()))

				var absent cell.Slice
				if err := fixture.oldState.Accounts.ShardAccounts.LoadValueByBytesKeyInto(requested[:], &absent); err == nil {
					t.Fatal("fixture contains the requested replacement contract")
				}
				keyBlock, err := testBuilder().BuildMaster(context.Background(), fixture.request)
				if err != nil {
					t.Fatalf("build configuration key block: %v", err)
				}
				assertMasterConfigAddressCandidate(t, fixture.request, keyBlock, tc.root, actual, true)

				next := fixture.request
				next.Previous.ID = keyBlock.ID
				next.Previous.State = keyBlock.State
				next.Config = testPrepareConfigAt(t, tc.root, actual)
				next.Groups = benchMasterSnapshot(t, keyBlock.ID, keyBlock.State, next.Header.GenUtime)
				next.Header.GenUtime++
				next.Header.GenUtimeMS += 1000
				next.ShardTops = nil
				if _, exists := next.Config.specials.set[actual]; !exists {
					t.Fatal("actual configuration contract lost its special status")
				}
				if _, exists := next.Config.specials.set[requested]; exists {
					t.Fatal("absent requested contract became a special account")
				}
				successor, err := testBuilder().BuildMaster(context.Background(), next)
				if err != nil {
					t.Fatalf("build successor in the installed configuration: %v", err)
				}
				assertMasterConfigAddressCandidate(t, next, successor, tc.root, actual, false)
			})
		})
	}
}

func assertMasterConfigAddressCandidate(
	t *testing.T,
	request MasterRequest,
	candidate *Candidate,
	root *cell.Cell,
	actual [32]byte,
	keyBlock bool,
) {
	t.Helper()

	if err := VerifyMasterCandidate(context.Background(), MasterVerificationRequest{
		Previous:           request.Previous,
		Config:             request.Config,
		Groups:             request.Groups,
		ShardTops:          request.ShardTops,
		Neighbors:          request.Neighbors,
		NeighborShardEndLT: request.NeighborShardEndLT,
		Semantics:          NewSemanticVerifier(tvm.NewTVM()),
		Candidate:          candidate,
	}); err != nil {
		t.Fatalf("verify masterchain candidate: %v", err)
	}
	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	var extra tlb.McStateExtra
	if err := parseExact(&extra, state.McStateExtra); err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(extra.ConfigParams.ConfigAddr, actual[:]) ||
		extra.ConfigParams.Config.Params.AsCell().HashKey() != root.HashKey() {
		t.Fatal("candidate changed the actual configuration address or installed the wrong root")
	}
	var blockExtra tlb.McBlockExtra
	if err := parseExact(&blockExtra, verificationMasterCustomCell(t, candidate)); err != nil {
		t.Fatal(err)
	}
	if blockExtra.KeyBlock != keyBlock {
		t.Fatalf("key block = %v, want %v", blockExtra.KeyBlock, keyBlock)
	}
}

func TestLocalConfigCacheDistinguishesStateAddress(t *testing.T) {
	root := loadMainnetConfig(t).execution.Root()
	cache := localConfigCache{entries: make(map[localConfigKey]localPreparedConfig)}
	firstAddress := [32]byte{0x67, 0x20}
	secondAddress := [32]byte{0x67, 0x30}
	first, err := cache.prepare(root, firstAddress)
	if err != nil {
		t.Fatal(err)
	}
	second, err := cache.prepare(root, secondAddress)
	if err != nil {
		t.Fatal(err)
	}
	if first.config == second.config {
		t.Fatal("different actual configuration addresses reused the same prepared config")
	}
	for _, tc := range []cachedConfigAddressCase{
		{name: "first address", prepared: first, actual: firstAddress, other: secondAddress},
		{name: "second address", prepared: second, actual: secondAddress, other: firstAddress},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.prepared.config.configAddress != tc.actual {
				t.Fatal("cache returned a config for another state address")
			}
			if _, exists := tc.prepared.config.specials.set[tc.actual]; !exists {
				t.Fatal("actual configuration contract is not a special account")
			}
			if _, exists := tc.prepared.config.specials.set[tc.other]; exists {
				t.Fatal("another state's configuration contract is a special account")
			}
			again, err := cache.prepare(root, tc.actual)
			if err != nil {
				t.Fatal(err)
			}
			if again.config != tc.prepared.config {
				t.Fatal("identical root and actual address did not reuse the cached config")
			}
		})
	}
}

func TestMasterRejectsConfigPreparedForAnotherStateAddress(t *testing.T) {
	fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{})
	candidate, err := testBuilder().BuildMaster(t.Context(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	wrong := testPrepareConfigAt(t, fixture.request.Config.execution.Root(), [32]byte{0x67, 0x40})
	request := fixture.request
	request.Config = wrong
	if _, err := testBuilder().BuildMaster(t.Context(), request); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("collation with another state's config address: %v", err)
	}
	if err := verifyMasterCandidateForTest(t.Context(), MasterVerificationRequest{
		Previous: request.Previous, Config: wrong, Groups: request.Groups,
		ShardTops: request.ShardTops, Neighbors: request.Neighbors,
		NeighborShardEndLT: request.NeighborShardEndLT,
		Semantics:          NewSemanticVerifier(tvm.NewTVM()), Candidate: candidate,
	}); !errors.Is(err, ErrInvalidInput) {
		t.Fatalf("validation with another state's config address: %v", err)
	}
}
