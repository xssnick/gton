package collator

import (
	"context"
	"errors"
	"fmt"
	"math"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type mandatoryConfigScenario struct {
	name      string
	oldRoot   *cell.Cell
	newRoot   *cell.Cell
	missing   bool
	inherited bool
}

func TestMasterConfigNegativeMandatoryParameters(t *testing.T) {
	base := loadMainnetConfig(t).execution.Root()

	for _, id := range []int32{-42, math.MinInt32} {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			withParameter := masterBuildConfigWithParam(t, base, int64(id),
				cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell())
			requiringParameter := masterConfigWithMandatoryParameter(t, withParameter, id)
			requiringMissing := masterConfigWithMandatoryParameter(t, base, id)

			cases := []mandatoryConfigScenario{
				{name: "new set present", oldRoot: base, newRoot: requiringParameter},
				{name: "new set missing", oldRoot: base, newRoot: requiringMissing, missing: true},
				{name: "inherited set present", oldRoot: requiringParameter, newRoot: withParameter, inherited: true},
				{name: "inherited set missing", oldRoot: requiringParameter, newRoot: base, missing: true, inherited: true},
			}
			for _, tc := range cases {
				t.Run(tc.name, func(t *testing.T) {
					fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{
						configRoot: tc.oldRoot, accountConfigRoot: tc.newRoot,
					})
					candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
					if tc.missing {
						if !errors.Is(err, ErrInvalidInput) ||
							!strings.Contains(err.Error(), fmt.Sprintf("parameter %d is absent", id)) {
							t.Fatalf("missing signed mandatory parameter: %v", err)
						}
						if tc.inherited && !strings.Contains(err.Error(), "old mandatory set") {
							t.Fatalf("predecessor requirement was not enforced: %v", err)
						}
						return
					}
					if err != nil {
						t.Fatalf("build with present mandatory parameter %d: %v", id, err)
					}

					verification := MasterVerificationRequest{
						Previous: fixture.request.Previous, Config: fixture.request.Config,
						Groups: fixture.request.Groups, ShardTops: fixture.request.ShardTops,
						Neighbors: fixture.request.Neighbors, NeighborShardEndLT: fixture.request.NeighborShardEndLT,
						Semantics: NewSemanticVerifier(tvm.NewTVM()), Candidate: candidate,
					}
					if err = verifyMasterCandidateForTest(context.Background(), verification); err != nil {
						t.Fatalf("verify with present mandatory parameter %d: %v", id, err)
					}
				})
			}
		})
	}
}

func masterConfigWithMandatoryParameter(t *testing.T, root *cell.Cell, id int32) *cell.Cell {
	t.Helper()

	parameter, err := (tlb.BlockchainConfig{Root: root}).GetParam(tlb.ConfigParamMandatoryParams)
	if err != nil {
		t.Fatal(err)
	}
	dict := parameter.AsDict(32).Copy()
	if err = dict.SetIntKey(big.NewInt(int64(id)), cell.BeginCell().EndCell()); err != nil {
		t.Fatal(err)
	}
	return masterBuildConfigWithParam(t, root, int64(tlb.ConfigParamMandatoryParams), dict.AsCell())
}
