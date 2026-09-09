package collator

import (
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestMasterConfigSupportedJettonBridgeParameters(t *testing.T) {
	base := loadMainnetConfig(t).execution.Root()
	parameter := masterConfigJettonBridgeParameter()
	for _, id := range []int64{79, 81, 82} {
		t.Run(fmt.Sprint(id), func(t *testing.T) {
			root := masterBuildConfigWithParam(t, base, id, parameter)
			fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{accountConfigRoot: root})
			candidate, err := testBuilder().BuildMaster(t.Context(), fixture.request)
			if err != nil {
				t.Fatal(err)
			}
			if err := verifyMasterCandidateForTest(t.Context(), masterConfigBridgeVerification(fixture, candidate)); err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestMasterConfigRejectsParameter80(t *testing.T) {
	base := loadMainnetConfig(t).execution.Root()
	parameter := masterConfigJettonBridgeParameter()
	invalidRoot := masterBuildConfigWithParam(t, base, 80, parameter)
	invalidFixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{accountConfigRoot: invalidRoot})
	t.Run("collation", func(t *testing.T) {
		_, err := testBuilder().BuildMaster(t.Context(), invalidFixture.request)
		assertInvalidConfigParameter(t, err, 80)
	})
	t.Run("validation", func(t *testing.T) {
		validRoot := masterBuildConfigWithParam(t, base, 79, parameter)
		fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{accountConfigRoot: validRoot})
		candidate, err := testBuilder().BuildMaster(t.Context(), fixture.request)
		if err != nil {
			t.Fatal(err)
		}
		verification := masterConfigBridgeVerification(fixture, candidate)
		if err = verifyMasterCandidateForTest(t.Context(), verification); err != nil {
			t.Fatal(err)
		}

		// Replace the supported bridge parameter update with parameter 80.
		// The config account has no transactions; bind the changed endpoints and
		// history before checking the complete candidate's semantic transition.
		forged := cloneVerificationCandidate(candidate)
		forged.State = replaceMasterConfigTestCell(t, candidate.State, validRoot.HashKey(), invalidRoot)
		previous := invalidFixture.request.Previous
		forged.State = masterConfigTestRewriteHistory(t, forged.State, previous)
		update, err := cell.CreateMerkleUpdate(previous.State, forged.State)
		if err != nil {
			t.Fatal(err)
		}
		forged.StateUpdate = update
		custom := replaceMasterConfigTestCell(t, verificationMasterCustomCell(t, candidate), validRoot.HashKey(), invalidRoot)
		rewriteVerificationMasterBlock(t, forged, custom, func(block *tlb.Block) {
			block.StateUpdate = update
			block.BlockInfo.PrevRef.Prev1.RootHash = previous.ID.RootHash
			block.BlockInfo.PrevRef.Prev1.FileHash = previous.ID.FileHash
		})
		verification.Previous = previous
		verification.Groups = invalidFixture.request.Groups
		verification.Candidate = forged
		assertInvalidConfigParameter(t, verifyMasterCandidateForTest(t.Context(), verification), 80)
	})
}

func masterConfigJettonBridgeParameter() *cell.Cell {
	prices := cell.BeginCell()
	for price := uint64(1); price <= 6; price++ {
		prices.MustStoreCoins(price)
	}
	return cell.BeginCell().MustStoreUInt(1, 8).
		MustStoreUInt(1, 256).MustStoreUInt(2, 256).
		MustStoreBoolBit(false).MustStoreUInt(0, 8).
		MustStoreRef(prices.EndCell()).MustStoreUInt(3, 256).EndCell()
}

func masterConfigBridgeVerification(fixture masterBuildFixture, candidate *Candidate) MasterVerificationRequest {
	return MasterVerificationRequest{
		Previous: fixture.request.Previous, Config: fixture.request.Config,
		Groups: fixture.request.Groups, ShardTops: fixture.request.ShardTops,
		Neighbors: fixture.request.Neighbors, NeighborShardEndLT: fixture.request.NeighborShardEndLT,
		Semantics: NewSemanticVerifier(tvm.NewTVM()), Candidate: candidate,
	}
}
