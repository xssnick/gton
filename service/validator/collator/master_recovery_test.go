package collator

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
)

func TestVerifyMasterCandidateAllowsOptionalFeeRecovery(t *testing.T) {
	fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{
		genUtime: uint32(time.Now().Unix()) - 120,
	})
	if !fixture.request.Config.fees.collector.ok {
		t.Fatal("fixture has no fee collector")
	}
	if fixture.request.Config.masterchain.createFee.Nano().Uint64() < 1_000_000_000 {
		t.Fatal("fixture does not reach the collator's recovery threshold")
	}

	for _, recoverFees := range []bool{true, false} {
		name := "with recovery"
		if !recoverFees {
			name = "without recovery"
		}
		t.Run(name, func(t *testing.T) {
			// Suppress only the builder's recovery policy. The config cell and
			// the verifier's config still contain the real fee collector.
			buildConfig := *fixture.request.Config
			buildConfig.fees.collector.ok = recoverFees
			buildRequest := fixture.request
			buildRequest.Config = &buildConfig
			candidate, err := testBuilder().BuildMaster(context.Background(), buildRequest)
			if err != nil {
				t.Fatal(err)
			}
			request := MasterVerificationRequest{
				Previous:           fixture.request.Previous,
				Config:             fixture.request.Config,
				Groups:             fixture.request.Groups,
				ShardTops:          fixture.request.ShardTops,
				Neighbors:          fixture.request.Neighbors,
				NeighborShardEndLT: fixture.request.NeighborShardEndLT,
				Semantics:          NewSemanticVerifier(tvm.NewTVM()),
				Candidate:          candidate,
			}
			verified, err := verifyCandidate(context.Background(), request.Config, candidate)
			if err != nil {
				t.Fatal(err)
			}
			if currencyZero(verified.flow.Recovered) == recoverFees ||
				(verified.block.Extra.Custom.Details.RecoverCreateMsg != nil) != recoverFees {
				t.Fatal("candidate recovery does not follow the requested builder policy")
			}
			if err = VerifyMasterCandidate(context.Background(), request); err != nil {
				t.Fatalf("verify candidate including semantic replay: %v", err)
			}

			previous, err := verifyPredecessor("master", &request.Previous)
			if err != nil {
				t.Fatal(err)
			}
			state, err := loadMasterCandidateState(request.Config, &previous, &verified, request.Groups)
			if err != nil {
				t.Fatal(err)
			}
			if err = verifyMasterValueFlow(request.Config, &verified, &state); err != nil {
				t.Fatalf("verify value flow before mutations: %v", err)
			}

			t.Run("recovery message presence must match amount", func(t *testing.T) {
				changed := verified
				changed.flow.Recovered = tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(1)}
				if recoverFees {
					changed.flow.Recovered = tlb.CurrencyCollection{}
				}
				err := verifyMasterValueFlow(request.Config, &changed, &state)
				if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "special messages") {
					t.Fatalf("mismatched recovery message presence error = %v", err)
				}
			})

			t.Run("unrecovered fees must remain in state", func(t *testing.T) {
				changed := verified
				changed.stats.TotalValidatorFees, err = changed.stats.TotalValidatorFees.Add(
					tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(1)},
				)
				if err != nil {
					t.Fatal(err)
				}
				err := verifyMasterValueFlow(request.Config, &changed, &state)
				if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "resulting validator fees") {
					t.Fatalf("mismatched retained fees error = %v", err)
				}
			})
		})
	}
}
