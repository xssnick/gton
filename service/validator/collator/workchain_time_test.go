package collator

import (
	"errors"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestBasechainActivationUsesReferenceMasterchainTime(t *testing.T) {
	req := emptyCandidateRequest(t)
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatal(err)
	}
	for _, test := range []struct {
		name         string
		enabledSince uint32
		reject       bool
	}{
		{name: "no activation delay"},
		{name: "enabled at reference", enabledSince: req.Masterchain.GenUtime},
		{name: "enabled only at candidate", enabledSince: req.Header.GenUtime, reject: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			workchains := cell.NewDict(32)
			description := masterShardFSMTestWorkchainDescriptor(masterShardFSMTestWorkchain{
				active: true, enabledSince: test.enabledSince, maxSplit: 60,
			})
			if err := workchains.SetIntKey(big.NewInt(0), description); err != nil {
				t.Fatal(err)
			}
			root := masterBuildConfigWithParam(t, req.Masterchain.Config.execution.Root(), int64(tlb.ConfigParamWorkchains),
				cell.BeginCell().MustStoreDict(workchains).EndCell())
			changed := req
			changed.Masterchain.Config = epochConfigOf(t, root)
			groups := *req.Masterchain.Groups
			groups.ConfigRootHash = root.HashKey()
			changed.Masterchain.Groups = &groups

			_, buildErr := testBuilder().BuildShard(t.Context(), changed)
			verification := shardVerificationRequest(changed, candidate)
			verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
			verifyErr := verifyShardCandidateForTest(t.Context(), verification)
			for _, err := range []error{buildErr, verifyErr} {
				if !test.reject && err != nil {
					t.Fatalf("enabled workchain rejected: %v", err)
				}
				if test.reject && (!errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "reference masterchain time")) {
					t.Fatalf("workchain enabled after the reference was accepted: %v", err)
				}
			}
		})
	}
}
