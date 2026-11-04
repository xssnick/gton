package collator

import (
	"context"
	"math/big"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
)

// The fixture's unchanged shard is empty; this isolates the real masterchain
// account/queue calculation while avoiding its intentionally synthetic shard ID.
type masterBalanceTestCalculator struct {
	nextDelta int64
}

func (c *masterBalanceTestCalculator) CalculateGlobalBalance(ctx context.Context, master PreviousBlock, version uint32) (*big.Int, error) {
	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, master.State); err != nil {
		return nil, err
	}
	parsed, err := parseGlobalBalanceShard(ctx, &state, blockShardIdent(master.ID), version)
	if err != nil {
		return nil, err
	}
	result, err := sumGlobalBalance(ctx, []*globalBalanceShard{parsed}, nil)
	if err == nil && master.ID.SeqNo != 0 {
		result.Add(result, big.NewInt(c.nextDelta))
	}
	return result, err
}

func TestGV17MasterUsesActualBalanceAtActivation(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	fixture.request.Config.globalVersion = 17
	fixture.request.ShardTops = nil
	calculator := &masterBalanceTestCalculator{}
	fixture.request.GlobalBalance = calculator
	ctx := context.Background()
	previous, err := calculator.CalculateGlobalBalance(ctx, fixture.request.Previous, 17)
	if err != nil {
		t.Fatal(err)
	}
	if previous.Cmp(fixture.oldExtra.GlobalBalance.Coins.NanoRef()) == 0 {
		t.Fatal("activation fixture must differ from its historical stored balance")
	}
	builder := testBuilder()
	candidate, err := builder.BuildMaster(ctx, fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	var extra tlb.McStateExtra
	if err = parseExact(&extra, state.McStateExtra); err != nil {
		t.Fatal(err)
	}
	actual, err := calculator.CalculateGlobalBalance(ctx, PreviousBlock{ID: candidate.ID, State: candidate.State}, 17)
	if err != nil {
		t.Fatal(err)
	}
	if actual.Cmp(extra.GlobalBalance.Coins.NanoRef()) != 0 {
		t.Fatalf("stored %s, actual %s", extra.GlobalBalance.Coins.NanoRef(), actual)
	}
	request := MasterVerificationRequest{
		Previous: fixture.request.Previous, Config: fixture.request.Config,
		Groups: fixture.request.Groups, GlobalBalance: calculator,
		Semantics: NewSemanticVerifier(builder.machine), Candidate: candidate,
	}
	if err = verifyMasterCandidateForTest(ctx, request); err != nil {
		t.Fatal(err)
	}
	calculator.nextDelta = 1
	if err = verifyMasterCandidateForTest(ctx, request); err == nil || !strings.Contains(err.Error(), "stored global balance") {
		t.Fatalf("inconsistent acquired state balance was accepted: %v", err)
	}
	request.GlobalBalance = nil
	if err = verifyMasterCandidateForTest(ctx, request); err == nil || !strings.Contains(err.Error(), "requires a global balance") {
		t.Fatalf("missing calculator was accepted by validation: %v", err)
	}
	fixture.request.GlobalBalance = nil
	if _, err = builder.BuildMaster(ctx, fixture.request); err == nil || !strings.Contains(err.Error(), "requires a global balance") {
		t.Fatalf("missing calculator was accepted by collation: %v", err)
	}
}
