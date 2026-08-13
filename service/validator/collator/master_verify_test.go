package collator

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"

	"github.com/xssnick/gton/service/validator/groups"
)

func TestVerifyMasterAugmentedValueFlow(t *testing.T) {
	currency := func(nano uint64) tlb.CurrencyCollection {
		return tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(nano)}
	}
	config := &Config{burning: tlb.BurningConfig{FeeBurnNum: 1, FeeBurnDenom: 2}}
	expected := masterCollation{
		feesImported: currency(100),
		importBurned: currency(40),
		created:      currency(5),
	}
	augmentations := candidateValueFlowAugmentations{
		importFees: tlb.ImportFees{
			FeesCollected: tlb.FromNanoTONU(10),
			ValueImported: currency(70),
		},
		exported:        currency(20),
		transactionFees: currency(30),
	}
	// Imported shard fees contribute 60 after burning, ordinary fees contribute
	// 20 after burning, and block creation contributes 5. Fee burning itself is
	// 40 + 20; any declared amount above that belongs to transaction replay.
	valid := tlb.ValueFlow{
		Imported:      currency(70),
		Exported:      currency(20),
		FeesCollected: currency(85),
		Burned:        currency(60),
	}
	minimumBurned, err := verifyMasterAugmentedValueFlow(config, &expected, augmentations, &valid)
	if err != nil {
		t.Fatalf("verify valid augmented value flow: %v", err)
	}
	if !minimumBurned.Equals(currency(60)) {
		t.Fatalf("minimum burned = %s, want 60 nanotons", minimumBurned.Coins.String())
	}

	tests := []struct {
		name   string
		mutate func(*tlb.ValueFlow)
	}{
		{name: "imported", mutate: func(flow *tlb.ValueFlow) { flow.Imported = currency(71) }},
		{name: "exported", mutate: func(flow *tlb.ValueFlow) { flow.Exported = currency(21) }},
		{name: "collected fees", mutate: func(flow *tlb.ValueFlow) { flow.FeesCollected = currency(86) }},
		{name: "fee burning", mutate: func(flow *tlb.ValueFlow) { flow.Burned = currency(59) }},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			changed := valid
			test.mutate(&changed)
			_, err := verifyMasterAugmentedValueFlow(config, &expected, augmentations, &changed)
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("verification error = %v, want ErrInvalidInput", err)
			}
		})
	}

	t.Run("blackhole remainder", func(t *testing.T) {
		changed := valid
		changed.Burned = currency(67)
		_, err := verifyMasterAugmentedValueFlow(config, &expected, augmentations, &changed)
		if err != nil {
			t.Fatalf("blackhole remainder rejected: %v", err)
		}
	})
}

func TestVerifyMasterCandidateBindsDeterministicInputs(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	request := MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    fixture.request.Config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Semantics: testCandidateTransitionVerifier,
		Candidate: candidate,
	}
	if err = VerifyMasterCandidate(context.Background(), request); err != nil {
		t.Fatalf("verify baseline candidate: %v", err)
	}

	t.Run("missing group snapshot", func(t *testing.T) {
		changed := request
		changed.Groups = nil
		err := VerifyMasterCandidate(context.Background(), changed)
		if err == nil || !strings.Contains(err.Error(), "group snapshot is absent") {
			t.Fatalf("missing snapshot error = %v", err)
		}
	})

	t.Run("stale group snapshot", func(t *testing.T) {
		changed := request
		snapshot := *request.Groups
		snapshot.MasterchainBlock.RootHash = bytes.Clone(snapshot.MasterchainBlock.RootHash)
		snapshot.MasterchainBlock.RootHash[0] ^= 1
		changed.Groups = &snapshot
		err := VerifyMasterCandidate(context.Background(), changed)
		if err == nil || !strings.Contains(err.Error(), "not derived from the predecessor") {
			t.Fatalf("stale snapshot error = %v", err)
		}
	})

	t.Run("active session", func(t *testing.T) {
		changed := request
		snapshot := *request.Groups
		snapshot.Active = append([]groups.Session(nil), request.Groups.Active...)
		for i := range snapshot.Active {
			if snapshot.Active[i].Shard.IsMasterchain() {
				snapshot.Active[i].ValidatorSetHash++
			}
		}
		changed.Groups = &snapshot
		err := VerifyMasterCandidate(context.Background(), changed)
		if err == nil || !strings.Contains(err.Error(), "active validator session") {
			t.Fatalf("active session error = %v", err)
		}
	})

	t.Run("shard descriptors", func(t *testing.T) {
		changed := request
		changed.ShardTops = nil
		err := VerifyMasterCandidate(context.Background(), changed)
		if err == nil || !strings.Contains(err.Error(), "unexpected top block") {
			t.Fatalf("missing shard inputs error = %v", err)
		}
	})

	t.Run("shard creators", func(t *testing.T) {
		changed := request
		changed.ShardTops = append([]ShardTop(nil), request.ShardTops...)
		changed.ShardTops[0].Creators = append([][32]byte(nil), request.ShardTops[0].Creators...)
		changed.ShardTops[0].Creators[0][0] ^= 1
		err := VerifyMasterCandidate(context.Background(), changed)
		if err == nil || !strings.Contains(err.Error(), "creator statistics") {
			t.Fatalf("wrong shard creator error = %v", err)
		}
	})
}

func TestVerifyMasterCandidateSemanticCore(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	request := MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    fixture.request.Config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Semantics: testCandidateTransitionVerifier,
		Candidate: candidate,
	}
	verified, err := verifyCandidate(context.Background(), request.Config, request.Candidate)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := verifyPredecessor("master", &request.Previous)
	if err != nil {
		t.Fatal(err)
	}
	state, err := loadMasterCandidateState(request.Config, &previous, &verified)
	if err != nil {
		t.Fatal(err)
	}
	if err = verifyMasterDeterministicTransition(request, &previous, &verified, &state); err != nil {
		t.Fatalf("verify semantic baseline: %v", err)
	}

	t.Run("start lt", func(t *testing.T) {
		changed := verified
		changed.block = verified.block
		_, maxEndLT, prepareErr := prepareMasterShardTops(fixture.request, verified.block.BlockInfo.SeqNo)
		if prepareErr != nil {
			t.Fatal(prepareErr)
		}
		changed.block.BlockInfo.StartLt = max(previous.GenLT, maxEndLT) + masterMaxLTGap + 1
		_, _, err := verifyMasterShardTransition(request, &previous, &changed, &state)
		if err == nil || !strings.Contains(err.Error(), "start lt") {
			t.Fatalf("start lt error = %v", err)
		}
	})

	t.Run("shard fees", func(t *testing.T) {
		changed := cloneVerifiedMasterCandidate(verified)
		top := request.ShardTops[0]
		leaf, decodeErr := decodeShardRegistryLeaf(top.Block.Workchain, top.Block.Shard, top.Descriptor)
		if decodeErr != nil {
			t.Fatal(decodeErr)
		}
		leaf.fields.fees = tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(1)}
		wrongFees, makeErr := buildShardFeesDictionary(map[shardRegistryKey]shardRegistryLeaf{
			shardTopKey(top.Block): leaf,
		})
		if makeErr != nil {
			t.Fatal(makeErr)
		}
		changed.block.Extra.Custom.ShardFees = wrongFees
		_, _, verifyErr := verifyMasterShardTransition(request, &previous, &changed, &state)
		if verifyErr == nil || !strings.Contains(verifyErr.Error(), "collected fees") {
			t.Fatalf("shard fees error = %v", verifyErr)
		}
	})

	t.Run("validator info", func(t *testing.T) {
		changed := state
		changed.nextInfo.ValidatorInfo.CatchainSeqno++
		err := verifyMasterStateInfoTransition(
			request,
			&previous,
			&verified,
			&changed,
			request.ShardTops,
		)
		if err == nil || !strings.Contains(err.Error(), "validator info") {
			t.Fatalf("validator info error = %v", err)
		}
	})

	t.Run("creator stats", func(t *testing.T) {
		changed := state
		// The statistics are decoded once with the state info, so the mutation
		// belongs to the decoded value rather than to the cell it came from.
		changed.nextCreators = blockCreateStats{entries: map[[32]byte]creatorStats{}}
		err := verifyMasterStateInfoTransition(
			request,
			&previous,
			&verified,
			&changed,
			request.ShardTops,
		)
		if err == nil || !strings.Contains(err.Error(), "creator statistics") {
			t.Fatalf("creator stats error = %v", err)
		}
	})

	t.Run("min ref", func(t *testing.T) {
		changed := verified
		changed.state.MinRefMCSeqno++
		registry, _, err := verifyMasterShardTransition(request, &previous, &verified, &state)
		if err != nil {
			t.Fatal(err)
		}
		err = verifyMasterMinRefMCSeqno(&changed, registry)
		if err == nil || !strings.Contains(err.Error(), "min ref mc seqno") {
			t.Fatalf("min ref error = %v", err)
		}
	})
}

func cloneVerifiedMasterCandidate(source verifiedCandidate) verifiedCandidate {
	cloned := source
	cloned.block = source.block
	extra := *source.block.Extra
	custom := *source.block.Extra.Custom
	extra.Custom = &custom
	cloned.block.Extra = &extra
	return cloned
}
