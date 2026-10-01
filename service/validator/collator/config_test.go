package collator

import (
	"context"
	"errors"
	"math"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
)

type candidateSizeLimitsTestCase struct {
	name         string
	config       any
	wantBlock    uint32
	wantCollated uint32
	wantErr      bool
}

func TestCandidateSizeLimits(t *testing.T) {
	tests := []candidateSizeLimitsTestCase{
		{name: "default", wantBlock: defaultCandidateSizeLimit, wantCollated: defaultCandidateSizeLimit},
		{name: "v1", config: tlb.ConsensusConfigV1{MaxBlockBytes: 11, MaxCollatedBytes: 12}, wantBlock: 11, wantCollated: 12},
		{name: "v2", config: tlb.ConsensusConfigV2{MaxBlockBytes: 21, MaxCollatedBytes: 22}, wantBlock: 21, wantCollated: 22},
		{name: "v3", config: tlb.ConsensusConfigV3{MaxBlockBytes: 31, MaxCollatedBytes: 32}, wantBlock: 31, wantCollated: 32},
		{name: "v4", config: tlb.ConsensusConfigV4{MaxBlockBytes: 41, MaxCollatedBytes: 42}, wantBlock: 41, wantCollated: 42},
		{name: "unsupported", config: &tlb.ConsensusConfigV4{}, wantErr: true},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			block, collated, err := candidateSizeLimits(tlb.ConsensusConfig{Config: test.config})
			if (err != nil) != test.wantErr {
				t.Fatalf("error = %v, wantErr %t", err, test.wantErr)
			}
			if block != test.wantBlock || collated != test.wantCollated {
				t.Fatalf("limits = (%d, %d), want (%d, %d)", block, collated, test.wantBlock, test.wantCollated)
			}
		})
	}
}

func TestVerifyBasechainWorkchainPolicy(t *testing.T) {
	base := loadMainnetConfig(t)
	if !base.basechainWorkchain.present || !base.basechainWorkchain.basic || !base.basechainWorkchain.active {
		t.Fatalf("unexpected mainnet basechain policy: %+v", base.basechainWorkchain)
	}
	tests := []struct {
		name   string
		policy workchainPolicy
		time   uint32
		valid  bool
	}{
		{name: "active basic", policy: workchainPolicy{present: true, basic: true, active: true, enabledSince: 10}, time: 10, valid: true},
		{name: "absent"},
		{name: "inactive", policy: workchainPolicy{present: true, basic: true}},
		{name: "extended", policy: workchainPolicy{present: true, active: true}},
		{name: "not enabled", policy: workchainPolicy{present: true, basic: true, active: true, enabledSince: 11}, time: 10},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			config := *base
			config.basechainWorkchain = test.policy
			err := verifyBasechainWorkchain(&config, test.time)
			if (err == nil) != test.valid {
				t.Fatalf("policy error = %v, valid=%t", err, test.valid)
			}
		})
	}
}

func TestBuildCandidateEnforcesSerializedSizeLimits(t *testing.T) {
	req := emptyCandidateRequest(t)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	blockSize := uint32(len(candidate.BlockBOC))
	collatedSize := uint32(len(candidate.CollatedData))
	req.Masterchain.Config.maxBlockBytes = blockSize
	req.Masterchain.Config.maxCollatedBytes = math.MaxUint32
	if _, err = testBuilder().BuildShard(context.Background(), req); err != nil {
		t.Fatalf("block at exact size limit: %v", err)
	}
	req.Masterchain.Config.maxBlockBytes = blockSize - 1
	if _, err = testBuilder().BuildShard(context.Background(), req); !errors.Is(err, ErrSizeLimit) {
		t.Fatalf("oversized block error = %v, want ErrSizeLimit", err)
	}

	req.Masterchain.Config.maxBlockBytes = math.MaxUint32
	req.Masterchain.Config.maxCollatedBytes = collatedSize
	if _, err = testBuilder().BuildShard(context.Background(), req); err != nil {
		t.Fatalf("collated data at exact size limit: %v", err)
	}
	req.Masterchain.Config.maxCollatedBytes = collatedSize - 1
	if _, err = testBuilder().BuildShard(context.Background(), req); !errors.Is(err, ErrSizeLimit) {
		t.Fatalf("oversized collated data error = %v, want ErrSizeLimit", err)
	}
}
