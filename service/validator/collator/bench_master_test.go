package collator

import (
	"context"
	"testing"

	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
)

// Masterchain collation is a different shape of work from a shard: the block
// carries few transactions but rebuilds McStateExtra around them — shard
// hashes, creator statistics, the previous-blocks history — and its limits are
// far tighter (1M gas soft against the basechain's 10M). These benchmarks are
// therefore sized by masterchain accounts rather than by message throughput.
//
// One shard reports its top block, matching the single-basechain-shard topology
// the reference node's own throughput benchmark assumes. Multi-shard tops would
// need a shard binary tree rather than the fixture's single-leaf one.

type benchMasterProfile struct {
	name string
	// accounts is how many masterchain contracts sit in the state on top of the
	// configuration account the fixture always creates.
	accounts int
	// externals land on those accounts, one each.
	externals int
}

var benchMasterProfiles = []benchMasterProfile{
	{name: "idle", accounts: 1_000},
	{name: "light", accounts: 10_000, externals: 25},
	{name: "typical", accounts: 50_000, externals: 100},
}

type benchMasterWorkload struct {
	profile   benchMasterProfile
	request   MasterRequest
	candidate *Candidate
}

func benchMasterWorkloadFor(tb testing.TB, profile benchMasterProfile) *benchMasterWorkload {
	tb.Helper()

	benchWorkloadMu.Lock()
	defer benchWorkloadMu.Unlock()
	if cached, ok := benchMasterCache[profile.name]; ok {
		return cached
	}
	built := buildBenchMasterWorkload(tb, profile)
	benchMasterCache[profile.name] = built
	return built
}

var benchMasterCache = map[string]*benchMasterWorkload{}

func buildBenchMasterWorkload(tb testing.TB, profile benchMasterProfile) *benchMasterWorkload {
	tb.Helper()

	req := benchMasterRequest(tb, profile)
	candidate, err := testBuilder().BuildMaster(context.Background(), req)
	if err != nil {
		tb.Fatalf("build %s masterchain candidate: %v", profile.name, err)
	}
	if int(candidate.Stats.ExternalIncluded) != profile.externals {
		tb.Fatalf("%s profile does not fit the masterchain limits: included %d/%d externals (load class %v)",
			profile.name, candidate.Stats.ExternalIncluded, profile.externals, candidate.Stats.Load)
	}

	return &benchMasterWorkload{profile: profile, request: req, candidate: candidate}
}

// benchMasterRequest takes the masterchain fixture — a real McStateExtra with
// config, validator info and a shard descriptor — and rewrites its state with
// the profile's accounts. The block id follows the state, and the validator
// group snapshot is re-derived from it, because both are bound to the state
// this request declares as its predecessor.
func benchMasterRequest(tb testing.TB, profile benchMasterProfile) MasterRequest {
	tb.Helper()

	fixture := newMasterBuildFixture(tb, false)
	req := fixture.request
	genUtime := fixture.oldState.GenUTime

	accounts := fixture.oldState.Accounts.ShardAccounts
	senders := make([]*address.Address, profile.externals)
	for i := range senders {
		senders[i] = benchMasterAddress("mc-sender", i)
		benchSetAccount(tb, accounts, activeContract{
			address: senders[i],
			code:    benchWorkCode(tb, benchSenderLoops, nil),
			balance: 100_000_000_000,
		}, genUtime)
	}
	for i := profile.externals; i < profile.accounts; i++ {
		benchSetAccount(tb, accounts, activeContract{
			address: benchMasterAddress("mc-idle", i),
			code:    externalAcceptCode(tb),
			balance: 1_000_000_000,
		}, genUtime)
	}

	balance, err := loadAccountsBalance(accounts)
	if err != nil {
		tb.Fatal(err)
	}
	stats := fixture.oldStats
	stats.TotalBalance = balance
	statsRoot, err := stats.ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	state := fixture.oldState
	state.Stats = statsRoot
	state.Accounts.ShardAccounts = accounts
	stateRoot, err := tlb.ToCell(&state)
	if err != nil {
		tb.Fatal(err)
	}

	stateHash := stateRoot.HashKey()
	req.Previous.ID.RootHash = stateHash[:]
	req.Previous.State = stateRoot
	req.Groups = benchMasterSnapshot(tb, req.Previous.ID, stateRoot, genUtime)

	req.Externals = make([]ExternalInput, profile.externals)
	for i := range req.Externals {
		message, err := tlb.ToCell(&tlb.ExternalMessage{
			DstAddr: senders[i],
			Body:    cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell(),
		})
		if err != nil {
			tb.Fatal(err)
		}
		req.Externals[i] = externalInput(tb, message)
	}
	req.MaxExternalAttempts = max(profile.externals, 1)
	return req
}

// benchMasterAddress derives a masterchain address; the workchain matters
// because the collator routes by it.
func benchMasterAddress(role string, index int) *address.Address {
	return address.NewAddress(0, 0xff, benchAddress(role, index).Data())
}

func benchMasterSnapshot(
	tb testing.TB,
	block ton.BlockIDExt,
	stateRoot *cell.Cell,
	genUtime uint32,
) *groups.Snapshot {
	tb.Helper()

	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	tracked, err := tracker.Apply(groups.ApplyInput{
		Block: block,
		Root:  stateRoot,
		AsOf:  time.Unix(int64(genUtime), 0),
	})
	if err != nil {
		tb.Fatalf("derive masterchain group snapshot: %v", err)
	}
	return tracked.Snapshot
}

func BenchmarkCollateMaster(b *testing.B) {
	for _, profile := range benchMasterProfiles {
		b.Run(profile.name, func(b *testing.B) {
			workload := benchMasterWorkloadFor(b, profile)
			builder := testBuilder()
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := builder.BuildMaster(ctx, workload.request); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportMasterShape(b, workload)
		})
	}
}

func BenchmarkVerifyMaster(b *testing.B) {
	for _, profile := range benchMasterProfiles {
		b.Run(profile.name, func(b *testing.B) {
			workload := benchMasterWorkloadFor(b, profile)
			verification := MasterVerificationRequest{
				Previous:           workload.request.Previous,
				Config:             workload.request.Config,
				Groups:             workload.request.Groups,
				ShardTops:          workload.request.ShardTops,
				Neighbors:          workload.request.Neighbors,
				NeighborShardEndLT: workload.request.NeighborShardEndLT,
				Semantics:          NewSemanticVerifier(tvm.NewTVM()),
				Candidate:          workload.candidate,
			}
			if err := VerifyMasterCandidate(context.Background(), verification); err != nil {
				b.Fatalf("verify %s masterchain candidate: %v", profile.name, err)
			}
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := VerifyMasterCandidate(ctx, verification); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			reportMasterShape(b, workload)
		})
	}
}

func reportMasterShape(b *testing.B, workload *benchMasterWorkload) {
	stats := workload.candidate.Stats
	b.ReportMetric(float64(stats.Transactions), "tx/block")
	b.ReportMetric(float64(stats.GasUsed), "gas/block")
	b.ReportMetric(float64(len(workload.candidate.BlockBOC)), "blockB")
}
