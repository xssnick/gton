package collator

import (
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
)

// The C++ collator is driven by the same inputs rather than by a workload
// reimplemented against it: a reimplementation would have to match this one
// account for account and logical time for logical time, and any drift would
// show up as a performance difference that is really a workload difference.
// TestExportBenchFixture writes the workload to disk — the two states, the
// predecessor block, the externals — so both implementations read identical
// bytes.
//
// It is skipped unless GTON_BENCH_FIXTURE_DIR names an output directory.

type benchFixtureMeta struct {
	Profile        string   `json:"profile"`
	Shard          string   `json:"shard"`
	GlobalID       int32    `json:"global_id"`
	PrevShardBlock blockRef `json:"prev_shard_block"`
	McBlock        blockRef `json:"mc_block"`
	McEndLT        uint64   `json:"mc_end_lt"`
	ShardGenLT     uint64   `json:"shard_gen_lt"`
	ShardGenUtime  uint32   `json:"shard_gen_utime"`
	OutQueueSize   uint64   `json:"out_queue_size"`
	GenUtime       uint32   `json:"gen_utime"`
	GenUtimeMS     uint64   `json:"gen_utime_ms"`
	RandSeed       string   `json:"rand_seed"`
	CreatedBy      string   `json:"created_by"`
	CatchainSeqno  uint32   `json:"catchain_seqno"`
	ValidatorHash  uint32   `json:"validator_list_hash_short"`
	Externals      int      `json:"externals"`
	// Candidate is the block this fixture's own collation produced, exported so
	// the reference validator can be timed on it — and so that it accepting the
	// block is itself a result.
	Candidate candidateRef `json:"candidate"`
	// Expected is what our own collation produced from these inputs, so the
	// C++ side can be checked against it rather than merely timed.
	Expected expectedBlock `json:"expected"`
}

type blockRef struct {
	Workchain int32  `json:"workchain"`
	Shard     string `json:"shard"`
	SeqNo     uint32 `json:"seqno"`
	RootHash  string `json:"root_hash"`
	FileHash  string `json:"file_hash"`
}

type candidateRef struct {
	Block            blockRef `json:"block"`
	CollatedFileHash string   `json:"collated_file_hash"`
	CreatedBy        string   `json:"created_by"`
}

type expectedBlock struct {
	Transactions uint32 `json:"transactions"`
	GasUsed      uint64 `json:"gas_used"`
	BlockBytes   int    `json:"block_bytes"`
}

func TestExportBenchFixture(t *testing.T) {
	dir := os.Getenv("GTON_BENCH_FIXTURE_DIR")
	if dir == "" {
		t.Skip("set GTON_BENCH_FIXTURE_DIR to export the collation fixture")
	}
	name := os.Getenv("GTON_BENCH_FIXTURE_PROFILE")
	if name == "" {
		name = "mid"
	}
	var (
		req     ShardRequest
		mcState *cell.Cell
		mcBlock *cell.Cell
	)
	switch name {
	case benchMainnetProfileName:
		req, mcState, mcBlock = benchMainnetFixtureRequest(t, 1)
	case benchMainnetHeavyProfileName:
		req, mcState, mcBlock = benchMainnetFixtureRequest(t, benchMainnetExportRepeat())
	default:
		var profile benchProfile
		for _, candidate := range benchProfiles {
			if candidate.name == name {
				profile = candidate
			}
		}
		if profile.name == "" {
			t.Fatalf("unknown profile %q", name)
		}
		req, mcState, mcBlock = benchFixtureRequest(t, profile)
	}
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("collate fixture: %v", err)
	}

	out := filepath.Join(dir, name)
	if err = os.MkdirAll(filepath.Join(out, "externals"), 0o755); err != nil {
		t.Fatal(err)
	}
	writeBOC(t, filepath.Join(out, "mc_state.boc"), mcState)
	writeBOC(t, filepath.Join(out, "shard_state.boc"), req.Previous.State)
	writeBOC(t, filepath.Join(out, "shard_block.boc"), req.Previous.Block)
	writeBOC(t, filepath.Join(out, "mc_block.boc"), mcBlock)
	if err = os.WriteFile(filepath.Join(out, "candidate.boc"), candidate.BlockBOC, 0o644); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(out, "candidate_collated.boc"), candidate.CollatedData, 0o644); err != nil {
		t.Fatal(err)
	}
	for i := range req.Externals {
		writeBOC(t, filepath.Join(out, "externals", fmt.Sprintf("%04d.boc", i)),
			req.Externals[i].message.Cell())
	}

	meta := benchFixtureMeta{
		Profile:        name,
		Shard:          fmt.Sprintf("%d:%016x", req.Shard.Workchain, uint64(req.Shard.Shard)),
		GlobalID:       testConfigGlobalID(t, req.Masterchain.Config),
		PrevShardBlock: toBlockRef(req.Previous.ID),
		McBlock:        toBlockRef(req.Masterchain.ID),
		McEndLT:        req.Masterchain.EndLT,
		ShardGenLT:     loadPreviousShardState(t, req).GenLT,
		ShardGenUtime:  loadPreviousShardState(t, req).GenUTime,
		OutQueueSize:   *req.Previous.OutQueueSize,
		GenUtime:       req.Header.GenUtime,
		GenUtimeMS:     req.Header.GenUtimeMS,
		RandSeed:       hex.EncodeToString(req.RandSeed[:]),
		CreatedBy:      hex.EncodeToString(req.CreatedBy[:]),
		CatchainSeqno:  req.Masterchain.Groups.Active[0].CatchainSeqno,
		ValidatorHash:  req.Masterchain.Groups.Active[0].ValidatorSetHash,
		Externals:      len(req.Externals),
		Candidate: candidateRef{
			Block:            toBlockRef(candidate.ID),
			CollatedFileHash: hex.EncodeToString(candidate.CollatedFileHash[:]),
			CreatedBy:        hex.EncodeToString(candidate.CreatedBy[:]),
		},
		Expected: expectedBlock{
			Transactions: candidate.Stats.Transactions,
			GasUsed:      candidate.Stats.GasUsed,
			BlockBytes:   len(candidate.BlockBOC),
		},
	}
	encoded, err := json.MarshalIndent(meta, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(filepath.Join(out, "meta.json"), append(encoded, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
	t.Logf("fixture %s written to %s: %d transactions, %d gas, %d byte block",
		name, out, candidate.Stats.Transactions, candidate.Stats.GasUsed, len(candidate.BlockBOC))
}

// BenchmarkCollateFixture collates exactly what the exported fixture holds, so
// its number is comparable with the reference node's run over the same files.
// The regular profiles are not: the fixture carries a minted predecessor and no
// deferred messages.
func BenchmarkCollateFixture(b *testing.B) {
	for _, profile := range benchProfiles {
		if profile.externals == 0 {
			continue
		}
		b.Run(profile.name, func(b *testing.B) {
			req, _, _ := benchFixtureRequest(b, profile)
			builder := testBuilder()
			ctx := context.Background()
			candidate, err := builder.BuildShard(ctx, req)
			if err != nil {
				b.Fatal(err)
			}

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if _, err := builder.BuildShard(ctx, req); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(candidate.Stats.Transactions), "tx/block")
			b.ReportMetric(float64(len(candidate.BlockBOC)), "blockB")
		})
	}
}

// BenchmarkVerifyFixture is the counterpart to BenchmarkCollateFixture. The
// plain BenchmarkVerifyShard measures a different block: the fixture request
// carries a collated predecessor, which shifts how much of the workload fits,
// so only this one is comparable with the reference node's validation of the
// exported fixture.
func BenchmarkVerifyFixture(b *testing.B) {
	for _, profile := range benchProfiles {
		if profile.externals == 0 {
			continue
		}
		b.Run(profile.name, func(b *testing.B) {
			req, _, _ := benchFixtureRequest(b, profile)
			candidate, err := testBuilder().BuildShard(context.Background(), req)
			if err != nil {
				b.Fatal(err)
			}
			verification := shardVerificationRequest(req, candidate)
			verification.NeighborShardEndLT = req.NeighborShardEndLT
			verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
			if err = VerifyShardCandidate(context.Background(), verification); err != nil {
				b.Fatalf("verify %s fixture candidate: %v", profile.name, err)
			}
			ctx := context.Background()

			b.ReportAllocs()
			b.ResetTimer()
			for b.Loop() {
				if err := VerifyShardCandidate(ctx, verification); err != nil {
					b.Fatal(err)
				}
			}
			b.StopTimer()
			b.ReportMetric(float64(candidate.Stats.Transactions), "tx/block")
			b.ReportMetric(float64(candidate.Stats.GasUsed), "gas/block")
			b.ReportMetric(float64(len(candidate.BlockBOC)), "blockB")
		})
	}
}

// benchFixtureRequest is the shard workload with a real masterchain state behind
// it. The benchmark request carries a hand-built group snapshot because nothing
// in collation needs the masterchain state itself; the C++ collator does, and it
// checks the shard against the shard configuration inside it, so the state has
// to register this very predecessor block.
func benchFixtureRequest(tb testing.TB, profile benchProfile) (ShardRequest, *cell.Cell, *cell.Cell) {
	tb.Helper()

	// The reference collator asks its manager for the predecessor block, so the
	// fixture needs a real one even when the capability that normally requires
	// it is off. Minting it costs the deferred messages: the collation that
	// mints drains the dispatch queue into the outbound one, which would leave
	// the benchmark block with no dispatch phase and a queue that no longer
	// matches the profile. The fixture therefore carries none — that phase is
	// about a percent of the block, and a workload that silently changed shape
	// would be worse than one that openly drops a small part of it.
	profile.mintPredecessor = true
	profile.deferred = 0
	// The reference collator loads the masterchain block too, purely to build a
	// correct ExtBlkRef to it — it needs the block's logical time range. Nothing
	// else reads it, so a header-only block is enough, but its hash has to be the
	// one the request names, and it has to be settled before the workload is
	// built: the predecessor minted below references the masterchain block, and
	// changing its identity afterwards puts the two references in conflict.
	base := emptyCandidateRequest(tb)
	mcBlock := benchMasterchainBlock(tb, base)
	mcHash := mcBlock.HashKey()
	base.Masterchain.ID.RootHash = mcHash[:]
	base.Masterchain.Groups.MasterchainBlock = base.Masterchain.ID

	req := benchRequestFrom(tb, profile, base)
	shardState := loadPreviousShardState(tb, req)
	mcState := benchMasterchainState(tb, req, &shardState)

	// Derive the group snapshot from that state instead of the hand-built one:
	// both sides then take their validator sessions from the same source.
	snapshot := benchMasterSnapshot(tb, req.Masterchain.ID, mcState, req.Masterchain.GenUtime)
	if !snapshot.Ready {
		tb.Fatal("masterchain fixture state did not yield a ready group snapshot")
	}
	req.Masterchain.Groups = snapshot
	return req, mcState, mcBlock
}

// benchMasterchainBlock is a header-only masterchain block: the reference
// collator wants its start and end logical times and nothing else.
func benchMasterchainBlock(tb testing.TB, req ShardRequest) *cell.Cell {
	tb.Helper()

	var header tlb.BlockHeader
	header.SeqNo = req.Masterchain.ID.SeqNo
	header.Shard = tlb.ShardIdent{WorkchainID: address.MasterchainID}
	header.GenUtime = req.Masterchain.GenUtime
	header.StartLt = req.Masterchain.EndLT - 999
	header.EndLt = req.Masterchain.EndLT
	header.MinRefMcSeqno = req.Masterchain.ID.SeqNo
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		EndLt:    req.Masterchain.EndLT - 1_000,
		SeqNo:    req.Masterchain.ID.SeqNo - 1,
		RootHash: bytes.Repeat([]byte{0x73}, 32),
		FileHash: bytes.Repeat([]byte{0x74}, 32),
	}}
	info, err := header.ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	zero := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(0)}
	flow, err := tlb.ValueFlow{
		FromPrevBlock: zero, ToNextBlock: zero, Imported: zero, Exported: zero,
		FeesCollected: zero, FeesImported: zero, Recovered: zero, Created: zero,
		Minted: zero, Burned: zero,
	}.ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	empty := cell.BeginCell().EndCell()
	return cell.BeginCell().
		MustStoreUInt(blockTag, 32).
		MustStoreInt(int64(testConfigGlobalID(tb, req.Masterchain.Config)), 32).
		MustStoreRef(info).
		MustStoreRef(flow).
		MustStoreRef(empty).
		MustStoreRef(empty).
		EndCell()
}

// benchMasterchainState builds the masterchain state the shard collation
// references: real configuration parameters, a validator set, and a shard
// configuration whose only descriptor is the shard's predecessor.
func benchMasterchainState(tb testing.TB, req ShardRequest, shardState *tlb.ShardStateUnsplit) *cell.Cell {
	tb.Helper()

	configRoot := req.Masterchain.Config.execution.Root()
	parsed, err := groups.ParseConfig(configRoot)
	if err != nil {
		tb.Fatalf("parse validator config: %v", err)
	}
	catchainSeqno := req.Masterchain.Groups.Active[0].CatchainSeqno
	validatorHash, err := groups.MasterchainStateValidatorHash(parsed, catchainSeqno)
	if err != nil {
		tb.Fatal(err)
	}
	rawConfig := tlb.BlockchainConfig{Root: configRoot}
	configAddress, err := rawConfig.GetConfigAddress()
	if err != nil || len(configAddress) != 32 {
		tb.Fatalf("load configuration address: %v", err)
	}

	descriptor := benchShardDescriptor(tb, req.Previous.ID, shardState, req.Masterchain.ID.SeqNo)
	history, err := cell.NewAugDict(32, oldMCBlocksAugmentation{})
	if err != nil {
		tb.Fatal(err)
	}
	// A masterchain state past seqno 0 must carry the zero state at key 0 of its
	// previous-block collection; the reference node refuses to load one without
	// it. Nothing in our own collation reads it, so only the fixture builds it.
	zeroRef, err := tlb.ToCell(&tlb.KeyExtBlkRef{
		IsKey:  true,
		BlkRef: tlb.ExtBlkRef{RootHash: make([]byte, 32), FileHash: make([]byte, 32)},
	})
	if err != nil {
		tb.Fatal(err)
	}
	if err = history.Set(cell.BeginCell().MustStoreUInt(0, 32).EndCell(), zeroRef); err != nil {
		tb.Fatal(err)
	}
	// The transaction context exposes the last sixteen masterchain blocks to
	// contracts, and the reference node builds that tuple by walking this
	// collection back seqno by seqno — a gap anywhere in the window fails the
	// whole collation.
	// Two windows are required: the sixteen immediate predecessors, and — from
	// global version 9 — sixteen more on a hundred-block stride, which is a
	// separate tuple the contract context also exposes.
	mcSeqno := req.Masterchain.ID.SeqNo
	wanted := map[uint32]struct{}{}
	for back := uint32(1); back <= 16 && back <= mcSeqno; back++ {
		wanted[mcSeqno-back] = struct{}{}
	}
	for seqno, taken := mcSeqno/100*100, 0; taken < 16; taken++ {
		wanted[seqno] = struct{}{}
		if seqno < 100 {
			break
		}
		seqno -= 100
	}
	for seqno := range wanted {
		if seqno == 0 {
			continue
		}
		entry, err := tlb.ToCell(&tlb.KeyExtBlkRef{
			BlkRef: tlb.ExtBlkRef{
				EndLt:    uint64(seqno) * 1_000,
				SeqNo:    seqno,
				RootHash: bytes.Repeat([]byte{byte(seqno)}, 32),
				FileHash: bytes.Repeat([]byte{byte(seqno + 1)}, 32),
			},
		})
		if err != nil {
			tb.Fatal(err)
		}
		if err = history.Set(cell.BeginCell().MustStoreUInt(uint64(seqno), 32).EndCell(), entry); err != nil {
			tb.Fatal(err)
		}
	}
	info := tlb.McStateExtraBlockInfo{
		ValidatorInfo: tlb.ValidatorInfo{
			ValidatorListHashShort: validatorHash,
			CatchainSeqno:          catchainSeqno,
			NextCCUpdated:          true,
		},
		PrevBlocks:    &tlb.OldMcBlocksInfoAugDict{AugmentedDictionary: history},
		AfterKeyBlock: true,
	}
	infoRoot, err := info.ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	params := tlb.ConfigParams{ConfigAddr: bytes.Clone(configAddress)}
	params.Config.Params = configRoot.AsDict(32)
	extraRoot, err := tlb.ToCell(&tlb.McStateExtra{
		ShardHashes:  masterBuildShardHashes(tb, descriptor),
		ConfigParams: params,
		Info:         infoRoot,
	})
	if err != nil {
		tb.Fatal(err)
	}

	accounts := masterBuildAccounts(tb, configRoot, configAddress, req.Masterchain.GenUtime, false)
	balance, err := loadAccountsBalance(accounts)
	if err != nil {
		tb.Fatal(err)
	}
	statsRoot, err := (tlb.ShardStateStats{TotalBalance: balance, Libraries: cell.NewDict(256)}).ToCell()
	if err != nil {
		tb.Fatal(err)
	}
	queueRoot, err := benchEmptyQueueInfo(tb)
	if err != nil {
		tb.Fatal(err)
	}
	state := tlb.ShardStateUnsplit{
		GlobalID:        shardState.GlobalID,
		ShardIdent:      tlb.ShardIdent{WorkchainID: address.MasterchainID},
		Seqno:           req.Masterchain.ID.SeqNo,
		VertSeqno:       shardState.VertSeqno,
		GenUTime:        req.Masterchain.GenUtime,
		GenLT:           req.Masterchain.EndLT,
		OutMsgQueueInfo: queueRoot,
		Stats:           statsRoot,
		McStateExtra:    extraRoot,
	}
	state.Accounts.ShardAccounts = accounts
	root, err := tlb.ToCell(&state)
	if err != nil {
		tb.Fatal(err)
	}
	return root
}

// benchShardDescriptor describes the shard predecessor the way the masterchain
// registered it. The logical times come from the state itself: the reference
// collator compares the shard's end lt against the state it proves.
func benchShardDescriptor(
	tb testing.TB,
	block ton.BlockIDExt,
	shardState *tlb.ShardStateUnsplit,
	regMCSeqno uint32,
) *cell.Cell {
	tb.Helper()

	root, err := tlb.ToCell(&tlb.ShardDesc{
		SeqNo:              block.SeqNo,
		RegMcSeqno:         regMCSeqno,
		StartLT:            shardState.GenLT - 1_000,
		EndLT:              shardState.GenLT,
		RootHash:           bytes.Clone(block.RootHash),
		FileHash:           bytes.Clone(block.FileHash),
		NextCatchainSeqNo:  regMCSeqno,
		NextValidatorShard: math.MinInt64,
		MinRefMcSeqNo:      shardState.MinRefMCSeqno,
		GenUTime:           shardState.GenUTime,
		SplitMergeAt:       tlb.FutureSplitMergeNone{},
	})
	if err != nil {
		tb.Fatal(err)
	}
	return root
}

func benchEmptyQueueInfo(tb testing.TB) (*cell.Cell, error) {
	tb.Helper()

	outQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		return nil, err
	}
	return (tlb.OutMsgQueueInfo{OutQueue: outQueue, ProcInfo: cell.NewDict(processedInfoKeyBits)}).ToCell()
}

func toBlockRef(id ton.BlockIDExt) blockRef {
	return blockRef{
		Workchain: id.Workchain,
		Shard:     fmt.Sprintf("%016x", uint64(id.Shard)),
		SeqNo:     id.SeqNo,
		RootHash:  hex.EncodeToString(id.RootHash),
		FileHash:  hex.EncodeToString(id.FileHash),
	}
}

func writeBOC(tb testing.TB, path string, root *cell.Cell) {
	tb.Helper()

	if err := os.WriteFile(path, root.WithoutTrace().ToBOC(), 0o644); err != nil {
		tb.Fatalf("write %s: %v", path, err)
	}
}
