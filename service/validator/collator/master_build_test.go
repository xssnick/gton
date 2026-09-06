package collator

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"math"
	"math/big"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

type masterBuildFixture struct {
	request       MasterRequest
	configRoot    *cell.Cell
	configAddress []byte
	oldState      tlb.ShardStateUnsplit
	oldStats      tlb.ShardStateStats
	oldExtra      tlb.McStateExtra
	oldInfo       tlb.McStateExtraBlockInfo
	oldShard      ton.BlockIDExt
	newShard      ton.BlockIDExt
	topProof      *cell.Cell
}

func TestBuildMasterEndToEnd(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	builder := testBuilder()

	first, err := builder.BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	second, err := builder.BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}

	if !bytes.Equal(first.BlockBOC, second.BlockBOC) ||
		!bytes.Equal(first.CollatedData, second.CollatedData) ||
		first.State.HashKey() != second.State.HashKey() ||
		first.StateUpdate.HashKey() != second.StateUpdate.HashKey() {
		t.Fatal("same masterchain request produced different candidate data")
	}
	if first.ID.Workchain != address.MasterchainID || first.ID.Shard != math.MinInt64 ||
		first.ID.SeqNo != fixture.request.Previous.ID.SeqNo+1 {
		t.Fatalf("candidate id = %+v", first.ID)
	}
	blockRoot := candidateBlock(t, first)
	fileHash := sha256.Sum256(first.BlockBOC)
	if !bytes.Equal(first.ID.RootHash, blockRoot.Hash()) || !bytes.Equal(first.ID.FileHash, fileHash[:]) {
		t.Fatal("candidate id hashes do not bind the block BOC")
	}
	if first.CreatedBy != fixture.request.CreatedBy || len(first.Externals) != 0 {
		t.Fatal("candidate metadata changed")
	}
	if err = VerifyMasterCandidate(context.Background(), MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    fixture.request.Config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Semantics: testCandidateTransitionVerifier,
		Candidate: first,
	}); err != nil {
		t.Fatalf("verify built masterchain candidate: %v", err)
	}

	if err = cell.ValidateMerkleUpdate(first.StateUpdate); err != nil {
		t.Fatalf("validate masterchain state update: %v", err)
	}
	applied, err := cell.ApplyMerkleUpdate(fixture.request.Previous.State, first.StateUpdate)
	if err != nil {
		t.Fatalf("apply masterchain state update: %v", err)
	}
	if applied.HashKey() != first.State.HashKey() {
		t.Fatal("masterchain state update does not produce candidate state")
	}

	var block tlb.Block
	if err = parseExact(&block, blockRoot); err != nil {
		t.Fatalf("decode masterchain block: %v", err)
	}
	masterSession := masterBuildActiveSession(t, fixture.request.Groups)
	header := &block.BlockInfo
	if header.NotMaster || header.AfterMerge || header.AfterSplit || header.BeforeSplit ||
		header.SeqNo != first.ID.SeqNo || header.GenUtime != fixture.request.Header.GenUtime ||
		header.GenCatchainSeqno != masterSession.CatchainSeqno ||
		header.GenValidatorListHashShort != masterSession.ValidatorSetHash ||
		header.PrevKeyBlockSeqno != fixture.request.Groups.LastKeyBlockSeqno {
		t.Fatalf("unexpected masterchain header: %+v", *header)
	}
	if block.Extra == nil || block.Extra.Custom == nil {
		t.Fatal("masterchain block extra is absent")
	}
	if !bytes.Equal(block.Extra.RandSeed, fixture.request.RandSeed[:]) ||
		!bytes.Equal(block.Extra.CreatedBy, fixture.request.CreatedBy[:]) {
		t.Fatal("masterchain block seed or creator changed")
	}

	var flow tlb.ValueFlow
	if err = parseExact(&flow, block.ValueFlow); err != nil {
		t.Fatalf("decode masterchain value flow: %v", err)
	}
	if err = flow.Validate(); err != nil {
		t.Fatalf("validate masterchain value flow: %v", err)
	}
	wantCreated := tlb.CurrencyCollection{Coins: fixture.request.Config.masterchain.createFee}
	if !flow.FromPrevBlock.Equals(fixture.oldStats.TotalBalance) || !flow.Created.Equals(wantCreated) ||
		!currencyZero(flow.FeesImported) {
		t.Fatalf("unexpected masterchain value flow: %+v", flow)
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, first.State); err != nil {
		t.Fatalf("decode next masterchain state: %v", err)
	}
	if state.ShardIdent != header.Shard || state.Seqno != header.SeqNo ||
		state.GenUTime != header.GenUtime || state.GenLT != header.EndLt ||
		state.MinRefMCSeqno != header.MinRefMcSeqno || state.McStateExtra == nil {
		t.Fatalf("next masterchain state disagrees with block header: %+v", state)
	}
	var stats tlb.ShardStateStats
	if err = parseExact(&stats, state.Stats); err != nil {
		t.Fatalf("decode next masterchain statistics: %v", err)
	}
	if !stats.TotalBalance.Equals(flow.ToNextBlock) {
		t.Fatal("next account balance differs from value flow")
	}
	wantValidatorFees, err := fixture.oldStats.TotalValidatorFees.Add(flow.FeesCollected)
	if err != nil {
		t.Fatal(err)
	}
	wantValidatorFees, err = wantValidatorFees.Sub(flow.Recovered)
	if err != nil {
		t.Fatal(err)
	}
	if !stats.TotalValidatorFees.Equals(wantValidatorFees) {
		t.Fatal("next validator fees disagree with collected and recovered value")
	}

	var stateExtra tlb.McStateExtra
	if err = parseExact(&stateExtra, state.McStateExtra); err != nil {
		t.Fatalf("decode next masterchain state extra: %v", err)
	}
	assertMasterBuildConfig(t, fixture, &stateExtra, block.Extra.Custom)
	assertMasterBuildHistory(t, fixture, &stateExtra, header)
	assertMasterBuildShardRegistry(t, fixture, &stateExtra, block.Extra.Custom)
	assertMasterBuildSpecials(t, fixture, header.StartLt, first, &flow, block.Extra.Custom)

	wantGlobal, err := fixture.oldExtra.GlobalBalance.Add(flow.Created)
	if err != nil {
		t.Fatal(err)
	}
	wantGlobal, err = wantGlobal.Add(flow.Minted)
	if err != nil {
		t.Fatal(err)
	}
	if !stateExtra.GlobalBalance.Equals(wantGlobal) {
		t.Fatalf("global balance = %s, want %s", stateExtra.GlobalBalance.Coins, wantGlobal.Coins)
	}

	assertMasterBuildConfigAccount(t, fixture, &state)
	assertMasterBuildCollatedOrder(t, fixture, first)
}

func TestBuildMasterSetsLoadHistoryWishes(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	fixture.oldStats.UnderloadHistory = 0x0000ffffffffffff
	statsRoot, err := fixture.oldStats.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	fixture.oldState.Stats = statsRoot
	stateRoot, err := tlb.ToCell(&fixture.oldState)
	if err != nil {
		t.Fatal(err)
	}
	fixture.request.Previous.State = stateRoot
	stateHash := stateRoot.HashKey()
	fixture.request.Previous.ID.RootHash = bytes.Clone(stateHash[:])
	fixture.request.Groups.MasterchainBlock = *fixture.request.Previous.ID.Copy()

	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	if block.BlockInfo.WantSplit || !block.BlockInfo.WantMerge {
		t.Fatalf("masterchain load wishes: split=%t merge=%t", block.BlockInfo.WantSplit, block.BlockInfo.WantMerge)
	}
	if err = VerifyMasterCandidate(context.Background(), MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    fixture.request.Config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Semantics: testCandidateTransitionVerifier,
		Candidate: candidate,
	}); err != nil {
		t.Fatalf("verify masterchain load wishes: %v", err)
	}
}

func TestBuildMasterActivatesMissingWorkchain(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	fixture.request.ShardTops = nil

	oldExtra := fixture.oldExtra
	oldExtra.ShardHashes = cell.NewDict(masterShardHashesKeyBits)
	extraRoot, err := tlb.ToCell(&oldExtra)
	if err != nil {
		t.Fatal(err)
	}
	oldState := fixture.oldState
	oldState.McStateExtra = extraRoot
	stateRoot, err := tlb.ToCell(&oldState)
	if err != nil {
		t.Fatal(err)
	}
	stateHash := stateRoot.HashKey()
	fixture.request.Previous.State = stateRoot
	fixture.request.Previous.ID.RootHash = bytes.Clone(stateHash[:])
	snapshot := *fixture.request.Groups
	snapshot.MasterchainBlock = fixture.request.Previous.ID
	fixture.request.Groups = &snapshot

	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	var stateExtra tlb.McStateExtra
	if err = parseExact(&stateExtra, state.McStateExtra); err != nil {
		t.Fatal(err)
	}
	registry, err := ParseShardRegistry(stateExtra.ShardHashes)
	if err != nil {
		t.Fatal(err)
	}
	tops := registry.Tops()
	if len(tops) != 1 || tops[0].Block.Workchain != 0 || tops[0].Block.Shard != math.MinInt64 ||
		tops[0].Block.SeqNo != 0 {
		t.Fatalf("activated shard registry = %+v", tops)
	}
	workchains, err := loadMasterShardWorkchains(fixture.configRoot)
	if err != nil {
		t.Fatal(err)
	}
	wc := workchains[0]
	if wc == nil || !bytes.Equal(tops[0].Block.RootHash, wc.zeroStateRootHash[:]) ||
		!bytes.Equal(tops[0].Block.FileHash, wc.zeroStateFileHash[:]) {
		t.Fatalf("activated workchain zero state = %+v", tops[0].Block)
	}

	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	if block.Extra.Custom.ShardFees.AugmentedDictionary.IsEmpty() {
		t.Fatal("activated workchain has no zero ShardFees entry")
	}
}

// TestBuildMasterRoundTripAcrossCatchainBoundaries drives the McStateExtra
// block-info branches the default fixture avoids on purpose: masterBuildSafeTime
// crosses no catchain lifetime boundary and the config account holds exactly the
// predecessor's configuration, so the catchain bump, NextCCUpdated, the shard
// catchain rotation and the key-block ladder are all dark.
//
// Collation (buildMasterStateInfo) and validation (verifyMasterStateInfoTransition)
// derive these values from two separate copies of the same rule, so only a
// build-then-verify round trip can show that the copies still agree.
func TestBuildMasterRoundTripAcrossCatchainBoundaries(t *testing.T) {
	base := loadMainnetConfig(t).execution.Root()

	t.Run("masterchain and shard catchain rotate", func(t *testing.T) {
		// Mainnet sets both lifetimes to 250, so one boundary crosses both.
		fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{
			genUtime: masterBuildBoundaryTime(t, 250, 0),
		})
		info, extra := masterBuildRoundTrip(t, fixture)
		assertMasterRoundTripInfo(t, fixture, info, extra, masterRoundTripWants{
			catchainSeqno: 18,
			nextCCUpdated: true,
			shardCatchain: 1,
		})
	})

	t.Run("masterchain catchain rotates without shard catchain", func(t *testing.T) {
		fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{
			configRoot: masterBuildCatchainLifetimes(t, base, 250, 500),
			genUtime:   masterBuildBoundaryTime(t, 250, 500),
		})
		info, extra := masterBuildRoundTrip(t, fixture)
		assertMasterRoundTripInfo(t, fixture, info, extra, masterRoundTripWants{
			catchainSeqno: 18,
			nextCCUpdated: false,
			shardCatchain: 0,
		})
	})

	t.Run("shard catchain rotates without masterchain catchain", func(t *testing.T) {
		fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{
			configRoot: masterBuildCatchainLifetimes(t, base, 500, 250),
			genUtime:   masterBuildBoundaryTime(t, 250, 500),
		})
		info, extra := masterBuildRoundTrip(t, fixture)
		assertMasterRoundTripInfo(t, fixture, info, extra, masterRoundTripWants{
			catchainSeqno: 17,
			nextCCUpdated: false,
			shardCatchain: 1,
		})
	})

	t.Run("key block", func(t *testing.T) {
		fixture := newMasterBuildFixtureWith(t, masterBuildFixtureOptions{
			accountConfigRoot: masterBuildKeyBlockConfig(t, base),
		})
		info, extra := masterBuildRoundTrip(t, fixture)
		// A key block rotates both catchains regardless of the generation time.
		assertMasterRoundTripInfo(t, fixture, info, extra, masterRoundTripWants{
			catchainSeqno: 18,
			nextCCUpdated: true,
			shardCatchain: 1,
			keyBlock:      true,
		})
	})
}

type masterRoundTripWants struct {
	catchainSeqno uint32
	nextCCUpdated bool
	shardCatchain uint32
	keyBlock      bool
}

// masterBuildRoundTrip collates a masterchain candidate, verifies it with the
// independent validation path and returns the resulting state block info.
func masterBuildRoundTrip(
	t *testing.T,
	fixture masterBuildFixture,
) (tlb.McStateExtraBlockInfo, *tlb.McStateExtra) {
	t.Helper()

	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatalf("build masterchain candidate: %v", err)
	}
	if err = VerifyMasterCandidate(context.Background(), MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    fixture.request.Config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Semantics: testCandidateTransitionVerifier,
		Candidate: candidate,
	}); err != nil {
		t.Fatalf("verify masterchain candidate: %v", err)
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	var stateExtra tlb.McStateExtra
	if err = parseExact(&stateExtra, state.McStateExtra); err != nil {
		t.Fatal(err)
	}
	info, err := parseMasterStateInfo(stateExtra.Info)
	if err != nil {
		t.Fatal(err)
	}

	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatal(err)
	}
	if block.Extra == nil || block.Extra.Custom == nil {
		t.Fatal("masterchain block extra is absent")
	}
	if block.Extra.Custom.KeyBlock != (block.Extra.Custom.ConfigParams != nil) {
		t.Fatal("key block flag disagrees with the embedded configuration")
	}
	if block.Extra.Custom.KeyBlock != info.AfterKeyBlock {
		t.Fatalf("block key flag = %t, state after-key-block = %t",
			block.Extra.Custom.KeyBlock, info.AfterKeyBlock)
	}

	return info, &stateExtra
}

func assertMasterRoundTripInfo(
	t *testing.T,
	fixture masterBuildFixture,
	info tlb.McStateExtraBlockInfo,
	stateExtra *tlb.McStateExtra,
	want masterRoundTripWants,
) {
	t.Helper()

	if info.AfterKeyBlock != want.keyBlock {
		t.Fatalf("after key block = %t, want %t", info.AfterKeyBlock, want.keyBlock)
	}
	// The fixture predecessor is itself a key block, so the resulting last key
	// block reference always names it.
	if info.LastKeyBlock == nil || info.LastKeyBlock.SeqNo != fixture.request.Previous.ID.SeqNo ||
		!bytes.Equal(info.LastKeyBlock.RootHash, fixture.request.Previous.ID.RootHash) {
		t.Fatalf("last key block = %+v", info.LastKeyBlock)
	}
	if info.ValidatorInfo.CatchainSeqno != want.catchainSeqno ||
		info.ValidatorInfo.NextCCUpdated != want.nextCCUpdated {
		t.Fatalf("validator info = %+v, want seqno %d next-cc-updated %t",
			info.ValidatorInfo, want.catchainSeqno, want.nextCCUpdated)
	}
	nextGroups, err := groups.ParseConfig(stateExtra.ConfigParams.Config.Params.AsCell())
	if err != nil {
		t.Fatal(err)
	}
	wantHash, err := groups.MasterchainStateValidatorHash(nextGroups, want.catchainSeqno)
	if err != nil {
		t.Fatal(err)
	}
	if info.ValidatorInfo.ValidatorListHashShort != wantHash {
		t.Fatalf("state validator hash = %x, want %x", info.ValidatorInfo.ValidatorListHashShort, wantHash)
	}

	registry, err := ParseShardRegistry(stateExtra.ShardHashes)
	if err != nil {
		t.Fatal(err)
	}
	tops := registry.Tops()
	if len(tops) != 1 {
		t.Fatalf("next shard registry = %+v", tops)
	}
	fields, err := parseShardDescriptorFields(tops[0].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if fields.nextCatchainSeqno != want.shardCatchain {
		t.Fatalf("shard next catchain seqno = %d, want %d", fields.nextCatchainSeqno, want.shardCatchain)
	}
}

func TestBuildMasterRejectsInvalidBindings(t *testing.T) {
	t.Run("stale group snapshot", func(t *testing.T) {
		fixture := newMasterBuildFixture(t, false)
		stale := *fixture.request.Groups
		stale.MasterchainBlock.RootHash = bytes.Clone(stale.MasterchainBlock.RootHash)
		stale.MasterchainBlock.RootHash[0] ^= 1
		fixture.request.Groups = &stale

		_, err := testBuilder().BuildMaster(context.Background(), fixture.request)
		if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "not derived from the predecessor") {
			t.Fatalf("stale snapshot error = %v", err)
		}
	})

	t.Run("group snapshot time", func(t *testing.T) {
		fixture := newMasterBuildFixture(t, false)
		stale := *fixture.request.Groups
		stale.GenUTime++
		fixture.request.Groups = &stale

		_, err := testBuilder().BuildMaster(context.Background(), fixture.request)
		if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "time differs from predecessor") {
			t.Fatalf("snapshot time error = %v", err)
		}
	})

	t.Run("malformed configuration account", func(t *testing.T) {
		fixture := newMasterBuildFixture(t, true)

		_, err := testBuilder().BuildMaster(context.Background(), fixture.request)
		if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "configuration is absent") {
			t.Fatalf("malformed configuration account error = %v", err)
		}
	})

	t.Run("missing shard top descriptor", func(t *testing.T) {
		fixture := newMasterBuildFixture(t, false)
		fixture.request.ShardTops[0].TopBlockDescr = nil

		_, err := testBuilder().BuildMaster(context.Background(), fixture.request)
		if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), "TopBlockDescr is absent") {
			t.Fatalf("missing shard TopBlockDescr error = %v", err)
		}
	})
}

// masterBuildFixtureOptions varies the parts of the fixture whose branches the
// default one deliberately avoids: masterBuildSafeTime crosses no catchain
// lifetime boundary and the config account holds exactly the predecessor's
// configuration, so the candidate is never a key block.
type masterBuildFixtureOptions struct {
	// malformedConfigAccount strips the configuration reference from the
	// config contract's data.
	malformedConfigAccount bool
	// configRoot replaces the mainnet configuration everywhere at once — in the
	// request Config, in the predecessor state extra and in the config account
	// — so the candidate still is not a key block.
	configRoot *cell.Cell
	// accountConfigRoot replaces the configuration held by the config contract
	// alone. That divergence from the predecessor state extra is exactly what
	// deriveMasterConfigTransition classifies as a key block.
	accountConfigRoot *cell.Cell
	// genUtime pins the predecessor generation time; the candidate is built at
	// genUtime+1. Zero picks masterBuildSafeTime.
	genUtime uint32
	// creatorStats seeds the predecessor's block_create_stats#17 cell. Nil
	// leaves the capability flag clear, which is the empty creator dictionary
	// every other fixture builds over.
	creatorStats *cell.Cell
}

func newMasterBuildFixture(t testing.TB, malformedConfigAccount bool) masterBuildFixture {
	t.Helper()

	return newMasterBuildFixtureWith(t, masterBuildFixtureOptions{
		malformedConfigAccount: malformedConfigAccount,
	})
}

func newMasterBuildFixtureWith(t testing.TB, options masterBuildFixtureOptions) masterBuildFixture {
	t.Helper()

	malformedConfigAccount := options.malformedConfigAccount
	config := loadMainnetConfig(t)
	if options.configRoot != nil {
		config = masterBuildPrepareConfig(t, options.configRoot)
	}
	configRoot := config.execution.Root()
	accountConfigRoot := configRoot
	if options.accountConfigRoot != nil {
		accountConfigRoot = options.accountConfigRoot
	}
	parsedGroups, err := groups.ParseConfig(configRoot)
	if err != nil {
		t.Fatalf("parse validator config: %v", err)
	}
	genUtime := masterBuildSafeTime(parsedGroups)
	if options.genUtime != 0 {
		genUtime = options.genUtime
	}
	catchainSeqno := uint32(17)
	validatorHash, err := groups.MasterchainStateValidatorHash(parsedGroups, catchainSeqno)
	if err != nil {
		t.Fatal(err)
	}
	rawConfig := tlb.BlockchainConfig{Root: configRoot}
	configAddress, err := rawConfig.GetConfigAddress()
	if err != nil || len(configAddress) != 32 {
		t.Fatalf("load configuration address: %v", err)
	}

	accounts := masterBuildAccounts(t, accountConfigRoot, configAddress, genUtime, malformedConfigAccount)
	balance, err := loadAccountsBalance(accounts)
	if err != nil {
		t.Fatal(err)
	}
	outQueue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	queueRoot, err := (tlb.OutMsgQueueInfo{
		OutQueue: outQueue,
		ProcInfo: cell.NewDict(processedInfoKeyBits),
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}

	oldShard := testBlockID(0, math.MinInt64, 10, 0x31)
	oldShardDescriptor := masterBuildShardDescriptor(t, oldShard, 0, genUtime-1)
	shardHashes := masterBuildShardHashes(t, oldShardDescriptor)
	history, err := cell.NewAugDict(32, oldMCBlocksAugmentation{})
	if err != nil {
		t.Fatal(err)
	}
	oldInfo := tlb.McStateExtraBlockInfo{
		ValidatorInfo: tlb.ValidatorInfo{
			ValidatorListHashShort: validatorHash,
			CatchainSeqno:          catchainSeqno,
			NextCCUpdated:          true,
		},
		PrevBlocks:       &tlb.OldMcBlocksInfoAugDict{AugmentedDictionary: history},
		AfterKeyBlock:    true,
		BlockCreateStats: options.creatorStats,
	}
	infoRoot, err := oldInfo.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	params := tlb.ConfigParams{ConfigAddr: bytes.Clone(configAddress)}
	params.Config.Params = configRoot.AsDict(32)
	oldExtra := tlb.McStateExtra{
		ShardHashes:  shardHashes,
		ConfigParams: params,
		Info:         infoRoot,
	}
	extraRoot, err := tlb.ToCell(&oldExtra)
	if err != nil {
		t.Fatal(err)
	}
	oldStats := tlb.ShardStateStats{
		TotalBalance: balance,
		Libraries:    cell.NewDict(256),
	}
	statsRoot, err := oldStats.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	oldState := tlb.ShardStateUnsplit{
		GlobalID:        testConfigGlobalID(t, config),
		ShardIdent:      tlb.ShardIdent{WorkchainID: address.MasterchainID},
		Seqno:           0,
		GenUTime:        genUtime,
		GenLT:           1_000_000,
		OutMsgQueueInfo: queueRoot,
		Stats:           statsRoot,
		McStateExtra:    extraRoot,
	}
	oldState.Accounts.ShardAccounts = accounts
	stateRoot, err := tlb.ToCell(&oldState)
	if err != nil {
		t.Fatal(err)
	}
	stateHash := stateRoot.HashKey()
	previousID := ton.BlockIDExt{
		Workchain: address.MasterchainID,
		Shard:     math.MinInt64,
		RootHash:  bytes.Clone(stateHash[:]),
		FileHash:  bytes.Repeat([]byte{0x42}, 32),
	}

	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracked, err := tracker.Apply(groups.ApplyInput{
		Block: previousID,
		Root:  stateRoot,
		AsOf:  time.Unix(int64(genUtime), 0),
	})
	if err != nil {
		t.Fatalf("derive validator group snapshot: %v", err)
	}
	if tracked.Snapshot.Config == nil || !tracked.Snapshot.Ready {
		t.Fatal("tracker did not publish a parsed ready group snapshot")
	}

	newShardRoot := masterBuildShardBlockRoot(
		t,
		tlb.ShardIdent{WorkchainID: 0},
		0,
		oldShard.SeqNo+1,
		genUtime+1,
		0,
		nil,
	)
	newShardHash := newShardRoot.HashKey()
	newShard := ton.BlockIDExt{
		Workchain: 0,
		Shard:     math.MinInt64,
		SeqNo:     oldShard.SeqNo + 1,
		RootHash:  bytes.Clone(newShardHash[:]),
		FileHash:  bytes.Repeat([]byte{0x52}, 32),
	}
	// RegMcSeqno is assigned by the masterchain core, not trusted from the
	// acquisition pipeline's descriptor projection.
	newShardDescriptor := masterBuildShardDescriptor(t, newShard, 0, genUtime+1)
	topProof := masterBuildProvenTopBlockDescr(t, newShard, newShardRoot)
	var randSeed, createdBy [32]byte
	for i := range randSeed {
		randSeed[i] = byte(i + 1)
		createdBy[i] = byte(0x80 + i)
	}
	queueSize := uint64(0)
	request := MasterRequest{
		Previous: PreviousBlock{
			ID:           previousID,
			State:        stateRoot,
			OutQueueSize: &queueSize,
		},
		Config: config,
		Groups: tracked.Snapshot,
		Header: HeaderParams{
			GenUtime:   genUtime + 1,
			GenUtimeMS: uint64(genUtime+1)*1000 + 321,
		},
		RandSeed:            randSeed,
		CreatedBy:           createdBy,
		MaxExternalAttempts: 64,
		Internals:           &msgpool.Cut{},
		ShardTops: []ShardTop{{
			Block:         newShard,
			Predecessors:  []ton.BlockIDExt{oldShard},
			Descriptor:    newShardDescriptor,
			TopBlockDescr: topProof,
			Creators:      [][32]byte{{0x66}},
		}},
	}

	return masterBuildFixture{
		request:       request,
		configRoot:    configRoot,
		configAddress: bytes.Clone(configAddress),
		oldState:      oldState,
		oldStats:      oldStats,
		oldExtra:      oldExtra,
		oldInfo:       oldInfo,
		oldShard:      oldShard,
		newShard:      newShard,
		topProof:      topProof,
	}
}

// masterBuildAccounts seeds the config contract with configRoot. That is the
// configuration the collator adopts, and it is the predecessor state extra's
// configuration unless the caller asked for a key block.
func masterBuildAccounts(
	t testing.TB,
	configRoot *cell.Cell,
	configAddress []byte,
	genUtime uint32,
	malformed bool,
) *tlb.ShardAccountsAugDict {
	t.Helper()

	data := cell.BeginCell()
	if !malformed {
		data.MustStoreRef(configRoot)
	}
	accountRoot, err := (tlb.AccountState{
		IsValid: true,
		Address: address.NewAddress(0, 0xff, configAddress),
		StorageInfo: tlb.StorageInfo{
			StorageUsed: tlb.StorageUsed{
				CellsUsed: big.NewInt(0),
				BitsUsed:  big.NewInt(0),
			},
			StorageExtra: tlb.StorageExtraNone{},
			LastPaid:     genUtime,
		},
		AccountStorage: tlb.AccountStorage{
			Status:  tlb.AccountStatusActive,
			Balance: tlb.FromNanoTONU(10_000_000_000),
			StateInit: &tlb.StateInit{
				Data: data.EndCell(),
			},
		},
	}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	shardAccount, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       accountRoot,
		LastTransHash: make([]byte, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	key := cell.BeginCell().MustStoreSlice(configAddress, 256).EndCell()
	if err = accounts.Set(key, shardAccount); err != nil {
		t.Fatal(err)
	}

	raw := tlb.BlockchainConfig{Root: configRoot}
	feeCollector, err := raw.GetFeeCollectorAddress()
	if err != nil {
		t.Fatal(err)
	}
	minter, err := raw.GetMinterAddress()
	if err != nil {
		t.Fatal(err)
	}
	seen := map[[32]byte]struct{}{}
	for _, accountID := range [][]byte{feeCollector, minter} {
		var id [32]byte
		copy(id[:], accountID)
		if bytes.Equal(id[:], configAddress) {
			continue
		}
		if _, exists := seen[id]; exists {
			continue
		}
		seen[id] = struct{}{}

		addr := address.NewAddress(0, 0xff, id[:])
		key = cell.BeginCell().MustStoreSlice(id[:], 256).EndCell()
		if err = accounts.Set(key, activeShardAccount(t, activeContract{
			address: addr,
			code:    externalAcceptCode(t),
			balance: 10_000_000_000,
		}, genUtime)); err != nil {
			t.Fatal(err)
		}
	}
	return accounts
}

func masterBuildPrepareConfig(t testing.TB, root *cell.Cell) *Config {
	t.Helper()

	return testPrepareConfig(t, root)
}

// masterBuildCatchainLifetimes rewrites config parameter 28 so the masterchain
// and shard catchain lifetimes can be crossed independently. Mainnet sets both
// to 250, which makes every boundary cross both at once.
func masterBuildCatchainLifetimes(t testing.TB, base *cell.Cell, mc, shard uint32) *cell.Cell {
	t.Helper()

	catchain, err := (tlb.BlockchainConfig{Root: base}).GetCatchainConfig()
	if err != nil {
		t.Fatal(err)
	}
	var parameter *cell.Cell
	switch value := catchain.Config.(type) {
	case tlb.CatchainConfigV1:
		value.McCatchainLifetime = mc
		value.ShardCatchainLifetime = shard
		parameter, err = tlb.ToCell(&value)
	case tlb.CatchainConfigV2:
		value.McCatchainLifetime = mc
		value.ShardCatchainLifetime = shard
		parameter, err = tlb.ToCell(&value)
	default:
		t.Fatalf("unexpected catchain config %T", catchain.Config)
	}
	if err != nil {
		t.Fatal(err)
	}
	return masterBuildConfigWithParam(t, base, 28, parameter)
}

// masterBuildConfigWithParam replaces one configuration parameter, keeping the
// rest of the dictionary as it was.
func masterBuildConfigWithParam(t testing.TB, base *cell.Cell, id int64, parameter *cell.Cell) *cell.Cell {
	t.Helper()

	dict := base.AsDict(32)
	value := cell.BeginCell().MustStoreRef(parameter).EndCell()
	if err := dict.SetIntKey(big.NewInt(id), value); err != nil {
		t.Fatal(err)
	}
	return dict.AsCell()
}

// masterBuildKeyBlockConfig produces a configuration that differs from base
// only by a negative parameter. validateMasterConfigData skips negative ids, so
// the result stays a valid configuration while its root hash moves — which is
// what makes deriveMasterConfigTransition report a key block.
func masterBuildKeyBlockConfig(t testing.TB, base *cell.Cell) *cell.Cell {
	t.Helper()

	return masterBuildConfigWithParam(t, base, -100, cell.BeginCell().MustStoreUInt(0xfeed, 32).EndCell())
}

// masterBuildBoundaryTime returns a predecessor generation time whose successor
// starts a new `period` window, and — when `notPeriod` is non-zero — stays
// inside the current `notPeriod` window.
func masterBuildBoundaryTime(t testing.TB, period, notPeriod uint32) uint32 {
	t.Helper()

	if period == 0 {
		t.Fatal("boundary period is zero")
	}
	for value := uint32(1_900_000_000); value < 1_900_100_000; value++ {
		next := value + 1
		if next%period != 0 {
			continue
		}
		if notPeriod != 0 && next%notPeriod == 0 {
			continue
		}
		return value
	}
	t.Fatalf("no generation time crosses %d without crossing %d", period, notPeriod)
	return 0
}

func masterBuildSafeTime(config *groups.Config) uint32 {
	value := uint32(1_900_000_000)
	for {
		crosses := false
		for _, lifetime := range [...]uint32{
			config.Catchain.MasterchainLifetime,
			config.Catchain.ShardLifetime,
		} {
			if lifetime > 0 && value/lifetime != (value+1)/lifetime {
				crosses = true
				value++
				break
			}
		}
		if !crosses {
			return value
		}
	}
}

func masterBuildShardDescriptor(
	t testing.TB,
	block ton.BlockIDExt,
	regMCSeqno uint32,
	genUtime uint32,
) *cell.Cell {
	t.Helper()

	descriptor := tlb.ShardDesc{
		SeqNo:              block.SeqNo,
		RegMcSeqno:         regMCSeqno,
		StartLT:            uint64(block.SeqNo) * 1_000,
		EndLT:              uint64(block.SeqNo)*1_000 + 999,
		RootHash:           bytes.Clone(block.RootHash),
		FileHash:           bytes.Clone(block.FileHash),
		NextCatchainSeqNo:  regMCSeqno,
		NextValidatorShard: block.Shard,
		MinRefMcSeqNo:      regMCSeqno,
		GenUTime:           genUtime,
		SplitMergeAt:       tlb.FutureSplitMergeNone{},
	}
	root, err := tlb.ToCell(&descriptor)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func masterBuildShardHashes(t testing.TB, descriptor *cell.Cell) *cell.Dictionary {
	t.Helper()

	leaf := cell.BeginCell().MustStoreBoolBit(false).MustStoreBuilder(descriptor.ToBuilder()).EndCell()
	dict := cell.NewDict(32)
	key := cell.BeginCell().MustStoreInt(0, 32).EndCell()
	value := cell.BeginCell().MustStoreRef(leaf).EndCell()
	if err := dict.Set(key, value); err != nil {
		t.Fatal(err)
	}
	return dict
}

// masterBuildShardBlockRoot assembles a shardchain block whose BlockInfo and
// ValueFlow carry the values a ShardDesc projects, so a TopBlockDescr proof over
// it binds the descriptor to a real header the way a shard descriptor is
// derived from a block. catchainSeqno doubles as min_ref_mc_seqno,
// matching masterBuildShardDescriptor's projection of its regMCSeqno argument.
func masterBuildShardBlockRoot(
	t testing.TB,
	shard tlb.ShardIdent,
	globalID int32,
	seqno, genUtime, catchainSeqno uint32,
	stateUpdate *cell.Cell,
) *cell.Cell {
	return masterBuildShardBlockRootWithBeforeSplit(
		t,
		shard,
		globalID,
		seqno,
		genUtime,
		catchainSeqno,
		false,
		stateUpdate,
	)
}

func masterBuildShardBlockRootWithBeforeSplit(
	t testing.TB,
	shard tlb.ShardIdent,
	globalID int32,
	seqno, genUtime, catchainSeqno uint32,
	beforeSplit bool,
	stateUpdate *cell.Cell,
) *cell.Cell {
	t.Helper()

	master := tlb.ExtBlkRef{
		EndLt:    uint64(seqno) * 1_000,
		SeqNo:    catchainSeqno,
		RootHash: bytes.Repeat([]byte{0x71}, 32),
		FileHash: bytes.Repeat([]byte{0x72}, 32),
	}
	var header tlb.BlockHeader
	header.NotMaster = true
	header.SeqNo = seqno
	header.Shard = shard
	header.GenUtime = genUtime
	header.StartLt = uint64(seqno) * 1_000
	header.EndLt = uint64(seqno)*1_000 + 999
	header.GenCatchainSeqno = catchainSeqno
	header.MinRefMcSeqno = catchainSeqno
	header.BeforeSplit = beforeSplit
	header.MasterRef = &master
	header.PrevRef = tlb.BlkPrevInfo{Prev1: tlb.ExtBlkRef{
		EndLt:    uint64(seqno-1) * 1_000,
		SeqNo:    seqno - 1,
		RootHash: bytes.Repeat([]byte{0x73}, 32),
		FileHash: bytes.Repeat([]byte{0x74}, 32),
	}}
	info, err := header.ToCell()
	if err != nil {
		t.Fatal(err)
	}

	zero := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(0)}
	flow, err := tlb.ValueFlow{
		FromPrevBlock: zero,
		ToNextBlock:   zero,
		Imported:      zero,
		Exported:      zero,
		FeesCollected: zero,
		FeesImported:  zero,
		Recovered:     zero,
		Created:       zero,
		Minted:        zero,
		Burned:        zero,
	}.ToCell()
	if err != nil {
		t.Fatal(err)
	}

	empty := cell.BeginCell().EndCell()
	if stateUpdate == nil {
		stateUpdate = empty
	}
	return cell.BeginCell().
		MustStoreUInt(blockTag, 32).
		MustStoreInt(int64(globalID), 32).
		MustStoreRef(info).
		MustStoreRef(flow).
		MustStoreRef(stateUpdate).
		MustStoreRef(empty).
		EndCell()
}

// masterBuildProvenTopBlockDescr wraps a real block into a single-link
// TopBlockDescr that survives the descriptor cross-check.
func masterBuildProvenTopBlockDescr(t testing.TB, block ton.BlockIDExt, blockRoot *cell.Cell) *cell.Cell {
	t.Helper()

	proof, err := cell.CreateMerkleProof(blockRoot)
	if err != nil {
		t.Fatal(err)
	}
	return masterBuildTopBlockDescrEnvelope(t, block, proof, nil)
}

// masterBuildTopBlockDescr builds an envelope over a stub proof. It is only
// usable where the shape of the TopBlockDescr matters and the projected
// descriptor is not checked against the proven header.
func masterBuildTopBlockDescr(t *testing.T, block ton.BlockIDExt) *cell.Cell {
	t.Helper()

	return masterBuildSignedTopBlockDescr(t, block, nil)
}

// masterBuildSignedTopBlockDescr is masterBuildTopBlockDescr with the optional
// BlockSignatures ref filled in. Every real descriptor carries one — C++
// prevalidate refuses a descriptor without signatures outside fake mode — so
// this is the shape the signature validator actually meets in production.
func masterBuildSignedTopBlockDescr(t testing.TB, block ton.BlockIDExt, signatures *cell.Cell) *cell.Cell {
	t.Helper()

	chainRoot, err := cell.CreateMerkleProof(cell.BeginCell().MustStoreUInt(blockTag, 32).EndCell())
	if err != nil {
		t.Fatal(err)
	}
	return masterBuildTopBlockDescrEnvelope(t, block, chainRoot, signatures)
}

// signatures is optional: a nil ref keeps the historic signature-less bytes,
// which the masterchain parity goldens are built over.
func masterBuildTopBlockDescrEnvelope(
	t testing.TB,
	block ton.BlockIDExt,
	chainRoot *cell.Cell,
	signatures *cell.Cell,
) *cell.Cell {
	t.Helper()

	ident, err := topologyShardIdent(groups.ShardID{Workchain: block.Workchain, Shard: block.Shard})
	if err != nil {
		t.Fatal(err)
	}
	identRoot, err := tlb.ToCell(&ident)
	if err != nil {
		t.Fatal(err)
	}
	envelope := cell.BeginCell().
		MustStoreUInt(0xd5, 8).
		MustStoreBuilder(identRoot.ToBuilder()).
		MustStoreUInt(uint64(block.SeqNo), 32).
		MustStoreSlice(block.RootHash, 256).
		MustStoreSlice(block.FileHash, 256).
		MustStoreBoolBit(signatures != nil)
	if signatures != nil {
		envelope.MustStoreRef(signatures)
	}
	return envelope.
		MustStoreUInt(1, 8).
		MustStoreRef(chainRoot).
		EndCell()
}

func masterBuildActiveSession(t *testing.T, snapshot *groups.Snapshot) *groups.Session {
	t.Helper()

	var found *groups.Session
	for i := range snapshot.Active {
		if !snapshot.Active[i].Shard.IsMasterchain() {
			continue
		}
		if found != nil {
			t.Fatal("multiple active masterchain sessions")
		}
		found = &snapshot.Active[i]
	}
	if found == nil {
		t.Fatal("active masterchain session is absent")
	}
	return found
}

func assertMasterBuildConfig(
	t *testing.T,
	fixture masterBuildFixture,
	stateExtra *tlb.McStateExtra,
	blockExtra *tlb.McBlockExtra,
) {
	t.Helper()

	if blockExtra.KeyBlock || blockExtra.ConfigParams != nil {
		t.Fatal("unchanged configuration produced a key block")
	}
	if !bytes.Equal(stateExtra.ConfigParams.ConfigAddr, fixture.configAddress) ||
		stateExtra.ConfigParams.Config.Params == nil ||
		stateExtra.ConfigParams.Config.Params.AsCell().HashKey() != fixture.configRoot.HashKey() {
		t.Fatal("next state configuration differs from the active config contract")
	}
}

func assertMasterBuildHistory(
	t *testing.T,
	fixture masterBuildFixture,
	stateExtra *tlb.McStateExtra,
	header *tlb.BlockHeader,
) {
	t.Helper()

	info, err := parseMasterStateInfo(stateExtra.Info)
	if err != nil {
		t.Fatal(err)
	}
	if info.AfterKeyBlock || info.LastKeyBlock == nil ||
		info.LastKeyBlock.SeqNo != fixture.request.Previous.ID.SeqNo ||
		!bytes.Equal(info.LastKeyBlock.RootHash, fixture.request.Previous.ID.RootHash) ||
		!bytes.Equal(info.LastKeyBlock.FileHash, fixture.request.Previous.ID.FileHash) {
		t.Fatalf("last key block history = %+v", info.LastKeyBlock)
	}
	if info.ValidatorInfo.CatchainSeqno != fixture.oldInfo.ValidatorInfo.CatchainSeqno ||
		info.ValidatorInfo.NextCCUpdated {
		t.Fatalf("validator info unexpectedly rotated: %+v", info.ValidatorInfo)
	}
	wantHash, err := groups.MasterchainStateValidatorHash(
		fixture.request.Groups.Config,
		info.ValidatorInfo.CatchainSeqno,
	)
	if err != nil {
		t.Fatal(err)
	}
	if info.ValidatorInfo.ValidatorListHashShort != wantHash {
		t.Fatalf("state validator hash = %x, want %x", info.ValidatorInfo.ValidatorListHashShort, wantHash)
	}
	if fixture.request.Config.capabilities&capCreateStats == 0 {
		t.Fatal("mainnet fixture unexpectedly disables creator statistics")
	}
	if info.Flags&1 == 0 || info.BlockCreateStats == nil {
		t.Fatal("resulting masterchain state has no creator statistics")
	}
	creatorStats, err := openBlockCreateStats(info.BlockCreateStats)
	if err != nil {
		t.Fatal(err)
	}
	assertMasterBuildCreatorStats(t, creatorStats, fixture.request.CreatedBy, [32]byte{0x66}, header.GenUtime)

	key := cell.BeginCell().MustStoreUInt(uint64(fixture.request.Previous.ID.SeqNo), 32).EndCell()
	value, err := info.PrevBlocks.LoadValue(key)
	if err != nil {
		t.Fatal(err)
	}
	var previous tlb.KeyExtBlkRef
	if err = loadExactSlice(&previous, value); err != nil {
		t.Fatal(err)
	}
	if !previous.IsKey || previous.BlkRef.SeqNo != fixture.request.Previous.ID.SeqNo ||
		previous.BlkRef.EndLt != fixture.oldState.GenLT ||
		!bytes.Equal(previous.BlkRef.RootHash, fixture.request.Previous.ID.RootHash) ||
		!bytes.Equal(previous.BlkRef.FileHash, fixture.request.Previous.ID.FileHash) {
		t.Fatalf("previous masterchain history entry = %+v", previous)
	}
	if header.PrevKeyBlockSeqno != fixture.request.Previous.ID.SeqNo {
		t.Fatalf("header previous key block = %d", header.PrevKeyBlockSeqno)
	}
}

func assertMasterBuildCreatorStats(
	t *testing.T,
	stats blockCreateStats,
	masterCreator [32]byte,
	shardCreator [32]byte,
	now uint32,
) {
	t.Helper()

	entries := blockCreateStatsTestEntries(t, stats)
	master := entries[masterCreator]
	if master.masterchain.total != 1 || master.masterchain.lastUpdated != now || master.shardchain.total != 0 {
		t.Fatalf("master creator stats = %+v", master)
	}
	shard := entries[shardCreator]
	if shard.shardchain.total != 1 || shard.shardchain.lastUpdated != now || shard.masterchain.total != 0 {
		t.Fatalf("shard creator stats = %+v", shard)
	}
	aggregate := entries[[32]byte{}]
	if aggregate.masterchain.total != 1 || aggregate.shardchain.total != 1 ||
		aggregate.masterchain.lastUpdated != now || aggregate.shardchain.lastUpdated != now {
		t.Fatalf("aggregate creator stats = %+v", aggregate)
	}
}

func assertMasterBuildShardRegistry(
	t *testing.T,
	fixture masterBuildFixture,
	stateExtra *tlb.McStateExtra,
	blockExtra *tlb.McBlockExtra,
) {
	t.Helper()

	if !equalDictionary(stateExtra.ShardHashes, blockExtra.ShardHashes) ||
		blockExtra.ShardFees == nil || !blockExtra.ShardFees.ValidateAll() {
		t.Fatal("masterchain block and state shard registries disagree")
	}
	registry, err := ParseShardRegistry(stateExtra.ShardHashes)
	if err != nil {
		t.Fatal(err)
	}
	tops := registry.Tops()
	if len(tops) != 1 || !sameShardBlock(tops[0].Block, fixture.newShard) {
		t.Fatalf("next shard registry = %+v", tops)
	}
	fields, err := parseShardDescriptorFields(tops[0].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if fields.regMCSeqno != fixture.request.Previous.ID.SeqNo+1 {
		t.Fatalf("shard registration seqno = %d, want %d", fields.regMCSeqno, fixture.request.Previous.ID.SeqNo+1)
	}
	fee := masterShardTestLoadFee(t, blockExtra.ShardFees, fixture.newShard)
	if !currencyZero(fee.Fees) || !currencyZero(fee.Create) {
		t.Fatalf("zero-fee shard top produced fees: %+v", fee)
	}
}

func assertMasterBuildSpecials(
	t *testing.T,
	fixture masterBuildFixture,
	startLT uint64,
	candidate *Candidate,
	flow *tlb.ValueFlow,
	extra *tlb.McBlockExtra,
) {
	t.Helper()

	recovered := !currencyZero(flow.Recovered)
	minted := !currencyZero(flow.Minted)
	if (extra.Details.RecoverCreateMsg != nil) != recovered || (extra.Details.MintMsg != nil) != minted {
		t.Fatal("recover or mint descriptors disagree with value flow")
	}
	inMessages, outMessages := candidateMessageDescriptors(t, candidate, fixture.request.Config.globalVersion)
	raw := tlb.BlockchainConfig{Root: fixture.configRoot}
	collector, err := raw.GetFeeCollectorAddress()
	if err != nil {
		t.Fatal(err)
	}
	minter, err := raw.GetMinterAddress()
	if err != nil {
		t.Fatal(err)
	}
	var zeroAddress [32]byte
	for _, special := range []struct {
		name        string
		descriptor  *cell.Cell
		amount      tlb.CurrencyCollection
		destination []byte
	}{
		{name: "recover", descriptor: extra.Details.RecoverCreateMsg, amount: flow.Recovered, destination: collector},
		{name: "mint", descriptor: extra.Details.MintMsg, amount: flow.Minted, destination: minter},
	} {
		descriptor := special.descriptor
		if descriptor == nil {
			continue
		}
		loader := descriptor.MustBeginParse()
		if tag := loader.MustLoadUInt(3); tag != 0b011 {
			t.Fatalf("%s descriptor tag = %03b, want msg_import_imm", special.name, tag)
		}
		envelopeRoot, err := loader.LoadRefCell()
		if err != nil {
			t.Fatal(err)
		}
		transactionRoot, err := loader.LoadRefCell()
		if err != nil {
			t.Fatal(err)
		}
		forwardFee, err := loader.LoadBigCoins()
		if err != nil || forwardFee.Sign() != 0 || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
			t.Fatalf("%s descriptor has non-zero fee or trailing data", special.name)
		}

		var envelope tlb.MsgEnvelope
		if err = parseExact(&envelope, envelopeRoot); err != nil {
			t.Fatalf("decode %s envelope: %v", special.name, err)
		}
		if envelope.CurAddr.Type != tlb.IntermediateAddressRegular || envelope.CurAddr.UseDestBits != routingAddressBits ||
			envelope.NextAddr.Type != tlb.IntermediateAddressRegular || envelope.NextAddr.UseDestBits != routingAddressBits ||
			envelope.FwdFeeRemaining.Nano().Sign() != 0 || envelope.EmittedLT != nil || envelope.Metadata != nil {
			t.Fatalf("%s envelope is not a zero-fee final route", special.name)
		}
		var message tlb.InternalMessage
		if err = parseExact(&message, envelope.Msg); err != nil {
			t.Fatalf("decode %s system message: %v", special.name, err)
		}
		amount := tlb.CurrencyCollection{Coins: message.Amount, ExtraCurrencies: message.ExtraCurrencies}
		if !message.IHRDisabled || !message.Bounce || message.Bounced ||
			message.SrcAddr.Workchain() != address.MasterchainID || !bytes.Equal(message.SrcAddr.Data(), zeroAddress[:]) ||
			message.DstAddr.Workchain() != address.MasterchainID || !bytes.Equal(message.DstAddr.Data(), special.destination) ||
			!amount.Equals(special.amount) || message.IHRFee.Nano().Sign() != 0 || message.FwdFee.Nano().Sign() != 0 ||
			message.CreatedLT != startLT || message.CreatedAt != fixture.request.Header.GenUtime ||
			message.StateInit != nil || message.Body == nil || message.Body.BitsSize() != 0 || message.Body.RefsNum() != 0 {
			t.Fatalf("%s system message differs from protocol template", special.name)
		}
		var transaction tlb.Transaction
		if err = parseExact(&transaction, transactionRoot); err != nil {
			t.Fatalf("decode %s transaction: %v", special.name, err)
		}
		if !bytes.Equal(transaction.AccountAddr, special.destination) || transaction.Now != fixture.request.Header.GenUtime ||
			transaction.LT != startLT+1 {
			t.Fatalf("%s transaction is not bound to the system message", special.name)
		}
		messageHash := envelope.Msg.HashKey()
		stored := descriptorByHash(t, inMessages.AugmentedDictionary, messageHash)
		if stored.MustToCell().HashKey() != descriptor.HashKey() {
			t.Fatalf("%s descriptor is not the exact inbound dictionary value", special.name)
		}
		key := cell.BeginCell().MustStoreSlice(messageHash[:], 256).EndCell()
		if _, err = outMessages.LoadValue(key); !isMissingKey(err) {
			t.Fatalf("%s system inbound unexpectedly has an outbound descriptor", special.name)
		}
		// Assert against the decoder that actually decides on the validation
		// path, so a masterchain descriptor the builder emits and the validator
		// would reject cannot pass here.
		if _, err := parseSemanticInDescriptor(*descriptor.MustBeginParse(), [32]byte(messageHash), nil); err != nil {
			t.Fatalf("parse %s descriptor: %v", special.name, err)
		}
	}
	wantTransactions := uint32(0)
	if recovered {
		wantTransactions++
	}
	if minted {
		wantTransactions++
	}
	if candidate.Stats.Transactions != wantTransactions {
		t.Fatalf("masterchain transaction count = %d, want %d protocol specials",
			candidate.Stats.Transactions, wantTransactions)
	}
}

func assertMasterBuildConfigAccount(t *testing.T, fixture masterBuildFixture, state *tlb.ShardStateUnsplit) {
	t.Helper()

	key := cell.BeginCell().MustStoreSlice(fixture.configAddress, 256).EndCell()
	value, err := state.Accounts.ShardAccounts.LoadValue(key)
	if err != nil {
		t.Fatal(err)
	}
	var shardAccount tlb.ShardAccount
	if err = loadExactSlice(&shardAccount, value); err != nil {
		t.Fatal(err)
	}
	var account tlb.AccountState
	if err = parseExact(&account, shardAccount.Account); err != nil {
		t.Fatal(err)
	}
	if account.Status != tlb.AccountStatusActive || account.StateInit == nil || account.StateInit.Data == nil {
		t.Fatal("configuration account is no longer active")
	}
	data := account.StateInit.Data.MustBeginParse()
	root, err := data.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if root.HashKey() != fixture.configRoot.HashKey() || data.BitsLeft() != 0 || data.RefsNum() != 0 {
		t.Fatal("configuration account data changed")
	}
}

func assertMasterBuildCollatedOrder(t *testing.T, fixture masterBuildFixture, candidate *Candidate) {
	t.Helper()

	hash := sha256.Sum256(candidate.CollatedData)
	if hash != candidate.CollatedFileHash {
		t.Fatal("collated file hash mismatch")
	}
	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	if len(roots) < 2 {
		t.Fatalf("collated roots = %d, want TopBlockDescrSet then ConsensusExtraData", len(roots))
	}
	set := roots[0].MustBeginParse()
	if tag := set.MustLoadUInt(32); tag != 0x4ac789f3 {
		t.Fatalf("first collated root tag = %x", tag)
	}
	dict := set.MustLoadDict(96)
	if set.BitsLeft() != 0 || set.RefsNum() != 0 || !dict.ValidateAll() {
		t.Fatal("malformed top block descriptor set")
	}
	key := cell.BeginCell().MustStoreInt(int64(fixture.newShard.Workchain), 32).
		MustStoreUInt(uint64(fixture.newShard.Shard), 64).EndCell()
	value, err := dict.LoadValue(key)
	if err != nil {
		t.Fatal(err)
	}
	proof, err := value.LoadRefCell()
	if err != nil || value.BitsLeft() != 0 || value.RefsNum() != 0 || proof.HashKey() != fixture.topProof.HashKey() {
		t.Fatal("collated top descriptor proof differs from request")
	}
	consensus := roots[1].MustBeginParse()
	if tag := consensus.MustLoadUInt(32); tag != consensusExtraDataTag {
		t.Fatalf("second collated root tag = %x", tag)
	}
	if flags := consensus.MustLoadUInt(32); flags != 0 {
		t.Fatalf("consensus flags = %x", flags)
	}
	if got := consensus.MustLoadUInt(64); got != fixture.request.Header.GenUtimeMS ||
		consensus.BitsLeft() != 0 || consensus.RefsNum() != 0 {
		t.Fatalf("consensus generation time = %d", got)
	}

	if fixture.request.Config.capabilities&capFullCollatedData == 0 {
		if len(roots) != 2 {
			t.Fatalf("compact collated roots = %d, want 2", len(roots))
		}
		return
	}
	var previousHash cell.Hash
	for i := 2; i < len(roots); i++ {
		loader := roots[i].MustBeginParse()
		if tag := loader.MustLoadUInt(32); tag != 0x37c1e3fc {
			t.Fatalf("collated root %d tag = %x, want account storage proof", i, tag)
		}
		proof, err := loader.LoadRefCell()
		if err != nil || loader.BitsLeft() != 0 || loader.RefsNum() != 0 ||
			proof.GetType() != cell.MerkleProofCellType {
			t.Fatalf("malformed account storage proof at root %d", i)
		}
		body := proof.MustPeekRef(0)
		hash := body.HashKey(0)
		if _, err = cell.UnwrapProof(proof, hash[:]); err != nil {
			t.Fatalf("unwrap account storage proof %d: %v", i, err)
		}
		if i > 2 && bytes.Compare(previousHash[:], hash[:]) >= 0 {
			t.Fatal("account storage proofs are not ordered by virtual root hash")
		}
		previousHash = hash
	}
}
