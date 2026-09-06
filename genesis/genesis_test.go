package genesis

import (
	"bytes"
	"context"
	"crypto/ed25519"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"math"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
	tonaddress "github.com/xssnick/tonutils-go/address"
	adnladdress "github.com/xssnick/tonutils-go/adnl/address"
	"github.com/xssnick/tonutils-go/adnl/dht"
	"github.com/xssnick/tonutils-go/adnl/keys"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	stateflow "github.com/xssnick/gton/service/state"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestGenerateBuildsPortableDatabaseAndIsIdempotent(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	paths := Paths{
		Genesis:      filepath.Join(dir, "genesis.json"),
		Data:         filepath.Join(dir, "data"),
		GlobalConfig: filepath.Join(dir, "global.config.json"),
		Lock:         filepath.Join(dir, "genesis.lock.json"),
	}
	spec := validTestSpec(t)
	writeSpec(t, paths.Genesis, spec)

	first, err := Generate(context.Background(), paths, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if !first.Created {
		t.Fatal("first generation reported no created artifacts")
	}
	if first.GenesisTime != spec.GenesisTime {
		t.Fatalf("genesis time = %d, want %d", first.GenesisTime, spec.GenesisTime)
	}

	global, err := liteclient.GetConfigFromFile(paths.GlobalConfig)
	if err != nil {
		t.Fatal(err)
	}
	if global.Type != "config.global" || len(global.DHT.StaticNodes.Nodes) != 1 {
		t.Fatalf("unexpected global config: %#v", global)
	}
	if global.Validator.ZeroState.Workchain != -1 ||
		!bytes.Equal(global.Validator.ZeroState.RootHash, first.Masterchain.RootHash) ||
		!bytes.Equal(global.Validator.ZeroState.FileHash, first.Masterchain.FileHash) {
		t.Fatalf("global config zerostate does not match result: %#v", global.Validator.ZeroState)
	}
	if global.Validator.InitBlock.Workchain != -1 ||
		global.Validator.InitBlock.Shard != first.Masterchain.Shard ||
		global.Validator.InitBlock.SeqNo != first.Masterchain.SeqNo ||
		!bytes.Equal(global.Validator.InitBlock.RootHash, first.Masterchain.RootHash) ||
		!bytes.Equal(global.Validator.InitBlock.FileHash, first.Masterchain.FileHash) {
		t.Fatalf("global config init block does not match zerostate: %#v", global.Validator.InitBlock)
	}

	lock, exists, err := readLock(paths.Lock)
	if err != nil {
		t.Fatal(err)
	}
	if !exists || len(lock.Validators) != 3 {
		t.Fatalf("unexpected lock: %#v", lock)
	}
	for name, value := range map[string]string{
		"wallet":  lock.Addresses.Wallet,
		"elector": lock.Addresses.Elector,
		"config":  lock.Addresses.Config,
	} {
		parsed, err := tonaddress.ParseAddr(value)
		if err != nil {
			t.Fatalf("parse %s address %q: %v", name, value, err)
		}
		if !parsed.IsBounceable() || !parsed.IsTestnetOnly() || parsed.Workchain() != -1 {
			t.Fatalf("%s address is not canonical testnet masterchain form: %s", name, value)
		}
		if strings.Contains(value, ":") {
			t.Fatalf("%s address is raw: %s", name, value)
		}
	}

	validated, err := validateSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	built, err := buildStates(validated, spec.GenesisTime)
	if err != nil {
		t.Fatal(err)
	}
	if err = verifyData(context.Background(), paths.Data, built); err != nil {
		t.Fatal(err)
	}
	copiedData := filepath.Join(dir, "copied-data")
	if err = os.CopyFS(copiedData, os.DirFS(paths.Data)); err != nil {
		t.Fatalf("copy closed genesis database: %v", err)
	}
	if err = verifyData(context.Background(), copiedData, built); err != nil {
		t.Fatalf("verify copied genesis database: %v", err)
	}

	second, err := Generate(context.Background(), paths, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	if second.Created {
		t.Fatal("idempotent generation rewrote an artifact")
	}
	if !second.Masterchain.Equals(&first.Masterchain) || !second.Basechain.Equals(&first.Basechain) {
		t.Fatal("idempotent generation changed zerostate IDs")
	}
}

func TestAutomaticGenesisTimeStartsCurrentMasterchainLifetime(t *testing.T) {
	t.Parallel()

	const lifetime = uint32(250)
	tests := []struct {
		name string
		now  uint32
		want uint32
	}{
		{name: "at boundary", now: 1_000, want: 1_000},
		{name: "safe before boundary", now: 1_189, want: 1_000},
		{name: "one minute before boundary", now: 1_190, want: 1_000},
		{name: "close to boundary", now: 1_248, want: 1_000},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			got, err := automaticGenesisTime(test.now, lifetime)
			if err != nil {
				t.Fatal(err)
			}
			if got != test.want {
				t.Fatalf("automaticGenesisTime(%d, %d) = %d, want %d", test.now, lifetime, got, test.want)
			}
		})
	}

	if _, err := automaticGenesisTime(1_000, 0); err == nil {
		t.Fatal("expected zero lifetime error")
	}
}

func TestGeneratedMasterStateIsAcceptedByValidatorTracker(t *testing.T) {
	t.Parallel()

	spec := validTestSpec(t)
	validated, err := validateSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	built, err := buildStates(validated, spec.GenesisTime)
	if err != nil {
		t.Fatal(err)
	}
	state, err := groups.ParseState(groups.StateInput{Block: built.master.block, Root: built.master.root})
	if err != nil {
		t.Fatalf("parse generated master state: %v", err)
	}
	config, err := groups.ParseConfig(state.ConfigRoot)
	if err != nil {
		t.Fatalf("parse generated validator config: %v", err)
	}
	if len(config.ActiveValidators.Validators) != len(spec.Validators) {
		t.Fatalf("active validators = %d, want %d", len(config.ActiveValidators.Validators), len(spec.Validators))
	}
	if config.ActiveValidators.Main != uint16(len(spec.Validators)) {
		t.Fatalf("masterchain validators = %d, want %d", config.ActiveValidators.Main, len(spec.Validators))
	}
	if config.NewConsensus.Masterchain == nil || config.NewConsensus.Shard == nil {
		t.Fatal("Simplex config is absent")
	}
	if config.NewConsensus.Masterchain.ProtocolVersion != 3 ||
		config.NewConsensus.Masterchain.SimplexParams().TargetRate.Milliseconds() != int64(spec.Consensus.TargetBlockRateMS) ||
		config.NewConsensus.Masterchain.SimplexParams().FirstBlockTimeout.Milliseconds() != int64(spec.Consensus.FirstBlockTimeoutMS) {
		t.Fatalf("unexpected masterchain Simplex config: %#v", config.NewConsensus.Masterchain)
	}
	workchain, err := (tlb.BlockchainConfig{Root: state.ConfigRoot}).GetWorkchainDescr(0)
	if err != nil {
		t.Fatalf("parse generated workchain descriptor: %v", err)
	}
	v2, ok := workchain.Descr.(tlb.WorkchainDescrV2)
	if !ok {
		t.Fatalf("generated workchain descriptor uses %T, want WorkchainDescrV2", workchain.Descr)
	}
	if v2.SplitMergeTimings.SplitMergeDelay != 20 ||
		v2.SplitMergeTimings.SplitMergeInterval != 20 ||
		v2.SplitMergeTimings.MinSplitMergeInterval != 10 ||
		v2.SplitMergeTimings.MaxSplitMergeDelay != 1000 ||
		v2.MaxSplit != 4 ||
		v2.PersistentStateSplitDepth != 0 {
		t.Fatalf("unexpected workchain v2 fields: %+v", v2)
	}
	if targets, err := state.CurrentTargets(); err != nil || len(targets) != 1 || targets[0].Shard.Workchain != -1 {
		t.Fatalf("genesis targets = %#v, err = %v", targets, err)
	}
	executionConfig, err := tvm.PrepareBlockchainConfig(state.ConfigRoot)
	if err != nil {
		t.Fatalf("prepare generated execution config: %v", err)
	}
	if _, err = collator.PrepareConfig(executionConfig); err != nil {
		t.Fatalf("prepare generated collator config: %v", err)
	}
}

func TestGeneratedValidatorSetCanRestrictMasterchainMembership(t *testing.T) {
	t.Parallel()

	spec := validTestSpec(t)
	spec.Consensus.MasterchainValidators = 1
	validated, err := validateSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	built, err := buildStates(validated, spec.GenesisTime)
	if err != nil {
		t.Fatal(err)
	}
	state, err := groups.ParseState(groups.StateInput{Block: built.master.block, Root: built.master.root})
	if err != nil {
		t.Fatal(err)
	}
	config, err := groups.ParseConfig(state.ConfigRoot)
	if err != nil {
		t.Fatal(err)
	}
	if config.ActiveValidators.Main != 1 {
		t.Fatalf("masterchain validators = %d, want 1", config.ActiveValidators.Main)
	}
	if len(config.ActiveValidators.Validators) != len(spec.Validators) {
		t.Fatalf("total validators = %d, want %d", len(config.ActiveValidators.Validators), len(spec.Validators))
	}
}

func TestGeneratedValidatorRegistryIsUsableByValidatorTracker(t *testing.T) {
	t.Parallel()

	spec := validTestSpec(t)
	spec.ValidatorRegistry = &ValidatorRegistry{MaxCollatorsPerValidator: 8}
	validated, err := validateSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	built, err := buildStates(validated, spec.GenesisTime)
	if err != nil {
		t.Fatal(err)
	}
	if built.validatorRegistry == nil {
		t.Fatal("validator registry address is absent")
	}

	state, err := groups.ParseState(groups.StateInput{Block: built.master.block, Root: built.master.root})
	if err != nil {
		t.Fatalf("parse generated master state: %v", err)
	}
	rawConfig := tlb.BlockchainConfig{Root: state.ConfigRoot}
	parameter, err := rawConfig.GetParam(configParamValidatorRegistry)
	if err != nil {
		t.Fatalf("load validator registry config: %v", err)
	}
	loader := parameter.MustBeginParse()
	constructor := loader.MustLoadUInt(32)
	registryAddress := loader.MustLoadSlice(256)
	maxCollators := loader.MustLoadUInt(32)
	hasNewCodeHash := loader.MustLoadBoolBit()
	if constructor != validatorRegistryConfigConstructor ||
		!bytes.Equal(registryAddress, built.validatorRegistry.Data()) ||
		maxCollators != 8 || hasNewCodeHash || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		t.Fatalf("unexpected validator registry config")
	}
	fundamental, err := rawConfig.GetFundamentalSmartContractAddresses()
	if err != nil {
		t.Fatalf("load fundamental addresses: %v", err)
	}
	if _, err = fundamental.Addresses.LoadValueByBytesKey(built.validatorRegistry.Data()); err != nil {
		t.Fatalf("validator registry is not a special contract: %v", err)
	}

	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := tracker.Apply(groups.ApplyInput{
		Block: built.master.block,
		Root:  built.master.root,
		AsOf:  time.Unix(int64(spec.GenesisTime), 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.CollatorRegistryIssue != nil {
		t.Fatalf("validator registry is unusable: %v", result.CollatorRegistryIssue)
	}
	if len(result.Snapshot.CollatorsByValidator) != 0 {
		t.Fatalf("new validator registry is not empty: %#v", result.Snapshot.CollatorsByValidator)
	}
}

func TestValidatorRegistryRequiresPositiveLimit(t *testing.T) {
	t.Parallel()

	spec := validTestSpec(t)
	spec.ValidatorRegistry = &ValidatorRegistry{}
	if _, err := validateSpec(spec); err == nil || !strings.Contains(err.Error(), "must be positive") {
		t.Fatalf("validator registry validation error = %v", err)
	}
}

func TestGeneratedMasterStateExposesBaseZeroState(t *testing.T) {
	t.Parallel()

	spec := validTestSpec(t)
	validated, err := validateSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	built, err := buildStates(validated, spec.GenesisTime)
	if err != nil {
		t.Fatal(err)
	}
	var parsed tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&parsed, built.master.root.MustBeginParse()); err != nil {
		t.Fatal(err)
	}

	assertBaseZeroState := func(name string, master *storage.BlockState) {
		t.Helper()

		shards, err := stateflow.ShardBlocksFromMasterState(master)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(shards) != 1 || !shards[0].Equals(&built.base.block) {
			t.Fatalf("%s shards = %+v, want base zerostate %s", name, shards, storage.FormatBlockRef(built.base.block))
		}
	}
	assertBaseZeroState("materialized", &storage.BlockState{
		Block:  built.master.block,
		Parsed: &parsed,
	})

	store, err := openGenesisStore(t.TempDir(), false, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lazyRoot, err := store.ImportStateCellTree(context.Background(), built.master.block, built.master.root, 0)
	if err != nil {
		t.Fatal(err)
	}
	lazyState, err := storage.ParseStateCell(
		&built.master.block,
		lazyRoot,
		nil,
		built.master.block.RootHash,
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	assertBaseZeroState("lazy", lazyState)
}

func TestGeneratedBaseStateUsesCanonicalRootShardIdent(t *testing.T) {
	t.Parallel()

	spec := validTestSpec(t)
	validated, err := validateSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	built, err := buildStates(validated, spec.GenesisTime)
	if err != nil {
		t.Fatal(err)
	}

	var state tlb.ShardStateUnsplit
	slice, err := built.base.root.BeginParse()
	if err != nil {
		t.Fatal(err)
	}
	if err = tlb.LoadFromCell(&state, slice); err != nil {
		t.Fatalf("parse generated base state: %v", err)
	}
	if state.ShardIdent.PrefixBits != 0 || state.ShardIdent.WorkchainID != 0 || state.ShardIdent.ShardPrefix != 0 {
		t.Fatalf("base zerostate shard ident = %#v, want workchain root", state.ShardIdent)
	}
}

func TestGeneratedStateBuildsFirstMasterchainBlock(t *testing.T) {
	t.Parallel()

	spec := validTestSpec(t)
	validated, err := validateSpec(spec)
	if err != nil {
		t.Fatal(err)
	}
	built, err := buildStates(validated, spec.GenesisTime)
	if err != nil {
		t.Fatal(err)
	}
	tracked, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	result, err := tracked.Apply(groups.ApplyInput{
		Block: built.master.block,
		Root:  built.master.root,
		AsOf:  time.Unix(int64(spec.GenesisTime), 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	masterState, err := groups.ParseState(groups.StateInput{Block: built.master.block, Root: built.master.root})
	if err != nil {
		t.Fatal(err)
	}
	executionConfig, err := tvm.PrepareBlockchainConfig(masterState.ConfigRoot)
	if err != nil {
		t.Fatal(err)
	}
	preparedConfig, err := collator.PrepareConfig(executionConfig)
	if err != nil {
		t.Fatal(err)
	}

	var previousState tlb.ShardStateUnsplit
	loader := built.master.root.MustBeginParse()
	if err = tlb.LoadFromCell(&previousState, loader); err != nil {
		t.Fatal(err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		t.Fatal("generated masterchain state has trailing data")
	}
	queueSize := uint64(0)
	randSeed := [32]byte{1}
	createdBy := [32]byte{2}
	candidate, err := collator.NewBuilder(tvm.NewTVM(), collator.SupportedSoftware()).BuildMaster(
		context.Background(),
		collator.MasterRequest{
			Previous: collator.PreviousBlock{
				ID:           built.master.block,
				State:        built.master.root,
				OutQueueSize: &queueSize,
			},
			Config: preparedConfig,
			Groups: result.Snapshot,
			Header: collator.HeaderParams{
				GenUtime:   spec.GenesisTime + 1,
				GenUtimeMS: uint64(spec.GenesisTime+1) * 1000,
			},
			RandSeed:            randSeed,
			CreatedBy:           createdBy,
			MaxExternalAttempts: 64,
			Dispatch:            collator.ReferenceDispatchPolicy(),
			Internals:           &msgpool.Cut{},
			Neighbors: []collator.Neighbor{{
				Block: built.master.block,
				Shard: msgpool.ShardIdent{Workchain: -1, Shard: uint64(1) << 63},
				EndLT: previousState.GenLT,
			}},
			NeighborShardEndLT: func(uint32, int32, uint64) uint64 { return 0 },
		},
	)
	if err != nil {
		t.Fatalf("build first masterchain block: %v", err)
	}
	if candidate.ID.SeqNo != 1 || candidate.ID.Workchain != -1 || candidate.ID.Shard != math.MinInt64 {
		t.Fatalf("first masterchain candidate ID = %#v", candidate.ID)
	}
	next, err := groups.ParseState(groups.StateInput{Block: candidate.ID, Root: candidate.State})
	if err != nil {
		t.Fatalf("parse first masterchain candidate state: %v", err)
	}
	if candidateConfigRoot := generatedConfigAccountRoot(t, candidate.State); next.ConfigRoot.HashKey() != candidateConfigRoot {
		t.Fatalf("first candidate config account %x differs from state config %x",
			candidateConfigRoot, next.ConfigRoot.HashKey())
	}
	projected, err := tracked.Project(result.Snapshot, groups.ApplyInput{
		Block: candidate.ID,
		Root:  candidate.State,
		AsOf:  time.Unix(int64(spec.GenesisTime+1), 0),
	})
	if err != nil {
		t.Fatalf("project first masterchain candidate state: %v", err)
	}
	assertActiveSessionAnchors(t, result.Snapshot, projected)

	// A node does not keep the zerostate BOC as its working state: it reloads a
	// lazy tree from CellDB. Verify that applying the candidate update preserves
	// enough of that tree for the following masterchain configuration snapshot.
	dataDir := filepath.Join(t.TempDir(), "data")
	if err = createDataAtomically(context.Background(), dataDir, built); err != nil {
		t.Fatalf("persist generated zerostate: %v", err)
	}
	store, err := openGenesisStore(dataDir, true, zerolog.Nop())
	if err != nil {
		t.Fatal(err)
	}
	defer store.Close()
	lazyRoot, err := store.LoadStateCellTree(context.Background(), built.master.block, built.master.block.RootHash)
	if err != nil {
		t.Fatalf("load persisted generated masterchain state: %v", err)
	}
	lazyTracked, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	lazySnapshot, err := lazyTracked.Apply(groups.ApplyInput{
		Block: built.master.block,
		Root:  lazyRoot,
		AsOf:  time.Unix(int64(spec.GenesisTime), 0),
	})
	if err != nil {
		t.Fatalf("track persisted generated zerostate: %v", err)
	}
	lazyState, err := groups.ParseState(groups.StateInput{Block: built.master.block, Root: lazyRoot})
	if err != nil {
		t.Fatalf("parse persisted generated zerostate: %v", err)
	}
	if lazyState.ConfigRoot.HashKey() != masterState.ConfigRoot.HashKey() {
		t.Fatalf("persisted zerostate config %x differs from BOC config %x",
			lazyState.ConfigRoot.HashKey(), masterState.ConfigRoot.HashKey())
	}
	lazyExecutionConfig, err := tvm.PrepareBlockchainConfig(lazyState.ConfigRoot)
	if err != nil {
		t.Fatalf("prepare persisted generated execution config: %v", err)
	}
	lazyConfig, err := collator.PrepareConfig(lazyExecutionConfig)
	if err != nil {
		t.Fatalf("prepare persisted generated collator config: %v", err)
	}
	lazyCandidate, err := collator.NewBuilder(tvm.NewTVM(), collator.SupportedSoftware()).BuildMaster(
		context.Background(),
		collator.MasterRequest{
			Previous: collator.PreviousBlock{
				ID:           built.master.block,
				State:        lazyRoot,
				OutQueueSize: &queueSize,
			},
			Config: lazyConfig,
			Groups: lazySnapshot.Snapshot,
			Header: collator.HeaderParams{
				GenUtime:   spec.GenesisTime + 1,
				GenUtimeMS: uint64(spec.GenesisTime+1) * 1000,
			},
			RandSeed:            randSeed,
			CreatedBy:           createdBy,
			MaxExternalAttempts: 64,
			Dispatch:            collator.ReferenceDispatchPolicy(),
			Internals:           &msgpool.Cut{},
			Neighbors: []collator.Neighbor{{
				Block: built.master.block,
				Shard: msgpool.ShardIdent{Workchain: -1, Shard: uint64(1) << 63},
				EndLT: previousState.GenLT,
			}},
			NeighborShardEndLT: func(uint32, int32, uint64) uint64 { return 0 },
		},
	)
	if err != nil {
		t.Fatalf("build first masterchain block from persisted zerostate: %v", err)
	}
	lazyCandidateState, err := groups.ParseState(groups.StateInput{Block: lazyCandidate.ID, Root: lazyCandidate.State})
	if err != nil {
		t.Fatalf("parse lazy first masterchain candidate state: %v", err)
	}
	if candidateConfigRoot := generatedConfigAccountRoot(t, lazyCandidate.State); lazyCandidateState.ConfigRoot.HashKey() != candidateConfigRoot {
		t.Fatalf("lazy first candidate config account %x differs from state config %x",
			candidateConfigRoot, lazyCandidateState.ConfigRoot.HashKey())
	}
	applied, err := cell.ApplyMerkleUpdate(lazyRoot.WithoutTrace(), lazyCandidate.StateUpdate)
	if err != nil {
		t.Fatalf("apply first masterchain update to persisted zerostate: %v", err)
	}
	if applied.HashKey() != lazyCandidate.State.HashKey() {
		t.Fatal("persisted zerostate update has a different candidate state root")
	}
	appliedState, err := groups.ParseState(groups.StateInput{Block: lazyCandidate.ID, Root: applied})
	if err != nil {
		t.Fatalf("parse persisted first masterchain candidate state: %v", err)
	}
	accountConfigRoot := generatedConfigAccountRoot(t, applied)
	if appliedState.ConfigRoot.HashKey() != accountConfigRoot {
		t.Fatal("persisted candidate config account differs from the state config")
	}
	lazyProjected, err := lazyTracked.Project(lazySnapshot.Snapshot, groups.ApplyInput{
		Block: lazyCandidate.ID,
		Root:  applied,
		AsOf:  time.Unix(int64(spec.GenesisTime+1), 0),
	})
	if err != nil {
		t.Fatalf("project persisted first masterchain candidate state: %v", err)
	}
	assertActiveSessionAnchors(t, lazySnapshot.Snapshot, lazyProjected)
	targets, err := next.CurrentTargets()
	if err != nil {
		t.Fatal(err)
	}
	if len(targets) != 2 || targets[1].Shard.Workchain != 0 || targets[1].Genesis[0].SeqNo != 0 {
		t.Fatalf("first masterchain block did not activate basechain zerostate: %#v", targets)
	}
}

func assertActiveSessionAnchors(t *testing.T, previous, next *groups.Snapshot) {
	t.Helper()

	for i := range previous.Active {
		before := &previous.Active[i]
		var after *groups.Session
		for j := range next.Active {
			if next.Active[j].ID == before.ID {
				after = &next.Active[j]
				break
			}
		}
		if after == nil {
			t.Fatalf("active session %x disappeared after first speculative block", before.ID)
		}
		if after.Shard != before.Shard || after.CatchainSeqno != before.CatchainSeqno ||
			!sameGeneratedBlockIDs(after.Genesis, before.Genesis) || !after.MinMasterchain.Equals(&before.MinMasterchain) ||
			!sameGeneratedValidators(after.Validators, before.Validators) {
			t.Fatalf("active session %x anchors changed after first speculative block", before.ID)
		}
	}
}

func sameGeneratedBlockIDs(left, right []ton.BlockIDExt) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if !left[i].Equals(&right[i]) {
			return false
		}
	}

	return true
}

func sameGeneratedValidators(left, right []groups.Validator) bool {
	if len(left) != len(right) {
		return false
	}
	for i := range left {
		if left[i] != right[i] {
			return false
		}
	}

	return true
}

func generatedConfigAccountRoot(t *testing.T, root *cell.Cell) cell.Hash {
	t.Helper()

	var state tlb.ShardStateUnsplit
	loader := root.MustBeginParse()
	if err := tlb.LoadFromCell(&state, loader); err != nil {
		t.Fatal(err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 || state.McStateExtra == nil {
		t.Fatal("generated masterchain state is malformed")
	}
	var extra tlb.McStateExtra
	if err := tlb.LoadFromCell(&extra, state.McStateExtra.MustBeginParse()); err != nil {
		t.Fatal(err)
	}
	key := cell.BeginCell().MustStoreSlice(extra.ConfigParams.ConfigAddr, 256).EndCell()
	value, err := state.Accounts.ShardAccounts.LoadValue(key)
	if err != nil {
		t.Fatal(err)
	}
	var shardAccount tlb.ShardAccount
	if err = tlb.LoadFromCell(&shardAccount, value); err != nil {
		t.Fatal(err)
	}
	prepared, err := tvm.PrepareAccount(&shardAccount, tonaddress.NewAddress(0, 0xff, extra.ConfigParams.ConfigAddr))
	if err != nil {
		t.Fatal(err)
	}
	data, err := prepared.State().StateInit.Data.BeginParse()
	if err != nil {
		t.Fatal(err)
	}
	config, err := data.LoadRefCell()
	if err != nil || data.BitsLeft() != 0 || data.RefsNum() != 0 {
		t.Fatalf("decode generated config account: %v", err)
	}
	return config.HashKey()
}

func TestGenerateRejectsChangedOrOverlappingOutputs(t *testing.T) {
	t.Parallel()

	dir := t.TempDir()
	paths := Paths{
		Genesis:      filepath.Join(dir, "genesis.json"),
		Data:         filepath.Join(dir, "data"),
		GlobalConfig: filepath.Join(dir, "global.config.json"),
		Lock:         filepath.Join(dir, "genesis.lock.json"),
	}
	spec := validTestSpec(t)
	writeSpec(t, paths.Genesis, spec)
	if _, err := Generate(context.Background(), paths, zerolog.Nop()); err != nil {
		t.Fatal(err)
	}

	spec.Validators[0].Weight++
	writeSpec(t, paths.Genesis, spec)
	if _, err := Generate(context.Background(), paths, zerolog.Nop()); err == nil || !strings.Contains(err.Error(), "different genesis.json") {
		t.Fatalf("changed genesis error = %v", err)
	}

	overlap := paths
	overlap.Data = dir
	if _, err := Generate(context.Background(), overlap, zerolog.Nop()); err == nil || !strings.Contains(err.Error(), "must not contain") {
		t.Fatalf("overlapping path error = %v", err)
	}
}

func TestTemplateIsExclusiveAndIncomplete(t *testing.T) {
	t.Parallel()

	path := filepath.Join(t.TempDir(), "genesis.json")
	if err := WriteTemplate(path); err != nil {
		t.Fatal(err)
	}
	if err := WriteTemplate(path); !errors.Is(err, os.ErrExist) {
		t.Fatalf("second template write error = %v", err)
	}
	spec, _, err := LoadSpec(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(spec.Validators) != 3 {
		t.Fatalf("template validators = %d, want 3", len(spec.Validators))
	}
	if _, err = validateSpec(spec); !errors.Is(err, ErrTemplateIncomplete) {
		t.Fatalf("template validation error = %v", err)
	}
}

func validTestSpec(t *testing.T) Spec {
	t.Helper()

	spec := DefaultSpec()
	spec.GenesisTime = 1_700_000_000
	for i := range spec.Validators {
		seed := bytes.Repeat([]byte{byte(i + 1)}, ed25519.SeedSize)
		privateKey := ed25519.NewKeyFromSeed(seed)
		publicKey := privateKey.Public().(ed25519.PublicKey)
		adnlSeed := bytes.Repeat([]byte{byte(i + 21)}, ed25519.SeedSize)
		adnlKey := ed25519.NewKeyFromSeed(adnlSeed)
		adnlPublic := adnlKey.Public().(ed25519.PublicKey)
		adnlID, err := tl.Hash(keys.PublicKeyED25519{Key: adnlPublic})
		if err != nil {
			t.Fatal(err)
		}
		spec.Validators[i].PublicKey = base64.StdEncoding.EncodeToString(publicKey)
		spec.Validators[i].ADNLID = base64.StdEncoding.EncodeToString(adnlID)
	}
	spec.DHTNodes = []liteclient.DHTNode{signedTestDHTNode(t)}
	return spec
}

func signedTestDHTNode(t *testing.T) liteclient.DHTNode {
	t.Helper()

	privateKey := ed25519.NewKeyFromSeed(bytes.Repeat([]byte{91}, ed25519.SeedSize))
	publicKey := privateKey.Public().(ed25519.PublicKey)
	ip := net.IPv4(127, 0, 0, 1).To4()
	list := &adnladdress.List{
		Addresses: []adnladdress.Address{&adnladdress.UDP{IP: ip, Port: 30304}},
		Version:   1,
	}
	node, err := dht.BuildSignedNode(keys.PublicKeyED25519{Key: publicKey}, list, 1, -1, privateKey)
	if err != nil {
		t.Fatal(err)
	}
	return liteclient.DHTNode{
		Type: "dht.node",
		ID:   liteclient.ServerID{Type: "pub.ed25519", Key: base64.StdEncoding.EncodeToString(publicKey)},
		AddrList: liteclient.DHTAddressList{
			Type: "adnl.addressList",
			Addrs: []liteclient.DHTAddress{{
				Type: "adnl.address.udp",
				IP:   int(binary.BigEndian.Uint32(ip)),
				Port: 30304,
			}},
			Version: 1,
		},
		Version:   1,
		Signature: base64.StdEncoding.EncodeToString(node.Signature),
	}
}

func writeSpec(t *testing.T, path string, spec Spec) {
	t.Helper()

	raw, err := json.MarshalIndent(spec, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(path, append(raw, '\n'), 0o644); err != nil {
		t.Fatal(err)
	}
}
