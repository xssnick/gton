package collator

import (
	"bytes"
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestVerifyMasterShardTransitionAcceptsPreV13EqualShardTime(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyCandidate(context.Background(), fixture.request.Config, candidate)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := verifyPredecessor("master", &fixture.request.Previous)
	if err != nil {
		t.Fatal(err)
	}

	config := *fixture.request.Config
	config.globalVersion = 12
	fixture.request.Config = &config
	if _, _, err := prepareMasterShardTops(
		fixture.request,
		fixture.request.Previous.ID.SeqNo+1,
	); err == nil {
		t.Fatal("pre-v13 collator accepted a shard top generated at the masterchain block time")
	}
	state, err := loadMasterCandidateState(&config, &previous, &verified)
	if err != nil {
		t.Fatal(err)
	}
	request := MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    &config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Candidate: candidate,
	}
	if _, _, err = verifyMasterShardTransition(request, &previous, &verified, &state); err != nil {
		t.Fatalf("validator rejected a shard top generated at the masterchain block time: %v", err)
	}
}

func TestBuildMasterCanonicalizesImportedDescriptor(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	fields, err := parseShardDescriptorFields(fixture.request.ShardTops[0].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	fields.nextCCUpdated = true
	fixture.request.ShardTops[0].Descriptor, err = storeMasterShardDescriptor(0xb, fields)
	if err != nil {
		t.Fatal(err)
	}

	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
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
	registry, err := ParseShardRegistry(extra.ShardHashes)
	if err != nil {
		t.Fatal(err)
	}
	tops := registry.Tops()
	if len(tops) != 1 {
		t.Fatalf("resulting shard count = %d, want 1", len(tops))
	}
	tag, err := loadMasterShardDescriptorTag(tops[0].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := parseShardDescriptorFields(tops[0].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if tag != 0xa || canonical.nextCCUpdated {
		t.Fatalf("imported descriptor serialized as tag %x nx_cc_updated=%t, want a/false",
			tag, canonical.nextCCUpdated)
	}
}

func TestBuildAndVerifyMasterClearsTerminalBeforeSplitFSM(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	split := tlb.FutureSplit{
		SplitUtime: fixture.request.Header.GenUtime - 1,
		Interval:   20,
	}

	oldRegistry, err := ParseShardRegistry(fixture.oldExtra.ShardHashes)
	if err != nil {
		t.Fatal(err)
	}
	oldFields, err := parseShardDescriptorFields(oldRegistry.Tops()[0].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	oldFields.splitMerge = split
	oldDescriptor, err := storeMasterShardDescriptor(0xa, oldFields)
	if err != nil {
		t.Fatal(err)
	}
	fixture.oldExtra.ShardHashes = masterBuildShardHashes(t, oldDescriptor)
	extraRoot, err := tlb.ToCell(&fixture.oldExtra)
	if err != nil {
		t.Fatal(err)
	}
	fixture.oldState.McStateExtra = extraRoot
	stateRoot, err := tlb.ToCell(&fixture.oldState)
	if err != nil {
		t.Fatal(err)
	}
	stateHash := stateRoot.HashKey()
	fixture.request.Previous.State = stateRoot
	fixture.request.Previous.ID.RootHash = bytes.Clone(stateHash[:])
	snapshot := *fixture.request.Groups
	snapshot.MasterchainBlock = *fixture.request.Previous.ID.Copy()
	fixture.request.Groups = &snapshot

	shardRoot := masterBuildShardBlockRootWithBeforeSplit(
		t,
		tlb.ShardIdent{WorkchainID: 0},
		0,
		fixture.newShard.SeqNo,
		fixture.request.Header.GenUtime,
		0,
		true,
		nil,
	)
	shardHash := shardRoot.HashKey()
	fixture.newShard.RootHash = bytes.Clone(shardHash[:])
	newDescriptor := masterBuildShardDescriptor(
		t,
		fixture.newShard,
		0,
		fixture.request.Header.GenUtime,
	)
	newFields, err := parseShardDescriptorFields(newDescriptor)
	if err != nil {
		t.Fatal(err)
	}
	newFields.beforeSplit = true
	newDescriptor, err = storeMasterShardDescriptor(0xa, newFields)
	if err != nil {
		t.Fatal(err)
	}
	fixture.request.ShardTops[0].Block = fixture.newShard
	fixture.request.ShardTops[0].Descriptor = newDescriptor
	fixture.request.ShardTops[0].TopBlockDescr = masterBuildProvenTopBlockDescr(
		t,
		fixture.newShard,
		shardRoot,
	)

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
	result, err := ParseShardRegistry(stateExtra.ShardHashes)
	if err != nil {
		t.Fatal(err)
	}
	resultFields := result.leaves[shardRegistryKey{workchain: 0, shard: fixture.newShard.Shard}].fields
	if !resultFields.beforeSplit || !masterShardFSMNone(resultFields.splitMerge) {
		t.Fatalf("terminal descriptor: before_split=%t fsm=%T, want true/none",
			resultFields.beforeSplit, resultFields.splitMerge)
	}

	err = VerifyMasterCandidate(context.Background(), MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    fixture.request.Config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Semantics: testCandidateTransitionVerifier,
		Candidate: candidate,
	})
	if err != nil {
		t.Fatalf("verify terminal before-split candidate: %v", err)
	}
}

func TestVerifyMasterCollatedShardTopsAllowsUnusedEntries(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	consumed := fixture.request.ShardTops[0]
	extra := consumed
	extra.Block.Workchain = 1
	extra.TopBlockDescr = masterBuildTopBlockDescr(t, extra.Block)

	set, err := buildMasterTopBlockDescrSet([]ShardTop{consumed, extra})
	if err != nil {
		t.Fatal(err)
	}
	if err = verifyMasterCollatedShardTops([]*cell.Cell{set}, []ShardTop{consumed}); err != nil {
		t.Fatalf("unused valid TopBlockDescrSet entry was rejected: %v", err)
	}
}

func TestVerifyMasterCollatedShardTopsBindsConsumedEntry(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	consumed := fixture.request.ShardTops[0]
	other := consumed
	other.Block.Workchain = 1
	other.TopBlockDescr = masterBuildTopBlockDescr(t, other.Block)

	set, err := buildMasterTopBlockDescrSet([]ShardTop{other})
	if err != nil {
		t.Fatal(err)
	}
	if err = verifyMasterCollatedShardTops([]*cell.Cell{set}, []ShardTop{consumed}); err == nil {
		t.Fatal("TopBlockDescrSet without the consumed shard key was accepted")
	}

	wrong := consumed
	wrong.TopBlockDescr = other.TopBlockDescr
	set, err = buildMasterTopBlockDescrSet([]ShardTop{wrong})
	if err != nil {
		t.Fatal(err)
	}
	if err = verifyMasterCollatedShardTops([]*cell.Cell{set}, []ShardTop{consumed}); err == nil {
		t.Fatal("TopBlockDescrSet with a different consumed value was accepted")
	}
}
