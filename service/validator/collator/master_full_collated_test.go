package collator

import (
	"bytes"
	"context"
	"errors"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

type fullMasterFixture struct {
	masterBuildFixture
	shardState *cell.Cell
	provider   *staticFullCollatedProofProvider
}

func TestBuildAndVerifyMasterFullCollatedNeighbors(t *testing.T) {
	fixture := newFullMasterFixture(t)

	withoutProvider := fixture.request
	withoutProvider.FullCollatedProofs = nil
	if _, err := testBuilder().BuildMaster(context.Background(), withoutProvider); !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "block proof is absent") {
		t.Fatalf("missing master neighbor proofs error = %v", err)
	}

	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.provider.called != 1 {
		t.Fatalf("provider calls = %d, want 1", fixture.provider.called)
	}
	verification := MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    fixture.request.Config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Neighbors: fixture.request.Neighbors,
		Semantics: testCandidateTransitionVerifier,
		Candidate: candidate,
	}
	if err = VerifyMasterCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify full masterchain candidate: %v", err)
	}

	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	filtered := make([]*cell.Cell, 0, len(roots)-1)
	for _, root := range roots {
		if root.IsSpecial() {
			virtual, proofErr := unwrapCollatedProof(root)
			if proofErr != nil {
				t.Fatal(proofErr)
			}
			if virtual.HashKey() == fixture.shardState.HashKey() {
				continue
			}
		}
		filtered = append(filtered, root)
	}
	tampered := cloneVerificationCandidate(candidate)
	rewriteVerificationCollatedData(t, tampered, filtered...)
	verification.Candidate = tampered
	if err = VerifyMasterCandidate(context.Background(), verification); !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "state proof is absent") {
		t.Fatalf("missing shard state proof error = %v", err)
	}
}

func TestBuildMasterRejectsFullProofProviderWithoutCapability(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	fixture.request.FullCollatedProofs = &staticFullCollatedProofProvider{}

	if _, err := testBuilder().BuildMaster(context.Background(), fixture.request); !errors.Is(err, ErrInvalidInput) ||
		!strings.Contains(err.Error(), "without capability") {
		t.Fatalf("provider without capability error = %v", err)
	}
}

func TestPrepareMasterAccountsNeighborProofsBeforeAdmission(t *testing.T) {
	fixture := newFullMasterFixture(t)
	set, err := buildMasterTopBlockDescrSet(fixture.request.ShardTops)
	if err != nil {
		t.Fatal(err)
	}
	compact, err := cell.ToBOCWithOptionsErr(
		[]*cell.Cell{set, consensusExtraRoot(fixture.request.Header.GenUtimeMS)},
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	boundary := uint64(len(compact) + 1)
	fixture.request.Config.masterchain.limits.collatedData = limitThresholds{
		boundary,
		boundary,
		boundary,
		boundary,
	}

	collation, err := testBuilder().prepareMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.provider.called != 1 {
		t.Fatalf("provider calls during prepare = %d, want 1", fixture.provider.called)
	}
	if collation.collatedFixedEstimate <= boundary {
		t.Fatalf("fixed collated estimate = %d, want above compact boundary %d",
			collation.collatedFixedEstimate, boundary)
	}
	if collation.master.topBlockDescrSet == nil {
		t.Fatal("master TopBlockDescr set was not cached during prepare")
	}
	fixedRoots, err := collation.masterCollatedPrefix(consensusExtraRoot(fixture.request.Header.GenUtimeMS))
	if err != nil {
		t.Fatal(err)
	}
	if fixedRoots[0] != collation.master.topBlockDescrSet {
		t.Fatal("master collated prefix did not reuse the prepared TopBlockDescr set")
	}
	fixedRoots = append(fixedRoots, collation.fullCollatedProofs...)
	finalRoots, err := collation.buildCollatedRoots(
		consensusExtraRoot(fixture.request.Header.GenUtimeMS),
		nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if finalRoots[0] != collation.master.topBlockDescrSet {
		t.Fatal("final master collated roots rebuilt the prepared TopBlockDescr set")
	}
	fixedBOC, err := cell.ToBOCWithOptionsErr(fixedRoots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	if collation.collatedFixedEstimate != uint64(len(fixedBOC)) {
		t.Fatalf("fixed collated estimate = %d, serialized size = %d",
			collation.collatedFixedEstimate, len(fixedBOC))
	}
	if collation.limits.fits(LoadNormal) {
		t.Fatal("neighbor proofs were not reflected in pre-admission collated-data limits")
	}
}

func newFullMasterFixture(t *testing.T) fullMasterFixture {
	t.Helper()

	fixture := newMasterBuildFixture(t, false)
	fixture.request.Config.capabilities |= capFullCollatedData

	base := emptyCandidateRequest(t)
	var shardState tlb.ShardStateUnsplit
	if err := parseExact(&shardState, base.Previous.State); err != nil {
		t.Fatal(err)
	}
	shardState.ShardIdent = tlb.ShardIdent{WorkchainID: 0}
	shardState.Seqno = fixture.oldShard.SeqNo + 1
	shardState.GenUTime = fixture.request.Header.GenUtime
	shardState.GenLT = uint64(shardState.Seqno)*1_000 + 999
	shardState.McStateExtra = nil
	stateRoot, err := tlb.ToCell(&shardState)
	if err != nil {
		t.Fatal(err)
	}
	stateUpdate, err := cell.CreateMerkleUpdate(cell.BeginCell().MustStoreUInt(0, 1).EndCell(), stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	blockRoot := masterBuildShardBlockRoot(
		t,
		shardState.ShardIdent,
		shardState.GlobalID,
		shardState.Seqno,
		fixture.request.Header.GenUtime,
		0,
		stateUpdate,
	)
	rootHash := blockRoot.HashKey()
	newShard := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(shardState.ShardIdent.GetShardID()),
		SeqNo:     shardState.Seqno,
		RootHash:  bytes.Clone(rootHash[:]),
		FileHash:  bytes.Repeat([]byte{0x53}, 32),
	}
	fixture.newShard = newShard
	fixture.request.ShardTops[0].Block = newShard
	fixture.request.ShardTops[0].Descriptor = masterBuildShardDescriptor(
		t,
		newShard,
		0,
		fixture.request.Header.GenUtime,
	)
	fixture.request.ShardTops[0].TopBlockDescr = masterBuildProvenTopBlockDescr(t, newShard, blockRoot)
	fixture.topProof = fixture.request.ShardTops[0].TopBlockDescr

	shardProcessed := loadFullMasterProcessed(t, shardState.OutMsgQueueInfo, uint64(newShard.Shard))
	masterProcessed := loadFullMasterProcessed(
		t,
		fixture.oldState.OutMsgQueueInfo,
		uint64(fixture.request.Previous.ID.Shard),
	)
	fixture.request.Neighbors = []Neighbor{
		{
			Block:     newShard,
			Shard:     msgpool.ShardIdent{Workchain: 0, Shard: uint64(newShard.Shard)},
			EndLT:     shardState.GenLT,
			Processed: shardProcessed,
		},
		{
			Block: fixture.request.Previous.ID,
			Shard: msgpool.ShardIdent{
				Workchain: address.MasterchainID,
				Shard:     uint64(fixture.request.Previous.ID.Shard),
			},
			EndLT:     fixture.oldState.GenLT,
			Processed: masterProcessed,
		},
	}
	fixture.request.NeighborShardEndLT = func(uint32, int32, uint64) uint64 {
		return shardState.GenLT
	}
	blockProof, err := cell.CreateMerkleProof(blockRoot)
	if err != nil {
		t.Fatal(err)
	}
	stateProof, err := cell.CreateMerkleProof(stateRoot)
	if err != nil {
		t.Fatal(err)
	}
	provider := &staticFullCollatedProofProvider{roots: []*cell.Cell{stateProof, blockProof}}
	fixture.request.FullCollatedProofs = provider

	return fullMasterFixture{
		masterBuildFixture: fixture,
		shardState:         stateRoot,
		provider:           provider,
	}
}

func loadFullMasterProcessed(t *testing.T, root *cell.Cell, owner uint64) []tlb.ProcessedUptoRecord {
	t.Helper()

	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, root); err != nil {
		t.Fatal(err)
	}
	records, err := tlb.LoadProcessedUptoRecords(queue.ProcInfo, owner)
	if err != nil {
		t.Fatal(err)
	}

	return records
}
