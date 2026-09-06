package collator

import (
	"bytes"
	"context"
	"errors"
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

type fullMasterFixture struct {
	masterBuildFixture
	shardState *cell.Cell
	// provider is the static stub; nil when the fixture was built with
	// tracedProvider, whose provider is the production one.
	provider *staticFullCollatedProofProvider
	// queued is the shard-to-master message enqueued by queueToMaster, nil
	// otherwise.
	queued *msgpool.InternalMessage
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

// masterShardIdent is the masterchain as a collated shard: the key under which
// a masterchain build keeps its neighbour-proof measurement on the Builder.
var masterShardIdent = msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll}

// The neighbour proofs are built after the import phase (finishMaster), so
// what prepareMaster can charge against the collated budget before admission is
// the last masterchain build's measured neighbour-proof size, on top of the
// fixed prefix — the TopBlockDescr set and the consensus root. A first build on
// a fresh Builder has no such measurement and charges the prefix alone; the
// build then records what its proofs cost, and the next prepare charges that
// (plus the quarter of headroom neighborProofHint adds) without calling the
// provider at all.
//
// "The last masterchain build's", not the last build's: one Builder collates
// the masterchain and the node's shard turn about (3,416 chain switches a day
// on the stand), and the two ship neighbour proofs two orders of magnitude
// apart — 3 kB against a shard p90 of 339 kB and a maximum of 2.7 MB. A shard
// build is therefore made on the same Builder before the first masterchain
// prepare, and a shard prepare after the masterchain build: neither chain may
// see the other's measurement. Charged the shard's, the masterchain block would
// start at or over the collated limit and refuse its first message; charged the
// masterchain's, the shard would be under-charged by the whole backlog its
// hint exists for.
func TestPrepareMasterChargesTheLastNeighborProofSizeBeforeAdmission(t *testing.T) {
	fixture := newFullMasterFixture(t)
	set, err := buildMasterTopBlockDescrSet(fixture.request.ShardTops)
	if err != nil {
		t.Fatal(err)
	}
	prefix, err := cell.ToBOCWithOptionsErr(
		[]*cell.Cell{set, consensusExtraRoot(fixture.request.Header.GenUtimeMS)},
		cell.BOCSerializeOptions{WithCRC32C: true},
	)
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := cell.ToBOCWithOptionsErr(fixture.provider.roots, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	// Between the prefix and the prefix plus the hint: a prepare that charges
	// the hint is over the limit, one that does not is under it.
	boundary := uint64(len(prefix) + 1)
	fixture.request.Config.masterchain.limits.collatedData = limitThresholds{
		boundary,
		boundary,
		boundary,
		boundary,
	}

	builder := testBuilder()

	// A shard build first, shipping neighbour proofs many times the size of the
	// masterchain's, the way a backed-up shard's do.
	shardRequest, shardProofBytes := fullCollatedShardRequestShippingProofs(t, 64)
	shardIdent := msgpool.ShardIdent{Workchain: shardRequest.Shard.Workchain, Shard: uint64(shardRequest.Shard.Shard)}
	if _, err = builder.BuildShard(context.Background(), shardRequest); err != nil {
		t.Fatal(err)
	}
	shardHint := builder.neighborProofHint(shardIdent)
	if want := shardProofBytes + shardProofBytes/4; shardHint != want {
		t.Fatalf("shard neighbour proof hint after the shard build = %d, want %d (%d bytes measured plus a quarter)",
			shardHint, want, shardProofBytes)
	}
	if shardHint <= len(proofs)+len(proofs)/4 {
		t.Fatalf("shard hint %d is not larger than the masterchain's %d; the test cannot tell the two apart",
			shardHint, len(proofs)+len(proofs)/4)
	}
	if got := builder.neighborProofHint(masterShardIdent); got != 0 {
		t.Fatalf("masterchain neighbour proof hint after a shard build = %d, want 0", got)
	}

	first, err := builder.prepareMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.provider.called != 0 {
		t.Fatalf("provider calls during prepare = %d, want 0: the proofs are built after the import phase", fixture.provider.called)
	}
	if first.fullCollatedProofs != nil {
		t.Fatal("prepareMaster built neighbour proofs before the import phase")
	}
	if first.master.topBlockDescrSet == nil {
		t.Fatal("master TopBlockDescr set was not cached during prepare")
	}
	if first.collatedCheapEstimate != uint64(len(prefix)) || first.collatedFixedEstimate != uint64(len(prefix)) {
		t.Fatalf("first prepare charges cheap=%d fixed=%d, want the %d-byte prefix alone (the shard's %d-byte hint is not the masterchain's)",
			first.collatedCheapEstimate, first.collatedFixedEstimate, len(prefix), shardHint)
	}

	// The build measures its proofs; the provider is asked exactly once, from
	// finishMaster.
	fixture.request.Config.masterchain.limits.collatedData = loadMainnetConfig(t).masterchain.limits.collatedData
	if _, err = builder.BuildMaster(context.Background(), fixture.request); err != nil {
		t.Fatal(err)
	}
	if fixture.provider.called != 1 {
		t.Fatalf("provider calls during build = %d, want 1", fixture.provider.called)
	}
	if got, want := builder.neighborProofHint(masterShardIdent), len(proofs)+len(proofs)/4; got != want {
		t.Fatalf("neighbour proof hint after the build = %d, want %d (%d bytes measured plus a quarter)", got, want, len(proofs))
	}
	if got := builder.neighborProofHint(shardIdent); got != shardHint {
		t.Fatalf("shard neighbour proof hint after a masterchain build = %d, want the shard's own %d", got, shardHint)
	}

	fixture.request.Config.masterchain.limits.collatedData = limitThresholds{
		boundary,
		boundary,
		boundary,
		boundary,
	}
	second, err := builder.prepareMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if fixture.provider.called != 1 {
		t.Fatalf("provider calls after the second prepare = %d, want still 1", fixture.provider.called)
	}
	if want := uint64(len(prefix) + builder.neighborProofHint(masterShardIdent)); second.collatedFixedEstimate != want {
		t.Fatalf("second prepare charges %d, want prefix %d plus hint %d = %d",
			second.collatedFixedEstimate, len(prefix), builder.neighborProofHint(masterShardIdent), want)
	}
	if second.limits.fits(LoadNormal) {
		t.Fatal("the last build's neighbour proof size was not reflected in pre-admission collated-data limits")
	}
	fixedRoots, err := second.masterCollatedPrefix(consensusExtraRoot(fixture.request.Header.GenUtimeMS))
	if err != nil {
		t.Fatal(err)
	}
	if fixedRoots[0] != second.master.topBlockDescrSet {
		t.Fatal("master collated prefix did not reuse the prepared TopBlockDescr set")
	}

	// And the shard, prepared after the masterchain build, still charges what
	// its own last build measured — not the masterchain's few kB.
	shardPrepared, err := builder.prepare(context.Background(), shardRequest)
	if err != nil {
		t.Fatal(err)
	}
	if want := shardPrepared.collatedCheapEstimate + uint64(shardHint); shardPrepared.collatedFixedEstimate != want {
		t.Fatalf("shard prepare after a masterchain build charges %d, want its cheap estimate %d plus its own hint %d = %d (masterchain hint %d)",
			shardPrepared.collatedFixedEstimate, shardPrepared.collatedCheapEstimate, shardHint, want,
			builder.neighborProofHint(masterShardIdent))
	}
}

// fullCollatedShardRequestShippingProofs is a shard build whose neighbour proof
// provider ships a proof of the given number of full cells — the shape of a
// backed-up queue prefix — and reports the bytes the build will measure for it.
// The proof stands beside the shard's own predecessor proofs in the collated
// data; nothing in the shard fixture reads it, which is the point: only its size
// matters here.
func fullCollatedShardRequestShippingProofs(t *testing.T, cells int) (ShardRequest, int) {
	t.Helper()

	// Advanced once first: full collated data needs the predecessor's block
	// root, which the empty request does not carry.
	req := advanceCandidateRequest(t, emptyCandidateRequest(t))
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	payload := bytes.Repeat([]byte{0xa5}, 127)
	tree := cell.BeginCell().MustStoreSlice(payload, 1016).EndCell()
	for range cells - 1 {
		tree = cell.BeginCell().MustStoreSlice(payload, 1016).MustStoreRef(tree).EndCell()
	}
	proof, err := cell.CreateMerkleProof(tree)
	if err != nil {
		t.Fatal(err)
	}
	req.FullCollatedProofs = &staticFullCollatedProofProvider{roots: []*cell.Cell{proof}}
	size, err := collatedBOCSize([]*cell.Cell{proof})
	if err != nil {
		t.Fatal(err)
	}

	return req, int(size)
}

// fullMasterFixtureOptions vary the two parts of the full-collated master
// fixture that the default one keeps trivial: the shard top's outbound queue
// is empty, and the proof provider is a stub that hands back whole-state
// proofs. Both defaults hide the production read this fixture exists to test —
// the validator's lookup of an imported message inside the neighbour proof.
type fullMasterFixtureOptions struct {
	// queueToMaster enqueues one message bound for the masterchain in the
	// shard top's outbound queue and offers it in the request's inbound cut,
	// so the candidate imports it as msg_import_fin.
	queueToMaster bool
	// tracedProvider replaces the static provider with the production one over
	// a traced view of the shard top state, so the collated neighbour proof is
	// exactly what the collation walked — no more, no less.
	tracedProvider bool
	// coveredByMaster gives the masterchain predecessor a ProcessedInfo record
	// that already covers the queued message, so the candidate skips it as
	// processed instead of importing it. The skip still advances the processed
	// bound, so the block writes a claim and the validator walks the neighbour
	// queue to it with no import to show for it — the shape of the second
	// error text of the incident.
	coveredByMaster bool
	// coveredBacklog enqueues this many further masterchain-bound messages in
	// the shard top's queue, all under the predecessor's ProcessedInfo (it
	// requires coveredByMaster) and none offered in the cut: processed but not
	// yet dequeued by the shard, which is what a masterchain block that
	// imports nothing walks to make its drained claim.
	coveredBacklog int
}

func newFullMasterFixture(t *testing.T) fullMasterFixture {
	t.Helper()

	return newFullMasterFixtureWith(t, fullMasterFixtureOptions{})
}

func newFullMasterFixtureWith(t *testing.T, options fullMasterFixtureOptions) fullMasterFixture {
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
	if options.coveredByMaster {
		// One record at the predecessor's own seqno whose lt bound is the
		// shard's end lt: every message the shard enqueued before that lt is
		// covered through the shard-end-lt branch of the coverage rule.
		masterFixtureCoverQueue(t, &fixture, shardState.GenLT)
	}
	source := msgpool.ShardIdent{Workchain: 0, Shard: uint64(shardState.ShardIdent.GetShardID())}
	var queued *msgpool.InternalMessage
	if options.queueToMaster {
		// Created inside the top block itself, which is where every message
		// the masterchain imports comes from on a live network: the top is
		// registered by the next masterchain block and its queue is read at
		// once. cur stays at the source (UseDestBits 0), next is the
		// destination, so the entry is a genuine shard-to-master hop.
		message, enqueued := queuedInternalWithReferencedBody(
			t,
			address.NewAddress(0, 0, bytes.Repeat([]byte{0x71}, 32)),
			address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x72}, 32)),
			shardState.GenLT-10,
			shardState.GenUTime-1,
			tlb.FromNanoTONU(100_000),
			tlb.FromNanoTONU(100_000),
			0,
			source,
		)
		message.SourceSeqno = shardState.Seqno
		stateRoot = stateWithQueueMessage(t, stateRoot, message.Key, enqueued)
		if err = parseExact(&shardState, stateRoot); err != nil {
			t.Fatal(err)
		}
		queued = message
	}
	if options.coveredBacklog != 0 {
		if !options.coveredByMaster {
			t.Fatal("coveredBacklog needs coveredByMaster: an uncovered backlog would be offered in the cut, not walked over")
		}
		// Below the queued message's lt and below the shard's end lt, so every
		// one of them is covered by the record masterFixtureCoverQueue wrote,
		// and each at its own lt so each is its own queue key.
		for i := range options.coveredBacklog {
			message, enqueued := queuedInternalWithReferencedBody(
				t,
				address.NewAddress(0, 0, append([]byte{byte(i)}, bytes.Repeat([]byte{0x73}, 31)...)),
				address.NewAddress(0, 0xff, bytes.Repeat([]byte{0x72}, 32)),
				shardState.GenLT-20-uint64(i),
				shardState.GenUTime-1,
				tlb.FromNanoTONU(100_000),
				tlb.FromNanoTONU(100_000),
				0,
				source,
			)
			stateRoot = stateWithQueueMessage(t, stateRoot, message.Key, enqueued)
		}
		if err = parseExact(&shardState, stateRoot); err != nil {
			t.Fatal(err)
		}
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
			Shard:     source,
			EndLT:     shardState.GenLT,
			Processed: shardProcessed,
			// The acquisition path fills this from the neighbour view
			// (localNeighbor); the validator's authenticatedNeighborQueue
			// refuses a neighbour without it before it ever reaches the
			// collated proof.
			OutMsgQueueInfo: shardState.OutMsgQueueInfo,
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
	if queued != nil {
		fixture.request.Internals = &msgpool.Cut{Messages: []*msgpool.InternalMessage{queued}}
	}

	var provider *staticFullCollatedProofProvider
	if options.tracedProvider {
		view, viewErr := localViewFromPrevious(PreviousBlock{ID: newShard, Block: blockRoot, State: stateRoot}, true, true)
		if viewErr != nil {
			t.Fatal(viewErr)
		}
		views := map[msgpool.ShardIdent]*localNeighborView{source: view}
		fixture.request.FullCollatedProofs = &localFullProofProvider{proofViews: views, messageViews: views}
	} else {
		blockProof, proofErr := cell.CreateMerkleProof(blockRoot)
		if proofErr != nil {
			t.Fatal(proofErr)
		}
		stateProof, proofErr := cell.CreateMerkleProof(stateRoot)
		if proofErr != nil {
			t.Fatal(proofErr)
		}
		provider = &staticFullCollatedProofProvider{roots: []*cell.Cell{stateProof, blockProof}}
		fixture.request.FullCollatedProofs = provider
	}

	return fullMasterFixture{
		masterBuildFixture: fixture,
		shardState:         stateRoot,
		provider:           provider,
		queued:             queued,
	}
}

// masterFixtureCoverQueue rewrites the masterchain predecessor with a
// ProcessedInfo record covering everything a shard enqueued below lt, and
// re-derives what hangs off the state root: the block id and the validator
// group snapshot the tracker publishes for it.
func masterFixtureCoverQueue(t *testing.T, fixture *masterBuildFixture, lt uint64) {
	t.Helper()

	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, fixture.oldState.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	records, err := tlb.ProcessedUptoDict([]tlb.ProcessedUptoRecord{{
		ShardPrefix: uint64(fixture.request.Previous.ID.Shard),
		MCSeqno:     fixture.request.Previous.ID.SeqNo,
		LastMsgLT:   lt,
		LastMsgHash: processedInfinityHash,
	}})
	if err != nil {
		t.Fatal(err)
	}
	queue.ProcInfo = records
	fixture.oldState.OutMsgQueueInfo, err = queue.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	masterRoot, err := tlb.ToCell(&fixture.oldState)
	if err != nil {
		t.Fatal(err)
	}
	masterHash := masterRoot.HashKey()
	fixture.request.Previous.ID.RootHash = bytes.Clone(masterHash[:])
	fixture.request.Previous.State = masterRoot

	tracker, err := groups.NewTracker(groups.TrackerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	tracked, err := tracker.Apply(groups.ApplyInput{
		Block: fixture.request.Previous.ID,
		Root:  masterRoot,
		AsOf:  time.Unix(int64(fixture.oldState.GenUTime), 0),
	})
	if err != nil {
		t.Fatal(err)
	}
	fixture.request.Groups = tracked.Snapshot
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
