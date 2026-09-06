package collator

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// On 2026-09-03 the stand's validator refused six of its own masterchain
// candidates in a row — every masterchain block it collated that imported a
// message from a shard — with
//
//	inbound message <hash>: load message from neighbor (0,8000000000000000,<seqno>):
//	dict has special cells in tree structure
//
// and the committee skipped each slot. The neighbour state proof in the
// collated data carried the shard's OutMsgQueue as a single pruned branch: the
// masterchain build asked for its neighbour proofs in prepareMaster, before the
// import phase had run, and the queue scan that widens those proofs is bounded
// by the ProcessedInfo claim the block is about to make — which, before any
// message has been imported, does not exist. No claim, no walk, no queue cells.
//
// The reference collates its neighbour proofs after the block, in
// create_collated_data (collator.cpp:6345-6360, "4. Proofs for message
// queues"), for the masterchain as much as for a shard — only the previous
// state proof is shard-only (:6319). The shard path here already builds them in
// finishShard; this pins the masterchain to the same order, end to end: a
// masterchain candidate that imports a message enqueued in the very shard top
// it registers, collated through the production proof provider and verified by
// the real semantic verifier on the collated proof alone.
//
// The second case is the other error text of the same incident (23:49:18Z):
// the queued message is already covered by the predecessor's ProcessedInfo, so
// the block imports nothing — but a skipped message still advances the bound
// (retireInternal, as process_inbound_message does at collator.cpp:3956 before
// its already-processed check), the block writes a claim, and the validator's
// walk to that claim opens the same neighbour queue.
func TestMasterCollatedNeighborProofCoversTheImportedShardMessage(t *testing.T) {
	for _, tc := range []struct {
		name     string
		covered  bool
		imported uint32
		skipped  uint32
	}{
		{name: "imported", imported: 1},
		{name: "skipped as covered", covered: true, skipped: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			testMasterCollatedNeighborProofCoversQueueReads(t, tc.covered, tc.imported, tc.skipped)
		})
	}
}

func testMasterCollatedNeighborProofCoversQueueReads(t *testing.T, covered bool, imported, skipped uint32) {
	fixture := newFullMasterFixtureWith(t, fullMasterFixtureOptions{
		queueToMaster:   true,
		tracedProvider:  true,
		coveredByMaster: covered,
	})

	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.InternalsImported != imported || candidate.Stats.InternalsSkipped != skipped {
		t.Fatalf("internals imported=%d skipped=%d, want %d/%d",
			candidate.Stats.InternalsImported, candidate.Stats.InternalsSkipped, imported, skipped)
	}

	verifyMasterCandidateOnItsCollatedProofs(t, fixture, candidate)
}

// verifyMasterCandidateOnItsCollatedProofs runs the real semantic verifier over
// the candidate with the neighbour set production validation would hand it,
// after first making the same two neighbour-queue reads by hand. The hand-made
// pass says whether the candidate claimed a bound at all — without one the
// verifier's walk is vacuous and the test proves nothing — and names the read
// that fails in the incident's own words.
func verifyMasterCandidateOnItsCollatedProofs(t *testing.T, fixture fullMasterFixture, candidate *Candidate) {
	t.Helper()

	// The validator's own reads, made against nothing but the candidate: the
	// lookup of every imported message in the neighbour proof
	// (findImportedMessage) and the walk to the claimed bound
	// (verifySourceProcessed).
	claimed, err := replayCollatedNeighborQueueReads(t, candidate.BlockBOC, candidate.CollatedData)
	if err != nil {
		t.Fatalf("collated neighbour proof does not serve the validator's queue reads: %v", err)
	}
	if !claimed {
		t.Fatal("the candidate wrote no ProcessedInfo claim, so the walk above was vacuous")
	}

	verification := MasterVerificationRequest{
		Previous:           fixture.request.Previous,
		Config:             fixture.request.Config,
		Groups:             fixture.request.Groups,
		ShardTops:          fixture.request.ShardTops,
		Neighbors:          collatedNeighbors(t, fixture.request.Neighbors, candidate.BlockBOC, candidate.CollatedData),
		NeighborShardEndLT: fixture.request.NeighborShardEndLT,
		Semantics:          NewSemanticVerifier(tvm.NewTVM()),
		Candidate:          candidate,
	}
	if err = VerifyMasterCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify the masterchain candidate on its collated proofs: %v", err)
	}
}

// collatedNeighbors is the neighbour set production validation builds
// (loadExpectedNeighbors over the candidate's collated data): each shard
// neighbour is the view decoded from the candidate's own collated state proof,
// pruned wherever the collation did not read. The fixture's neighbours carry the
// resident shard state instead, and authenticatedNeighborQueue accepts that by
// hash — a pruned proof of the same queue has the same hash — and hands the
// verifier the full resident queue, so on the fixture's neighbours the verifier
// would walk a queue the proof never carried and pass a candidate the stand
// refused. Only on these does VerifyMasterCandidate read the proof and nothing
// else.
func collatedNeighbors(t *testing.T, neighbors []Neighbor, blockBOC, collatedBOC []byte) []Neighbor {
	t.Helper()

	_, verified := decodeCollatedCandidate(t, blockBOC, collatedBOC)
	collated := slices.Clone(neighbors)
	for i := range collated {
		if collated[i].Block.Workchain == address.MasterchainID {
			continue
		}
		state, err := verified.StateRoot(collated[i].Block)
		if err != nil {
			t.Fatal(err)
		}
		view, err := localViewFromPrevious(PreviousBlock{ID: collated[i].Block, State: state, Proven: true}, false, false)
		if err != nil {
			t.Fatal(err)
		}
		collated[i] = localNeighbor(view, collated[i].Shard)
	}

	return collated
}

// A masterchain block that imports nothing claims the drained bound —
// everything below its predecessor's end lt is processed — and, unlike a
// shard's, that claim is never abandoned: the reference's update_processed_upto
// always writes it, and the prefix behind it is only what the shards enqueued
// to the masterchain and have not dequeued yet. Here the shard top holds one
// more such entry than the shard's scan budget allows, all of them under the
// predecessor's ProcessedInfo and none offered in the cut. The block must still
// claim, its proof must carry the walk, and the real verifier must accept it on
// that proof alone. Under the shard's budget the claim would be dropped
// silently, and every masterchain block until the next import with it.
func TestMasterDrainedClaimIsNotBudgeted(t *testing.T) {
	fixture := newFullMasterFixtureWith(t, fullMasterFixtureOptions{
		tracedProvider:  true,
		coveredByMaster: true,
		coveredBacklog:  drainedQueueScanBudget + 1,
	})

	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.InternalsImported != 0 || candidate.Stats.InternalsSkipped != 0 {
		t.Fatalf("internals imported=%d skipped=%d, want none: the backlog is covered and was not offered",
			candidate.Stats.InternalsImported, candidate.Stats.InternalsSkipped)
	}
	verifyMasterCandidateOnItsCollatedProofs(t, fixture, candidate)
}

// The order is the invariant. prepareMaster must not ask for the neighbour
// proofs, and finishMaster must ask for them after the import phase and before
// the ProcessedInfo update, because the scan that fills them is bounded by the
// same prospective claim updateProcessedInfo writes — read from one place, by
// both, with nothing that moves the bound in between.
func TestMasterNeighborProofsAreBuiltAfterTheImportPhase(t *testing.T) {
	prepare := methodCallOrder(t, "master.go", "prepareMaster", "c")
	for _, early := range []string{"prepareFullCollatedProofs", "buildDeferredCollatedProofs"} {
		if slices.Contains(prepare, early) {
			t.Fatalf("prepareMaster calls %s before the import phase; the neighbour queue scan has no bound yet and traces nothing: %v", early, prepare)
		}
	}

	finish := methodCallOrder(t, "master.go", "finishMaster", "c")
	proofs := slices.Index(finish, "buildDeferredCollatedProofs")
	processed := slices.Index(finish, "updateProcessedInfo")
	switch {
	case proofs < 0:
		t.Fatalf("finishMaster no longer builds the neighbour proofs: %v", finish)
	case processed < 0:
		t.Fatalf("finishMaster no longer updates ProcessedInfo: %v", finish)
	case proofs > processed:
		t.Fatalf("finishMaster builds the neighbour proofs after ProcessedInfo is written: %v", finish)
	}
	// Nothing between the two may move the processed bound; only the bound's
	// two writers are in imports.go/execute.go, so any call here that is not
	// the proof build itself would have to be audited against them.
	if between := finish[proofs+1 : processed]; len(between) != 0 {
		t.Fatalf("calls between the proof build and the ProcessedInfo update may move the bound the proof was built for: %v", between)
	}
	if !slices.Contains(methodCallOrder(t, "master.go", "prepareMasterPhases", "c"), "processInternals") {
		t.Fatal("prepareMasterPhases no longer runs the import phase; the order guard above is meaningless")
	}
}

// Two refused candidates captured on the stand, replayed against the exact
// reads their validator made. The fixtures are the candidate's own block and
// collated data; nothing else is needed, because the failing read never
// reached the predecessor state.
//
// 82429825 (17:47:37Z, slot 394) imports msg 9450619132ce... from
// (0,8000000000000000,87669527) and fails the message lookup; 82484082
// (23:49:18Z, slot 272) imports nothing, but a covered message advanced its
// processed bound and the validator's walk to that bound fails at the queue
// root. Same collated shape, both error texts of the incident.
func TestRefusedMasterCandidateDumpsReadAnUntracedNeighborQueue(t *testing.T) {
	for _, dump := range []struct {
		seqno string
		want  string
	}{
		{
			seqno: "82429825",
			// The journal line of 2026-09-03T17:47:37Z, after "verify candidate
			// semantic transition: ".
			want: "inbound message 9450619132ce84cd8ca5c74de09598cc341757f11ae6309e85aaa544f5083447: collator: invalid input: load message from neighbor (0,8000000000000000,87669527): dict has special cells in tree structure",
		},
		{
			seqno: "82484082",
			// The journal line of 2026-09-03T23:49:18Z.
			want: "collator: invalid input: scan inbound queue from (0,8000000000000000): augmented dict has special cells in tree structure",
		},
	} {
		t.Run(dump.seqno, func(t *testing.T) {
			dir := filepath.Join(packageSourceFile(t, "testdata"), "failed-master-candidates", dump.seqno)
			blockBOC, err := os.ReadFile(filepath.Join(dir, "block.boc"))
			if err != nil {
				t.Fatal(err)
			}
			collatedBOC, err := os.ReadFile(filepath.Join(dir, "collated.boc"))
			if err != nil {
				t.Fatal(err)
			}

			_, err = replayCollatedNeighborQueueReads(t, blockBOC, collatedBOC)
			if !errors.Is(err, cell.ErrDictHasSpecialCells) {
				t.Fatalf("replay = %v, want the validator's pruned-branch refusal", err)
			}
			if !strings.Contains(err.Error(), dump.want) {
				t.Fatalf("replay = %q, want %q", err, dump.want)
			}
			// The shape behind the text: the whole queue is one pruned branch,
			// not a proof that stops short somewhere below the root.
			for _, queue := range collatedShardNeighborQueues(t, blockBOC, collatedBOC) {
				root := queue.OutQueue.RootCell()
				if root == nil || !root.IsSpecial() {
					t.Fatal("neighbour queue root is present in the proof; the incident shipped it pruned")
				}
			}
		})
	}
}

// replayCollatedNeighborQueueReads makes, from a candidate's block and collated
// data alone, the two neighbour-queue reads the semantic verifier makes: the
// lookup of every queued inbound message in the proof of the neighbour that
// owns its current hop (findImportedMessage), and the walk of every neighbour
// proof to the bound the candidate's ProcessedInfo claims
// (verifySourceProcessed). Errors are wrapped as the verifier wraps them, so a
// refusal captured on a node can be matched by text. The flag says whether the
// candidate claimed a bound at all — without one the walk is vacuous, and a
// caller expecting a walk should know.
func replayCollatedNeighborQueueReads(t *testing.T, blockBOC, collatedBOC []byte) (bool, error) {
	t.Helper()

	block, _ := decodeCollatedCandidate(t, blockBOC, collatedBOC)
	target := msgpool.ShardIdent{Workchain: address.MasterchainID, Shard: msgpool.ShardAll}
	queues := collatedShardNeighborQueues(t, blockBOC, collatedBOC)

	inbound, err := loadInMsgDescriptors(block.Extra.InMsgDesc, 0)
	if err != nil {
		t.Fatal(err)
	}
	iterator, err := inbound.IteratorExtra(false, false)
	if err != nil {
		t.Fatal(err)
	}
	var envelopes semanticEnvelopeCache
	for iterator.Next() {
		item := iterator.View()
		hash, keyErr := semanticDescriptorKey(&item.Key)
		if keyErr != nil {
			t.Fatal(keyErr)
		}
		descriptor, parseErr := parseSemanticInDescriptor(item.Value, hash, &envelopes)
		if parseErr != nil {
			t.Fatal(parseErr)
		}
		if descriptor.tag != semanticInFinal && descriptor.tag != semanticInTransit {
			continue
		}
		envelope := descriptor.envelope
		if target.ContainsPrefix(envelope.current) {
			continue
		}
		key := msgpool.MakeQueueKey(envelope.next, envelope.message.HashKey())
		for _, queue := range queues {
			if !queue.owner.ContainsPrefix(envelope.current) {
				continue
			}
			if _, err = loadSemanticQueueEntryFromNeighbor(queue.OutQueue, key); err != nil && !isMissingKey(err) {
				return false, fmt.Errorf("inbound message %x: %w: load message from neighbor (%d,%016x,%d): %w",
					hash, ErrInvalidInput, queue.owner.Workchain, queue.owner.Shard, queue.seqno, err)
			}
		}
	}
	if err = iterator.Err(); err != nil {
		t.Fatal(err)
	}

	// The claim, from the new side of the state update: the cells that changed
	// are there, and a written ProcessedInfo is one of them.
	newState, err := block.StateUpdate.PeekRef(1)
	if err != nil {
		t.Fatal(err)
	}
	var state tlb.ShardStateUnsplit
	if err = parseProofExact(&state, newState); err != nil {
		t.Fatal(err)
	}
	info, err := parseNeighborQueueInfo(state.OutMsgQueueInfo)
	if err != nil {
		t.Fatal(err)
	}
	records, err := tlb.LoadProcessedUptoRecords(info.ProcInfo, target.Shard)
	if err != nil {
		t.Fatal(err)
	}
	claimed := false
	for _, record := range records {
		if record.MCSeqno != block.BlockInfo.SeqNo {
			continue
		}
		claimed = true
		bound := semanticMessageBound{lt: record.LastMsgLT, hash: record.LastMsgHash}
		for _, queue := range queues {
			err = walkSemanticQueuePrefix(queue.OutQueue, target, bound, func(semanticQueueEntry) error { return nil })
			if err != nil {
				return true, fmt.Errorf("%w: scan inbound queue from (%d,%016x): %w",
					ErrInvalidInput, queue.owner.Workchain, queue.owner.Shard, err)
			}
		}
	}

	return claimed, nil
}

// collatedShardNeighborQueue is one shard neighbour's outbound queue as the
// validator sees it: parsed out of the state proof in the collated data.
type collatedShardNeighborQueue struct {
	owner msgpool.ShardIdent
	seqno uint32
	tlb.OutMsgQueueInfo
}

func collatedShardNeighborQueues(t *testing.T, blockBOC, collatedBOC []byte) []collatedShardNeighborQueue {
	t.Helper()

	_, verified := decodeCollatedCandidate(t, blockBOC, collatedBOC)
	var queues []collatedShardNeighborQueue
	for _, root := range verified.virtualRoots {
		var state tlb.ShardStateUnsplit
		if err := parseProofExact(&state, root); err != nil {
			// The other virtual roots are block proofs.
			continue
		}
		if state.ShardIdent.WorkchainID == address.MasterchainID {
			continue
		}
		info, err := parseNeighborQueueInfo(state.OutMsgQueueInfo)
		if err != nil {
			t.Fatal(err)
		}
		queues = append(queues, collatedShardNeighborQueue{
			owner:           msgpool.ShardIdent{Workchain: state.ShardIdent.WorkchainID, Shard: uint64(state.ShardIdent.GetShardID())},
			seqno:           state.Seqno,
			OutMsgQueueInfo: info,
		})
	}
	if len(queues) == 0 {
		t.Fatal("collated data carries no shard neighbour state proof")
	}

	return queues
}

func decodeCollatedCandidate(t *testing.T, blockBOC, collatedBOC []byte) (tlb.Block, verifiedCollatedData) {
	t.Helper()

	blockRoot, err := cell.FromBOC(blockBOC)
	if err != nil {
		t.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, blockRoot); err != nil {
		t.Fatal(err)
	}
	roots, err := cell.FromBOCMultiRoot(collatedBOC)
	if err != nil {
		t.Fatal(err)
	}
	verified, err := verifyCollatedRoots(roots, block.BlockInfo.GenUtime)
	if err != nil {
		t.Fatal(err)
	}
	if !verified.full {
		t.Fatal("candidate carries no full collated data")
	}

	return block, verified
}
