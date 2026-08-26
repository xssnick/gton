package collator

import (
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
)

func TestAfterSplitFullCollatedProofCoversDispatchBoundary(t *testing.T) {
	parentRequest := emptyCandidateRequest(t)
	startLT := requestStartLT(t, parentRequest)
	lts := make([]uint64, 30)
	for i := range lts {
		lts[i] = startLT - 300 + uint64(i)*10
	}
	parentRequest.Previous.State = previousStateWithDispatchQueue(t, parentRequest.Previous.State,
		makeDispatchQueue(t,
			dispatchFixtureAccount{accountID: [32]byte{0x11}, bodyInRef: true, lts: lts},
			dispatchFixtureAccount{accountID: [32]byte{0x91}, bodyInRef: true, lts: lts},
		),
	)
	parentSession := &parentRequest.Masterchain.Groups.Active[0]
	parentSession.Registered[0].FSM = groups.ShardFSM{
		Kind: groups.ShardFSMSplit, UTime: parentRequest.Header.GenUtime, Interval: 10,
	}
	parentRequest.BeforeSplit = true

	parentCandidate, err := testBuilder().BuildShard(context.Background(), parentRequest)
	if err != nil {
		t.Fatal(err)
	}
	parentQueueSize := parentCandidate.Stats.OutQueueSize
	parent := PreviousBlock{
		ID: parentCandidate.ID, Block: candidateBlock(t, parentCandidate), State: parentCandidate.State,
		OutQueueSize: &parentQueueSize,
	}

	childRequest := parentRequest
	childRequest.Shard = groups.ShardID{
		Workchain: parentRequest.Shard.Workchain,
		Shard:     mustPredecessorChild(t, parentRequest.Shard.Shard, true),
	}
	childRequest.Previous = parent
	childRequest.BeforeSplit = false
	childRequest.Header.GenUtime++
	childRequest.Header.GenUtimeMS = uint64(childRequest.Header.GenUtime) * 1_000
	childRequest.Masterchain.Config.capabilities |= capFullCollatedData
	childRequest.Masterchain.Groups.Active = []groups.Session{{
		Shard: childRequest.Shard, CatchainSeqno: parentSession.CatchainSeqno,
		ValidatorSetHash: parentSession.ValidatorSetHash,
		Genesis:          []ton.BlockIDExt{*parent.ID.Copy()},
		Registered: []groups.ShardDescription{{
			Shard: parentRequest.Shard, Block: *parent.ID.Copy(), BeforeSplit: true,
		}},
	}}
	parentNeighbor := fullCollatedTestNeighbor(t, parent)
	childRequest.Neighbors = append([]Neighbor{parentNeighbor}, masterchainNeighbor(childRequest)...)
	childRequest.NeighborShardEndLT = func(uint32, int32, uint64) uint64 { return parentNeighbor.EndLT }

	candidate, err := testBuilder().BuildShard(context.Background(), childRequest)
	if err != nil {
		t.Fatal(err)
	}
	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatal(err)
	}
	var provenParent *cell.Cell
	for _, root := range roots {
		virtual, unwrapErr := cell.UnwrapProofVirtualized(root, parent.State.Hash())
		if unwrapErr == nil {
			provenParent = virtual
			break
		}
	}
	if provenParent == nil {
		t.Fatal("collated parent proof absent")
	}
	var provenState tlb.ShardStateUnsplit
	if err = parseProofExact(&provenState, provenParent); err != nil {
		t.Fatal(err)
	}
	var oldQueue tlb.OutMsgQueueInfo
	if err = parseProofExact(&oldQueue, provenState.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	effective := &tlb.DispatchQueueAugDict{AugmentedDictionary: oldQueue.Extra.DispatchQueue.Copy()}
	if ok, cutErr := effective.CutPrefixSubdict(
		shardPrefixCell(mustPredecessorIdent(t, childRequest.Shard)), false,
	); cutErr != nil || !ok {
		t.Fatalf("cut proven dispatch: ok=%v err=%v", ok, cutErr)
	}
	var candidateState tlb.ShardStateUnsplit
	if err = parseExact(&candidateState, candidate.State); err != nil {
		t.Fatal(err)
	}
	var candidateQueue tlb.OutMsgQueueInfo
	if err = parseExact(&candidateQueue, candidateState.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	if err = effective.ScanDiff(candidateQueue.Extra.DispatchQueue.AugmentedDictionary, true, func(
		_ *cell.Cell, oldValueExtra, newValueExtra *cell.Slice,
	) error {
		if oldValueExtra != nil {
			oldAccount, diffErr := dispatchDiffAccount(*oldValueExtra)
			if diffErr != nil {
				return diffErr
			}
			if _, _, diffErr = oldAccount.Messages.LoadMax(); diffErr != nil {
				return diffErr
			}
		}
		if newValueExtra != nil {
			newAccount, diffErr := dispatchDiffAccount(*newValueExtra)
			if diffErr != nil {
				return diffErr
			}
			if _, _, diffErr = newAccount.Messages.LoadMin(); diffErr != nil {
				return diffErr
			}
		}
		return nil
	}); err != nil {
		t.Fatalf("validate C++ dispatch diff from first child proof: %v", err)
	}
}
