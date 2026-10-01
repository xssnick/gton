package collator

import (
	"bytes"
	"context"
	"errors"
	"slices"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestPrepareAndBuildShardAfterSplit(t *testing.T) {
	tests := []struct {
		name        string
		right       bool
		wantBalance uint64
		wantFees    uint64
	}{
		{name: "left takes fee floor", wantBalance: 11_000_000_000, wantFees: 2},
		{name: "right takes fee ceil", right: true, wantBalance: 22_000_000_000, wantFees: 3},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newSplitPredecessorFixture(t, test.right)
			prepared, err := preparePredecessor(fixture.req, predecessorReadSet(t, fixture.req))
			if err != nil {
				t.Fatalf("preparePredecessor: %v", err)
			}

			if prepared.topology.kind != topologyAfterSplit || prepared.topology.target != fixture.target {
				t.Fatalf("topology = %+v, want after-split target %+v", prepared.topology, fixture.target)
			}
			if prepared.oldRoot.WithoutTrace().HashKey() != fixture.req.Previous.State.HashKey() {
				t.Fatal("split predecessor update is not rooted at the parent state")
			}
			if prepared.state.ShardIdent != fixture.target || prepared.state.BeforeSplit {
				t.Fatalf("prepared state has wrong split header: %+v", prepared.state)
			}
			assertPredecessorCurrency(t, prepared.stats.TotalBalance, test.wantBalance, "split total balance")
			assertPredecessorCurrency(t, prepared.stats.TotalValidatorFees, test.wantFees, "split validator fees")
			if prepared.stats.OverloadHistory != 0 || prepared.stats.UnderloadHistory != 0 {
				t.Fatalf("split load history was not reset: %+v", prepared.stats)
			}

			assertPredecessorCount(t, prepared.state.Accounts.ShardAccounts.AugmentedDictionary, 1, "split accounts")
			assertPredecessorKey(t, prepared.state.Accounts.ShardAccounts.AugmentedDictionary,
				predecessorAccountKey(fixture.keptAccount), true, "kept account")
			assertPredecessorKey(t, prepared.state.Accounts.ShardAccounts.AugmentedDictionary,
				predecessorAccountKey(fixture.removedAccount), false, "removed account")

			if prepared.queueSize != 1 {
				t.Fatalf("split queue size = %d, want 1", prepared.queueSize)
			}
			assertPredecessorCount(t, prepared.queueInfo.OutQueue.AugmentedDictionary, 1, "split outbound queue")
			assertPredecessorKey(t, prepared.queueInfo.OutQueue.AugmentedDictionary,
				predecessorQueueKey(fixture.keptQueue), true, "kept queue entry")
			assertPredecessorKey(t, prepared.queueInfo.OutQueue.AugmentedDictionary,
				predecessorQueueKey(fixture.removedQueue), false, "removed queue entry")
			assertPredecessorRecords(t, prepared.queueInfo.ProcInfo, uint64(fixture.target.GetShardID()), fixture.records)

			candidate, err := testBuilder().BuildShard(context.Background(), fixture.req)
			if err != nil {
				t.Fatalf("BuildShard: %v", err)
			}
			if candidate.ID.Workchain != fixture.req.Shard.Workchain || candidate.ID.Shard != fixture.req.Shard.Shard ||
				candidate.ID.SeqNo != fixture.req.Previous.ID.SeqNo+1 {
				t.Fatalf("candidate id = %+v", candidate.ID)
			}
			if candidate.Stats.OutQueueSize != 1 {
				t.Fatalf("candidate queue size = %d, want 1", candidate.Stats.OutQueueSize)
			}

			var block tlb.Block
			if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
				t.Fatalf("decode split block: %v", err)
			}
			if !block.BlockInfo.AfterSplit || block.BlockInfo.AfterMerge || block.BlockInfo.Shard != fixture.target {
				t.Fatalf("split block header flags = %+v", block.BlockInfo)
			}
			applied, err := cell.ApplyMerkleUpdate(fixture.req.Previous.State, candidate.StateUpdate)
			if err != nil {
				t.Fatalf("apply split state update: %v", err)
			}
			if applied.HashKey() != candidate.State.HashKey() {
				t.Fatal("split state update does not produce the candidate state")
			}

			var nextState tlb.ShardStateUnsplit
			if err = parseExact(&nextState, candidate.State); err != nil {
				t.Fatalf("decode split candidate state: %v", err)
			}
			assertPredecessorCount(t, nextState.Accounts.ShardAccounts.AugmentedDictionary, 1, "candidate accounts")
			queue := candidateQueueInfo(t, candidate)
			assertPredecessorCount(t, queue.OutQueue.AugmentedDictionary, 1, "candidate outbound queue")
			assertPredecessorRecords(t, queue.ProcInfo, uint64(fixture.target.GetShardID()), fixture.records)
		})
	}
}

func TestFullCollatedProofCarriesPredecessorAccountsValidation(t *testing.T) {
	req := emptyCandidateRequest(t)
	req.Previous.State = stateWithAccounts(
		t,
		req.Previous.State,
		activeContracts(
			t,
			req.Header.GenUtime,
			activeContract{
				address: predecessorAddress(0x11),
				code:    externalAcceptCode(t),
				balance: 11_000_000_000,
			},
			activeContract{
				address: predecessorAddress(0x91),
				code:    externalAcceptCode(t),
				balance: 22_000_000_000,
			},
		),
	)
	req = advanceCandidateRequest(t, req)
	lazyState, err := cell.FromBOCWithOptions(
		req.Previous.State.ToBOCWithOptions(cell.BOCSerializeOptions{}),
		cell.BOCParseOptions{Lazy: true},
	)
	if err != nil {
		t.Fatalf("decode production-shaped lazy predecessor: %v", err)
	}
	req.Previous.State = lazyState
	attachFullCollatedTestNeighbors(t, &req)
	req.Masterchain.Config.capabilities |= capFullCollatedData

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("build linear full-collated candidate: %v", err)
	}
	if candidate.Stats.Transactions != 0 {
		t.Fatalf("no-op candidate transactions = %d, want zero", candidate.Stats.Transactions)
	}

	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		t.Fatalf("decode collated data: %v", err)
	}
	var provenPredecessor *cell.Cell
	for _, root := range roots {
		virtual, proofErr := cell.UnwrapProofVirtualized(root, req.Previous.State.Hash())
		if proofErr == nil {
			provenPredecessor = virtual
			break
		}
	}
	if provenPredecessor == nil {
		t.Fatal("collated data carries no predecessor state proof")
	}

	var proven tlb.ShardStateUnsplit
	if err = parseProofExact(&proven, provenPredecessor); err != nil {
		t.Fatalf("decode proven predecessor state: %v", err)
	}
	if err = proven.Accounts.ShardAccounts.Validate(); err != nil {
		t.Fatalf("validate C++ predecessor ShardAccounts root: %v", err)
	}
}

func TestPreparePredecessorTracesDispatchQueueRootValidation(t *testing.T) {
	req := emptyCandidateRequest(t)
	dispatch := nonEmptyDispatchQueue(t, req.Header.GenUtime-1, requestStartLT(t, req)-10)
	req.Previous.State = previousStateWithDispatchQueue(t, req.Previous.State, dispatch)

	usage := predecessorReadSet(t, req)
	if _, err := preparePredecessor(req, usage); err != nil {
		t.Fatalf("prepare predecessor: %v", err)
	}
	proof, err := usage.Proof()
	if err != nil {
		t.Fatalf("build predecessor usage proof: %v", err)
	}
	proven, err := cell.UnwrapProofVirtualized(proof, req.Previous.State.Hash())
	if err != nil {
		t.Fatalf("unwrap predecessor usage proof: %v", err)
	}

	var state tlb.ShardStateUnsplit
	if err = parseProofExact(&state, proven); err != nil {
		t.Fatalf("decode proven predecessor: %v", err)
	}
	var queue tlb.OutMsgQueueInfo
	if err = parseProofExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatalf("decode proven predecessor queue: %v", err)
	}
	if queue.Extra == nil {
		t.Fatal("proven predecessor has no dispatch queue")
	}
	if err = queue.Extra.DispatchQueue.Validate(); err != nil {
		t.Fatalf("validate proven dispatch queue root: %v", err)
	}
}

func TestAfterSplitFullCollatedProofCarriesChangedAccount(t *testing.T) {
	for _, test := range []struct {
		name  string
		right bool
	}{
		{name: "left"},
		{name: "right", right: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			parentRequest := emptyCandidateRequest(t)
			leftAddress := predecessorAddress(0x11)
			rightAddress := predecessorAddress(0x91)
			parentRequest.Previous.State = stateWithAccounts(
				t,
				parentRequest.Previous.State,
				activeContracts(t, parentRequest.Header.GenUtime,
					activeContract{address: leftAddress, code: externalAcceptCode(t), balance: 11_000_000_000},
					activeContract{address: rightAddress, code: externalAcceptCode(t), balance: 22_000_000_000},
				),
			)
			parentSession := &parentRequest.Masterchain.Groups.Active[0]
			parentSession.Registered[0].FSM = groups.ShardFSM{
				Kind:     groups.ShardFSMSplit,
				UTime:    parentRequest.Header.GenUtime,
				Interval: 10,
			}
			parentRequest.BeforeSplit = true

			parentCandidate, err := testBuilder().BuildShard(context.Background(), parentRequest)
			if err != nil {
				t.Fatalf("build parent before split: %v", err)
			}
			parentQueueSize := parentCandidate.Stats.OutQueueSize
			parent := PreviousBlock{
				ID:           parentCandidate.ID,
				Block:        candidateBlock(t, parentCandidate),
				State:        parentCandidate.State,
				OutQueueSize: &parentQueueSize,
			}

			childRequest := parentRequest
			childRequest.Shard = groups.ShardID{
				Workchain: parentRequest.Shard.Workchain,
				Shard:     mustPredecessorChild(t, parentRequest.Shard.Shard, !test.right),
			}
			childRequest.Previous = parent
			childRequest.BeforeSplit = false
			childRequest.Header.GenUtime++
			childRequest.Header.GenUtimeMS = uint64(childRequest.Header.GenUtime) * 1_000
			childRequest.Masterchain.Config.capabilities |= capFullCollatedData
			childRequest.Masterchain.Groups.Active = []groups.Session{{
				Shard:            childRequest.Shard,
				CatchainSeqno:    parentSession.CatchainSeqno,
				ValidatorSetHash: parentSession.ValidatorSetHash,
				Genesis:          []ton.BlockIDExt{*parent.ID.Copy()},
				Registered: []groups.ShardDescription{{
					Shard:       parentRequest.Shard,
					Block:       *parent.ID.Copy(),
					BeforeSplit: true,
				}},
			}}
			parentNeighbor := fullCollatedTestNeighbor(t, parent)
			childRequest.Neighbors = append([]Neighbor{parentNeighbor}, masterchainNeighbor(childRequest)...)
			childRequest.NeighborShardEndLT = func(uint32, int32, uint64) uint64 {
				return parentNeighbor.EndLT
			}

			changedAddress := leftAddress
			if test.right {
				changedAddress = rightAddress
			}
			external, err := tlb.ToCell(&tlb.ExternalMessage{
				DstAddr: changedAddress,
				Body:    cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell(),
			})
			if err != nil {
				t.Fatal(err)
			}
			childRequest.Externals = []ExternalInput{externalInput(t, external)}

			candidate, err := testBuilder().BuildShard(context.Background(), childRequest)
			if err != nil {
				t.Fatalf("build first child block: %v", err)
			}
			if candidate.Stats.Transactions != 1 || candidate.Stats.ExternalIncluded != 1 {
				t.Fatalf("child transaction stats = %+v", candidate.Stats)
			}
			applied, err := cell.ApplyMerkleUpdate(parent.State, candidate.StateUpdate)
			if err != nil {
				t.Fatalf("apply child state update to parent: %v", err)
			}
			if applied.HashKey() != candidate.State.HashKey() {
				t.Fatal("child state update does not produce the candidate state")
			}

			roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
			if err != nil {
				t.Fatalf("decode child collated data: %v", err)
			}
			var provenParent *cell.Cell
			for _, root := range roots {
				virtual, proofErr := cell.UnwrapProof(root, parent.State.Hash())
				if proofErr == nil {
					provenParent = virtual
					break
				}
			}
			if provenParent == nil {
				t.Fatal("collated data carries no parent state proof")
			}

			// This mirrors the C++ validator path: split_prev_state first cuts
			// the proven parent dictionary, then precheck_account_updates opens
			// every changed ShardAccount from that effective child state.
			var oldState tlb.ShardStateUnsplit
			if err = parseExact(&oldState, provenParent); err != nil {
				t.Fatalf("decode proven parent state: %v", err)
			}
			// ShardState::unpack_state validates the predecessor ShardAccounts
			// wrapper and root extra before it performs the split. Descendants are
			// retained only when the split or changed-account checks read them.
			if err = oldState.Accounts.ShardAccounts.Validate(); err != nil {
				t.Fatalf("validate C++ predecessor ShardAccounts root: %v", err)
			}
			oldAccounts := &tlb.ShardAccountsAugDict{
				AugmentedDictionary: oldState.Accounts.ShardAccounts.Copy(),
			}
			if ok, cutErr := oldAccounts.CutPrefixSubdict(shardPrefixCell(mustPredecessorIdent(t, childRequest.Shard)), false); cutErr != nil || !ok {
				t.Fatalf("split proven parent accounts: ok=%v err=%v", ok, cutErr)
			}

			var newState tlb.ShardStateUnsplit
			if err = parseExact(&newState, candidate.State); err != nil {
				t.Fatalf("decode child state: %v", err)
			}
			key := cell.BeginCell().MustStoreSlice(changedAddress.Data(), 256).EndCell()
			oldValue, err := oldAccounts.LoadValue(key)
			if err != nil {
				t.Fatalf("load changed account from proven split parent: %v", err)
			}
			newValue, err := newState.Accounts.ShardAccounts.LoadValue(key)
			if err != nil {
				t.Fatalf("load changed account from child state: %v", err)
			}
			oldValueCell, err := oldValue.ToCell()
			if err != nil {
				t.Fatalf("materialize old shard account: %v", err)
			}
			newValueCell, err := newValue.ToCell()
			if err != nil {
				t.Fatalf("materialize new shard account: %v", err)
			}

			var oldAccount, newAccount tlb.ShardAccount
			if err = loadExactSlice(&oldAccount, oldValue); err != nil {
				t.Fatalf("decode changed account from proven split parent: %v", err)
			}
			if err = loadExactSlice(&newAccount, newValue); err != nil {
				t.Fatalf("decode changed account from child state: %v", err)
			}
			if _, err = tvm.PrepareAccount(&oldAccount, changedAddress); err != nil {
				t.Fatalf("open old account state as C++ precheck_account_updates does: %v", err)
			}
			if _, err = tvm.PrepareAccount(&newAccount, changedAddress); err != nil {
				t.Fatalf("open new account state: %v", err)
			}
			if oldAccount.LastTransLT == newAccount.LastTransLT {
				t.Fatalf("external transaction did not advance last transaction lt %d", oldAccount.LastTransLT)
			}
			if oldValueCell.HashKey() == newValueCell.HashKey() {
				t.Fatal("external transaction did not change the retained shard account cell")
			}
		})
	}
}

func TestSplitOutboundQueueProofIncludesEnvelopeLoadedThroughAliasedTrace(t *testing.T) {
	req := emptyCandidateRequest(t)
	const queueEntries = 32
	root := stateWithSplitOutboundQueue(t, req.Previous.State, queueEntries)

	usage := cell.NewReadSet(root)
	estimator := newProofSizeEstimator(0)
	usage.SetRecordCallback(estimator.addLoadedCell)
	tracedRoot := usage.Root()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, tracedRoot); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	items, err := queue.OutQueue.RangeExtra(false, false)
	if err != nil || len(items) == 0 {
		t.Fatalf("load traced outbound queue: items=%d err=%v", len(items), err)
	}
	var first tlb.EnqueuedMsg
	if err = loadExactSlice(&first, items[0].Value.Copy()); err != nil {
		t.Fatal(err)
	}
	if first.Msg.Trace() == nil {
		t.Fatal("queued envelope has no usage trace")
	}

	// A split rebuild can reuse a synthetic trace path previously occupied by
	// another physical cell. The full-collated proof is selected by hashes, so
	// loading the real envelope through that path must still report its hash.
	decoy := cell.BeginCell().MustStoreUInt(0, 4).EndCell().WithTrace(first.Msg.Trace())
	if _, err = decoy.BeginParse(); err != nil {
		t.Fatal(err)
	}
	target := mustPredecessorIdent(t, groups.ShardID{
		Workchain: req.Shard.Workchain,
		Shard:     mustPredecessorChild(t, req.Shard.Shard, true),
	})
	effective := &tlb.OutMsgQueueAugDict{AugmentedDictionary: queue.OutQueue.Copy()}
	if _, err = filterOutQueue(effective, state.ShardIdent, target); err != nil {
		t.Fatalf("split full outbound queue: %v", err)
	}

	loaded := estimator.loadedHashes()
	proof, err := root.CreateHashUsageProof(loaded.loaded)
	if err != nil {
		t.Fatalf("build full-collated predecessor proof: %v", err)
	}
	provenRoot, err := cell.UnwrapProofVirtualized(proof, root.Hash())
	if err != nil {
		t.Fatalf("unwrap full-collated predecessor proof: %v", err)
	}
	var provenState tlb.ShardStateUnsplit
	if err = parseProofExact(&provenState, provenRoot); err != nil {
		t.Fatalf("decode proven predecessor state: %v", err)
	}
	var provenQueue tlb.OutMsgQueueInfo
	if err = parseProofExact(&provenQueue, provenState.OutMsgQueueInfo); err != nil {
		t.Fatalf("decode proven outbound queue: %v", err)
	}
	provenEffective := &tlb.OutMsgQueueAugDict{AugmentedDictionary: provenQueue.OutQueue.Copy()}
	kept, err := filterOutQueue(provenEffective, provenState.ShardIdent, target)
	if err != nil {
		t.Fatalf("run C++ split outbound queue filter from collated proof: %v", err)
	}
	if kept != queueEntries/2 {
		t.Fatalf("split outbound queue kept %d entries, want %d", kept, queueEntries/2)
	}
}

func stateWithSplitOutboundQueue(t *testing.T, root *cell.Cell, count int) *cell.Cell {
	t.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		t.Fatal(err)
	}
	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatal(err)
	}
	fee := tlb.FromNanoTONU(100_000)
	for i := range count {
		var sourceID, destinationID [32]byte
		if i%2 == 0 {
			sourceID[0] = 0x20
		} else {
			sourceID[0] = 0xa0
		}
		sourceID[1], sourceID[31] = byte(i>>8), byte(i)
		destinationID[0], destinationID[1], destinationID[31] = 0x80, byte(i>>8), byte(i)
		message, enqueued := queuedInternalWithReferencedBody(
			t,
			address.NewAddress(0, 0, sourceID[:]),
			address.NewAddress(0, 0, destinationID[:]),
			1_000_000+uint64(i),
			state.GenUTime,
			fee,
			fee,
			0,
			msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll},
		)
		key := cell.BeginCell().MustStoreSlice(message.Key[:], 352).EndCell()
		inserted, err := queue.OutQueue.SetWithMode(key, enqueued, cell.DictSetModeAdd)
		if err != nil || !inserted {
			t.Fatalf("insert outbound queue entry %d: inserted=%t err=%v", i, inserted, err)
		}
	}
	var err error
	state.OutMsgQueueInfo, err = queue.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	root, err = tlb.ToCell(&state)
	if err != nil {
		t.Fatal(err)
	}
	return root
}

func TestPrepareAndBuildShardAfterMerge(t *testing.T) {
	fixture := newMergePredecessorFixture(t)
	mechanicalRoot, err := mechanicalSplitState(fixture.req.Previous.State, fixture.req.Previous2.State)
	if err != nil {
		t.Fatalf("mechanicalSplitState: %v", err)
	}
	assertMechanicalSplitRoot(t, mechanicalRoot, fixture.req.Previous.State, fixture.req.Previous2.State)

	prepared, err := preparePredecessor(fixture.req, predecessorReadSet(t, fixture.req))
	if err != nil {
		t.Fatalf("preparePredecessor: %v", err)
	}
	if prepared.topology.kind != topologyAfterMerge || prepared.topology.target != fixture.target {
		t.Fatalf("topology = %+v, want after-merge target %+v", prepared.topology, fixture.target)
	}
	if prepared.oldRoot.WithoutTrace().HashKey() != mechanicalRoot.HashKey() {
		t.Fatal("merge predecessor old root is not the mechanical ShardStateSplit")
	}
	if prepared.topology.seqno != 104 || prepared.topology.previousGenLT != fixture.rightGenLT ||
		prepared.topology.previousGenUtime != fixture.rightGenUtime || prepared.topology.previousVertSeqno != 4 {
		t.Fatalf("merge predecessor maxima = %+v", prepared.topology)
	}
	if prepared.state.ShardIdent != fixture.target || prepared.state.Seqno != 103 ||
		prepared.state.GenLT != fixture.rightGenLT || prepared.state.GenUTime != fixture.rightGenUtime ||
		prepared.state.VertSeqno != 4 || prepared.state.MinRefMCSeqno != 70 {
		t.Fatalf("merged state header = %+v", prepared.state)
	}
	assertPredecessorCurrency(t, prepared.stats.TotalBalance, 33_000_000_000, "merged total balance")
	assertPredecessorCurrency(t, prepared.stats.TotalValidatorFees, 7, "merged validator fees")
	if prepared.stats.OverloadHistory != 0 || prepared.stats.UnderloadHistory != 0 {
		t.Fatalf("merge load history was not reset: %+v", prepared.stats)
	}
	if prepared.stats.MasterRef == nil || prepared.stats.MasterRef.SeqNo != fixture.rightMasterRef.SeqNo {
		t.Fatalf("merged master reference = %+v, want newer %+v", prepared.stats.MasterRef, fixture.rightMasterRef)
	}
	assertPredecessorCount(t, prepared.state.Accounts.ShardAccounts.AugmentedDictionary, 2, "merged accounts")
	assertPredecessorCount(t, prepared.queueInfo.OutQueue.AugmentedDictionary, 2, "merged outbound queue")
	if prepared.queueSize != 2 {
		t.Fatalf("merged queue size = %d, want 2", prepared.queueSize)
	}
	for _, key := range fixture.queueKeys {
		assertPredecessorKey(t, prepared.queueInfo.OutQueue.AugmentedDictionary,
			predecessorQueueKey(key), true, "merged queue entry")
	}
	assertPredecessorRecords(t, prepared.queueInfo.ProcInfo, uint64(fixture.target.GetShardID()), fixture.records)

	candidate, err := testBuilder().BuildShard(context.Background(), fixture.req)
	if err != nil {
		t.Fatalf("BuildShard: %v", err)
	}
	if candidate.ID.Workchain != fixture.req.Shard.Workchain || candidate.ID.Shard != fixture.req.Shard.Shard ||
		candidate.ID.SeqNo != 104 || candidate.Stats.OutQueueSize != 2 {
		t.Fatalf("merged candidate identity/stats = %+v / %+v", candidate.ID, candidate.Stats)
	}

	var block tlb.Block
	if err = parseExact(&block, candidateBlock(t, candidate)); err != nil {
		t.Fatalf("decode merge block: %v", err)
	}
	if !block.BlockInfo.AfterMerge || block.BlockInfo.AfterSplit || block.BlockInfo.PrevRef.Prev2 == nil {
		t.Fatalf("merge block header flags/references = %+v", block.BlockInfo)
	}
	assertRequestBlockReference(t, block.BlockInfo.PrevRef.Prev1, fixture.req.Previous.ID, fixture.leftGenLT)
	assertRequestBlockReference(t, *block.BlockInfo.PrevRef.Prev2, fixture.req.Previous2.ID, fixture.rightGenLT)

	applied, err := cell.ApplyMerkleUpdate(mechanicalRoot, candidate.StateUpdate)
	if err != nil {
		t.Fatalf("apply merge state update to mechanical root: %v", err)
	}
	if applied.HashKey() != candidate.State.HashKey() {
		t.Fatal("merge state update does not produce the candidate state")
	}
	if _, err = cell.ApplyMerkleUpdate(fixture.req.Previous.State, candidate.StateUpdate); err == nil {
		t.Fatal("merge state update unexpectedly applies to one child instead of the mechanical split root")
	}

	var nextState tlb.ShardStateUnsplit
	if err = parseExact(&nextState, candidate.State); err != nil {
		t.Fatalf("decode merged candidate state: %v", err)
	}
	assertPredecessorCount(t, nextState.Accounts.ShardAccounts.AugmentedDictionary, 2, "candidate merged accounts")
	queue := candidateQueueInfo(t, candidate)
	assertPredecessorCount(t, queue.OutQueue.AugmentedDictionary, 2, "candidate merged outbound queue")
	assertPredecessorRecords(t, queue.ProcInfo, uint64(fixture.target.GetShardID()), fixture.records)
}

func TestPreparePredecessorRejectsMergeDictionaryConflicts(t *testing.T) {
	tests := []struct {
		name    string
		wantErr string
		mutate  func(*testing.T, *mergePredecessorFixture)
	}{
		{
			name: "duplicate account key",
			// Two valid sibling states cannot contain the same account key. This
			// malformed fixture is therefore rejected at the stronger per-state
			// shard-prefix invariant before dictionary combination.
			wantErr: "outside the state shard",
			mutate: func(t *testing.T, fixture *mergePredecessorFixture) {
				left := predecessorTestState(t, fixture.req.Previous.State)
				right := predecessorTestState(t, fixture.req.Previous2.State)
				right.Accounts.ShardAccounts = left.Accounts.ShardAccounts

				leftStats := predecessorTestStats(t, &left)
				rightStats := predecessorTestStats(t, &right)
				rightStats.TotalBalance = leftStats.TotalBalance
				right.Stats = predecessorTestStatsCell(t, rightStats)
				fixture.req.Previous2.State = predecessorTestStateCell(t, right)
			},
		},
		{
			name:    "duplicate outbound queue key",
			wantErr: "merge outbound queues",
			mutate: func(t *testing.T, fixture *mergePredecessorFixture) {
				left := predecessorTestState(t, fixture.req.Previous.State)
				right := predecessorTestState(t, fixture.req.Previous2.State)
				leftQueue := predecessorTestQueueInfo(t, &left)
				rightQueue := predecessorTestQueueInfo(t, &right)
				rightQueue.OutQueue = leftQueue.OutQueue
				right.OutMsgQueueInfo = predecessorTestQueueCell(t, rightQueue)
				fixture.req.Previous2.State = predecessorTestStateCell(t, right)
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			fixture := newMergePredecessorFixture(t)
			test.mutate(t, fixture)

			_, err := preparePredecessor(fixture.req, predecessorReadSet(t, fixture.req))
			if !errors.Is(err, ErrInvalidInput) || !strings.Contains(err.Error(), test.wantErr) {
				t.Fatalf("error = %v, want ErrInvalidInput containing %q", err, test.wantErr)
			}
		})
	}
}

type splitPredecessorFixture struct {
	req            ShardRequest
	target         tlb.ShardIdent
	keptAccount    *address.Address
	removedAccount *address.Address
	keptQueue      msgpool.QueueKey
	removedQueue   msgpool.QueueKey
	records        []tlb.ProcessedUptoRecord
}

func newSplitPredecessorFixture(t *testing.T, right bool) splitPredecessorFixture {
	t.Helper()

	req := emptyCandidateRequest(t)
	parent := req.Shard
	left := groups.ShardID{Workchain: parent.Workchain, Shard: mustPredecessorChild(t, parent.Shard, true)}
	rightShard := groups.ShardID{Workchain: parent.Workchain, Shard: mustPredecessorChild(t, parent.Shard, false)}
	target, removed := left, rightShard
	keptAddress := predecessorAddress(0x11)
	removedAddress := predecessorAddress(0x91)
	keptBalance, removedBalance := uint64(11_000_000_000), uint64(22_000_000_000)
	if right {
		target, removed = rightShard, left
		keptAddress, removedAddress = removedAddress, keptAddress
		keptBalance, removedBalance = removedBalance, keptBalance
	}

	baseState := predecessorTestState(t, req.Previous.State)
	createdLT := baseState.GenLT - 100
	fee := tlb.FromNanoTONU(100_000)
	kept, keptValue := queuedInternal(t, keptAddress, removedAddress, createdLT, req.Header.GenUtime-1,
		fee, fee, 0, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})
	removedMessage, removedValue := queuedInternal(t, removedAddress, keptAddress, createdLT+1, req.Header.GenUtime-1,
		fee, fee, 0, msgpool.ShardIdent{Workchain: 0, Shard: msgpool.ShardAll})

	narrow := mustPredecessorChild(t, target.Shard, true)
	records := []tlb.ProcessedUptoRecord{
		{ShardPrefix: uint64(parent.Shard), MCSeqno: 1, LastMsgLT: 10, LastMsgHash: [32]byte{0x10}},
		{ShardPrefix: uint64(narrow), MCSeqno: 2, LastMsgLT: 20, LastMsgHash: [32]byte{0x20}},
		{ShardPrefix: uint64(removed.Shard), MCSeqno: 3, LastMsgLT: 30, LastMsgHash: [32]byte{0x30}},
	}
	processed, err := tlb.ProcessedUptoDict(records)
	if err != nil {
		t.Fatal(err)
	}
	accounts := activeContracts(t, req.Header.GenUtime,
		activeContract{address: keptAddress, code: externalAcceptCode(t), balance: keptBalance},
		activeContract{address: removedAddress, code: externalAcceptCode(t), balance: removedBalance},
	)
	stateRoot := predecessorStateRoot(t, req.Previous.State, predecessorStateOptions{
		ident:       mustPredecessorIdent(t, parent),
		seqno:       req.Previous.ID.SeqNo,
		vertSeqno:   baseState.VertSeqno,
		genUtime:    baseState.GenUTime,
		genLT:       baseState.GenLT,
		minRefMC:    baseState.MinRefMCSeqno,
		beforeSplit: true,
		accounts:    accounts,
		fees:        5,
		masterRef:   predecessorTestStats(t, &baseState).MasterRef,
		processed:   processed,
		queue: []predecessorQueueEntry{
			{key: kept.Key, value: keptValue},
			{key: removedMessage.Key, value: removedValue},
		},
	})

	req.Shard = target
	req.Previous.State = stateRoot
	queueSize := uint64(2)
	req.Previous.OutQueueSize = &queueSize
	req.Internals = &msgpool.Cut{More: true}
	req.Masterchain.Groups.Active = []groups.Session{{
		Shard:            target,
		Genesis:          []ton.BlockIDExt{*req.Previous.ID.Copy()},
		CatchainSeqno:    17,
		ValidatorSetHash: 0x10203040,
		Registered: []groups.ShardDescription{{
			Shard:       parent,
			Block:       *req.Previous.ID.Copy(),
			BeforeSplit: true,
		}},
	}}

	targetIdent := mustPredecessorIdent(t, target)
	keptCurrent := predecessorPrefix(t, keptAddress)
	keptNext := predecessorPrefix(t, removedAddress)
	if !sharddomain.Contains(int64(targetIdent.GetShardID()), int64(keptCurrent.Prefix)) ||
		sharddomain.Contains(int64(targetIdent.GetShardID()), int64(keptNext.Prefix)) {
		t.Fatal("split queue fixture does not distinguish current routing prefix from key/destination prefix")
	}

	wantRecords := []tlb.ProcessedUptoRecord{
		{ShardPrefix: uint64(target.Shard), MCSeqno: 1, LastMsgLT: 10, LastMsgHash: [32]byte{0x10}},
		{ShardPrefix: uint64(narrow), MCSeqno: 2, LastMsgLT: 20, LastMsgHash: [32]byte{0x20}},
	}
	return splitPredecessorFixture{
		req:            req,
		target:         targetIdent,
		keptAccount:    keptAddress,
		removedAccount: removedAddress,
		keptQueue:      kept.Key,
		removedQueue:   removedMessage.Key,
		records:        wantRecords,
	}
}

type mergePredecessorFixture struct {
	req            ShardRequest
	target         tlb.ShardIdent
	queueKeys      []msgpool.QueueKey
	records        []tlb.ProcessedUptoRecord
	leftGenLT      uint64
	rightGenLT     uint64
	rightGenUtime  uint32
	rightMasterRef *tlb.ExtBlkRef
}

func newMergePredecessorFixture(t *testing.T) *mergePredecessorFixture {
	t.Helper()

	req := emptyCandidateRequest(t)
	target := req.Shard
	left := groups.ShardID{Workchain: target.Workchain, Shard: mustPredecessorChild(t, target.Shard, true)}
	right := groups.ShardID{Workchain: target.Workchain, Shard: mustPredecessorChild(t, target.Shard, false)}
	baseState := predecessorTestState(t, req.Previous.State)

	leftAddress := predecessorAddress(0x11)
	rightAddress := predecessorAddress(0x91)
	leftAccounts := activeContracts(t, req.Header.GenUtime,
		activeContract{address: leftAddress, code: externalAcceptCode(t), balance: 11_000_000_000})
	rightAccounts := activeContracts(t, req.Header.GenUtime,
		activeContract{address: rightAddress, code: externalAcceptCode(t), balance: 22_000_000_000})

	fee := tlb.FromNanoTONU(100_000)
	leftMessage, leftValue := queuedInternal(t, leftAddress, predecessorAddress(0x12), baseState.GenLT-200,
		req.Header.GenUtime-1, fee, fee, 96, msgpool.ShardIdent{Workchain: 0, Shard: uint64(left.Shard)})
	rightMessage, rightValue := queuedInternal(t, rightAddress, predecessorAddress(0x92), baseState.GenLT-100,
		req.Header.GenUtime-1, fee, fee, 96, msgpool.ShardIdent{Workchain: 0, Shard: uint64(right.Shard)})

	leftRecord := tlb.ProcessedUptoRecord{
		ShardPrefix: uint64(left.Shard), MCSeqno: 10, LastMsgLT: 100, LastMsgHash: [32]byte{0x41},
	}
	rightRecord := tlb.ProcessedUptoRecord{
		ShardPrefix: uint64(right.Shard), MCSeqno: 11, LastMsgLT: 110, LastMsgHash: [32]byte{0x42},
	}
	leftProcessed, err := tlb.ProcessedUptoDict([]tlb.ProcessedUptoRecord{leftRecord})
	if err != nil {
		t.Fatal(err)
	}
	rightProcessed, err := tlb.ProcessedUptoDict([]tlb.ProcessedUptoRecord{rightRecord})
	if err != nil {
		t.Fatal(err)
	}

	leftMasterRef := testExtBlkRef(req.Masterchain.ID.SeqNo-2, 0x71)
	rightMasterRef := testExtBlkRef(req.Masterchain.ID.SeqNo-1, 0x81)
	leftGenLT := baseState.GenLT + 100
	rightGenLT := baseState.GenLT + 300
	rightGenUtime := baseState.GenUTime + 2
	leftRoot := predecessorStateRoot(t, req.Previous.State, predecessorStateOptions{
		ident:     mustPredecessorIdent(t, left),
		seqno:     100,
		vertSeqno: 2,
		genUtime:  baseState.GenUTime,
		genLT:     leftGenLT,
		minRefMC:  80,
		accounts:  leftAccounts,
		fees:      3,
		masterRef: leftMasterRef,
		processed: leftProcessed,
		queue:     []predecessorQueueEntry{{key: leftMessage.Key, value: leftValue}},
	})
	rightRoot := predecessorStateRoot(t, req.Previous.State, predecessorStateOptions{
		ident:     mustPredecessorIdent(t, right),
		seqno:     103,
		vertSeqno: 4,
		genUtime:  rightGenUtime,
		genLT:     rightGenLT,
		minRefMC:  70,
		accounts:  rightAccounts,
		fees:      4,
		masterRef: rightMasterRef,
		processed: rightProcessed,
		queue:     []predecessorQueueEntry{{key: rightMessage.Key, value: rightValue}},
	})

	leftSize, rightSize := uint64(1), uint64(1)
	req.Previous = PreviousBlock{
		ID:           testBlockID(left.Workchain, left.Shard, 100, 0x31),
		State:        leftRoot,
		OutQueueSize: &leftSize,
	}
	req.Previous2 = &PreviousBlock{
		ID:           testBlockID(right.Workchain, right.Shard, 103, 0x41),
		State:        rightRoot,
		OutQueueSize: &rightSize,
	}
	req.Header.GenUtime = rightGenUtime + 1
	req.Header.GenUtimeMS = uint64(req.Header.GenUtime) * 1_000
	req.Masterchain.GenUtime = rightGenUtime
	req.Masterchain.VertSeqno = 4
	req.Masterchain.Groups.GenUTime = rightGenUtime
	req.Masterchain.Groups.Active = []groups.Session{{
		Shard:            target,
		Genesis:          []ton.BlockIDExt{*req.Previous.ID.Copy(), *req.Previous2.ID.Copy()},
		CatchainSeqno:    17,
		ValidatorSetHash: 0x10203040,
		Registered: []groups.ShardDescription{
			{
				Shard:       left,
				Block:       *req.Previous.ID.Copy(),
				BeforeMerge: true,
			},
			{
				Shard:       right,
				Block:       *req.Previous2.ID.Copy(),
				BeforeMerge: true,
			},
		},
	}}
	req.Internals = &msgpool.Cut{More: true}

	return &mergePredecessorFixture{
		req:            req,
		target:         mustPredecessorIdent(t, target),
		queueKeys:      []msgpool.QueueKey{leftMessage.Key, rightMessage.Key},
		records:        []tlb.ProcessedUptoRecord{leftRecord, rightRecord},
		leftGenLT:      leftGenLT,
		rightGenLT:     rightGenLT,
		rightGenUtime:  rightGenUtime,
		rightMasterRef: rightMasterRef,
	}
}

type predecessorStateOptions struct {
	ident       tlb.ShardIdent
	seqno       uint32
	vertSeqno   uint32
	genUtime    uint32
	genLT       uint64
	minRefMC    uint32
	beforeSplit bool
	accounts    *tlb.ShardAccountsAugDict
	fees        uint64
	masterRef   *tlb.ExtBlkRef
	processed   *cell.Dictionary
	queue       []predecessorQueueEntry
}

type predecessorQueueEntry struct {
	key   msgpool.QueueKey
	value *cell.Cell
}

func predecessorStateRoot(t *testing.T, base *cell.Cell, options predecessorStateOptions) *cell.Cell {
	t.Helper()

	state := predecessorTestState(t, base)
	state.ShardIdent = options.ident
	state.Seqno = options.seqno
	state.VertSeqno = options.vertSeqno
	state.GenUTime = options.genUtime
	state.GenLT = options.genLT
	state.MinRefMCSeqno = options.minRefMC
	state.BeforeSplit = options.beforeSplit
	state.Accounts.ShardAccounts = options.accounts

	stats := predecessorTestStats(t, &state)
	balance, err := loadAccountsBalance(options.accounts)
	if err != nil {
		t.Fatalf("load test account balance: %v", err)
	}
	stats.OverloadHistory = 0x1234
	stats.UnderloadHistory = 0x5678
	stats.TotalBalance = balance
	stats.TotalValidatorFees = tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(options.fees)}
	stats.MasterRef = options.masterRef
	state.Stats = predecessorTestStatsCell(t, stats)

	queue, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range options.queue {
		inserted, setErr := queue.SetWithMode(predecessorQueueKey(entry.key), entry.value, cell.DictSetModeAdd)
		if setErr != nil {
			t.Fatalf("insert test queue entry: %v", setErr)
		}
		if !inserted {
			t.Fatal("test queue entry was not inserted")
		}
	}
	state.OutMsgQueueInfo = predecessorTestQueueCell(t, tlb.OutMsgQueueInfo{
		OutQueue: queue,
		ProcInfo: options.processed,
	})

	return predecessorTestStateCell(t, state)
}

func assertMechanicalSplitRoot(t *testing.T, root, left, right *cell.Cell) {
	t.Helper()

	loader := root.MustBeginParse()
	if tag := loader.MustLoadUInt(32); tag != shardStateSplitTag {
		t.Fatalf("mechanical split tag = %08x, want %08x", tag, shardStateSplitTag)
	}
	leftRef, err := loader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	rightRef, err := loader.LoadRefCell()
	if err != nil {
		t.Fatal(err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 0 || leftRef.HashKey() != left.HashKey() || rightRef.HashKey() != right.HashKey() {
		t.Fatal("mechanical split root does not contain the ordered predecessor states")
	}
}

func assertPredecessorCurrency(t *testing.T, got tlb.CurrencyCollection, nano uint64, label string) {
	t.Helper()

	want := tlb.CurrencyCollection{Coins: tlb.FromNanoTONU(nano)}
	if !got.Equals(want) {
		t.Fatalf("%s = %s, want %s", label, got.Coins, want.Coins)
	}
}

func assertPredecessorRecords(
	t *testing.T,
	dict *cell.Dictionary,
	owner uint64,
	want []tlb.ProcessedUptoRecord,
) {
	t.Helper()

	got, err := tlb.LoadProcessedUptoRecords(dict, owner)
	if err != nil {
		t.Fatalf("load processed records: %v", err)
	}
	want = tlb.CompactifyProcessedUpto(append([]tlb.ProcessedUptoRecord(nil), want...))
	if !slices.Equal(got, want) {
		t.Fatalf("processed records = %+v, want %+v", got, want)
	}
}

func assertPredecessorCount(t *testing.T, dict *cell.AugmentedDictionary, want int, label string) {
	t.Helper()

	got, err := dict.Count()
	if err != nil {
		t.Fatalf("count %s: %v", label, err)
	}
	if got != want {
		t.Fatalf("%s count = %d, want %d", label, got, want)
	}
}

func assertPredecessorKey(t *testing.T, dict *cell.AugmentedDictionary, key *cell.Cell, want bool, label string) {
	t.Helper()

	_, err := dict.LoadValue(key)
	if want && err != nil {
		t.Fatalf("%s is absent: %v", label, err)
	}
	if !want && !isMissingKey(err) {
		t.Fatalf("%s lookup error = %v, want missing key", label, err)
	}
}

func predecessorTestState(t *testing.T, root *cell.Cell) tlb.ShardStateUnsplit {
	t.Helper()

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, root); err != nil {
		t.Fatalf("decode test predecessor state: %v", err)
	}
	return state
}

func predecessorTestStateCell(t *testing.T, state tlb.ShardStateUnsplit) *cell.Cell {
	t.Helper()

	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("serialize test predecessor state: %v", err)
	}
	return root
}

func predecessorTestStats(t *testing.T, state *tlb.ShardStateUnsplit) tlb.ShardStateStats {
	t.Helper()

	var stats tlb.ShardStateStats
	if err := parseExact(&stats, state.Stats); err != nil {
		t.Fatalf("decode test predecessor stats: %v", err)
	}
	return stats
}

func predecessorTestStatsCell(t *testing.T, stats tlb.ShardStateStats) *cell.Cell {
	t.Helper()

	root, err := stats.ToCell()
	if err != nil {
		t.Fatalf("serialize test predecessor stats: %v", err)
	}
	return root
}

func predecessorTestQueueInfo(t *testing.T, state *tlb.ShardStateUnsplit) tlb.OutMsgQueueInfo {
	t.Helper()

	var queue tlb.OutMsgQueueInfo
	if err := parseExact(&queue, state.OutMsgQueueInfo); err != nil {
		t.Fatalf("decode test predecessor queue: %v", err)
	}
	return queue
}

func predecessorTestQueueCell(t *testing.T, queue tlb.OutMsgQueueInfo) *cell.Cell {
	t.Helper()

	root, err := queue.ToCell()
	if err != nil {
		t.Fatalf("serialize test predecessor queue: %v", err)
	}
	return root
}

func mustPredecessorIdent(t *testing.T, id groups.ShardID) tlb.ShardIdent {
	t.Helper()

	ident, err := topologyShardIdent(id)
	if err != nil {
		t.Fatalf("topologyShardIdent(%+v): %v", id, err)
	}
	return ident
}

func mustPredecessorChild(t *testing.T, parent int64, left bool) int64 {
	t.Helper()

	child, err := sharddomain.Child(parent, left)
	if err != nil {
		t.Fatalf("shard.Child(%016x, %t): %v", uint64(parent), left, err)
	}
	return child
}

func predecessorAddress(prefix byte) *address.Address {
	return address.NewAddress(0, 0, bytes.Repeat([]byte{prefix}, 32))
}

func predecessorPrefix(t *testing.T, addr *address.Address) msgpool.AccountPrefix {
	t.Helper()

	prefix, err := accountPrefixFromAddress(addr)
	if err != nil {
		t.Fatalf("account prefix: %v", err)
	}
	return prefix
}

func predecessorAccountKey(addr *address.Address) *cell.Cell {
	return cell.BeginCell().MustStoreSlice(addr.Data(), 256).EndCell()
}

func predecessorQueueKey(key msgpool.QueueKey) *cell.Cell {
	return cell.BeginCell().MustStoreSlice(key[:], 352).EndCell()
}

// predecessorReadSet opens the recorder the collation opens for this request, so
// tests drive preparePredecessor over the same root the state update is built
// from — the split cell rather than either parent after a merge.
func predecessorReadSet(t *testing.T, req ShardRequest) *cell.ReadSet {
	t.Helper()
	read, err := openPredecessorReadSet(req, 0)
	if err != nil {
		t.Fatalf("open predecessor read set: %v", err)
	}
	return read
}
