package collator

import (
	"context"
	"crypto/sha256"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
	stackop "github.com/xssnick/tonutils-go/tvm/op/stack"
)

func TestAfterMergeFullCollatedProofCarriesChangedAccount(t *testing.T) {
	fixture := newMergePredecessorFixture(t)
	req := fixture.req
	bindSyntheticPredecessorBlock(t, &req.Previous)
	bindSyntheticPredecessorBlock(t, req.Previous2)

	session := &req.Masterchain.Groups.Active[0]
	session.Genesis = []ton.BlockIDExt{*req.Previous.ID.Copy(), *req.Previous2.ID.Copy()}
	session.Registered[0].Block = *req.Previous.ID.Copy()
	session.Registered[1].Block = *req.Previous2.ID.Copy()
	req.Masterchain.Config.capabilities |= capFullCollatedData

	leftNeighbor := fullCollatedTestNeighbor(t, req.Previous)
	rightNeighbor := fullCollatedTestNeighbor(t, *req.Previous2)
	req.Neighbors = append([]Neighbor{leftNeighbor, rightNeighbor}, masterchainNeighbor(req)...)
	req.NeighborShardEndLT = func(_ uint32, workchain int32, shard uint64) uint64 {
		switch {
		case workchain == leftNeighbor.Shard.Workchain && shard == leftNeighbor.Shard.Shard:
			return leftNeighbor.EndLT
		case workchain == rightNeighbor.Shard.Workchain && shard == rightNeighbor.Shard.Shard:
			return rightNeighbor.EndLT
		default:
			return req.Masterchain.EndLT
		}
	}

	changed := predecessorAddress(0x11)
	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: changed,
		Body:    cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, external)}

	c, candidate := finishAccountReceiptCandidate(t, req)
	if c.accountMutationDiff == nil {
		t.Fatal("full-collated merge did not retain the bulk account mutation receipt")
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.ExternalIncluded != 1 {
		t.Fatalf("merge transaction stats = %+v", candidate.Stats)
	}

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	verification.Previous.State = narrowedStateRoot(t, req.Previous.State)
	narrowedSecond := *req.Previous2
	narrowedSecond.State = narrowedStateRoot(t, req.Previous2.State)
	verification.Previous2 = &narrowedSecond
	verification.Neighbors = collatedNeighborQueues(t, req, candidate)
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify full-collated merge candidate on its own proof: %v", err)
	}
}

func TestDestroyedAccountFinishUsesCanonicalDiffProof(t *testing.T) {
	req := emptyCandidateRequest(t)
	destroyed := predecessorAddress(0x40)
	deleteCode := externalDeleteCode(t, predecessorAddress(0xf0))
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(
		t,
		req.Header.GenUtime,
		activeContract{address: predecessorAddress(0x00), code: externalAcceptCode(t), balance: 10_000_000_000},
		activeContract{address: destroyed, code: deleteCode, balance: 100_000_000_000},
		activeContract{address: predecessorAddress(0x80), code: externalAcceptCode(t), balance: 20_000_000_000},
	))
	req = advanceCandidateRequest(t, req)
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	external, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: destroyed,
		Body:    cell.BeginCell().MustStoreUInt(0x5678, 16).EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, external)}

	c, candidate := finishAccountReceiptCandidate(t, req)
	if c.accountMutationDiff != nil {
		t.Fatal("destroyed account retained a bulk receipt instead of selecting the canonical diff")
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.ExternalIncluded != 1 {
		t.Fatalf("destroyed-account transaction stats = %+v", candidate.Stats)
	}

	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, candidate.State); err != nil {
		t.Fatal(err)
	}
	if _, err = state.Accounts.ShardAccounts.LoadValue(predecessorAccountKey(destroyed)); !isMissingKey(err) {
		t.Fatalf("destroyed account remains in candidate state: %v", err)
	}

	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	verification.Previous.State = narrowedStateRoot(t, req.Previous.State)
	verification.Neighbors = collatedNeighborQueues(t, req, candidate)
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify destroyed-account candidate on canonical diff proof: %v", err)
	}
}

func finishAccountReceiptCandidate(tb testing.TB, req ShardRequest) (*collation, *Candidate) {
	tb.Helper()

	builder := testBuilder()
	c, err := builder.prepareShardPhases(context.Background(), req, 0)
	if err != nil {
		tb.Fatalf("prepare account-receipt collation: %v", err)
	}
	if err = c.processExternals(); err != nil {
		tb.Fatalf("execute account-receipt transaction: %v", err)
	}
	if err = c.processNewMessages(c.blockFull || c.haveUnprocessedDispatchQueue || req.internalsIncomplete()); err != nil {
		tb.Fatalf("process account-receipt outputs: %v", err)
	}
	if err = c.updateProcessedInfo(); err != nil {
		tb.Fatal(err)
	}
	if err = c.cleanupClaimedLocalDequeues(); err != nil {
		tb.Fatal(err)
	}
	if err = c.finishAccounts(); err != nil {
		tb.Fatalf("finish account receipt: %v", err)
	}

	candidate, err := c.finish()
	if err != nil {
		tb.Fatalf("finish account-receipt candidate: %v", err)
	}

	return c, candidate
}

func externalDeleteCode(tb testing.TB, destination *address.Address) *cell.Cell {
	tb.Helper()

	message, err := tlb.ToCell(&tlb.InternalMessage{
		IHRDisabled: true,
		SrcAddr:     address.NewAddressNone(),
		DstAddr:     destination,
		Amount:      tlb.FromNanoTONU(0),
		Body:        cell.BeginCell().EndCell(),
	})
	if err != nil {
		tb.Fatal(err)
	}

	code := externalAcceptCode(tb).ToBuilder()
	for _, instruction := range []*cell.Builder{
		stackop.PUSHREF(message).Serialize(),
		stackop.PUSHINT(big.NewInt(160)).Serialize(),
		funcsop.SENDRAWMSG().Serialize(),
	} {
		if err = code.StoreBuilder(instruction); err != nil {
			tb.Fatal(err)
		}
	}
	return code.EndCell()
}

func bindSyntheticPredecessorBlock(tb testing.TB, previous *PreviousBlock) {
	tb.Helper()

	base, err := testBuilder().BuildShard(context.Background(), emptyCandidateRequest(tb))
	if err != nil {
		tb.Fatal(err)
	}
	var block tlb.Block
	if err = parseExact(&block, candidateBlock(tb, base)); err != nil {
		tb.Fatal(err)
	}
	var state tlb.ShardStateUnsplit
	if err = parseExact(&state, previous.State); err != nil {
		tb.Fatal(err)
	}
	block.GlobalID = state.GlobalID
	block.BlockInfo.NotMaster = true
	block.BlockInfo.Shard = state.ShardIdent
	block.BlockInfo.SeqNo = previous.ID.SeqNo
	block.BlockInfo.VertSeqNo = state.VertSeqno
	block.BlockInfo.GenUtime = state.GenUTime
	block.StateUpdate, err = cell.CreateMerkleUpdate(cell.BeginCell().EndCell(), previous.State)
	if err != nil {
		tb.Fatal(err)
	}
	root, err := tlb.ToCell(&block)
	if err != nil {
		tb.Fatal(err)
	}
	boc, err := root.ToBOCWithOptionsErr(cell.BOCSerializeOptions{})
	if err != nil {
		tb.Fatal(err)
	}
	fileHash := sha256.Sum256(boc)
	previous.ID.RootHash = root.Hash()
	previous.ID.FileHash = fileHash[:]
	previous.Block = root
}
