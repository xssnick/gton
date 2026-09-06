package validator

import (
	"bytes"
	"crypto/sha256"
	"errors"
	"fmt"
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"

	sharddomain "github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"
	"github.com/xssnick/gton/service/validator/collator"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/gton/service/validator/simplex"
)

type acceptanceTestPublisher struct {
	blocks []AcceptedBlockPublication
}

func (p *acceptanceTestPublisher) PublishAcceptedBlock(block AcceptedBlockPublication) {
	p.blocks = append(p.blocks, block)
}

func TestBlockAccepterPublishesAndInstallsFinalShardTop(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: sharddomain.Root})
	fixture.config.Identity.Validator = &ValidatorIdentity{Index: 0}
	node := &acceptanceTestNode{}
	publisher := &acceptanceTestPublisher{}
	inbox, err := collator.NewShardTopInbox(collator.ShardTopInboxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config:    fixture.config,
		Node:      node,
		Publisher: publisher,
		ShardTops: inbox,
	})
	if err != nil {
		t.Fatal(err)
	}

	acceptance := fixture.acceptance(simplex.VoteFinalize, true)
	if err = accepter.acceptForTest(t.Context(), acceptance, acceptanceTestViewResolver(fixture.view(t))); err != nil {
		t.Fatal(err)
	}
	if len(node.blocks) != 1 || len(publisher.blocks) != 1 {
		t.Fatalf(
			"local/published counts = %d/%d, want 1/1",
			len(node.blocks),
			len(publisher.blocks),
		)
	}
	if inbox.Len() != 1 {
		t.Fatalf("local shard top inbox size = %d, want 1", inbox.Len())
	}
	if !publisher.blocks[0].Signatures.Final() {
		t.Fatal("published accepted block does not carry final signatures")
	}
	if !publisher.blocks[0].Public {
		t.Fatal("scheduled local leader did not mark its full block for public publication")
	}
	if publisher.blocks[0].SessionID != fixture.config.SessionID {
		t.Fatal("accepted block publication lost its session identity")
	}
}

func TestBlockAccepterReplaysOnlyToLocalIngress(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: -1, Shard: sharddomain.Root})
	node := &acceptanceTestNode{}
	publisher := &acceptanceTestPublisher{}
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config:    fixture.config,
		Node:      node,
		Publisher: publisher,
	})
	if err != nil {
		t.Fatal(err)
	}

	acceptance := fixture.acceptance(simplex.VoteFinalize, false)
	acceptance.Replay = true
	if err = accepter.acceptForTest(t.Context(), acceptance, acceptanceTestViewResolver(BlockAcceptanceView{})); err != nil {
		t.Fatal(err)
	}
	if len(node.blocks) != 1 {
		t.Fatalf("locally submitted blocks = %d, want 1", len(node.blocks))
	}
	if len(publisher.blocks) != 0 {
		t.Fatalf("published replayed blocks = %d, want 0", len(publisher.blocks))
	}
}

func TestBlockAccepterBuildsShardTopWithHistoricalMasterchainAnchor(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: sharddomain.Root})
	reference := fixture.masterchainBlock(t)
	view := fixture.view(t)
	view.MasterchainBlock = acceptanceTestBlockID(
		groups.ShardID{Workchain: -1, Shard: sharddomain.Root},
		reference.SeqNo+5,
		0xa1,
	)
	node := &acceptanceTestNode{history: map[storage.BlockSeqRef]ton.BlockIDExt{
		{
			Workchain: -1,
			Shard:     sharddomain.Root,
			SeqNo:     reference.SeqNo,
		}: reference,
	}}
	inbox, err := collator.NewShardTopInbox(collator.ShardTopInboxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config:    fixture.config,
		Node:      node,
		ShardTops: inbox,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = accepter.acceptForTest(
		t.Context(),
		fixture.acceptance(simplex.VoteFinalize, false),
		acceptanceTestViewResolver(view),
	); err != nil {
		t.Fatalf("accept shard top with historical masterchain anchor: %v", err)
	}
	if inbox.Len() != 1 {
		t.Fatalf("local shard top inbox size = %d, want 1", inbox.Len())
	}
}

func TestBlockAccepterRejectsWrongShardTopMasterchainAnchor(t *testing.T) {
	t.Run("same height", func(t *testing.T) {
		fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: sharddomain.Root})
		reference := fixture.masterchainBlock(t)
		view := fixture.view(t)
		view.MasterchainBlock = acceptanceTestBlockID(
			groups.ShardID{Workchain: -1, Shard: sharddomain.Root},
			reference.SeqNo,
			0xb1,
		)
		publisher := &acceptanceTestPublisher{}
		accepter, err := NewBlockAccepter(BlockAccepterOptions{
			Config:    fixture.config,
			Node:      &acceptanceTestNode{},
			Publisher: publisher,
		})
		if err != nil {
			t.Fatal(err)
		}

		err = accepter.acceptForTest(t.Context(), fixture.acceptance(simplex.VoteFinalize, false), acceptanceTestViewResolver(view))
		if err == nil || !strings.Contains(err.Error(), "reference differs from masterchain view") {
			t.Fatalf("same-height masterchain anchor error = %v", err)
		}
	})

	t.Run("history", func(t *testing.T) {
		fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: sharddomain.Root})
		reference := fixture.masterchainBlock(t)
		view := fixture.view(t)
		view.MasterchainBlock = acceptanceTestBlockID(
			groups.ShardID{Workchain: -1, Shard: sharddomain.Root},
			reference.SeqNo+5,
			0xc1,
		)
		node := &acceptanceTestNode{history: map[storage.BlockSeqRef]ton.BlockIDExt{
			{
				Workchain: -1,
				Shard:     sharddomain.Root,
				SeqNo:     reference.SeqNo,
			}: acceptanceTestBlockID(
				groups.ShardID{Workchain: -1, Shard: sharddomain.Root},
				reference.SeqNo,
				0xc2,
			),
		}}
		publisher := &acceptanceTestPublisher{}
		accepter, err := NewBlockAccepter(BlockAccepterOptions{
			Config:    fixture.config,
			Node:      node,
			Publisher: publisher,
		})
		if err != nil {
			t.Fatal(err)
		}

		err = accepter.acceptForTest(t.Context(), fixture.acceptance(simplex.VoteFinalize, false), acceptanceTestViewResolver(view))
		if err == nil || !strings.Contains(err.Error(), "masterchain history contains another block") {
			t.Fatalf("historical masterchain anchor error = %v", err)
		}
	})
}

// The predecessor link comes from the node's own storage, the only place it is
// kept. Accepting it locally is asynchronous, so the test stores it the way the
// node pipeline eventually does.
func TestBlockAccepterBuildsShardTopFromNodeStoredPredecessor(t *testing.T) {
	first := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: sharddomain.Root})
	second := acceptanceTestSuccessorFixture(t, first, 3, first.candidate.Candidate.Block)
	node := &acceptanceTestNode{proofs: make(map[string][]byte)}
	publisher := &acceptanceTestPublisher{}
	inbox, err := collator.NewShardTopInbox(collator.ShardTopInboxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config:    first.config,
		Node:      node,
		Publisher: publisher,
		ShardTops: inbox,
	})
	if err != nil {
		t.Fatal(err)
	}

	if err = accepter.acceptForTest(
		t.Context(),
		first.acceptance(simplex.VoteNotarize, false),
		acceptanceTestViewResolver(BlockAcceptanceView{}),
	); err != nil {
		t.Fatalf("accept notarized predecessor: %v", err)
	}
	predecessor, err := accepter.Prepare(t.Context(), first.acceptance(simplex.VoteNotarize, false), nil)
	if err != nil {
		t.Fatal(err)
	}
	node.proofs[storage.FormatBlockRef(first.candidate.Candidate.Block)] = predecessor.block.ProofBOC
	prepared, err := accepter.Prepare(t.Context(), second.acceptance(simplex.VoteFinalize, false), nil)
	if err != nil {
		t.Fatal(err)
	}
	anchor, err := resolveBlockAcceptanceAnchor(first.config.Shard, first.view(t))
	if err != nil {
		t.Fatal(err)
	}
	links, err := accepter.loadAcceptedProofChain(t.Context(), anchor, prepared)
	if err != nil {
		t.Fatal(err)
	}
	if len(links) != 2 || !links[1].block.Equals(&first.candidate.Candidate.Block) {
		t.Fatalf("proof chain = %+v", links)
	}

	if err = accepter.acceptForTest(
		t.Context(),
		second.acceptance(simplex.VoteFinalize, false),
		acceptanceTestViewResolver(first.view(t)),
	); err != nil {
		t.Fatalf("accept final successor: %v", err)
	}
	if inbox.Len() != 1 {
		t.Fatalf("local shard top inbox size = %d, want 1", inbox.Len())
	}
}

func TestAcceptedMasterchainAncestorWaitsForNewerView(t *testing.T) {
	masterchain := acceptanceTestBlockID(groups.ShardID{Workchain: -1, Shard: sharddomain.Root}, 10, 0x51)
	reference := acceptanceTestBlockID(groups.ShardID{Workchain: -1, Shard: sharddomain.Root}, 11, 0x61)

	err := validateAcceptedMasterchainAncestor(t.Context(), &acceptanceTestNode{}, masterchain, reference)
	if !errors.Is(err, ErrBlockNotReady) {
		t.Fatalf("newer masterchain reference error = %v, want ErrBlockNotReady", err)
	}
}

func TestBlockAccepterRetriesMissingProofWithoutRepublishing(t *testing.T) {
	first := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: sharddomain.Root})
	second := acceptanceTestSuccessorFixture(t, first, 3, first.candidate.Candidate.Block)
	node := &acceptanceTestNode{proofs: make(map[string][]byte)}
	publisher := &acceptanceTestPublisher{}
	inbox, err := collator.NewShardTopInbox(collator.ShardTopInboxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config:    first.config,
		Node:      node,
		Publisher: publisher,
		ShardTops: inbox,
	})
	if err != nil {
		t.Fatal(err)
	}
	acceptance := second.acceptance(simplex.VoteFinalize, false)

	pending, err := accepter.Prepare(t.Context(), acceptance, acceptanceTestViewResolver(first.view(t)))
	if err != nil {
		t.Fatal(err)
	}
	if err = pending.Submit(t.Context()); err != nil {
		t.Fatal(err)
	}
	err = pending.Describe(t.Context())
	if !errors.Is(err, ErrBlockNotReady) {
		t.Fatalf("missing predecessor error = %v, want ErrBlockNotReady", err)
	}
	if len(node.blocks) != 1 || len(publisher.blocks) != 1 || inbox.Len() != 0 {
		t.Fatalf(
			"first attempt local/published/top counts = %d/%d/%d",
			len(node.blocks),
			len(publisher.blocks),
			inbox.Len(),
		)
	}
	prepared, err := accepter.Prepare(t.Context(), first.acceptance(simplex.VoteNotarize, false), nil)
	if err != nil {
		t.Fatal(err)
	}
	node.proofs[storage.FormatBlockRef(first.candidate.Candidate.Block)] = prepared.block.ProofBOC
	if err = pending.Describe(t.Context()); err != nil {
		t.Fatalf("retry acceptance: %v", err)
	}
	if len(node.blocks) != 1 || len(publisher.blocks) != 1 || inbox.Len() != 1 {
		t.Fatalf(
			"retry local/published/top counts = %d/%d/%d, want 1/1/1",
			len(node.blocks),
			len(publisher.blocks),
			inbox.Len(),
		)
	}
}

func TestBlockAccepterPublishesWhenFirstAcceptanceViewIsNotReady(t *testing.T) {
	fixture := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: sharddomain.Root})
	node := &acceptanceTestNode{}
	publisher := &acceptanceTestPublisher{}
	inbox, err := collator.NewShardTopInbox(collator.ShardTopInboxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config:    fixture.config,
		Node:      node,
		Publisher: publisher,
		ShardTops: inbox,
	})
	if err != nil {
		t.Fatal(err)
	}

	// The group tracker has no usable snapshot yet, exactly as at node start and
	// during split or merge reconciliation.
	ready := false
	view := fixture.view(t)
	resolveView := func() (BlockAcceptanceView, error) {
		if !ready {
			return BlockAcceptanceView{}, fmt.Errorf("%w: test view is not ready", ErrBlockNotReady)
		}

		return view, nil
	}

	acceptance := fixture.acceptance(simplex.VoteFinalize, false)
	pending, err := accepter.Prepare(t.Context(), acceptance, resolveView)
	if err != nil {
		t.Fatal(err)
	}
	if err = pending.Submit(t.Context()); err != nil {
		t.Fatal(err)
	}
	err = pending.Describe(t.Context())
	if !errors.Is(err, ErrBlockNotReady) {
		t.Fatalf("unresolved acceptance view error = %v, want ErrBlockNotReady", err)
	}
	if len(node.blocks) != 1 || len(publisher.blocks) != 1 || inbox.Len() != 0 {
		t.Fatalf(
			"first attempt local/published/top counts = %d/%d/%d, want 1/1/0",
			len(node.blocks),
			len(publisher.blocks),
			inbox.Len(),
		)
	}

	ready = true
	if err = pending.Describe(t.Context()); err != nil {
		t.Fatalf("retry acceptance: %v", err)
	}
	if len(node.blocks) != 1 || len(publisher.blocks) != 1 || inbox.Len() != 1 {
		t.Fatalf(
			"retry local/published/top counts = %d/%d/%d, want 1/1/1",
			len(node.blocks),
			len(publisher.blocks),
			inbox.Len(),
		)
	}
}

func TestBlockAccepterRejectsShardTopLongerThanEightProofs(t *testing.T) {
	base := newAcceptanceTestFixture(t, groups.ShardID{Workchain: 0, Shard: sharddomain.Root})
	anchor := base.view(t)
	anchor.Registered[0].Block.SeqNo = 1
	long := acceptanceTestSuccessorFixture(t, base, 10, acceptanceTestBlockID(
		base.config.Shard,
		9,
		0x91,
	))
	node := &acceptanceTestNode{}
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config: base.config,
		Node:   node,
	})
	if err != nil {
		t.Fatal(err)
	}

	err = accepter.acceptForTest(t.Context(), long.acceptance(simplex.VoteFinalize, false), acceptanceTestViewResolver(anchor))
	if err == nil || !strings.Contains(err.Error(), "more than eight") {
		t.Fatalf("long shard proof chain error = %v", err)
	}
}

func TestBlockAccepterBuildsShardTopAcrossSplitAndMerge(t *testing.T) {
	left, err := sharddomain.Child(sharddomain.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := sharddomain.Child(sharddomain.Root, false)
	if err != nil {
		t.Fatal(err)
	}

	t.Run("split", func(t *testing.T) {
		target := groups.ShardID{Workchain: 0, Shard: left}
		parent := acceptanceTestBlockID(groups.ShardID{Workchain: 0, Shard: sharddomain.Root}, 1, 0x71)
		fixture := newAcceptanceTestFixtureWithHeader(t, target, func(header *tlb.BlockHeader) {
			header.AfterSplit = true
			header.PrevRef.Prev1 = acceptanceTestRefFromBlock(parent)
		})
		view := BlockAcceptanceView{
			MasterchainBlock: fixture.masterchainBlock(t),
			Registered: []groups.ShardDescription{{
				Shard:       groups.ShardID{Workchain: 0, Shard: sharddomain.Root},
				Block:       parent,
				BeforeSplit: true,
			}},
		}
		requireAcceptanceTestTop(t, fixture, view)
	})

	t.Run("merge", func(t *testing.T) {
		target := groups.ShardID{Workchain: 0, Shard: sharddomain.Root}
		leftBlock := acceptanceTestBlockID(groups.ShardID{Workchain: 0, Shard: left}, 1, 0x81)
		rightBlock := acceptanceTestBlockID(groups.ShardID{Workchain: 0, Shard: right}, 1, 0x82)
		fixture := newAcceptanceTestFixtureWithHeader(t, target, func(header *tlb.BlockHeader) {
			header.AfterMerge = true
			header.PrevRef.Prev1 = acceptanceTestRefFromBlock(leftBlock)
			second := acceptanceTestRefFromBlock(rightBlock)
			header.PrevRef.Prev2 = &second
		})
		view := BlockAcceptanceView{
			MasterchainBlock: fixture.masterchainBlock(t),
			Registered: []groups.ShardDescription{
				{Shard: groups.ShardID{Workchain: 0, Shard: left}, Block: leftBlock, BeforeMerge: true},
				{Shard: groups.ShardID{Workchain: 0, Shard: right}, Block: rightBlock, BeforeMerge: true},
			},
		}
		requireAcceptanceTestTop(t, fixture, view)
	})
}

func requireAcceptanceTestTop(
	t *testing.T,
	fixture acceptanceTestFixture,
	view BlockAcceptanceView,
) {
	t.Helper()

	inbox, err := collator.NewShardTopInbox(collator.ShardTopInboxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	accepter, err := NewBlockAccepter(BlockAccepterOptions{
		Config:    fixture.config,
		Node:      &acceptanceTestNode{},
		ShardTops: inbox,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = accepter.acceptForTest(t.Context(), fixture.acceptance(simplex.VoteFinalize, false), acceptanceTestViewResolver(view)); err != nil {
		t.Fatal(err)
	}
	if inbox.Len() != 1 {
		t.Fatalf("local shard top inbox size = %d, want 1", inbox.Len())
	}
}

func acceptanceTestSuccessorFixture(
	t *testing.T,
	base acceptanceTestFixture,
	seqno uint32,
	previous ton.BlockIDExt,
) acceptanceTestFixture {
	t.Helper()

	root := acceptanceTestBlockRoot(
		t,
		base.config.Shard,
		base.config.CatchainSeqno,
		base.config.ValidatorSetHash,
		func(header *tlb.BlockHeader) {
			header.SeqNo = seqno
			header.PrevRef.Prev1 = acceptanceTestRefFromBlock(previous)
		},
	)
	blockBOC := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: true})
	rootHash := root.HashKey()
	fileHash := sha256.Sum256(blockBOC)
	candidate := simplex.Candidate{
		Parent:           simplex.Genesis(),
		Leader:           0,
		Block:            ton.BlockIDExt{Workchain: base.config.Shard.Workchain, Shard: base.config.Shard.Shard, SeqNo: seqno, RootHash: rootHash[:], FileHash: fileHash[:]},
		CollatedFileHash: sha256.Sum256([]byte("successor collated data")),
	}
	candidate.ID = candidate.ComputeID(seqno + 1)

	base.candidate = &CandidateArtifact{Candidate: candidate, BlockBOC: blockBOC}
	return base
}

func acceptanceTestBlockID(shard groups.ShardID, seqno uint32, marker byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: shard.Workchain,
		Shard:     shard.Shard,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{marker}, 32),
		FileHash:  bytes.Repeat([]byte{marker + 1}, 32),
	}
}

func acceptanceTestRefFromBlock(block ton.BlockIDExt) tlb.ExtBlkRef {
	return tlb.ExtBlkRef{
		EndLt:    1,
		SeqNo:    block.SeqNo,
		RootHash: bytes.Clone(block.RootHash),
		FileHash: bytes.Clone(block.FileHash),
	}
}
