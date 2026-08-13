package collator

import (
	"context"
	"math/big"
	"testing"

	"github.com/xssnick/gton/service/p2p"
	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type shardTopInboxTestProvider struct {
	sets         ShardTopValidatorSets
	setsErr      error
	ready        bool
	readyBlocks  map[uint32]bool
	readyCheck   func(context.Context, ton.BlockIDExt) (bool, error)
	ancestor     bool
	ancestorErr  error
	ancestorCall int
	setsCall     int
	readyCall    int
}

func (p *shardTopInboxTestProvider) ValidatorSets(
	context.Context,
	ton.BlockIDExt,
	ton.BlockIDExt,
) (ShardTopValidatorSets, error) {
	p.setsCall++
	if p.setsErr != nil {
		return ShardTopValidatorSets{}, p.setsErr
	}

	return p.sets, nil
}

func (p *shardTopInboxTestProvider) IsMasterchainAncestor(
	_ context.Context,
	_, _ ton.BlockIDExt,
) (bool, error) {
	p.ancestorCall++
	if p.ancestorErr != nil {
		return false, p.ancestorErr
	}

	return p.ancestor, nil
}

func (p *shardTopInboxTestProvider) ShardTopReady(ctx context.Context, block ton.BlockIDExt) (bool, error) {
	p.readyCall++
	if p.readyCheck != nil {
		return p.readyCheck(ctx, block)
	}
	if p.readyBlocks != nil {
		return p.readyBlocks[block.SeqNo], nil
	}
	return p.ready, nil
}

func TestShardTopInboxSelectsLinearTipWithoutMutatingRegistry(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 10, 0x10)
	registry := shardTopInboxTestRegistry(t, oldBlock, masterShardTestDescriptorOptions{
		genUtime:          90,
		nextCatchainSeqno: 7,
	})
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xa0)
	block := masterShardTestBlock(0, shard.Root, 11, 0x11)
	description, root := shardTopInboxTestDescription(t, block, []ton.BlockIDExt{oldBlock}, masterchain, 7, 77, 100)
	description.Chain[0].FeesCollected = shardTopInboxTestCurrency(9)
	description.Chain[0].FundsCreated = shardTopInboxTestCurrency(4)
	description.Chain[0].CreatedBy[0] = 0xcc

	// The decoded proof need not have Go pointer identity with the proof held by
	// the outer root. Its representation hash is the protocol identity.
	proofCopy, err := cell.FromBOC(description.Chain[0].TopBlockProof.ToBOC())
	if err != nil {
		t.Fatal(err)
	}
	description.Chain[0].TopBlockProof = proofCopy

	inbox := shardTopInboxTestNew(t)
	if err = inbox.StoreShardTopDescription(context.Background(), description, root); err != nil {
		t.Fatal(err)
	}
	provider := &shardTopInboxTestProvider{
		sets: ShardTopValidatorSets{Current: ShardTopValidatorSet{
			CatchainSeqno:    7,
			ValidatorSetHash: 77,
		}},
		ready: true,
	}
	tops, err := inbox.Select(context.Background(), shardTopInboxTestSelection(masterchain, registry, provider, 101, 12))
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 1 {
		t.Fatalf("selected top count = %d, want 1", len(tops))
	}
	top := tops[0]
	if !sameShardBlock(top.Block, block) || len(top.Predecessors) != 1 ||
		!sameShardBlock(top.Predecessors[0], oldBlock) {
		t.Fatalf("selected top = %+v, predecessors = %+v", top.Block, top.Predecessors)
	}
	if top.TopBlockDescr != root {
		t.Fatal("selection did not preserve the exact outer TopBlockDescr root")
	}
	if len(top.Creators) != 1 || top.Creators[0][0] != 0xcc {
		t.Fatalf("creators = %x, want the accepted link creator", top.Creators)
	}
	fields, err := parseShardDescriptorFields(top.Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if fields.fees.Coins.Nano().Int64() != 9 || fields.created.Coins.Nano().Int64() != 4 {
		t.Fatalf("projected fees = %s/%s, want 9/4", fields.fees.Coins.Nano(), fields.created.Coins.Nano())
	}
	registryTop := registry.Tops()[0]
	if !sameShardBlock(registryTop.Block, oldBlock) {
		t.Fatal("Select mutated the input registry")
	}
}

func TestShardTopInboxMaterializesPersistentReadyForEveryProvider(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 10, 0x12)
	block := masterShardTestBlock(0, shard.Root, 11, 0x13)
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xaf)
	registry := shardTopInboxTestRegistry(t, oldBlock, masterShardTestDescriptorOptions{nextCatchainSeqno: 7})
	description, root := shardTopInboxTestDescription(
		t, block, []ton.BlockIDExt{oldBlock}, masterchain, 7, 77, 99,
	)
	inbox := shardTopInboxTestNew(t)
	if err := inbox.StoreShardTopDescription(context.Background(), description, root); err != nil {
		t.Fatal(err)
	}

	first := shardTopInboxTestReadyProvider(7, 77)
	firstSelection := shardTopInboxTestSelection(masterchain, registry, first, 100, 13)
	firstTops, err := inbox.Select(context.Background(), firstSelection)
	if err != nil {
		t.Fatal(err)
	}
	if len(firstTops) != 1 || first.readyCall != 1 {
		t.Fatalf("first provider selection=%d readiness_calls=%d, want 1/1", len(firstTops), first.readyCall)
	}

	second := shardTopInboxTestReadyProvider(7, 77)
	secondSelection := shardTopInboxTestSelection(masterchain, registry, second, 100, 13)
	secondTops, err := inbox.Select(context.Background(), secondSelection)
	if err != nil {
		t.Fatal(err)
	}
	if len(secondTops) != 1 || second.readyCall != 1 {
		t.Fatalf("second provider selection=%d readiness_calls=%d, want 1/1", len(secondTops), second.readyCall)
	}
}

func TestShardTopInboxDefersFutureMasterchainReference(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 10, 0x20)
	registry := shardTopInboxTestRegistry(t, oldBlock, masterShardTestDescriptorOptions{nextCatchainSeqno: 7})
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xa1)
	futureMasterchain := masterShardTestBlock(-1, shard.Root, 101, 0xa2)
	block := masterShardTestBlock(0, shard.Root, 11, 0x21)
	description, root := shardTopInboxTestDescription(t, block, []ton.BlockIDExt{oldBlock}, futureMasterchain, 7, 77, 99)

	inbox := shardTopInboxTestNew(t)
	if err := inbox.StoreShardTopDescription(context.Background(), description, root); err != nil {
		t.Fatal(err)
	}
	provider := shardTopInboxTestReadyProvider(7, 77)
	tops, err := inbox.Select(context.Background(), shardTopInboxTestSelection(masterchain, registry, provider, 100, 13))
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 0 || inbox.Len() != 1 {
		t.Fatalf("future descriptor selected/removed: tops=%d inbox=%d", len(tops), inbox.Len())
	}
	if provider.ancestorCall != 0 || provider.setsCall != 0 || provider.readyCall != 1 {
		t.Fatalf("future descriptor reached provider: ancestor=%d sets=%d ready=%d",
			provider.ancestorCall, provider.setsCall, provider.readyCall)
	}
}

func TestShardTopInboxDiscardsEqualHeightMasterchainFork(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 10, 0x30)
	registry := shardTopInboxTestRegistry(t, oldBlock, masterShardTestDescriptorOptions{nextCatchainSeqno: 7})
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xa3)
	fork := masterShardTestBlock(-1, shard.Root, 100, 0xa4)
	block := masterShardTestBlock(0, shard.Root, 11, 0x31)
	description, root := shardTopInboxTestDescription(t, block, []ton.BlockIDExt{oldBlock}, fork, 7, 77, 99)

	inbox := shardTopInboxTestNew(t)
	if err := inbox.StoreShardTopDescription(context.Background(), description, root); err != nil {
		t.Fatal(err)
	}
	provider := shardTopInboxTestReadyProvider(7, 77)
	tops, err := inbox.Select(context.Background(), shardTopInboxTestSelection(masterchain, registry, provider, 100, 13))
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 0 || inbox.Len() != 0 {
		t.Fatalf("fork descriptor retained/selected: tops=%d inbox=%d", len(tops), inbox.Len())
	}
	if provider.ancestorCall != 0 || provider.setsCall != 0 || provider.readyCall != 1 {
		t.Fatal("equal-height fork reached later provider checks")
	}
}

func TestShardTopInboxDefersNextValidatorSetAndUnavailableData(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 10, 0x40)
	registry := shardTopInboxTestRegistry(t, oldBlock, masterShardTestDescriptorOptions{nextCatchainSeqno: 8})
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xa5)
	block := masterShardTestBlock(0, shard.Root, 11, 0x41)
	description, root := shardTopInboxTestDescription(t, block, []ton.BlockIDExt{oldBlock}, masterchain, 8, 88, 99)

	inbox := shardTopInboxTestNew(t)
	if err := inbox.StoreShardTopDescription(context.Background(), description, root); err != nil {
		t.Fatal(err)
	}
	provider := &shardTopInboxTestProvider{
		sets: ShardTopValidatorSets{
			Current: ShardTopValidatorSet{CatchainSeqno: 7, ValidatorSetHash: 77},
			Next:    ShardTopValidatorSet{CatchainSeqno: 8, ValidatorSetHash: 88},
			HasNext: true,
		},
		ready: false,
	}
	selection := shardTopInboxTestSelection(masterchain, registry, provider, 100, 13)
	tops, err := inbox.Select(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 0 || inbox.Len() != 1 || provider.readyCall != 1 {
		t.Fatal("next-set descriptor was not deferred before readiness")
	}

	provider.sets.Current = provider.sets.Next
	provider.sets.HasNext = false
	tops, err = inbox.Select(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 0 || inbox.Len() != 1 || provider.readyCall != 2 {
		t.Fatal("unavailable descriptor was not retained")
	}

	provider.ready = true
	tops, err = inbox.Select(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 1 {
		t.Fatalf("current, ready descriptor count = %d, want 1", len(tops))
	}
}

func TestShardTopInboxTimestampCapability(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 10, 0x50)
	registry := shardTopInboxTestRegistry(t, oldBlock, masterShardTestDescriptorOptions{nextCatchainSeqno: 7})
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xa6)
	block := masterShardTestBlock(0, shard.Root, 11, 0x51)
	description, root := shardTopInboxTestDescription(t, block, []ton.BlockIDExt{oldBlock}, masterchain, 7, 77, 100)

	inbox := shardTopInboxTestNew(t)
	if err := inbox.StoreShardTopDescription(context.Background(), description, root); err != nil {
		t.Fatal(err)
	}
	provider := shardTopInboxTestReadyProvider(7, 77)
	tops, err := inbox.Select(context.Background(), shardTopInboxTestSelection(masterchain, registry, provider, 100, 12))
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 0 || provider.setsCall != 0 {
		t.Fatal("pre-v13 selection accepted an equal generation time")
	}

	tops, err = inbox.Select(context.Background(), shardTopInboxTestSelection(masterchain, registry, provider, 100, 13))
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 1 {
		t.Fatalf("v13 selection count = %d, want 1", len(tops))
	}
}

func TestShardTopInboxSelectsSplitOnlyAsCompletePair(t *testing.T) {
	parent := masterShardTestBlock(0, shard.Root, 30, 0x60)
	registry := shardTopInboxTestRegistry(t, parent, masterShardTestDescriptorOptions{
		beforeSplit:       true,
		nextCatchainSeqno: 7,
	})
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xa7)
	left, right := masterShardTestChildren(t, shard.Root)
	leftBlock := masterShardTestBlock(0, left, 31, 0x61)
	rightBlock := masterShardTestBlock(0, right, 31, 0x62)
	leftDescription, leftRoot := shardTopInboxTestDescription(
		t, leftBlock, []ton.BlockIDExt{parent}, masterchain, 7, 77, 99,
	)
	rightDescription, rightRoot := shardTopInboxTestDescription(
		t, rightBlock, []ton.BlockIDExt{parent}, masterchain, 7, 77, 99,
	)

	inbox := shardTopInboxTestNew(t)
	if err := inbox.StoreShardTopDescription(context.Background(), leftDescription, leftRoot); err != nil {
		t.Fatal(err)
	}
	provider := shardTopInboxTestReadyProvider(7, 77)
	selection := shardTopInboxTestSelection(masterchain, registry, provider, 100, 13)
	tops, err := inbox.Select(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 0 {
		t.Fatalf("half split selected %d tops", len(tops))
	}

	if err = inbox.StoreShardTopDescription(context.Background(), rightDescription, rightRoot); err != nil {
		t.Fatal(err)
	}
	tops, err = inbox.Select(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 2 || tops[0].Block.Shard != left || tops[1].Block.Shard != right {
		t.Fatalf("split selection = %+v, want canonical left/right pair", tops)
	}
}

func TestShardTopInboxUsesOnlyNewPartOfLongerProofChain(t *testing.T) {
	olderBlock := masterShardTestBlock(0, shard.Root, 10, 0x70)
	currentBlock := masterShardTestBlock(0, shard.Root, 11, 0x71)
	registry := shardTopInboxTestRegistry(t, currentBlock, masterShardTestDescriptorOptions{
		nextCatchainSeqno: 7,
	})
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xa8)
	tip := masterShardTestBlock(0, shard.Root, 12, 0x72)
	root := shardTopInboxTestTopBlockDescr(t, tip, 2)
	proofs, err := validateMasterTopBlockDescrBinding(root, tip, 2)
	if err != nil {
		t.Fatal(err)
	}
	description := &p2p.ShardBlockDescription{
		Block:            tip,
		CatchainSeqno:    7,
		ValidatorSetHash: 77,
		Chain: []p2p.ShardDescriptionLink{
			{
				Block:          tip,
				PrevRefs:       []ton.BlockIDExt{currentBlock},
				MasterchainRef: masterchain.Copy(),
				TopBlockProof:  proofs[0],
				GenUtime:       100,
				StartLT:        12_000,
				EndLT:          12_999,
				CreatedBy:      [32]byte{0x72},
				FeesCollected:  shardTopInboxTestCurrency(5),
			},
			{
				Block:          currentBlock,
				PrevRefs:       []ton.BlockIDExt{olderBlock},
				MasterchainRef: masterchain.Copy(),
				TopBlockProof:  proofs[1],
				GenUtime:       99,
				StartLT:        11_000,
				EndLT:          11_999,
				CreatedBy:      [32]byte{0x71},
				FeesCollected:  shardTopInboxTestCurrency(9),
			},
		},
	}

	inbox := shardTopInboxTestNew(t)
	if err = inbox.StoreShardTopDescription(context.Background(), description, root); err != nil {
		t.Fatal(err)
	}
	provider := shardTopInboxTestReadyProvider(7, 77)
	tops, err := inbox.Select(context.Background(), shardTopInboxTestSelection(masterchain, registry, provider, 101, 13))
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 1 || len(tops[0].Predecessors) != 1 ||
		!sameShardBlock(tops[0].Predecessors[0], currentBlock) {
		t.Fatalf("partial chain selection = %+v", tops)
	}
	if len(tops[0].Creators) != 1 || tops[0].Creators[0][0] != 0x72 {
		t.Fatalf("partial chain creators = %x, want only new link", tops[0].Creators)
	}
	fields, err := parseShardDescriptorFields(tops[0].Descriptor)
	if err != nil {
		t.Fatal(err)
	}
	if fields.fees.Coins.Nano().Int64() != 5 {
		t.Fatalf("partial chain fees = %s, want only new-link fees", fields.fees.Coins.Nano())
	}
}

func TestShardTopInboxSelectsMergeWithCanonicalPredecessors(t *testing.T) {
	left, right := masterShardTestChildren(t, shard.Root)
	leftBlock := masterShardTestBlock(0, left, 40, 0x80)
	rightBlock := masterShardTestBlock(0, right, 40, 0x81)
	mergeWindow := tlb.FutureMerge{MergeUtime: 90, Interval: 20}
	dictionary := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: 0,
		tree: masterShardTestFork(
			masterShardTestLeaf(masterShardTestDescriptor(t, leftBlock, masterShardTestDescriptorOptions{
				beforeMerge:       true,
				nextCatchainSeqno: 7,
				splitMerge:        mergeWindow,
			})),
			masterShardTestLeaf(masterShardTestDescriptor(t, rightBlock, masterShardTestDescriptorOptions{
				beforeMerge:       true,
				nextCatchainSeqno: 7,
				splitMerge:        mergeWindow,
			})),
		),
	})
	registry, err := ParseShardRegistry(dictionary)
	if err != nil {
		t.Fatal(err)
	}
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xa9)
	parent := masterShardTestBlock(0, shard.Root, 41, 0x82)
	description, root := shardTopInboxTestDescription(
		t,
		parent,
		// BlkPrevInfo order is accepted independently of Go slice order; the
		// selected transition is projected in canonical left/right order.
		[]ton.BlockIDExt{rightBlock, leftBlock},
		masterchain,
		8,
		88,
		99,
	)
	description.Chain[0].AfterMerge = true

	inbox := shardTopInboxTestNew(t)
	if err = inbox.StoreShardTopDescription(context.Background(), description, root); err != nil {
		t.Fatal(err)
	}
	provider := shardTopInboxTestReadyProvider(8, 88)
	tops, err := inbox.Select(context.Background(), shardTopInboxTestSelection(masterchain, registry, provider, 100, 13))
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 1 || len(tops[0].Predecessors) != 2 ||
		tops[0].Predecessors[0].Shard != left || tops[0].Predecessors[1].Shard != right {
		t.Fatalf("merge selection = %+v, want canonical left/right predecessors", tops)
	}
}

func TestShardTopInboxKeepsReadyLowerWhileHigherLoads(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 10, 0x83)
	lower := masterShardTestBlock(0, shard.Root, 11, 0x84)
	higher := masterShardTestBlock(0, shard.Root, 12, 0x85)
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xab)
	registry := shardTopInboxTestRegistry(t, oldBlock, masterShardTestDescriptorOptions{nextCatchainSeqno: 7})
	lowerDescription, lowerRoot := shardTopInboxTestDescription(
		t, lower, []ton.BlockIDExt{oldBlock}, masterchain, 7, 77, 98,
	)
	higherDescription, higherRoot := shardTopInboxTestTwoLinkDescription(
		t, higher, lower, oldBlock, masterchain, 7, 77, 99,
	)
	inbox := shardTopInboxTestNew(t)
	provider := shardTopInboxTestReadyProvider(7, 77)
	provider.readyBlocks = map[uint32]bool{lower.SeqNo: true, higher.SeqNo: false}
	selection := shardTopInboxTestSelection(masterchain, registry, provider, 100, 13)

	if err := inbox.StoreShardTopDescription(context.Background(), lowerDescription, lowerRoot); err != nil {
		t.Fatal(err)
	}
	tops, err := inbox.Select(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 1 || !tops[0].Block.Equals(&lower) {
		t.Fatalf("initial ready top = %+v, want lower", tops)
	}
	if err = inbox.StoreShardTopDescription(context.Background(), higherDescription, higherRoot); err != nil {
		t.Fatal(err)
	}
	tops, err = inbox.Select(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 1 || !tops[0].Block.Equals(&lower) {
		t.Fatalf("selection while higher loads = %+v, want ready lower", tops)
	}
	if inbox.Len() != 2 {
		t.Fatalf("latest/ready entry count = %d, want 2", inbox.Len())
	}
}

func TestShardTopInboxReadyHigherBlocksDeferredFallback(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 10, 0x86)
	lower := masterShardTestBlock(0, shard.Root, 11, 0x87)
	higher := masterShardTestBlock(0, shard.Root, 12, 0x88)
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xac)
	registry := shardTopInboxTestRegistry(t, oldBlock, masterShardTestDescriptorOptions{nextCatchainSeqno: 7})
	lowerDescription, lowerRoot := shardTopInboxTestDescription(
		t, lower, []ton.BlockIDExt{oldBlock}, masterchain, 7, 77, 98,
	)
	// Readiness is independent of masterchain time/lt eligibility. Once this
	// higher descriptor is ready it is the sole ready_desc, so selection must
	// not fall back to the lower descriptor while the higher one is deferred.
	higherDescription, higherRoot := shardTopInboxTestTwoLinkDescription(
		t, higher, lower, oldBlock, masterchain, 7, 77, 101,
	)
	inbox := shardTopInboxTestNew(t)
	provider := shardTopInboxTestReadyProvider(7, 77)
	selection := shardTopInboxTestSelection(masterchain, registry, provider, 100, 13)

	if err := inbox.StoreShardTopDescription(context.Background(), lowerDescription, lowerRoot); err != nil {
		t.Fatal(err)
	}
	if tops, err := inbox.Select(context.Background(), selection); err != nil || len(tops) != 1 {
		t.Fatalf("prepare lower ready top: tops=%d err=%v", len(tops), err)
	}
	if err := inbox.StoreShardTopDescription(context.Background(), higherDescription, higherRoot); err != nil {
		t.Fatal(err)
	}
	tops, err := inbox.Select(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 0 {
		t.Fatalf("deferred higher fell back to lower: %+v", tops)
	}
	if inbox.Len() != 1 || inbox.entries[lowerRoot.HashKey()] != nil || inbox.entries[higherRoot.HashKey()] == nil {
		t.Fatal("higher ready descriptor did not replace the lower ready descriptor")
	}
}

func TestShardTopInboxInvalidHigherPreservesReadyLower(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 10, 0x8f)
	lower := masterShardTestBlock(0, shard.Root, 11, 0x90)
	higher := masterShardTestBlock(0, shard.Root, 12, 0x91)
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xb1)
	registry := shardTopInboxTestRegistry(t, oldBlock, masterShardTestDescriptorOptions{nextCatchainSeqno: 7})
	lowerDescription, lowerRoot := shardTopInboxTestDescription(
		t, lower, []ton.BlockIDExt{oldBlock}, masterchain, 7, 77, 98,
	)
	// The higher queue is available, but its declared validator set is neither
	// current nor next. It is rejected at the post-preload validity check
	// and leaves the already ready lower descriptor intact.
	higherDescription, higherRoot := shardTopInboxTestTwoLinkDescription(
		t, higher, lower, oldBlock, masterchain, 7, 99, 99,
	)
	inbox := shardTopInboxTestNew(t)
	provider := shardTopInboxTestReadyProvider(7, 77)
	selection := shardTopInboxTestSelection(masterchain, registry, provider, 100, 13)

	if err := inbox.StoreShardTopDescription(context.Background(), lowerDescription, lowerRoot); err != nil {
		t.Fatal(err)
	}
	if tops, err := inbox.Select(context.Background(), selection); err != nil || len(tops) != 1 {
		t.Fatalf("prepare lower ready top: tops=%d err=%v", len(tops), err)
	}
	if err := inbox.StoreShardTopDescription(context.Background(), higherDescription, higherRoot); err != nil {
		t.Fatal(err)
	}
	tops, err := inbox.Select(context.Background(), selection)
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 1 || !tops[0].Block.Equals(&lower) {
		t.Fatalf("invalid higher displaced ready lower: %+v", tops)
	}
	groupKey := shardTopInboxGroupKey{shard: shardTopKey(lower), catchainSeqno: 7}
	group := inbox.groups[groupKey]
	if group == nil || group.ready == nil || !group.ready.description.Block.Equals(&lower) ||
		group.latest != group.ready || inbox.entries[higherRoot.HashKey()] != nil || inbox.Len() != 1 {
		t.Fatal("invalid higher changed the latest/ready roles")
	}
}

func TestShardTopInboxSameHeightKeepsFirstInstalled(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 10, 0x89)
	first := masterShardTestBlock(0, shard.Root, 11, 0x8a)
	second := masterShardTestBlock(0, shard.Root, 11, 0x8b)
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xad)
	registry := shardTopInboxTestRegistry(t, oldBlock, masterShardTestDescriptorOptions{nextCatchainSeqno: 7})
	firstDescription, firstRoot := shardTopInboxTestDescription(
		t, first, []ton.BlockIDExt{oldBlock}, masterchain, 7, 77, 99,
	)
	secondDescription, secondRoot := shardTopInboxTestDescription(
		t, second, []ton.BlockIDExt{oldBlock}, masterchain, 7, 77, 99,
	)
	inbox := shardTopInboxTestNew(t)

	if err := inbox.StoreShardTopDescription(context.Background(), firstDescription, firstRoot); err != nil {
		t.Fatal(err)
	}
	if err := inbox.StoreShardTopDescription(context.Background(), secondDescription, secondRoot); err != nil {
		t.Fatal(err)
	}
	provider := shardTopInboxTestReadyProvider(7, 77)
	tops, err := inbox.Select(
		context.Background(),
		shardTopInboxTestSelection(masterchain, registry, provider, 100, 13),
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(tops) != 1 || !tops[0].Block.Equals(&first) {
		t.Fatalf("same-height selection = %+v, want first installed", tops)
	}
	if inbox.Len() != 1 || inbox.entries[firstRoot.HashKey()] == nil || inbox.entries[secondRoot.HashKey()] != nil {
		t.Fatal("same-height fork replaced the first installed descriptor")
	}
}

func TestShardTopInboxStoreDuringReadinessKeepsFallback(t *testing.T) {
	oldBlock := masterShardTestBlock(0, shard.Root, 10, 0x8c)
	lower := masterShardTestBlock(0, shard.Root, 11, 0x8d)
	higher := masterShardTestBlock(0, shard.Root, 12, 0x8e)
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xae)
	registry := shardTopInboxTestRegistry(t, oldBlock, masterShardTestDescriptorOptions{nextCatchainSeqno: 7})
	lowerDescription, lowerRoot := shardTopInboxTestDescription(
		t, lower, []ton.BlockIDExt{oldBlock}, masterchain, 7, 77, 98,
	)
	higherDescription, higherRoot := shardTopInboxTestTwoLinkDescription(
		t, higher, lower, oldBlock, masterchain, 7, 77, 99,
	)
	inbox := shardTopInboxTestNew(t)
	if err := inbox.StoreShardTopDescription(context.Background(), lowerDescription, lowerRoot); err != nil {
		t.Fatal(err)
	}
	started := make(chan struct{})
	release := make(chan struct{})
	readinessCalls := 0
	provider := shardTopInboxTestReadyProvider(7, 77)
	provider.readyCheck = func(ctx context.Context, block ton.BlockIDExt) (bool, error) {
		if block.SeqNo != lower.SeqNo {
			return false, nil
		}
		readinessCalls++
		if readinessCalls > 1 {
			return true, nil
		}
		close(started)
		select {
		case <-ctx.Done():
			return false, ctx.Err()
		case <-release:
			return true, nil
		}
	}
	selection := shardTopInboxTestSelection(masterchain, registry, provider, 100, 13)
	topsResult := make(chan []ShardTop, 1)
	errorResult := make(chan error, 1)
	go func() {
		tops, err := inbox.Select(context.Background(), selection)
		topsResult <- tops
		errorResult <- err
	}()
	<-started
	if err := inbox.StoreShardTopDescription(context.Background(), higherDescription, higherRoot); err != nil {
		t.Fatal(err)
	}
	close(release)
	tops := <-topsResult
	if err := <-errorResult; err != nil {
		t.Fatal(err)
	}
	if len(tops) != 1 || !tops[0].Block.Equals(&lower) {
		t.Fatalf("in-flight ready fallback = %+v, want lower", tops)
	}
}

func TestShardTopInboxEvictsByPerShardAndGlobalBounds(t *testing.T) {
	masterchain := masterShardTestBlock(-1, shard.Root, 100, 0xaa)

	t.Run("per shard", func(t *testing.T) {
		inbox, err := NewShardTopInbox(ShardTopInboxOptions{
			MaxEntries:         10,
			MaxEntriesPerShard: 1,
		})
		if err != nil {
			t.Fatal(err)
		}
		first := masterShardTestBlock(0, shard.Root, 11, 0x91)
		second := masterShardTestBlock(0, shard.Root, 12, 0x92)
		firstDescription, firstRoot := shardTopInboxTestDescription(
			t, first, []ton.BlockIDExt{masterShardTestBlock(0, shard.Root, 10, 0x90)}, masterchain, 7, 77, 90,
		)
		secondDescription, secondRoot := shardTopInboxTestDescription(
			t, second, []ton.BlockIDExt{first}, masterchain, 7, 77, 91,
		)
		if err = inbox.StoreShardTopDescription(context.Background(), firstDescription, firstRoot); err != nil {
			t.Fatal(err)
		}
		if err = inbox.StoreShardTopDescription(context.Background(), secondDescription, secondRoot); err != nil {
			t.Fatal(err)
		}
		if inbox.Len() != 1 || inbox.entries[firstRoot.HashKey()] != nil || inbox.entries[secondRoot.HashKey()] == nil {
			t.Fatal("per-shard bound did not evict the oldest exact root")
		}
	})

	t.Run("global", func(t *testing.T) {
		inbox, err := NewShardTopInbox(ShardTopInboxOptions{
			MaxEntries:         2,
			MaxEntriesPerShard: 2,
		})
		if err != nil {
			t.Fatal(err)
		}
		roots := make([]*cell.Cell, 0, 3)
		for workchain := int32(0); workchain < 3; workchain++ {
			previous := masterShardTestBlock(workchain, shard.Root, 10, byte(0xa0+workchain))
			block := masterShardTestBlock(workchain, shard.Root, 11, byte(0xb0+workchain))
			description, root := shardTopInboxTestDescription(
				t, block, []ton.BlockIDExt{previous}, masterchain, 7, 77, 90,
			)
			if err = inbox.StoreShardTopDescription(context.Background(), description, root); err != nil {
				t.Fatal(err)
			}
			roots = append(roots, root)
		}
		if inbox.Len() != 2 || inbox.entries[roots[0].HashKey()] != nil ||
			inbox.entries[roots[1].HashKey()] == nil || inbox.entries[roots[2].HashKey()] == nil {
			t.Fatal("global bound did not evict the oldest exact root")
		}

		first := masterShardTestBlock(0, shard.Root, 11, 0xb0)
		replacement := masterShardTestBlock(0, shard.Root, 12, 0xc0)
		description, root := shardTopInboxTestDescription(
			t, replacement, []ton.BlockIDExt{first}, masterchain, 7, 77, 91,
		)
		if err = inbox.StoreShardTopDescription(context.Background(), description, root); err != nil {
			t.Fatalf("store after latest eviction: %v", err)
		}
		groupKey := shardTopInboxGroupKey{
			shard:         shardTopKey(replacement),
			catchainSeqno: 7,
		}
		if inbox.Len() != 2 || inbox.groups[groupKey] == nil ||
			inbox.groups[groupKey].latest != inbox.entries[root.HashKey()] {
			t.Fatal("global eviction left a stale latest descriptor pointer")
		}
	})
}

func shardTopInboxTestNew(t *testing.T) *ShardTopInbox {
	t.Helper()
	inbox, err := NewShardTopInbox(ShardTopInboxOptions{})
	if err != nil {
		t.Fatal(err)
	}
	return inbox
}

func shardTopInboxTestRegistry(
	t *testing.T,
	block ton.BlockIDExt,
	options masterShardTestDescriptorOptions,
) *ShardRegistry {
	t.Helper()
	dictionary := masterShardTestHashes(t, masterShardTestWorkchain{
		workchain: block.Workchain,
		tree:      masterShardTestLeaf(masterShardTestDescriptor(t, block, options)),
	})
	registry, err := ParseShardRegistry(dictionary)
	if err != nil {
		t.Fatal(err)
	}
	return registry
}

func shardTopInboxTestDescription(
	t *testing.T,
	block ton.BlockIDExt,
	predecessors []ton.BlockIDExt,
	masterchain ton.BlockIDExt,
	catchainSeqno uint32,
	validatorSetHash uint32,
	genUtime uint32,
) (*p2p.ShardBlockDescription, *cell.Cell) {
	t.Helper()
	root := masterBuildTopBlockDescr(t, block)
	proofs, err := validateMasterTopBlockDescrBinding(root, block, 1)
	if err != nil {
		t.Fatal(err)
	}
	return &p2p.ShardBlockDescription{
		Block:            block,
		CatchainSeqno:    catchainSeqno,
		ValidatorSetHash: validatorSetHash,
		Chain: []p2p.ShardDescriptionLink{{
			Block:          block,
			PrevRefs:       predecessors,
			MasterchainRef: masterchain.Copy(),
			TopBlockProof:  proofs[0],
			GenUtime:       genUtime,
			StartLT:        uint64(block.SeqNo) * 1_000,
			EndLT:          uint64(block.SeqNo)*1_000 + 999,
		}},
	}, root
}

func shardTopInboxTestTwoLinkDescription(
	t *testing.T,
	tip ton.BlockIDExt,
	middle ton.BlockIDExt,
	predecessor ton.BlockIDExt,
	masterchain ton.BlockIDExt,
	catchainSeqno uint32,
	validatorSetHash uint32,
	genUtime uint32,
) (*p2p.ShardBlockDescription, *cell.Cell) {
	t.Helper()
	root := shardTopInboxTestTopBlockDescr(t, tip, 2)
	proofs, err := validateMasterTopBlockDescrBinding(root, tip, 2)
	if err != nil {
		t.Fatal(err)
	}

	return &p2p.ShardBlockDescription{
		Block:            tip,
		CatchainSeqno:    catchainSeqno,
		ValidatorSetHash: validatorSetHash,
		Chain: []p2p.ShardDescriptionLink{
			{
				Block:          tip,
				PrevRefs:       []ton.BlockIDExt{middle},
				MasterchainRef: masterchain.Copy(),
				TopBlockProof:  proofs[0],
				GenUtime:       genUtime,
				StartLT:        uint64(tip.SeqNo) * 1_000,
				EndLT:          uint64(tip.SeqNo)*1_000 + 999,
			},
			{
				Block:          middle,
				PrevRefs:       []ton.BlockIDExt{predecessor},
				MasterchainRef: masterchain.Copy(),
				TopBlockProof:  proofs[1],
				GenUtime:       genUtime - 1,
				StartLT:        uint64(middle.SeqNo) * 1_000,
				EndLT:          uint64(middle.SeqNo)*1_000 + 999,
			},
		},
	}, root
}

func shardTopInboxTestTopBlockDescr(t *testing.T, block ton.BlockIDExt, chainLength int) *cell.Cell {
	t.Helper()
	if chainLength < 1 {
		t.Fatal("chain length must be positive")
	}
	proofs := make([]*cell.Cell, chainLength)
	for index := range proofs {
		proof, err := cell.CreateMerkleProof(
			cell.BeginCell().MustStoreUInt(blockTag, 32).MustStoreUInt(uint64(index), 8).EndCell(),
		)
		if err != nil {
			t.Fatal(err)
		}
		proofs[index] = proof
	}

	ident, err := topologyShardIdent(groups.ShardID{Workchain: block.Workchain, Shard: block.Shard})
	if err != nil {
		t.Fatal(err)
	}
	identRoot, err := tlb.ToCell(&ident)
	if err != nil {
		t.Fatal(err)
	}
	builder := cell.BeginCell().
		MustStoreUInt(masterTopBlockDescrTag, 8).
		MustStoreBuilder(identRoot.ToBuilder()).
		MustStoreUInt(uint64(block.SeqNo), 32).
		MustStoreSlice(block.RootHash, 256).
		MustStoreSlice(block.FileHash, 256).
		MustStoreBoolBit(false).
		MustStoreUInt(uint64(chainLength), 8).
		MustStoreRef(proofs[0])
	if chainLength > 1 {
		builder.MustStoreRef(shardTopInboxTestProofChainTail(proofs[1:]))
	}

	return builder.EndCell()
}

func shardTopInboxTestProofChainTail(proofs []*cell.Cell) *cell.Cell {
	builder := cell.BeginCell().MustStoreRef(proofs[0])
	if len(proofs) > 1 {
		builder.MustStoreRef(shardTopInboxTestProofChainTail(proofs[1:]))
	}
	return builder.EndCell()
}

func shardTopInboxTestCurrency(nano int64) tlb.CurrencyCollection {
	return tlb.CurrencyCollection{Coins: tlb.FromNanoTON(big.NewInt(nano))}
}

func shardTopInboxTestReadyProvider(catchainSeqno, validatorSetHash uint32) *shardTopInboxTestProvider {
	return &shardTopInboxTestProvider{
		sets: ShardTopValidatorSets{Current: ShardTopValidatorSet{
			CatchainSeqno:    catchainSeqno,
			ValidatorSetHash: validatorSetHash,
		}},
		ready:    true,
		ancestor: true,
	}
}

func shardTopInboxTestSelection(
	masterchain ton.BlockIDExt,
	registry *ShardRegistry,
	provider ShardTopSelectionProvider,
	genUtime uint32,
	globalVersion uint32,
) ShardTopSelection {
	return ShardTopSelection{
		Masterchain:   masterchain,
		VertSeqno:     0,
		GenUtime:      genUtime,
		GlobalVersion: globalVersion,
		LTLimit:       1_000_000,
		Registry:      registry,
		Provider:      provider,
	}
}
