package validator

import (
	"bytes"
	"testing"
	"time"

	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/validator/groups"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func chainStateBlock(shardID int64, seqno uint32, marker byte) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: 0,
		Shard:     shardID,
		SeqNo:     seqno,
		RootHash:  bytes.Repeat([]byte{marker}, 32),
		FileHash:  bytes.Repeat([]byte{marker + 1}, 32),
	}
}

func chainStateData(blocks ...ton.BlockIDExt) ChainStateData {
	tips := make([]ChainTip, len(blocks))
	for i := range blocks {
		tips[i] = ChainTip{
			ID:       blocks[i],
			BlockBOC: []byte{0x01},
			State:    cell.BeginCell().MustStoreUInt(uint64(i), 1).EndCell(),
		}
	}

	return ChainStateData{Tips: tips}
}

func TestChainStateRequiresDirectSplitParent(t *testing.T) {
	left, err := shard.Child(shard.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	leftLeft, err := shard.Child(left, true)
	if err != nil {
		t.Fatal(err)
	}
	parent := chainStateBlock(shard.Root, 10, 0x10)

	directRequest := ChainStateRequest{
		Shard:  groups.ShardID{Workchain: 0, Shard: left},
		Blocks: []ton.BlockIDExt{parent},
	}
	state, err := newChainState(directRequest, chainStateData(parent))
	if err != nil {
		t.Fatalf("direct parent rejected: %v", err)
	}
	if _, err = state.NormalBlock(); err == nil {
		t.Fatal("before-split topology was exposed as a normal tip")
	}

	deepRequest := directRequest
	deepRequest.Shard.Shard = leftLeft
	if _, err = newChainState(deepRequest, chainStateData(parent)); err == nil {
		t.Fatal("arbitrary ancestor was accepted as a split predecessor")
	}
}

func TestChainStateMergeShapeMatchesReference(t *testing.T) {
	left, err := shard.Child(shard.Root, true)
	if err != nil {
		t.Fatal(err)
	}
	right, err := shard.Child(shard.Root, false)
	if err != nil {
		t.Fatal(err)
	}
	leftBlock := chainStateBlock(left, 11, 0x21)
	rightBlock := chainStateBlock(right, 12, 0x31)
	request := ChainStateRequest{
		Shard:  groups.ShardID{Workchain: 0, Shard: shard.Root},
		Blocks: []ton.BlockIDExt{leftBlock, rightBlock},
	}
	if _, err = newChainState(request, chainStateData(leftBlock, rightBlock)); err != nil {
		t.Fatalf("ordered merge children rejected: %v", err)
	}

	reversed := ChainStateRequest{Shard: request.Shard, Blocks: []ton.BlockIDExt{rightBlock, leftBlock}}
	if _, err = newChainState(reversed, chainStateData(rightBlock, leftBlock)); err == nil {
		t.Fatal("reversed merge children were accepted")
	}

	zeroLeft := chainStateBlock(left, 0, 0x41)
	zeroRequest := ChainStateRequest{Shard: request.Shard, Blocks: []ton.BlockIDExt{zeroLeft, rightBlock}}
	zeroData := chainStateData(zeroLeft, rightBlock)
	zeroData.Tips[0].BlockBOC = nil
	if _, err = newChainState(zeroRequest, zeroData); err == nil {
		t.Fatal("zero merge predecessor was accepted")
	}
}

func TestCandidateGenerationTimeUsesConsensusExtraData(t *testing.T) {
	want := time.UnixMilli(1_765_432_109_876)
	extra := cell.BeginCell().
		MustStoreUInt(consensusExtraDataTag, 32).
		MustStoreUInt(0, 32).
		MustStoreUInt(uint64(want.UnixMilli()), 64).
		EndCell()
	boc, err := cell.ToBOCWithOptionsErr([]*cell.Cell{extra}, cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	got, err := candidateGenUtime(boc)
	if err != nil {
		t.Fatal(err)
	}
	if !got.Equal(want) {
		t.Fatalf("candidate generation time = %v, want %v", got, want)
	}
}
