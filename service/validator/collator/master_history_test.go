package collator

import (
	"bytes"
	"math"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
)

func TestMasterPrevBlocksTupleMatchesConfigInfoLayout(t *testing.T) {
	previous := masterHistoryTestBlock(250)
	history := masterHistoryTestDictionary(t, 250)
	lastKey := masterHistoryTestReference(200)
	info := tlb.McStateExtraBlockInfo{
		PrevBlocks:   history,
		LastKeyBlock: &lastKey,
	}

	got, err := masterPrevBlocksTuple(previous, &info, 9)
	if err != nil {
		t.Fatal(err)
	}
	if got.Len() != 3 {
		t.Fatalf("PrevBlocksInfo length = %d, want 3", got.Len())
	}

	recent := masterHistoryTestTupleAt(t, got, 0)
	if recent.Len() != masterHistoryTupleLimit {
		t.Fatalf("recent history length = %d, want %d", recent.Len(), masterHistoryTupleLimit)
	}
	for i := 0; i < masterHistoryTupleLimit; i++ {
		masterHistoryAssertBlockTuple(t, masterHistoryTestTupleAt(t, recent, i), uint32(250-i))
	}

	masterHistoryAssertBlockTuple(t, masterHistoryTestTupleAt(t, got, 1), 200)
	byHundred := masterHistoryTestTupleAt(t, got, 2)
	if byHundred.Len() != 3 {
		t.Fatalf("hundred-step history length = %d, want 3", byHundred.Len())
	}
	for i, seqno := range []uint32{200, 100, 0} {
		masterHistoryAssertBlockTuple(t, masterHistoryTestTupleAt(t, byHundred, i), seqno)
	}
}

func TestMasterPrevBlocksTupleVersionAndKeyState(t *testing.T) {
	previous := masterHistoryTestBlock(3)
	info := tlb.McStateExtraBlockInfo{
		PrevBlocks:    masterHistoryTestDictionary(t, 3),
		AfterKeyBlock: true,
	}

	got, err := masterPrevBlocksTuple(previous, &info, 8)
	if err != nil {
		t.Fatal(err)
	}
	if got.Len() != 2 {
		t.Fatalf("v8 PrevBlocksInfo length = %d, want 2", got.Len())
	}
	masterHistoryAssertBlockTuple(t, masterHistoryTestTupleAt(t, got, 1), previous.SeqNo)
}

func TestMasterPrevBlocksTupleUsesCurrentAtHundredBoundary(t *testing.T) {
	previous := masterHistoryTestBlock(200)
	info := tlb.McStateExtraBlockInfo{
		PrevBlocks:    masterHistoryTestDictionary(t, 200),
		AfterKeyBlock: true,
	}

	got, err := masterPrevBlocksTuple(previous, &info, 9)
	if err != nil {
		t.Fatal(err)
	}
	byHundred := masterHistoryTestTupleAt(t, got, 2)
	masterHistoryAssertBlockTuple(t, masterHistoryTestTupleAt(t, byHundred, 0), previous.SeqNo)
}

func TestMasterPrevBlocksTupleRejectsMissingHistoryAndKey(t *testing.T) {
	previous := masterHistoryTestBlock(2)
	history := masterHistoryTestDictionary(t, 1)
	info := tlb.McStateExtraBlockInfo{PrevBlocks: history}
	if _, err := masterPrevBlocksTuple(previous, &info, 9); err == nil {
		t.Fatal("missing recent block was accepted")
	}

	previous = masterHistoryTestBlock(0)
	info = tlb.McStateExtraBlockInfo{PrevBlocks: masterHistoryTestDictionary(t, 0)}
	if _, err := masterPrevBlocksTuple(previous, &info, 8); err == nil {
		t.Fatal("missing previous key block was accepted")
	}
}

func masterHistoryTestDictionary(t *testing.T, end uint32) *tlb.OldMcBlocksInfoAugDict {
	t.Helper()
	dict, err := cell.NewAugDict(32, oldMCBlocksAugmentation{})
	if err != nil {
		t.Fatal(err)
	}
	for seqno := uint32(0); seqno < end; seqno++ {
		ref := masterHistoryTestReference(seqno)
		value, cellErr := tlb.ToCell(&tlb.KeyExtBlkRef{IsKey: seqno%100 == 0, BlkRef: ref})
		if cellErr != nil {
			t.Fatal(cellErr)
		}
		key := cell.BeginCell().MustStoreUInt(uint64(seqno), 32).EndCell()
		inserted, setErr := dict.SetWithMode(key, value, cell.DictSetModeAdd)
		if setErr != nil || !inserted {
			t.Fatalf("store history block %d: inserted=%t err=%v", seqno, inserted, setErr)
		}
	}
	return &tlb.OldMcBlocksInfoAugDict{AugmentedDictionary: dict}
}

func masterHistoryTestBlock(seqno uint32) ton.BlockIDExt {
	ref := masterHistoryTestReference(seqno)
	return ton.BlockIDExt{
		Workchain: masterchainWorkchainID,
		Shard:     math.MinInt64,
		SeqNo:     seqno,
		RootHash:  ref.RootHash,
		FileHash:  ref.FileHash,
	}
}

func masterHistoryTestReference(seqno uint32) tlb.ExtBlkRef {
	return tlb.ExtBlkRef{
		EndLt:    uint64(seqno+1) * logicalTimeAlignment,
		SeqNo:    seqno,
		RootHash: bytes.Repeat([]byte{byte(seqno)}, 32),
		FileHash: bytes.Repeat([]byte{byte(seqno ^ 0x5a)}, 32),
	}
}

func masterHistoryTestTupleAt(t *testing.T, source tuple.Tuple, index int) tuple.Tuple {
	t.Helper()
	value, err := source.Index(index)
	if err != nil {
		t.Fatal(err)
	}
	result, ok := value.(tuple.Tuple)
	if !ok {
		t.Fatalf("tuple item %d has type %T", index, value)
	}
	return result
}

func masterHistoryAssertBlockTuple(t *testing.T, block tuple.Tuple, seqno uint32) {
	t.Helper()
	if block.Len() != 5 {
		t.Fatalf("block tuple length = %d, want 5", block.Len())
	}
	want := masterHistoryTestBlock(seqno)
	values := []*big.Int{
		big.NewInt(int64(want.Workchain)),
		new(big.Int).SetUint64(uint64(want.Shard)),
		new(big.Int).SetUint64(uint64(want.SeqNo)),
		new(big.Int).SetBytes(want.RootHash),
		new(big.Int).SetBytes(want.FileHash),
	}
	for i, expected := range values {
		value, err := block.Index(i)
		if err != nil {
			t.Fatal(err)
		}
		integer, ok := value.(*big.Int)
		if !ok || integer.Cmp(expected) != 0 {
			t.Fatalf("block %d item %d = %v (%T), want %v", seqno, i, value, value, expected)
		}
	}
}
