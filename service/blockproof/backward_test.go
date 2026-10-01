package blockproof

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type testOldMcBlocksAugmentation struct{}

func (testOldMcBlocksAugmentation) SkipExtra(loader *cell.Slice) error {
	var extra tlb.KeyMaxLt
	return tlb.LoadFromCell(&extra, loader)
}

func (testOldMcBlocksAugmentation) EmptyExtra(dst *cell.Builder) error {
	return storeTestKeyMaxLT(dst, tlb.KeyMaxLt{})
}

func (testOldMcBlocksAugmentation) LeafExtra(value *cell.Slice, dst *cell.Builder) error {
	var ref tlb.KeyExtBlkRef
	if err := tlb.LoadFromCell(&ref, value.Copy()); err != nil {
		return err
	}
	return storeTestKeyMaxLT(dst, tlb.KeyMaxLt{IsKey: ref.IsKey, MaxEndLT: ref.BlkRef.EndLt})
}

func (testOldMcBlocksAugmentation) CombineExtra(leftExtra, rightExtra *cell.Slice, dst *cell.Builder) error {
	var left, right tlb.KeyMaxLt
	if err := tlb.LoadFromCell(&left, leftExtra.Copy()); err != nil {
		return err
	}
	if err := tlb.LoadFromCell(&right, rightExtra.Copy()); err != nil {
		return err
	}
	return storeTestKeyMaxLT(dst, tlb.KeyMaxLt{
		IsKey:    left.IsKey || right.IsKey,
		MaxEndLT: max(left.MaxEndLT, right.MaxEndLT),
	})
}

func storeTestKeyMaxLT(dst *cell.Builder, extra tlb.KeyMaxLt) error {
	if err := dst.StoreBoolBit(extra.IsKey); err != nil {
		return err
	}
	return dst.StoreUInt(extra.MaxEndLT, 64)
}

type testOldMasterBlock struct {
	id    ton.BlockIDExt
	isKey bool
	endLT uint64
}

func TestOldMasterBlockIDAndLastKeyBlockCopyHashes(t *testing.T) {
	old := testBlockID(-1, 7)
	old.RootHash = bytes.Repeat([]byte{0x11}, 32)
	old.FileHash = bytes.Repeat([]byte{0x22}, 32)

	lastKey := testBlockID(-1, 5)
	lastKey.RootHash = bytes.Repeat([]byte{0x33}, 32)
	lastKey.FileHash = bytes.Repeat([]byte{0x44}, 32)

	info, err := LoadMasterStateInfo(testMasterStateInfoCell(t, false, &lastKey, []testOldMasterBlock{{
		id:    old,
		isKey: true,
	}}))
	if err != nil {
		t.Fatalf("load master state info: %v", err)
	}

	gotOld, err := OldMasterBlockID(info.PrevBlocks, old.SeqNo)
	if err != nil {
		t.Fatalf("load old master block: %v", err)
	}
	gotLastKey, err := info.LastKeyBlockID(testBlockID(-1, 9))
	if err != nil {
		t.Fatalf("load last key block: %v", err)
	}

	gotOld.RootHash[0] ^= 0xff
	gotOld.FileHash[0] ^= 0xff
	gotLastKey.RootHash[0] ^= 0xff
	gotLastKey.FileHash[0] ^= 0xff

	gotOld, err = OldMasterBlockID(info.PrevBlocks, old.SeqNo)
	if err != nil {
		t.Fatalf("reload old master block: %v", err)
	}
	gotLastKey, err = info.LastKeyBlockID(testBlockID(-1, 9))
	if err != nil {
		t.Fatalf("reload last key block: %v", err)
	}

	if !bytes.Equal(gotOld.RootHash, old.RootHash) || !bytes.Equal(gotOld.FileHash, old.FileHash) {
		t.Fatal("old master block id reuses caller-mutated hash slices")
	}
	if !bytes.Equal(gotLastKey.RootHash, lastKey.RootHash) || !bytes.Equal(gotLastKey.FileHash, lastKey.FileHash) {
		t.Fatal("last key block id reuses caller-mutated hash slices")
	}
}

func TestMasterStateInfoLastKeyBlockUsesCurrentBlockAfterKeyBlock(t *testing.T) {
	master := testBlockID(-1, 15)
	master.RootHash = bytes.Repeat([]byte{0x55}, 32)
	master.FileHash = bytes.Repeat([]byte{0x66}, 32)

	info, err := LoadMasterStateInfo(testMasterStateInfoCell(t, true, nil, []testOldMasterBlock{{
		id: testBlockID(-1, 14),
	}}))
	if err != nil {
		t.Fatalf("load master state info: %v", err)
	}

	got, err := info.LastKeyBlockID(master)
	if err != nil {
		t.Fatalf("last key block after key block: %v", err)
	}
	if !BlockIDEqual(got, master) {
		t.Fatalf("last key block = %v, want current master %v", got, master)
	}
}

func testMasterStateInfoCell(
	t testing.TB,
	afterKeyBlock bool,
	lastKey *ton.BlockIDExt,
	oldBlocks []testOldMasterBlock,
) *cell.Cell {
	t.Helper()

	prevBlocks, err := cell.NewAugDict(32, testOldMcBlocksAugmentation{})
	if err != nil {
		t.Fatalf("create old mc blocks dict: %v", err)
	}
	for _, old := range oldBlocks {
		endLT := old.endLT
		if endLT == 0 {
			endLT = uint64(old.id.SeqNo+1) * 100
		}

		ref, err := tlb.ToCell(&tlb.KeyExtBlkRef{
			IsKey: old.isKey,
			BlkRef: tlb.ExtBlkRef{
				EndLt:    endLT,
				SeqNo:    old.id.SeqNo,
				RootHash: bytes.Clone(old.id.RootHash),
				FileHash: bytes.Clone(old.id.FileHash),
			},
		})
		if err != nil {
			t.Fatalf("build old mc block ref: %v", err)
		}
		if err = prevBlocks.SetIntKey(big.NewInt(int64(old.id.SeqNo)), ref); err != nil {
			t.Fatalf("set old mc block ref: %v", err)
		}
	}

	prevBlocksCell, err := prevBlocks.ToCell()
	if err != nil {
		t.Fatalf("serialize old mc blocks: %v", err)
	}

	builder := cell.BeginCell().
		MustStoreUInt(0, 16).
		MustStoreUInt(0, 32).
		MustStoreUInt(0, 32).
		MustStoreBoolBit(false).
		MustStoreBuilder(prevBlocksCell.ToBuilder()).
		MustStoreBoolBit(afterKeyBlock).
		MustStoreBoolBit(lastKey != nil)

	if lastKey != nil {
		ref, err := tlb.ToCell(&tlb.ExtBlkRef{
			SeqNo:    lastKey.SeqNo,
			RootHash: bytes.Clone(lastKey.RootHash),
			FileHash: bytes.Clone(lastKey.FileHash),
		})
		if err != nil {
			t.Fatalf("build last key block ref: %v", err)
		}
		builder.MustStoreBuilder(ref.ToBuilder())
	}

	return builder.EndCell()
}
