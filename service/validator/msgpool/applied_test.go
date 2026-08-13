package msgpool

import (
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// buildInMsgDescr assembles a real InMsgDescr aug-dictionary with
// msg_import_ext entries for the given messages (plus their dummy
// transaction refs), using the shared tonutils-go augmentation.
func buildInMsgDescr(t *testing.T, msgs []*cell.Cell) *cell.Cell {
	t.Helper()
	dict, err := cell.NewAugDict(256, tlb.AugInMsgDescr{})
	if err != nil {
		t.Fatal(err)
	}
	for _, msg := range msgs {
		value := cell.BeginCell().
			MustStoreUInt(0b000, 3). // msg_import_ext
			MustStoreRef(msg).
			MustStoreRef(cell.BeginCell().MustStoreUInt(0xdead, 32).EndCell()). // transaction stub
			EndCell()
		if err = dict.SetIntKey(new(big.Int).SetBytes(msg.Hash()), value); err != nil {
			t.Fatal(err)
		}
	}
	return dict.AsCell()
}

func TestAppliedNormHashesFromInMsgDescr(t *testing.T) {
	addr := testAddr(0x61)
	body := bodyWithTag(5)

	// The imported variant differs from the pooled one by import fee — the
	// normalized hash must still match.
	pooledRaw := buildExtMsg(t, 0, addr, body, msgOpts{importFee: 0})
	importedRaw := buildExtMsg(t, 0, addr, body, msgOpts{importFee: 500})
	pooled := assembleMsg(t, pooledRaw)
	imported := assembleMsg(t, importedRaw)
	otherRaw := buildExtMsg(t, 0, testAddr(0x62), bodyWithTag(6), msgOpts{})
	other := assembleMsg(t, otherRaw)

	descr := buildInMsgDescr(t, []*cell.Cell{imported.Root, other.Root})
	hashes, err := AppliedNormHashesFromInMsgDescr(descr)
	if err != nil {
		t.Fatal(err)
	}
	requireEqual(t, len(hashes), 2, "hashes extracted")
	found := map[[32]byte]bool{}
	for _, h := range hashes {
		found[h] = true
	}
	requireEqual(t, found[pooled.HashNorm], true, "normalized hash matches the pooled variant")
	requireEqual(t, found[other.HashNorm], true, "second message extracted")

	// End to end: pooling the message and erasing by the block-extracted
	// hashes removes it.
	env := newPoolEnv(t)
	env.mustAdd(pooledRaw, 0)
	requireEqual(t, env.pool.Stats().Pooled, 1, "pooled")
	env.pool.EraseApplied(hashes)
	requireEqual(t, env.pool.Stats().Pooled, 0, "erased by applied block")
	requireEqual(t, env.pool.Stats().AppliedDeleted, uint64(1), "applied deleted")
}
