package collator

import (
	"bytes"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestUpdateMasterPublicLibrariesAddsAndRemovesPublisher(t *testing.T) {
	library := cell.BeginCell().MustStoreUInt(0xcafe, 16).EndCell()
	accountID := bytes.Repeat([]byte{0x41}, 32)
	addr := address.NewAddress(0, 0xff, accountID)
	nonPublic := testAccountLibraries(t, library, false)
	public := testAccountLibraries(t, library, true)

	added := testLibraryCollation(t, addr, nonPublic, public, nil)
	if err := added.updateMasterPublicLibraries(); err != nil {
		t.Fatal(err)
	}
	key := library.HashKey()
	descriptor := loadTestGlobalLibrary(t, added.master.libraries, key)
	if descriptor.root.HashKey() != key {
		t.Fatal("global library root differs from its key")
	}
	publishers, err := descriptor.publishers.LoadAll()
	if err != nil || len(publishers) != 1 {
		t.Fatalf("publisher count = %d, err = %v", len(publishers), err)
	}

	removed := testLibraryCollation(t, addr, public, nonPublic, added.master.libraries)
	if err := removed.updateMasterPublicLibraries(); err != nil {
		t.Fatal(err)
	}
	if !removed.master.libraries.IsEmpty() {
		t.Fatal("library with its last publisher removed remains in the global dictionary")
	}
}

func TestRemoveMasterPublicLibraryKeepsOtherPublisher(t *testing.T) {
	library := cell.BeginCell().MustStoreUInt(0xbeef, 16).EndCell()
	key := library.HashKey()
	firstID := [32]byte{1}
	secondID := [32]byte{2}
	global := cell.NewDict(256)
	if err := addLibraryPublisher(global, key, firstID, library); err != nil {
		t.Fatal(err)
	}
	if err := addLibraryPublisher(global, key, secondID, library); err != nil {
		t.Fatal(err)
	}

	addr := address.NewAddress(0, 0xff, firstID[:])
	c := testLibraryCollation(
		t,
		addr,
		testAccountLibraries(t, library, true),
		testAccountLibraries(t, library, false),
		global,
	)
	if err := c.updateMasterPublicLibraries(); err != nil {
		t.Fatal(err)
	}
	descriptor := loadTestGlobalLibrary(t, c.master.libraries, key)
	publishers, err := descriptor.publishers.LoadAll()
	if err != nil || len(publishers) != 1 {
		t.Fatalf("publisher count = %d, err = %v", len(publishers), err)
	}
	secondKey := cell.BeginCell().MustStoreSlice(secondID[:], 256).EndCell()
	if _, err := descriptor.publishers.LoadValue(secondKey); err != nil {
		t.Fatalf("remaining publisher is absent: %v", err)
	}
}

func TestMasterExecutionLibrariesConvertsAndValidatesDescriptors(t *testing.T) {
	library := cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell()
	key := library.HashKey()
	publisher := [32]byte{1}
	global := cell.NewDict(256)
	if err := addLibraryPublisher(global, key, publisher, library); err != nil {
		t.Fatal(err)
	}

	converted, err := masterExecutionLibraries(global)
	if err != nil {
		t.Fatal(err)
	}
	if len(converted) != 1 {
		t.Fatalf("execution library roots = %d, want 1", len(converted))
	}
	value, err := converted[0].AsDict(256).LoadValue(
		cell.BeginCell().MustStoreSlice(key[:], 256).EndCell(),
	)
	if err != nil {
		t.Fatal(err)
	}
	root, err := value.LoadRefCell()
	if err != nil || value.BitsLeft() != 0 || value.RefsNum() != 0 || root.HashKey() != key {
		t.Fatalf("converted library is malformed: err=%v bits=%d refs=%d", err, value.BitsLeft(), value.RefsNum())
	}

	wrongKey := key
	wrongKey[0] ^= 0xff
	malformed := cell.NewDict(256)
	descriptor, err := serializeGlobalLibrary(globalLibrary{
		root:       library,
		publishers: testPublisherDictionary(t, publisher),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err = malformed.Set(
		cell.BeginCell().MustStoreSlice(wrongKey[:], 256).EndCell(),
		descriptor,
	); err != nil {
		t.Fatal(err)
	}
	if _, err = masterExecutionLibraries(malformed); err == nil {
		t.Fatal("library descriptor whose root differs from its key was accepted")
	}
}

func TestMasterExecutionLibrariesEmpty(t *testing.T) {
	for _, libraries := range []*cell.Dictionary{nil, cell.NewDict(256)} {
		converted, err := masterExecutionLibraries(libraries)
		if err != nil {
			t.Fatal(err)
		}
		if converted != nil {
			t.Fatalf("empty libraries converted to %d roots", len(converted))
		}
	}
}

func testPublisherDictionary(t *testing.T, publisher [32]byte) *cell.Dictionary {
	t.Helper()
	dict := cell.NewDict(256)
	if err := dict.Set(
		cell.BeginCell().MustStoreSlice(publisher[:], 256).EndCell(),
		cell.BeginCell().EndCell(),
	); err != nil {
		t.Fatal(err)
	}
	return dict
}

func testLibraryCollation(
	t *testing.T,
	addr *address.Address,
	before, after *cell.Dictionary,
	global *cell.Dictionary,
) *collation {
	t.Helper()

	original := testLibraryShardAccount(t, addr, before)
	currentShard := testLibraryShardAccount(t, addr, after)
	current, err := tvm.PrepareAccount(currentShard, nil)
	if err != nil {
		t.Fatal(err)
	}
	transactions, err := tlb.NewAccountTransactionsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	key := [32]byte{}
	copy(key[:], addr.Data())
	return &collation{
		oldStats: tlb.ShardStateStats{Libraries: global},
		master:   &masterCollation{},
		lanes: map[[32]byte]*accountLane{
			key: {
				key:          key,
				original:     original,
				current:      current,
				transactions: transactions,
			},
		},
	}
}

func testLibraryShardAccount(
	t *testing.T,
	addr *address.Address,
	libraries *cell.Dictionary,
) *tlb.ShardAccount {
	t.Helper()

	stateInit, err := tlb.ToCell(&tlb.StateInit{
		Code: cell.BeginCell().EndCell(),
		Data: cell.BeginCell().EndCell(),
		Lib:  libraries,
	})
	if err != nil {
		t.Fatal(err)
	}
	storage, err := tlb.ToCell(&tlb.StorageInfo{
		StorageUsed: tlb.StorageUsed{
			CellsUsed: big.NewInt(0),
			BitsUsed:  big.NewInt(0),
		},
		StorageExtra: tlb.StorageExtraNone{},
	})
	if err != nil {
		t.Fatal(err)
	}
	account := cell.BeginCell().
		MustStoreBoolBit(true).
		MustStoreAddr(addr).
		MustStoreBuilder(storage.ToBuilder()).
		MustStoreUInt(0, 64).
		MustStoreBigCoins(big.NewInt(1_000_000_000)).
		MustStoreDict(nil).
		MustStoreBoolBit(true).
		MustStoreBuilder(stateInit.ToBuilder()).
		EndCell()
	return &tlb.ShardAccount{
		Account:       account,
		LastTransHash: make([]byte, 32),
	}
}

func testAccountLibraries(t *testing.T, library *cell.Cell, public bool) *cell.Dictionary {
	t.Helper()

	dict := cell.NewDict(256)
	key := cell.BeginCell().MustStoreSlice(library.Hash(), 256).EndCell()
	value := cell.BeginCell().MustStoreBoolBit(public).MustStoreRef(library).EndCell()
	if err := dict.Set(key, value); err != nil {
		t.Fatal(err)
	}
	return dict
}

func loadTestGlobalLibrary(t *testing.T, libraries *cell.Dictionary, key [32]byte) globalLibrary {
	t.Helper()

	value, err := libraries.LoadValue(cell.BeginCell().MustStoreSlice(key[:], 256).EndCell())
	if err != nil {
		t.Fatal(err)
	}
	descriptor, err := parseGlobalLibrary(value, key)
	if err != nil {
		t.Fatal(err)
	}
	return descriptor
}
