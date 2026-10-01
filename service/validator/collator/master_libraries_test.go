package collator

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"math/big"
	"os"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	vmcore "github.com/xssnick/tonutils-go/tvm/vm"
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

func TestMasterExecutionLibrariesUsesGlobalDescriptorIndexDirectly(t *testing.T) {
	library := cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell()
	key := library.HashKey()
	publisher := [32]byte{1}
	global := cell.NewDict(256)
	if err := addLibraryPublisher(global, key, publisher, library); err != nil {
		t.Fatal(err)
	}

	execution, err := masterExecutionLibraries(global)
	if err != nil {
		t.Fatal(err)
	}
	if len(execution) != 1 {
		t.Fatalf("execution library roots = %d, want 1", len(execution))
	}
	if execution[0].HashKey() != global.AsCell().HashKey() {
		t.Fatal("execution library collection differs from the LibDescr index")
	}

	state := &vmcore.State{GlobalVersion: 14}
	state.SetLibraries(execution...)
	resolved, err := state.LoadLibraryByHash(key[:])
	if err != nil {
		t.Fatal(err)
	}
	if resolved == nil || resolved.HashKey() != key {
		t.Fatal("TVM did not resolve the library from its LibDescr")
	}
}

func TestMasterExecutionLibrariesDefersDescriptorValidationToLookup(t *testing.T) {
	library := cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell()
	key := library.HashKey()
	wrongKey := key
	wrongKey[0] ^= 0xff
	publisher := [32]byte{1}

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
	execution, err := masterExecutionLibraries(malformed)
	if err != nil {
		t.Fatalf("unused inherited descriptor rejected before lookup: %v", err)
	}

	state := &vmcore.State{GlobalVersion: 14}
	state.SetLibraries(execution...)
	resolved, err := state.LoadLibraryByHash(wrongKey[:])
	if err != nil {
		t.Fatal(err)
	}
	if resolved != nil {
		t.Fatal("TVM accepted a library whose root differs from the requested hash")
	}
}

func TestMasterExecutionLibrariesEmpty(t *testing.T) {
	for _, libraries := range []*cell.Dictionary{nil, cell.NewDict(256)} {
		execution, err := masterExecutionLibraries(libraries)
		if err != nil {
			t.Fatal(err)
		}
		if execution != nil {
			t.Fatalf("empty libraries exposed as %d roots", len(execution))
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
	key := [32]byte{}
	copy(key[:], addr.Data())
	return &collation{
		oldStats: tlb.ShardStateStats{Libraries: global},
		master:   &masterCollation{},
		lanes: map[[32]byte]*accountLane{
			key: {
				key:      key,
				original: original,
				current:  current,
				touched:  true,
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

func TestMasterExecutionLibrariesPreservesLookupTrace(t *testing.T) {
	library := cell.BeginCell().MustStoreUInt(0x1234, 16).EndCell()
	publisher := [32]byte{1}
	global := cell.NewDict(256)
	if err := addLibraryPublisher(global, library.HashKey(), publisher, library); err != nil {
		t.Fatal(err)
	}

	loads := 0
	trace := cell.NewTrace(cell.TraceHooks{OnLoad: func(*cell.Cell) { loads++ }})
	traced := global.Copy().SetTrace(trace)

	execution, err := masterExecutionLibraries(traced)
	if err != nil {
		t.Fatal(err)
	}
	before := loads
	state := &vmcore.State{GlobalVersion: 14}
	state.SetLibraries(execution...)
	root, err := state.LoadLibraryByHash(library.Hash())
	if err != nil {
		t.Fatal(err)
	}
	if root == nil || root.HashKey() != library.HashKey() {
		t.Fatal("TVM did not resolve traced library")
	}
	if loads == before {
		t.Fatal("library lookup did not record its path in the predecessor trace")
	}
}

// benchMainnetLibraryIndex loads the public library index the mainnet fixture
// captured. It is the only realistically sized one available offline: 1322
// descriptors over roughly 750 KB of cells.
func benchMainnetLibraryIndex(tb testing.TB) *cell.Dictionary {
	tb.Helper()

	raw, err := os.ReadFile(benchMainnetFixturePath)
	if err != nil {
		tb.Skip(err)
	}
	var doc struct {
		Config struct {
			LibrariesBOCBase64 []string `json:"libraries_boc_base64"`
		} `json:"config"`
	}
	if err = json.Unmarshal(raw, &doc); err != nil {
		tb.Fatal(err)
	}
	if len(doc.Config.LibrariesBOCBase64) == 0 {
		tb.Skip("mainnet fixture carries no public libraries")
	}
	boc, err := base64.StdEncoding.DecodeString(doc.Config.LibrariesBOCBase64[0])
	if err != nil {
		tb.Fatal(err)
	}
	root, err := cell.FromBOC(boc)
	if err != nil {
		tb.Fatal(err)
	}
	return root.AsDict(256)
}

// BenchmarkMasterExecutionLibrariesMainnet keeps the hot-path contract honest:
// exposing an immutable LibDescr root must stay independent of index size.
func BenchmarkMasterExecutionLibrariesMainnet(b *testing.B) {
	libraries := benchMainnetLibraryIndex(b)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := masterExecutionLibraries(libraries); err != nil {
			b.Fatal(err)
		}
	}
}
