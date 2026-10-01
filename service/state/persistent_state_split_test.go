package state

import (
	"errors"
	"math/big"
	"testing"

	"github.com/xssnick/gton/internal/shardstate"
	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestSplitPersistentStateRejectsZeroShard(t *testing.T) {
	_, err := SplitPersistentState(ton.BlockIDExt{Workchain: 0}, nil, 1)
	if !errors.Is(err, shard.ErrInvalidID) {
		t.Fatalf("SplitPersistentState() error = %v, want ErrInvalidID", err)
	}
}

func TestSplitPersistentStateBuildsHeaderAndAccountParts(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     42,
	}
	root := mustTestPersistentStateSplitRoot(t, block,
		new(big.Int).Lsh(big.NewInt(0), 254),
		new(big.Int).Lsh(big.NewInt(1), 254),
		new(big.Int).Lsh(big.NewInt(2), 254),
		new(big.Int).Lsh(big.NewInt(3), 254),
	)

	parts, err := SplitPersistentState(block, root, 2)
	if err != nil {
		t.Fatalf("split persistent state: %v", err)
	}
	if len(parts) != 5 {
		t.Fatalf("parts count = %d, want 5", len(parts))
	}

	expectedShards := []int64{
		int64(0x2000000000000000),
		int64(0x6000000000000000),
		-int64(0x6000000000000000),
		-int64(0x2000000000000000),
	}
	for i, expected := range expectedShards {
		part := parts[i]
		if part.Kind != PersistentStatePartSplitAccount {
			t.Fatalf("part %d kind = %s, want split account", i, part.Kind)
		}
		if part.EffectiveShard != expected {
			t.Fatalf("part %d effective shard = %016x, want %016x", i, uint64(part.EffectiveShard), uint64(expected))
		}
		if part.Root == nil {
			t.Fatalf("part %d root is nil", i)
		}
	}

	headerPart := parts[len(parts)-1]
	if headerPart.Kind != PersistentStatePartSplitHeader {
		t.Fatalf("header kind = %s, want split header", headerPart.Kind)
	}
	if headerPart.EffectiveShard != block.Shard {
		t.Fatalf("header effective shard = %016x, want %016x", uint64(headerPart.EffectiveShard), uint64(block.Shard))
	}

	headerRoot, err := cell.UnwrapProofVirtualized(headerPart.Root, root.Hash())
	if err != nil {
		t.Fatalf("unwrap split header proof: %v", err)
	}

	var header tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&header, headerRoot.MustBeginParse()); err != nil {
		t.Fatalf("parse split header: %v", err)
	}
	accounts := header.Accounts.ShardAccounts
	if accounts == nil {
		t.Fatal("split header has no accounts")
	}

	for i, part := range parts[:len(parts)-1] {
		prefix := cell.BeginCell().MustStoreUInt(uint64(part.EffectiveShard)>>62, 2).EndCell()
		partRoot, err := accounts.ExtractPrefixSubdictRoot(prefix, false)
		if err != nil {
			t.Fatalf("extract header part %d: %v", i, err)
		}
		wrapped, err := shardstate.WrapAccountsRoot(partRoot)
		if err != nil {
			t.Fatalf("wrap header part %d: %v", i, err)
		}
		if wrapped.HashKey() != part.Root.HashKey() {
			t.Fatalf("part %d root hash mismatch", i)
		}
	}

	emptyAccounts, err := cell.NewAugDict(256, tlb.AugShardAccounts{})
	if err != nil {
		t.Fatalf("create empty accounts dict: %v", err)
	}
	header.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: emptyAccounts}
	repacked, err := tlb.ToCell(&header)
	if err != nil {
		t.Fatalf("repack split header without accounts: %v", err)
	}
	if repacked.IsVirtualized() {
		t.Fatal("split header is pruned outside accounts dict")
	}
}

func TestSplitPersistentStateReturnsUnsplitWhenDepthDoesNotCrossShard(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     42,
	}
	root := mustTestPersistentStateSplitRoot(t, block, big.NewInt(1))

	parts, err := SplitPersistentState(block, root, 0)
	if err != nil {
		t.Fatalf("split persistent state: %v", err)
	}
	if len(parts) != 1 {
		t.Fatalf("parts count = %d, want 1", len(parts))
	}
	if parts[0].Kind != PersistentStatePartUnsplit || parts[0].EffectiveShard != 0 || parts[0].Root != root {
		t.Fatalf("unexpected unsplit part: %+v", parts[0])
	}
}

func TestSplitPersistentStateHeaderProvesEmptyPrefixes(t *testing.T) {
	block := ton.BlockIDExt{
		Workchain: 0,
		Shard:     int64(-1 << 63),
		SeqNo:     42,
	}
	root := mustTestPersistentStateSplitRoot(t, block,
		new(big.Int).Lsh(big.NewInt(0), 254),
		new(big.Int).Lsh(big.NewInt(2), 254),
	)

	parts, err := SplitPersistentState(block, root, 2)
	if err != nil {
		t.Fatalf("split persistent state: %v", err)
	}
	if len(parts) != 3 {
		t.Fatalf("parts count = %d, want 3", len(parts))
	}

	headerRoot, err := cell.UnwrapProofVirtualized(parts[len(parts)-1].Root, root.Hash())
	if err != nil {
		t.Fatalf("unwrap split header proof: %v", err)
	}

	var header tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&header, headerRoot.MustBeginParse()); err != nil {
		t.Fatalf("parse split header: %v", err)
	}

	for _, prefixValue := range []uint64{1, 3} {
		prefix := cell.BeginCell().MustStoreUInt(prefixValue, 2).EndCell()
		partRoot, err := header.Accounts.ShardAccounts.ExtractPrefixSubdictRoot(prefix, false)
		if err != nil {
			t.Fatalf("extract empty prefix %d: %v", prefixValue, err)
		}
		if partRoot != nil {
			t.Fatalf("empty prefix %d returned non-empty root", prefixValue)
		}
	}
}

func mustTestPersistentStateSplitRoot(t *testing.T, block ton.BlockIDExt, accountIDs ...*big.Int) *cell.Cell {
	t.Helper()

	accounts, err := cell.NewAugDict(256, tlb.AugShardAccounts{})
	if err != nil {
		t.Fatalf("create accounts dict: %v", err)
	}

	for i, accountID := range accountIDs {
		account, err := tlb.ToCell(&tlb.ShardAccount{
			Account:       cell.BeginCell().MustStoreBoolBit(false).EndCell(),
			LastTransHash: make([]byte, 32),
			LastTransLT:   uint64(i + 1),
		})
		if err != nil {
			t.Fatalf("build account: %v", err)
		}
		if err = accounts.Set(cell.BeginCell().MustStoreSlice(accountID.FillBytes(make([]byte, 32)), 256).EndCell(), account); err != nil {
			t.Fatalf("set account: %v", err)
		}
	}

	state := tlb.ShardStateUnsplit{
		GlobalID: -239,
		ShardIdent: tlb.ShardIdent{
			PrefixBits:  0,
			WorkchainID: block.Workchain,
			ShardPrefix: 0,
		},
		Seqno:           block.SeqNo,
		OutMsgQueueInfo: cell.BeginCell().EndCell(),
		Stats:           cell.BeginCell().EndCell(),
	}
	state.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: accounts}

	root, err := tlb.ToCell(&state)
	if err != nil {
		t.Fatalf("build shard state: %v", err)
	}
	return root
}
