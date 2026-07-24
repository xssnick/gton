package service

import (
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func testBlockID(workchain int32, shard int64, seqno uint32) ton.BlockIDExt {
	root := testBlockHash(0x01, workchain, shard, seqno)
	file := testBlockHash(0x02, workchain, shard, seqno)
	return ton.BlockIDExt{
		Workchain: workchain,
		Shard:     shard,
		SeqNo:     seqno,
		RootHash:  root,
		FileHash:  file,
	}
}

func testBlockHash(kind byte, workchain int32, shard int64, seqno uint32) []byte {
	var data [17]byte
	data[0] = kind
	binary.BigEndian.PutUint32(data[1:5], uint32(workchain))
	binary.BigEndian.PutUint64(data[5:13], uint64(shard))
	binary.BigEndian.PutUint32(data[13:17], seqno)
	hash := sha256.Sum256(data[:])
	return append([]byte(nil), hash[:]...)
}

func testShardStateCell(tb testing.TB, block ton.BlockIDExt) *cell.Cell {
	tb.Helper()

	accounts, err := cell.NewAugDict(256, testShardAccountsAugmentation{})
	if err != nil {
		tb.Fatalf("create accounts dict: %v", err)
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
		tb.Fatalf("build shard state cell: %v", err)
	}
	return root
}

type testShardAccountsAugmentation struct{}

func newTestStateCellWindowCache(base cell.LazyCellLoader) *stateCellWindowCache {
	return newStateCellWindowCache(base, &lazyCellLoadCounters{})
}

func (testShardAccountsAugmentation) SkipExtra(loader *cell.Slice) error {
	if _, err := loader.LoadUInt(5); err != nil {
		return err
	}
	if _, err := loader.LoadBigCoins(); err != nil {
		return err
	}
	_, err := loader.LoadMaybeRef()
	return err
}

func (testShardAccountsAugmentation) EmptyExtra() (*cell.Cell, error) {
	return cell.BeginCell().
		MustStoreUInt(0, 5).
		MustStoreBigCoins(big.NewInt(0)).
		MustStoreDict(nil).
		EndCell(), nil
}

func (testShardAccountsAugmentation) LeafExtra(*cell.Slice) (*cell.Cell, error) {
	return nil, fmt.Errorf("test shard account leaf extra is not implemented")
}

func (testShardAccountsAugmentation) CombineExtra(*cell.Slice, *cell.Slice) (*cell.Cell, error) {
	return nil, fmt.Errorf("test shard account combine extra is not implemented")
}
