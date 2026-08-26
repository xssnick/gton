package collator

import "github.com/xssnick/tonutils-go/tvm/cell"

func dispatchAccountKey(accountID [32]byte) *cell.Cell {
	return cell.BeginCell().MustStoreSlice(accountID[:], 256).EndCell()
}

func dispatchLTKey(lt uint64) *cell.Cell {
	return cell.BeginCell().MustStoreUInt(lt, 64).EndCell()
}
