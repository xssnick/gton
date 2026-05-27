package blockproof

import (
	"fmt"
	"math/big"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type mcStateExtraPrefix struct {
	_           tlb.Magic           `tlb:"#cc26"`
	ShardHashes *cell.Dictionary    `tlb:"dict 32"`
	Config      mcStateConfigPrefix `tlb:"."`
	Info        *cell.Cell          `tlb:"^"`
}

type mcStateConfigPrefix struct {
	ConfigAddr []byte     `tlb:"bits 256"`
	Config     *cell.Cell `tlb:"^"`
}

func OldMasterBlockIDFromState(stateRoot *cell.Cell, seqno uint32) (ton.BlockIDExt, error) {
	prefix, err := loadMcStateExtraPrefix(stateRoot)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	loader, err := prefix.Info.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadUInt(16); err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return ton.BlockIDExt{}, err
	}
	if _, err := loader.LoadBoolBit(); err != nil {
		return ton.BlockIDExt{}, err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err := prevBlocks.LoadFromCell(loader); err != nil {
		return ton.BlockIDExt{}, err
	}
	return oldMasterBlockID(prevBlocks, seqno)
}

func loadMcStateExtraPrefix(stateRoot *cell.Cell) (mcStateExtraPrefix, error) {
	if stateRoot == nil {
		return mcStateExtraPrefix{}, fmt.Errorf("masterchain state root is nil")
	}
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return mcStateExtraPrefix{}, err
	}
	custom, err := stateLoader.PeekRefCellAt(3)
	if err != nil {
		return mcStateExtraPrefix{}, err
	}

	var prefix mcStateExtraPrefix
	loader, err := custom.BeginParse()
	if err != nil {
		return mcStateExtraPrefix{}, err
	}
	if err = tlb.LoadFromCell(&prefix, loader); err != nil {
		return mcStateExtraPrefix{}, err
	}
	return prefix, nil
}

func oldMasterBlockID(prevBlocks *tlb.OldMcBlocksInfoAugDict, seqno uint32) (ton.BlockIDExt, error) {
	if prevBlocks == nil || prevBlocks.IsEmpty() {
		return ton.BlockIDExt{}, fmt.Errorf("cannot fetch old mc block")
	}

	value, err := prevBlocks.LoadValueByIntKey(new(big.Int).SetUint64(uint64(seqno)))
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("cannot fetch old mc block: %w", err)
	}

	var ref tlb.KeyExtBlkRef
	if err = tlb.LoadFromCell(&ref, value); err != nil {
		return ton.BlockIDExt{}, err
	}
	if ref.BlkRef.SeqNo != seqno {
		return ton.BlockIDExt{}, fmt.Errorf("old mc block seqno mismatch: got %d want %d", ref.BlkRef.SeqNo, seqno)
	}

	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     ref.BlkRef.SeqNo,
		RootHash:  append([]byte(nil), ref.BlkRef.RootHash...),
		FileHash:  append([]byte(nil), ref.BlkRef.FileHash...),
	}, nil
}
