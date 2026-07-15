package blockproof

import (
	"fmt"
	"math/big"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type McStateExtraPrefix struct {
	_           tlb.Magic           `tlb:"#cc26"`
	ShardHashes *cell.Dictionary    `tlb:"dict 32"`
	Config      McStateConfigPrefix `tlb:"."`
	Info        *cell.Cell          `tlb:"^"`
}

type McStateConfigPrefix struct {
	ConfigAddr []byte     `tlb:"bits 256"`
	Config     *cell.Cell `tlb:"^"`
}

func OldMasterBlockIDFromState(stateRoot *cell.Cell, seqno uint32) (ton.BlockIDExt, error) {
	prefix, err := LoadMcStateExtraPrefix(stateRoot, false)
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

// LoadMcStateExtraPrefix extracts the mc_state_extra prefix from a masterchain
// state root. With visitHeader set it walks the full shard-state header first,
// so usage proofs built around it include the header cells; without it the
// loader jumps straight to the extra ref and leaves the header untouched.
func LoadMcStateExtraPrefix(stateRoot *cell.Cell, visitHeader bool) (McStateExtraPrefix, error) {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return McStateExtraPrefix{}, err
	}

	var custom *cell.Cell
	if visitHeader {
		state, err := VisitShardStateHeader(stateLoader)
		if err != nil {
			return McStateExtraPrefix{}, err
		}
		if state.McStateExtra == nil {
			return McStateExtraPrefix{}, fmt.Errorf("state is missing mc_state_extra")
		}
		custom = state.McStateExtra
	} else {
		custom, err = stateLoader.PeekRefCellAt(3)
		if err != nil {
			return McStateExtraPrefix{}, err
		}
	}

	var prefix McStateExtraPrefix
	loader, err := custom.BeginParse()
	if err != nil {
		return McStateExtraPrefix{}, err
	}
	if err = tlb.LoadFromCell(&prefix, loader); err != nil {
		return McStateExtraPrefix{}, err
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
