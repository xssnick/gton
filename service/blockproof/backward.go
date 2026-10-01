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

type MasterStateInfo struct {
	PrevBlocks    *tlb.OldMcBlocksInfoAugDict
	AfterKeyBlock bool
	LastKeyBlock  *tlb.ExtBlkRef
}

func LoadMasterStateInfo(info *cell.Cell) (MasterStateInfo, error) {
	if info == nil {
		return MasterStateInfo{}, fmt.Errorf("state is missing mc_state_extra info")
	}

	loader, err := info.BeginParse()
	if err != nil {
		return MasterStateInfo{}, err
	}
	if _, err = loader.LoadUInt(16); err != nil {
		return MasterStateInfo{}, err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return MasterStateInfo{}, err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return MasterStateInfo{}, err
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return MasterStateInfo{}, err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err = prevBlocks.LoadFromCell(loader); err != nil {
		return MasterStateInfo{}, err
	}

	afterKeyBlock, err := loader.LoadBoolBit()
	if err != nil {
		return MasterStateInfo{}, err
	}

	hasLastKeyBlock, err := loader.LoadBoolBit()
	if err != nil {
		return MasterStateInfo{}, err
	}

	var lastKeyBlock *tlb.ExtBlkRef
	if hasLastKeyBlock {
		ref := &tlb.ExtBlkRef{}
		if err = tlb.LoadFromCell(ref, loader); err != nil {
			return MasterStateInfo{}, err
		}
		lastKeyBlock = ref
	}

	return MasterStateInfo{
		PrevBlocks:    prevBlocks,
		AfterKeyBlock: afterKeyBlock,
		LastKeyBlock:  lastKeyBlock,
	}, nil
}

func OldMasterBlockIDFromState(stateRoot *cell.Cell, seqno uint32) (ton.BlockIDExt, error) {
	prefix, err := LoadMcStateExtraPrefix(stateRoot, false)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	info, err := LoadMasterStateInfo(prefix.Info)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	return OldMasterBlockID(info.PrevBlocks, seqno)
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

func OldMasterBlockID(prevBlocks *tlb.OldMcBlocksInfoAugDict, seqno uint32) (ton.BlockIDExt, error) {
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

	return masterExtBlkRefBlockID(ref.BlkRef), nil
}

func (info MasterStateInfo) LastKeyBlockID(master ton.BlockIDExt) (ton.BlockIDExt, error) {
	if info.AfterKeyBlock {
		return master, nil
	}
	if info.LastKeyBlock == nil {
		return ton.BlockIDExt{}, fmt.Errorf("cannot fetch last key block")
	}
	return masterExtBlkRefBlockID(*info.LastKeyBlock), nil
}

func masterExtBlkRefBlockID(ref tlb.ExtBlkRef) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     int64(-1 << 63),
		SeqNo:     ref.SeqNo,
		RootHash:  append([]byte(nil), ref.RootHash...),
		FileHash:  append([]byte(nil), ref.FileHash...),
	}
}
