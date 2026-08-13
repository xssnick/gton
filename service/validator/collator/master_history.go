package collator

import (
	"fmt"
	"math"
	"math/big"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/tuple"
)

const masterHistoryTupleLimit = 16

// masterPrevBlocksTuple builds c7's PrevBlocksInfo from the predecessor state.
// It intentionally runs before the predecessor itself is appended to
// OldMcBlocksInfo: ConfigInfo::get_prev_blocks_info sees the current block ID
// separately and uses the dictionary only for older IDs.
func masterPrevBlocksTuple(
	previous ton.BlockIDExt,
	info *tlb.McStateExtraBlockInfo,
	globalVersion uint32,
) (tuple.Tuple, error) {
	recent := make([]any, 0, masterHistoryTupleLimit)
	recent = append(recent, masterBlockIDTuple(previous))
	for seqno := previous.SeqNo; seqno > 0 && len(recent) < masterHistoryTupleLimit; {
		seqno--
		old, err := oldMasterBlockID(info.PrevBlocks, seqno)
		if err != nil {
			return tuple.Tuple{}, err
		}
		recent = append(recent, masterBlockIDTuple(old))
	}

	lastKey, err := previousMasterKeyBlock(previous, info)
	if err != nil {
		return tuple.Tuple{}, err
	}
	values := make([]any, 0, 3)
	values = append(values, tuple.NewTupleOwned(recent), masterBlockIDTuple(lastKey))
	if globalVersion >= 9 {
		byHundred := make([]any, 0, masterHistoryTupleLimit)
		for seqno := previous.SeqNo / 100 * 100; len(byHundred) < masterHistoryTupleLimit; {
			old, loadErr := oldMasterBlockIDOrCurrent(previous, info.PrevBlocks, seqno)
			if loadErr != nil {
				return tuple.Tuple{}, loadErr
			}
			byHundred = append(byHundred, masterBlockIDTuple(old))
			if seqno < 100 {
				break
			}
			seqno -= 100
		}
		values = append(values, tuple.NewTupleOwned(byHundred))
	}

	return tuple.NewTupleOwned(values), nil
}

func oldMasterBlockIDOrCurrent(
	current ton.BlockIDExt,
	history *tlb.OldMcBlocksInfoAugDict,
	seqno uint32,
) (ton.BlockIDExt, error) {
	if seqno == current.SeqNo {
		return current, nil
	}
	return oldMasterBlockID(history, seqno)
}

func oldMasterBlockID(history *tlb.OldMcBlocksInfoAugDict, seqno uint32) (ton.BlockIDExt, error) {
	if history == nil || history.AugmentedDictionary == nil {
		return ton.BlockIDExt{}, fmt.Errorf("%w: masterchain block history is absent", ErrInvalidInput)
	}
	value, err := history.LoadValueByIntKey(new(big.Int).SetUint64(uint64(seqno)))
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("%w: cannot fetch old masterchain block %d: %v", ErrInvalidInput, seqno, err)
	}
	var ref tlb.KeyExtBlkRef
	if err = loadExactSlice(&ref, value); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("%w: decode old masterchain block %d: %v", ErrInvalidInput, seqno, err)
	}
	if ref.BlkRef.SeqNo != seqno || len(ref.BlkRef.RootHash) != 32 || len(ref.BlkRef.FileHash) != 32 {
		return ton.BlockIDExt{}, fmt.Errorf("%w: old masterchain block %d has an invalid reference", ErrInvalidInput, seqno)
	}

	return ton.BlockIDExt{
		Workchain: masterchainWorkchainID,
		Shard:     math.MinInt64,
		SeqNo:     ref.BlkRef.SeqNo,
		RootHash:  ref.BlkRef.RootHash,
		FileHash:  ref.BlkRef.FileHash,
	}, nil
}

func previousMasterKeyBlock(previous ton.BlockIDExt, info *tlb.McStateExtraBlockInfo) (ton.BlockIDExt, error) {
	if info.AfterKeyBlock {
		return previous, nil
	}
	if info.LastKeyBlock == nil || len(info.LastKeyBlock.RootHash) != 32 || len(info.LastKeyBlock.FileHash) != 32 {
		return ton.BlockIDExt{}, fmt.Errorf("%w: previous key block is absent or malformed", ErrInvalidInput)
	}

	return ton.BlockIDExt{
		Workchain: masterchainWorkchainID,
		Shard:     math.MinInt64,
		SeqNo:     info.LastKeyBlock.SeqNo,
		RootHash:  info.LastKeyBlock.RootHash,
		FileHash:  info.LastKeyBlock.FileHash,
	}, nil
}

func masterBlockIDTuple(id ton.BlockIDExt) tuple.Tuple {
	return tuple.NewTupleValue(
		big.NewInt(int64(id.Workchain)),
		new(big.Int).SetUint64(uint64(id.Shard)),
		new(big.Int).SetUint64(uint64(id.SeqNo)),
		new(big.Int).SetBytes(id.RootHash),
		new(big.Int).SetBytes(id.FileHash),
	)
}
