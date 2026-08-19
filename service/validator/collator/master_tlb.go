package collator

import (
	"errors"
	"fmt"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// parseMasterStateInfo handles the inline BlockCreateStats tail explicitly.
// tonutils-go exposes that tail with Slice.ToCell without consuming the source
// slice, so the generic parseExact helper would report valid creator stats as
// trailing data.
func parseMasterStateInfo(root *cell.Cell) (tlb.McStateExtraBlockInfo, error) {
	info, _, err := parseMasterStateInfoWithStats(root)
	return info, err
}

// parseMasterStateInfoWithStats additionally hands back the creator statistics
// dictionary. The returned dictionary is nil exactly when the info carries no
// statistics.
//
// Opening the statistics is not optional even for the callers that never read
// them: when the flag is set, that tail is the rest of the Info cell, so its
// header is the only thing standing between a malformed state and an accepted
// one. It costs a tag byte and one maybe-reference — the traversal that used to
// come with it is what moved to the passes that actually read entries.
func parseMasterStateInfoWithStats(root *cell.Cell) (tlb.McStateExtraBlockInfo, blockCreateStats, error) {
	if root == nil {
		return tlb.McStateExtraBlockInfo{}, blockCreateStats{}, errors.New("cell is absent")
	}
	loader, err := root.BeginParse()
	if err != nil {
		return tlb.McStateExtraBlockInfo{}, blockCreateStats{}, err
	}
	var info tlb.McStateExtraBlockInfo
	if err = info.LoadFromCell(loader); err != nil {
		return tlb.McStateExtraBlockInfo{}, blockCreateStats{}, err
	}
	if info.BlockCreateStats == nil {
		if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
			return tlb.McStateExtraBlockInfo{}, blockCreateStats{}, fmt.Errorf(
				"trailing data: %d bits, %d refs", loader.BitsLeft(), loader.RefsNum(),
			)
		}
		return info, blockCreateStats{}, nil
	}
	stats, err := openBlockCreateStats(info.BlockCreateStats)
	if err != nil {
		return tlb.McStateExtraBlockInfo{}, blockCreateStats{}, err
	}

	return info, stats, nil
}

// serializeMcBlockExtra stores masterchain_block_extra#cca5 exactly as defined
// by block.tlb. This is local until tonutils-go supports McBlockExtra.ToCell.
func serializeMcBlockExtra(extra *tlb.McBlockExtra) (*cell.Cell, error) {
	if extra == nil {
		return nil, fmt.Errorf("%w: masterchain block extra is nil", ErrInvalidInput)
	}
	if extra.ShardHashes == nil {
		return nil, fmt.Errorf("%w: masterchain shard hashes dictionary is nil", ErrInvalidInput)
	}
	if extra.ShardHashes.GetKeySize() != 32 {
		return nil, fmt.Errorf("%w: masterchain shard hashes key size is %d, want 32",
			ErrInvalidInput, extra.ShardHashes.GetKeySize())
	}
	if extra.ShardFees == nil || extra.ShardFees.AugmentedDictionary == nil {
		return nil, fmt.Errorf("%w: masterchain shard fees dictionary is nil", ErrInvalidInput)
	}
	if extra.ShardFees.GetKeySize() != 96 {
		return nil, fmt.Errorf("%w: masterchain shard fees key size is %d, want 96",
			ErrInvalidInput, extra.ShardFees.GetKeySize())
	}
	if extra.Details.PrevBlockSignatures == nil {
		return nil, fmt.Errorf("%w: previous block signatures dictionary is nil", ErrInvalidInput)
	}
	if extra.Details.PrevBlockSignatures.GetKeySize() != 16 {
		return nil, fmt.Errorf("%w: previous block signatures key size is %d, want 16",
			ErrInvalidInput, extra.Details.PrevBlockSignatures.GetKeySize())
	}

	if extra.KeyBlock {
		if extra.ConfigParams == nil {
			return nil, fmt.Errorf("%w: key block has no configuration parameters", ErrInvalidInput)
		}
		if len(extra.ConfigParams.ConfigAddr) != 32 {
			return nil, fmt.Errorf("%w: configuration address is %d bytes, want 32",
				ErrInvalidInput, len(extra.ConfigParams.ConfigAddr))
		}
		if extra.ConfigParams.Config.Params == nil {
			return nil, fmt.Errorf("%w: configuration parameters dictionary is nil", ErrInvalidInput)
		}
		if extra.ConfigParams.Config.Params.GetKeySize() != 32 {
			return nil, fmt.Errorf("%w: configuration parameters key size is %d, want 32",
				ErrInvalidInput, extra.ConfigParams.Config.Params.GetKeySize())
		}
		if extra.ConfigParams.Config.Params.IsEmpty() {
			return nil, fmt.Errorf("%w: configuration parameters dictionary is empty", ErrInvalidInput)
		}
	} else if extra.ConfigParams != nil {
		return nil, fmt.Errorf("%w: non-key block has configuration parameters", ErrInvalidInput)
	}

	shardFees, err := extra.ShardFees.ToCell()
	if err != nil {
		return nil, fmt.Errorf("%w: serialize masterchain shard fees: %v", ErrInvalidInput, err)
	}

	details := cell.BeginCell()
	if err = details.StoreDict(extra.Details.PrevBlockSignatures); err != nil {
		return nil, fmt.Errorf("%w: serialize previous block signatures: %v", ErrInvalidInput, err)
	}
	if err = details.StoreMaybeRef(extra.Details.RecoverCreateMsg); err != nil {
		return nil, fmt.Errorf("%w: serialize recover message: %v", ErrInvalidInput, err)
	}
	if err = details.StoreMaybeRef(extra.Details.MintMsg); err != nil {
		return nil, fmt.Errorf("%w: serialize mint message: %v", ErrInvalidInput, err)
	}

	b := cell.BeginCell()
	if err = b.StoreUInt(0xcca5, 16); err != nil {
		return nil, fmt.Errorf("serialize masterchain block extra tag: %w", err)
	}
	if err = b.StoreBoolBit(extra.KeyBlock); err != nil {
		return nil, fmt.Errorf("serialize masterchain key block flag: %w", err)
	}
	if err = b.StoreDict(extra.ShardHashes); err != nil {
		return nil, fmt.Errorf("%w: serialize masterchain shard hashes: %v", ErrInvalidInput, err)
	}
	if err = b.StoreBuilder(shardFees.ToBuilder()); err != nil {
		return nil, fmt.Errorf("%w: serialize inline masterchain shard fees: %v", ErrInvalidInput, err)
	}
	if err = b.StoreRef(details.EndCell()); err != nil {
		return nil, fmt.Errorf("%w: serialize masterchain block details: %v", ErrInvalidInput, err)
	}

	if extra.KeyBlock {
		config, configErr := tlb.ToCell(extra.ConfigParams)
		if configErr != nil {
			return nil, fmt.Errorf("%w: serialize masterchain configuration parameters: %v", ErrInvalidInput, configErr)
		}
		if err = b.StoreBuilder(config.ToBuilder()); err != nil {
			return nil, fmt.Errorf("%w: serialize inline masterchain configuration parameters: %v",
				ErrInvalidInput, err)
		}
	}

	return b.EndCell(), nil
}
