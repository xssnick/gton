package p2p

import (
	"fmt"

	tnstate "github.com/xssnick/gton/service/state"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type splitStatePart struct {
	effectiveShard uint64
	rootHash       []byte
}

func splitStateParts(block ton.BlockIDExt, proof *cell.Cell, splitDepth uint32, stateRootHash []byte) (*tlb.ShardStateUnsplit, []splitStatePart, error) {
	root, err := cell.UnwrapProofVirtualized(proof, stateRootHash)
	if err != nil {
		return nil, nil, fmt.Errorf("unwrap split state header proof: %w", err)
	}

	loader, err := root.BeginParse()
	if err != nil {
		return nil, nil, fmt.Errorf("begin parse split state header: %w", err)
	}

	var header tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&header, loader); err != nil {
		return nil, nil, fmt.Errorf("parse split state header: %w", err)
	}
	if err = validateSplitStateHeader(block, &header); err != nil {
		return nil, nil, err
	}

	accounts := header.Accounts.ShardAccounts
	if accounts == nil {
		return nil, nil, fmt.Errorf("split state header has no accounts dict")
	}

	shardPrefixLen := tnstate.ShardPrefixLength(block.Shard)
	if splitDepth <= uint32(shardPrefixLen) || splitDepth > 63 {
		return nil, nil, fmt.Errorf("invalid split depth %d for shard prefix length %d", splitDepth, shardPrefixLen)
	}

	partsCount := 1 << (splitDepth - uint32(shardPrefixLen))
	effectiveShard := uint64(block.Shard) ^ (uint64(1) << (63 - shardPrefixLen)) ^ (uint64(1) << (63 - splitDepth))
	increment := uint64(1) << (64 - splitDepth)

	parts := make([]splitStatePart, 0, partsCount)
	for i := 0; i < partsCount; i++ {
		prefix := cell.BeginCell().MustStoreUInt(effectiveShard>>(64-splitDepth), uint(splitDepth)).EndCell()

		partRoot, err := accounts.ExtractPrefixSubdictRoot(prefix, false)
		if err != nil {
			return nil, nil, fmt.Errorf("cut accounts prefix %016x: %w", effectiveShard, err)
		}
		if partRoot != nil {
			wrappedPartRoot, err := tnstate.WrapShardAccountsRoot(partRoot)
			if err != nil {
				return nil, nil, fmt.Errorf("build split state part root %016x: %w", effectiveShard, err)
			}
			rootHash := wrappedPartRoot.HashKey()
			parts = append(parts, splitStatePart{
				effectiveShard: effectiveShard,
				rootHash:       rootHash[:],
			})
		}

		effectiveShard += increment
	}

	if len(parts) == 0 {
		return nil, nil, fmt.Errorf("split state header has no non-empty account parts")
	}
	if err = validateSplitStateHeaderPruning(&header); err != nil {
		return nil, nil, err
	}
	return &header, parts, nil
}

func validateSplitStateHeader(block ton.BlockIDExt, state *tlb.ShardStateUnsplit) error {
	workchain, shard := tlb.ConvertShardIdentToShard(state.ShardIdent)
	if workchain != block.Workchain || int64(shard) != block.Shard {
		return fmt.Errorf("split state shard mismatch for %s: got wc=%d shard=%016x", formatBlockRef(block), workchain, shard)
	}
	if state.Seqno != block.SeqNo {
		return fmt.Errorf("split state seqno mismatch for %s: got %d", formatBlockRef(block), state.Seqno)
	}
	return nil
}

func validateSplitStateHeaderPruning(header *tlb.ShardStateUnsplit) error {
	emptyAccounts, err := tnstate.NewShardAccountsAugDict()
	if err != nil {
		return err
	}

	copyHeader := *header
	copyHeader.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: emptyAccounts}

	root, err := tlb.ToCell(&copyHeader)
	if err != nil {
		return fmt.Errorf("rebuild split state header: %w", err)
	}
	if root.IsVirtualized() {
		return fmt.Errorf("split state header is pruned outside accounts dict")
	}
	return nil
}
