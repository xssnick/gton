package state

import (
	"fmt"
	"math/bits"

	storage2 "github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

type PersistentStatePartKind uint8

const (
	PersistentStatePartUnsplit PersistentStatePartKind = iota
	PersistentStatePartSplitAccount
	PersistentStatePartSplitHeader
)

func (k PersistentStatePartKind) String() string {
	switch k {
	case PersistentStatePartUnsplit:
		return "unsplit"
	case PersistentStatePartSplitAccount:
		return "split_account"
	case PersistentStatePartSplitHeader:
		return "split_header"
	default:
		return fmt.Sprintf("unknown_%d", k)
	}
}

type PersistentStatePart struct {
	Kind           PersistentStatePartKind
	EffectiveShard int64
	Root           *cell.Cell
}

func SplitPersistentState(block ton.BlockIDExt, root *cell.Cell, splitDepth uint32) ([]PersistentStatePart, error) {
	shardPrefixLen := ShardPrefixLength(block.Shard)
	if block.Workchain == -1 || splitDepth <= uint32(shardPrefixLen) {
		return []PersistentStatePart{{
			Kind:           PersistentStatePartUnsplit,
			EffectiveShard: 0,
			Root:           root,
		}}, nil
	}
	if splitDepth > 63 {
		return nil, fmt.Errorf("invalid persistent state split depth %d", splitDepth)
	}

	proofBuilder := cell.NewMerkleProofBuilder(root)
	proofRoot := proofBuilder.Root()

	loader, err := proofRoot.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse shard state: %w", err)
	}

	var header tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&header, loader); err != nil {
		return nil, fmt.Errorf("parse shard state: %w", err)
	}
	if err := validatePersistentStateSplitSource(block, &header); err != nil {
		return nil, err
	}

	accounts := header.Accounts.ShardAccounts
	if accounts == nil || accounts.AugmentedDictionary == nil {
		return nil, fmt.Errorf("shard state has no accounts dict")
	}

	observedAccounts := accounts.Copy()

	partsCount := uint64(1) << (splitDepth - uint32(shardPrefixLen))
	effectiveShard := uint64(block.Shard) ^ (uint64(1) << (63 - shardPrefixLen)) ^ (uint64(1) << (63 - splitDepth))
	increment := uint64(1) << (64 - splitDepth)

	parts := make([]PersistentStatePart, 0)
	for i := uint64(0); i < partsCount; i++ {
		prefix := cell.BeginCell().MustStoreUInt(effectiveShard>>(64-splitDepth), uint(splitDepth)).EndCell()

		partRoot, err := observedAccounts.ExtractPrefixSubdictRoot(prefix, false)
		if err != nil {
			return nil, fmt.Errorf("cut accounts prefix %016x: %w", effectiveShard, err)
		}
		if partRoot != nil {
			wrappedPartRoot, err := WrapShardAccountsRoot(partRoot)
			if err != nil {
				return nil, fmt.Errorf("build split state part root %016x: %w", effectiveShard, err)
			}
			parts = append(parts, PersistentStatePart{
				Kind:           PersistentStatePartSplitAccount,
				EffectiveShard: int64(effectiveShard),
				Root:           wrappedPartRoot,
			})
		}

		effectiveShard += increment
	}

	if len(parts) == 0 {
		return nil, fmt.Errorf("split persistent state has no non-empty account parts")
	}

	if err := visitSplitPersistentStateHeader(proofRoot); err != nil {
		return nil, err
	}
	proof, err := proofBuilder.CreateProof()
	if err != nil {
		return nil, err
	}
	parts = append(parts, PersistentStatePart{
		Kind:           PersistentStatePartSplitHeader,
		EffectiveShard: block.Shard,
		Root:           proof,
	})
	return parts, nil
}

func validatePersistentStateSplitSource(block ton.BlockIDExt, state *tlb.ShardStateUnsplit) error {
	workchain, shard := tlb.ConvertShardIdentToShard(state.ShardIdent)
	if workchain != block.Workchain || int64(shard) != block.Shard {
		return fmt.Errorf("shard state block mismatch for %s: got wc=%d shard=%016x", storage2.FormatBlockRef(block), workchain, shard)
	}
	if state.Seqno != block.SeqNo {
		return fmt.Errorf("shard state seqno mismatch for %s: got %d", storage2.FormatBlockRef(block), state.Seqno)
	}
	return nil
}

func visitSplitPersistentStateHeader(root *cell.Cell) error {
	refs := int(root.RefsNum())
	if refs < 3 {
		return fmt.Errorf("shard state root has too few refs: %d", refs)
	}

	for i := 0; i < refs; i++ {
		if i == 1 {
			continue
		}
		ref, err := root.PeekRef(i)
		if err != nil {
			return err
		}
		if err = visitCellRecursive(ref); err != nil {
			return err
		}
	}

	return nil
}

func visitCellRecursive(root *cell.Cell) error {
	return visitCellRecursiveSeen(root, map[cell.Hash]struct{}{})
}

func visitCellRecursiveSeen(root *cell.Cell, seen map[cell.Hash]struct{}) error {
	if root == nil {
		return nil
	}
	key := root.HashKey()
	if _, ok := seen[key]; ok {
		return nil
	}
	seen[key] = struct{}{}

	loader, err := root.BeginParse()
	if err != nil {
		return err
	}
	for loader.RefsNum() > 0 {
		ref, err := loader.LoadRefCell()
		if err != nil {
			return err
		}
		if err = visitCellRecursiveSeen(ref, seen); err != nil {
			return err
		}
	}
	return nil
}

func WrapShardAccountsRoot(root *cell.Cell) (*cell.Cell, error) {
	extra, err := root.AsAugDict(256, tlb.AugShardAccounts{}).LoadRootExtra()
	if err != nil {
		return nil, err
	}

	extraCell, err := extra.ToCell()
	if err != nil {
		return nil, err
	}

	return cell.BeginCell().
		MustStoreUInt(1, 1).
		MustStoreRef(root).
		MustStoreBuilder(extraCell.ToBuilder()).
		EndCell(), nil
}

func NewShardAccountsAugDict() (*cell.AugmentedDictionary, error) {
	return cell.NewAugDict(256, tlb.AugShardAccounts{})
}

func LoadShardAccountsRoot(root *cell.Cell) (*cell.AugmentedDictionary, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}
	return loader.LoadAugDict(256, cell.ReadOnlyAugmentation{
		SkipExtraFn: tlb.AugShardAccounts{}.SkipExtra,
	}, false)
}

func MergeSplitState(header *tlb.ShardStateUnsplit, parts []*cell.Cell) (*cell.Cell, error) {
	accounts, err := NewShardAccountsAugDict()
	if err != nil {
		return nil, err
	}

	for i, root := range parts {
		partAccounts, err := LoadShardAccountsRoot(root)
		if err != nil {
			return nil, fmt.Errorf("parse split state part %d accounts: %w", i+1, err)
		}

		merged, err := accounts.CombineWith(partAccounts)
		if err != nil {
			return nil, fmt.Errorf("merge split state part %d accounts: %w", i+1, err)
		}
		if !merged {
			return nil, fmt.Errorf("duplicate account in split state part %d", i+1)
		}
	}

	return MergeSplitStateAccounts(header, accounts)
}

func MergeSplitStateAccounts(header *tlb.ShardStateUnsplit, accounts *cell.AugmentedDictionary) (*cell.Cell, error) {
	full := *header
	full.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: accounts}
	return tlb.ToCell(&full)
}

func ShardPrefixLength(shard int64) int {
	x := uint64(shard)
	lowBit := x & -x
	if lowBit == 0 {
		return 64
	}
	return 63 - bits.TrailingZeros64(lowBit)
}
