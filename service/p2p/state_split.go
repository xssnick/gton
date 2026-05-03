package p2p

import (
	"errors"
	"fmt"
	"math/big"
	"math/bits"

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

	var header tlb.ShardStateUnsplit
	if err = tlb.LoadFromCell(&header, root.BeginParse()); err != nil {
		return nil, nil, fmt.Errorf("parse split state header: %w", err)
	}
	if err = validateSplitStateHeader(block, &header); err != nil {
		return nil, nil, err
	}

	accounts := header.Accounts.ShardAccounts
	if accounts == nil {
		return nil, nil, fmt.Errorf("split state header has no accounts dict")
	}

	shardPrefixLen := shardPrefixLength(block.Shard)
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
			wrappedPartRoot, err := wrapShardAccountsRoot(partRoot)
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

func wrapShardAccountsRoot(root *cell.Cell) (*cell.Cell, error) {
	extra, err := root.AsAugDict(256, shardAccountsAugmentation{}).LoadRootExtra()
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
	emptyAccounts, err := cell.NewAugDict(256, shardAccountsAugmentation{})
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

func mergeSplitState(header *tlb.ShardStateUnsplit, parts []*cell.Cell) (*cell.Cell, error) {
	accounts, err := cell.NewAugDict(256, shardAccountsAugmentation{})
	if err != nil {
		return nil, err
	}

	for i, root := range parts {
		partAccounts, err := loadSplitStatePartAccounts(root)
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

	return mergeSplitStateAccounts(header, accounts)
}

func mergeSplitStateAccounts(header *tlb.ShardStateUnsplit, accounts *cell.AugmentedDictionary) (*cell.Cell, error) {
	full := *header
	full.Accounts.ShardAccounts = &tlb.ShardAccountsAugDict{AugmentedDictionary: accounts}
	return tlb.ToCell(&full)
}

func loadSplitStatePartAccounts(root *cell.Cell) (*cell.AugmentedDictionary, error) {
	return root.BeginParse().LoadAugDict(256, cell.ReadOnlyAugmentation{
		SkipExtraFn: shardAccountsAugmentation{}.SkipExtra,
	}, true)
}

type shardAccountsAugmentation struct{}

func (shardAccountsAugmentation) SkipExtra(loader *cell.Slice) error {
	if _, err := loadDepthBalanceInfo(loader); err != nil {
		return err
	}
	return nil
}

func (shardAccountsAugmentation) EmptyExtra() (*cell.Cell, error) {
	return storeDepthBalanceInfo(0, tlb.ZeroCoins, nil)
}

func (shardAccountsAugmentation) LeafExtra(value *cell.Slice) (*cell.Cell, error) {
	var account tlb.ShardAccount
	if err := tlb.LoadFromCell(&account, value); err != nil {
		return nil, err
	}
	if account.Account == nil {
		return storeDepthBalanceInfo(0, tlb.ZeroCoins, nil)
	}

	var state tlb.AccountState
	if err := tlb.LoadFromCell(&state, account.Account.BeginParse()); err != nil {
		return nil, err
	}
	if !state.IsValid {
		return storeDepthBalanceInfo(0, tlb.ZeroCoins, nil)
	}

	var depth uint64
	if anycast := state.Address.Anycast(); anycast != nil {
		depth = uint64(anycast.Depth())
	}
	return storeDepthBalanceInfo(depth, state.Balance, state.ExtraCurrencies)
}

func (shardAccountsAugmentation) CombineExtra(leftExtra, rightExtra *cell.Slice) (*cell.Cell, error) {
	left, err := loadDepthBalanceInfo(leftExtra)
	if err != nil {
		return nil, err
	}
	right, err := loadDepthBalanceInfo(rightExtra)
	if err != nil {
		return nil, err
	}

	currencies, err := addCurrencyCollections(left.Currencies, right.Currencies)
	if err != nil {
		return nil, err
	}
	depth := left.Depth
	if right.Depth > depth {
		depth = right.Depth
	}
	return storeDepthBalanceInfo(uint64(depth), currencies.Coins, currencies.ExtraCurrencies)
}

func loadDepthBalanceInfo(loader *cell.Slice) (tlb.DepthBalanceInfo, error) {
	var info tlb.DepthBalanceInfo
	err := tlb.LoadFromCell(&info, loader)
	return info, err
}

func storeDepthBalanceInfo(depth uint64, coins tlb.Coins, extra *cell.Dictionary) (*cell.Cell, error) {
	if depth > 30 {
		return nil, fmt.Errorf("invalid account depth %d", depth)
	}

	b := cell.BeginCell()
	if err := b.StoreUInt(depth, 5); err != nil {
		return nil, err
	}
	if err := b.StoreBigCoins(coins.Nano()); err != nil {
		return nil, err
	}
	if isEmptyDict(extra) {
		extra = nil
	}
	if err := b.StoreDict(extra); err != nil {
		return nil, err
	}
	return b.EndCell(), nil
}

func addCurrencyCollections(left, right tlb.CurrencyCollection) (tlb.CurrencyCollection, error) {
	coins, err := left.Coins.Add(right.Coins)
	if err != nil {
		return tlb.CurrencyCollection{}, err
	}

	extra, err := addExtraCurrencyDicts(left.ExtraCurrencies, right.ExtraCurrencies)
	if err != nil {
		return tlb.CurrencyCollection{}, err
	}
	return tlb.CurrencyCollection{Coins: coins, ExtraCurrencies: extra}, nil
}

func addExtraCurrencyDicts(left, right *cell.Dictionary) (*cell.Dictionary, error) {
	if isEmptyDict(left) && isEmptyDict(right) {
		return nil, nil
	}

	out := cell.NewDict(32)
	for _, dict := range []*cell.Dictionary{left, right} {
		if isEmptyDict(dict) {
			continue
		}

		items, err := dict.Range(false, false)
		if err != nil {
			return nil, err
		}
		for _, item := range items {
			amount, err := item.Value.LoadVarUInt(32)
			if err != nil {
				return nil, err
			}

			existing, err := out.LoadValue(item.Key)
			if err == nil {
				prev, loadErr := existing.LoadVarUInt(32)
				if loadErr != nil {
					return nil, loadErr
				}
				amount = new(big.Int).Add(prev, amount)
			} else if !errors.Is(err, cell.ErrNoSuchKeyInDict) {
				return nil, err
			}

			value := cell.BeginCell().MustStoreBigVarUInt(amount, 32).EndCell()
			if err = out.Set(item.Key, value); err != nil {
				return nil, err
			}
		}
	}
	return out, nil
}

func isEmptyDict(dict *cell.Dictionary) bool {
	return dict == nil || dict.IsEmpty()
}

func shardPrefixLength(shard int64) int {
	x := uint64(shard)
	lowBit := x & -x
	if lowBit == 0 {
		return 64
	}
	return 63 - bits.TrailingZeros64(lowBit)
}
