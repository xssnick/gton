package liveview

import (
	"bytes"
	"errors"
	"fmt"
	"math"
	"math/bits"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func stateRootHashFromBlock(id ton.BlockIDExt, root *cell.Cell) ([]byte, error) {
	if _, err := storage.ParseVerifiedBlockCell(id, root); err != nil {
		return nil, err
	}

	rootLoader, err := root.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("load block root %s: %w", storage.FormatBlockRef(id), err)
	}

	update, err := rootLoader.PeekRefCellAt(2)
	if err != nil {
		return nil, fmt.Errorf("block %s has no state update: %w", storage.FormatBlockRef(id), err)
	}
	updateLoader, err := update.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("load block state update %s: %w", storage.FormatBlockRef(id), err)
	}

	nextState, err := updateLoader.PeekRefCellAt(1)
	if err != nil {
		return nil, fmt.Errorf("load block state update target %s: %w", storage.FormatBlockRef(id), err)
	}

	hash := nextState.HashKey(0)
	return bytes.Clone(hash[:]), nil
}

func createUsageProof(root *cell.Cell, visit func(*cell.Cell) error) (*cell.Cell, error) {
	builder := newProofBuilder(root)
	if visit != nil {
		if err := visit(builder.Root()); err != nil {
			return nil, err
		}
	}
	return builder.CreateProof()
}

func newProofBuilder(root *cell.Cell) *cell.MerkleProofBuilder {
	if root != nil {
		root = root.Virtualize(0)
	}
	return cell.NewMerkleProofBuilder(root)
}

func accountStateProofAndCell(stateRoot *cell.Cell, accountID []byte) (*cell.Cell, *cell.Cell, error) {
	var accountCell *cell.Cell

	proof, err := createUsageProof(stateRoot, func(root *cell.Cell) error {
		loader, err := root.BeginParse()
		if err != nil {
			return err
		}
		if _, err = visitShardStateHeader(loader); err != nil {
			return err
		}

		dictRoot, err := accountsDictRoot(root)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}

		value, err := dictRoot.AsDict(256).LoadValue(accountKey(accountID))
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return nil
		}
		if err != nil {
			return err
		}

		if err = tlb.LoadFromCell(new(tlb.DepthBalanceInfo), value.Copy()); err != nil {
			return err
		}

		accountValue := value.Copy().WithoutTrace()
		var balance tlb.DepthBalanceInfo
		if err = tlb.LoadFromCell(&balance, accountValue); err != nil {
			return err
		}

		var account tlb.ShardAccount
		if err = tlb.LoadFromCell(&account, accountValue); err != nil {
			return err
		}
		accountCell = account.Account
		return nil
	})
	if err != nil {
		return nil, nil, err
	}
	return proof, accountCell, nil
}

func accountPrunedProof(account *cell.Cell) (*cell.Cell, error) {
	source, err := account.BeginParse()
	if err != nil {
		return nil, err
	}

	root := source.BaseCell().WithTrace(nil)
	builder := newProofBuilder(root)
	loader, err := builder.Root().BeginParse()
	if err != nil {
		return nil, err
	}
	if err = visitPrunedAccount(loader); err != nil {
		return nil, err
	}
	return builder.CreateProof()
}

func visitPrunedAccount(loader *cell.Slice) error {
	isAccount, err := loader.LoadBoolBit()
	if err != nil || !isAccount {
		return err
	}

	if _, err = loader.LoadAddr(); err != nil {
		return err
	}

	var storageInfo tlb.StorageInfo
	if err = tlb.LoadFromCell(&storageInfo, loader); err != nil {
		return err
	}

	if _, err = loader.LoadUInt(64); err != nil {
		return err
	}
	if _, err = loader.LoadBigCoins(); err != nil {
		return err
	}

	extraCurrencies, err := loader.LoadDict(32)
	if err != nil {
		return err
	}
	if extraCurrencies != nil && !extraCurrencies.IsEmpty() {
		if _, err = extraCurrencies.LoadAll(); err != nil {
			return err
		}
	}

	active, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if active {
		return skipPrunedStateInit(loader)
	}

	frozen, err := loader.LoadBoolBit()
	if err != nil || !frozen {
		return err
	}
	_, err = loader.LoadSlice(256)
	return err
}

func skipPrunedStateInit(loader *cell.Slice) error {
	hasDepth, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if hasDepth {
		if _, err = loader.LoadUInt(5); err != nil {
			return err
		}
	}

	hasTickTock, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if hasTickTock {
		if _, err = loader.LoadBoolBit(); err != nil {
			return err
		}
		if _, err = loader.LoadBoolBit(); err != nil {
			return err
		}
	}

	for range 3 {
		hasRef, err := loader.LoadBoolBit()
		if err != nil {
			return err
		}
		if hasRef {
			if err = loader.SkipBitsAndRefs(0, 1); err != nil {
				return err
			}
		}
	}
	return nil
}

func accountCellFromAccountsRoot(dictRoot *cell.Cell, accountID []byte) (*cell.Cell, error) {
	if dictRoot == nil {
		return nil, cell.ErrNoSuchKeyInDict
	}
	value, err := dictRoot.AsDict(256).LoadValue(accountKey(accountID))
	if err != nil {
		return nil, fmt.Errorf("load account value from accounts dict: %w", err)
	}

	var balance tlb.DepthBalanceInfo
	if err = tlb.LoadFromCell(&balance, value); err != nil {
		return nil, fmt.Errorf("load account depth balance: %w", err)
	}

	var account tlb.ShardAccount
	if err = tlb.LoadFromCell(&account, value); err != nil {
		return nil, fmt.Errorf("load shard account: %w", err)
	}

	return account.Account, nil
}

func accountsDictRoot(stateRoot *cell.Cell) (*cell.Cell, error) {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse shard state root: %w", err)
	}
	accounts, err := stateLoader.PeekRefCellAt(1)
	if err != nil {
		return nil, fmt.Errorf("load shard state accounts ref: %w", err)
	}

	loader, err := accounts.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse shard accounts cell: %w", err)
	}
	hasRoot, err := loader.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("load shard accounts dict flag: %w", err)
	}
	if !hasRoot {
		return nil, storage.ErrNotFound
	}

	root, err := loader.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load shard accounts dict root ref: %w", err)
	}
	return root, nil
}

func accountKey(accountID []byte) *cell.Cell {
	return cell.BeginCell().MustStoreSlice(accountID, 256).EndCell()
}

func blockStateRootProof(root *cell.Cell) (*cell.Cell, error) {
	if root != nil {
		root = root.Virtualize(0)
	}

	rootLoader, err := visitBlockRoot(root)
	if err != nil {
		return nil, err
	}

	info, err := rootLoader.PeekRefCellAt(0)
	if err != nil {
		return nil, err
	}
	info, err = info.Prewarm()
	if err != nil {
		return nil, err
	}
	valueFlow, err := rootLoader.PeekRefCellAt(1)
	if err != nil {
		return nil, err
	}
	update, err := rootLoader.PeekRefCellAt(2)
	if err != nil {
		return nil, fmt.Errorf("block has no state update: %w", err)
	}
	extra, err := rootLoader.PeekRefCellAt(3)
	if err != nil {
		return nil, err
	}

	proofUpdate, err := blockStateRootProofUpdate(update)
	if err != nil {
		return nil, err
	}
	valueFlowProof, err := cell.CreatePrunedBranch(valueFlow, 1, 0)
	if err != nil {
		return nil, err
	}
	extraProof, err := cell.CreatePrunedBranch(extra, 1, 0)
	if err != nil {
		return nil, err
	}
	body, err := root.RebuildWithRefs([]*cell.Cell{info, valueFlowProof, proofUpdate, extraProof})
	if err != nil {
		return nil, err
	}
	return cell.CreateMerkleProof(body)
}

func blockStateRootProofUpdate(update *cell.Cell) (*cell.Cell, error) {
	loader, err := update.BeginParse()
	if err != nil {
		return nil, err
	}
	if loader.BaseCell().GetType() != cell.MerkleUpdateCellType {
		return nil, fmt.Errorf("invalid Merkle update in block")
	}
	if loader.RefsNum() < 2 {
		return nil, fmt.Errorf("invalid Merkle update in block")
	}

	from, err := loader.PeekRefCellAt(0)
	if err != nil {
		return nil, err
	}
	to, err := loader.PeekRefCellAt(1)
	if err != nil {
		return nil, err
	}

	fromProof, err := cell.CreatePrunedBranch(from, 2, 1)
	if err != nil {
		return nil, err
	}
	toProof, err := cell.CreatePrunedBranch(to, 2, 1)
	if err != nil {
		return nil, err
	}

	return cell.CreateMerkleUpdate(fromProof, toProof)
}

func shardHashesProof(stateRoot *cell.Cell, workchain int32, shard int64, exact bool) (*cell.Cell, error) {
	return createUsageProof(stateRoot, func(root *cell.Cell) error {
		loader, err := root.BeginParse()
		if err != nil {
			return err
		}

		state, err := visitShardStateHeader(loader)
		if err != nil {
			return err
		}
		if state.McStateExtra == nil {
			return fmt.Errorf("state is missing mc_state_extra")
		}

		extraLoader, err := state.McStateExtra.BeginParse()
		if err != nil {
			return err
		}

		var extra tlb.McStateExtra
		if err = tlb.LoadFromCell(&extra, extraLoader); err != nil {
			return err
		}
		if extra.Info != nil {
			if err = visitMcStateExtraInfo(extra.Info); err != nil {
				return err
			}
		}
		if extra.ShardHashes == nil {
			return fmt.Errorf("state is missing shard hashes")
		}

		value, err := extra.ShardHashes.LoadValue(cell.BeginCell().MustStoreInt(int64(workchain), 32).EndCell())
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return nil
		}
		if err != nil {
			return err
		}

		root, err = value.LoadRefCell()
		if err != nil {
			return err
		}

		trueShard, leaf, err := findShardLeaf(root, uint64(shard), exact)
		if errors.Is(err, storage.ErrNotFound) {
			return nil
		}
		if err != nil {
			return err
		}
		_, err = blockIDFromShardLeaf(workchain, trueShard, leaf)
		return err
	})
}

func visitShardStateHeader(loader *cell.Slice) (tlb.ShardStateUnsplit, error) {
	var state tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&state, loader); err != nil {
		return tlb.ShardStateUnsplit{}, err
	}
	if err := visitShardStateStats(state.Stats); err != nil {
		return tlb.ShardStateUnsplit{}, err
	}
	return state, nil
}

func visitShardStateStats(stats *cell.Cell) error {
	if stats == nil {
		return fmt.Errorf("state is missing shard state stats")
	}

	loader, err := stats.BeginParse()
	if err != nil {
		return err
	}
	if _, err = loader.LoadUInt(64); err != nil {
		return err
	}
	if _, err = loader.LoadUInt(64); err != nil {
		return err
	}
	if err = skipCurrencyCollection(loader); err != nil {
		return err
	}
	if err = skipCurrencyCollection(loader); err != nil {
		return err
	}
	if _, err = loader.LoadDict(256); err != nil {
		return err
	}

	hasMasterRef, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if hasMasterRef {
		var masterRef tlb.ExtBlkRef
		if err = tlb.LoadFromCell(&masterRef, loader); err != nil {
			return err
		}
	}
	return nil
}

func visitMcStateExtraInfo(info *cell.Cell) error {
	loader, err := info.BeginParse()
	if err != nil {
		return err
	}
	flags, err := loader.LoadUInt(16)
	if err != nil {
		return err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return err
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err = prevBlocks.LoadFromCell(loader); err != nil {
		return err
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return err
	}
	hasLastKeyBlock, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if hasLastKeyBlock {
		var lastKey tlb.ExtBlkRef
		if err = tlb.LoadFromCell(&lastKey, loader); err != nil {
			return err
		}
	}
	if flags&1 != 0 {
		return visitBlockCreateStats(loader)
	}
	return nil
}

func visitBlockCreateStats(loader *cell.Slice) error {
	magic, err := loader.LoadUInt(8)
	if err != nil {
		return err
	}
	switch magic {
	case 0x17:
		_, err = loader.LoadDict(256)
		return err
	case 0x34:
		_, err = loader.LoadAugDict(256, cell.ReadOnlyAugmentation{SkipExtraFn: skipUint32Boundary}, false)
		return err
	default:
		return fmt.Errorf("invalid block_create_stats magic %x", magic)
	}
}

func skipUint32Boundary(loader *cell.Slice) error {
	_, err := loader.LoadUInt(32)
	return err
}

func visitBlockRoot(root *cell.Cell) (*cell.Slice, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}
	magic, err := loader.LoadUInt(32)
	if err != nil {
		return nil, err
	}
	if magic != 0x11ef55aa {
		return nil, fmt.Errorf("invalid block magic %x", magic)
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return nil, err
	}
	if loader.RefsNum() < 4 {
		return nil, fmt.Errorf("block has too few refs")
	}
	return loader, nil
}

func skipCurrencyCollection(loader *cell.Slice) error {
	if _, err := loader.LoadBigCoins(); err != nil {
		return err
	}
	_, err := loader.LoadMaybeRef()
	return err
}

func mcStateExtra(root *cell.Cell) (*tlb.McStateExtra, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}

	var state tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&state, loader); err != nil {
		return nil, err
	}
	if state.McStateExtra == nil {
		return nil, fmt.Errorf("state is missing mc_state_extra")
	}

	extraLoader, err := state.McStateExtra.BeginParse()
	if err != nil {
		return nil, err
	}

	var extra tlb.McStateExtra
	if err := tlb.LoadFromCell(&extra, extraLoader); err != nil {
		return nil, err
	}
	return &extra, nil
}

func findShardLeaf(root *cell.Cell, shard uint64, exact bool) (int64, *cell.Cell, error) {
	prefixLen := shardPrefixLen(shard)
	if prefixLen < 0 {
		return 0, nil, storage.ErrNotFound
	}

	node := root
	z := shard
	m := uint64(math.MaxUint64)
	remaining := prefixLen
	for {
		loader, err := node.BeginParse()
		if err != nil {
			return 0, nil, err
		}
		typ, err := loader.LoadUInt(1)
		if err != nil {
			return 0, nil, err
		}
		if typ == 0 {
			if remaining != 0 && exact {
				return 0, nil, storage.ErrNotFound
			}
			trueShard := (shard | m) - (m >> 1)
			return int64(trueShard), node, nil
		}

		if remaining == 0 || loader.BitsLeft() != 0 || loader.RefsNum() != 2 {
			return 0, nil, storage.ErrNotFound
		}

		node, err = loader.BaseCell().PeekRef(int(z >> 63))
		if err != nil {
			return 0, nil, err
		}
		z <<= 1
		remaining--
		m >>= 1
	}
}

func blockIDFromShardLeaf(wc int32, shard int64, leaf *cell.Cell) (ton.BlockIDExt, error) {
	loader, err := leaf.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	typ, err := loader.LoadUInt(1)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if typ != 0 {
		return ton.BlockIDExt{}, fmt.Errorf("shard leaf has invalid tag")
	}

	magic, err := loader.LoadUInt(4)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	switch magic {
	case 0xa:
		var desc tlb.ShardDesc
		if err = tlb.LoadFromCell(&desc, loader, true); err != nil {
			return ton.BlockIDExt{}, err
		}
		return ton.BlockIDExt{Workchain: wc, Shard: shard, SeqNo: desc.SeqNo, RootHash: desc.RootHash, FileHash: desc.FileHash}, nil
	case 0xb:
		var desc tlb.ShardDescB
		if err = tlb.LoadFromCell(&desc, loader, true); err != nil {
			return ton.BlockIDExt{}, err
		}
		return ton.BlockIDExt{Workchain: wc, Shard: shard, SeqNo: desc.SeqNo, RootHash: desc.RootHash, FileHash: desc.FileHash}, nil
	default:
		return ton.BlockIDExt{}, fmt.Errorf("wrong ShardDesc magic: %x", magic)
	}
}

func shardPrefixLen(shard uint64) int {
	if shard == 0 {
		return -1
	}
	low := shard & -shard
	if low == 0 {
		return -1
	}
	return 63 - bits.TrailingZeros64(low)
}

func shardIntersects(a, b storage.ShardKey) bool {
	if a.Workchain != b.Workchain {
		return false
	}
	aLen := shardPrefixLen(uint64(a.Shard))
	bLen := shardPrefixLen(uint64(b.Shard))
	if aLen < 0 || bLen < 0 {
		return false
	}
	minLen := aLen
	if bLen < minLen {
		minLen = bLen
	}
	return shardPrefix(uint64(a.Shard), minLen) == shardPrefix(uint64(b.Shard), minLen)
}

func shardPrefix(shard uint64, length int) uint64 {
	if length <= 0 {
		return 0
	}
	return shard >> (64 - length)
}

func librariesDict(stateRoot *cell.Cell) (*cell.Dictionary, error) {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return nil, err
	}

	var state tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&state, stateLoader); err != nil {
		return nil, err
	}
	if state.Stats == nil {
		return nil, fmt.Errorf("state is missing shard state extras")
	}

	loader, err := state.Stats.BeginParse()
	if err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}
	if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}
	return loader.LoadDict(256)
}
