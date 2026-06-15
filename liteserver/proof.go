package liteserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	configModeNeedStateRoot     uint32 = 1
	configModeNeedLibraries     uint32 = 2
	configModeNeedStateExtra    uint32 = 4
	configModeNeedShardHashes   uint32 = 8
	configModeNeedValidatorSet  uint32 = 16
	configModeNeedSpecialSmc    uint32 = 32
	configModeNeedAccountsRoot  uint32 = 64
	configModeNeedPrevBlocks    uint32 = 128
	configModeNeedWorkchainInfo uint32 = 256
	configModeNeedCapabilities  uint32 = 512
	configModePreviousKeyBlock  uint32 = 0x8000
)

func (s *Server) blockHeader(ctx context.Context, id ton.BlockIDExt, mode uint32) (ton.BlockHeader, error) {
	root, err := s.loadBlockRoot(ctx, id)
	if err != nil {
		return ton.BlockHeader{}, err
	}

	proof, err := blockHeaderProof(root, id, mode)
	if err != nil {
		return ton.BlockHeader{}, err
	}

	return ton.BlockHeader{
		ID:          cloneBlockID(id),
		Mode:        mode,
		HeaderProof: proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
	}, nil
}

func (s *Server) accountProof(ctx context.Context, block ton.BlockIDExt, accountID []byte, pruned bool) ([]*cell.Cell, *cell.Cell, error) {
	fragments, err := s.blockFragments(ctx, block)
	if err != nil {
		return nil, nil, err
	}
	return fragments.accountProof(accountID, pruned)
}

func (s *Server) masterShardProof(ctx context.Context, master ton.BlockIDExt, addr *address.Address) ([]*cell.Cell, ton.BlockIDExt, error) {
	fragments, err := s.blockFragments(ctx, master)
	if err != nil {
		return nil, ton.BlockIDExt{}, err
	}

	extra, err := fragments.mcStateExtra()
	if err != nil {
		return nil, ton.BlockIDExt{}, err
	}

	leafShard := accountLeafShard(addr)
	stateProof, err := fragments.shardHashesProof(addr.Workchain(), leafShard, false)
	if err != nil {
		return nil, ton.BlockIDExt{}, err
	}

	proof := []*cell.Cell{fragments.blockStateRootProof, stateProof}

	shardBlock, _, err := shardInfoFromHashes(extra.ShardHashes, addr.Workchain(), leafShard, false)
	if err != nil {
		return proof, ton.BlockIDExt{}, err
	}

	return proof, shardBlock, nil
}

func accountLeafShard(addr *address.Address) int64 {
	return int64(binary.BigEndian.Uint64(addr.Data()[:8]) | 1)
}

func createUsageProof(root *cell.Cell, visit func(*cell.Cell) error) (*cell.Cell, error) {
	builder := newLiteServerProofBuilder(root)
	if visit != nil {
		if err := visit(builder.Root()); err != nil {
			return nil, err
		}
	}
	return builder.CreateProof()
}

func newLiteServerProofBuilder(root *cell.Cell) *cell.MerkleProofBuilder {
	if root != nil {
		root = root.Virtualize(0)
	}
	return cell.NewMerkleProofBuilder(root)
}

func (s *Server) loadBlockRoot(ctx context.Context, id ton.BlockIDExt) (*cell.Cell, error) {
	root, err := s.store.BlockRoot(ctx, id)
	if err != nil {
		return nil, err
	}
	return root, nil
}

func (s *Server) loadStateRoot(ctx context.Context, id ton.BlockIDExt) (*cell.Cell, error) {
	stateRootHash, err := s.loadStateRootHash(ctx, id)
	if err != nil {
		return nil, err
	}

	root, err := s.store.LoadStateCellTree(ctx, id, stateRootHash)
	if err != nil {
		return nil, fmt.Errorf("load state root %x for %s: %w", stateRootHash, storage.FormatBlockRef(id), err)
	}
	return root, nil
}

func (s *Server) loadStateRootWithBlockRoot(ctx context.Context, id ton.BlockIDExt) (*cell.Cell, *cell.Cell, error) {
	stateRootHash, err := s.loadStateRootHash(ctx, id)
	if err != nil {
		return nil, nil, err
	}

	blockRoot, err := s.loadBlockRoot(ctx, id)
	if err != nil {
		return nil, nil, fmt.Errorf("load block root for state %s: %w", storage.FormatBlockRef(id), err)
	}

	blockStateRootHash, err := stateRootHashFromBlock(id, blockRoot)
	if err != nil {
		return nil, nil, err
	}
	if !bytes.Equal(blockStateRootHash, stateRootHash) {
		return nil, nil, fmt.Errorf("state root hash mismatch for %s: meta=%x block=%x", storage.FormatBlockRef(id), stateRootHash, blockStateRootHash)
	}

	root, err := s.store.LoadStateCellTree(ctx, id, stateRootHash)
	if err != nil {
		return nil, nil, fmt.Errorf("load state root %x for %s: %w", stateRootHash, storage.FormatBlockRef(id), err)
	}
	return root, blockRoot, nil
}

func (s *Server) loadStateRootHash(ctx context.Context, id ton.BlockIDExt) ([]byte, error) {
	meta, err := s.store.BlockMeta(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load block meta for state %s: %w", storage.FormatBlockRef(id), err)
	}
	if len(meta.StateRootHash) != 32 {
		return nil, fmt.Errorf("state root hash is missing for %s", storage.FormatBlockRef(id))
	}
	return bytes.Clone(meta.StateRootHash), nil
}

func (s *Server) blockFragments(ctx context.Context, block ton.BlockIDExt) (*liveBlockFragments, error) {
	return s.store.BlockFragments(ctx, block)
}

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
	builder := newLiteServerProofBuilder(root)
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

func blockHeaderProof(root *cell.Cell, id ton.BlockIDExt, mode uint32) (*cell.Cell, error) {
	return createUsageProof(root, func(root *cell.Cell) error {
		return visitBlockHeader(root, id, mode)
	})
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

func allShardsInfoProof(root *cell.Cell) (*cell.Cell, error) {
	return createUsageProof(root, func(root *cell.Cell) error {
		extra, err := loadBlockExtra(root)
		if err != nil {
			return err
		}
		if extra.Custom == nil || extra.Custom.ShardHashes == nil {
			return fmt.Errorf("masterchain block extra is missing shard hashes")
		}
		return nil
	})
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

func visitBlockHeader(root *cell.Cell, id ton.BlockIDExt, mode uint32) error {
	rootLoader, err := visitBlockRoot(root)
	if err != nil {
		return err
	}

	info, err := rootLoader.PeekRefCellAt(0)
	if err != nil {
		return err
	}
	if err = visitCellRecursive(info); err != nil {
		return err
	}

	if mode&1 != 0 {
		update, err := rootLoader.PeekRefCellAt(2)
		if err != nil {
			return fmt.Errorf("block %s has no state update: %w", storage.FormatBlockRef(id), err)
		}
		if err = visitMerkleUpdate(update); err != nil {
			return err
		}
	}
	if mode&2 != 0 {
		valueFlow, err := rootLoader.PeekRefCellAt(1)
		if err != nil {
			return fmt.Errorf("block %s has no value flow: %w", storage.FormatBlockRef(id), err)
		}
		if err = visitCellRecursive(valueFlow); err != nil {
			return err
		}
	}
	if mode&16 != 0 {
		extra, err := loadBlockExtra(root)
		if err != nil {
			return err
		}
		if id.Workchain == masterchainID {
			if extra.Custom == nil {
				return fmt.Errorf("masterchain block extra is missing custom data")
			}
			if mode&32 != 0 {
				if extra.Custom.ShardHashes == nil {
					return fmt.Errorf("masterchain block extra is missing shard hashes")
				}
				if err = visitCellRecursive(extra.Custom.ShardHashes.AsCell()); err != nil {
					return err
				}
			}
			if mode&64 != 0 && extra.Custom.Details.PrevBlockSignatures != nil {
				if err = visitCellRecursive(extra.Custom.Details.PrevBlockSignatures.AsCell()); err != nil {
					return err
				}
			}
		}
	}

	return nil
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

func visitMerkleUpdate(update *cell.Cell) error {
	loader, err := update.BeginParse()
	if err != nil {
		return err
	}
	if loader.BaseCell().GetType() != cell.MerkleUpdateCellType {
		return fmt.Errorf("invalid Merkle update in block")
	}
	if loader.RefsNum() < 2 {
		return fmt.Errorf("invalid Merkle update in block")
	}
	to, err := loader.PeekRefCellAt(1)
	if err != nil {
		return err
	}
	if err = visitCell(to); err != nil {
		return err
	}
	return nil
}

func loadBlockHeader(root *cell.Cell) (tlb.BlockHeader, error) {
	rootLoader, err := root.BeginParse()
	if err != nil {
		return tlb.BlockHeader{}, err
	}
	info, err := rootLoader.PeekRefCellAt(0)
	if err != nil {
		return tlb.BlockHeader{}, err
	}
	loader, err := info.BeginParse()
	if err != nil {
		return tlb.BlockHeader{}, err
	}

	var header tlb.BlockHeader
	if err = tlb.LoadFromCell(&header, loader); err != nil {
		return tlb.BlockHeader{}, err
	}
	return header, nil
}

func loadBlockExtra(root *cell.Cell) (tlb.BlockExtra, error) {
	rootLoader, err := root.BeginParse()
	if err != nil {
		return tlb.BlockExtra{}, err
	}
	extra, err := rootLoader.PeekRefCellAt(3)
	if err != nil {
		return tlb.BlockExtra{}, err
	}
	loader, err := extra.BeginParse()
	if err != nil {
		return tlb.BlockExtra{}, err
	}

	var blockExtra tlb.BlockExtra
	if err = tlb.LoadFromCell(&blockExtra, loader); err != nil {
		return tlb.BlockExtra{}, err
	}
	return blockExtra, nil
}

func visitCell(root *cell.Cell) error {
	if root == nil {
		return nil
	}
	_, err := root.BeginParse()
	return err
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
			return fmt.Errorf("load proof ref: %w", err)
		}
		if err = visitCellRecursiveSeen(ref, seen); err != nil {
			return err
		}
	}
	return nil
}

func visitSliceRefsRecursive(sl *cell.Slice) error {
	if sl == nil {
		return nil
	}

	loader := sl.Copy()
	seen := map[cell.Hash]struct{}{}
	for loader.RefsNum() > 0 {
		ref, err := loader.LoadRefCell()
		if err != nil {
			return fmt.Errorf("load proof ref: %w", err)
		}
		if err = visitCellRecursiveSeen(ref, seen); err != nil {
			return err
		}
	}
	return nil
}

func configProof(stateRoot *cell.Cell, mode uint32, all bool, params []int32) (*cell.Cell, error) {
	return createUsageProof(stateRoot, func(root *cell.Cell) error {
		prefix, err := loadMcStateExtraPrefix(root)
		if err != nil {
			return err
		}
		if err = visitMcStateExtraInfo(prefix.Info); err != nil {
			return err
		}
		if err = visitConfigDict(prefix.Config.Config, mode, all, params); err != nil {
			return err
		}

		if mode&configModeNeedShardHashes != 0 {
			if err = visitMcStateShardHashes(root); err != nil {
				return err
			}
		}
		if mode&configModeNeedPrevBlocks != 0 {
			seqno, err := shardStateSeqno(root)
			if err != nil {
				return err
			}
			globalVersion, err := configGlobalVersion(prefix.Config.Config)
			if err != nil {
				return err
			}
			if err = visitMcStatePrevBlocks(prefix.Info, seqno, globalVersion.Version); err != nil {
				return err
			}
		}
		if mode&configModeNeedAccountsRoot != 0 {
			rootLoader, err := root.BeginParse()
			if err != nil {
				return err
			}
			accounts, err := rootLoader.PeekRefCellAt(1)
			if err != nil {
				return err
			}
			if err = visitCell(accounts); err != nil {
				return err
			}
		}
		if mode&configModeNeedLibraries != 0 {
			if err = visitStateLibraries(root); err != nil {
				return err
			}
		}
		return nil
	})
}

func keyBlockConfigProof(root *cell.Cell, mode uint32, all bool, params []int32) (*cell.Cell, error) {
	return createUsageProof(root, func(root *cell.Cell) error {
		rootLoader, err := visitBlockRoot(root)
		if err != nil {
			return err
		}
		header, err := loadBlockHeader(root)
		if err != nil {
			return err
		}

		configParams, err := loadKeyBlockConfigParams(root)
		if err != nil {
			return err
		}
		if !header.KeyBlock || configParams == nil {
			return fmt.Errorf("key block is missing config params")
		}

		info, err := rootLoader.PeekRefCellAt(0)
		if err != nil {
			return err
		}
		if err = visitCellRecursive(info); err != nil {
			return err
		}
		return visitConfigDict(configParams.AsCell(), mode, all, params)
	})
}

func loadKeyBlockConfigParams(root *cell.Cell) (*cell.Dictionary, error) {
	rootLoader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}
	extra, err := rootLoader.PeekRefCellAt(3)
	if err != nil {
		return nil, err
	}

	loader, err := extra.BeginParse()
	if err != nil {
		return nil, err
	}
	magic, err := loader.LoadUInt(32)
	if err != nil {
		return nil, err
	}
	if magic != 0x4a33f6fd {
		return nil, fmt.Errorf("invalid block extra magic %x", magic)
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, err
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, err
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, err
	}
	if err = loader.SkipBits(512); err != nil {
		return nil, err
	}
	hasCustom, err := loader.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if !hasCustom {
		return nil, fmt.Errorf("key block is missing custom data")
	}
	custom, err := loader.LoadRefCell()
	if err != nil {
		return nil, err
	}

	customLoader, err := custom.BeginParse()
	if err != nil {
		return nil, err
	}
	magic, err = customLoader.LoadUInt(16)
	if err != nil {
		return nil, err
	}
	if magic != 0xcca5 {
		return nil, fmt.Errorf("invalid masterchain block extra magic %x", magic)
	}
	keyBlock, err := customLoader.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if _, err = customLoader.LoadDict(32); err != nil {
		return nil, err
	}
	if err = skipShardFeesAugDict(customLoader); err != nil {
		return nil, err
	}
	details, err := customLoader.LoadRefCell()
	if err != nil {
		return nil, err
	}
	if err = visitMcBlockExtraDetails(details); err != nil {
		return nil, err
	}
	if !keyBlock {
		return nil, fmt.Errorf("block is not key block")
	}

	var config tlb.ConfigParams
	if err = tlb.LoadFromCell(&config, customLoader); err != nil {
		return nil, err
	}
	if config.Config.Params == nil {
		return nil, fmt.Errorf("key block config params dictionary is missing")
	}
	return config.Config.Params, nil
}

func visitMcBlockExtraDetails(details *cell.Cell) error {
	loader, err := details.BeginParse()
	if err != nil {
		return err
	}
	if _, err = loader.LoadDict(16); err != nil {
		return err
	}
	if _, err = loadMaybeRefCell(loader); err != nil {
		return err
	}
	_, err = loadMaybeRefCell(loader)
	return err
}

func skipShardFeesAugDict(loader *cell.Slice) error {
	if _, err := loadMaybeRefCell(loader); err != nil {
		return err
	}
	return skipShardFeeCreated(loader)
}

func skipShardFeeCreated(loader *cell.Slice) error {
	if err := skipCurrencyCollection(loader); err != nil {
		return err
	}
	return skipCurrencyCollection(loader)
}

func skipCurrencyCollection(loader *cell.Slice) error {
	if _, err := loader.LoadBigCoins(); err != nil {
		return err
	}
	_, err := loader.LoadMaybeRef()
	return err
}

func visitConfigDict(configRoot *cell.Cell, mode uint32, all bool, params []int32) error {
	if configRoot == nil {
		return fmt.Errorf("configuration root not set")
	}

	paramsDict := configRoot.AsDict(32)

	if all {
		if err := visitCellRecursive(configRoot); err != nil {
			return err
		}
	}

	if mode&configModeNeedValidatorSet != 0 {
		if err := markValidatorSetConfigParam(paramsDict); err != nil {
			return err
		}
	}
	if mode&configModeNeedSpecialSmc != 0 {
		if _, err := markConfigParam(paramsDict, int32(tlb.ConfigParamFundamentalSMCAddresses)); err != nil {
			return err
		}
	}
	if mode&configModeNeedWorkchainInfo != 0 {
		if _, err := markConfigParam(paramsDict, int32(tlb.ConfigParamWorkchains)); err != nil {
			return err
		}
	}
	if mode&configModeNeedCapabilities != 0 {
		if value, err := markConfigParam(paramsDict, int32(tlb.ConfigParamGlobalVersion)); err != nil {
			return err
		} else if value != nil {
			if _, err = globalVersionFromConfigValue(value); err != nil {
				return err
			}
		}
	}

	if !all {
		params = append([]int32(nil), params...)
		sort.Slice(params, func(i, j int) bool { return params[i] < params[j] })

		for i, param := range params {
			if i > 0 && params[i-1] == param {
				continue
			}
			if _, err := markConfigParam(paramsDict, param); err != nil {
				return err
			}
		}
	}

	return nil
}

func markConfigParam(dict *cell.Dictionary, param int32) (*cell.Slice, error) {
	value, err := dict.LoadValueByIntKey(big.NewInt(int64(param)))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	if err = visitSliceRefsRecursive(value); err != nil {
		return nil, err
	}
	return value, nil
}

func markValidatorSetConfigParam(dict *cell.Dictionary) error {
	if _, err := markConfigParam(dict, int32(tlb.ConfigParamCatchainConfig)); err != nil {
		return err
	}

	if _, err := markConfigParam(dict, int32(tlb.ConfigParamCurrentValidators)); err != nil {
		return err
	}

	_, err := markConfigParam(dict, int32(tlb.ConfigParamCurrentTempValidators))
	return err
}

func globalVersionFromConfigValue(value *cell.Slice) (tlb.GlobalVersion, error) {
	ref, err := value.Copy().LoadRefCell()
	if err != nil {
		return tlb.GlobalVersion{}, err
	}

	var globalVersion tlb.GlobalVersion
	loader, err := ref.BeginParse()
	if err != nil {
		return tlb.GlobalVersion{}, err
	}
	if err = tlb.LoadFromCell(&globalVersion, loader); err != nil {
		return tlb.GlobalVersion{}, fmt.Errorf("cannot extract global blockchain version and capabilities from GlobalVersion in configuration parameter #8")
	}
	return globalVersion, nil
}

func configGlobalVersion(configRoot *cell.Cell) (tlb.GlobalVersion, error) {
	value, err := configRoot.AsDict(32).LoadValueByIntKey(big.NewInt(int64(tlb.ConfigParamGlobalVersion)))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return tlb.GlobalVersion{}, nil
	}
	if err != nil {
		return tlb.GlobalVersion{}, err
	}
	return globalVersionFromConfigValue(value)
}

func shardStateSeqno(stateRoot *cell.Cell) (uint32, error) {
	var state tlb.ShardStateUnsplit
	loader, err := stateRoot.BeginParse()
	if err != nil {
		return 0, err
	}
	if err := tlb.LoadFromCell(&state, loader); err != nil {
		return 0, err
	}
	return state.Seqno, nil
}

func visitStateLibraries(stateRoot *cell.Cell) error {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return err
	}
	stats, err := stateLoader.PeekRefCellAt(2)
	if err != nil {
		return err
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
	if err = tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return err
	}
	if err = tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return err
	}
	if _, err = loader.LoadDict(256); err != nil {
		return err
	}

	return nil
}

func visitMcStateShardHashes(stateRoot *cell.Cell) error {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return err
	}
	custom, err := stateLoader.PeekRefCellAt(3)
	if err != nil {
		return err
	}

	loader, err := custom.BeginParse()
	if err != nil {
		return err
	}
	if _, err = loader.LoadUInt(16); err != nil {
		return err
	}
	shards, err := loader.LoadDict(32)
	if err != nil {
		return err
	}
	if shards == nil {
		return nil
	}
	return visitCellRecursive(shards.AsCell())
}

func visitMcStatePrevBlocks(info *cell.Cell, seqno uint32, globalVersion uint32) error {
	loader, err := info.BeginParse()
	if err != nil {
		return err
	}
	if _, err := loader.LoadUInt(16); err != nil {
		return err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return err
	}
	if _, err := loader.LoadBoolBit(); err != nil {
		return err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err := prevBlocks.LoadFromCell(loader); err != nil {
		return err
	}

	afterKeyBlock, err := loader.LoadBoolBit()
	if err != nil {
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
	if !afterKeyBlock && !hasLastKeyBlock {
		return fmt.Errorf("cannot fetch last key block")
	}

	for _, prevSeqno := range configPrevBlockProofSeqnos(seqno, globalVersion) {
		value, err := prevBlocks.LoadValueByIntKey(new(big.Int).SetUint64(uint64(prevSeqno)))
		if err != nil {
			return fmt.Errorf("cannot fetch old mc block seqno=%d: %w", prevSeqno, err)
		}
		var ref tlb.KeyExtBlkRef
		if err = tlb.LoadFromCell(&ref, value); err != nil {
			return err
		}
		if ref.BlkRef.SeqNo != prevSeqno {
			return fmt.Errorf("old mc block seqno mismatch: got %d want %d", ref.BlkRef.SeqNo, prevSeqno)
		}
	}

	return nil
}

func configPrevBlockProofSeqnos(seqno uint32, globalVersion uint32) []uint32 {
	seen := map[uint32]struct{}{}
	seqnos := make([]uint32, 0, 33)
	add := func(prevSeqno uint32) {
		if prevSeqno == seqno {
			return
		}
		if _, ok := seen[prevSeqno]; ok {
			return
		}
		seen[prevSeqno] = struct{}{}
		seqnos = append(seqnos, prevSeqno)
	}

	if seqno > 0 {
		add(0)
		for prev, count := seqno, 0; prev > 0 && count < 15; count++ {
			prev--
			add(prev)
		}
	}

	if globalVersion >= 9 {
		for prev, count := seqno/100*100, 0; count < 16; count++ {
			add(prev)
			if prev < 100 {
				break
			}
			prev -= 100
		}
	}
	return seqnos
}

func librariesProof(stateRoot *cell.Cell, hashes [][]byte, mode uint32) (*cell.Cell, error) {
	return createUsageProof(stateRoot, func(root *cell.Cell) error {
		return visitLibraries(root, hashes, mode)
	})
}

func visitLibraries(stateRoot *cell.Cell, hashes [][]byte, mode uint32) error {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return err
	}

	state, err := visitShardStateHeader(stateLoader)
	if err != nil {
		return err
	}
	stats := state.Stats

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
	if err = tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return err
	}
	if err = tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return err
	}

	libraries, err := loader.LoadDict(256)
	if err != nil {
		return err
	}
	for _, hash := range uniqueHashes(hashes, 16) {
		value, err := libraries.LoadValue(accountKey(hash))
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			continue
		}
		if err != nil {
			return err
		}
		if _, err = value.LoadUInt(2); err != nil {
			return err
		}
		if _, err = value.LoadRefCell(); err != nil {
			return err
		}

		publishers, err := value.LoadDict(256)
		if err != nil {
			return err
		}
		if mode&1 == 0 {
			continue
		}
		publishers = publishers.Copy()
		for i := 0; i < 16; i++ {
			_, value, err := publishers.LoadMinAndDelete()
			if errors.Is(err, cell.ErrNoSuchKeyInDict) {
				break
			}
			if err != nil {
				return err
			}
			if err = visitSliceRefsRecursive(value); err != nil {
				return err
			}
		}
	}

	return nil
}

func validatorStatsProofAndCount(stateRoot *cell.Cell, mode uint32, limit int32, startAfter []byte) (*cell.Cell, int32, bool, error) {
	var count int32
	var complete bool

	proof, err := createUsageProof(stateRoot, func(root *cell.Cell) error {
		var err error
		count, complete, err = validatorStatsCount(root, mode, limit, startAfter)
		return err
	})
	if err != nil {
		return nil, 0, false, err
	}
	return proof, count, complete, nil
}

type mcStateExtraPrefix struct {
	_            tlb.Magic           `tlb:"#cc26"`
	ShardHashes  *cell.Dictionary    `tlb:"dict 32"`
	Config       mcStateConfigPrefix `tlb:"."`
	Info         *cell.Cell          `tlb:"^"`
	configRefIdx int                 `tlb:"-"`
	infoRefIdx   int                 `tlb:"-"`
}

type mcStateConfigPrefix struct {
	ConfigAddr []byte     `tlb:"bits 256"`
	Config     *cell.Cell `tlb:"^"`
}

func loadMcStateExtraPrefix(stateRoot *cell.Cell) (mcStateExtraPrefix, error) {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return mcStateExtraPrefix{}, err
	}

	state, err := visitShardStateHeader(stateLoader)
	if err != nil {
		return mcStateExtraPrefix{}, err
	}
	if state.McStateExtra == nil {
		return mcStateExtraPrefix{}, fmt.Errorf("state is missing mc_state_extra")
	}

	var prefix mcStateExtraPrefix
	loader, err := state.McStateExtra.BeginParse()
	if err != nil {
		return mcStateExtraPrefix{}, err
	}
	if err = tlb.LoadFromCell(&prefix, loader); err != nil {
		return mcStateExtraPrefix{}, err
	}
	if prefix.ShardHashes != nil && !prefix.ShardHashes.IsEmpty() {
		prefix.configRefIdx++
	}
	prefix.infoRefIdx = prefix.configRefIdx + 1

	return prefix, nil
}

func outMsgQueueSizeProof(stateRoot *cell.Cell) (*cell.Cell, error) {
	return createUsageProof(stateRoot, func(root *cell.Cell) error {
		_, err := outMsgQueueSize(root)
		return err
	})
}

func loadOutMsgQueueSize(queue *cell.Cell) (uint64, error) {
	loader, err := queue.BeginParse()
	if err != nil {
		return 0, err
	}

	var info tlb.OutMsgQueueInfo
	if err := tlb.LoadFromCell(&info, loader); err != nil {
		return 0, err
	}
	if info.Extra == nil {
		return 0, fmt.Errorf("no out_msg_queue_size in shard state")
	}
	if info.Extra.OutQueueSize == nil {
		return 0, fmt.Errorf("no out_msg_queue_size in shard state")
	}
	return *info.Extra.OutQueueSize, nil
}

func loadMaybeRefCell(loader *cell.Slice) (bool, error) {
	has, err := loader.LoadBoolBit()
	if err != nil {
		return false, err
	}
	if !has {
		return false, nil
	}
	_, err = loader.LoadRefCell()
	return true, err
}

func skipOldMcBlocksInfoAugDict(loader *cell.Slice) (bool, error) {
	has, err := loadMaybeRefCell(loader)
	if err != nil {
		return false, err
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return false, err
	}
	if _, err = loader.LoadUInt(64); err != nil {
		return false, err
	}
	return has, nil
}

func blockTransactionProof(root *cell.Cell, account []byte, lt uint64) (*cell.Cell, *cell.Cell, error) {
	builder := newLiteServerProofBuilder(root)
	tx, err := findBlockTransaction(builder.Root(), account, lt)
	if errors.Is(err, storage.ErrNotFound) {
		tx = nil
	} else if err != nil {
		return nil, nil, err
	}

	proof, err := builder.CreateProof()
	if err != nil {
		return nil, nil, err
	}
	return proof, tx, nil
}
