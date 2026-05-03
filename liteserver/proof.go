package liteserver

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"flexserver/service/storage"

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

	sk, err := blockHeaderProofSkeleton(root, id, mode)
	if err != nil {
		return ton.BlockHeader{}, err
	}

	proof, err := root.CreateProof(sk)
	if err != nil {
		return ton.BlockHeader{}, err
	}

	return ton.BlockHeader{
		ID:          cloneBlockID(id),
		Mode:        mode,
		HeaderProof: proof.ToBOCWithFlags(false),
	}, nil
}

func (s *Server) accountProof(ctx context.Context, block ton.BlockIDExt, accountID []byte, pruned bool) ([]*cell.Cell, *cell.Cell, error) {
	stateRoot, err := s.loadStateRoot(ctx, block)
	if err != nil {
		return nil, nil, err
	}

	stateSk, err := accountStateProofSkeleton(stateRoot, accountID)
	if err != nil {
		return nil, nil, err
	}

	proof, err := s.blockStateProof(ctx, block, stateRoot, stateSk)
	if err != nil {
		return nil, nil, err
	}

	state, err := accountCell(stateRoot, accountID)
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return proof, nil, nil
	}
	if err != nil {
		return nil, nil, err
	}
	if pruned {
		stateProof, err := state.CreateProof(cell.CreateProofSkeleton())
		if err != nil {
			return nil, nil, err
		}
		state = stateProof
	}

	return proof, state, nil
}

func (s *Server) masterShardProof(ctx context.Context, master ton.BlockIDExt, stateRoot *cell.Cell, addr *address.Address) ([]*cell.Cell, ton.BlockIDExt, error) {
	extra, err := mcStateExtra(stateRoot)
	if err != nil {
		return nil, ton.BlockIDExt{}, err
	}

	stateSk, err := shardHashesProofSkeleton(stateRoot, addr.Workchain())
	if err != nil {
		return nil, ton.BlockIDExt{}, err
	}

	proof, err := s.blockStateProof(ctx, master, stateRoot, stateSk)
	if err != nil {
		return nil, ton.BlockIDExt{}, err
	}

	shardBlock, _, err := shardInfoFromHashes(extra.ShardHashes, addr.Workchain(), accountLeafShard(addr), false)
	if err != nil {
		return proof, ton.BlockIDExt{}, err
	}

	return proof, shardBlock, nil
}

func accountLeafShard(addr *address.Address) int64 {
	return int64(binary.BigEndian.Uint64(addr.Data()[:8]) | 1)
}

func (s *Server) blockStateProof(ctx context.Context, block ton.BlockIDExt, stateRoot *cell.Cell, stateSk *cell.ProofSkeleton) ([]*cell.Cell, error) {
	blockProof, err := s.blockProof(ctx, block)
	if err != nil {
		return nil, err
	}

	stateProof, err := stateRoot.CreateProof(stateSk)
	if err != nil {
		return nil, err
	}

	return []*cell.Cell{blockProof, stateProof}, nil
}

func (s *Server) blockProof(ctx context.Context, block ton.BlockIDExt) (*cell.Cell, error) {
	root, err := s.loadBlockRoot(ctx, block)
	if err != nil {
		return nil, err
	}

	return root.CreateProof(blockStateRootProofSkeleton())
}

func (s *Server) loadBlockRoot(ctx context.Context, id ton.BlockIDExt) (*cell.Cell, error) {
	if cached, ok := s.store.(interface {
		BlockRoot(context.Context, ton.BlockIDExt) (*cell.Cell, error)
	}); ok {
		root, err := cached.BlockRoot(ctx, id)
		if err == nil {
			return root, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, err
		}
	}

	data, err := s.store.BlockData(ctx, id)
	if err != nil {
		return nil, err
	}

	return parseTrustedBlockBOC(id, data)
}

func (s *Server) loadStateRoot(ctx context.Context, id ton.BlockIDExt) (*cell.Cell, error) {
	blockRoot, err := s.loadBlockRoot(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load block root for state %s: %w", storage.FormatBlockRef(id), err)
	}

	stateRootHash, err := stateRootHashFromBlock(id, blockRoot)
	if err != nil {
		return nil, err
	}

	root, _, err := s.store.LoadStateCellTree(ctx, id, stateRootHash)
	if err != nil {
		return nil, fmt.Errorf("load state root %x for %s: %w", stateRootHash, storage.FormatBlockRef(id), err)
	}
	return root, nil
}

func stateRootHashFromBlock(id ton.BlockIDExt, root *cell.Cell) ([]byte, error) {
	block, err := storage.ParseVerifiedBlockCell(id, root)
	if err != nil {
		return nil, err
	}
	if block.StateUpdate == nil {
		return nil, fmt.Errorf("block %s has no state update", storage.FormatBlockRef(id))
	}

	nextState, err := block.StateUpdate.PeekRef(1)
	if err != nil {
		return nil, fmt.Errorf("load block state update target %s: %w", storage.FormatBlockRef(id), err)
	}

	hash := nextState.HashKey(0)
	return bytes.Clone(hash[:]), nil
}

func accountStateProofSkeleton(stateRoot *cell.Cell, accountID []byte) (*cell.ProofSkeleton, error) {
	sk := cell.CreateProofSkeleton()

	dictRoot, err := accountsDictRoot(stateRoot)
	if errors.Is(err, storage.ErrNotFound) {
		return sk, nil
	}
	if err != nil {
		return nil, err
	}

	trace := cell.NewProofTrace()
	dict := dictRoot.AsDict(256).SetObserver(trace)
	_, err = dict.LoadValue(accountKey(accountID))
	if err != nil && !errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, err
	}

	sk.ProofRef(1).ProofRef(0).Merge(trace.UsageSkeleton())
	return sk, nil
}

func accountCell(stateRoot *cell.Cell, accountID []byte) (*cell.Cell, error) {
	dictRoot, err := accountsDictRoot(stateRoot)
	if errors.Is(err, storage.ErrNotFound) {
		return nil, cell.ErrNoSuchKeyInDict
	}
	if err != nil {
		return nil, err
	}

	value, err := dictRoot.AsDict(256).LoadValue(accountKey(accountID))
	if err != nil {
		return nil, err
	}

	var balance tlb.DepthBalanceInfo
	if err = tlb.LoadFromCell(&balance, value); err != nil {
		return nil, err
	}

	var account tlb.ShardAccount
	if err = tlb.LoadFromCell(&account, value); err != nil {
		return nil, err
	}

	return account.Account, nil
}

func accountsDictRoot(stateRoot *cell.Cell) (*cell.Cell, error) {
	accounts, err := stateRoot.PeekRef(1)
	if err != nil {
		return nil, err
	}

	loader := accounts.BeginParse()
	hasRoot, err := loader.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if !hasRoot {
		return nil, storage.ErrNotFound
	}

	root, err := loader.LoadRefCell()
	if err != nil {
		return nil, err
	}
	return root, nil
}

func accountKey(accountID []byte) *cell.Cell {
	return cell.BeginCell().MustStoreSlice(accountID, 256).EndCell()
}

func blockHeaderProofSkeleton(root *cell.Cell, id ton.BlockIDExt, mode uint32) (*cell.ProofSkeleton, error) {
	sk := cell.CreateProofSkeleton()

	// C++ always unpacks BlockInfo and recursively visits its small ref fields:
	// master_ref, prev_ref and prev_vert_ref.
	sk.ProofRef(0).SetRecursive()

	if mode&1 != 0 {
		sk.ProofRef(2).ProofRef(1)
	}
	if mode&2 != 0 {
		sk.ProofRef(1).SetRecursive()
	}
	if mode&16 != 0 {
		extraSk := sk.ProofRef(3)
		if id.Workchain == masterchainID && mode&(32|64) != 0 {
			extra, err := root.PeekRef(3)
			if err != nil {
				return nil, err
			}
			customSk, err := mcBlockExtraProofSkeleton(extra, mode)
			if err != nil {
				return nil, err
			}
			if customSk != nil {
				extraSk.AttachAt(3, customSk)
			}
		}
	}

	return sk, nil
}

func blockStateRootProofSkeleton() *cell.ProofSkeleton {
	sk := cell.CreateProofSkeleton()
	sk.ProofRef(0).SetRecursive()
	sk.ProofRef(2).ProofRef(1)
	return sk
}

func allShardsInfoProofSkeleton(root *cell.Cell) (*cell.ProofSkeleton, error) {
	extra, err := root.PeekRef(3)
	if err != nil {
		return nil, err
	}

	customSk, err := mcBlockExtraProofSkeleton(extra, 32)
	if err != nil {
		return nil, err
	}
	if customSk == nil {
		return nil, fmt.Errorf("masterchain block extra is missing custom data")
	}

	sk := cell.CreateProofSkeleton()
	sk.ProofRef(3).AttachAt(3, customSk)
	return sk, nil
}

func shardHashesProofSkeleton(stateRoot *cell.Cell, workchain int32) (*cell.ProofSkeleton, error) {
	custom, err := stateRoot.PeekRef(3)
	if err != nil {
		return nil, err
	}

	trace := cell.NewProofTrace()
	loader := custom.BeginParse().SetObserver(trace)
	if _, err = loader.LoadUInt(16); err != nil {
		return nil, err
	}

	shards, err := loader.LoadDict(32)
	if err != nil {
		return nil, err
	}
	value, err := shards.LoadValue(cell.BeginCell().MustStoreInt(int64(workchain), 32).EndCell())
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		sk := cell.CreateProofSkeleton()
		sk.AttachAt(3, trace.Skeleton())
		return sk, nil
	}
	if err != nil {
		return nil, err
	}
	trace.MarkRecursive(value)

	sk := cell.CreateProofSkeleton()
	sk.AttachAt(3, trace.Skeleton())
	return sk, nil
}

func mcBlockExtraProofSkeleton(extra *cell.Cell, mode uint32) (*cell.ProofSkeleton, error) {
	if extra.RefsNum() < 4 {
		return nil, fmt.Errorf("masterchain block extra is missing custom data")
	}

	custom, err := extra.PeekRef(3)
	if err != nil {
		return nil, err
	}

	customSk := cell.CreateProofSkeleton()
	loader := custom.BeginParse()
	if _, err = loader.LoadUInt(16); err != nil {
		return nil, err
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return nil, err
	}

	refIdx := 0
	hasShardHashes, err := loadMaybeRefCell(loader)
	if err != nil {
		return nil, err
	}
	if mode&32 != 0 && hasShardHashes {
		customSk.ProofRef(refIdx).SetRecursive()
	}
	if hasShardHashes {
		refIdx++
	}
	if mode&64 == 0 {
		return customSk, nil
	}

	hasShardFees, err := loadMaybeRefCell(loader)
	if err != nil {
		return nil, err
	}
	for i := 0; i < 2; i++ {
		if _, err = loader.LoadBigCoins(); err != nil {
			return nil, err
		}
		if _, err = loader.LoadMaybeRef(); err != nil {
			return nil, err
		}
	}
	if hasShardFees {
		refIdx++
	}

	details, err := loader.LoadRefCell()
	if err != nil {
		return nil, err
	}
	detailsSk := customSk.ProofRef(refIdx)
	detailsLoader := details.BeginParse()
	hasSignatures, err := loadMaybeRefCell(detailsLoader)
	if err != nil {
		return nil, err
	}
	if hasSignatures {
		detailsSk.ProofRef(0).SetRecursive()
	}

	return customSk, nil
}

func configProofSkeleton(stateRoot *cell.Cell, mode uint32, all bool, params []int32) (*cell.ProofSkeleton, error) {
	prefix, err := loadMcStateExtraPrefix(stateRoot)
	if err != nil {
		return nil, err
	}

	customSk := cell.CreateProofSkeleton()
	configSk, err := configDictProofSkeleton(prefix.Config.Config, mode, all, params)
	if err != nil {
		return nil, err
	}
	customSk.AttachAt(prefix.configRefIdx, configSk)

	useConfigInfo := mode&configModeNeedPrevBlocks != 0
	if useConfigInfo && mode&configModeNeedShardHashes != 0 {
		shardsSk, err := mcStateExtraShardHashesProofSkeleton(stateRoot)
		if err != nil {
			return nil, err
		}
		customSk.Merge(shardsSk)
	}
	if useConfigInfo {
		seqno, err := shardStateSeqno(stateRoot)
		if err != nil {
			return nil, err
		}
		globalVersion, err := configGlobalVersion(prefix.Config.Config)
		if err != nil {
			return nil, err
		}
		infoSk, err := mcStatePrevBlocksProofSkeleton(prefix.Info, seqno, globalVersion.Version)
		if err != nil {
			return nil, err
		}
		customSk.AttachAt(prefix.infoRefIdx, infoSk)
	}

	sk := cell.CreateProofSkeleton()
	if useConfigInfo && mode&configModeNeedAccountsRoot != 0 {
		sk.ProofRef(1)
	}
	if useConfigInfo && mode&configModeNeedLibraries != 0 {
		librariesSk, err := stateLibrariesProofSkeleton(stateRoot)
		if err != nil {
			return nil, err
		}
		sk.Merge(librariesSk)
	}
	sk.AttachAt(3, customSk)
	return sk, nil
}

func keyBlockConfigProofSkeleton(root *cell.Cell, mode uint32, all bool, params []int32) (*cell.ProofSkeleton, error) {
	extra, err := root.PeekRef(3)
	if err != nil {
		return nil, err
	}
	if extra.RefsNum() < 4 {
		return nil, fmt.Errorf("key block extra is missing custom data")
	}

	custom, err := extra.PeekRef(3)
	if err != nil {
		return nil, err
	}
	var mcExtra tlb.McBlockExtra
	if err = tlb.LoadFromCell(&mcExtra, custom.BeginParse()); err != nil {
		return nil, err
	}
	if !mcExtra.KeyBlock || mcExtra.ConfigParams == nil || mcExtra.ConfigParams.Config.Params == nil {
		return nil, fmt.Errorf("key block is missing config params")
	}

	configSk, err := configDictProofSkeleton(mcExtra.ConfigParams.Config.Params.AsCell(), mode, all, params)
	if err != nil {
		return nil, err
	}
	customSk := mcBlockExtraConfigProofSkeleton(custom, configSk)

	sk := cell.CreateProofSkeleton()
	sk.ProofRef(0).SetRecursive()
	sk.ProofRef(3).AttachAt(3, customSk)
	return sk, nil
}

func configDictProofSkeleton(configRoot *cell.Cell, mode uint32, all bool, params []int32) (*cell.ProofSkeleton, error) {
	if configRoot == nil {
		return nil, fmt.Errorf("configuration root not set")
	}

	configSk := cell.CreateProofSkeleton()
	trace := cell.NewProofTrace()
	paramsDict := configRoot.AsDict(32).SetObserver(trace)

	if all {
		configSk.SetRecursive()
	}

	if mode&configModeNeedValidatorSet != 0 {
		if _, err := markConfigParamFallback(paramsDict, trace, int32(tlb.ConfigParamCurrentTempValidators), int32(tlb.ConfigParamCurrentValidators)); err != nil {
			return nil, err
		}
	}
	if mode&configModeNeedSpecialSmc != 0 {
		if _, err := markConfigParam(paramsDict, trace, int32(tlb.ConfigParamFundamentalSMCAddresses)); err != nil {
			return nil, err
		}
	}
	if mode&configModeNeedWorkchainInfo != 0 {
		if _, err := markConfigParam(paramsDict, trace, int32(tlb.ConfigParamWorkchains)); err != nil {
			return nil, err
		}
	}
	if mode&configModeNeedCapabilities != 0 {
		if value, err := markConfigParam(paramsDict, trace, int32(tlb.ConfigParamGlobalVersion)); err != nil {
			return nil, err
		} else if value != nil {
			if _, err = globalVersionFromConfigValue(value); err != nil {
				return nil, err
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
			if _, err := markConfigParam(paramsDict, trace, param); err != nil {
				return nil, err
			}
		}
	}

	configSk.Merge(trace.Skeleton())
	return configSk, nil
}

func markConfigParam(dict *cell.Dictionary, trace *cell.ProofTrace, param int32) (*cell.Slice, error) {
	value, err := dict.LoadValueByIntKey(big.NewInt(int64(param)))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	trace.MarkRecursive(value)
	return value, nil
}

func markConfigParamFallback(dict *cell.Dictionary, trace *cell.ProofTrace, first int32, fallback int32) (*cell.Slice, error) {
	value, err := markConfigParam(dict, trace, first)
	if err != nil || value != nil {
		return value, err
	}
	return markConfigParam(dict, trace, fallback)
}

func globalVersionFromConfigValue(value *cell.Slice) (tlb.GlobalVersion, error) {
	ref, err := value.Copy().LoadRefCell()
	if err != nil {
		return tlb.GlobalVersion{}, err
	}

	var globalVersion tlb.GlobalVersion
	if err = tlb.LoadFromCell(&globalVersion, ref.BeginParse()); err != nil {
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
	if err := tlb.LoadFromCell(&state, stateRoot.BeginParse()); err != nil {
		return 0, err
	}
	return state.Seqno, nil
}

func stateLibrariesProofSkeleton(stateRoot *cell.Cell) (*cell.ProofSkeleton, error) {
	stats, err := stateRoot.PeekRef(2)
	if err != nil {
		return nil, err
	}

	trace := cell.NewProofTrace()
	loader := stats.BeginParse().SetObserver(trace)
	if _, err = loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if _, err = loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if err = tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}
	if err = tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}
	if _, err = loader.LoadDict(256); err != nil {
		return nil, err
	}

	sk := cell.CreateProofSkeleton()
	sk.AttachAt(2, trace.Skeleton())
	return sk, nil
}

func mcStateExtraShardHashesProofSkeleton(stateRoot *cell.Cell) (*cell.ProofSkeleton, error) {
	custom, err := stateRoot.PeekRef(3)
	if err != nil {
		return nil, err
	}

	trace := cell.NewProofTrace()
	loader := custom.BeginParse().SetObserver(trace)
	if _, err = loader.LoadUInt(16); err != nil {
		return nil, err
	}
	if _, err = loader.LoadDict(32); err != nil {
		return nil, err
	}
	return trace.Skeleton(), nil
}

func mcStatePrevBlocksProofSkeleton(info *cell.Cell, seqno uint32, globalVersion uint32) (*cell.ProofSkeleton, error) {
	trace := cell.NewProofTrace()
	loader := info.BeginParse().SetObserver(trace)
	if _, err := loader.LoadUInt(16); err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(32); err != nil {
		return nil, err
	}
	if _, err := loader.LoadBoolBit(); err != nil {
		return nil, err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err := prevBlocks.LoadFromCell(loader); err != nil {
		return nil, err
	}
	hasPrevBlocksRoot := prevBlocks.AugmentedDictionary != nil && !prevBlocks.IsEmpty()

	afterKeyBlock, err := loader.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	hasLastKeyBlock, err := loader.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if hasLastKeyBlock {
		var lastKey tlb.ExtBlkRef
		if err = tlb.LoadFromCell(&lastKey, loader); err != nil {
			return nil, err
		}
	}
	if !afterKeyBlock && !hasLastKeyBlock {
		return nil, fmt.Errorf("cannot fetch last key block")
	}

	for _, prevSeqno := range configPrevBlockProofSeqnos(seqno, globalVersion) {
		value, err := prevBlocks.LoadValueByIntKey(new(big.Int).SetUint64(uint64(prevSeqno)))
		if err != nil {
			return nil, fmt.Errorf("cannot fetch old mc block")
		}
		var ref tlb.KeyExtBlkRef
		if err = tlb.LoadFromCell(&ref, value); err != nil {
			return nil, err
		}
		if ref.BlkRef.SeqNo != prevSeqno {
			return nil, fmt.Errorf("old mc block seqno mismatch: got %d want %d", ref.BlkRef.SeqNo, prevSeqno)
		}
	}

	sk := trace.Skeleton()
	if hasPrevBlocksRoot {
		sk.ProofRef(0).SetRecursive()
	}
	return sk, nil
}

func configPrevBlockProofSeqnos(seqno uint32, globalVersion uint32) []uint32 {
	seen := map[uint32]struct{}{}
	seqnos := make([]uint32, 0, 33)
	add := func(seqno uint32) {
		if _, ok := seen[seqno]; ok {
			return
		}
		seen[seqno] = struct{}{}
		seqnos = append(seqnos, seqno)
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

func mcBlockExtraConfigProofSkeleton(custom *cell.Cell, configSk *cell.ProofSkeleton) *cell.ProofSkeleton {
	customSk := cell.CreateProofSkeleton()

	refIdx := 0
	loader := custom.BeginParse()
	if _, err := loader.LoadUInt(16); err != nil {
		customSk.SetRecursive()
		return customSk
	}
	if _, err := loader.LoadBoolBit(); err != nil {
		customSk.SetRecursive()
		return customSk
	}
	shards, err := loader.LoadDict(32)
	if err != nil {
		customSk.SetRecursive()
		return customSk
	}
	if shards != nil && !shards.IsEmpty() {
		refIdx++
	}
	var shardFees tlb.ShardFeesAugDict
	if err = shardFees.LoadFromCell(loader); err != nil {
		customSk.SetRecursive()
		return customSk
	}
	if shardFees.AugmentedDictionary != nil && !shardFees.IsEmpty() {
		refIdx++
	}
	customSk.AttachAt(refIdx+1, configSk)
	return customSk
}

func librariesProofSkeleton(stateRoot *cell.Cell, hashes [][]byte, mode uint32) (*cell.ProofSkeleton, error) {
	stats, err := stateRoot.PeekRef(2)
	if err != nil {
		return nil, err
	}

	trace := cell.NewProofTrace()
	loader := stats.BeginParse().SetObserver(trace)
	if _, err = loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if _, err = loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if err = tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}
	if err = tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}

	libraries, err := loader.LoadDict(256)
	if err != nil {
		return nil, err
	}
	for _, hash := range uniqueHashes(hashes, 16) {
		value, err := libraries.LoadValue(accountKey(hash))
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			continue
		}
		if err != nil {
			return nil, err
		}
		if _, err = value.LoadUInt(2); err != nil {
			return nil, err
		}
		if _, err = value.LoadRefCell(); err != nil {
			return nil, err
		}

		publishers, err := value.LoadDict(256)
		if err != nil {
			return nil, err
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
				return nil, err
			}
			trace.MarkPath(value)
		}
	}

	sk := cell.CreateProofSkeleton()
	sk.AttachAt(2, trace.Skeleton())
	return sk, nil
}

func validatorStatsProofSkeleton(stateRoot *cell.Cell) (*cell.ProofSkeleton, error) {
	prefix, err := loadMcStateExtraPrefix(stateRoot)
	if err != nil {
		return nil, err
	}

	customSk := cell.CreateProofSkeleton()
	infoSk := customSk.ProofRef(prefix.infoRefIdx)
	infoRefIdx := 0
	infoLoader := prefix.Info.BeginParse()
	flags, err := infoLoader.LoadUInt(16)
	if err != nil {
		return nil, err
	}
	if _, err = infoLoader.LoadUInt(32); err != nil {
		return nil, err
	}
	if _, err = infoLoader.LoadUInt(32); err != nil {
		return nil, err
	}
	if _, err = infoLoader.LoadBoolBit(); err != nil {
		return nil, err
	}

	hasPrevBlocks, err := skipOldMcBlocksInfoAugDict(infoLoader)
	if err != nil {
		return nil, err
	}
	if hasPrevBlocks {
		infoRefIdx++
	}

	if _, err = infoLoader.LoadBoolBit(); err != nil {
		return nil, err
	}
	hasLastKey, err := infoLoader.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if hasLastKey {
		if err = tlb.LoadFromCell(new(tlb.ExtBlkRef), infoLoader); err != nil {
			return nil, err
		}
	}
	if flags&1 == 0 {
		return nil, fmt.Errorf("masterchain state is missing block create stats")
	}
	magic, err := infoLoader.LoadUInt(8)
	if err != nil {
		return nil, err
	}
	switch magic {
	case 0x17:
		hasStats, err := loadMaybeRefCell(infoLoader)
		if err != nil {
			return nil, err
		}
		if hasStats {
			infoSk.ProofRef(infoRefIdx).SetRecursive()
		}
	case 0x34:
		hasStats, err := loadMaybeRefCell(infoLoader)
		if err != nil {
			return nil, err
		}
		if hasStats {
			infoSk.ProofRef(infoRefIdx).SetRecursive()
		}
		if _, err = infoLoader.LoadUInt(32); err != nil {
			return nil, err
		}
	default:
		return nil, fmt.Errorf("invalid block create stats magic %x", magic)
	}

	sk := cell.CreateProofSkeleton()
	sk.AttachAt(3, customSk)
	return sk, nil
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
	custom, err := stateRoot.PeekRef(3)
	if err != nil {
		return mcStateExtraPrefix{}, err
	}

	var prefix mcStateExtraPrefix
	if err = tlb.LoadFromCell(&prefix, custom.BeginParse()); err != nil {
		return mcStateExtraPrefix{}, err
	}
	if prefix.ShardHashes != nil && !prefix.ShardHashes.IsEmpty() {
		prefix.configRefIdx++
	}
	prefix.infoRefIdx = prefix.configRefIdx + 1

	return prefix, nil
}

func outMsgQueueSizeProofSkeleton(stateRoot *cell.Cell) (*cell.ProofSkeleton, error) {
	queue, err := stateRoot.PeekRef(0)
	if err != nil {
		return nil, err
	}

	if _, err = loadOutMsgQueueSize(queue); err != nil {
		return nil, err
	}

	sk := cell.CreateProofSkeleton()
	sk.ProofRef(0)
	return sk, nil
}

func loadOutMsgQueueSize(queue *cell.Cell) (uint64, error) {
	var info outMsgQueueInfo
	if err := tlb.LoadFromCell(&info, queue.BeginParse()); err != nil {
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

type outMsgQueueInfo struct {
	OutQueue uint64AugDict     `tlb:"."`
	ProcInfo *cell.Dictionary  `tlb:"dict 96"`
	Extra    *outMsgQueueExtra `tlb:"maybe ."`
}

type outMsgQueueExtra struct {
	_            tlb.Magic     `tlb:"$0"`
	Dispatch     uint64AugDict `tlb:"."`
	OutQueueSize *uint64       `tlb:"maybe ## 48"`
}

type uint64AugDict struct{}

func (d *uint64AugDict) LoadFromCell(loader *cell.Slice) error {
	if _, err := loadMaybeRefCell(loader); err != nil {
		return err
	}
	_, err := loader.LoadUInt(64)
	return err
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

func blockTransactionsProofSkeleton(withMetadata bool) *cell.ProofSkeleton {
	sk := cell.CreateProofSkeleton()
	extraSk := sk.ProofRef(3)
	if withMetadata {
		extraSk.ProofRef(0).SetRecursive()
	}
	extraSk.ProofRef(2).SetRecursive()
	return sk
}
