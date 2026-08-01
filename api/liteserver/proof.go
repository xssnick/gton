package liteserver

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/liveview"
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

// blockHeaderProofModeMask keeps only the mode bits visitBlockHeader interprets,
// so unknown bits do not multiply cache entries for the same proof.
const blockHeaderProofModeMask uint32 = 1 | 2 | 16 | 32 | 64

func (s *Server) blockHeader(ctx context.Context, id ton.BlockIDExt, mode uint32) (ton.BlockHeader, error) {
	key := liteResponseKey{kind: liteResponseBlockHeader, mode: mode & blockHeaderProofModeMask, a: liteBlockKeyFromBlock(id)}
	value, err := s.respCache.do(ctx, key, func(ctx context.Context) (any, error) {
		root, err := s.store.BlockRoot(ctx, id)
		if err != nil {
			return nil, err
		}

		proof, err := blockproof.BlockHeaderProof(root, id, mode)
		if err != nil {
			return nil, err
		}
		return proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}), nil
	})
	if err != nil {
		return ton.BlockHeader{}, err
	}

	headerProof, ok := value.([]byte)
	if !ok {
		return ton.BlockHeader{}, errors.New("invalid block header cache value")
	}
	return ton.BlockHeader{
		ID:          blockproof.CloneBlockID(id),
		Mode:        mode,
		HeaderProof: headerProof,
	}, nil
}

func (s *Server) accountProof(ctx context.Context, block ton.BlockIDExt, accountID []byte, pruned bool) ([]*cell.Cell, *cell.Cell, error) {
	fragments, err := s.store.BlockFragments(ctx, block)
	if err != nil {
		return nil, nil, err
	}
	return fragments.AccountProof(accountID, pruned)
}

func (s *Server) masterShardProof(fragments *liveview.BlockView, addr *address.Address, withProof bool) ([]*cell.Cell, ton.BlockIDExt, error) {
	extra, err := fragments.McStateExtra()
	if err != nil {
		return nil, ton.BlockIDExt{}, err
	}

	leafShard := accountLeafShard(addr)
	shardBlock, _, resolveErr := blockproof.ShardInfoFromHashes(extra.ShardHashes, addr.Workchain(), leafShard, false)
	if resolveErr != nil && !errors.Is(resolveErr, storage.ErrNotFound) {
		return nil, ton.BlockIDExt{}, resolveErr
	}
	if !withProof {
		return nil, shardBlock, resolveErr
	}

	// The proof visits the shard tree path to the resolved leaf, which is identical
	// for every account inside that shard, so cache it by the true shard instead of
	// the per-account prefix. The unresolved case keeps the prefix to prove absence.
	proofShard := leafShard
	if resolveErr == nil {
		proofShard = shardBlock.Shard
	}
	stateProof, err := fragments.ShardHashesProof(addr.Workchain(), proofShard, false)
	if err != nil {
		return nil, ton.BlockIDExt{}, err
	}

	proof := []*cell.Cell{fragments.BlockStateRootProof(), stateProof}
	return proof, shardBlock, resolveErr
}

func accountLeafShard(addr *address.Address) int64 {
	return int64(binary.BigEndian.Uint64(addr.Data()[:8]) | 1)
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

func (s *Server) loadStateRootHash(ctx context.Context, id ton.BlockIDExt) ([]byte, error) {
	meta, err := s.store.BlockMeta(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load block meta for state %s: %w", storage.FormatBlockRef(id), err)
	}
	if len(meta.StateRootHash) != 32 {
		return nil, fmt.Errorf("state root hash is missing for %s", storage.FormatBlockRef(id))
	}
	return meta.StateRootHash, nil
}

func allShardsInfoProof(root *cell.Cell) (*cell.Cell, error) {
	return blockproof.CreateUsageProof(root, func(root *cell.Cell) error {
		extra, err := blockproof.LoadBlockExtra(root)
		if err != nil {
			return err
		}
		if extra.Custom == nil || extra.Custom.ShardHashes == nil {
			return fmt.Errorf("masterchain block extra is missing shard hashes")
		}
		return nil
	})
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

func configProof(stateRoot *cell.Cell, mode uint32, all bool, params []int32) (*cell.Cell, error) {
	return blockproof.CreateUsageProof(stateRoot, func(root *cell.Cell) error {
		prefix, err := blockproof.LoadMcStateExtraPrefix(root, true)
		if err != nil {
			return err
		}
		if err = blockproof.VisitMcStateExtraInfo(prefix.Info); err != nil {
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
			seqno, err := blockproof.ShardStateSeqno(root)
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
			if err = blockproof.VisitCell(accounts); err != nil {
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
	return blockproof.CreateUsageProof(root, func(root *cell.Cell) error {
		rootLoader, err := blockproof.VisitBlockRoot(root)
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
		if err = blockproof.VisitCellRecursive(info); err != nil {
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
	if err := blockproof.SkipCurrencyCollection(loader); err != nil {
		return err
	}
	return blockproof.SkipCurrencyCollection(loader)
}

func visitConfigDict(configRoot *cell.Cell, mode uint32, all bool, params []int32) error {
	if configRoot == nil {
		return fmt.Errorf("configuration root not set")
	}

	paramsDict := configRoot.AsDict(32)

	if all {
		if err := blockproof.VisitCellRecursive(configRoot); err != nil {
			return err
		}
	}

	if mode&configModeNeedValidatorSet != 0 {
		if err := markValidatorSetConfigParam(paramsDict); err != nil {
			return err
		}
	}
	if mode&configModeNeedSpecialSmc != 0 {
		if err := markConfigParamIfPresent(paramsDict, int32(tlb.ConfigParamFundamentalSMCAddresses)); err != nil {
			return err
		}
	}
	if mode&configModeNeedWorkchainInfo != 0 {
		if err := markConfigParamIfPresent(paramsDict, int32(tlb.ConfigParamWorkchains)); err != nil {
			return err
		}
	}
	if mode&configModeNeedCapabilities != 0 {
		value, err := loadAndMarkConfigParam(paramsDict, int32(tlb.ConfigParamGlobalVersion))
		if err != nil && !errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return err
		}
		// Config parameter #8 is optional in proofs.
		if err == nil {
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
			if err := markConfigParamIfPresent(paramsDict, param); err != nil {
				return err
			}
		}
	}

	return nil
}

func loadAndMarkConfigParam(dict *cell.Dictionary, param int32) (*cell.Slice, error) {
	value, err := dict.LoadValueByIntKey(big.NewInt(int64(param)))
	if err != nil {
		return nil, err
	}
	if err = blockproof.VisitSliceRefsRecursive(value); err != nil {
		return nil, err
	}
	return value, nil
}

func markConfigParamIfPresent(dict *cell.Dictionary, param int32) error {
	_, err := loadAndMarkConfigParam(dict, param)
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil
	}
	return err
}

func markValidatorSetConfigParam(dict *cell.Dictionary) error {
	if err := markConfigParamIfPresent(dict, int32(tlb.ConfigParamCatchainConfig)); err != nil {
		return err
	}

	if err := markConfigParamIfPresent(dict, int32(tlb.ConfigParamCurrentValidators)); err != nil {
		return err
	}

	return markConfigParamIfPresent(dict, int32(tlb.ConfigParamCurrentTempValidators))
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
	return blockproof.VisitCellRecursive(shards.AsCell())
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
	return blockproof.CreateUsageProof(stateRoot, func(root *cell.Cell) error {
		return visitLibraries(root, hashes, mode)
	})
}

func visitLibraries(stateRoot *cell.Cell, hashes [][]byte, mode uint32) error {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return err
	}

	state, err := blockproof.VisitShardStateHeader(stateLoader)
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
		value, err := libraries.LoadValue(blockproof.AccountKey(hash))
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
			if err = blockproof.VisitSliceRefsRecursive(value); err != nil {
				return err
			}
		}
	}

	return nil
}

func validatorStatsProofAndCount(stateRoot *cell.Cell, mode uint32, limit int32, startAfter []byte) (*cell.Cell, int32, bool, error) {
	var count int32
	var complete bool

	proof, err := blockproof.CreateUsageProof(stateRoot, func(root *cell.Cell) error {
		var err error
		count, complete, err = validatorStatsCount(root, mode, limit, startAfter)
		return err
	})
	if err != nil {
		return nil, 0, false, err
	}
	return proof, count, complete, nil
}

func outMsgQueueSizeProof(stateRoot *cell.Cell) (*cell.Cell, error) {
	return blockproof.CreateUsageProof(stateRoot, func(root *cell.Cell) error {
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
	builder := blockproof.NewProofBuilder(root)
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
