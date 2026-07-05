package liveview

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"sort"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
)

const (
	runMethodConfigParamCount           = 6
	runMethodConfigParamSizeLimitsIndex = 5
)

var runMethodConfigParamIDs = [runMethodConfigParamCount]uint32{
	tlb.ConfigParamGlobalID,
	tlb.ConfigParamGasPricesMasterchain,
	tlb.ConfigParamGasPricesBasechain,
	tlb.ConfigParamMsgForwardPricesMasterchain,
	tlb.ConfigParamMsgForwardPricesBasechain,
	tlb.ConfigParamSizeLimits,
}

type RunMethodConfigInfo struct {
	Root          *cell.Cell
	Present       bool
	GlobalVersion int
	PrevBlocks    any
	Unpacked      any
	Precompiled   *big.Int
}

type runMethodBaseConfig struct {
	Root          *cell.Cell
	Present       bool
	GlobalVersion int
	PrevBlocks    any
	Config        tlb.BlockchainConfig
	Unpacked      runMethodUnpackedConfigCells
	Precompiled   *cell.Dictionary
}

type runMethodStoragePrice struct {
	since uint64
	value *cell.Slice
}

type runMethodUnpackedConfigCells struct {
	StoragePrices []runMethodStoragePrice
	Params        [runMethodConfigParamCount]*cell.Cell
}

func RunMethodConfig(master ton.BlockIDExt, masterState *cell.Cell, now uint32, code *cell.Cell) (RunMethodConfigInfo, error) {
	extra, err := mcStateExtra(masterState)
	if err != nil {
		return RunMethodConfigInfo{}, err
	}
	base, err := buildRunMethodBaseConfig(master, extra)
	if err != nil {
		return RunMethodConfigInfo{}, err
	}
	return runMethodConfigFromBase(base, now, code)
}

func buildRunMethodBaseConfig(master ton.BlockIDExt, extra *tlb.McStateExtra) (*runMethodBaseConfig, error) {
	if extra.ConfigParams.Config.Params == nil || extra.ConfigParams.Config.Params.IsEmpty() {
		return nil, fmt.Errorf("masterchain config is empty")
	}

	root, err := prewarmCachedCell(extra.ConfigParams.Config.Params.AsCell(), liveConfigRootPrewarmDepth)
	if err != nil {
		return nil, err
	}

	config := tlb.BlockchainConfig{Root: root}
	version, err := config.GetGlobalVersion()
	if err != nil {
		return nil, err
	}

	globalVersion := int(version.Version)
	if globalVersion < tvm.MinSupportedGlobalVersion {
		return nil, fmt.Errorf("unsupported global version %d, minimum supported is %d", globalVersion, tvm.MinSupportedGlobalVersion)
	}
	if globalVersion > tvm.MaxSupportedGlobalVersion {
		return nil, fmt.Errorf("unsupported global version %d, maximum supported is %d", globalVersion, tvm.MaxSupportedGlobalVersion)
	}

	prevBlocks, err := runMethodPrevBlocksInfo(master, extra)
	if err != nil {
		return nil, err
	}

	unpacked, err := loadRunMethodUnpackedConfigCells(config)
	if err != nil {
		return nil, err
	}

	precompiled, err := loadRunMethodPrecompiledContracts(config)
	if err != nil {
		return nil, err
	}

	return &runMethodBaseConfig{
		Root:          config.Root,
		Present:       true,
		GlobalVersion: globalVersion,
		PrevBlocks:    prevBlocks,
		Config:        config,
		Unpacked:      unpacked,
		Precompiled:   precompiled,
	}, nil
}

func runMethodConfigFromBase(base *runMethodBaseConfig, now uint32, code *cell.Cell) (RunMethodConfigInfo, error) {
	unpacked, err := runMethodUnpackedConfig(base.Unpacked, now)
	if err != nil {
		return RunMethodConfigInfo{}, err
	}

	precompiled, err := runMethodPrecompiledGas(base.Precompiled, code)
	if err != nil {
		return RunMethodConfigInfo{}, err
	}

	return RunMethodConfigInfo{
		Root:          base.Root,
		Present:       base.Present,
		GlobalVersion: base.GlobalVersion,
		PrevBlocks:    base.PrevBlocks,
		Unpacked:      unpacked,
		Precompiled:   precompiled,
	}, nil
}

type runMethodMasterInfo struct {
	prevBlocks    *tlb.OldMcBlocksInfoAugDict
	afterKeyBlock bool
	lastKeyBlock  *tlb.ExtBlkRef
}

func runMethodPrevBlocksInfo(master ton.BlockIDExt, extra *tlb.McStateExtra) (tuple.Tuple, error) {
	info, err := loadRunMethodMasterInfo(extra)
	if err != nil {
		return tuple.Tuple{}, err
	}
	oldBlock := func(seqno uint32) (ton.BlockIDExt, error) {
		if seqno == master.SeqNo {
			return master, nil
		}
		return runMethodOldMasterBlockID(info.prevBlocks, seqno)
	}

	lastMCBlocks := []any{runMethodBlockIDTuple(master)}
	for seqno := master.SeqNo; seqno > 0 && len(lastMCBlocks) < 16; {
		seqno--
		block, err := oldBlock(seqno)
		if err != nil {
			return tuple.Tuple{}, err
		}
		lastMCBlocks = append(lastMCBlocks, runMethodBlockIDTuple(block))
	}

	lastKeyBlock, err := runMethodLastKeyBlock(master, info)
	if err != nil {
		return tuple.Tuple{}, err
	}

	lastMCBlocks100 := make([]any, 0, 16)
	for seqno := master.SeqNo / 100 * 100; len(lastMCBlocks100) < 16; {
		block, err := oldBlock(seqno)
		if err != nil {
			return tuple.Tuple{}, err
		}
		lastMCBlocks100 = append(lastMCBlocks100, runMethodBlockIDTuple(block))
		if seqno < 100 {
			break
		}
		seqno -= 100
	}

	return tuple.NewTupleValue(
		tuple.NewTupleValue(lastMCBlocks...),
		runMethodBlockIDTuple(lastKeyBlock),
		tuple.NewTupleValue(lastMCBlocks100...),
	), nil
}

func loadRunMethodMasterInfo(extra *tlb.McStateExtra) (runMethodMasterInfo, error) {
	if extra.Info == nil {
		return runMethodMasterInfo{}, fmt.Errorf("state is missing mc_state_extra info")
	}

	loader, err := extra.Info.BeginParse()
	if err != nil {
		return runMethodMasterInfo{}, err
	}
	if _, err = loader.LoadUInt(16); err != nil {
		return runMethodMasterInfo{}, err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return runMethodMasterInfo{}, err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return runMethodMasterInfo{}, err
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return runMethodMasterInfo{}, err
	}

	prevBlocks := &tlb.OldMcBlocksInfoAugDict{}
	if err = prevBlocks.LoadFromCell(loader); err != nil {
		return runMethodMasterInfo{}, err
	}

	afterKeyBlock, err := loader.LoadBoolBit()
	if err != nil {
		return runMethodMasterInfo{}, err
	}

	hasLastKeyBlock, err := loader.LoadBoolBit()
	if err != nil {
		return runMethodMasterInfo{}, err
	}

	var lastKeyBlock *tlb.ExtBlkRef
	if hasLastKeyBlock {
		ref := &tlb.ExtBlkRef{}
		if err = tlb.LoadFromCell(ref, loader); err != nil {
			return runMethodMasterInfo{}, err
		}
		lastKeyBlock = ref
	}

	return runMethodMasterInfo{
		prevBlocks:    prevBlocks,
		afterKeyBlock: afterKeyBlock,
		lastKeyBlock:  lastKeyBlock,
	}, nil
}

func runMethodOldMasterBlockID(prevBlocks *tlb.OldMcBlocksInfoAugDict, seqno uint32) (ton.BlockIDExt, error) {
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

	return runMethodExtBlkRef(ref.BlkRef), nil
}

func runMethodLastKeyBlock(master ton.BlockIDExt, info runMethodMasterInfo) (ton.BlockIDExt, error) {
	if info.afterKeyBlock {
		return master, nil
	}
	if info.lastKeyBlock == nil {
		return ton.BlockIDExt{}, fmt.Errorf("cannot fetch last key block")
	}
	return runMethodExtBlkRef(*info.lastKeyBlock), nil
}

func runMethodExtBlkRef(ref tlb.ExtBlkRef) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     ref.SeqNo,
		RootHash:  append([]byte(nil), ref.RootHash...),
		FileHash:  append([]byte(nil), ref.FileHash...),
	}
}

func runMethodBlockIDTuple(id ton.BlockIDExt) tuple.Tuple {
	return tuple.NewTupleValue(
		big.NewInt(int64(id.Workchain)),
		new(big.Int).SetUint64(uint64(id.Shard)),
		new(big.Int).SetUint64(uint64(id.SeqNo)),
		new(big.Int).SetBytes(id.RootHash),
		new(big.Int).SetBytes(id.FileHash),
	)
}

func loadRunMethodUnpackedConfigCells(config tlb.BlockchainConfig) (runMethodUnpackedConfigCells, error) {
	storagePricesRoot, err := runMethodConfigParamCell(config, tlb.ConfigParamStoragePrices)
	if err != nil {
		return runMethodUnpackedConfigCells{}, err
	}
	storagePrices, err := loadRunMethodStoragePrices(storagePricesRoot)
	if err != nil {
		return runMethodUnpackedConfigCells{}, err
	}

	var params [runMethodConfigParamCount]*cell.Cell
	for i, id := range runMethodConfigParamIDs {
		param, err := runMethodConfigParamCell(config, id)
		if err != nil {
			return runMethodUnpackedConfigCells{}, err
		}
		params[i] = param
	}

	return runMethodUnpackedConfigCells{
		StoragePrices: storagePrices,
		Params:        params,
	}, nil
}

func runMethodUnpackedConfig(config runMethodUnpackedConfigCells, now uint32) (tuple.Tuple, error) {
	storagePrices := runMethodCurrentStoragePrices(config.StoragePrices, now)

	values := []any{runMethodMaybeSlice(storagePrices)}
	for _, paramCell := range config.Params {
		param, err := runMethodConfigParamSlice(paramCell)
		if err != nil {
			return tuple.Tuple{}, err
		}
		values = append(values, runMethodMaybeSlice(param))
	}

	return tuple.NewTupleValue(values...), nil
}

func loadRunMethodStoragePrices(root *cell.Cell) ([]runMethodStoragePrice, error) {
	if root == nil {
		return nil, nil
	}

	entries, err := root.AsDict(32).LoadAll()
	if err != nil {
		return nil, err
	}

	prices := make([]runMethodStoragePrice, 0, len(entries))
	for _, entry := range entries {
		since, err := entry.Key.LoadUInt(32)
		if err != nil {
			return nil, err
		}
		prices = append(prices, runMethodStoragePrice{since: since, value: entry.Value})
	}
	sort.Slice(prices, func(i, j int) bool { return prices[i].since < prices[j].since })
	return prices, nil
}

func runMethodCurrentStoragePrices(prices []runMethodStoragePrice, now uint32) *cell.Slice {
	idx := sort.Search(len(prices), func(i int) bool { return prices[i].since > uint64(now) })
	if idx == 0 {
		return nil
	}
	return prices[idx-1].value.Copy()
}

func runMethodConfigParamSlice(param *cell.Cell) (*cell.Slice, error) {
	if param == nil {
		return nil, nil
	}
	return param.BeginParse()
}

func runMethodConfigParamCell(config tlb.BlockchainConfig, id uint32) (*cell.Cell, error) {
	if config.Root == nil {
		return nil, nil
	}

	param, err := config.GetParam(id)
	if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
		return nil, nil
	}
	return param, err
}

func runMethodMaybeSlice(value *cell.Slice) any {
	if value == nil {
		return nil
	}
	return value
}

func loadRunMethodPrecompiledContracts(config tlb.BlockchainConfig) (*cell.Dictionary, error) {
	if config.Root == nil {
		return nil, nil
	}

	precompiled, err := config.GetPrecompiledContractsConfig()
	if err != nil {
		return nil, err
	}
	if precompiled.List == nil || precompiled.List.IsEmpty() {
		return nil, nil
	}
	return precompiled.List, nil
}

func runMethodPrecompiledGas(precompiled *cell.Dictionary, code *cell.Cell) (*big.Int, error) {
	if precompiled == nil || code == nil {
		return nil, nil
	}

	value, err := precompiled.LoadValue(accountKey(code.Hash()))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	var smc tlb.PrecompiledSmc
	if err = tlb.LoadFromCell(&smc, value); err != nil {
		return nil, err
	}

	return new(big.Int).SetUint64(smc.GasUsage), nil
}

func RunMethodLibraries(masterState *cell.Cell, accountLibs *cell.Dictionary) ([]*cell.Cell, error) {
	globalLibs, err := librariesDict(masterState)
	if err != nil {
		return nil, err
	}
	return runMethodLibrariesFromGlobal(globalLibs, accountLibs), nil
}

func runMethodLibrariesFromGlobal(globalLibs *cell.Dictionary, accountLibs *cell.Dictionary) []*cell.Cell {
	libraries := make([]*cell.Cell, 0, 2)
	if globalLibs != nil && !globalLibs.IsEmpty() {
		libraries = append(libraries, globalLibs.AsCell())
	}
	if accountLibs != nil && !accountLibs.IsEmpty() {
		libraries = append(libraries, accountLibs.AsCell())
	}

	return libraries
}

func LoadRunMethodAccountState(accountCell *cell.Cell) (tlb.AccountState, error) {
	loader, err := accountCell.BeginParse()
	if err != nil {
		return tlb.AccountState{}, err
	}

	var account tlb.AccountState
	if err := tlb.LoadFromCell(&account, loader); err != nil {
		return tlb.AccountState{}, err
	}
	if !account.IsValid || account.Status != tlb.AccountStatusActive || account.StateInit == nil ||
		account.StateInit.Code == nil || account.StateInit.Data == nil {
		return tlb.AccountState{}, fmt.Errorf("account is not active")
	}
	return account, nil
}

type RunMethodShardHeader struct {
	_         tlb.Magic      `tlb:"#9023afe2"`
	GlobalID  int32          `tlb:"## 32"`
	Shard     tlb.ShardIdent `tlb:"."`
	Seqno     uint32         `tlb:"## 32"`
	VertSeqno uint32         `tlb:"## 32"`
	GenUTime  uint32         `tlb:"## 32"`
	GenLT     uint64         `tlb:"## 64"`
}

func runMethodShardStateHeader(stateRoot *cell.Cell) (RunMethodShardHeader, error) {
	loader, err := stateRoot.BeginParse()
	if err != nil {
		return RunMethodShardHeader{}, err
	}

	var header RunMethodShardHeader
	if err := tlb.LoadFromCell(&header, loader); err != nil {
		return RunMethodShardHeader{}, err
	}
	return header, nil
}

type RunMethodC7Config struct {
	Address     *address.Address
	Code        *cell.Cell
	ConfigRoot  *cell.Cell
	HasConfig   bool
	PrevBlocks  any
	Unpacked    any
	Precompiled *big.Int
	Now         uint32
	LT          uint64
	Balance     tuple.Tuple
	DuePayment  *big.Int
}

func RunMethodC7(cfg RunMethodC7Config) (tuple.Tuple, error) {
	randSeed, err := runMethodRandSeed()
	if err != nil {
		return tuple.Tuple{}, err
	}

	inner := []any{
		uint32(0x076ef1ea),
		uint8(0),
		uint8(0),
		int64(cfg.Now),
		int64(cfg.LT),
		int64(cfg.LT),
		randSeed,
		cfg.Balance,
		cell.BeginCell().MustStoreAddr(cfg.Address).ToSlice(),
		nil,
	}

	if cfg.HasConfig {
		inner[9] = cfg.ConfigRoot
		inner = append(inner,
			cfg.Code,
			tuple.NewTupleValue(big.NewInt(0), nil),
			big.NewInt(0),
			cfg.PrevBlocks,
			cfg.Unpacked,
			bigOrZero(cfg.DuePayment),
			cfg.Precompiled,
			runMethodInMsgParams(),
		)
	}

	return tuple.NewTupleValue(tuple.NewTupleValue(normalizeTupleValues(inner)...)), nil
}

func runMethodRandSeed() (*big.Int, error) {
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil, fmt.Errorf("generate c7 random seed: %w", err)
	}
	return new(big.Int).SetBytes(seed[:]), nil
}

func runMethodInMsgParams() tuple.Tuple {
	return tuple.NewTupleValue(
		int64(0),
		int64(0),
		cell.BeginCell().MustStoreUInt(0, 2).ToSlice(),
		int64(0),
		int64(0),
		int64(0),
		int64(0),
		int64(0),
		int64(0),
		nil,
		nil,
	)
}

func AccountBalanceTuple(account tlb.AccountState) tuple.Tuple {
	var extra *cell.Cell
	if account.ExtraCurrencies != nil && !account.ExtraCurrencies.IsEmpty() {
		extra = account.ExtraCurrencies.AsCell()
	}
	return tuple.NewTupleValue(account.Balance.Nano(), extra)
}

func AccountDuePayment(account tlb.AccountState) *big.Int {
	if account.StorageInfo.DuePayment == nil {
		return big.NewInt(0)
	}
	return account.StorageInfo.DuePayment.Nano()
}

func normalizeTupleValues(values []any) []any {
	out := make([]any, len(values))
	for i, value := range values {
		out[i] = normalizeTupleValue(value)
	}
	return out
}

func normalizeTupleValue(value any) any {
	switch v := value.(type) {
	case int:
		return big.NewInt(int64(v))
	case int8:
		return big.NewInt(int64(v))
	case int16:
		return big.NewInt(int64(v))
	case int32:
		return big.NewInt(int64(v))
	case int64:
		return big.NewInt(v)
	case uint8:
		return new(big.Int).SetUint64(uint64(v))
	case uint16:
		return new(big.Int).SetUint64(uint64(v))
	case uint32:
		return new(big.Int).SetUint64(uint64(v))
	case uint64:
		return new(big.Int).SetUint64(v)
	case *big.Int:
		if v == nil {
			return nil
		}
		return new(big.Int).Set(v)
	default:
		return value
	}
}

func bigOrZero(value *big.Int) *big.Int {
	if value == nil {
		return big.NewInt(0)
	}
	return new(big.Int).Set(value)
}
