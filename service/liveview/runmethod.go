package liveview

import (
	"container/list"
	"crypto/rand"
	"fmt"
	"math/big"
	"sync"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
)

// liveConfigEpochCacheLimit bounds the shared per-config-epoch cache. The
// blockchain config changes only at key blocks, so a couple of entries cover
// the live window plus occasional historic queries.
const liveConfigEpochCacheLimit = 4

// liveConfigEpoch is everything derived from one blockchain config root: the
// immutable tvm execution context plus the run-method extras the tvm does not
// expose. It is built once per config epoch and shared by every BlockView
// whose master state carries the same config root.
type liveConfigEpoch struct {
	prepared     *tvm.PreparedBlockchainConfig
	precompiled  map[cell.Hash]uint64
	extMsgLimits ExternalMessageSizeLimits
}

type liveConfigEpochEntry struct {
	key   cell.Hash
	epoch *liveConfigEpoch
}

var liveConfigEpochCache = struct {
	mu      sync.Mutex
	entries map[cell.Hash]*list.Element
	order   *list.List
}{
	entries: map[cell.Hash]*list.Element{},
	order:   list.New(),
}

// liveConfigEpochForRoot returns the shared config-epoch context for a config
// root, building and caching it on first use (bounded LRU keyed by the config
// root hash).
func liveConfigEpochForRoot(configRoot *cell.Cell) (*liveConfigEpoch, error) {
	key := configRoot.HashKey()

	cache := &liveConfigEpochCache
	cache.mu.Lock()
	if element, ok := cache.entries[key]; ok {
		cache.order.MoveToFront(element)
		epoch := element.Value.(*liveConfigEpochEntry).epoch
		cache.mu.Unlock()
		return epoch, nil
	}
	cache.mu.Unlock()

	epoch, err := buildLiveConfigEpoch(configRoot)
	if err != nil {
		return nil, err
	}

	cache.mu.Lock()
	defer cache.mu.Unlock()
	if element, ok := cache.entries[key]; ok {
		// A concurrent request built the same epoch; keep the published one.
		cache.order.MoveToFront(element)
		return element.Value.(*liveConfigEpochEntry).epoch, nil
	}
	cache.entries[key] = cache.order.PushFront(&liveConfigEpochEntry{key: key, epoch: epoch})
	for len(cache.entries) > liveConfigEpochCacheLimit {
		oldest := cache.order.Back()
		cache.order.Remove(oldest)
		delete(cache.entries, oldest.Value.(*liveConfigEpochEntry).key)
	}
	return epoch, nil
}

func buildLiveConfigEpoch(configRoot *cell.Cell) (*liveConfigEpoch, error) {
	root, err := configRoot.PrewarmRecursive(liveConfigRootPrewarmDepth)
	if err != nil {
		return nil, err
	}

	prepared, err := tvm.PrepareBlockchainConfig(root)
	if err != nil {
		return nil, err
	}

	precompiled, err := liveConfigPrecompiledContracts(root)
	if err != nil {
		return nil, err
	}

	limits, err := externalMessageLimitsFromConfigRoot(root)
	if err != nil {
		return nil, err
	}

	return &liveConfigEpoch{
		prepared:     prepared,
		precompiled:  precompiled,
		extMsgLimits: limits,
	}, nil
}

// unpackedConfig builds the c7 unpacked-config value active at the given
// block time. The entry layout comes from the prepared config and matches the
// C++ representation (raw dict value slices).
func (e *liveConfigEpoch) unpackedConfig(now uint32) (any, error) {
	block, err := e.prepared.NewBlockContext(tvm.BlockOptions{Now: now})
	if err != nil {
		return nil, err
	}
	unpacked, ok := block.UnpackedConfig()
	if !ok {
		return nil, fmt.Errorf("prepared blockchain config has no unpacked config")
	}
	return unpacked, nil
}

// precompiledGas returns the configured gas usage of a precompiled contract
// code (config param 45), or nil when the code is not precompiled.
func (e *liveConfigEpoch) precompiledGas(code *cell.Cell) *big.Int {
	if code == nil || len(e.precompiled) == 0 {
		return nil
	}
	gas, ok := e.precompiled[code.HashKey()]
	if !ok {
		return nil
	}
	return new(big.Int).SetUint64(gas)
}

// liveConfigPrecompiledContracts mirrors the precompiled map PreparedBlockchainConfig
// holds internally: the run-method c7 needs the gas usage as an explicit
// value, which the tvm does not expose.
func liveConfigPrecompiledContracts(root *cell.Cell) (map[cell.Hash]uint64, error) {
	precompiled, err := tlb.BlockchainConfig{Root: root}.GetPrecompiledContractsConfig()
	if err != nil {
		return nil, err
	}
	if precompiled.List == nil || precompiled.List.IsEmpty() {
		return nil, nil
	}

	items, err := precompiled.List.LoadAll(true)
	if err != nil {
		return nil, err
	}
	out := make(map[cell.Hash]uint64, len(items))
	for _, item := range items {
		codeHash, err := item.Key.LoadSlice(256)
		if err != nil {
			return nil, err
		}
		var smc tlb.PrecompiledSmc
		if err = tlb.LoadFromCell(&smc, item.Value); err != nil {
			return nil, err
		}
		out[cell.Hash(codeHash)] = smc.GasUsage
	}
	return out, nil
}

type RunMethodConfigInfo struct {
	Prepared    *tvm.PreparedBlockchainConfig
	Root        *cell.Cell
	Present     bool
	PrevBlocks  any
	Unpacked    any
	Precompiled *big.Int
}

// runMethodBaseConfig is the per-master-block execution base: the shared
// config-epoch context plus the master-specific prev-blocks tuple.
type runMethodBaseConfig struct {
	epoch      *liveConfigEpoch
	prevBlocks tuple.Tuple
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

	epoch, err := liveConfigEpochForRoot(extra.ConfigParams.Config.Params.AsCell())
	if err != nil {
		return nil, err
	}

	prevBlocks, err := runMethodPrevBlocksInfo(master, extra)
	if err != nil {
		return nil, err
	}

	return &runMethodBaseConfig{
		epoch:      epoch,
		prevBlocks: prevBlocks,
	}, nil
}

func runMethodConfigFromBase(base *runMethodBaseConfig, now uint32, code *cell.Cell) (RunMethodConfigInfo, error) {
	unpacked, err := base.epoch.unpackedConfig(now)
	if err != nil {
		return RunMethodConfigInfo{}, err
	}

	return RunMethodConfigInfo{
		Prepared:    base.epoch.prepared,
		Root:        base.epoch.prepared.Root(),
		Present:     true,
		PrevBlocks:  base.prevBlocks,
		Unpacked:    unpacked,
		Precompiled: base.epoch.precompiledGas(code),
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
