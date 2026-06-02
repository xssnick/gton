package liteserver

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
	vmcore "github.com/xssnick/tonutils-go/tvm/vm"
)

const (
	runMethodSupportedMode uint32 = 0x3f
	runMethodMaxParamBytes        = 65536
	runMethodGasLimit      int64  = 300000
)

type runMethodAccount struct {
	base        ton.BlockIDExt
	master      ton.BlockIDExt
	shard       ton.BlockIDExt
	shardProof  []*cell.Cell
	proof       []*cell.Cell
	accountCell *cell.Cell
	masterState *cell.Cell
	masterCache *liveBlockFragments
	genUTime    uint32
	genLT       uint64
}

func (s *Server) handleRunSmcMethod(ctx context.Context, query ton.RunSmcMethod) any {
	if query.Mode&^runMethodSupportedMode != 0 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "unsupported mode in runSmcMethod"}
	}

	stack, err := runMethodStack(query.MethodID, query.Params)
	if err != nil {
		return ton.LSError{Code: errCodeProtoViolation, Text: err.Error()}
	}

	info, err := s.runMethodAccount(ctx, query.ID, query.Account, query.Mode)
	if err != nil {
		return errorResponse(err, "cannot get account state")
	}

	result := ton.RunMethodResult{
		Mode:       query.Mode,
		ID:         cloneBlockID(info.base),
		ShardBlock: cloneBlockID(info.shard),
		ExitCode:   ton.ErrCodeContractNotInitialized,
	}
	if query.Mode&1 != 0 {
		result.ShardProof = info.shardProof
		result.Proof = info.proof
	}
	if info.accountCell == nil {
		return result
	}

	account, err := loadRunMethodAccountState(info.accountCell)
	if err != nil {
		if query.Mode&2 != 0 {
			result.StateProof, err = s.runMethodInactiveAccountProof(info.accountCell)
			if err != nil {
				return errorResponse(err, "cannot create account state proof")
			}
		}
		return result
	}

	var config runMethodConfigInfo
	if info.masterCache != nil {
		config, err = info.masterCache.runMethodConfig(info.genUTime, account.StateInit.Code)
	} else {
		config, err = runMethodConfig(info.master, info.masterState, info.genUTime, account.StateInit.Code)
	}
	if err != nil {
		return errorResponse(err, "cannot load masterchain config")
	}

	var accountLibs *cell.Dictionary
	if query.Mode&2 == 0 {
		accountLibs = account.StateInit.Lib
	}

	var libraries []*cell.Cell
	if info.masterCache != nil {
		libraries, err = info.masterCache.runMethodLibraries(accountLibs)
	} else {
		libraries, err = runMethodLibraries(info.masterState, accountLibs)
	}
	if err != nil {
		return errorResponse(err, "cannot load libraries")
	}

	machine, err := s.tvm.WithGlobalVersion(config.GlobalVersion)
	if err != nil {
		return errorResponse(err, "cannot configure tvm")
	}

	c7, err := runMethodC7(runMethodC7Config{
		Address:     account.Address,
		Code:        account.StateInit.Code,
		ConfigRoot:  config.Root,
		HasConfig:   config.Present,
		PrevBlocks:  config.PrevBlocks,
		Unpacked:    config.Unpacked,
		Precompiled: config.Precompiled,
		Now:         info.genUTime,
		LT:          info.genLT,
		Balance:     accountBalanceTuple(account),
		DuePayment:  accountDuePayment(account),
	})
	if err != nil {
		return errorResponse(err, "cannot create c7")
	}

	var execResult *tvm.ExecutionResult
	if query.Mode&2 != 0 {
		execResult, err = machine.ExecuteDetailedWithAccountProof(
			info.accountCell,
			c7,
			vmcore.GasWithLimit(runMethodGasLimit),
			stack,
			libraries...,
		)
	} else {
		execResult, err = machine.ExecuteGetMethodDetailedWithLibraries(
			account.StateInit.Code,
			account.StateInit.Data,
			c7,
			vmcore.GasWithLimit(runMethodGasLimit),
			stack,
			libraries...,
		)
	}
	if err != nil {
		return errorResponse(err, "cannot run get method")
	}

	result.ExitCode = int32(execResult.ExitCode)

	resultStack, err := vmStackToCell(execResult.Stack)
	if err != nil {
		return errorResponse(err, "cannot serialize resulting stack")
	}
	if query.Mode&4 != 0 {
		result.Result = resultStack
	}
	if query.Mode&8 != 0 {
		c7ForResult := c7
		if query.Mode&32 == 0 {
			c7ForResult, err = runMethodC7(runMethodC7Config{
				Address: account.Address,
				Now:     info.genUTime,
				LT:      info.genLT,
				Balance: accountBalanceTuple(account),
			})
			if err != nil {
				return errorResponse(err, "cannot create c7")
			}
		}

		result.InitC7, err = stackValueToCell(tupleToTLB(c7ForResult))
		if err != nil {
			return errorResponse(err, "cannot serialize c7")
		}
	}
	if query.Mode&2 != 0 {
		result.StateProof = execResult.Proof
	}

	return result
}

func runMethodStack(methodID uint64, params *cell.Cell) (*vmcore.Stack, error) {
	if params != nil && len(params.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})) >= runMethodMaxParamBytes {
		return nil, fmt.Errorf("more than 64k parameter bytes passed")
	}

	var stack tlb.Stack
	if params != nil {
		loader, err := params.BeginParse()
		if err != nil {
			return nil, fmt.Errorf("parameter list boc cannot be deserialized as a VmStack: %w", err)
		}
		if err := stack.LoadFromCell(loader); err != nil {
			return nil, fmt.Errorf("parameter list boc cannot be deserialized as a VmStack: %w", err)
		}
		if loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
			return nil, fmt.Errorf("parameter list boc cannot be deserialized as a VmStack")
		}
	}

	values := make([]any, 0, stack.Depth())
	for stack.Depth() > 0 {
		value, err := stack.Pop()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}

	vmStack := vmcore.NewStack()
	for _, value := range values {
		if err := pushVMStackValue(vmStack, value); err != nil {
			return nil, err
		}
	}
	if err := vmStack.PushInt(big.NewInt(int64(methodID))); err != nil {
		return nil, err
	}
	return vmStack, nil
}

func (s *Server) runMethodAccount(ctx context.Context, id *ton.BlockIDExt, account ton.AccountID, mode uint32) (runMethodAccount, error) {
	ref, err := s.resolveAccountReference(ctx, id, account, "runSmcMethod()", true)
	if err != nil {
		return runMethodAccount{}, err
	}

	shardProof := ref.shardProof
	if mode&1 == 0 {
		shardProof = nil
	}
	if !isFullBlockID(&ref.shard) {
		return runMethodAccount{
			base:        ref.base,
			shard:       ton.BlockIDExt{},
			shardProof:  shardProof,
			masterState: ref.masterState,
			masterCache: ref.masterCache,
		}, nil
	}

	master := ref.master
	masterState := ref.masterState
	masterCache := ref.masterCache
	if masterState == nil {
		masterBlock, root, fragments, err := s.runMethodMasterState(ctx, ref.shard)
		if err != nil {
			return runMethodAccount{}, fmt.Errorf("load run method master state: %w", err)
		}
		master = masterBlock
		masterState = root
		masterCache = fragments
	}

	shardFragments, err := s.blockFragments(ctx, ref.shard)
	if err != nil {
		return runMethodAccount{}, fmt.Errorf("load run method shard fragments: %w", err)
	}

	header := shardFragments.shardHeader

	var proof []*cell.Cell
	var stateCell *cell.Cell
	if mode&1 != 0 {
		proof, stateCell, err = shardFragments.accountProof(account.ID, false)
	} else {
		stateCell, err = shardFragments.accountCell(account.ID)
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			err = nil
		}
	}
	if err != nil {
		return runMethodAccount{}, fmt.Errorf("load run method account cell: %w", err)
	}

	return runMethodAccount{
		base:        ref.base,
		master:      master,
		shard:       ref.shard,
		shardProof:  shardProof,
		proof:       proof,
		accountCell: stateCell,
		masterState: masterState,
		masterCache: masterCache,
		genUTime:    header.GenUTime,
		genLT:       header.GenLT,
	}, nil
}

func (s *Server) runMethodMasterState(ctx context.Context, shard ton.BlockIDExt) (ton.BlockIDExt, *cell.Cell, *liveBlockFragments, error) {
	shardFragments, err := s.blockFragments(ctx, shard)
	if err != nil {
		return ton.BlockIDExt{}, nil, nil, fmt.Errorf("load shard state fragments: %w", err)
	}

	master, err := shardStateMasterRef(shardFragments.stateRoot)
	if errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, nil, nil, fmt.Errorf("masterchain ref block is not available")
	}
	if err != nil {
		return ton.BlockIDExt{}, nil, nil, fmt.Errorf("load shard state master ref: %w", err)
	}
	fragments, err := s.blockFragments(ctx, master)
	if err != nil {
		return ton.BlockIDExt{}, nil, nil, fmt.Errorf("load master state fragments %s: %w", storage.FormatBlockRef(master), err)
	}
	return master, fragments.stateRoot, fragments, nil
}

func shardStateMasterRef(stateRoot *cell.Cell) (ton.BlockIDExt, error) {
	var state tlb.ShardStateUnsplit
	loader, err := stateRoot.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse shard state root: %w", err)
	}
	if err = tlb.LoadFromCell(&state, loader); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("load shard state header: %w", err)
	}
	if state.Stats == nil {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}

	stats, err := state.Stats.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("parse shard state stats: %w", err)
	}
	if _, err = stats.LoadUInt(64); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("load shard stats overload history: %w", err)
	}
	if _, err = stats.LoadUInt(64); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("load shard stats underload history: %w", err)
	}

	var totalBalance tlb.CurrencyCollection
	if err = tlb.LoadFromCell(&totalBalance, stats); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("load shard stats total balance: %w", err)
	}
	var totalValidatorFees tlb.CurrencyCollection
	if err = tlb.LoadFromCell(&totalValidatorFees, stats); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("load shard stats total validator fees: %w", err)
	}
	if _, err = stats.LoadDict(256); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("load shard stats libraries dict: %w", err)
	}

	hasMasterRef, err := stats.LoadBoolBit()
	if err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("load shard stats master ref flag: %w", err)
	}
	if !hasMasterRef {
		return ton.BlockIDExt{}, storage.ErrNotFound
	}

	var masterRef tlb.ExtBlkRef
	if err = tlb.LoadFromCell(&masterRef, stats); err != nil {
		return ton.BlockIDExt{}, fmt.Errorf("load shard stats master ref info: %w", err)
	}
	return blockIDFromExtRef(masterchainID, masterchainShard, masterRef), nil
}

type runMethodConfigInfo struct {
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
}

func runMethodConfig(master ton.BlockIDExt, masterState *cell.Cell, now uint32, code *cell.Cell) (runMethodConfigInfo, error) {
	extra, err := mcStateExtra(masterState)
	if err != nil {
		return runMethodConfigInfo{}, err
	}
	base, err := buildRunMethodBaseConfig(master, extra)
	if err != nil {
		return runMethodConfigInfo{}, err
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

	prevBlocks, err := runMethodPrevBlocksInfo(master, extra)
	if err != nil {
		return nil, err
	}

	return &runMethodBaseConfig{
		Root:          config.Root,
		Present:       true,
		GlobalVersion: globalVersion,
		PrevBlocks:    prevBlocks,
		Config:        config,
	}, nil
}

func runMethodConfigFromBase(base *runMethodBaseConfig, now uint32, code *cell.Cell) (runMethodConfigInfo, error) {
	unpacked, err := runMethodUnpackedConfig(base.Config, now)
	if err != nil {
		return runMethodConfigInfo{}, err
	}

	precompiled, err := runMethodPrecompiledGas(base.Config, code)
	if err != nil {
		return runMethodConfigInfo{}, err
	}

	return runMethodConfigInfo{
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

func runMethodUnpackedConfig(config tlb.BlockchainConfig, now uint32) (tuple.Tuple, error) {
	storagePrices, err := runMethodCurrentStoragePrices(config, now)
	if err != nil {
		return tuple.Tuple{}, err
	}

	values := []any{runMethodMaybeSlice(storagePrices)}
	for _, id := range []uint32{
		tlb.ConfigParamGlobalID,
		tlb.ConfigParamGasPricesMasterchain,
		tlb.ConfigParamGasPricesBasechain,
		tlb.ConfigParamMsgForwardPricesMasterchain,
		tlb.ConfigParamMsgForwardPricesBasechain,
		tlb.ConfigParamSizeLimits,
	} {
		param, err := runMethodConfigParamSlice(config, id)
		if err != nil {
			return tuple.Tuple{}, err
		}
		values = append(values, runMethodMaybeSlice(param))
	}

	return tuple.NewTupleValue(values...), nil
}

func runMethodCurrentStoragePrices(config tlb.BlockchainConfig, now uint32) (*cell.Slice, error) {
	root, err := runMethodConfigParamCell(config, tlb.ConfigParamStoragePrices)
	if err != nil || root == nil {
		return nil, err
	}

	entries, err := root.AsDict(32).LoadAll()
	if err != nil {
		return nil, err
	}

	var best *cell.Slice
	var bestSince uint64
	for _, entry := range entries {
		since, err := entry.Key.LoadUInt(32)
		if err != nil {
			return nil, err
		}
		if since > uint64(now) || best != nil && since <= bestSince {
			continue
		}
		best = entry.Value.Copy()
		bestSince = since
	}

	return best, nil
}

func runMethodConfigParamSlice(config tlb.BlockchainConfig, id uint32) (*cell.Slice, error) {
	param, err := runMethodConfigParamCell(config, id)
	if err != nil || param == nil {
		return nil, err
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

func runMethodPrecompiledGas(config tlb.BlockchainConfig, code *cell.Cell) (*big.Int, error) {
	if config.Root == nil || code == nil {
		return nil, nil
	}

	precompiled, err := config.GetPrecompiledContractsConfig()
	if err != nil {
		return nil, err
	}
	if precompiled.List == nil || precompiled.List.IsEmpty() {
		return nil, nil
	}

	value, err := precompiled.List.LoadValue(accountKey(code.Hash()))
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

func runMethodLibraries(masterState *cell.Cell, accountLibs *cell.Dictionary) ([]*cell.Cell, error) {
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

func loadRunMethodAccountState(accountCell *cell.Cell) (tlb.AccountState, error) {
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

func (s *Server) runMethodInactiveAccountProof(accountCell *cell.Cell) (*cell.Cell, error) {
	res, err := s.tvm.ExecuteDetailedWithAccountProof(
		accountCell,
		tuple.Tuple{},
		vmcore.GasWithLimit(runMethodGasLimit),
		vmcore.NewStack(),
	)
	if err != nil {
		return nil, err
	}
	return res.Proof, nil
}

type runMethodShardHeader struct {
	_         tlb.Magic      `tlb:"#9023afe2"`
	GlobalID  int32          `tlb:"## 32"`
	Shard     tlb.ShardIdent `tlb:"."`
	Seqno     uint32         `tlb:"## 32"`
	VertSeqno uint32         `tlb:"## 32"`
	GenUTime  uint32         `tlb:"## 32"`
	GenLT     uint64         `tlb:"## 64"`
}

func runMethodShardStateHeader(stateRoot *cell.Cell) (runMethodShardHeader, error) {
	loader, err := stateRoot.BeginParse()
	if err != nil {
		return runMethodShardHeader{}, err
	}

	var header runMethodShardHeader
	if err := tlb.LoadFromCell(&header, loader); err != nil {
		return runMethodShardHeader{}, err
	}
	return header, nil
}

type runMethodC7Config struct {
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

func runMethodC7(cfg runMethodC7Config) (tuple.Tuple, error) {
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

func accountBalanceTuple(account tlb.AccountState) tuple.Tuple {
	var extra *cell.Cell
	if account.ExtraCurrencies != nil && !account.ExtraCurrencies.IsEmpty() {
		extra = account.ExtraCurrencies.AsCell()
	}
	return tuple.NewTupleValue(account.Balance.Nano(), extra)
}

func accountDuePayment(account tlb.AccountState) *big.Int {
	if account.StorageInfo.DuePayment == nil {
		return big.NewInt(0)
	}
	return account.StorageInfo.DuePayment.Nano()
}

func pushVMStackValue(stack *vmcore.Stack, value any) error {
	return stack.PushAny(tlbToVMStackValue(value))
}

func tlbToVMStackValue(value any) any {
	switch v := value.(type) {
	case []any:
		values := make([]any, len(v))
		for i := range v {
			values[i] = tlbToVMStackValue(v[i])
		}
		return tuple.NewTupleValue(values...)
	case tlb.StackNaN:
		return vmcore.NaN{}
	case *tlb.StackNaN:
		return vmcore.NaN{}
	default:
		return value
	}
}

func vmStackToCell(stack *vmcore.Stack) (*cell.Cell, error) {
	cp := stack.Copy()
	tlbStack := tlb.NewStack()
	for range cp.Len() {
		value, err := cp.PopAny()
		if err != nil {
			return nil, err
		}
		tlbStack.Push(vmToTLBStackValue(value))
	}
	return tlbStack.ToCell()
}

func vmToTLBStackValue(value any) any {
	switch v := value.(type) {
	case tuple.Tuple:
		return tupleToTLB(v)
	case vmcore.NaN:
		return tlb.StackNaN{}
	case *vmcore.NaN:
		return tlb.StackNaN{}
	case *cell.Cell:
		if v == nil {
			return nil
		}
		return v
	case *cell.Slice:
		if v == nil {
			return nil
		}
		return v
	case *cell.Builder:
		if v == nil {
			return nil
		}
		return v
	default:
		return value
	}
}

func tupleToTLB(value tuple.Tuple) []any {
	result := make([]any, 0, value.Len())
	for i := 0; i < value.Len(); i++ {
		v, err := value.Index(i)
		if err != nil {
			panic(err)
		}
		result = append(result, vmToTLBStackValue(v))
	}
	return result
}

func stackValueToCell(value any) (*cell.Cell, error) {
	builder := cell.BeginCell()
	if err := tlb.SerializeStackValue(builder, value); err != nil {
		return nil, err
	}
	return builder.EndCell(), nil
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
