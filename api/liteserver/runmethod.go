package liteserver

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
	vmcore "github.com/xssnick/tonutils-go/tvm/vm"
)

const (
	runMethodSupportedMode uint32 = 0x3f
	runMethodMaxParamBytes        = 65536
)

type runMethodAccount struct {
	base        ton.BlockIDExt
	master      ton.BlockIDExt
	shard       ton.BlockIDExt
	shardProof  []*cell.Cell
	proof       []*cell.Cell
	accountCell *cell.Cell
	masterState *cell.Cell
	masterCache *liveview.BlockView
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

	account, err := liveview.LoadRunMethodAccountState(info.accountCell)
	if err != nil {
		if query.Mode&2 != 0 {
			result.StateProof, err = liveview.RunMethodInactiveAccountProof(s.tvm, info.accountCell)
			if err != nil {
				return errorResponse(err, "cannot create account state proof")
			}
		}
		return result
	}

	exec, err := liveview.ExecuteRunMethod(s.tvm, account, liveview.RunMethodRequest{
		Source: liveview.RunMethodSource{
			Master:      info.master,
			MasterState: info.masterState,
			View:        info.masterCache,
		},
		Address:     account.Address,
		AccountRoot: info.accountCell,
		Stack:       stack,
		Now:         info.genUTime,
		LT:          info.genLT,
		Proof:       query.Mode&2 != 0,
	})
	if err != nil {
		return errorResponse(err, "cannot run get method")
	}

	result.ExitCode = int32(exec.Result.ExitCode)

	if query.Mode&4 != 0 {
		resultStack, err := vmStackToCell(exec.Result.Stack)
		if err != nil {
			return errorResponse(err, "cannot serialize resulting stack")
		}
		result.Result = resultStack
	}
	if query.Mode&8 != 0 {
		c7ForResult := exec.C7
		if query.Mode&32 == 0 {
			c7ForResult, err = liveview.RunMethodC7(liveview.RunMethodC7Config{
				Address: account.Address,
				Now:     info.genUTime,
				LT:      info.genLT,
				Balance: liveview.AccountBalanceTuple(account),
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
		result.StateProof = exec.Result.Proof
	}

	return result
}

func runMethodStack(methodID uint64, params []byte) (*vmcore.Stack, error) {
	if len(params) >= runMethodMaxParamBytes {
		return nil, fmt.Errorf("more than 64k parameter bytes passed")
	}

	var stack tlb.Stack
	if len(params) > 0 {
		paramsCell, err := cell.FromBOC(params)
		if err != nil {
			return nil, fmt.Errorf("parameter list boc cannot be deserialized as a VmStack: %w", err)
		}

		loader, err := paramsCell.BeginParse()
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

	depth := stack.Depth()
	values := make([]any, 0, depth)
	for i := uint(0); i < depth; i++ {
		value, err := stack.Pop()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}

	return liveview.NewRunMethodStack(methodID, values)
}

func (s *Server) runMethodAccount(ctx context.Context, id *ton.BlockIDExt, account ton.AccountID, mode uint32) (runMethodAccount, error) {
	ref, err := s.resolveAccountReference(ctx, id, account, "runSmcMethod()", true, mode&1 != 0)
	if err != nil {
		return runMethodAccount{}, err
	}

	shardProof := ref.shardProof
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

	shardFragments, err := s.store.BlockFragments(ctx, ref.shard)
	if err != nil {
		return runMethodAccount{}, fmt.Errorf("load run method shard fragments: %w", err)
	}

	header := shardFragments.Header()

	var proof []*cell.Cell
	var stateCell *cell.Cell
	if mode&1 != 0 {
		proof, stateCell, err = shardFragments.AccountProof(account.ID, false)
	} else {
		stateCell, err = shardFragments.AccountCell(account.ID)
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

func (s *Server) runMethodMasterState(ctx context.Context, shard ton.BlockIDExt) (ton.BlockIDExt, *cell.Cell, *liveview.BlockView, error) {
	shardFragments, err := s.store.BlockFragments(ctx, shard)
	if err != nil {
		return ton.BlockIDExt{}, nil, nil, fmt.Errorf("load shard state fragments: %w", err)
	}

	master, err := shardStateMasterRef(shardFragments.StateRoot())
	if errors.Is(err, storage.ErrNotFound) {
		return ton.BlockIDExt{}, nil, nil, fmt.Errorf("masterchain ref block is not available")
	}
	if err != nil {
		return ton.BlockIDExt{}, nil, nil, fmt.Errorf("load shard state master ref: %w", err)
	}
	fragments, err := s.store.BlockFragments(ctx, master)
	if err != nil {
		return ton.BlockIDExt{}, nil, nil, fmt.Errorf("load master state fragments %s: %w", storage.FormatBlockRef(master), err)
	}
	return master, fragments.StateRoot(), fragments, nil
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

func runMethodExtBlkRef(ref tlb.ExtBlkRef) ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: masterchainID,
		Shard:     masterchainShard,
		SeqNo:     ref.SeqNo,
		RootHash:  append([]byte(nil), ref.RootHash...),
		FileHash:  append([]byte(nil), ref.FileHash...),
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
