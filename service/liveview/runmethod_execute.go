package liveview

import (
	"fmt"
	"math/big"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
	vmcore "github.com/xssnick/tonutils-go/tvm/vm"
)

const RunMethodGasLimit int64 = 300000

type RunMethodSource struct {
	Master      ton.BlockIDExt
	MasterState *cell.Cell
	View        *BlockView
}

type RunMethodRequest struct {
	Source      RunMethodSource
	Address     *address.Address
	AccountRoot *cell.Cell
	Stack       *vmcore.Stack
	Now         uint32
	LT          uint64
	Proof       bool
}

type RunMethodExecutionResult struct {
	Result *tvm.ExecutionResult
	C7     tuple.Tuple
	Stack  []any
}

func CanRunMethod(account *tlb.AccountState) bool {
	return account != nil && account.IsValid && account.Status == tlb.AccountStatusActive &&
		account.StateInit != nil && account.StateInit.Code != nil && account.StateInit.Data != nil
}

func NewRunMethodStack(methodID uint64, values []any) (*vmcore.Stack, error) {
	stack := vmcore.NewStack()
	for _, value := range values {
		if err := stack.PushAny(RunMethodStackValueToVM(value)); err != nil {
			return nil, err
		}
	}
	if err := stack.PushInt(new(big.Int).SetUint64(methodID)); err != nil {
		return nil, err
	}
	return stack, nil
}

func RunMethodStackValueToVM(value any) any {
	switch v := value.(type) {
	case []any:
		values := make([]any, len(v))
		for i := range v {
			values[i] = RunMethodStackValueToVM(v[i])
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

func RunMethodStackValues(stack *vmcore.Stack) ([]any, error) {
	if stack == nil {
		return nil, nil
	}

	cp := stack.Copy()
	values := make([]any, 0, cp.Len())
	for cp.Len() > 0 {
		value, err := cp.PopAny()
		if err != nil {
			return nil, err
		}
		values = append(values, value)
	}
	return values, nil
}

func ExecuteRunMethod(vm *tvm.TVM, account tlb.AccountState, req RunMethodRequest) (RunMethodExecutionResult, error) {
	view := req.Source.View
	master := req.Source.Master
	masterState := req.Source.MasterState
	addr := req.Address
	accountRoot := req.AccountRoot
	stack := req.Stack
	now := req.Now
	lt := req.LT
	proof := req.Proof
	stateInit := account.StateInit
	code := stateInit.Code
	data := stateInit.Data

	var config RunMethodConfigInfo
	var err error
	if view != nil {
		config, err = view.RunMethodConfig(now, code)
	} else {
		config, err = RunMethodConfig(master, masterState, now, code)
	}
	if err != nil {
		return RunMethodExecutionResult{}, fmt.Errorf("load masterchain config: %w", err)
	}

	var accountLibs *cell.Dictionary
	if !proof {
		accountLibs = stateInit.Lib
	}

	var libraries []*cell.Cell
	if view != nil {
		libraries, err = view.RunMethodLibraries(accountLibs)
	} else {
		libraries, err = RunMethodLibraries(masterState, accountLibs)
	}
	if err != nil {
		return RunMethodExecutionResult{}, fmt.Errorf("load libraries: %w", err)
	}

	c7, err := RunMethodC7(RunMethodC7Config{
		Address:     addr,
		Code:        code,
		ConfigRoot:  config.Root,
		HasConfig:   config.Present,
		PrevBlocks:  config.PrevBlocks,
		Unpacked:    config.Unpacked,
		Precompiled: config.Precompiled,
		Now:         now,
		LT:          lt,
		Balance:     AccountBalanceTuple(account),
		DuePayment:  AccountDuePayment(account),
	})
	if err != nil {
		return RunMethodExecutionResult{}, fmt.Errorf("create c7: %w", err)
	}

	execConfig := tvm.ExecutionConfig{
		Libraries:        libraries,
		GlobalVersion:    config.GlobalVersion,
		GlobalVersionSet: true,
	}

	if proof {
		execConfig.AccountRoot = accountRoot
		code = nil
		data = nil
	}

	execResult, err := vm.ExecuteGetMethod(
		code,
		data,
		c7,
		vmcore.GasWithLimit(RunMethodGasLimit),
		stack,
		execConfig,
	)
	if err != nil {
		return RunMethodExecutionResult{}, fmt.Errorf("execute get method: %w", err)
	}
	return RunMethodExecutionResult{
		Result: execResult,
		C7:     c7,
	}, nil
}

func RunMethodInactiveAccountProof(vm *tvm.TVM, accountRoot *cell.Cell) (*cell.Cell, error) {
	res, err := vm.ExecuteGetMethod(
		nil,
		nil,
		tuple.Tuple{},
		vmcore.GasWithLimit(RunMethodGasLimit),
		vmcore.NewStack(),
		tvm.ExecutionConfig{AccountRoot: accountRoot},
	)
	if err != nil {
		return nil, err
	}
	return res.Proof, nil
}
