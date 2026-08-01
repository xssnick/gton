package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"math/big"
	"strconv"
	"strings"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
	"github.com/xssnick/tonutils-go/tvm/tuple"
)

const (
	runGetMethodResultType   = "smc.runResult"
	tvmStackEntryNumber      = "tvm.stackEntryNumber"
	tvmStackEntryCell        = "tvm.stackEntryCell"
	tvmStackEntrySlice       = "tvm.stackEntrySlice"
	tvmStackEntryTuple       = "tvm.stackEntryTuple"
	tvmStackEntryList        = "tvm.stackEntryList"
	tvmStackEntryUnsupported = "tvm.stackEntryUnsupported"
	tvmNumberDecimalType     = "tvm.numberDecimal"
	tvmSliceType             = "tvm.slice"
	tvmTupleType             = "tvm.tuple"
	tvmListType              = "tvm.list"
)

type runGetMethodResult struct {
	Type              string                `json:"@type"`
	GasUsed           int64                 `json:"gas_used"`
	Stack             []legacyStackEntry    `json:"stack"`
	ExitCode          int32                 `json:"exit_code"`
	BlockID           blockIDExt            `json:"block_id"`
	LastTransactionID internalTransactionID `json:"last_transaction_id"`
}

type runGetMethodStdResult struct {
	Type     string          `json:"@type"`
	GasUsed  int64           `json:"gas_used"`
	Stack    []tvmStackEntry `json:"stack"`
	ExitCode int32           `json:"exit_code"`
}

type legacyStackEntry []any

type legacyStackEntryCell struct {
	Bytes string `json:"bytes"`
}

type tvmStackEntry struct {
	Type   string     `json:"@type"`
	Number *tvmNumber `json:"number,omitempty"`
	Cell   *tvmCell   `json:"cell,omitempty"`
	Slice  *tvmSlice  `json:"slice,omitempty"`
	Tuple  *tvmTuple  `json:"tuple,omitempty"`
	List   *tvmList   `json:"list,omitempty"`
}

type tvmNumber struct {
	Type   string `json:"@type"`
	Number string `json:"number"`
}

type tvmSlice struct {
	Type  string `json:"@type"`
	Bytes string `json:"bytes"`
}

type tvmTuple struct {
	Type     string          `json:"@type"`
	Elements []tvmStackEntry `json:"elements"`
}

type tvmList struct {
	Type     string          `json:"@type"`
	Elements []tvmStackEntry `json:"elements"`
}

type runMethodAccount struct {
	base            ton.BlockIDExt
	masterFragments *liveview.BlockView
	shard           *tlb.ShardAccount
	account         *tlb.AccountState
	genUTime        uint32
	genLT           uint64
}

func (s *Server) handleRunGetMethod(ctx context.Context, params requestParams) (any, *apiError) {
	return s.handleRunGetMethodWithStack(ctx, params, false)
}

func (s *Server) handleRunGetMethodStd(ctx context.Context, params requestParams) (any, *apiError) {
	return s.handleRunGetMethodWithStack(ctx, params, true)
}

func (s *Server) handleRunGetMethodWithStack(ctx context.Context, params requestParams, std bool) (any, *apiError) {
	addr, apiErr := runMethodAddress(params)
	if apiErr != nil {
		return nil, apiErr
	}
	methodID, apiErr := runMethodID(params)
	if apiErr != nil {
		return nil, apiErr
	}
	seqno, err := params.optionalUint32("seqno")
	hasSeqno := err == nil
	if err != nil && !errors.Is(err, errRequestParamNotFound) {
		return nil, asAPIError(err)
	}

	var values []any
	if std {
		values, apiErr = typedStackValues(params)
	} else {
		values, apiErr = legacyStackValues(params)
	}
	if apiErr != nil {
		return nil, apiErr
	}

	var seqnoPtr *uint32
	if hasSeqno {
		seqnoPtr = &seqno
	}
	info, apiErr := s.runMethodAccount(ctx, addr, seqnoPtr)
	if apiErr != nil {
		return nil, apiErr
	}

	result, apiErr := s.executeRunMethod(values, methodID, addr, info)
	if apiErr != nil {
		return nil, apiErr
	}

	exitCode := int32(ton.ErrCodeContractNotInitialized)
	gasUsed := int64(0)
	if result.Result != nil {
		exitCode = int32(result.Result.ExitCode)
		gasUsed = result.Result.GasUsed
	}
	if std {
		return runGetMethodStdResult{
			Type:     runGetMethodResultType,
			GasUsed:  gasUsed,
			Stack:    typedStackEntries(result.Stack),
			ExitCode: exitCode,
		}, nil
	}

	return runGetMethodResult{
		Type:              runGetMethodResultType,
		GasUsed:           gasUsed,
		Stack:             legacyStackEntries(result.Stack),
		ExitCode:          exitCode,
		BlockID:           blockIDExtFromTON(info.base),
		LastTransactionID: transactionIDFromShardAccount(info.shard),
	}, nil
}

func (s *Server) executeRunMethod(values []any, methodID uint64, addr *address.Address, info runMethodAccount) (liveview.RunMethodExecutionResult, *apiError) {
	if !liveview.CanRunMethod(info.account) {
		return liveview.RunMethodExecutionResult{}, nil
	}

	stack, err := liveview.NewRunMethodStack(methodID, values)
	if err != nil {
		return liveview.RunMethodExecutionResult{}, validationError("failed to parse request: cannot build stack: " + err.Error())
	}

	execResult, err := liveview.ExecuteRunMethod(s.tvm, *info.account, liveview.RunMethodRequest{
		Source: liveview.RunMethodSource{
			View: info.masterFragments,
		},
		Address: addr,
		Stack:   stack,
		Now:     info.genUTime,
		LT:      info.genLT,
	})
	if err != nil {
		return liveview.RunMethodExecutionResult{}, internalError("cannot run get method: " + err.Error())
	}

	output, err := liveview.RunMethodStackValues(execResult.Result.Stack)
	if err != nil {
		return liveview.RunMethodExecutionResult{}, internalError("cannot serialize resulting stack: " + err.Error())
	}
	execResult.Stack = output
	return execResult, nil
}

func (s *Server) runMethodAccount(ctx context.Context, addr *address.Address, seqno *uint32) (runMethodAccount, *apiError) {
	var (
		base            ton.BlockIDExt
		shard           ton.BlockIDExt
		masterFragments *liveview.BlockView
		err             error
	)

	if seqno == nil {
		blocks, err := s.store.CurrentAccountBlocks(ctx, addr.Workchain(), addr.Data())
		if err != nil {
			return runMethodAccount{}, internalError("cannot load current account blocks: " + err.Error())
		}
		base = blocks.Master
		shard = blocks.Account
		masterFragments, err = s.store.BlockFragments(ctx, blocks.Master)
		if err != nil {
			return runMethodAccount{}, internalError("cannot load masterchain state: " + err.Error())
		}
	} else {
		base, err = s.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{
			Workchain: masterchainWorkchain,
			Shard:     masterchainShard,
			SeqNo:     *seqno,
		})
		if err != nil {
			return runMethodAccount{}, internalError("cannot lookup masterchain block: " + err.Error())
		}
		masterFragments, err = s.store.BlockFragments(ctx, base)
		if err != nil {
			return runMethodAccount{}, internalError("cannot load masterchain state: " + err.Error())
		}
		shard = base
		if addr.Workchain() != masterchainWorkchain {
			extra, err := masterFragments.McStateExtra()
			if err != nil {
				return runMethodAccount{}, internalError("cannot unpack masterchain state: " + err.Error())
			}
			shard, _, err = blockproof.ShardInfoFromHashes(extra.ShardHashes, addr.Workchain(), accountLeafShard(addr), false)
			if errors.Is(err, storage.ErrNotFound) {
				return runMethodAccount{base: base, masterFragments: masterFragments}, nil
			}
			if err != nil {
				return runMethodAccount{}, internalError("cannot resolve account shard: " + err.Error())
			}
		}
	}

	shardFragments, err := s.store.BlockFragments(ctx, shard)
	if err != nil {
		return runMethodAccount{}, internalError("cannot load account shard state: " + err.Error())
	}
	shardAccount, account, err := shardFragments.ExternalMessageAccount(addr)
	if err != nil {
		return runMethodAccount{}, internalError("cannot load account state: " + err.Error())
	}
	header := shardFragments.Header()

	return runMethodAccount{
		base:            base,
		masterFragments: masterFragments,
		shard:           shardAccount,
		account:         account,
		genUTime:        header.GenUTime,
		genLT:           header.GenLT,
	}, nil
}

func runMethodAddress(params requestParams) (*address.Address, *apiError) {
	addr, _, apiErr := parseStdAddressParam(params, "address")
	if apiErr != nil {
		return nil, apiErr
	}
	return addr, nil
}

func runMethodID(params requestParams) (uint64, *apiError) {
	raw, ok := params.body["method"]
	if !ok {
		return 0, validationError(`failed to parse request: missing required field "method"`)
	}

	var value any
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&value); err != nil {
		return 0, validationError("failed to parse request: " + err.Error())
	}

	switch v := value.(type) {
	case string:
		if strings.TrimSpace(v) == "" {
			return 0, validationError(`failed to parse request: missing required field "method"`)
		}
		return tlb.MethodNameHash(v), nil
	case json.Number:
		id, err := strconv.ParseInt(v.String(), 10, 32)
		if err != nil {
			return 0, validationError(`failed to parse request: field "method" must be int32 or string`)
		}
		return uint64(uint32(id)), nil
	default:
		return 0, validationError(`failed to parse request: field "method" must be int32 or string`)
	}
}

func legacyStackValues(params requestParams) ([]any, *apiError) {
	raw, ok := params.body["stack"]
	if !ok {
		return nil, validationError(`failed to parse request: missing required field "stack"`)
	}

	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, validationError("failed to parse request: stack must be an array")
	}

	values := make([]any, 0, len(items))
	for _, item := range items {
		value, err := legacyStackValue(item)
		if err != nil {
			return nil, validationError("failed to parse request: " + err.Error())
		}
		values = append(values, value)
	}
	return values, nil
}

func legacyStackValue(raw json.RawMessage) (any, error) {
	var pair []json.RawMessage
	if err := json.Unmarshal(raw, &pair); err != nil {
		return nil, fmt.Errorf("legacy stack entry must be an array")
	}
	if len(pair) != 2 {
		return nil, fmt.Errorf("legacy stack entry must contain type and value")
	}

	var typ string
	if err := json.Unmarshal(pair[0], &typ); err != nil {
		return nil, fmt.Errorf("legacy stack entry type must be a string")
	}
	switch strings.ToLower(typ) {
	case "num":
		return parseStackNumber(pair[1])
	case "cell", "tvm.cell":
		return stackCellFromJSON(pair[1])
	case "slice", "tvm.slice":
		root, err := stackCellFromJSON(pair[1])
		if err != nil {
			return nil, err
		}
		return root.BeginParse()
	case "tuple", "tvm.tuple":
		return legacyTupleValue(pair[1])
	case "list", "tvm.list":
		return legacyListValue(pair[1])
	default:
		return nil, fmt.Errorf("unsupported legacy stack entry type %q", typ)
	}
}

func typedStackValues(params requestParams) ([]any, *apiError) {
	raw, ok := params.body["stack"]
	if !ok {
		return nil, validationError(`failed to parse request: missing required field "stack"`)
	}

	var items []json.RawMessage
	if err := json.Unmarshal(raw, &items); err != nil {
		return nil, validationError("failed to parse request: stack must be an array")
	}

	values := make([]any, 0, len(items))
	for _, item := range items {
		entry, err := typedStackEntryFromJSON(item)
		if err != nil {
			return nil, validationError("failed to parse request: " + err.Error())
		}
		value, err := stackValueFromTypedEntry(entry)
		if err != nil {
			return nil, validationError("failed to parse request: " + err.Error())
		}
		values = append(values, value)
	}
	return values, nil
}

func typedStackEntryFromJSON(raw json.RawMessage) (tvmStackEntry, error) {
	var entry tvmStackEntry
	if err := json.Unmarshal(raw, &entry); err != nil {
		return tvmStackEntry{}, fmt.Errorf("typed stack entry must be an object")
	}
	return entry, nil
}

func legacyTupleValue(raw json.RawMessage) (any, error) {
	var value tvmTuple
	if err := json.Unmarshal(raw, &value); err == nil && value.Type == tvmTupleType {
		return tupleFromEntries(value.Elements)
	}

	entry, err := typedStackEntryFromJSON(raw)
	if err != nil {
		return nil, err
	}
	return stackValueFromTypedEntry(entry)
}

func legacyListValue(raw json.RawMessage) (any, error) {
	var value tvmList
	if err := json.Unmarshal(raw, &value); err == nil && value.Type == tvmListType {
		return tupleFromEntries(value.Elements)
	}

	entry, err := typedStackEntryFromJSON(raw)
	if err != nil {
		return nil, err
	}
	return stackValueFromTypedEntry(entry)
}

func stackValueFromTypedEntry(entry tvmStackEntry) (any, error) {
	switch entry.Type {
	case tvmStackEntryNumber:
		if entry.Number == nil {
			return nil, fmt.Errorf("number stack entry is missing number")
		}
		return parseBigIntString(entry.Number.Number)
	case tvmStackEntryCell:
		if entry.Cell == nil {
			return nil, fmt.Errorf("cell stack entry is missing cell")
		}
		return parseBOCCell(entry.Cell.Bytes)
	case tvmStackEntrySlice:
		if entry.Slice == nil {
			return nil, fmt.Errorf("slice stack entry is missing slice")
		}
		root, err := parseBOCCell(entry.Slice.Bytes)
		if err != nil {
			return nil, err
		}
		return root.BeginParse()
	case tvmStackEntryTuple:
		if entry.Tuple == nil {
			return nil, fmt.Errorf("tuple stack entry is missing tuple")
		}
		return tupleFromEntries(entry.Tuple.Elements)
	case tvmStackEntryList:
		if entry.List == nil {
			return nil, fmt.Errorf("list stack entry is missing list")
		}
		return tupleFromEntries(entry.List.Elements)
	default:
		return nil, fmt.Errorf("unsupported typed stack entry type %q", entry.Type)
	}
}

func tupleFromEntries(entries []tvmStackEntry) (tuple.Tuple, error) {
	values := make([]any, 0, len(entries))
	for _, entry := range entries {
		value, err := stackValueFromTypedEntry(entry)
		if err != nil {
			return tuple.Tuple{}, err
		}
		values = append(values, value)
	}
	return tuple.NewTupleValue(values...), nil
}

func stackCellFromJSON(raw json.RawMessage) (*cell.Cell, error) {
	var encoded string
	if err := json.Unmarshal(raw, &encoded); err == nil {
		return parseBOCCell(encoded)
	}

	var obj legacyStackEntryCell
	if err := json.Unmarshal(raw, &obj); err != nil {
		return nil, fmt.Errorf("cell stack entry value must be base64 BOC")
	}
	return parseBOCCell(obj.Bytes)
}

func parseBOCCell(encoded string) (*cell.Cell, error) {
	data, err := base64.StdEncoding.DecodeString(encoded)
	if err != nil {
		return nil, fmt.Errorf("cell BOC must be base64: %w", err)
	}
	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, fmt.Errorf("cell BOC cannot be deserialized: %w", err)
	}
	return root, nil
}

func parseStackNumber(raw json.RawMessage) (*big.Int, error) {
	var str string
	if err := json.Unmarshal(raw, &str); err == nil {
		return parseBigIntString(str)
	}

	var number json.Number
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	if err := decoder.Decode(&number); err != nil {
		return nil, fmt.Errorf("number stack entry value must be a number or string")
	}
	return parseBigIntString(number.String())
}

func parseBigIntString(raw string) (*big.Int, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil, fmt.Errorf("number is empty")
	}

	sign := 1
	if strings.HasPrefix(raw, "-") {
		sign = -1
		raw = strings.TrimPrefix(raw, "-")
	}

	base := 10
	if strings.HasPrefix(raw, "0x") || strings.HasPrefix(raw, "0X") {
		base = 16
		raw = raw[2:]
	}
	value, ok := new(big.Int).SetString(raw, base)
	if !ok {
		return nil, fmt.Errorf("invalid number format")
	}
	if sign < 0 {
		value.Neg(value)
	}
	return value, nil
}

func legacyStackEntries(values []any) []legacyStackEntry {
	entries := make([]legacyStackEntry, 0, len(values))
	for _, value := range values {
		entries = append(entries, legacyStackEntryFromValue(value))
	}
	return entries
}

func legacyStackEntryFromValue(value any) legacyStackEntry {
	switch v := value.(type) {
	case *big.Int:
		return legacyStackEntry{"num", hexNumber(v)}
	case *cell.Cell:
		return legacyStackEntry{"cell", legacyStackEntryCell{Bytes: bocBase64(v)}}
	case *cell.Slice:
		return legacyStackEntry{"slice", legacyStackEntryCell{Bytes: bocBase64(v.MustToCell())}}
	case tuple.Tuple:
		return legacyStackEntry{"tuple", tvmTupleFromTuple(v)}
	default:
		return legacyStackEntry{"unsupported", tvmStackEntry{Type: tvmStackEntryUnsupported}}
	}
}

func typedStackEntries(values []any) []tvmStackEntry {
	entries := make([]tvmStackEntry, 0, len(values))
	for _, value := range values {
		entries = append(entries, typedStackEntryFromValue(value))
	}
	return entries
}

func typedStackEntryFromValue(value any) tvmStackEntry {
	switch v := value.(type) {
	case *big.Int:
		return tvmStackEntry{
			Type: tvmStackEntryNumber,
			Number: &tvmNumber{
				Type:   tvmNumberDecimalType,
				Number: v.String(),
			},
		}
	case *cell.Cell:
		return tvmStackEntry{
			Type: tvmStackEntryCell,
			Cell: &tvmCell{
				Type:  tvmCellType,
				Bytes: bocBase64(v),
			},
		}
	case *cell.Slice:
		return tvmStackEntry{
			Type: tvmStackEntrySlice,
			Slice: &tvmSlice{
				Type:  tvmSliceType,
				Bytes: bocBase64(v.MustToCell()),
			},
		}
	case tuple.Tuple:
		tupleValue := tvmTupleFromTuple(v)
		return tvmStackEntry{Type: tvmStackEntryTuple, Tuple: &tupleValue}
	default:
		return tvmStackEntry{Type: tvmStackEntryUnsupported}
	}
}

func tvmTupleFromTuple(value tuple.Tuple) tvmTuple {
	entries := make([]tvmStackEntry, 0, value.Len())
	for i := 0; i < value.Len(); i++ {
		item, err := value.Index(i)
		if err != nil {
			entries = append(entries, tvmStackEntry{Type: tvmStackEntryUnsupported})
			continue
		}
		entries = append(entries, typedStackEntryFromValue(item))
	}
	return tvmTuple{Type: tvmTupleType, Elements: entries}
}

func hexNumber(value *big.Int) string {
	if value == nil {
		return "0x0"
	}
	if value.Sign() < 0 {
		return "-0x" + new(big.Int).Abs(value).Text(16)
	}
	return "0x" + value.Text(16)
}

func accountLeafShard(addr *address.Address) int64 {
	return int64(binary.BigEndian.Uint64(addr.Data()[:8]) | 1)
}
