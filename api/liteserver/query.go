package liteserver

import (
	"context"
	"errors"
	"fmt"
	"math"
	"strings"
	"time"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/liveview"
	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	errCodeUnspecified     int32 = 0
	errCodeProtoViolation  int32 = -400
	errCodeTonProtoError   int32 = 621
	errCodeNotReady        int32 = 651
	errCodeInternal        int32 = -400
	errCodeTooManyRequests int32 = 429

	masterchainID    int32 = -1
	masterchainShard int64 = -1 << 63
	workchainInvalid int32 = -1 << 31

	getMasterchainInfoExtShardClientState uint32 = 1

	queryErrorReasonRateLimited = "rate_limited"
)

var errInvalidLookupBlock = errors.New("invalid lookupBlock request")

func (s *Server) handleQuery(ctx context.Context, query any) tl.Serializable {
	switch q := query.(type) {
	case ton.GetVersion:
		return ton.Version{
			Mode:         0,
			Version:      s.version,
			Capabilities: s.capabilities,
			Now:          unixNow(s.now),
		}
	case ton.GetTime:
		return ton.CurrentTime{Now: unixNow(s.now)}
	case ton.GetMasterchainInf:
		resp, _, err := s.masterchainInfoWithUTime(ctx)
		if err != nil {
			return errorResponse(err, "cannot obtain a valid masterchain state")
		}
		return resp
	case ton.GetMasterchainInfoExt:
		return s.handleMasterchainInfoExt(ctx, q.Mode)
	case ton.GetBlockData:
		return s.handleBlockData(ctx, q.ID)
	case ton.GetBlockHeader:
		return s.handleBlockHeader(ctx, q.ID, q.Mode)
	case ton.GetBlockProof:
		return s.handleBlockProof(ctx, q)
	case ton.GetShardBlockProof:
		return s.handleShardBlockProof(ctx, q)
	case ton.GetState:
		return s.handleState(ctx, q.ID)
	case ton.SendMessage:
		return s.handleSendMessage(ctx, q)
	case ton.LookupBlock:
		return s.handleLookupBlock(ctx, q)
	case ton.LookupBlockWithProof:
		return s.handleLookupBlockWithProof(ctx, q)
	case ton.GetAccountState:
		return s.handleAccountState(ctx, q.ID, q.Account, false)
	case ton.GetAccountStatePruned:
		return s.handleAccountState(ctx, q.ID, q.Account, true)
	case ton.RunSmcMethod:
		return s.handleRunSmcMethod(ctx, q)
	case ton.GetAllShardsInfo:
		return s.handleAllShardsInfo(ctx, q.ID)
	case ton.GetShardInfo:
		return s.handleShardInfo(ctx, q)
	case ton.GetConfigAll:
		return s.handleConfig(ctx, q.BlockID, q.Mode, true, nil)
	case ton.GetConfigParams:
		return s.handleConfig(ctx, q.BlockID, q.Mode, false, q.Params)
	case ton.GetLibraries:
		return s.handleLibraries(ctx, q)
	case ton.GetLibrariesWithProof:
		return s.handleLibrariesWithProof(ctx, q)
	case ton.GetOneTransaction:
		return s.handleOneTransaction(ctx, q)
	case ton.GetTransactions:
		return s.handleGetTransactions(ctx, q)
	case ton.ListBlockTransactions:
		return s.handleListBlockTransactions(ctx, q)
	case ton.ListBlockTransactionsExt:
		return s.handleListBlockTransactionsExt(ctx, q)
	case ton.GetValidatorStats:
		return s.handleValidatorStats(ctx, q)
	case ton.GetBlockOutMsgQueueSize:
		return s.handleBlockOutMsgQueueSize(ctx, q)
	case ton.GetOutMsgQueueSizes:
		return s.handleOutMsgQueueSizes(ctx, q)
	case ton.GetDispatchQueueInfo:
		return s.handleDispatchQueueInfo(ctx, q)
	case ton.GetDispatchQueueMessages:
		return s.handleDispatchQueueMessages(ctx, q)
	case ton.NonfinalGetPendingShardBlocks:
		return s.handleNonfinalPendingShardBlocks(q)
	case ton.NonfinalGetValidatorGroups, ton.NonfinalGetCandidate:
		return ton.LSError{Code: errCodeUnspecified, Text: "query is not allowed"}
	default:
		return ton.LSError{Code: errCodeTonProtoError, Text: "unknown query"}
	}
}

func (s *Server) handleNonfinalPendingShardBlocks(query ton.NonfinalGetPendingShardBlocks) tl.Serializable {
	if !s.nonFinal {
		return ton.LSError{Code: errCodeUnspecified, Text: "query is not allowed"}
	}
	if query.Mode&^uint32(1) != 0 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "unsupported nonfinal.getPendingShardBlocks mode"}
	}

	var filter *storage.ShardKey
	if query.Mode&1 != 0 {
		if err := shard.Validate(query.Shard); err != nil {
			return ton.LSError{Code: errCodeProtoViolation, Text: "requested shard is invalid"}
		}
		filter = &storage.ShardKey{Workchain: query.WC, Shard: query.Shard}
	}

	signed, candidates := s.store.NonfinalPendingShardBlocks(filter)
	return ton.NonfinalPendingShardBlocks{
		SignedBlocks: blockIDPtrSlice(signed),
		Candidates:   blockIDPtrSlice(candidates),
	}
}

func blockIDPtrSlice(blocks []ton.BlockIDExt) []*ton.BlockIDExt {
	ptrs := make([]*ton.BlockIDExt, 0, len(blocks))
	for i := range blocks {
		ptrs = append(ptrs, &blocks[i])
	}
	return ptrs
}

func (s *Server) handleMasterchainInfoExt(ctx context.Context, mode uint32) tl.Serializable {
	effectiveMode := mode & 0x7fffffff
	if effectiveMode&^getMasterchainInfoExtShardClientState != 0 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "unsupported getMasterchainInfo mode"}
	}

	info, lastUTime, err := s.masterchainInfoWithUTime(ctx)
	if err != nil {
		return errorResponse(err, "cannot obtain a valid masterchain state")
	}

	return ton.MasterchainInfoExt{
		Mode:          mode,
		Version:       s.version,
		Capabilities:  s.capabilities,
		Last:          info.Last,
		LastUTime:     lastUTime,
		Now:           unixNow(s.now),
		StateRootHash: info.StateRootHash,
		Init:          info.Init,
	}
}

func (s *Server) handleBlockData(ctx context.Context, id *ton.BlockIDExt) tl.Serializable {
	if !isFullBlockID(id) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid BlockIdExt"}
	}

	data, err := s.store.BlockData(ctx, *id)
	if err != nil {
		return errorResponse(err, "cannot load block "+storage.FormatBlockRef(*id))
	}

	return ton.BlockData{ID: blockproof.CloneBlockID(*id), Payload: data}
}

func (s *Server) handleBlockHeader(ctx context.Context, id *ton.BlockIDExt, mode uint32) tl.Serializable {
	if !isFullBlockID(id) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid BlockIdExt"}
	}

	header, err := s.blockHeader(ctx, *id, mode)
	if err != nil {
		return errorResponse(err, "cannot load block "+storage.FormatBlockRef(*id))
	}
	return header
}

func (s *Server) handleLookupBlock(ctx context.Context, query ton.LookupBlock) tl.Serializable {
	id, responseMode, err := s.lookupBlock(ctx, query)
	if err != nil {
		if errors.Is(err, errInvalidLookupBlock) {
			text := strings.TrimPrefix(err.Error(), errInvalidLookupBlock.Error()+": ")
			return ton.LSError{Code: errCodeProtoViolation, Text: text}
		}
		return errorResponse(err, "cannot lookup block")
	}

	header, err := s.blockHeader(ctx, id, responseMode)
	if err != nil {
		return errorResponse(err, "cannot load block "+storage.FormatBlockRef(id))
	}
	return header
}

func (s *Server) handleAccountState(ctx context.Context, id *ton.BlockIDExt, account ton.AccountID, pruned bool) tl.Serializable {
	state, err := s.accountState(ctx, id, account, pruned)
	if err != nil {
		return errorResponse(err, "cannot get account state")
	}
	return state
}

func (s *Server) masterchainInfoWithUTime(ctx context.Context) (ton.MasterchainInfo, uint32, error) {
	block, stateRoot, lastUTime, err := s.store.CurrentMasterchainInfo(ctx)
	if err != nil {
		return ton.MasterchainInfo{}, 0, err
	}

	info, err := s.masterchainInfo(block, stateRoot)
	return info, lastUTime, err
}

func (s *Server) masterchainInfo(block ton.BlockIDExt, stateRoot []byte) (ton.MasterchainInfo, error) {
	if len(stateRoot) != 32 {
		return ton.MasterchainInfo{}, fmt.Errorf("masterchain state root hash is missing")
	}
	return ton.MasterchainInfo{
		Last:          blockproof.CloneBlockID(block),
		StateRootHash: stateRoot,
		Init:          cloneZeroStatePtr(s.zeroState),
	}, nil
}

func (s *Server) lookupBlock(ctx context.Context, query ton.LookupBlock) (ton.BlockIDExt, uint32, error) {
	if query.ID == nil {
		return ton.BlockIDExt{}, 0, fmt.Errorf("%w: invalid block id requested", errInvalidLookupBlock)
	}

	selector := query.Mode & 7
	if selector != 1 && selector != 2 && selector != 4 {
		return ton.BlockIDExt{}, 0, fmt.Errorf("%w: exactly one of mode.0, mode.1 and mode.2 bits must be set", errInvalidLookupBlock)
	}

	key := storage.BlockHistoryKey{
		Workchain: query.ID.Workchain,
		Shard:     query.ID.Shard,
	}

	switch selector {
	case 1:
		id, err := s.lookupBlockBySeqNoForPrefix(ctx, storage.BlockSeqRef{Workchain: key.Workchain, Shard: key.Shard, SeqNo: uint32(query.ID.Seqno)})
		return id, query.Mode >> 4, err
	case 2:
		id, err := s.lookupBlockByLTForPrefix(ctx, key, query.LT)
		return id, query.Mode >> 4, err
	default:
		id, err := s.lookupBlockByUnixTimeForPrefix(ctx, key, query.UTime)
		return id, query.Mode >> 4, err
	}
}

func (s *Server) lookupBlockBySeqNoForPrefix(ctx context.Context, ref storage.BlockSeqRef) (ton.BlockIDExt, error) {
	if store, ok := s.store.(prefixLookupStore); ok {
		return store.LookupBlockBySeqNoForPrefix(ctx, ref)
	}
	// Store predates prefix-aware lookups. The exact-shard method preserves
	// the behavior of released implementations that do not expose the new
	// optional capability.
	return s.store.LookupBlockBySeqNo(ctx, ref)
}

func (s *Server) lookupBlockByLTForPrefix(ctx context.Context, key storage.BlockHistoryKey, lt uint64) (ton.BlockIDExt, error) {
	if store, ok := s.store.(prefixLookupStore); ok {
		return store.LookupBlockByLTForPrefix(ctx, key, lt)
	}
	return s.store.LookupBlockByLT(ctx, key, lt)
}

func (s *Server) lookupBlockByUnixTimeForPrefix(ctx context.Context, key storage.BlockHistoryKey, utime uint32) (ton.BlockIDExt, error) {
	if store, ok := s.store.(prefixLookupStore); ok {
		return store.LookupBlockByUnixTimeForPrefix(ctx, key, utime)
	}
	return s.store.LookupBlockByUnixTime(ctx, key, utime)
}

type accountReference struct {
	base        ton.BlockIDExt
	master      ton.BlockIDExt
	shard       ton.BlockIDExt
	shardProof  []*cell.Cell
	masterState *cell.Cell
	masterCache *liveview.BlockView
}

func (s *Server) resolveAccountReference(ctx context.Context, id *ton.BlockIDExt, account ton.AccountID, request string, keepMasterState bool, withShardProof bool) (accountReference, error) {
	if id == nil {
		return accountReference{}, fmt.Errorf("reference block id for a %s is invalid", request)
	}
	if len(account.ID) != 32 {
		return accountReference{}, fmt.Errorf("invalid account id")
	}

	base := *id
	if isCurrentMasterchainID(base) {
		current, err := s.store.CurrentState(ctx)
		if err != nil {
			return accountReference{}, err
		}
		base = current.Masterchain.Block
	}
	if !isFullBlockID(&base) {
		return accountReference{}, fmt.Errorf("reference block id for a %s is invalid", request)
	}
	if base.Workchain != masterchainID && base.Workchain != account.Workchain {
		return accountReference{}, fmt.Errorf("reference block for a %s must belong to the masterchain", request)
	}

	addr := address.NewAddress(0, byte(account.Workchain), account.ID)
	if base.Workchain == account.Workchain && !tlb.ShardID(uint64(base.Shard)).ContainsAddress(addr) {
		return accountReference{}, fmt.Errorf("requested account id is not contained in the shard of the reference block")
	}

	ref := accountReference{
		base:  base,
		shard: base,
	}
	if base.Workchain != masterchainID {
		return ref, nil
	}

	if keepMasterState || account.Workchain != masterchainID {
		masterFragments, err := s.store.BlockFragments(ctx, base)
		if err != nil {
			return accountReference{}, err
		}
		ref.master = base
		ref.masterState = masterFragments.StateRoot()
		ref.masterCache = masterFragments
	}
	if account.Workchain == masterchainID {
		return ref, nil
	}

	proof, shardBlock, err := s.masterShardProof(ref.masterCache, addr, withShardProof)
	if errors.Is(err, storage.ErrNotFound) {
		ref.shard = ton.BlockIDExt{}
		ref.shardProof = proof
		return ref, nil
	}
	if err != nil {
		return accountReference{}, err
	}

	ref.shard = shardBlock
	ref.shardProof = proof
	return ref, nil
}

func (s *Server) accountState(ctx context.Context, id *ton.BlockIDExt, account ton.AccountID, pruned bool) (ton.AccountState, error) {
	ref, err := s.resolveAccountReference(ctx, id, account, "getAccountState()", false, true)
	if err != nil {
		return ton.AccountState{}, err
	}
	if !isFullBlockID(&ref.shard) {
		return ton.AccountState{
			ID:         blockproof.CloneBlockID(ref.base),
			Shard:      &ton.BlockIDExt{},
			ShardProof: ref.shardProof,
		}, nil
	}

	proof, state, err := s.accountProof(ctx, ref.shard, account.ID, pruned)
	if err != nil {
		return ton.AccountState{}, fmt.Errorf("load account state from shard %s: %w", storage.FormatBlockRef(ref.shard), err)
	}

	return ton.AccountState{
		ID:         blockproof.CloneBlockID(ref.base),
		Shard:      blockproof.CloneBlockID(ref.shard),
		ShardProof: ref.shardProof,
		Proof:      proof,
		State:      state,
	}, nil
}

func isFullBlockID(id *ton.BlockIDExt) bool {
	return id != nil && blockproof.IsFullBlockID(*id)
}

func isCurrentMasterchainID(id ton.BlockIDExt) bool {
	return id.Workchain == masterchainID && id.SeqNo == math.MaxUint32
}

func unixNow(now func() time.Time) uint32 {
	return uint32(now().Unix())
}

const cxxZeroStateNotInDB = "zerostate not in db"

type queryError struct {
	code int32
	text string
}

func (e *queryError) Error() string {
	return e.text
}

func newQueryError(code int32, text string) error {
	return &queryError{code: code, text: text}
}

func zeroStateErrorResponse(err error, block ton.BlockIDExt) ton.LSError {
	if errors.Is(err, storage.ErrNotFound) {
		return ton.LSError{Code: errCodeNotReady, Text: cxxZeroStateNotInDB}
	}
	return errorResponse(err, "cannot load zerostate of "+cxxBlockIDExtString(block))
}

func zeroStateProofError(err error, block ton.BlockIDExt) error {
	if errors.Is(err, storage.ErrNotFound) {
		return newQueryError(errCodeNotReady, "cannot load zerostate of "+cxxBlockIDExtString(block)+" : "+cxxZeroStateNotInDB)
	}
	return err
}

func errorResponse(err error, fallback string) ton.LSError {
	var queryErr *queryError
	if errors.As(err, &queryErr) {
		return ton.LSError{Code: queryErr.code, Text: queryErr.text}
	}
	if errors.Is(err, storage.ErrNotFound) {
		text := fallback + ": not found"
		if err.Error() != storage.ErrNotFound.Error() {
			text = fallback + ": " + err.Error()
		}
		return ton.LSError{Code: errCodeNotReady, Text: text}
	}
	return ton.LSError{Code: errCodeInternal, Text: fallback + ": " + err.Error()}
}
