package liteserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"sort"

	"github.com/xssnick/gton/internal/extmsg"
	"github.com/xssnick/gton/service/blockproof"
	admission "github.com/xssnick/gton/service/externalmsg"
	"github.com/xssnick/gton/service/shard"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxGetTransactions       = 16
	maxListBlockTransactions = 256
	outMsgQueueSizeLimit     = 8000

	sendMessageErrorReasonNotConfigured      = "not_configured"
	sendMessageErrorReasonOversized          = "oversized"
	sendMessageErrorReasonParseFailed        = "parse_failed"
	sendMessageErrorReasonRateLimited        = "rate_limited"
	sendMessageErrorReasonAddressLimiterFull = "address_limiter_full"
	sendMessageErrorReasonNotReady           = "not_ready"
	sendMessageErrorReasonTVMRejected        = "tvm_rejected"
	sendMessageErrorReasonCheckFailed        = "check_failed"
	sendMessageErrorReasonOffline            = "offline"
	sendMessageErrorReasonBroadcastCapacity  = "broadcast_capacity"
	sendMessageErrorReasonBroadcastFailed    = "broadcast_failed"
)

var (
	errBlockCannotContainAccount = errors.New("obtained a block that cannot contain specified account")
	errInMsgEnvelopeNotFound     = errors.New("in message envelope not found")
)

func (s *Server) handleState(ctx context.Context, id *ton.BlockIDExt) any {
	if !isFullBlockID(id) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid BlockIdExt"}
	}
	if id.SeqNo > 1000 {
		return ton.LSError{Code: errCodeInternal, Text: "cannot request total state: possibly too large"}
	}
	if id.SeqNo == 0 {
		data, err := s.store.ZeroState(ctx, *id)
		if err != nil {
			return zeroStateErrorResponse(err, *id)
		}

		return ton.BlockState{
			ID:       blockproof.CloneBlockID(*id),
			RootHash: id.RootHash,
			FileHash: id.FileHash,
			Data:     data,
		}
	}

	root, err := s.loadStateRoot(ctx, *id)
	if err != nil {
		return errorResponse(err, "cannot load state "+storage.FormatBlockRef(*id))
	}

	data := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	fileHash := sha256.Sum256(data)

	return ton.BlockState{
		ID:       blockproof.CloneBlockID(*id),
		RootHash: root.Hash(0),
		FileHash: fileHash[:],
		Data:     data,
	}
}

func (s *Server) handleSendMessage(ctx context.Context, query ton.SendMessage) any {
	resp, _ := s.handleSendMessageWithReason(ctx, query)
	return resp
}

func (s *Server) handleSendMessageWithReason(ctx context.Context, query ton.SendMessage) (tl.Serializable, string) {
	const errorPrefix = "cannot apply external message to current state "

	sender := s.externalMessageSender
	if sender == nil {
		if s.messageSender == nil {
			return sendMessageLSError(sendMessageErrorReasonNotConfigured, errorPrefix+": not configured")
		}
		var err error
		sender, err = s.newExternalMessageSender()
		if err != nil {
			return sendMessageLSError(sendMessageErrorReasonNotConfigured, errorPrefix+": "+err.Error())
		}
		s.externalMessageSender = sender
	}

	if err := sender.Send(ctx, query.Body); err != nil {
		return sendMessageLSError(sendMessageErrorReason(err), errorPrefix+": "+err.Error())
	}
	return ton.SendMessageStatus{Status: 1}, ""
}

func sendMessageErrorReason(err error) string {
	switch {
	case errors.Is(err, admission.ErrMessageTooLarge):
		return sendMessageErrorReasonOversized
	case errors.Is(err, admission.ErrParseFailed):
		return sendMessageErrorReasonParseFailed
	case errors.Is(err, extmsg.ErrAddressRateLimited), errors.Is(err, extmsg.ErrAddressLimiterFull):
		return sendMessageAddressLimitErrorReason(err)
	case errors.Is(err, storage.ErrNotFound), errors.Is(err, admission.ErrRejected):
		return sendMessageCheckErrorReason(err)
	case errors.Is(err, admission.ErrBroadcastFailed):
		return sendMessageBroadcastErrorReason(err)
	default:
		return sendMessageErrorReasonCheckFailed
	}
}

func sendMessageLSError(reason string, text string) (tl.Serializable, string) {
	return ton.LSError{Code: errCodeUnspecified, Text: text}, reason
}

func sendMessageAddressLimitErrorReason(err error) string {
	switch {
	case errors.Is(err, extmsg.ErrAddressRateLimited):
		return sendMessageErrorReasonRateLimited
	case errors.Is(err, extmsg.ErrAddressLimiterFull):
		return sendMessageErrorReasonAddressLimiterFull
	default:
		return sendMessageErrorReasonCheckFailed
	}
}

func sendMessageCheckErrorReason(err error) string {
	switch {
	case errors.Is(err, storage.ErrNotFound):
		return sendMessageErrorReasonNotReady
	case errors.Is(err, admission.ErrRejected):
		return sendMessageErrorReasonTVMRejected
	default:
		return sendMessageErrorReasonCheckFailed
	}
}

func sendMessageBroadcastErrorReason(err error) string {
	if errors.Is(err, extmsg.ErrNetworkOffline) {
		return sendMessageErrorReasonOffline
	}
	if errors.Is(err, extmsg.ErrExternalBroadcastCapacityExceeded) {
		return sendMessageErrorReasonBroadcastCapacity
	}
	return sendMessageErrorReasonBroadcastFailed
}

type allShardsInfoParts struct {
	proof *cell.Cell
	data  *cell.Cell
}

func (s *Server) handleAllShardsInfo(ctx context.Context, id *ton.BlockIDExt) any {
	if !isFullBlockID(id) || id.Workchain != masterchainID {
		return ton.LSError{Code: errCodeProtoViolation, Text: "reference block must belong to the masterchain"}
	}

	key := liteResponseKey{kind: liteResponseAllShardsInfo, a: liteBlockKeyFromBlock(*id)}
	value, err := s.respCache.do(ctx, key, func(ctx context.Context) (any, error) {
		return s.buildAllShardsInfoParts(ctx, *id)
	})
	if err != nil {
		return errorResponse(err, "cannot load all shards info")
	}

	parts, ok := value.(allShardsInfoParts)
	if !ok {
		return ton.LSError{Code: errCodeInternal, Text: "invalid all shards info cache value"}
	}
	return ton.AllShardsInfo{
		ID:    blockproof.CloneBlockID(*id),
		Proof: []*cell.Cell{parts.proof},
		Data:  parts.data,
	}
}

func (s *Server) buildAllShardsInfoParts(ctx context.Context, id ton.BlockIDExt) (allShardsInfoParts, error) {
	root, err := s.store.BlockRoot(ctx, id)
	if err != nil {
		return allShardsInfoParts{}, queryErrorFromResponse(errorResponse(err, "cannot load block "+storage.FormatBlockRef(id)))
	}

	loader, err := root.BeginParse()
	if err != nil {
		return allShardsInfoParts{}, queryErrorFromResponse(errorResponse(err, "cannot unpack block "+storage.FormatBlockRef(id)))
	}

	var block tlb.Block
	if err = tlb.LoadFromCell(&block, loader); err != nil {
		return allShardsInfoParts{}, queryErrorFromResponse(errorResponse(err, "cannot unpack block "+storage.FormatBlockRef(id)))
	}
	if block.Extra == nil || block.Extra.Custom == nil || block.Extra.Custom.ShardHashes == nil {
		return allShardsInfoParts{}, newQueryError(errCodeInternal, "cannot unpack header of block "+cxxBlockIDExtString(id))
	}

	proof, err := allShardsInfoProof(root)
	if err != nil {
		return allShardsInfoParts{}, queryErrorFromResponse(errorResponse(err, "cannot create all shards proof"))
	}

	data := cell.BeginCell().MustStoreDict(block.Extra.Custom.ShardHashes).EndCell()
	return allShardsInfoParts{proof: proof, data: data}, nil
}

func (s *Server) handleShardInfo(ctx context.Context, query ton.GetShardInfo) any {
	if err := shard.Validate(query.Shard); err != nil {
		return ton.LSError{Code: errCodeProtoViolation, Text: "requested shard is invalid"}
	}
	if !isFullBlockID(query.ID) || query.ID.Workchain != masterchainID {
		return ton.LSError{Code: errCodeProtoViolation, Text: "reference block must belong to the masterchain"}
	}

	fragments, err := s.store.BlockFragments(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load masterchain state "+storage.FormatBlockRef(*query.ID))
	}

	extra, err := fragments.McStateExtra()
	if err != nil {
		return errorResponse(err, "cannot unpack masterchain state")
	}

	shardBlock, leaf, err := blockproof.ShardInfoFromHashes(extra.ShardHashes, query.Workchain, query.Shard, query.Exact)
	if errors.Is(err, storage.ErrNotFound) {
		shardBlock = ton.BlockIDExt{}
		leaf = nil
	} else if err != nil {
		return errorResponse(err, "cannot load shard info")
	}

	stateProof, err := fragments.ShardHashesProof(query.Workchain, query.Shard, query.Exact)
	if err != nil {
		return errorResponse(err, "cannot create shard info proof")
	}

	return ton.ShardInfo{
		ID:               blockproof.CloneBlockID(*query.ID),
		ShardBlock:       blockproof.CloneBlockID(shardBlock),
		ShardProof:       []*cell.Cell{fragments.BlockStateRootProof(), stateProof},
		ShardDescription: leaf,
	}
}

type configAllParts struct {
	stateProof  []byte
	configProof []byte
}

func (s *Server) handleConfig(ctx context.Context, id *ton.BlockIDExt, mode int32, all bool, params []int32) any {
	if !isFullBlockID(id) || id.Workchain != masterchainID {
		return ton.LSError{Code: errCodeProtoViolation, Text: "configuration parameters can be loaded with respect to a masterchain block only"}
	}
	publicMode := uint32(mode) & 0xffff
	if publicMode&configModePreviousKeyBlock != 0 {
		return s.handlePreviousKeyBlockConfig(ctx, *id, publicMode, all, params)
	}
	effectiveMode := publicMode
	if effectiveMode&configModeNeedPrevBlocks != 0 {
		effectiveMode |= configModeNeedCapabilities
	}

	if all {
		// getConfigAll is fully deterministic per (block, effectiveMode): both
		// proof BOCs are rebuilt from immutable block content, so memoize them.
		// getConfigParams stays uncached: its params set varies per client and
		// would only dilute the shared cache.
		key := liteResponseKey{kind: liteResponseConfigAll, mode: effectiveMode, a: liteBlockKeyFromBlock(*id)}
		value, err := s.respCache.do(ctx, key, func(ctx context.Context) (any, error) {
			return s.buildConfigAllParts(ctx, *id, effectiveMode)
		})
		if err != nil {
			return errorResponse(err, "cannot load config")
		}

		parts, ok := value.(configAllParts)
		if !ok {
			return ton.LSError{Code: errCodeInternal, Text: "invalid config cache value"}
		}
		return ton.ConfigAll{
			Mode:        int(effectiveMode),
			ID:          blockproof.CloneBlockID(*id),
			StateProof:  parts.stateProof,
			ConfigProof: parts.configProof,
		}
	}

	fragments, err := s.store.BlockFragments(ctx, *id)
	if err != nil {
		return errorResponse(err, "cannot load masterchain state "+storage.FormatBlockRef(*id))
	}

	stateProof, err := configProof(fragments.StateRoot(), effectiveMode, all, params)
	if err != nil {
		return errorResponse(err, "cannot create config proof")
	}
	proof := []*cell.Cell{fragments.BlockStateRootProof(), stateProof}

	return ton.ConfigAll{
		Mode:        int(effectiveMode),
		ID:          blockproof.CloneBlockID(*id),
		StateProof:  proof[0].ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
		ConfigProof: proof[1].ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
	}
}

func (s *Server) buildConfigAllParts(ctx context.Context, id ton.BlockIDExt, effectiveMode uint32) (configAllParts, error) {
	fragments, err := s.store.BlockFragments(ctx, id)
	if err != nil {
		return configAllParts{}, queryErrorFromResponse(errorResponse(err, "cannot load masterchain state "+storage.FormatBlockRef(id)))
	}

	stateProof, err := configProof(fragments.StateRoot(), effectiveMode, true, nil)
	if err != nil {
		return configAllParts{}, queryErrorFromResponse(errorResponse(err, "cannot create config proof"))
	}

	return configAllParts{
		stateProof:  fragments.BlockStateRootProof().ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
		configProof: stateProof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
	}, nil
}

func (s *Server) handlePreviousKeyBlockConfig(ctx context.Context, id ton.BlockIDExt, mode uint32, all bool, params []int32) any {
	keyBlock, keyRoot, err := s.previousKeyBlockRoot(ctx, id)
	if err != nil {
		return errorResponse(err, "cannot load previous key block config")
	}

	proof, err := keyBlockConfigProof(keyRoot, mode, all, params)
	if err != nil {
		return errorResponse(err, "cannot create key block config proof")
	}

	return ton.ConfigAll{
		Mode:        int(mode),
		ID:          blockproof.CloneBlockID(keyBlock),
		StateProof:  nil,
		ConfigProof: proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
	}
}

func (s *Server) previousKeyBlockRoot(ctx context.Context, id ton.BlockIDExt) (ton.BlockIDExt, *cell.Cell, error) {
	root, err := s.store.BlockRoot(ctx, id)
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}

	loader, err := root.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}

	var block tlb.Block
	if err = tlb.LoadFromCell(&block, loader); err != nil {
		return ton.BlockIDExt{}, nil, err
	}
	if block.BlockInfo.KeyBlock {
		return id, root, nil
	}
	if block.BlockInfo.PrevKeyBlockSeqno == 0 {
		return ton.BlockIDExt{}, nil, storage.ErrNotFound
	}

	keyBlock, err := s.store.LookupBlockBySeqNo(ctx, storage.BlockSeqRef{Workchain: masterchainID, Shard: masterchainShard, SeqNo: block.BlockInfo.PrevKeyBlockSeqno})
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}

	keyRoot, err := s.store.BlockRoot(ctx, keyBlock)
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}

	keyLoader, err := keyRoot.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}

	var key tlb.Block
	if err = tlb.LoadFromCell(&key, keyLoader); err != nil {
		return ton.BlockIDExt{}, nil, err
	}
	if !key.BlockInfo.KeyBlock {
		return ton.BlockIDExt{}, nil, fmt.Errorf("block %s is not a key block", storage.FormatBlockRef(keyBlock))
	}

	return keyBlock, keyRoot, nil
}

func (s *Server) handleLibraries(ctx context.Context, query ton.GetLibraries) any {
	current, err := s.store.CurrentState(ctx)
	if err != nil {
		return errorResponse(err, "cannot obtain current masterchain state")
	}

	fragments, err := s.store.BlockFragments(ctx, current.Masterchain.Block)
	if err != nil {
		return errorResponse(err, "cannot load current masterchain state")
	}

	libraries, err := fragments.GlobalLibraries()
	if err != nil {
		return errorResponse(err, "cannot load libraries")
	}

	entries, err := libraryEntriesFromDict(libraries, query.LibraryList, 0)
	if err != nil {
		return errorResponse(err, "cannot load libraries")
	}
	return ton.LibraryResult{Result: entries}
}

func (s *Server) handleLibrariesWithProof(ctx context.Context, query ton.GetLibrariesWithProof) any {
	if !isFullBlockID(query.ID) || query.ID.Workchain != masterchainID {
		return ton.LSError{Code: errCodeProtoViolation, Text: "reference block must belong to the masterchain"}
	}

	fragments, err := s.store.BlockFragments(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load masterchain state "+storage.FormatBlockRef(*query.ID))
	}

	libraries, err := fragments.GlobalLibraries()
	if err != nil {
		return errorResponse(err, "cannot load libraries")
	}

	entries, err := libraryEntriesFromDict(libraries, query.LibraryList, query.Mode)
	if err != nil {
		return errorResponse(err, "cannot load libraries")
	}

	dataProof, err := librariesProof(fragments.StateRoot(), query.LibraryList, query.Mode)
	if err != nil {
		return errorResponse(err, "cannot create library proof")
	}
	proof := []*cell.Cell{fragments.BlockStateRootProof(), dataProof}

	return ton.LibraryResultWithProof{
		ID:         blockproof.CloneBlockID(*query.ID),
		Mode:       query.Mode,
		Result:     entries,
		StateProof: proof[0].ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
		DataProof:  proof[1].ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
	}
}

func (s *Server) handleOneTransaction(ctx context.Context, query ton.GetOneTransaction) any {
	if !isFullBlockID(query.ID) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "block id in getOneTransaction() is invalid"}
	}
	if query.AccID == nil || len(query.AccID.ID) != 32 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid account id"}
	}
	if query.ID.Workchain != query.AccID.Workchain {
		return ton.LSError{Code: errCodeProtoViolation, Text: "requested account id is not contained in the shard of the specified block"}
	}

	addr := address.NewAddress(0, byte(query.AccID.Workchain), query.AccID.ID)
	if !tlb.ShardID(uint64(query.ID.Shard)).ContainsAddress(addr) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "requested account id is not contained in the shard of the specified block"}
	}

	root, err := s.store.BlockRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load block "+storage.FormatBlockRef(*query.ID))
	}

	proof, tx, err := blockTransactionProof(root, query.AccID.ID, uint64(query.LT))
	if err != nil {
		return errorResponse(err, "cannot create transaction proof")
	}

	var data []byte
	if tx != nil {
		data = tx.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	}

	return ton.TransactionInfo{
		ID:          blockproof.CloneBlockID(*query.ID),
		Proof:       proof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
		Transaction: data,
	}
}

func (s *Server) handleGetTransactions(ctx context.Context, query ton.GetTransactions) any {
	if query.AccID == nil || len(query.AccID.ID) != 32 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid account id"}
	}
	if len(query.TxHash) != 32 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid transaction hash"}
	}
	if query.AccID.Workchain == workchainInvalid {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid workchain specified"}
	}

	limit := int(query.Limit)
	if limit > maxGetTransactions {
		limit = maxGetTransactions
	}
	if limit <= 0 {
		return ton.TransactionList{IDs: []*ton.BlockIDExt{}}
	}

	account := query.AccID.ID
	cursorLT := uint64(query.LT)
	cursorHash := query.TxHash
	roots := make([]*cell.Cell, 0, limit)
	ids := make([]*ton.BlockIDExt, 0, limit)

	var currentBlock *transactionSearchBlock

	for cursorLT != 0 && len(roots) < limit {
		tx, block, err := accountTransactionFromCurrentBlock(currentBlock, query.AccID.Workchain, account, cursorLT)
		switch {
		case errors.Is(err, storage.ErrNotFound):
			tx, block, err = s.accountTransactionFromShardHint(ctx, currentBlock, query.AccID.Workchain, account, cursorLT)
			switch {
			case errors.Is(err, storage.ErrNotFound):
				blockID, err := s.lookupBlockByAccountLT(ctx, query.AccID.Workchain, account, cursorLT)
				if errors.Is(err, storage.ErrNotFound) {
					if len(roots) > 0 {
						return getTransactionsResponse(roots, ids)
					}
					return ton.LSError{Code: errCodeProtoViolation, Text: "cannot locate transaction in block with specified logical time"}
				}
				if errors.Is(err, errBlockCannotContainAccount) {
					return ton.LSError{Code: errCodeProtoViolation, Text: err.Error()}
				}
				if err != nil {
					if len(roots) > 0 {
						return getTransactionsResponse(roots, ids)
					}
					return ton.LSError{Code: errCodeProtoViolation, Text: "cannot compute block with specified transaction: " + err.Error()}
				}

				root, err := s.store.BlockRoot(ctx, blockID)
				if err != nil {
					if len(roots) > 0 {
						return getTransactionsResponse(roots, ids)
					}
					return ton.LSError{Code: errCodeProtoViolation, Text: fmt.Sprintf("cannot load block %s with specified transaction: %s", cxxBlockIDExtString(blockID), err.Error())}
				}

				block, err = transactionSearchBlockFromRoot(blockID, root)
				if err != nil {
					if len(roots) > 0 {
						return getTransactionsResponse(roots, ids)
					}
					return ton.LSError{Code: errCodeProtoViolation, Text: fmt.Sprintf("cannot unpack block %s with specified transaction: %s", cxxBlockIDExtString(blockID), err.Error())}
				}
				if !block.containsLT(cursorLT) {
					if len(roots) > 0 {
						return getTransactionsResponse(roots, ids)
					}
					return ton.LSError{Code: errCodeProtoViolation, Text: "cannot locate transaction in block with specified logical time"}
				}

				tx, err = block.findTransaction(account, cursorLT)
				if errors.Is(err, storage.ErrNotFound) {
					if len(roots) > 0 {
						return getTransactionsResponse(roots, ids)
					}
					return ton.LSError{Code: errCodeProtoViolation, Text: "cannot locate transaction in block with specified logical time"}
				}
				if err != nil {
					return errorResponse(err, "cannot get transaction")
				}
			case err != nil:
				return errorResponse(err, "cannot get transaction")
			}

			currentBlock = block
		case errors.Is(err, errBlockCannotContainAccount):
			return ton.LSError{Code: errCodeProtoViolation, Text: err.Error()}
		case err != nil:
			return errorResponse(err, "cannot get transaction")
		}

		txHash := tx.Hash()
		if !bytes.Equal(txHash, cursorHash) {
			return ton.LSError{Code: errCodeProtoViolation, Text: "transaction hash mismatch"}
		}

		header, err := loadTransactionChainHeader(tx)
		if err != nil {
			return errorResponse(err, "cannot unpack transaction")
		}
		if header.lt != cursorLT {
			return ton.LSError{Code: errCodeProtoViolation, Text: "transaction lt mismatch"}
		}
		if !bytes.Equal(header.account, account) {
			return ton.LSError{Code: errCodeProtoViolation, Text: "transaction account mismatch"}
		}
		if header.prevLT >= cursorLT {
			return ton.LSError{Code: errCodeProtoViolation, Text: "previous transaction time is not less than the current one"}
		}

		roots = append(roots, tx)
		ids = append(ids, blockproof.CloneBlockID(block.id))

		cursorLT = header.prevLT
		if cursorLT == 0 {
			break
		}
		if len(header.prevHash) != 32 {
			return ton.LSError{Code: errCodeProtoViolation, Text: "transaction previous hash is invalid"}
		}
		cursorHash = header.prevHash
	}

	return getTransactionsResponse(roots, ids)
}

type transactionSearchBlock struct {
	id      ton.BlockIDExt
	root    *cell.Cell
	startLT uint64
	endLT   uint64

	// The parsed dictionaries belong to a single query handler, so chains that
	// stay inside one block can reuse them without locking.
	accounts     *tlb.ShardAccountBlocks
	accountBlock *tlb.AccountBlock
}

func (b *transactionSearchBlock) findTransaction(account []byte, lt uint64) (*cell.Cell, error) {
	if b.accounts == nil {
		accounts, err := blockTransactionAccounts(b.root)
		if err != nil {
			return nil, err
		}
		b.accounts = accounts
	}
	if b.accountBlock == nil {
		if b.accounts.Accounts == nil {
			return nil, storage.ErrNotFound
		}

		value, err := b.accounts.Accounts.LoadValueWithExtra(blockproof.AccountKey(account))
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return nil, storage.ErrNotFound
		}
		if err != nil {
			return nil, fmt.Errorf("load account block: %w", err)
		}
		b.accountBlock, err = accountBlockFromValue(value)
		if err != nil {
			return nil, err
		}
	}
	return findTransactionInAccountBlock(b.accountBlock, lt)
}

func transactionSearchBlockFromRoot(id ton.BlockIDExt, root *cell.Cell) (*transactionSearchBlock, error) {
	header, err := loadBlockHeader(root)
	if err != nil {
		return nil, err
	}

	return &transactionSearchBlock{
		id:      id,
		root:    root,
		startLT: header.StartLt,
		endLT:   header.EndLt,
	}, nil
}

func (b *transactionSearchBlock) containsLT(lt uint64) bool {
	return b.startLT < lt && lt < b.endLT
}

func accountTransactionFromCurrentBlock(block *transactionSearchBlock, workchain int32, account []byte, lt uint64) (*cell.Cell, *transactionSearchBlock, error) {
	if block == nil {
		return nil, nil, storage.ErrNotFound
	}
	if !blockproof.BlockContainsAccount(block.id, workchain, account) {
		return nil, nil, errBlockCannotContainAccount
	}
	if !block.containsLT(lt) {
		return nil, nil, storage.ErrNotFound
	}

	tx, err := block.findTransaction(account, lt)
	if err != nil {
		return nil, nil, err
	}
	return tx, block, nil
}

func (s *Server) accountTransactionFromShardHint(ctx context.Context, current *transactionSearchBlock, workchain int32, account []byte, lt uint64) (*cell.Cell, *transactionSearchBlock, error) {
	if current == nil || !blockproof.BlockContainsAccount(current.id, workchain, account) {
		return nil, nil, storage.ErrNotFound
	}

	// The shard hint is only a fast path: chains can cross split/merge boundaries,
	// so any hint failure falls back to the authoritative account-prefix lookup.
	hintKey := storage.BlockHistoryKey{Workchain: workchain, Shard: current.id.Shard}
	hintedBlock, err := s.store.LookupBlockByLT(ctx, hintKey, lt)
	if err != nil {
		return nil, nil, storage.ErrNotFound
	}
	if !blockproof.BlockContainsAccount(hintedBlock, workchain, account) {
		return nil, nil, storage.ErrNotFound
	}

	root, err := s.store.BlockRoot(ctx, hintedBlock)
	if err != nil {
		return nil, nil, storage.ErrNotFound
	}

	block, err := transactionSearchBlockFromRoot(hintedBlock, root)
	if err != nil {
		return nil, nil, storage.ErrNotFound
	}
	if !block.containsLT(lt) {
		return nil, nil, storage.ErrNotFound
	}

	tx, err := block.findTransaction(account, lt)
	if err != nil {
		return nil, nil, storage.ErrNotFound
	}
	return tx, block, nil
}

func (s *Server) lookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error) {
	block, err := s.store.LookupBlockByAccountLT(ctx, workchain, account, lt)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if !blockproof.BlockContainsAccount(block, workchain, account) {
		return ton.BlockIDExt{}, errBlockCannotContainAccount
	}
	return block, nil
}

func getTransactionsResponse(roots []*cell.Cell, ids []*ton.BlockIDExt) any {
	data, err := cell.ToBOCWithOptionsErr(roots, cell.BOCSerializeOptions{})
	if err != nil {
		return errorResponse(err, "cannot serialize transactions")
	}
	return ton.TransactionList{
		IDs:          ids,
		Transactions: data,
	}
}

type transactionChainHeader struct {
	account  []byte
	lt       uint64
	prevHash []byte
	prevLT   uint64
}

func loadTransactionChainHeader(tx *cell.Cell) (transactionChainHeader, error) {
	loader, err := tx.BeginParse()
	if err != nil {
		return transactionChainHeader{}, err
	}

	magic, err := loader.LoadUInt(4)
	if err != nil {
		return transactionChainHeader{}, err
	}
	if magic != 0x7 {
		return transactionChainHeader{}, fmt.Errorf("invalid transaction magic %x", magic)
	}

	account, err := loader.LoadSlice(256)
	if err != nil {
		return transactionChainHeader{}, err
	}
	lt, err := loader.LoadUInt(64)
	if err != nil {
		return transactionChainHeader{}, err
	}
	prevHash, err := loader.LoadSlice(256)
	if err != nil {
		return transactionChainHeader{}, err
	}
	prevLT, err := loader.LoadUInt(64)
	if err != nil {
		return transactionChainHeader{}, err
	}

	return transactionChainHeader{
		account:  account,
		lt:       lt,
		prevHash: prevHash,
		prevLT:   prevLT,
	}, nil
}

func cxxBlockIDExtString(block ton.BlockIDExt) string {
	return fmt.Sprintf("(%d,%016x,%d):%x:%x", block.Workchain, uint64(block.Shard), block.SeqNo, block.RootHash, block.FileHash)
}

func (s *Server) handleListBlockTransactions(ctx context.Context, query ton.ListBlockTransactions) any {
	if !isFullBlockID(query.ID) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid BlockIdExt"}
	}
	if query.Mode&128 != 0 && (query.After == nil || len(query.After.Account) != 32) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid start transaction id"}
	}

	root, err := s.store.BlockRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load block "+storage.FormatBlockRef(*query.ID))
	}

	proofRoot := root
	var proofBuilder *cell.MerkleProofBuilder
	if query.Mode&32 != 0 {
		proofBuilder = blockproof.NewProofBuilder(root)
		proofRoot = proofBuilder.Root()
	}

	txs, err := listBlockTransactions(proofRoot, query.Mode, query.Count, query.After, true)
	if err != nil {
		return errorResponse(err, "cannot list block transactions")
	}

	var proof *cell.Cell
	if proofBuilder != nil {
		blockProof, err := proofBuilder.CreateProof()
		if err != nil {
			return errorResponse(err, "cannot create block transaction proof")
		}
		proof = blockProof
	}

	ids := make([]ton.TransactionID, len(txs.items))
	txIDMode := query.Mode &^ 256
	for i, item := range txs.items {
		mode := txIDMode
		if item.metadata != nil {
			mode |= 256
		}
		ids[i] = ton.TransactionID{
			Flags:    mode,
			Account:  item.account,
			LT:       item.lt,
			Hash:     item.hash,
			Metadata: item.metadata,
		}
	}

	return ton.BlockTransactions{
		ID:             blockproof.CloneBlockID(*query.ID),
		ReqCount:       int32(query.Count),
		Incomplete:     txs.incomplete,
		TransactionIds: ids,
		Proof:          proof,
	}
}

func (s *Server) handleListBlockTransactionsExt(ctx context.Context, query ton.ListBlockTransactionsExt) any {
	if !isFullBlockID(query.ID) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid BlockIdExt"}
	}
	if query.Mode&128 != 0 && (query.After == nil || len(query.After.Account) != 32) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid start transaction id"}
	}

	root, err := s.store.BlockRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load block "+storage.FormatBlockRef(*query.ID))
	}

	proofRoot := root
	var proofBuilder *cell.MerkleProofBuilder
	if query.Mode&32 != 0 || query.Mode&256 != 0 {
		proofBuilder = blockproof.NewProofBuilder(root)
		proofRoot = proofBuilder.Root()
	}

	list, err := listBlockTransactions(proofRoot, query.Mode, query.Count, query.After, false)
	if err != nil {
		return errorResponse(err, "cannot list block transactions")
	}

	txs := make([]*cell.Cell, len(list.items))
	for i, item := range list.items {
		txs[i] = item.cell
	}

	var proof []byte
	if proofBuilder != nil {
		blockProof, err := proofBuilder.CreateProof()
		if err != nil {
			return errorResponse(err, "cannot create block transaction proof")
		}
		proof = blockProof.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	}

	return ton.BlockTransactionsExt{
		ID:           blockproof.CloneBlockID(*query.ID),
		ReqCount:     int32(query.Count),
		Incomplete:   list.incomplete,
		Transactions: txs,
		Proof:        proof,
	}
}

func (s *Server) handleValidatorStats(ctx context.Context, query ton.GetValidatorStats) any {
	if query.Limit <= 0 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "requested entry count limit must be positive"}
	}
	if query.Mode&^uint32(7) != 0 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "unknown flags set in mode"}
	}
	if !isFullBlockID(query.ID) || query.ID.Workchain != masterchainID {
		return ton.LSError{Code: errCodeProtoViolation, Text: "reference block must belong to the masterchain"}
	}

	fragments, err := s.store.BlockFragments(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load masterchain state "+storage.FormatBlockRef(*query.ID))
	}

	dataProof, count, complete, err := validatorStatsProofAndCount(fragments.StateRoot(), query.Mode, query.Limit, query.StartAfter)
	if err != nil {
		return errorResponse(err, "cannot load validator stats")
	}
	proof := []*cell.Cell{fragments.BlockStateRootProof(), dataProof}

	return ton.ValidatorStats{
		Mode:       query.Mode & 0xff,
		ID:         blockproof.CloneBlockID(*query.ID),
		Count:      count,
		Complete:   complete,
		StateProof: proof[0].ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
		DataProof:  proof[1].ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false}),
	}
}

func (s *Server) handleBlockOutMsgQueueSize(ctx context.Context, query ton.GetBlockOutMsgQueueSize) any {
	if !isFullBlockID(query.ID) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid BlockIdExt"}
	}

	var blockProof *cell.Cell
	var root *cell.Cell
	if query.Mode&1 != 0 {
		fragments, err := s.store.BlockFragments(ctx, *query.ID)
		if err != nil {
			return errorResponse(err, "cannot load state "+storage.FormatBlockRef(*query.ID))
		}
		root = fragments.StateRoot()
		blockProof = fragments.BlockStateRootProof()
	} else {
		stateRoot, err := s.loadStateRoot(ctx, *query.ID)
		if err != nil {
			return errorResponse(err, "cannot load state "+storage.FormatBlockRef(*query.ID))
		}
		root = stateRoot
	}

	size, err := outMsgQueueSize(root)
	if err != nil {
		return errorResponse(err, "cannot load out msg queue size")
	}

	resp := ton.BlockOutMsgQueueSize{
		Mode: query.Mode,
		ID:   blockproof.CloneBlockID(*query.ID),
		Size: int64(size),
	}
	if query.Mode&1 != 0 {
		dataProof, err := outMsgQueueSizeProof(root)
		if err != nil {
			return errorResponse(err, "cannot create out msg queue proof")
		}
		proof := []*cell.Cell{blockProof, dataProof}
		resp.Proof = cell.ToBOCWithOptions(proof, cell.BOCSerializeOptions{WithCRC32C: false})
	}
	return resp
}

func (s *Server) handleOutMsgQueueSizes(ctx context.Context, query ton.GetOutMsgQueueSizes) any {
	var filter *storage.ShardKey
	if query.Mode&1 != 0 {
		if err := shard.Validate(query.Shard); err != nil {
			return ton.LSError{Code: errCodeProtoViolation, Text: "requested shard is invalid"}
		}
		filter = &storage.ShardKey{Workchain: query.WC, Shard: query.Shard}
	}

	current, err := s.store.CurrentState(ctx)
	if err != nil {
		return errorResponse(err, "cannot obtain current masterchain state")
	}

	blocks := make([]ton.BlockIDExt, 0, 1+len(current.Shards))
	if filter == nil || filter.Workchain == current.Masterchain.Block.Workchain && shard.Intersects(filter.Shard, current.Masterchain.Block.Shard) {
		blocks = append(blocks, current.Masterchain.Block)
	}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		state := current.Shards[key]
		if filter != nil && (filter.Workchain != state.Block.Workchain || !shard.Intersects(filter.Shard, state.Block.Shard)) {
			continue
		}
		blocks = append(blocks, state.Block)
	}

	sizes := make([]ton.OutMsgQueueSize, 0, len(blocks))
	for _, block := range blocks {
		root, err := s.loadStateRoot(ctx, block)
		if err != nil {
			return errorResponse(err, "cannot load state "+storage.FormatBlockRef(block))
		}

		size, err := outMsgQueueSize(root)
		if err != nil {
			return errorResponse(err, "cannot load out msg queue size")
		}
		if size > math.MaxInt32 {
			size = math.MaxInt32
		}
		sizes = append(sizes, ton.OutMsgQueueSize{
			ID:   blockproof.CloneBlockID(block),
			Size: int32(size),
		})
	}

	return ton.OutMsgQueueSizes{
		Shards:               sizes,
		ExtMsgQueueSizeLimit: outMsgQueueSizeLimit,
	}
}

type blockTxItem struct {
	account  []byte
	lt       uint64
	hash     []byte
	cell     *cell.Cell
	metadata *ton.TransactionMetadata
}

type blockTransactionList struct {
	items      []blockTxItem
	incomplete bool
}

// skipSingleRef bounds an aug dict leaf value of the `^X` shape without
// materializing the ref.
func skipSingleRef(loader *cell.Slice) error {
	return loader.SkipBitsAndRefs(0, 1)
}

// listBlockTransactions walks ShardAccountBlocks with positioned iterators:
// one descent to the start position per dictionary instead of a root-to-leaf
// lookup per returned item, visiting the same cells, so proofs built from a
// traced root are unchanged. attachMetadata distinguishes the plain variant
// (metadata objects go into transaction ids) from the Ext one, which only
// needs the InMsgDescr path visited so the proof retains it.
func listBlockTransactions(root *cell.Cell, mode uint32, reqCount uint32, after *ton.TransactionID3, attachMetadata bool) (blockTransactionList, error) {
	withMetadata := mode&256 != 0
	data, err := blockTransactionData(root, withMetadata)
	if err != nil {
		return blockTransactionList{}, err
	}
	if reqCount == 0 {
		return blockTransactionList{incomplete: true}, nil
	}
	if data.shardAccounts.Accounts == nil || data.shardAccounts.Accounts.AugmentedDictionary == nil {
		return blockTransactionList{}, nil
	}

	reverse := mode&64 != 0
	limit := int(reqCount)
	if limit > maxListBlockTransactions {
		limit = maxListBlockTransactions
	}

	accounts := data.shardAccounts.Accounts.AugmentedDictionary
	var accIt *cell.AugDictIterator
	var continueAccount []byte
	var continueLT uint64
	if mode&128 != 0 && after != nil {
		continueAccount = after.Account
		continueLT = after.LT
		accIt, err = accounts.IteratorExtraAt(blockproof.AccountKey(continueAccount), reverse, false, true)
	} else {
		accIt, err = accounts.IteratorExtra(reverse, false)
	}
	if err != nil {
		return blockTransactionList{}, fmt.Errorf("lookup account block: %w", err)
	}

	out := make([]blockTxItem, 0, limit)
	eof := false

	for len(out) < limit {
		if !accIt.Next() {
			if err = accIt.Err(); err != nil {
				return blockTransactionList{}, fmt.Errorf("lookup account block: %w", err)
			}
			eof = true
			break
		}

		accountKeyLoader, err := accIt.Key().BeginParse()
		if err != nil {
			return blockTransactionList{}, fmt.Errorf("begin parse account key: %w", err)
		}
		account, err := accountKeyLoader.LoadSlice(256)
		if err != nil {
			return blockTransactionList{}, fmt.Errorf("load account key: %w", err)
		}

		// acc_trans#5 account_addr:bits256 transactions:(HashmapAug 64 ^Transaction CurrencyCollection)
		accountValue := accIt.Value()
		magic, err := accountValue.LoadUInt(4)
		if err != nil {
			return blockTransactionList{}, fmt.Errorf("load account block magic: %w", err)
		}
		if magic != 0x5 {
			return blockTransactionList{}, fmt.Errorf("invalid account block magic %x", magic)
		}
		if err = accountValue.SkipBits(256); err != nil {
			return blockTransactionList{}, fmt.Errorf("load account block address: %w", err)
		}

		var ltIt *cell.AugDictIterator
		if continueAccount != nil && bytes.Equal(account, continueAccount) {
			startLT := cell.BeginCell().MustStoreUInt(continueLT, 64).EndCell()
			ltIt, err = accountValue.AugDictInlineIteratorAt(64, tlb.AugAccountTransactions{}, skipSingleRef, startLT, reverse, false, false)
		} else {
			ltIt, err = accountValue.AugDictInlineIterator(64, tlb.AugAccountTransactions{}, skipSingleRef, reverse, false)
		}
		if err != nil {
			return blockTransactionList{}, fmt.Errorf("lookup account transaction: %w", err)
		}

		for len(out) < limit && ltIt.Next() {
			txKeyLoader, err := ltIt.Key().BeginParse()
			if err != nil {
				return blockTransactionList{}, fmt.Errorf("begin parse transaction lt: %w", err)
			}
			lt, err := txKeyLoader.LoadUInt(64)
			if err != nil {
				return blockTransactionList{}, fmt.Errorf("load transaction lt: %w", err)
			}
			txCell, err := ltIt.Value().LoadRefCell()
			if err != nil {
				return blockTransactionList{}, fmt.Errorf("load transaction cell: %w", err)
			}

			item := blockTxItem{
				account: account,
				lt:      lt,
				hash:    txCell.Hash(),
				cell:    txCell,
			}
			if withMetadata {
				if attachMetadata {
					metadata, err := transactionMetadata(data.inMsgDescr, txCell)
					if err != nil {
						return blockTransactionList{}, err
					}
					item.metadata = metadata
				} else if err = visitTransactionMetadata(data.inMsgDescr, txCell); err != nil {
					return blockTransactionList{}, err
				}
			}
			out = append(out, item)
		}
		if err = ltIt.Err(); err != nil {
			return blockTransactionList{}, fmt.Errorf("lookup account transaction: %w", err)
		}
	}

	return blockTransactionList{items: out, incomplete: !eof}, nil
}

type blockTransactionsData struct {
	shardAccounts *tlb.ShardAccountBlocks
	inMsgDescr    *cell.AugmentedDictionary
}

func blockTransactionAccounts(root *cell.Cell) (*tlb.ShardAccountBlocks, error) {
	data, err := blockTransactionData(root, false)
	if err != nil {
		return nil, err
	}
	return data.shardAccounts, nil
}

func blockTransactionData(root *cell.Cell, withMetadata bool) (blockTransactionsData, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return blockTransactionsData{}, fmt.Errorf("begin parse block: %w", err)
	}
	magic, err := loader.LoadUInt(32)
	if err != nil {
		return blockTransactionsData{}, fmt.Errorf("load block magic: %w", err)
	}
	if magic != 0x11ef55aa {
		return blockTransactionsData{}, fmt.Errorf("invalid block magic %x", magic)
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return blockTransactionsData{}, fmt.Errorf("load block global id: %w", err)
	}

	extraRoot, err := root.PeekRef(3)
	if err != nil {
		return blockTransactionsData{}, fmt.Errorf("load block extra: %w", err)
	}
	extraLoader, err := extraRoot.BeginParse()
	if err != nil {
		return blockTransactionsData{}, fmt.Errorf("begin parse block extra: %w", err)
	}

	var extra tlb.BlockExtra
	if err := tlb.LoadFromCell(&extra, extraLoader); err != nil {
		return blockTransactionsData{}, fmt.Errorf("load block extra: %w", err)
	}
	if extra.ShardAccountBlocks == nil {
		return blockTransactionsData{}, fmt.Errorf("block does not contain shard account blocks")
	}

	shardAccountsLoader, err := extra.ShardAccountBlocks.BeginParse()
	if err != nil {
		return blockTransactionsData{}, fmt.Errorf("begin parse shard account blocks: %w", err)
	}

	var shardAccounts tlb.ShardAccountBlocks
	if err := tlb.LoadFromCell(&shardAccounts, shardAccountsLoader); err != nil {
		return blockTransactionsData{}, fmt.Errorf("load shard account blocks: %w", err)
	}

	var inMsgDescr *cell.AugmentedDictionary
	if withMetadata && extra.InMsgDesc != nil && (extra.InMsgDesc.BitsSize() > 0 || extra.InMsgDesc.RefsNum() > 0) {
		inMsgLoader, err := extra.InMsgDesc.BeginParse()
		if err != nil {
			return blockTransactionsData{}, fmt.Errorf("begin parse inbound message descriptor: %w", err)
		}
		dict, err := inMsgLoader.LoadAugDict(256, cell.ReadOnlyAugmentation{SkipExtraFn: skipImportFees}, false)
		if err != nil {
			return blockTransactionsData{}, fmt.Errorf("load inbound message descriptor: %w", err)
		}
		inMsgDescr = dict
	}

	return blockTransactionsData{shardAccounts: &shardAccounts, inMsgDescr: inMsgDescr}, nil
}

// skipCurrencyCollection consumes a CurrencyCollection without materializing
// coin values or the extra-currency dictionary; the skipped dict ref stays
// unvisited so proof contents are unchanged.
func skipCurrencyCollection(loader *cell.Slice) error {
	coinsLen, err := loader.LoadUInt(4)
	if err != nil {
		return err
	}
	if err = loader.SkipBits(uint(coinsLen) * 8); err != nil {
		return err
	}
	hasExtra, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if !hasExtra {
		return nil
	}
	return loader.SkipBitsAndRefs(0, 1)
}

func accountBlockFromValue(value *cell.Slice) (*tlb.AccountBlock, error) {
	if err := skipCurrencyCollection(value); err != nil {
		return nil, fmt.Errorf("load account block augmentation: %w", err)
	}

	var accountBlock tlb.AccountBlock
	if err := tlb.LoadFromCell(&accountBlock, value); err != nil {
		return nil, fmt.Errorf("load account block: %w", err)
	}
	return &accountBlock, nil
}

func transactionCellFromValue(value *cell.Slice) (*cell.Cell, error) {
	if err := skipCurrencyCollection(value); err != nil {
		return nil, fmt.Errorf("load transaction augmentation: %w", err)
	}
	txCell, err := value.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load transaction cell: %w", err)
	}
	return txCell, nil
}

func transactionMetadata(inMsgDescr *cell.AugmentedDictionary, txCell *cell.Cell) (*ton.TransactionMetadata, error) {
	envelope, err := transactionInboundMessageEnvelope(inMsgDescr, txCell)
	if err != nil || envelope == nil {
		return nil, err
	}
	return msgEnvelopeMetadata(envelope)
}

// visitTransactionMetadata walks the same InMsgDescr path as transactionMetadata
// so a traced root retains it in the proof, but only loads the envelope cell
// without validating the metadata payload, mirroring the C++
// process_all_in_msg_metadata behaviour of listBlockTransactionsExt.
func visitTransactionMetadata(inMsgDescr *cell.AugmentedDictionary, txCell *cell.Cell) error {
	envelope, err := transactionInboundMessageEnvelope(inMsgDescr, txCell)
	if err != nil || envelope == nil {
		return err
	}
	if _, err = envelope.BeginParse(); err != nil {
		return err
	}
	return nil
}

func transactionInboundMessageEnvelope(inMsgDescr *cell.AugmentedDictionary, txCell *cell.Cell) (*cell.Cell, error) {
	msg, err := transactionInboundMessageCell(txCell)
	if err != nil {
		return nil, err
	}
	if msg == nil {
		return nil, nil
	}

	msgHash := msg.Hash()
	if inMsgDescr == nil {
		return nil, fmt.Errorf("no InMsg in InMsgDescr for message with hash %x", msgHash)
	}

	value, err := inMsgDescr.LoadValueWithExtra(blockproof.AccountKey(msgHash))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, fmt.Errorf("no InMsg in InMsgDescr for message with hash %x", msgHash)
	}
	if err != nil {
		return nil, fmt.Errorf("lookup inbound message descriptor: %w", err)
	}
	if err = skipImportFees(value); err != nil {
		return nil, fmt.Errorf("load inbound message descriptor augmentation: %w", err)
	}

	envelope, err := inMsgEnvelope(value)
	if errors.Is(err, errInMsgEnvelopeNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return envelope, nil
}

func transactionInboundMessageCell(txCell *cell.Cell) (*cell.Cell, error) {
	loader, err := txCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse transaction: %w", err)
	}

	magic, err := loader.LoadUInt(4)
	if err != nil {
		return nil, fmt.Errorf("load transaction magic: %w", err)
	}
	if magic != 0b0111 {
		return nil, fmt.Errorf("invalid transaction magic %b", magic)
	}
	const headerBits = 256 + 64 + 256 + 64 + 32 + 15 + 2 + 2
	if err = loader.SkipBits(headerBits); err != nil {
		return nil, fmt.Errorf("load transaction header: %w", err)
	}

	ioCell, err := loader.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load transaction messages: %w", err)
	}
	if err = tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, fmt.Errorf("load transaction total fees: %w", err)
	}
	if loader.BitsLeft() != 0 || loader.RefsNum() != 2 {
		return nil, fmt.Errorf("invalid transaction tail: %d bits, %d refs", loader.BitsLeft(), loader.RefsNum())
	}

	io, err := ioCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse transaction messages: %w", err)
	}
	hasInbound, err := io.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("load transaction inbound message flag: %w", err)
	}
	var inbound *cell.Cell
	if hasInbound {
		inbound, err = io.LoadRefCell()
		if err != nil {
			return nil, fmt.Errorf("load transaction inbound message: %w", err)
		}
	}
	if _, err = io.LoadDict(15); err != nil {
		return nil, fmt.Errorf("load transaction outbound messages: %w", err)
	}
	if io.BitsLeft() != 0 || io.RefsNum() != 0 {
		return nil, fmt.Errorf("invalid transaction messages tail: %d bits, %d refs", io.BitsLeft(), io.RefsNum())
	}
	return inbound, nil
}

func skipImportFees(loader *cell.Slice) error {
	coinsLen, err := loader.LoadUInt(4)
	if err != nil {
		return err
	}
	if err = loader.SkipBits(uint(coinsLen) * 8); err != nil {
		return err
	}
	return skipCurrencyCollection(loader)
}

func inMsgEnvelope(loader *cell.Slice) (*cell.Cell, error) {
	tag3, err := loader.LoadUInt(3)
	if err != nil {
		return nil, err
	}

	switch tag3 {
	case 0b011, 0b100:
		return loader.LoadRefCell()
	case 0b001:
		tag2, err := loader.LoadUInt(2)
		if err != nil {
			return nil, err
		}
		if tag2 != 0 {
			return nil, errInMsgEnvelopeNotFound
		}
		return loader.LoadRefCell()
	default:
		return nil, errInMsgEnvelopeNotFound
	}
}

func msgEnvelopeMetadata(envelope *cell.Cell) (*ton.TransactionMetadata, error) {
	loader, err := envelope.BeginParse()
	if err != nil {
		return nil, err
	}
	tag, err := loader.LoadUInt(4)
	if err != nil {
		return nil, err
	}
	if tag != 5 {
		return nil, nil
	}
	if err = loadRegularIntermediateAddress(loader); err != nil {
		return nil, err
	}
	if err = loadRegularIntermediateAddress(loader); err != nil {
		return nil, err
	}
	if _, err = loader.LoadBigCoins(); err != nil {
		return nil, err
	}
	if _, err = loader.LoadRefCell(); err != nil {
		return nil, err
	}

	hasEmittedLT, err := loader.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if hasEmittedLT {
		if _, err = loader.LoadUInt(64); err != nil {
			return nil, err
		}
	}

	hasMetadata, err := loader.LoadBoolBit()
	if err != nil || !hasMetadata {
		return nil, err
	}

	metadataTag, err := loader.LoadUInt(4)
	if err != nil {
		return nil, err
	}
	if metadataTag != 0 {
		return nil, fmt.Errorf("invalid message metadata tag %x", metadataTag)
	}
	depth, err := loader.LoadUInt(32)
	if err != nil {
		return nil, err
	}
	initiator, err := loader.LoadAddr()
	if err != nil {
		return nil, err
	}
	if initiator == nil {
		return nil, fmt.Errorf("message metadata has no initiator")
	}
	initiatorLT, err := loader.LoadUInt(64)
	if err != nil {
		return nil, err
	}

	return &ton.TransactionMetadata{
		Mode:  0,
		Depth: int32(depth),
		Initiator: ton.AccountID{
			Workchain: initiator.Workchain(),
			ID:        initiator.Data(),
		},
		InitiatorLT: initiatorLT,
	}, nil
}

func loadRegularIntermediateAddress(loader *cell.Slice) error {
	tag, err := loader.LoadUInt(1)
	if err != nil {
		return err
	}
	if tag != 0 {
		return fmt.Errorf("unsupported intermediate address tag")
	}
	useDestBits, err := loader.LoadUInt(7)
	if err != nil {
		return err
	}
	if useDestBits > 96 {
		return fmt.Errorf("invalid intermediate address")
	}
	return nil
}

func findBlockTransaction(root *cell.Cell, account []byte, lt uint64) (*cell.Cell, error) {
	shardAccounts, err := blockTransactionAccounts(root)
	if err != nil {
		return nil, err
	}
	return findTransactionInAccountBlocks(shardAccounts, account, lt)
}

func findTransactionInAccountBlocks(shardAccounts *tlb.ShardAccountBlocks, account []byte, lt uint64) (*cell.Cell, error) {
	if shardAccounts.Accounts == nil {
		return nil, storage.ErrNotFound
	}

	accountValue, err := shardAccounts.Accounts.LoadValueWithExtra(blockproof.AccountKey(account))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load account block: %w", err)
	}

	accountBlock, err := accountBlockFromValue(accountValue)
	if err != nil {
		return nil, err
	}
	return findTransactionInAccountBlock(accountBlock, lt)
}

func findTransactionInAccountBlock(accountBlock *tlb.AccountBlock, lt uint64) (*cell.Cell, error) {
	if accountBlock.Transactions == nil {
		return nil, storage.ErrNotFound
	}

	txValue, err := accountBlock.Transactions.LoadValueWithExtra(cell.BeginCell().MustStoreUInt(lt, 64).EndCell())
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load account transaction: %w", err)
	}
	return transactionCellFromValue(txValue)
}

func libraryEntriesFromDict(libraries *cell.Dictionary, hashes [][]byte, mode uint32) ([]*ton.LibraryEntry, error) {
	if libraries == nil {
		return nil, nil
	}

	hashes = uniqueHashes(hashes, 16)
	result := make([]*ton.LibraryEntry, 0, len(hashes))
	for _, hash := range hashes {
		value, err := libraries.LoadValue(blockproof.AccountKey(hash))
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			continue
		}
		if err != nil {
			return nil, err
		}

		tag, err := value.LoadUInt(2)
		if err != nil {
			return nil, err
		}
		if tag != 0 {
			return nil, fmt.Errorf("invalid shared library descriptor")
		}

		lib, err := value.LoadRefCell()
		if err != nil {
			return nil, err
		}
		if !bytes.Equal(lib.Hash(), hash) {
			continue
		}

		var data []byte
		if mode&2 == 0 {
			data = lib.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
		}
		result = append(result, &ton.LibraryEntry{Hash: hash, Data: data})
	}
	return result, nil
}

func validatorStatsCount(stateRoot *cell.Cell, mode uint32, limit int32, startAfter []byte) (int32, bool, error) {
	stats, err := blockCreateStatsDict(stateRoot)
	if err != nil {
		return 0, false, err
	}

	if limit > 1000 {
		limit = 1000
	}

	cursor := validatorStatsKey(startAfter)
	allowEqual := mode&3 != 1
	for count := int32(0); count < limit; count++ {
		key, value, err := stats.LookupNearestKey(cursor, true, allowEqual, false)
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			return count, true, nil
		}
		if err != nil {
			return 0, false, err
		}
		if err = validateCreatorStats(value); err != nil {
			return 0, false, err
		}

		cursor = key
		allowEqual = false
	}
	return limit, false, nil
}

func validatorStatsKey(startAfter []byte) *cell.Cell {
	if len(startAfter) == 32 {
		return cell.BeginCell().MustStoreSlice(startAfter, 256).EndCell()
	}
	return cell.BeginCell().MustStoreUInt(0, 256).EndCell()
}

func blockCreateStatsDict(stateRoot *cell.Cell) (*cell.Dictionary, error) {
	prefix, err := blockproof.LoadMcStateExtraPrefix(stateRoot, true)
	if err != nil {
		return nil, err
	}

	loader, err := prefix.Info.BeginParse()
	if err != nil {
		return nil, err
	}
	flags, err := loader.LoadUInt(16)
	if err != nil {
		return nil, err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return nil, err
	}
	if _, err = loader.LoadUInt(32); err != nil {
		return nil, err
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return nil, err
	}
	if _, err = skipOldMcBlocksInfoAugDict(loader); err != nil {
		return nil, err
	}
	if _, err = loader.LoadBoolBit(); err != nil {
		return nil, err
	}
	hasLastKey, err := loader.LoadBoolBit()
	if err != nil {
		return nil, err
	}
	if hasLastKey {
		if err = tlb.LoadFromCell(new(tlb.ExtBlkRef), loader); err != nil {
			return nil, err
		}
	}
	if flags&1 == 0 {
		return nil, fmt.Errorf("state is missing block create stats")
	}

	magic, err := loader.LoadUInt(8)
	if err != nil {
		return nil, err
	}
	// Only block_create_stats#17 exists in practice: the reference collator
	// hardcodes that tag, and a state carrying block_create_stats_ext#34 is
	// rejected at state load and at block validation, so the reference
	// liteserver refuses this query rather than answering it.
	if magic != 0x17 {
		return nil, fmt.Errorf("invalid block create stats magic %x", magic)
	}
	return loader.LoadDict(256)
}

func validateCreatorStats(value *cell.Slice) error {
	if tag, err := value.LoadUInt(4); err != nil {
		return err
	} else if tag != 4 {
		return fmt.Errorf("invalid CreatorStats tag %x", tag)
	}
	if err := validateDiscountedCounter(value); err != nil {
		return err
	}
	if err := validateDiscountedCounter(value); err != nil {
		return err
	}
	if value.BitsLeft() != 0 || value.RefsNum() != 0 {
		return fmt.Errorf("invalid CreatorStats record")
	}
	return nil
}

func validateDiscountedCounter(value *cell.Slice) error {
	lastUpdated, err := value.LoadUInt(32)
	if err != nil {
		return err
	}
	total, err := value.LoadUInt(64)
	if err != nil {
		return err
	}
	cnt2048, err := value.LoadUInt(64)
	if err != nil {
		return err
	}
	cnt65536, err := value.LoadUInt(64)
	if err != nil {
		return err
	}
	if total == 0 && (cnt2048 != 0 || cnt65536 != 0) {
		return fmt.Errorf("invalid discounted counter")
	}
	if total != 0 && lastUpdated == 0 {
		return fmt.Errorf("invalid discounted counter")
	}
	return nil
}

func outMsgQueueSize(stateRoot *cell.Cell) (uint64, error) {
	loader, err := stateRoot.BeginParse()
	if err != nil {
		return 0, err
	}

	state, err := blockproof.VisitShardStateHeader(loader)
	if err != nil {
		return 0, err
	}
	if state.OutMsgQueueInfo == nil {
		return 0, fmt.Errorf("state is missing out_msg_queue_info")
	}

	return loadOutMsgQueueSize(state.OutMsgQueueInfo)
}

func uniqueHashes(src [][]byte, limit int) [][]byte {
	hashes := make([][]byte, 0, len(src))
	for _, hash := range src {
		if len(hash) != 32 {
			continue
		}
		hashes = append(hashes, hash)
		if len(hashes) >= limit {
			break
		}
	}
	sort.Slice(hashes, func(i, j int) bool { return bytes.Compare(hashes[i], hashes[j]) < 0 })
	out := hashes[:0]
	for _, hash := range hashes {
		if len(out) == 0 || !bytes.Equal(out[len(out)-1], hash) {
			out = append(out, hash)
		}
	}
	return out
}
