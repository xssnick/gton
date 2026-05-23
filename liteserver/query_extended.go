package liteserver

import (
	"bytes"
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"math"
	"math/bits"
	"sort"

	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	maxGetTransactions       = 16
	maxListBlockTransactions = 256
	outMsgQueueSizeLimit     = 8000
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
			return errorResponse(err, "cannot load zero state "+storage.FormatBlockRef(*id))
		}

		return ton.BlockState{
			ID:       cloneBlockID(*id),
			RootHash: append([]byte(nil), id.RootHash...),
			FileHash: append([]byte(nil), id.FileHash...),
			Data:     append([]byte(nil), data...),
		}
	}

	root, err := s.loadStateRoot(ctx, *id)
	if err != nil {
		return errorResponse(err, "cannot load state "+storage.FormatBlockRef(*id))
	}

	data := root.ToBOCWithFlags(false)
	fileHash := sha256.Sum256(data)

	return ton.BlockState{
		ID:       cloneBlockID(*id),
		RootHash: root.Hash(0),
		FileHash: fileHash[:],
		Data:     data,
	}
}

func (s *Server) handleSendMessage(ctx context.Context, query ton.SendMessage) any {
	const errorPrefix = "cannot apply external message to current state "

	if s.messageSender == nil {
		return ton.LSError{Code: errCodeUnspecified, Text: errorPrefix + ": not configured"}
	}
	if err := s.checkExternalMessage(ctx, query.Body); err != nil {
		return ton.LSError{Code: errCodeUnspecified, Text: errorPrefix + ": " + err.Error()}
	}
	if err := s.messageSender.SendExternalMessage(ctx, query.Body); err != nil {
		return ton.LSError{Code: errCodeUnspecified, Text: errorPrefix + ": " + err.Error()}
	}
	return ton.SendMessageStatus{Status: 1}
}

func (s *Server) handleAllShardsInfo(ctx context.Context, id *ton.BlockIDExt) any {
	if !isFullBlockID(id) || id.Workchain != masterchainID {
		return ton.LSError{Code: errCodeProtoViolation, Text: "reference block must belong to the masterchain"}
	}

	root, err := s.loadBlockRoot(ctx, *id)
	if err != nil {
		return errorResponse(err, "cannot load block "+storage.FormatBlockRef(*id))
	}

	loader, err := root.BeginParse()
	if err != nil {
		return errorResponse(err, "cannot unpack block "+storage.FormatBlockRef(*id))
	}

	var block tlb.Block
	if err = tlb.LoadFromCell(&block, loader); err != nil {
		return errorResponse(err, "cannot unpack block "+storage.FormatBlockRef(*id))
	}
	if block.Extra == nil || block.Extra.Custom == nil || block.Extra.Custom.ShardHashes == nil {
		return ton.LSError{Code: errCodeInternal, Text: "cannot unpack header of block " + cxxBlockIDExtString(*id)}
	}

	proof, err := allShardsInfoProof(root)
	if err != nil {
		return errorResponse(err, "cannot create all shards proof")
	}

	data := cell.BeginCell().MustStoreDict(block.Extra.Custom.ShardHashes).EndCell()
	return ton.AllShardsInfo{
		ID:    cloneBlockID(*id),
		Proof: []*cell.Cell{proof},
		Data:  data,
	}
}

func (s *Server) handleShardInfo(ctx context.Context, query ton.GetShardInfo) any {
	if shardPrefixLen(uint64(query.Shard)) < 0 {
		return ton.LSError{Code: errCodeProtoViolation, Text: "requested shard is invalid"}
	}
	if !isFullBlockID(query.ID) || query.ID.Workchain != masterchainID {
		return ton.LSError{Code: errCodeProtoViolation, Text: "reference block must belong to the masterchain"}
	}

	stateRoot, err := s.loadStateRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load masterchain state "+storage.FormatBlockRef(*query.ID))
	}

	extra, err := mcStateExtra(stateRoot)
	if err != nil {
		return errorResponse(err, "cannot unpack masterchain state")
	}

	shardBlock, leaf, err := shardInfoFromHashes(extra.ShardHashes, query.Workchain, query.Shard, query.Exact)
	if errors.Is(err, storage.ErrNotFound) {
		shardBlock = ton.BlockIDExt{}
		leaf = nil
	} else if err != nil {
		return errorResponse(err, "cannot load shard info")
	}

	stateProof, err := shardHashesProof(stateRoot, query.Workchain)
	if err != nil {
		return errorResponse(err, "cannot create shard info proof")
	}
	proof, err := s.blockStateProof(ctx, *query.ID, stateProof)
	if err != nil {
		return errorResponse(err, "cannot create shard info proof")
	}

	return ton.ShardInfo{
		ID:               cloneBlockID(*query.ID),
		ShardBlock:       cloneBlockID(shardBlock),
		ShardProof:       proof,
		ShardDescription: leaf,
	}
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

	stateRoot, blockRoot, err := s.loadStateRootWithBlockRoot(ctx, *id)
	if err != nil {
		return errorResponse(err, "cannot load masterchain state "+storage.FormatBlockRef(*id))
	}

	stateProof, err := configProof(stateRoot, effectiveMode, all, params)
	if err != nil {
		return errorResponse(err, "cannot create config proof")
	}
	proof, err := blockStateProofFromRoot(blockRoot, stateProof)
	if err != nil {
		return errorResponse(err, "cannot create config proof")
	}

	return ton.ConfigAll{
		Mode:        int(effectiveMode),
		ID:          cloneBlockID(*id),
		StateProof:  proof[0].ToBOCWithFlags(false),
		ConfigProof: proof[1].ToBOCWithFlags(false),
	}
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
		ID:          cloneBlockID(keyBlock),
		StateProof:  nil,
		ConfigProof: proof.ToBOCWithFlags(false),
	}
}

func (s *Server) previousKeyBlockRoot(ctx context.Context, id ton.BlockIDExt) (ton.BlockIDExt, *cell.Cell, error) {
	root, err := s.loadBlockRoot(ctx, id)
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

	keyBlock, err := s.store.LookupBlockBySeqNo(ctx, storage.BlockHistoryKey{Workchain: masterchainID, Shard: masterchainShard}, block.BlockInfo.PrevKeyBlockSeqno)
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}

	keyRoot, err := s.loadBlockRoot(ctx, keyBlock)
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

	root, err := s.loadStateRoot(ctx, current.Masterchain.Block)
	if err != nil {
		return errorResponse(err, "cannot load current masterchain state")
	}

	entries, err := libraryEntries(root, query.LibraryList, 0)
	if err != nil {
		return errorResponse(err, "cannot load libraries")
	}
	return ton.LibraryResult{Result: entries}
}

func (s *Server) handleLibrariesWithProof(ctx context.Context, query ton.GetLibrariesWithProof) any {
	if !isFullBlockID(query.ID) || query.ID.Workchain != masterchainID {
		return ton.LSError{Code: errCodeProtoViolation, Text: "reference block must belong to the masterchain"}
	}

	root, err := s.loadStateRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load masterchain state "+storage.FormatBlockRef(*query.ID))
	}

	entries, err := libraryEntries(root, query.LibraryList, query.Mode)
	if err != nil {
		return errorResponse(err, "cannot load libraries")
	}

	dataProof, err := librariesProof(root, query.LibraryList, query.Mode)
	if err != nil {
		return errorResponse(err, "cannot create library proof")
	}
	proof, err := s.blockStateProof(ctx, *query.ID, dataProof)
	if err != nil {
		return errorResponse(err, "cannot create library proof")
	}

	return ton.LibraryResultWithProof{
		ID:         cloneBlockID(*query.ID),
		Mode:       query.Mode,
		Result:     entries,
		StateProof: proof[0].ToBOCWithFlags(false),
		DataProof:  proof[1].ToBOCWithFlags(false),
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

	addr := address.NewAddress(0, byte(query.AccID.Workchain), append([]byte(nil), query.AccID.ID...))
	if !tlb.ShardID(uint64(query.ID.Shard)).ContainsAddress(addr) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "requested account id is not contained in the shard of the specified block"}
	}

	root, err := s.loadBlockRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load block "+storage.FormatBlockRef(*query.ID))
	}

	proof, tx, err := blockTransactionProof(root, query.AccID.ID, uint64(query.LT))
	if err != nil {
		return errorResponse(err, "cannot create transaction proof")
	}

	var data []byte
	if tx != nil {
		data = tx.ToBOCWithFlags(false)
	}

	return ton.TransactionInfo{
		ID:          cloneBlockID(*query.ID),
		Proof:       proof.ToBOCWithFlags(false),
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

	account := bytes.Clone(query.AccID.ID)
	cursorLT := uint64(query.LT)
	cursorHash := bytes.Clone(query.TxHash)
	roots := make([]*cell.Cell, 0, limit)
	ids := make([]*ton.BlockIDExt, 0, limit)

	var currentBlock ton.BlockIDExt
	var currentRoot *cell.Cell
	hasCurrentBlock := false

	for cursorLT != 0 && len(roots) < limit {
		tx, block, root, err := accountTransactionFromCurrentBlock(hasCurrentBlock, currentBlock, currentRoot, query.AccID.Workchain, account, cursorLT)
		if errors.Is(err, storage.ErrNotFound) {
			block, err = s.lookupBlockByAccountLT(ctx, query.AccID.Workchain, account, cursorLT)
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

			root, err = s.loadBlockRoot(ctx, block)
			if err != nil {
				if len(roots) > 0 {
					return getTransactionsResponse(roots, ids)
				}
				return ton.LSError{Code: errCodeProtoViolation, Text: fmt.Sprintf("cannot load block %s with specified transaction: %s", cxxBlockIDExtString(block), err.Error())}
			}

			currentBlock = block
			currentRoot = root
			hasCurrentBlock = true

			tx, err = findBlockTransaction(root, account, cursorLT)
			if errors.Is(err, storage.ErrNotFound) {
				if len(roots) > 0 {
					return getTransactionsResponse(roots, ids)
				}
				return ton.LSError{Code: errCodeProtoViolation, Text: "cannot locate transaction in block with specified logical time"}
			}
			if err != nil {
				return errorResponse(err, "cannot get transaction")
			}
		} else if errors.Is(err, errBlockCannotContainAccount) {
			return ton.LSError{Code: errCodeProtoViolation, Text: err.Error()}
		} else if err != nil {
			return errorResponse(err, "cannot get transaction")
		}

		txHash := tx.Hash()
		if !bytes.Equal(txHash, cursorHash) {
			return ton.LSError{Code: errCodeProtoViolation, Text: "transaction hash mismatch"}
		}

		txLoader, err := tx.BeginParse()
		if err != nil {
			return errorResponse(err, "cannot unpack transaction")
		}

		var parsed tlb.Transaction
		if err = tlb.LoadFromCell(&parsed, txLoader); err != nil {
			return errorResponse(err, "cannot unpack transaction")
		}
		if parsed.LT != cursorLT {
			return ton.LSError{Code: errCodeProtoViolation, Text: "transaction lt mismatch"}
		}
		if !bytes.Equal(parsed.AccountAddr, account) {
			return ton.LSError{Code: errCodeProtoViolation, Text: "transaction account mismatch"}
		}
		if parsed.PrevTxLT >= cursorLT {
			return ton.LSError{Code: errCodeProtoViolation, Text: "previous transaction time is not less than the current one"}
		}

		roots = append(roots, tx)
		ids = append(ids, cloneBlockID(block))

		cursorLT = parsed.PrevTxLT
		if cursorLT == 0 {
			break
		}
		if len(parsed.PrevTxHash) != 32 {
			return ton.LSError{Code: errCodeProtoViolation, Text: "transaction previous hash is invalid"}
		}
		cursorHash = bytes.Clone(parsed.PrevTxHash)
	}

	return getTransactionsResponse(roots, ids)
}

func accountTransactionFromCurrentBlock(hasBlock bool, block ton.BlockIDExt, root *cell.Cell, workchain int32, account []byte, lt uint64) (*cell.Cell, ton.BlockIDExt, *cell.Cell, error) {
	if !hasBlock || root == nil {
		return nil, ton.BlockIDExt{}, nil, storage.ErrNotFound
	}
	if !blockContainsAccount(block, workchain, account) {
		return nil, ton.BlockIDExt{}, nil, errBlockCannotContainAccount
	}

	tx, err := findBlockTransaction(root, account, lt)
	if err != nil {
		return nil, ton.BlockIDExt{}, nil, err
	}
	return tx, block, root, nil
}

func (s *Server) lookupBlockByAccountLT(ctx context.Context, workchain int32, account []byte, lt uint64) (ton.BlockIDExt, error) {
	block, err := s.store.LookupBlockByAccountLT(ctx, workchain, account, lt)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if !blockContainsAccount(block, workchain, account) {
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

func cxxBlockIDExtString(block ton.BlockIDExt) string {
	return fmt.Sprintf("(%d,%016x,%d):%x:%x", block.Workchain, uint64(block.Shard), block.SeqNo, block.RootHash, block.FileHash)
}

func blockContainsAccount(block ton.BlockIDExt, workchain int32, account []byte) bool {
	if block.Workchain != workchain || len(account) != 32 {
		return false
	}

	addr := address.NewAddress(0, byte(workchain), bytes.Clone(account))
	return tlb.ShardID(uint64(block.Shard)).ContainsAddress(addr)
}

func blockMetaContainsLT(meta *storage.BlockMeta, lt uint64) bool {
	if meta == nil {
		return false
	}
	return meta.StartLT < lt && (meta.EndLT == 0 || lt < meta.EndLT)
}

func (s *Server) handleListBlockTransactions(ctx context.Context, query ton.ListBlockTransactions) any {
	if !isFullBlockID(query.ID) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid BlockIdExt"}
	}
	if query.Mode&128 != 0 && (query.After == nil || len(query.After.Account) != 32) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid start transaction id"}
	}

	root, err := s.loadBlockRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load block "+storage.FormatBlockRef(*query.ID))
	}

	proofRoot := root
	var proofBuilder *cell.MerkleProofBuilder
	if query.Mode&32 != 0 {
		proofBuilder = cell.NewMerkleProofBuilder(root)
		proofRoot = proofBuilder.Root()
	}

	txs, err := listBlockTransactions(proofRoot, query.Mode, query.Count, query.After)
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
		ID:             cloneBlockID(*query.ID),
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

	root, err := s.loadBlockRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load block "+storage.FormatBlockRef(*query.ID))
	}

	proofRoot := root
	var proofBuilder *cell.MerkleProofBuilder
	if query.Mode&32 != 0 || query.Mode&256 != 0 {
		proofBuilder = cell.NewMerkleProofBuilder(root)
		proofRoot = proofBuilder.Root()
	}

	list, err := listBlockTransactions(proofRoot, query.Mode, query.Count, query.After)
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
		proof = blockProof.ToBOCWithFlags(false)
	}

	return ton.BlockTransactionsExt{
		ID:           cloneBlockID(*query.ID),
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

	root, err := s.loadStateRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load masterchain state "+storage.FormatBlockRef(*query.ID))
	}

	dataProof, count, complete, err := validatorStatsProofAndCount(root, query.Mode, query.Limit, query.StartAfter)
	if err != nil {
		return errorResponse(err, "cannot load validator stats")
	}
	proof, err := s.blockStateProof(ctx, *query.ID, dataProof)
	if err != nil {
		return errorResponse(err, "cannot create validator stats proof")
	}

	return ton.ValidatorStats{
		Mode:       query.Mode & 0xff,
		ID:         cloneBlockID(*query.ID),
		Count:      count,
		Complete:   complete,
		StateProof: proof[0].ToBOCWithFlags(false),
		DataProof:  proof[1].ToBOCWithFlags(false),
	}
}

func (s *Server) handleBlockOutMsgQueueSize(ctx context.Context, query ton.GetBlockOutMsgQueueSize) any {
	if !isFullBlockID(query.ID) {
		return ton.LSError{Code: errCodeProtoViolation, Text: "invalid BlockIdExt"}
	}

	root, err := s.loadStateRoot(ctx, *query.ID)
	if err != nil {
		return errorResponse(err, "cannot load state "+storage.FormatBlockRef(*query.ID))
	}

	size, err := outMsgQueueSize(root)
	if err != nil {
		return errorResponse(err, "cannot load out msg queue size")
	}

	resp := ton.BlockOutMsgQueueSize{
		Mode: query.Mode,
		ID:   cloneBlockID(*query.ID),
		Size: int64(size),
	}
	if query.Mode&1 != 0 {
		dataProof, err := outMsgQueueSizeProof(root)
		if err != nil {
			return errorResponse(err, "cannot create out msg queue proof")
		}
		proof, err := s.blockStateProof(ctx, *query.ID, dataProof)
		if err != nil {
			return errorResponse(err, "cannot create out msg queue proof")
		}
		resp.Proof = cell.ToBOCWithFlags(proof, false)
	}
	return resp
}

func (s *Server) handleOutMsgQueueSizes(ctx context.Context, query ton.GetOutMsgQueueSizes) any {
	var filter *storage.ShardKey
	if query.Mode&1 != 0 {
		if shardPrefixLen(uint64(query.Shard)) < 0 {
			return ton.LSError{Code: errCodeProtoViolation, Text: "requested shard is invalid"}
		}
		filter = &storage.ShardKey{Workchain: query.WC, Shard: query.Shard}
	}

	current, err := s.store.CurrentState(ctx)
	if err != nil {
		return errorResponse(err, "cannot obtain current masterchain state")
	}

	blocks := make([]ton.BlockIDExt, 0, 1+len(current.Shards))
	if filter == nil || shardIntersects(*filter, storage.ShardKeyFromBlock(current.Masterchain.Block)) {
		blocks = append(blocks, current.Masterchain.Block)
	}
	for _, key := range storage.SortedShardKeys(current.Shards) {
		state := current.Shards[key]
		if filter != nil && !shardIntersects(*filter, storage.ShardKeyFromBlock(state.Block)) {
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
			ID:   cloneBlockID(block),
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

func listBlockTransactions(root *cell.Cell, mode uint32, reqCount uint32, after *ton.TransactionID3) (blockTransactionList, error) {
	data, err := blockTransactionData(root, mode&256 != 0)
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

	startAccount := make([]byte, 32)
	startLT := uint64(0)
	if reverse {
		for i := range startAccount {
			startAccount[i] = 0xff
		}
		startLT = math.MaxUint64
	}
	if mode&128 != 0 && after != nil {
		startAccount = append([]byte(nil), after.Account...)
		startLT = after.LT
	}

	out := make([]blockTxItem, 0, limit)
	currentAccount := append([]byte(nil), startAccount...)
	allowSameAccount := true
	fetchNext := !reverse
	eof := false

	for len(out) < limit {
		accountKeyCell, accountValue, err := data.shardAccounts.Accounts.LookupNearestKey(accountKey(currentAccount), fetchNext, allowSameAccount, false)
		if errors.Is(err, cell.ErrNoSuchKeyInDict) {
			eof = true
			break
		}
		if err != nil {
			return blockTransactionList{}, fmt.Errorf("lookup account block: %w", err)
		}

		accountKeyLoader, err := accountKeyCell.BeginParse()
		if err != nil {
			return blockTransactionList{}, fmt.Errorf("begin parse account key: %w", err)
		}
		account, err := accountKeyLoader.LoadSlice(256)
		if err != nil {
			return blockTransactionList{}, fmt.Errorf("load account key: %w", err)
		}
		accountBlock, err := accountBlockFromValue(accountValue)
		if err != nil {
			return blockTransactionList{}, err
		}

		txStart := startLT
		if !bytes.Equal(account, startAccount) {
			txStart = 0
			if reverse {
				txStart = math.MaxUint64
			}
		}

		if accountBlock.Transactions != nil && accountBlock.Transactions.AugmentedDictionary != nil {
			currentLT := txStart
			for len(out) < limit {
				txKeyCell, txValue, err := accountBlock.Transactions.LookupNearestKey(
					cell.BeginCell().MustStoreUInt(currentLT, 64).EndCell(),
					fetchNext,
					false,
					false,
				)
				if errors.Is(err, cell.ErrNoSuchKeyInDict) {
					break
				}
				if err != nil {
					return blockTransactionList{}, fmt.Errorf("lookup account transaction: %w", err)
				}

				txKeyLoader, err := txKeyCell.BeginParse()
				if err != nil {
					return blockTransactionList{}, fmt.Errorf("begin parse transaction lt: %w", err)
				}
				lt, err := txKeyLoader.LoadUInt(64)
				if err != nil {
					return blockTransactionList{}, fmt.Errorf("load transaction lt: %w", err)
				}
				txCell, err := transactionCellFromValue(txValue)
				if err != nil {
					return blockTransactionList{}, err
				}

				item := blockTxItem{
					account: append([]byte(nil), account...),
					lt:      lt,
					hash:    txCell.Hash(),
					cell:    txCell,
				}
				if mode&256 != 0 && data.inMsgDescr != nil {
					metadata, err := transactionMetadata(data.inMsgDescr, txCell)
					if err != nil {
						return blockTransactionList{}, err
					}
					item.metadata = metadata
				}
				out = append(out, item)
				currentLT = lt
			}
		}

		currentAccount = account
		allowSameAccount = false
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

func accountBlockFromValue(value *cell.Slice) (*tlb.AccountBlock, error) {
	if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), value); err != nil {
		return nil, fmt.Errorf("load account block augmentation: %w", err)
	}

	var accountBlock tlb.AccountBlock
	if err := tlb.LoadFromCell(&accountBlock, value); err != nil {
		return nil, fmt.Errorf("load account block: %w", err)
	}
	return &accountBlock, nil
}

func transactionCellFromValue(value *cell.Slice) (*cell.Cell, error) {
	if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), value); err != nil {
		return nil, fmt.Errorf("load transaction augmentation: %w", err)
	}
	txCell, err := value.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load transaction cell: %w", err)
	}
	return txCell, nil
}

func transactionMetadata(inMsgDescr *cell.AugmentedDictionary, txCell *cell.Cell) (*ton.TransactionMetadata, error) {
	loader, err := txCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse transaction: %w", err)
	}

	var tx tlb.Transaction
	if err := tlb.LoadFromCell(&tx, loader); err != nil {
		return nil, fmt.Errorf("load transaction: %w", err)
	}
	if tx.IO.In == nil {
		return nil, nil
	}

	msg, err := tx.IO.In.ToCell()
	if err != nil {
		return nil, fmt.Errorf("load transaction inbound message: %w", err)
	}

	value, err := inMsgDescr.LoadValueWithExtra(accountKey(msg.Hash()))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, nil
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
	return msgEnvelopeMetadata(envelope)
}

func skipImportFees(loader *cell.Slice) error {
	if _, err := loader.LoadBigCoins(); err != nil {
		return err
	}
	return tlb.LoadFromCell(new(tlb.CurrencyCollection), loader)
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
		return nil, fmt.Errorf("message metadata initiator is nil")
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
			ID:        append([]byte(nil), initiator.Data()...),
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
	if shardAccounts.Accounts == nil {
		return nil, storage.ErrNotFound
	}

	accountValue, err := shardAccounts.Accounts.LoadValueWithExtra(accountKey(account))
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

func mcStateExtra(root *cell.Cell) (*tlb.McStateExtra, error) {
	loader, err := root.BeginParse()
	if err != nil {
		return nil, err
	}

	var state tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&state, loader); err != nil {
		return nil, err
	}
	if state.McStateExtra == nil {
		return nil, fmt.Errorf("state is missing mc_state_extra")
	}

	extraLoader, err := state.McStateExtra.BeginParse()
	if err != nil {
		return nil, err
	}

	var extra tlb.McStateExtra
	if err := tlb.LoadFromCell(&extra, extraLoader); err != nil {
		return nil, err
	}
	return &extra, nil
}

func shardInfoFromHashes(hashes *cell.Dictionary, wc int32, shard int64, exact bool) (ton.BlockIDExt, *cell.Cell, error) {
	if hashes == nil {
		return ton.BlockIDExt{}, nil, storage.ErrNotFound
	}

	wcKey := cell.BeginCell().MustStoreInt(int64(wc), 32).EndCell()
	wcValue, err := hashes.LoadValue(wcKey)
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return ton.BlockIDExt{}, nil, storage.ErrNotFound
	}
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}

	root, err := wcValue.LoadRefCell()
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}

	trueShard, leaf, err := findShardLeaf(root, uint64(shard), exact)
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}

	block, err := blockIDFromShardLeaf(wc, trueShard, leaf)
	if err != nil {
		return ton.BlockIDExt{}, nil, err
	}
	return block, leaf, nil
}

func findShardLeaf(root *cell.Cell, shard uint64, exact bool) (int64, *cell.Cell, error) {
	prefixLen := shardPrefixLen(shard)
	if prefixLen < 0 {
		return 0, nil, storage.ErrNotFound
	}

	node := root
	z := shard
	m := uint64(math.MaxUint64)
	remaining := prefixLen
	for {
		loader, err := node.BeginParse()
		if err != nil {
			return 0, nil, err
		}
		typ, err := loader.LoadUInt(1)
		if err != nil {
			return 0, nil, err
		}
		if typ == 0 {
			if remaining != 0 && exact {
				return 0, nil, storage.ErrNotFound
			}
			trueShard := (shard | m) - (m >> 1)
			return int64(trueShard), node, nil
		}

		if remaining == 0 || loader.BitsLeft() != 0 || loader.RefsNum() != 2 {
			return 0, nil, storage.ErrNotFound
		}

		node = node.MustPeekRef(int(z >> 63))
		z <<= 1
		remaining--
		m >>= 1
	}
}

func blockIDFromShardLeaf(wc int32, shard int64, leaf *cell.Cell) (ton.BlockIDExt, error) {
	loader, err := leaf.BeginParse()
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	typ, err := loader.LoadUInt(1)
	if err != nil {
		return ton.BlockIDExt{}, err
	}
	if typ != 0 {
		return ton.BlockIDExt{}, fmt.Errorf("shard leaf has invalid tag")
	}

	magic, err := loader.LoadUInt(4)
	if err != nil {
		return ton.BlockIDExt{}, err
	}

	switch magic {
	case 0xa:
		var desc tlb.ShardDesc
		if err = tlb.LoadFromCell(&desc, loader, true); err != nil {
			return ton.BlockIDExt{}, err
		}
		return ton.BlockIDExt{Workchain: wc, Shard: shard, SeqNo: desc.SeqNo, RootHash: desc.RootHash, FileHash: desc.FileHash}, nil
	case 0xb:
		var desc tlb.ShardDescB
		if err = tlb.LoadFromCell(&desc, loader, true); err != nil {
			return ton.BlockIDExt{}, err
		}
		return ton.BlockIDExt{Workchain: wc, Shard: shard, SeqNo: desc.SeqNo, RootHash: desc.RootHash, FileHash: desc.FileHash}, nil
	default:
		return ton.BlockIDExt{}, fmt.Errorf("wrong ShardDesc magic: %x", magic)
	}
}

func shardPrefixLen(shard uint64) int {
	if shard == 0 {
		return -1
	}
	low := shard & -shard
	if low == 0 {
		return -1
	}
	return 63 - bits.TrailingZeros64(low)
}

func shardIntersects(a, b storage.ShardKey) bool {
	if a.Workchain != b.Workchain {
		return false
	}
	aLen := shardPrefixLen(uint64(a.Shard))
	bLen := shardPrefixLen(uint64(b.Shard))
	if aLen < 0 || bLen < 0 {
		return false
	}
	minLen := aLen
	if bLen < minLen {
		minLen = bLen
	}
	return shardPrefix(uint64(a.Shard), minLen) == shardPrefix(uint64(b.Shard), minLen)
}

func shardPrefix(shard uint64, length int) uint64 {
	if length <= 0 {
		return 0
	}
	return shard >> (64 - length)
}

func libraryEntries(stateRoot *cell.Cell, hashes [][]byte, mode uint32) ([]*ton.LibraryEntry, error) {
	libraries, err := librariesDict(stateRoot)
	if err != nil {
		return nil, err
	}
	if libraries == nil {
		return nil, nil
	}

	hashes = uniqueHashes(hashes, 16)
	result := make([]*ton.LibraryEntry, 0, len(hashes))
	for _, hash := range hashes {
		value, err := libraries.LoadValue(accountKey(hash))
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
			data = lib.ToBOCWithFlags(false)
		}
		result = append(result, &ton.LibraryEntry{Hash: append([]byte(nil), hash...), Data: data})
	}
	return result, nil
}

func librariesDict(stateRoot *cell.Cell) (*cell.Dictionary, error) {
	stateLoader, err := stateRoot.BeginParse()
	if err != nil {
		return nil, err
	}

	var state tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&state, stateLoader); err != nil {
		return nil, err
	}
	if state.Stats == nil {
		return nil, fmt.Errorf("state is missing shard state extras")
	}

	loader, err := state.Stats.BeginParse()
	if err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if _, err := loader.LoadUInt(64); err != nil {
		return nil, err
	}
	if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}
	if err := tlb.LoadFromCell(new(tlb.CurrencyCollection), loader); err != nil {
		return nil, err
	}
	return loader.LoadDict(256)
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

type creatorStatsDict interface {
	LookupNearestKey(key *cell.Cell, fetchNext bool, allowEq bool, invertFirst bool) (*cell.Cell, *cell.Slice, error)
}

func blockCreateStatsDict(stateRoot *cell.Cell) (creatorStatsDict, error) {
	prefix, err := loadMcStateExtraPrefix(stateRoot)
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
	switch magic {
	case 0x17:
		dict, err := loader.LoadDict(256)
		if err != nil {
			return nil, err
		}
		return dict, nil
	case 0x34:
		skipExtra := func(loader *cell.Slice) error {
			_, err := loader.LoadUInt(32)
			return err
		}

		dict, err := loader.LoadAugDict(256, cell.ReadOnlyAugmentation{SkipExtraFn: skipExtra}, false)
		if err != nil {
			return nil, err
		}
		return dict, nil
	default:
		return nil, fmt.Errorf("invalid block create stats magic %x", magic)
	}
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

	var state tlb.ShardStateUnsplit
	if err := tlb.LoadFromCell(&state, loader); err != nil {
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
		hashes = append(hashes, append([]byte(nil), hash...))
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
