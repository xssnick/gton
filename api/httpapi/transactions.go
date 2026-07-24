package httpapi

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/big"
	"net/http"

	"github.com/xssnick/gton/service/blockproof"
	"github.com/xssnick/gton/service/storage"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	blockTransactionsType    = "blocks.transactions"
	blockTransactionsExtType = "blocks.transactionsExt"
	shortTxIDType            = "blocks.shortTxId"
	extTransactionType       = "ext.transaction"
	rawTransactionExtType    = "raw.transactionExt"
	rawTransactionType       = "raw.transaction"
	rawTransactionsType      = "raw.transactions"
	rawMessageType           = "raw.message"
	accountAddressType       = "accountAddress"
	extMessageType           = "ext.message"
	msgDataRawType           = "msg.dataRaw"
	defaultTxCount           = 40
	maxTxCount               = 256
	shortTxIDMode            = 135
	defaultHistoryLimit      = 10
	maxHistoryLimit          = 100
	messageLookupBlockLimit  = 40
	messageLookupMinHistory  = 10
	messageLookupLTStep      = 1_000_000
)

type blockTransactions struct {
	Type         string      `json:"@type"`
	ID           blockIDExt  `json:"id"`
	ReqCount     uint32      `json:"req_count"`
	Incomplete   bool        `json:"incomplete"`
	Transactions []shortTxID `json:"transactions"`
}

type blockTransactionsExt struct {
	Type         string           `json:"@type"`
	ID           blockIDExt       `json:"id"`
	ReqCount     uint32           `json:"req_count"`
	Incomplete   bool             `json:"incomplete"`
	Transactions []extTransaction `json:"transactions"`
}

type shortTxID struct {
	Type    string `json:"@type"`
	Mode    int    `json:"mode"`
	Account string `json:"account"`
	LT      string `json:"lt"`
	Hash    string `json:"hash"`
}

type extTransaction struct {
	Type          string                `json:"@type"`
	Address       accountAddress        `json:"address"`
	Account       string                `json:"account"`
	UTime         uint32                `json:"utime"`
	Data          string                `json:"data"`
	TransactionID internalTransactionID `json:"transaction_id"`
	Fee           string                `json:"fee"`
	StorageFee    string                `json:"storage_fee"`
	OtherFee      string                `json:"other_fee"`
	InMsg         *extMessage           `json:"in_msg,omitempty"`
	OutMsgs       []extMessage          `json:"out_msgs"`
}

type accountAddress struct {
	Type           string `json:"@type"`
	AccountAddress string `json:"account_address"`
}

type extMessage struct {
	Type            string          `json:"@type"`
	Hash            string          `json:"hash"`
	Source          string          `json:"source"`
	Destination     string          `json:"destination"`
	Value           string          `json:"value"`
	ExtraCurrencies []extraCurrency `json:"extra_currencies"`
	FwdFee          string          `json:"fwd_fee"`
	IHRFee          string          `json:"ihr_fee"`
	CreatedLT       string          `json:"created_lt"`
	BodyHash        string          `json:"body_hash"`
	MsgData         msgDataRaw      `json:"msg_data"`
	Message         string          `json:"message"`
}

type msgDataRaw struct {
	Type      string `json:"@type"`
	Body      string `json:"body"`
	InitState string `json:"init_state"`
}

type rawTransactions struct {
	Type                  string                `json:"@type"`
	Transactions          []rawTransaction      `json:"transactions"`
	PreviousTransactionID internalTransactionID `json:"previous_transaction_id"`
}

type rawTransaction struct {
	Type          string                `json:"@type"`
	Address       accountAddress        `json:"address"`
	UTime         uint32                `json:"utime"`
	Data          string                `json:"data"`
	TransactionID internalTransactionID `json:"transaction_id"`
	Fee           string                `json:"fee"`
	StorageFee    string                `json:"storage_fee"`
	OtherFee      string                `json:"other_fee"`
	InMsg         *rawMessage           `json:"in_msg,omitempty"`
	OutMsgs       []rawMessage          `json:"out_msgs"`
}

type rawMessage struct {
	Type            string          `json:"@type"`
	Hash            string          `json:"hash"`
	Source          accountAddress  `json:"source"`
	Destination     accountAddress  `json:"destination"`
	Value           string          `json:"value"`
	ExtraCurrencies []extraCurrency `json:"extra_currencies"`
	FwdFee          string          `json:"fwd_fee"`
	IHRFee          string          `json:"ihr_fee"`
	CreatedLT       string          `json:"created_lt"`
	BodyHash        string          `json:"body_hash"`
	MsgData         msgDataRaw      `json:"msg_data"`
}

func (s *Server) handleBlockTransactions(ctx context.Context, params requestParams) (any, *apiError) {
	id, count, page, apiErr := s.blockTransactionPage(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}

	items := make([]shortTxID, 0, len(page.entries))
	for _, entry := range page.entries {
		items = append(items, shortTxID{
			Type:    shortTxIDType,
			Mode:    shortTxIDMode,
			Account: fmt.Sprintf("%d:%x", id.Workchain, entry.account),
			LT:      fmt.Sprintf("%d", entry.lt),
			Hash:    tonHash(entry.cell.Hash()),
		})
	}

	return blockTransactions{
		Type:         blockTransactionsType,
		ID:           blockIDExtFromTON(id),
		ReqCount:     count,
		Incomplete:   page.incomplete,
		Transactions: items,
	}, nil
}

func (s *Server) handleBlockTransactionsExt(ctx context.Context, params requestParams) (any, *apiError) {
	id, count, page, apiErr := s.blockTransactionPage(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}

	items := make([]extTransaction, 0, len(page.entries))
	for _, entry := range page.entries {
		tx, apiErr := parseAccountTransaction(entry.cell)
		if apiErr != nil {
			return nil, apiErr
		}
		formatted, err := extTransactionFromTLB(rawTransactionExtType, id.Workchain, tx, entry.cell)
		if err != nil {
			return nil, internalError("cannot format transaction: " + err.Error())
		}
		items = append(items, formatted)
	}

	return blockTransactionsExt{
		Type:         blockTransactionsExtType,
		ID:           blockIDExtFromTON(id),
		ReqCount:     count,
		Incomplete:   page.incomplete,
		Transactions: items,
	}, nil
}

type blockTxEntry struct {
	account []byte
	lt      uint64
	cell    *cell.Cell
}

type blockTxPage struct {
	entries    []blockTxEntry
	incomplete bool
}

// blockTransactionPage selects the requested transaction page of a block via
// aug-dict cursors. Only dict keys and cell hashes are touched: no transaction
// is TLB-decoded, and only the returned page is materialized.
func (s *Server) blockTransactionPage(ctx context.Context, params requestParams) (ton.BlockIDExt, uint32, blockTxPage, *apiError) {
	id, apiErr := s.blockIDFromParams(ctx, params)
	if apiErr != nil {
		return ton.BlockIDExt{}, 0, blockTxPage{}, apiErr
	}

	count, apiErr := blockTransactionCount(params)
	if apiErr != nil {
		return ton.BlockIDExt{}, 0, blockTxPage{}, apiErr
	}

	accounts, apiErr := s.loadBlockAccountBlocks(ctx, id)
	if apiErr != nil {
		return ton.BlockIDExt{}, 0, blockTxPage{}, apiErr
	}

	afterLT, err := params.optionalUint64("after_lt")
	hasAfterLT := err == nil
	if err != nil && !errors.Is(err, errRequestParamNotFound) {
		return ton.BlockIDExt{}, 0, blockTxPage{}, asAPIError(err)
	}
	afterHash, err := optionalHashParam(params, "after_hash")
	hasAfterHash := err == nil
	if err != nil && !errors.Is(err, errRequestParamNotFound) {
		return ton.BlockIDExt{}, 0, blockTxPage{}, asAPIError(err)
	}

	// The after cursor carries no account, so the resume point cannot be sought
	// directly: walk the dict keys comparing lt and the tx cell hash, then keep
	// collecting from the entry right after the match.
	found := !hasAfterLT && !hasAfterHash
	page := blockTxPage{entries: make([]blockTxEntry, 0, count)}
	err = walkBlockTransactions(accounts, func(entry blockTxEntry) (bool, error) {
		if !found {
			if hasAfterLT && entry.lt != afterLT {
				return true, nil
			}
			if hasAfterHash && !bytes.Equal(entry.cell.Hash(), afterHash) {
				return true, nil
			}
			found = true
			return true, nil
		}
		if uint32(len(page.entries)) >= count {
			page.incomplete = true
			return false, nil
		}
		page.entries = append(page.entries, entry)
		return true, nil
	})
	if err != nil {
		return ton.BlockIDExt{}, 0, blockTxPage{}, internalError("cannot list block transactions: " + err.Error())
	}
	if !found {
		return ton.BlockIDExt{}, 0, blockTxPage{}, validationError("failed to parse request: after transaction was not found")
	}
	return id, count, page, nil
}

// skipSingleRef bounds an aug dict leaf value of the `^X` shape without
// materializing the ref.
func skipSingleRef(loader *cell.Slice) error {
	return loader.SkipBitsAndRefs(0, 1)
}

// walkBlockTransactions iterates block transactions in (account, lt) ascending
// order — the order block.ListTransactions() produces — with lazy aug-dict
// iterators, without TLB-decoding any transaction. fn returns false to stop;
// nothing past the stop point is materialized.
func walkBlockTransactions(accounts *tlb.ShardAccountBlocks, fn func(entry blockTxEntry) (bool, error)) error {
	if accounts.Accounts == nil || accounts.Accounts.AugmentedDictionary == nil {
		return nil
	}

	accIt, err := accounts.Accounts.IteratorExtra(false, false)
	if err != nil {
		return fmt.Errorf("range account blocks: %w", err)
	}
	for accIt.Next() {
		accountKeyLoader, err := accIt.Key().BeginParse()
		if err != nil {
			return fmt.Errorf("begin parse account key: %w", err)
		}
		account, err := accountKeyLoader.LoadSlice(256)
		if err != nil {
			return fmt.Errorf("load account key: %w", err)
		}

		// acc_trans#5 account_addr:bits256 transactions:(HashmapAug 64 ^Transaction CurrencyCollection)
		accountValue := accIt.Value()
		magic, err := accountValue.LoadUInt(4)
		if err != nil {
			return fmt.Errorf("load account block magic: %w", err)
		}
		if magic != 0x5 {
			return fmt.Errorf("invalid account block magic %x", magic)
		}
		if err = accountValue.SkipBits(256); err != nil {
			return fmt.Errorf("load account block address: %w", err)
		}

		txIt, err := accountValue.AugDictInlineIterator(64, tlb.AugAccountTransactions{}, skipSingleRef, false, false)
		if err != nil {
			return fmt.Errorf("range account transactions: %w", err)
		}
		for txIt.Next() {
			txKeyLoader, err := txIt.Key().BeginParse()
			if err != nil {
				return fmt.Errorf("begin parse transaction lt: %w", err)
			}
			lt, err := txKeyLoader.LoadUInt(64)
			if err != nil {
				return fmt.Errorf("load transaction lt: %w", err)
			}
			txCell, err := txIt.Value().LoadRefCell()
			if err != nil {
				return fmt.Errorf("load transaction cell: %w", err)
			}

			cont, err := fn(blockTxEntry{account: account, lt: lt, cell: txCell})
			if err != nil || !cont {
				return err
			}
		}
		if err = txIt.Err(); err != nil {
			return fmt.Errorf("range account transactions: %w", err)
		}
	}
	if err = accIt.Err(); err != nil {
		return fmt.Errorf("range account blocks: %w", err)
	}
	return nil
}

func (s *Server) handleTransactions(ctx context.Context, params requestParams) (any, *apiError) {
	addr, _, apiErr := parseStdAddressParam(params, "address")
	if apiErr != nil {
		return nil, apiErr
	}

	limit, apiErr := historyLimit(params)
	if apiErr != nil {
		return nil, apiErr
	}
	if limit == 0 {
		return []extTransaction{}, nil
	}

	cursorLT, err := params.optionalUint64("lt")
	hasLT := err == nil
	if err != nil && !errors.Is(err, errRequestParamNotFound) {
		return nil, asAPIError(err)
	}
	cursorHash, err := optionalHashParam(params, "hash")
	hasHash := err == nil
	if err != nil && !errors.Is(err, errRequestParamNotFound) {
		return nil, asAPIError(err)
	}
	toLT, err := params.optionalUint64("to_lt")
	hasToLT := err == nil
	if err != nil && !errors.Is(err, errRequestParamNotFound) {
		return nil, asAPIError(err)
	}
	if !hasLT {
		snapshot, apiErr := s.loadAccountSnapshot(ctx, params)
		if apiErr != nil {
			return nil, apiErr
		}
		if snapshot.shard == nil || snapshot.shard.LastTransLT == 0 {
			return []extTransaction{}, nil
		}
		cursorLT = snapshot.shard.LastTransLT
		cursorHash = snapshot.shard.LastTransHash
		hasHash = true
	}

	account := addr.Data()
	out := make([]extTransaction, 0, limit)
	var current *accountTxBlock
	for cursorLT != 0 && len(out) < limit {
		if hasToLT && cursorLT <= toLT {
			break
		}

		tx, txCell, block, apiErr := s.loadAccountTransaction(ctx, addr.Workchain(), account, cursorLT, current)
		if apiErr != nil {
			return nil, apiErr
		}
		current = block
		if hasHash && !bytes.Equal(tx.Hash, cursorHash) {
			return nil, validationError("failed to parse request: transaction hash mismatch")
		}

		formatted, err := extTransactionFromTLB(extTransactionType, addr.Workchain(), tx, txCell)
		if err != nil {
			return nil, internalError("cannot format transaction: " + err.Error())
		}
		out = append(out, formatted)

		cursorLT = tx.PrevTxLT
		cursorHash = tx.PrevTxHash
		hasHash = true
	}
	return out, nil
}

func (s *Server) handleTransactionsStd(ctx context.Context, params requestParams) (any, *apiError) {
	addr, _, apiErr := parseStdAddressParam(params, "address")
	if apiErr != nil {
		return nil, apiErr
	}

	limit, apiErr := historyLimit(params)
	if apiErr != nil {
		return nil, apiErr
	}

	cursorLT, err := params.optionalUint64("lt")
	hasLT := err == nil
	if err != nil && !errors.Is(err, errRequestParamNotFound) {
		return nil, asAPIError(err)
	}
	cursorHash, err := optionalHashParam(params, "hash")
	hasHash := err == nil
	if err != nil && !errors.Is(err, errRequestParamNotFound) {
		return nil, asAPIError(err)
	}
	toLT, err := params.optionalUint64("to_lt")
	hasToLT := err == nil
	if err != nil && !errors.Is(err, errRequestParamNotFound) {
		return nil, asAPIError(err)
	}
	if !hasLT {
		snapshot, apiErr := s.loadAccountSnapshot(ctx, params)
		if apiErr != nil {
			return nil, apiErr
		}
		if snapshot.shard != nil {
			cursorLT = snapshot.shard.LastTransLT
			cursorHash = snapshot.shard.LastTransHash
			hasHash = true
		}
	}

	previous := internalTransactionID{
		Type: internalTransactionIDType,
		LT:   fmt.Sprintf("%d", cursorLT),
		Hash: tonHash(cursorHash),
	}
	if limit == 0 || cursorLT == 0 {
		return rawTransactions{Type: rawTransactionsType, Transactions: []rawTransaction{}, PreviousTransactionID: previous}, nil
	}

	account := addr.Data()
	out := make([]rawTransaction, 0, limit)
	var current *accountTxBlock
	for cursorLT != 0 && len(out) < limit {
		if hasToLT && cursorLT <= toLT {
			break
		}

		tx, txCell, block, apiErr := s.loadAccountTransaction(ctx, addr.Workchain(), account, cursorLT, current)
		if apiErr != nil {
			return nil, apiErr
		}
		current = block
		if hasHash && !bytes.Equal(tx.Hash, cursorHash) {
			return nil, validationError("failed to parse request: transaction hash mismatch")
		}

		formatted, err := rawTransactionFromTLB(addr.Workchain(), tx, txCell)
		if err != nil {
			return nil, internalError("cannot format transaction: " + err.Error())
		}
		out = append(out, formatted)

		cursorLT = tx.PrevTxLT
		cursorHash = tx.PrevTxHash
		previous = internalTransactionID{Type: internalTransactionIDType, LT: fmt.Sprintf("%d", cursorLT), Hash: tonHash(cursorHash)}
		hasHash = true
	}

	return rawTransactions{Type: rawTransactionsType, Transactions: out, PreviousTransactionID: previous}, nil
}

func (s *Server) handleTryLocateSourceTx(ctx context.Context, params requestParams) (any, *apiError) {
	query, apiErr := messageTransactionQueryFromParams(params)
	if apiErr != nil {
		return nil, apiErr
	}

	located, apiErr := s.locateOutboundMessageTransaction(ctx, query)
	if apiErr != nil {
		return nil, apiErr
	}
	return formatLocatedMessageTransaction(located)
}

func (s *Server) handleTryLocateInboundMessageTransaction(ctx context.Context, params requestParams) (any, *apiError) {
	query, apiErr := messageTransactionQueryFromParams(params)
	if apiErr != nil {
		return nil, apiErr
	}

	located, apiErr := s.locateInboundMessageTransaction(ctx, query)
	if apiErr != nil {
		return nil, apiErr
	}
	return formatLocatedMessageTransaction(located)
}

type messageTransactionQuery struct {
	source      *address.Address
	destination *address.Address
	createdLT   uint64
}

type locatedMessageTransaction struct {
	block ton.BlockIDExt
	cell  *cell.Cell
}

func messageTransactionQueryFromParams(params requestParams) (messageTransactionQuery, *apiError) {
	source, _, apiErr := parseStdAddressParam(params, "source")
	if apiErr != nil {
		return messageTransactionQuery{}, apiErr
	}
	destination, _, apiErr := parseStdAddressParam(params, "destination")
	if apiErr != nil {
		return messageTransactionQuery{}, apiErr
	}
	createdLT, err := params.optionalUint64("created_lt")
	if errors.Is(err, errRequestParamNotFound) {
		return messageTransactionQuery{}, validationError("failed to parse request: missing required field \"created_lt\"")
	}
	if err != nil {
		return messageTransactionQuery{}, asAPIError(err)
	}

	return messageTransactionQuery{source: source, destination: destination, createdLT: createdLT}, nil
}

func formatLocatedMessageTransaction(located locatedMessageTransaction) (any, *apiError) {
	tx, apiErr := parseAccountTransaction(located.cell)
	if apiErr != nil {
		return nil, apiErr
	}
	formatted, err := extTransactionFromTLB(extTransactionType, located.block.Workchain, tx, located.cell)
	if err != nil {
		return nil, internalError("cannot format transaction: " + err.Error())
	}
	return formatted, nil
}

func messageTransactionNotFound() *apiError {
	return &apiError{
		status:  http.StatusNotFound,
		code:    http.StatusNotFound,
		message: "transaction was not found",
	}
}

func (s *Server) locateOutboundMessageTransaction(ctx context.Context, query messageTransactionQuery) (locatedMessageTransaction, *apiError) {
	account := query.source.Data()
	block, err := s.store.LookupBlockByAccountLT(ctx, query.source.Workchain(), account, query.createdLT)
	if errors.Is(err, storage.ErrNotFound) {
		return locatedMessageTransaction{}, messageTransactionNotFound()
	}
	if err != nil {
		return locatedMessageTransaction{}, internalError("cannot locate transaction block: " + err.Error())
	}
	if !blockproof.BlockContainsAccount(block, query.source.Workchain(), account) {
		return locatedMessageTransaction{}, internalError("cannot locate transaction: block cannot contain source account")
	}

	accounts, apiErr := s.loadBlockAccountBlocks(ctx, block)
	if apiErr != nil {
		return locatedMessageTransaction{}, apiErr
	}

	entry, err := findBlockTransactionBefore(accounts, account, query.createdLT)
	if errors.Is(err, storage.ErrNotFound) {
		return locatedMessageTransaction{}, messageTransactionNotFound()
	}
	if err != nil {
		return locatedMessageTransaction{}, internalError("cannot locate source transaction: " + err.Error())
	}

	header, err := parseTransactionLookupHeader(entry.cell)
	if err != nil {
		return locatedMessageTransaction{}, internalError("cannot parse source transaction: " + err.Error())
	}
	if header.lt != entry.lt {
		return locatedMessageTransaction{}, internalError("cannot locate source transaction: transaction LT does not match dictionary key")
	}
	if !bytes.Equal(header.account[:], account) {
		return locatedMessageTransaction{}, internalError("cannot locate source transaction: transaction account does not match account block")
	}

	// TON assigns an outbound message created_lt as transaction LT + 1 + its
	// output dictionary index, so one point lookup identifies the message.
	delta := query.createdLT - header.lt
	if delta == 0 || delta > uint64(header.outMsgCount) {
		return locatedMessageTransaction{}, messageTransactionNotFound()
	}
	messageCell, err := transactionOutboundMessageCell(header.io, uint16(delta-1))
	if errors.Is(err, storage.ErrNotFound) {
		return locatedMessageTransaction{}, messageTransactionNotFound()
	}
	if err != nil {
		return locatedMessageTransaction{}, internalError("cannot load source transaction message: " + err.Error())
	}
	match, err := internalMessageMatches(messageCell, query)
	if err != nil {
		return locatedMessageTransaction{}, internalError("cannot parse source transaction message: " + err.Error())
	}
	if !match {
		return locatedMessageTransaction{}, messageTransactionNotFound()
	}

	return locatedMessageTransaction{block: block, cell: entry.cell}, nil
}

func (s *Server) locateInboundMessageTransaction(ctx context.Context, query messageTransactionQuery) (locatedMessageTransaction, *apiError) {
	account := query.destination.Data()
	seenBlocks := make(map[storage.BlockRootHash]struct{}, 3)
	seenTransactions := make(map[uint64]struct{}, messageLookupBlockLimit)

	// An inbound message may be processed well after it was created. Match the
	// bounded LT probes used by ton-http-api instead of allowing an unbounded
	// archive scan on a request path.
	for probe := uint64(0); probe < 3; probe++ {
		offset := probe * messageLookupLTStep
		if offset > ^uint64(0)-query.createdLT {
			break
		}

		block, err := s.store.LookupBlockByAccountLT(ctx, query.destination.Workchain(), account, query.createdLT+offset)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return locatedMessageTransaction{}, internalError("cannot locate transaction block: " + err.Error())
		}
		if !blockproof.BlockContainsAccount(block, query.destination.Workchain(), account) {
			return locatedMessageTransaction{}, internalError("cannot locate transaction: block cannot contain destination account")
		}

		blockKey := storage.BlockKey(block)
		if _, ok := seenBlocks[blockKey]; ok {
			continue
		}
		seenBlocks[blockKey] = struct{}{}

		accounts, apiErr := s.loadBlockAccountBlocks(ctx, block)
		if apiErr != nil {
			return locatedMessageTransaction{}, apiErr
		}
		entries, err := latestBlockTransactions(accounts, account, messageLookupBlockLimit)
		if errors.Is(err, storage.ErrNotFound) {
			continue
		}
		if err != nil {
			return locatedMessageTransaction{}, internalError("cannot locate result transaction: " + err.Error())
		}
		current := &accountTxBlock{id: block, accounts: accounts}
		for i := range entries {
			entries[i].block = current
		}
		// A single descending iterator covers a dense account block. Sparse blocks
		// are extended to ten history items, while dense blocks stay capped at the
		// same forty-item bound as ton-http-api.
		historyLimit := len(entries)
		if historyLimit < messageLookupMinHistory {
			historyLimit = messageLookupMinHistory
		}
		for scanned := 0; scanned < historyLimit && scanned < len(entries); scanned++ {
			entry := entries[scanned]
			if _, ok := seenTransactions[entry.lt]; ok {
				break
			}
			seenTransactions[entry.lt] = struct{}{}

			header, err := parseTransactionLookupHeader(entry.cell)
			if err != nil {
				return locatedMessageTransaction{}, internalError("cannot parse result transaction: " + err.Error())
			}
			if header.lt != entry.lt {
				return locatedMessageTransaction{}, internalError("cannot locate result transaction: transaction LT does not match dictionary key")
			}
			if !bytes.Equal(header.account[:], account) {
				return locatedMessageTransaction{}, internalError("cannot locate result transaction: transaction account does not match account block")
			}
			if header.lt <= query.createdLT {
				break
			}

			messageCell, err := transactionInboundMessageCellFromIO(header.io)
			if err != nil {
				return locatedMessageTransaction{}, internalError("cannot load result transaction message: " + err.Error())
			}
			if messageCell != nil {
				match, err := internalMessageMatches(messageCell, query)
				if err != nil {
					return locatedMessageTransaction{}, internalError("cannot parse result transaction message: " + err.Error())
				}
				if match {
					return locatedMessageTransaction{block: entry.block.id, cell: entry.cell}, nil
				}
			}

			if header.prevLT == 0 {
				break
			}
			if header.prevLT >= header.lt {
				return locatedMessageTransaction{}, internalError("cannot locate result transaction: invalid previous transaction LT")
			}
			if scanned+1 < len(entries) {
				previous := entries[scanned+1]
				if previous.lt != header.prevLT || previous.cell.HashKey() != header.prevHash {
					return locatedMessageTransaction{}, internalError("cannot locate result transaction: previous transaction does not match account block")
				}
				continue
			}
			if len(entries) >= historyLimit {
				break
			}

			previousCell, previousBlock, err := s.lookupAccountTransactionCell(ctx,
				query.destination.Workchain(), account, header.prevLT, current)
			if errors.Is(err, storage.ErrNotFound) {
				break
			}
			if err != nil {
				return locatedMessageTransaction{}, internalError("cannot locate previous result transaction: " + err.Error())
			}
			if previousCell.HashKey() != header.prevHash {
				return locatedMessageTransaction{}, internalError("cannot locate result transaction: previous transaction hash mismatch")
			}
			current = previousBlock
			entries = append(entries, accountTxEntry{block: previousBlock, lt: header.prevLT, cell: previousCell})
		}
	}

	return locatedMessageTransaction{}, messageTransactionNotFound()
}

func blockTransactionCount(params requestParams) (uint32, *apiError) {
	count, err := params.optionalUint32("count")
	if errors.Is(err, errRequestParamNotFound) {
		return defaultTxCount, nil
	}
	if err != nil {
		return 0, asAPIError(err)
	}
	if count > maxTxCount {
		return maxTxCount, nil
	}
	return count, nil
}

func historyLimit(params requestParams) (int, *apiError) {
	limit, err := params.optionalUint32("limit")
	if errors.Is(err, errRequestParamNotFound) {
		return defaultHistoryLimit, nil
	}
	if err != nil {
		return 0, asAPIError(err)
	}
	if limit > maxHistoryLimit {
		return maxHistoryLimit, nil
	}
	return int(limit), nil
}

// accountTxBlock caches a visited block for account history walks: consecutive
// transactions of a busy account often land in the same block, so keeping the
// parsed ShardAccountBlocks aug-dict avoids reloading and re-decoding it.
type accountTxBlock struct {
	id       ton.BlockIDExt
	accounts *tlb.ShardAccountBlocks
}

type accountTxEntry struct {
	block *accountTxBlock
	lt    uint64
	cell  *cell.Cell
}

type accountTransactionBlockLookupError struct {
	err error
}

func (e *accountTransactionBlockLookupError) Error() string {
	return e.err.Error()
}

func (e *accountTransactionBlockLookupError) Unwrap() error {
	return e.err
}

var errAccountBlockMismatch = errors.New("block cannot contain requested account")

// loadAccountTransaction locates one account transaction by logical time and
// extracts only its cell via the ShardAccountBlocks aug-dict descent, without
// decoding any other transaction of the block. current may carry the block from
// the previous history iteration; the returned block should be passed back on
// the next call to reuse it.
func (s *Server) loadAccountTransaction(ctx context.Context, workchain int32, account []byte, lt uint64, current *accountTxBlock) (*tlb.Transaction, *cell.Cell, *accountTxBlock, *apiError) {
	txCell, block, apiErr := s.loadAccountTransactionCell(ctx, workchain, account, lt, current)
	if apiErr != nil {
		return nil, nil, nil, apiErr
	}
	tx, apiErr := parseAccountTransaction(txCell)
	if apiErr != nil {
		return nil, nil, nil, apiErr
	}
	return tx, txCell, block, nil
}

func (s *Server) loadAccountTransactionCell(ctx context.Context, workchain int32, account []byte, lt uint64, current *accountTxBlock) (*cell.Cell, *accountTxBlock, *apiError) {
	txCell, block, err := s.lookupAccountTransactionCell(ctx, workchain, account, lt, current)
	var blockLookupErr *accountTransactionBlockLookupError
	if errors.As(err, &blockLookupErr) {
		return nil, nil, internalError("cannot locate transaction block: " + blockLookupErr.Error())
	}
	if errors.Is(err, errAccountBlockMismatch) {
		return nil, nil, validationError("failed to parse request: block cannot contain requested account")
	}
	if errors.Is(err, storage.ErrNotFound) {
		return nil, nil, validationError("failed to parse request: cannot locate transaction in block with specified logical time")
	}
	if err != nil {
		return nil, nil, internalError("cannot list block transactions: " + err.Error())
	}
	return txCell, block, nil
}

func (s *Server) lookupAccountTransactionCell(ctx context.Context, workchain int32, account []byte, lt uint64, current *accountTxBlock) (*cell.Cell, *accountTxBlock, error) {
	// (account, lt) identifies a transaction chain-wide, so a hit in the cached
	// block is always the right transaction; on a miss fall back to the
	// authoritative account-LT index.
	if current != nil {
		txCell, err := findBlockTransactionCell(current.accounts, account, lt)
		if err == nil {
			return txCell, current, nil
		}
		if !errors.Is(err, storage.ErrNotFound) {
			return nil, nil, fmt.Errorf("load cached account transaction: %w", err)
		}
	}

	block, err := s.store.LookupBlockByAccountLT(ctx, workchain, account, lt)
	if err != nil {
		return nil, nil, &accountTransactionBlockLookupError{err: err}
	}
	if !blockproof.BlockContainsAccount(block, workchain, account) {
		return nil, nil, errAccountBlockMismatch
	}

	accounts, err := s.loadBlockAccountBlocksData(ctx, block)
	if err != nil {
		return nil, nil, err
	}

	txCell, err := findBlockTransactionCell(accounts, account, lt)
	if err != nil {
		return nil, nil, fmt.Errorf("load account transaction: %w", err)
	}
	return txCell, &accountTxBlock{id: block, accounts: accounts}, nil
}

// loadBlockAccountBlocks loads the ShardAccountBlocks aug-dict of a block
// without decoding the block header or any transaction.
func (s *Server) loadBlockAccountBlocks(ctx context.Context, id ton.BlockIDExt) (*tlb.ShardAccountBlocks, *apiError) {
	accounts, err := s.loadBlockAccountBlocksData(ctx, id)
	if err != nil {
		return nil, internalError("cannot load block transactions: " + err.Error())
	}
	return accounts, nil
}

func (s *Server) loadBlockAccountBlocksData(ctx context.Context, id ton.BlockIDExt) (*tlb.ShardAccountBlocks, error) {
	root, err := s.store.BlockRoot(ctx, id)
	if err != nil {
		return nil, fmt.Errorf("load block: %w", err)
	}
	accounts, err := blockAccountBlocks(root)
	if err != nil {
		return nil, fmt.Errorf("unpack block: %w", err)
	}
	return accounts, nil
}

func blockAccountBlocks(root *cell.Cell) (*tlb.ShardAccountBlocks, error) {
	extra, err := blockproof.LoadBlockExtra(root)
	if err != nil {
		return nil, fmt.Errorf("load block extra: %w", err)
	}
	if extra.ShardAccountBlocks == nil {
		return nil, fmt.Errorf("block does not contain shard account blocks")
	}

	loader, err := extra.ShardAccountBlocks.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse shard account blocks: %w", err)
	}
	var accounts tlb.ShardAccountBlocks
	if err = tlb.LoadFromCell(&accounts, loader); err != nil {
		return nil, fmt.Errorf("load shard account blocks: %w", err)
	}
	return &accounts, nil
}

// findBlockTransactionCell extracts a single transaction cell from the
// ShardAccountBlocks aug-dict by (account, lt) key without decoding it.
// Returns storage.ErrNotFound when the block has no such transaction.
func findBlockTransactionCell(accounts *tlb.ShardAccountBlocks, account []byte, lt uint64) (*cell.Cell, error) {
	accountTransactions, err := blockAccountTransactions(accounts, account)
	if err != nil {
		return nil, err
	}

	// A positioned inline iterator doubles as a point lookup: the first item is
	// the exact key or the transaction is absent.
	txIt, err := accountTransactions.AugDictInlineIteratorAt(64, tlb.AugAccountTransactions{}, skipSingleRef,
		uint64Cell(lt), false, false, true)
	if err != nil {
		return nil, fmt.Errorf("load account transaction: %w", err)
	}
	entry, err := nextAccountTransaction(txIt)
	if err != nil {
		return nil, err
	}
	if entry.lt != lt {
		return nil, storage.ErrNotFound
	}
	return entry.cell, nil
}

func findBlockTransactionBefore(accounts *tlb.ShardAccountBlocks, account []byte, lt uint64) (accountTxEntry, error) {
	accountTransactions, err := blockAccountTransactions(accounts, account)
	if err != nil {
		return accountTxEntry{}, err
	}

	txIt, err := accountTransactions.AugDictInlineIteratorAt(64, tlb.AugAccountTransactions{}, skipSingleRef,
		uint64Cell(lt), true, false, false)
	if err != nil {
		return accountTxEntry{}, fmt.Errorf("load account transaction before LT: %w", err)
	}
	return nextAccountTransaction(txIt)
}

func latestBlockTransactions(accounts *tlb.ShardAccountBlocks, account []byte, limit int) ([]accountTxEntry, error) {
	accountTransactions, err := blockAccountTransactions(accounts, account)
	if err != nil {
		return nil, err
	}

	txIt, err := accountTransactions.AugDictInlineIterator(64, tlb.AugAccountTransactions{}, skipSingleRef, true, false)
	if err != nil {
		return nil, fmt.Errorf("load latest account transactions: %w", err)
	}

	entries := make([]accountTxEntry, 0, limit)
	for len(entries) < limit && txIt.Next() {
		entry, err := accountTransactionFromIterator(txIt)
		if err != nil {
			return nil, err
		}
		entries = append(entries, entry)
	}
	if err = txIt.Err(); err != nil {
		return nil, fmt.Errorf("load latest account transactions: %w", err)
	}
	if len(entries) == 0 {
		return nil, storage.ErrNotFound
	}
	return entries, nil
}

func blockAccountTransactions(accounts *tlb.ShardAccountBlocks, account []byte) (*cell.Slice, error) {
	if accounts.Accounts == nil || accounts.Accounts.AugmentedDictionary == nil {
		return nil, storage.ErrNotFound
	}

	accountTransactions, err := accounts.Accounts.LoadValueWithExtra(blockproof.AccountKey(account))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load account block: %w", err)
	}

	// LoadValueWithExtra keeps the augmentation in front of the value
	if err = (tlb.AugShardAccountBlocks{}).SkipExtra(accountTransactions); err != nil {
		return nil, fmt.Errorf("load account block augmentation: %w", err)
	}

	// acc_trans#5 account_addr:bits256 transactions:(HashmapAug 64 ^Transaction CurrencyCollection)
	magic, err := accountTransactions.LoadUInt(4)
	if err != nil {
		return nil, fmt.Errorf("load account block magic: %w", err)
	}
	if magic != 0x5 {
		return nil, fmt.Errorf("invalid account block magic %x", magic)
	}
	storedAccount, err := accountTransactions.LoadSlice(256)
	if err != nil {
		return nil, fmt.Errorf("load account block address: %w", err)
	}
	if !bytes.Equal(storedAccount, account) {
		return nil, fmt.Errorf("account block address does not match dictionary key")
	}
	return accountTransactions, nil
}

func nextAccountTransaction(txIt *cell.AugDictIterator) (accountTxEntry, error) {
	if !txIt.Next() {
		if err := txIt.Err(); err != nil {
			return accountTxEntry{}, fmt.Errorf("load account transaction: %w", err)
		}
		return accountTxEntry{}, storage.ErrNotFound
	}
	return accountTransactionFromIterator(txIt)
}

func accountTransactionFromIterator(txIt *cell.AugDictIterator) (accountTxEntry, error) {
	txKeyLoader, err := txIt.Key().BeginParse()
	if err != nil {
		return accountTxEntry{}, fmt.Errorf("begin parse transaction LT: %w", err)
	}
	lt, err := txKeyLoader.LoadUInt(64)
	if err != nil {
		return accountTxEntry{}, fmt.Errorf("load transaction LT: %w", err)
	}
	txCell, err := txIt.Value().LoadRefCell()
	if err != nil {
		return accountTxEntry{}, fmt.Errorf("load transaction cell: %w", err)
	}
	return accountTxEntry{lt: lt, cell: txCell}, nil
}

func uint64Cell(value uint64) *cell.Cell {
	return cell.BeginCell().MustStoreUInt(value, 64).EndCell()
}

func parseAccountTransaction(txCell *cell.Cell) (*tlb.Transaction, *apiError) {
	var tx tlb.Transaction
	if err := tlb.Parse(&tx, txCell); err != nil {
		return nil, internalError("cannot parse transaction: " + err.Error())
	}
	tx.Hash = txCell.Hash()
	return &tx, nil
}

func extTransactionFromTLB(typeName string, workchain int32, tx *tlb.Transaction, txCell *cell.Cell) (extTransaction, error) {
	inboundCell, err := transactionInboundMessageCell(txCell)
	if err != nil {
		return extTransaction{}, err
	}
	fees := transactionFees(tx)

	inMsg, err := extMessageFromTLB(tx.IO.In, inboundCell)
	if err != nil {
		return extTransaction{}, err
	}
	outMsgs, err := outMessagesFromTLB(tx.IO.Out)
	if err != nil {
		return extTransaction{}, err
	}

	return extTransaction{
		Type: typeName,
		Address: accountAddress{
			Type:           accountAddressType,
			AccountAddress: accountAddressString(workchain, tx.AccountAddr),
		},
		Account:       fmt.Sprintf("%d:%x", workchain, tx.AccountAddr),
		UTime:         tx.Now,
		Data:          bocBase64(txCell),
		TransactionID: internalTransactionID{Type: internalTransactionIDType, LT: fmt.Sprintf("%d", tx.LT), Hash: tonHash(tx.Hash)},
		Fee:           tx.TotalFees.Coins.Nano().String(),
		StorageFee:    fees.storage.String(),
		OtherFee:      fees.other.String(),
		InMsg:         inMsg,
		OutMsgs:       outMsgs,
	}, nil
}

func rawTransactionFromTLB(workchain int32, tx *tlb.Transaction, txCell *cell.Cell) (rawTransaction, error) {
	inboundCell, err := transactionInboundMessageCell(txCell)
	if err != nil {
		return rawTransaction{}, err
	}
	inMsg, err := rawMessageFromTLB(tx.IO.In, inboundCell)
	if err != nil {
		return rawTransaction{}, err
	}
	outMsgs, totalOutFees, err := rawOutMessagesFromTLB(tx.IO.Out)
	if err != nil {
		return rawTransaction{}, err
	}

	fees := transactionFees(tx)
	totalFee := tx.TotalFees.Coins.Nano()
	totalFee.Add(totalFee, totalOutFees)
	otherFee := new(big.Int).Sub(totalFee, fees.storage)
	if otherFee.Sign() < 0 {
		otherFee.SetInt64(0)
	}

	return rawTransaction{
		Type: rawTransactionType,
		Address: accountAddress{
			Type:           accountAddressType,
			AccountAddress: accountAddressString(workchain, tx.AccountAddr),
		},
		UTime:         tx.Now,
		Data:          bocBase64(txCell),
		TransactionID: internalTransactionID{Type: internalTransactionIDType, LT: fmt.Sprintf("%d", tx.LT), Hash: tonHash(tx.Hash)},
		Fee:           totalFee.String(),
		StorageFee:    fees.storage.String(),
		OtherFee:      otherFee.String(),
		InMsg:         inMsg,
		OutMsgs:       outMsgs,
	}, nil
}

type transactionLookupHeader struct {
	account     [32]byte
	lt          uint64
	prevHash    cell.Hash
	prevLT      uint64
	outMsgCount uint16
	io          *cell.Cell
}

func parseTransactionLookupHeader(txCell *cell.Cell) (transactionLookupHeader, error) {
	loader, err := txCell.BeginParse()
	if err != nil {
		return transactionLookupHeader{}, fmt.Errorf("begin parse transaction: %w", err)
	}
	magic, err := loader.LoadUInt(4)
	if err != nil {
		return transactionLookupHeader{}, fmt.Errorf("load transaction magic: %w", err)
	}
	if magic != 0b0111 {
		return transactionLookupHeader{}, fmt.Errorf("invalid transaction magic %b", magic)
	}
	var account [32]byte
	if err = loader.LoadSliceInto(account[:], 256); err != nil {
		return transactionLookupHeader{}, fmt.Errorf("load transaction account: %w", err)
	}
	lt, err := loader.LoadUInt(64)
	if err != nil {
		return transactionLookupHeader{}, fmt.Errorf("load transaction LT: %w", err)
	}
	var prevHash cell.Hash
	if err = loader.LoadSliceInto(prevHash[:], 256); err != nil {
		return transactionLookupHeader{}, fmt.Errorf("load previous transaction hash: %w", err)
	}
	prevLT, err := loader.LoadUInt(64)
	if err != nil {
		return transactionLookupHeader{}, fmt.Errorf("load previous transaction LT: %w", err)
	}
	if err = loader.SkipBits(32); err != nil {
		return transactionLookupHeader{}, fmt.Errorf("load transaction time: %w", err)
	}
	outMsgCount, err := loader.LoadUInt(15)
	if err != nil {
		return transactionLookupHeader{}, fmt.Errorf("load transaction output message count: %w", err)
	}
	if err = loader.SkipBits(4); err != nil {
		return transactionLookupHeader{}, fmt.Errorf("load transaction account statuses: %w", err)
	}
	ioCell, err := loader.LoadRefCell()
	if err != nil {
		return transactionLookupHeader{}, fmt.Errorf("load transaction messages: %w", err)
	}
	return transactionLookupHeader{
		account:     account,
		lt:          lt,
		prevHash:    prevHash,
		prevLT:      prevLT,
		outMsgCount: uint16(outMsgCount),
		io:          ioCell,
	}, nil
}

func transactionInboundMessageCell(txCell *cell.Cell) (*cell.Cell, error) {
	loader, err := txCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse transaction messages: %w", err)
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
	return transactionInboundMessageCellFromIO(ioCell)
}

func transactionInboundMessageCellFromIO(ioCell *cell.Cell) (*cell.Cell, error) {
	io, err := ioCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse transaction messages: %w", err)
	}
	hasInbound, err := io.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("load transaction inbound message flag: %w", err)
	}
	if !hasInbound {
		return nil, nil
	}
	inbound, err := io.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load transaction inbound message: %w", err)
	}
	return inbound, nil
}

func transactionOutboundMessageCell(ioCell *cell.Cell, index uint16) (*cell.Cell, error) {
	io, err := ioCell.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("begin parse transaction messages: %w", err)
	}
	hasInbound, err := io.LoadBoolBit()
	if err != nil {
		return nil, fmt.Errorf("load transaction inbound message flag: %w", err)
	}
	if hasInbound {
		if err = io.SkipBitsAndRefs(0, 1); err != nil {
			return nil, fmt.Errorf("skip transaction inbound message: %w", err)
		}
	}
	outMessages, err := io.LoadDict(15)
	if err != nil {
		return nil, fmt.Errorf("load transaction output messages: %w", err)
	}
	if outMessages == nil {
		return nil, storage.ErrNotFound
	}
	messageValue, err := outMessages.LoadValueByUintKey(uint64(index))
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return nil, storage.ErrNotFound
	}
	if err != nil {
		return nil, fmt.Errorf("load transaction output message %d: %w", index, err)
	}
	messageCell, err := messageValue.LoadRefCell()
	if err != nil {
		return nil, fmt.Errorf("load transaction output message %d: %w", index, err)
	}
	return messageCell, nil
}

var errUnsupportedMessageLookupAddress = errors.New("message address is not a standard 256-bit address")

type messageLookupAddress struct {
	workchain int32
	account   [32]byte
}

func internalMessageMatches(messageCell *cell.Cell, query messageTransactionQuery) (bool, error) {
	loader, err := messageCell.BeginParse()
	if err != nil {
		return false, fmt.Errorf("begin parse message: %w", err)
	}
	isExternal, err := loader.LoadBoolBit()
	if err != nil {
		return false, fmt.Errorf("load message type: %w", err)
	}
	if isExternal {
		return false, nil
	}
	if _, err = loader.LoadUInt(3); err != nil {
		return false, fmt.Errorf("load internal message flags: %w", err)
	}

	source, err := loadMessageLookupAddress(loader)
	if errors.Is(err, errUnsupportedMessageLookupAddress) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load internal message source: %w", err)
	}
	destination, err := loadMessageLookupAddress(loader)
	if errors.Is(err, errUnsupportedMessageLookupAddress) {
		return false, nil
	}
	if err != nil {
		return false, fmt.Errorf("load internal message destination: %w", err)
	}
	if err = skipMessageLookupCoins(loader); err != nil {
		return false, fmt.Errorf("load internal message amount: %w", err)
	}
	if err = skipMessageLookupMaybeRef(loader); err != nil {
		return false, fmt.Errorf("load internal message extra currencies: %w", err)
	}
	if err = skipMessageLookupCoins(loader); err != nil {
		return false, fmt.Errorf("load internal message IHR fee: %w", err)
	}
	if err = skipMessageLookupCoins(loader); err != nil {
		return false, fmt.Errorf("load internal message forward fee: %w", err)
	}
	createdLT, err := loader.LoadUInt(64)
	if err != nil {
		return false, fmt.Errorf("load internal message created LT: %w", err)
	}

	return createdLT == query.createdLT &&
		source.workchain == query.source.Workchain() &&
		bytes.Equal(source.account[:], query.source.Data()) &&
		destination.workchain == query.destination.Workchain() &&
		bytes.Equal(destination.account[:], query.destination.Data()), nil
}

func loadMessageLookupAddress(loader *cell.Slice) (messageLookupAddress, error) {
	kind, err := loader.LoadUInt(2)
	if err != nil {
		return messageLookupAddress{}, err
	}

	switch kind {
	case 0:
		return messageLookupAddress{}, errUnsupportedMessageLookupAddress
	case 1:
		bits, err := loader.LoadUInt(9)
		if err != nil {
			return messageLookupAddress{}, err
		}
		if err = loader.SkipBits(uint(bits)); err != nil {
			return messageLookupAddress{}, err
		}
		return messageLookupAddress{}, errUnsupportedMessageLookupAddress
	case 2:
		if err = skipMessageLookupAnycast(loader); err != nil {
			return messageLookupAddress{}, err
		}
		workchain, err := loader.LoadUInt(8)
		if err != nil {
			return messageLookupAddress{}, err
		}
		value := messageLookupAddress{workchain: int32(int8(workchain))}
		if err = loader.LoadSliceInto(value.account[:], 256); err != nil {
			return messageLookupAddress{}, err
		}
		return value, nil
	case 3:
		if err = skipMessageLookupAnycast(loader); err != nil {
			return messageLookupAddress{}, err
		}
		bits, err := loader.LoadUInt(9)
		if err != nil {
			return messageLookupAddress{}, err
		}
		if err = loader.SkipBits(32 + uint(bits)); err != nil {
			return messageLookupAddress{}, err
		}
		return messageLookupAddress{}, errUnsupportedMessageLookupAddress
	default:
		return messageLookupAddress{}, fmt.Errorf("unsupported message address kind %d", kind)
	}
}

func skipMessageLookupAnycast(loader *cell.Slice) error {
	hasAnycast, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if !hasAnycast {
		return nil
	}
	depth, err := loader.LoadUInt(5)
	if err != nil {
		return err
	}
	if depth == 0 || depth > 30 {
		return fmt.Errorf("invalid anycast depth %d", depth)
	}
	return loader.SkipBits(uint(depth))
}

func skipMessageLookupCoins(loader *cell.Slice) error {
	byteLen, err := loader.LoadUInt(4)
	if err != nil {
		return err
	}
	return loader.SkipBits(uint(byteLen * 8))
}

func skipMessageLookupMaybeRef(loader *cell.Slice) error {
	hasRef, err := loader.LoadBoolBit()
	if err != nil {
		return err
	}
	if !hasRef {
		return nil
	}
	return loader.SkipBitsAndRefs(0, 1)
}

type originalMessage struct {
	message tlb.Message
	cell    *cell.Cell
}

func originalMessages(list *tlb.MessagesList) ([]originalMessage, error) {
	if list == nil || list.List == nil {
		return []originalMessage{}, nil
	}

	items, err := list.List.LoadAll()
	if err != nil {
		return nil, fmt.Errorf("load outbound messages: %w", err)
	}
	messages := make([]originalMessage, len(items))
	for i, item := range items {
		msgCell, err := item.Value.LoadRefCell()
		if err != nil {
			return nil, fmt.Errorf("load outbound message %d: %w", i, err)
		}
		loader, err := msgCell.BeginParse()
		if err != nil {
			return nil, fmt.Errorf("begin parse outbound message %d: %w", i, err)
		}
		if err = tlb.LoadFromCell(&messages[i].message, loader); err != nil {
			return nil, fmt.Errorf("parse outbound message %d: %w", i, err)
		}
		messages[i].cell = msgCell
	}
	return messages, nil
}

func rawMessageFromTLB(msg *tlb.Message, msgCell *cell.Cell) (*rawMessage, error) {
	if msg == nil || msg.Msg == nil {
		return nil, nil
	}

	formatted := rawMessage{
		Type:            rawMessageType,
		Hash:            tonHash(msgCell.Hash()),
		Source:          accountAddress{Type: accountAddressType},
		Destination:     accountAddress{Type: accountAddressType},
		ExtraCurrencies: []extraCurrency{},
		MsgData:         msgDataRaw{Type: msgDataRawType},
	}

	switch typed := msg.Msg.(type) {
	case *tlb.InternalMessage:
		formatted.Source.AccountAddress = friendlyAddress(typed.SrcAddr)
		formatted.Destination.AccountAddress = friendlyAddress(typed.DstAddr)
		formatted.Value = typed.Amount.Nano().String()
		formatted.FwdFee = typed.FwdFee.Nano().String()
		formatted.IHRFee = typed.IHRFee.Nano().String()
		formatted.CreatedLT = fmt.Sprintf("%d", typed.CreatedLT)
		formatted.MsgData = msgDataFromStateAndBody(typed.StateInit, typed.Body)
		formatted.BodyHash = messageBodyHash(typed.Body)
	case *tlb.ExternalMessage:
		formatted.Destination.AccountAddress = friendlyAddress(typed.DstAddr)
		formatted.Value = "0"
		formatted.FwdFee = "0"
		formatted.IHRFee = "0"
		formatted.CreatedLT = "0"
		formatted.MsgData = msgDataFromStateAndBody(typed.StateInit, typed.Body)
		formatted.BodyHash = messageBodyHash(typed.Body)
	case *tlb.ExternalMessageOut:
		formatted.Source.AccountAddress = friendlyAddress(typed.SrcAddr)
		formatted.Value = "0"
		formatted.FwdFee = "0"
		formatted.IHRFee = "0"
		formatted.CreatedLT = fmt.Sprintf("%d", typed.CreatedLT)
		formatted.MsgData = msgDataFromStateAndBody(typed.StateInit, typed.Body)
		formatted.BodyHash = messageBodyHash(typed.Body)
	default:
		return nil, fmt.Errorf("unsupported message type %T", msg.Msg)
	}
	return &formatted, nil
}

func rawOutMessagesFromTLB(list *tlb.MessagesList) ([]rawMessage, *big.Int, error) {
	messages, err := originalMessages(list)
	if err != nil {
		return nil, nil, err
	}

	totalFees := big.NewInt(0)
	out := make([]rawMessage, 0, len(messages))
	for i := range messages {
		msg, err := rawMessageFromTLB(&messages[i].message, messages[i].cell)
		if err != nil {
			return nil, nil, err
		}
		if msg == nil {
			continue
		}
		totalFees.Add(totalFees, messageForwardFee(&messages[i].message))
		out = append(out, *msg)
	}
	return out, totalFees, nil
}

func messageForwardFee(msg *tlb.Message) *big.Int {
	total := big.NewInt(0)
	if msg == nil || msg.Msg == nil {
		return total
	}

	if typed, ok := msg.Msg.(*tlb.InternalMessage); ok {
		total.Add(total, typed.FwdFee.Nano())
		total.Add(total, typed.IHRFee.Nano())
	}
	return total
}

type feeBreakdown struct {
	storage *big.Int
	other   *big.Int
}

func transactionFees(tx *tlb.Transaction) feeBreakdown {
	storage := big.NewInt(0)
	switch desc := tx.Description.(type) {
	case *tlb.TransactionDescriptionOrdinary:
		if desc.StoragePhase != nil {
			storage = desc.StoragePhase.StorageFeesCollected.Nano()
		}
	case *tlb.TransactionDescriptionStorage:
		storage = desc.StoragePhase.StorageFeesCollected.Nano()
	case *tlb.TransactionDescriptionTickTock:
		storage = desc.StoragePhase.StorageFeesCollected.Nano()
	case *tlb.TransactionDescriptionSplitPrepare:
		if desc.StoragePhase != nil {
			storage = desc.StoragePhase.StorageFeesCollected.Nano()
		}
	case *tlb.TransactionDescriptionMergePrepare:
		storage = desc.StoragePhase.StorageFeesCollected.Nano()
	case *tlb.TransactionDescriptionMergeInstall:
		if desc.StoragePhase != nil {
			storage = desc.StoragePhase.StorageFeesCollected.Nano()
		}
	}

	other := tx.TotalFees.Coins.Nano()
	other.Sub(other, storage)
	if other.Sign() < 0 {
		other.SetInt64(0)
	}
	return feeBreakdown{storage: storage, other: other}
}

func extMessageFromTLB(msg *tlb.Message, msgCell *cell.Cell) (*extMessage, error) {
	if msg == nil || msg.Msg == nil {
		return nil, nil
	}

	formatted := extMessage{
		Type:            extMessageType,
		Hash:            tonHash(msgCell.Hash()),
		ExtraCurrencies: []extraCurrency{},
		MsgData:         msgDataRaw{Type: msgDataRawType},
	}

	switch typed := msg.Msg.(type) {
	case *tlb.InternalMessage:
		formatted.Source = friendlyAddress(typed.SrcAddr)
		formatted.Destination = friendlyAddress(typed.DstAddr)
		formatted.Value = typed.Amount.Nano().String()
		formatted.FwdFee = typed.FwdFee.Nano().String()
		formatted.IHRFee = typed.IHRFee.Nano().String()
		formatted.CreatedLT = fmt.Sprintf("%d", typed.CreatedLT)
		formatted.MsgData = msgDataFromStateAndBody(typed.StateInit, typed.Body)
		formatted.BodyHash = messageBodyHash(typed.Body)
	case *tlb.ExternalMessage:
		formatted.Source = friendlyAddress(typed.SrcAddr)
		formatted.Destination = friendlyAddress(typed.DstAddr)
		formatted.Value = "0"
		formatted.FwdFee = "0"
		formatted.IHRFee = "0"
		formatted.CreatedLT = "0"
		formatted.MsgData = msgDataFromStateAndBody(typed.StateInit, typed.Body)
		formatted.BodyHash = messageBodyHash(typed.Body)
	case *tlb.ExternalMessageOut:
		formatted.Source = friendlyAddress(typed.SrcAddr)
		formatted.Destination = friendlyAddress(typed.DstAddr)
		formatted.Value = "0"
		formatted.FwdFee = "0"
		formatted.IHRFee = "0"
		formatted.CreatedLT = fmt.Sprintf("%d", typed.CreatedLT)
		formatted.MsgData = msgDataFromStateAndBody(typed.StateInit, typed.Body)
		formatted.BodyHash = messageBodyHash(typed.Body)
	default:
		return nil, fmt.Errorf("unsupported message type %T", msg.Msg)
	}
	return &formatted, nil
}

func outMessagesFromTLB(list *tlb.MessagesList) ([]extMessage, error) {
	messages, err := originalMessages(list)
	if err != nil {
		return nil, err
	}

	out := make([]extMessage, 0, len(messages))
	for i := range messages {
		msg, err := extMessageFromTLB(&messages[i].message, messages[i].cell)
		if err != nil {
			return nil, err
		}
		if msg != nil {
			out = append(out, *msg)
		}
	}
	return out, nil
}

func msgDataFromStateAndBody(state *tlb.StateInit, body *cell.Cell) msgDataRaw {
	data := msgDataRaw{Type: msgDataRawType}
	if body != nil {
		data.Body = bocBase64(body)
	}
	if state != nil {
		if stateCell, err := state.ToCell(); err == nil {
			data.InitState = bocBase64(stateCell)
		}
	}
	return data
}

func messageBodyHash(body *cell.Cell) string {
	if body == nil {
		return tonHash(make([]byte, 32))
	}
	return tonHash(body.Hash())
}

func friendlyAddress(addr *address.Address) string {
	if addr == nil {
		return ""
	}
	return addr.String()
}

func accountAddressString(workchain int32, data []byte) string {
	if len(data) != 32 {
		return ""
	}
	return address.NewAddress(0, byte(workchain), data).String()
}
