package httpapi

import (
	"context"
	"encoding/base64"
	"errors"
	"fmt"
	"math/big"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton/wallet"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	fullAccountStateType      = "raw.fullAccountState"
	extendedAccountStateType  = "fullAccountState"
	rawAccountStateType       = "raw.accountState"
	walletInformationType     = "ext.accounts.walletInformation"
	internalTransactionIDType = "internal.transactionId"
)

type accountSnapshot struct {
	addr      *address.Address
	block     blockIDExt
	shard     *tlb.ShardAccount
	account   *tlb.AccountState
	syncUTime uint32
	runMethod runMethodAccount
}

type fullAccountState struct {
	Type              string                `json:"@type"`
	Balance           string                `json:"balance"`
	ExtraCurrencies   []extraCurrency       `json:"extra_currencies"`
	LastTransactionID internalTransactionID `json:"last_transaction_id"`
	BlockID           blockIDExt            `json:"block_id"`
	Code              string                `json:"code"`
	Data              string                `json:"data"`
	FrozenHash        string                `json:"frozen_hash"`
	SyncUTime         uint32                `json:"sync_utime"`
	State             string                `json:"state"`
	Suspended         bool                  `json:"suspended"`
}

type internalTransactionID struct {
	Type string `json:"@type"`
	LT   string `json:"lt"`
	Hash string `json:"hash"`
}

type extraCurrency struct {
	Type   string `json:"@type"`
	ID     int32  `json:"id"`
	Amount string `json:"amount"`
}

type extendedAccountState struct {
	Type              string                `json:"@type"`
	Address           accountAddress        `json:"address"`
	Balance           string                `json:"balance"`
	ExtraCurrencies   []extraCurrency       `json:"extra_currencies"`
	LastTransactionID internalTransactionID `json:"last_transaction_id"`
	BlockID           blockIDExt            `json:"block_id"`
	SyncUTime         uint32                `json:"sync_utime"`
	AccountState      rawAccountState       `json:"account_state"`
	Revision          int                   `json:"revision"`
}

type rawAccountState struct {
	Type       string `json:"@type"`
	Code       string `json:"code"`
	Data       string `json:"data"`
	FrozenHash string `json:"frozen_hash"`
}

type walletInformation struct {
	Type              string                `json:"@type"`
	Wallet            bool                  `json:"wallet"`
	Balance           string                `json:"balance"`
	AccountState      string                `json:"account_state"`
	WalletType        string                `json:"wallet_type,omitempty"`
	Seqno             *uint64               `json:"seqno,omitempty"`
	LastTransactionID internalTransactionID `json:"last_transaction_id"`
	WalletID          *int64                `json:"wallet_id,omitempty"`
	SignatureAllowed  *bool                 `json:"is_signature_allowed,omitempty"`
}

func (s *Server) handleAddressBalance(ctx context.Context, params requestParams) (any, *apiError) {
	snapshot, apiErr := s.loadAccountSnapshot(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	return accountBalance(snapshot.account), nil
}

func (s *Server) handleAddressState(ctx context.Context, params requestParams) (any, *apiError) {
	snapshot, apiErr := s.loadAccountSnapshot(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	return accountStateName(snapshot.account), nil
}

func (s *Server) handleShardAccountCell(ctx context.Context, params requestParams) (any, *apiError) {
	snapshot, apiErr := s.loadAccountSnapshot(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}

	shardAccount, err := tlb.ToCell(snapshot.shard)
	if err != nil {
		return nil, internalError("cannot serialize shard account")
	}
	return bocBase64(shardAccount), nil
}

func (s *Server) handleAddressInformation(ctx context.Context, params requestParams) (any, *apiError) {
	snapshot, apiErr := s.loadAccountSnapshot(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}

	// The suspended flag needs a masterchain config descent, and only this
	// handler reports it, so it is computed here instead of in the snapshot.
	suspended, err := suspendedAddress(snapshot.runMethod, snapshot.addr, snapshot.syncUTime)
	if err != nil {
		return nil, internalError("cannot check suspended address: " + err.Error())
	}
	return fullAccountStateFromSnapshot(snapshot, suspended), nil
}

func (s *Server) handleExtendedAddressInformation(ctx context.Context, params requestParams) (any, *apiError) {
	snapshot, apiErr := s.loadAccountSnapshot(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}
	return extendedAccountStateFromSnapshot(snapshot), nil
}

func (s *Server) handleWalletInformation(ctx context.Context, params requestParams) (any, *apiError) {
	snapshot, apiErr := s.loadAccountSnapshot(ctx, params)
	if apiErr != nil {
		return nil, apiErr
	}

	version := walletVersion(snapshot.account, snapshot.shard)
	info := walletInformationFromSnapshot(snapshot, version)
	if !info.Wallet {
		return info, nil
	}

	apiErr = s.enrichWalletInformation(ctx, snapshot, version, &info)
	if apiErr != nil {
		return nil, apiErr
	}
	return info, nil
}

func (s *Server) loadAccountSnapshot(ctx context.Context, params requestParams) (accountSnapshot, *apiError) {
	addr, _, _, apiErr := parseStdAddressParam(params, "address")
	if apiErr != nil {
		return accountSnapshot{}, apiErr
	}

	seqno, hasSeqno, apiErr := params.optionalUint32("seqno")
	if apiErr != nil {
		return accountSnapshot{}, apiErr
	}

	var seqnoPtr *uint32
	if hasSeqno {
		seqnoPtr = &seqno
	}

	info, apiErr := s.runMethodAccount(ctx, addr, seqnoPtr)
	if apiErr != nil {
		return accountSnapshot{}, apiErr
	}

	syncUTime := info.genUTime
	if syncUTime == 0 && info.masterFragments != nil {
		syncUTime = info.masterFragments.Header().GenUTime
	}

	return accountSnapshot{
		addr:      addr,
		block:     blockIDExtFromTON(info.base),
		shard:     info.shard,
		account:   info.account,
		syncUTime: syncUTime,
		runMethod: info,
	}, nil
}

func extendedAccountStateFromSnapshot(snapshot accountSnapshot) extendedAccountState {
	return extendedAccountState{
		Type: extendedAccountStateType,
		Address: accountAddress{
			Type:           accountAddressType,
			AccountAddress: snapshot.addr.String(),
		},
		Balance:           accountBalance(snapshot.account),
		ExtraCurrencies:   []extraCurrency{},
		LastTransactionID: transactionIDFromShardAccount(snapshot.shard),
		BlockID:           snapshot.block,
		SyncUTime:         snapshot.syncUTime,
		AccountState: rawAccountState{
			Type:       rawAccountStateType,
			Code:       accountCode(snapshot.account),
			Data:       accountData(snapshot.account),
			FrozenHash: accountFrozenHash(snapshot.account),
		},
		Revision: 0,
	}
}

func fullAccountStateFromSnapshot(snapshot accountSnapshot, suspended bool) fullAccountState {
	state := snapshot.account
	return fullAccountState{
		Type:              fullAccountStateType,
		Balance:           accountBalance(state),
		ExtraCurrencies:   []extraCurrency{},
		LastTransactionID: transactionIDFromShardAccount(snapshot.shard),
		BlockID:           snapshot.block,
		Code:              accountCode(state),
		Data:              accountData(state),
		FrozenHash:        accountFrozenHash(state),
		SyncUTime:         snapshot.syncUTime,
		State:             accountStateName(state),
		Suspended:         suspended,
	}
}

func walletInformationFromSnapshot(snapshot accountSnapshot, version wallet.Version) walletInformation {
	info := walletInformation{
		Type:              walletInformationType,
		Wallet:            false,
		Balance:           accountBalance(snapshot.account),
		AccountState:      accountStateName(snapshot.account),
		LastTransactionID: transactionIDFromShardAccount(snapshot.shard),
	}

	if version == wallet.Unknown {
		return info
	}

	info.Wallet = true
	info.WalletType = walletTypeName(version)
	return info
}

func (s *Server) enrichWalletInformation(ctx context.Context, snapshot accountSnapshot, version wallet.Version, info *walletInformation) *apiError {
	if snapshot.account == nil || snapshot.account.StateInit == nil || snapshot.account.StateInit.Data == nil {
		return nil
	}

	walletID, err := walletIDFromData(version, snapshot.account.StateInit.Data)
	if err != nil {
		return internalError("cannot parse wallet data: " + err.Error())
	}
	info.WalletID = walletID

	if walletSupportsSeqno(version) {
		seqno, apiErr := s.walletGetMethodInt(ctx, snapshot, "seqno")
		if apiErr != nil {
			return apiErr
		}
		seqnoValue := seqno.Uint64()
		info.Seqno = &seqnoValue
	}

	if walletSupportsSignatureFlag(version) {
		allowed, apiErr := s.walletGetMethodInt(ctx, snapshot, "is_signature_allowed")
		if apiErr != nil {
			return apiErr
		}
		allowedValue := allowed.Sign() != 0
		info.SignatureAllowed = &allowedValue
	}
	return nil
}

func (s *Server) walletGetMethodInt(ctx context.Context, snapshot accountSnapshot, method string) (*big.Int, *apiError) {
	result, apiErr := s.executeRunMethod(nil, tlb.MethodNameHash(method), snapshot.addr, snapshot.runMethod)
	if apiErr != nil {
		return nil, apiErr
	}
	if result.Result == nil || result.Result.ExitCode != 0 {
		return nil, internalError(fmt.Sprintf("cannot run wallet method %s", method))
	}
	if len(result.Stack) == 0 {
		return nil, internalError(fmt.Sprintf("wallet method %s returned empty stack", method))
	}

	value, ok := result.Stack[0].(*big.Int)
	if !ok {
		return nil, internalError(fmt.Sprintf("wallet method %s returned non-integer result", method))
	}
	return value, nil
}

func walletTypeName(version wallet.Version) string {
	switch version {
	case wallet.V1R1:
		return "wallet v1 r1"
	case wallet.V1R2:
		return "wallet v1 r2"
	case wallet.V1R3:
		return "wallet v1 r3"
	case wallet.V2R1:
		return "wallet v2 r1"
	case wallet.V2R2:
		return "wallet v2 r2"
	case wallet.V3R1:
		return "wallet v3 r1"
	case wallet.V3R2:
		return "wallet v3 r2"
	case wallet.V4R1:
		return "wallet v4 r1"
	case wallet.V4R2:
		return "wallet v4 r2"
	case wallet.V5R1Beta, wallet.V5R1Final:
		return "wallet v5 r1"
	case wallet.HighloadV2R2:
		return "highload wallet v2 r2"
	case wallet.HighloadV2Verified:
		return "highload wallet v2 r2 verified"
	case wallet.HighloadV3:
		return "highload wallet v3"
	case wallet.Lockup:
		return "lockup wallet"
	default:
		return version.String()
	}
}

func walletSupportsSeqno(version wallet.Version) bool {
	switch version {
	case wallet.V1R1, wallet.V1R2, wallet.V1R3,
		wallet.V2R1, wallet.V2R2,
		wallet.V3R1, wallet.V3R2,
		wallet.V4R1, wallet.V4R2,
		wallet.V5R1Beta, wallet.V5R1Final:
		return true
	default:
		return false
	}
}

func walletSupportsSignatureFlag(version wallet.Version) bool {
	return version == wallet.V5R1Beta || version == wallet.V5R1Final
}

func walletIDFromData(version wallet.Version, data *cell.Cell) (*int64, error) {
	loader, err := data.BeginParse()
	if err != nil {
		return nil, err
	}

	var id uint64
	switch version {
	case wallet.V3R1, wallet.V3R2, wallet.V4R1, wallet.V4R2:
		if _, err = loader.LoadUInt(32); err != nil {
			return nil, err
		}
		id, err = loader.LoadUInt(32)
	case wallet.V5R1Beta:
		if _, err = loader.LoadSlice(33 + 32 + 8 + 8); err != nil {
			return nil, err
		}
		id, err = loader.LoadUInt(32)
	case wallet.V5R1Final:
		if _, err = loader.LoadBoolBit(); err != nil {
			return nil, err
		}
		if _, err = loader.LoadUInt(32); err != nil {
			return nil, err
		}
		id, err = loader.LoadUInt(32)
	case wallet.HighloadV2R2, wallet.HighloadV2Verified:
		id, err = loader.LoadUInt(32)
	case wallet.HighloadV3:
		if _, err = loader.LoadSlice(256); err != nil {
			return nil, err
		}
		id, err = loader.LoadUInt(32)
	default:
		return nil, nil
	}
	if err != nil {
		return nil, err
	}

	value := int64(id)
	return &value, nil
}

func walletVersion(state *tlb.AccountState, shard *tlb.ShardAccount) wallet.Version {
	if state == nil || shard == nil || !state.IsValid || state.StateInit == nil || state.StateInit.Code == nil || state.StateInit.Data == nil {
		return wallet.Unknown
	}

	account := &tlb.Account{
		IsActive:   state.Status == tlb.AccountStatusActive,
		State:      state,
		Code:       state.StateInit.Code,
		Data:       state.StateInit.Data,
		LastTxLT:   shard.LastTransLT,
		LastTxHash: shard.LastTransHash,
	}
	return wallet.GetWalletVersion(account)
}

func suspendedAddress(info runMethodAccount, addr *address.Address, now uint32) (bool, error) {
	if info.masterFragments == nil {
		return false, nil
	}

	extra, err := info.masterFragments.McStateExtra()
	if err != nil {
		return false, err
	}
	list, err := (tlb.BlockchainConfig{Root: extra.ConfigParams.Config.Params.AsCell()}).GetSuspendedAddressList()
	if errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) || errors.Is(err, tlb.ErrBlockchainConfigRootNil) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if list == nil || list.Addresses == nil || list.SuspendedUntil <= now {
		return false, nil
	}

	key := cell.BeginCell().
		MustStoreInt(int64(addr.Workchain()), 32).
		MustStoreSlice(addr.Data(), 256).
		EndCell()
	_, err = list.Addresses.LoadValue(key)
	if errors.Is(err, cell.ErrNoSuchKeyInDict) {
		return false, nil
	}
	return err == nil, err
}

func transactionIDFromShardAccount(shard *tlb.ShardAccount) internalTransactionID {
	if shard == nil {
		return internalTransactionID{Type: internalTransactionIDType, LT: "0", Hash: tonHash(make([]byte, 32))}
	}
	return internalTransactionID{
		Type: internalTransactionIDType,
		LT:   fmt.Sprintf("%d", shard.LastTransLT),
		Hash: tonHash(shard.LastTransHash),
	}
}

func accountBalance(account *tlb.AccountState) string {
	if account == nil || !account.IsValid {
		return "0"
	}
	return account.Balance.Nano().String()
}

func accountStateName(account *tlb.AccountState) string {
	if account == nil || !account.IsValid {
		return "uninitialized"
	}

	switch account.Status {
	case tlb.AccountStatusActive:
		return "active"
	case tlb.AccountStatusFrozen:
		return "frozen"
	default:
		return "uninitialized"
	}
}

func accountCode(account *tlb.AccountState) string {
	if account == nil || account.StateInit == nil || account.StateInit.Code == nil {
		return ""
	}
	return bocBase64(account.StateInit.Code)
}

func accountData(account *tlb.AccountState) string {
	if account == nil || account.StateInit == nil || account.StateInit.Data == nil {
		return ""
	}
	return bocBase64(account.StateInit.Data)
}

func accountFrozenHash(account *tlb.AccountState) string {
	if account == nil || account.Status != tlb.AccountStatusFrozen {
		return ""
	}
	return tonHash(account.StateHash)
}

func bocBase64(root *cell.Cell) string {
	if root == nil {
		return ""
	}
	return base64.StdEncoding.EncodeToString(root.ToBOCWithOptions(cell.BOCSerializeOptions{}))
}
