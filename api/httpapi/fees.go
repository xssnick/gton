package httpapi

import (
	"context"
	"crypto/rand"
	"errors"
	"fmt"
	"math"
	"math/big"

	"github.com/xssnick/gton/service/liveview"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	queryFeesType       = "query.fees"
	feesType            = "fees"
	estimateFeeMaxCells = 1 << 17
)

type queryFees struct {
	Type            string `json:"@type"`
	SourceFees      fees   `json:"source_fees"`
	DestinationFees []fees `json:"destination_fees"`
}

type fees struct {
	Type       string `json:"@type"`
	InFwdFee   int64  `json:"in_fwd_fee"`
	StorageFee int64  `json:"storage_fee"`
	GasFee     int64  `json:"gas_fee"`
	FwdFee     int64  `json:"fwd_fee"`
}

type messageUsage struct {
	cells uint64
	bits  uint64
}

func (s *Server) handleEstimateFee(ctx context.Context, params requestParams) (any, *apiError) {
	addr, apiErr := runMethodAddress(params)
	if apiErr != nil {
		return nil, apiErr
	}

	body, apiErr := requiredBOCCell(params, "body")
	if apiErr != nil {
		return nil, apiErr
	}
	stateInit, apiErr := estimateFeeStateInit(params)
	if apiErr != nil {
		return nil, apiErr
	}

	ignoreChkSig := true
	if value, err := params.optionalBool("ignore_chksig"); err == nil {
		ignoreChkSig = value
	} else if !errors.Is(err, errRequestParamNotFound) {
		return nil, asAPIError(err)
	}

	info, apiErr := s.runMethodAccount(ctx, addr, nil)
	if apiErr != nil {
		return nil, apiErr
	}
	msg := &tlb.ExternalMessage{
		SrcAddr:   address.NewAddressNone(),
		DstAddr:   addr,
		ImportFee: tlb.Coins{},
		StateInit: stateInit,
		Body:      body,
	}
	msgCell, err := tlb.ToCell(msg)
	if err != nil {
		return nil, validationError("failed to parse request: cannot build external message: " + err.Error())
	}

	accountCode := estimateFeeAccountCode(info.account, stateInit)
	config, err := info.masterFragments.RunMethodConfig(info.genUTime, accountCode)
	if err != nil {
		return nil, internalError("cannot load masterchain config: " + err.Error())
	}

	sourceFees, apiErr := estimateFeeBaseSourceFees(addr, info, config, msgCell)
	if apiErr != nil {
		return nil, apiErr
	}

	// Emulate the inbound external message to derive the storage/gas/forward
	// fees. The block context carries the config epoch, prev-blocks and global
	// libraries; the account's own libraries come from the prepared account and
	// deploy-time libraries from the message state init.
	block, err := info.masterFragments.BlockContext(info.genUTime, info.genLT)
	if err != nil {
		return nil, internalError("cannot build block context: " + err.Error())
	}
	account, err := tvm.PrepareParsedAccount(info.shard, info.account, addr)
	if err != nil {
		return nil, internalError("cannot prepare account: " + err.Error())
	}
	message, err := tvm.PrepareParsedMessage(msgCell, &tlb.Message{
		MsgType: tlb.MsgTypeExternalIn,
		Msg:     msg,
	})
	if err != nil {
		return nil, internalError("cannot prepare message: " + err.Error())
	}
	res, err := s.tvm.EmulateTransaction(block, account, message, tvm.TransactionOptions{
		LogicalTime:                 int64(info.genLT + 2),
		RandSeed:                    randomSeed(),
		SignatureCheckAlwaysSucceed: ignoreChkSig,
	})
	if err != nil {
		return nil, internalError("cannot estimate fee: " + err.Error())
	}
	tx, err := res.ParseTransaction()
	if err != nil {
		return nil, internalError("cannot parse emulated transaction: " + err.Error())
	}
	sourceFees, apiErr = estimateFeesFromTransaction(sourceFees, tx)
	if apiErr != nil {
		return nil, apiErr
	}

	return queryFees{
		Type:            queryFeesType,
		SourceFees:      sourceFees,
		DestinationFees: []fees{},
	}, nil
}

func estimateFeeStateInit(params requestParams) (*tlb.StateInit, *apiError) {
	code, err := optionalBOCCell(params, "init_code")
	if errors.Is(err, errRequestParamNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, asAPIError(err)
	}

	data, apiErr := requiredBOCCell(params, "init_data")
	if apiErr != nil {
		return nil, apiErr
	}
	return &tlb.StateInit{Code: code, Data: data}, nil
}

func estimateFeeAccountCode(account *tlb.AccountState, stateInit *tlb.StateInit) *cell.Cell {
	if stateInit != nil && stateInit.Code != nil {
		return stateInit.Code
	}
	if account != nil && account.StateInit != nil {
		return account.StateInit.Code
	}
	return nil
}

func estimateFeeBaseSourceFees(addr *address.Address, info runMethodAccount, config liveview.RunMethodConfigInfo, msgCell *cell.Cell) (fees, *apiError) {
	cfg := tlb.BlockchainConfig{Root: config.Root}

	inFwdFee, err := estimateFeeImportFee(cfg, addr, msgCell)
	if err != nil {
		return fees{}, internalError("cannot compute inbound forward fee: " + err.Error())
	}
	storageFee, err := estimateFeeStorageFee(cfg, addr, info.account, info.genUTime)
	if err != nil {
		return fees{}, internalError("cannot compute storage fee: " + err.Error())
	}

	inFwd, ok := int64FromBig(inFwdFee)
	if !ok {
		return fees{}, internalError("inbound forward fee does not fit int64")
	}
	storage, ok := int64FromBig(storageFee)
	if !ok {
		return fees{}, internalError("storage fee does not fit int64")
	}

	return fees{
		Type:       feesType,
		InFwdFee:   inFwd,
		StorageFee: storage,
	}, nil
}

func estimateFeeImportFee(config tlb.BlockchainConfig, addr *address.Address, msgCell *cell.Cell) (*big.Int, error) {
	prices, err := config.GetMsgForwardPrices(addr.Workchain() == masterchainWorkchain)
	if err != nil {
		if errors.Is(err, tlb.ErrBlockchainConfigRootNil) || errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
			return big.NewInt(0), nil
		}
		return nil, err
	}
	usage, err := estimateFeeMessageTailUsage(msgCell)
	if err != nil {
		return nil, err
	}
	return prices.ComputeForwardFee(usage.cells, usage.bits), nil
}

func estimateFeeStorageFee(config tlb.BlockchainConfig, addr *address.Address, account *tlb.AccountState, now uint32) (*big.Int, error) {
	total := big.NewInt(0)
	if account == nil || !account.IsValid {
		return total, nil
	}
	if account.StorageInfo.DuePayment != nil {
		total.Add(total, account.StorageInfo.DuePayment.Nano())
	}
	usage := account.StorageInfo.StorageUsed
	if usage.CellsUsed == nil || usage.BitsUsed == nil || now <= account.StorageInfo.LastPaid || account.StorageInfo.LastPaid == 0 {
		return total, nil
	}

	fee, err := config.ComputeStorageFee(
		addr.Workchain() == masterchainWorkchain,
		account.StorageInfo.LastPaid,
		now,
		usage.BitsUsed.Uint64(),
		usage.CellsUsed.Uint64(),
	)
	if err != nil {
		if errors.Is(err, tlb.ErrBlockchainConfigRootNil) || errors.Is(err, tlb.ErrBlockchainConfigParamAbsent) {
			return total, nil
		}
		return nil, err
	}
	total.Add(total, fee)
	return total, nil
}

func estimateFeesFromTransaction(base fees, tx *tlb.Transaction) (fees, *apiError) {
	desc, ok := tx.Description.(*tlb.TransactionDescriptionOrdinary)
	if !ok {
		return fees{}, internalError(fmt.Sprintf("cannot estimate fee from transaction description %T", tx.Description))
	}

	out := base
	if desc.StoragePhase != nil {
		value, ok := int64FromBig(desc.StoragePhase.StorageFeesCollected.Nano())
		if !ok {
			return fees{}, internalError("storage fee does not fit int64")
		}
		out.StorageFee = value
	}
	if vmPhase, ok := desc.ComputePhase.Phase.(*tlb.ComputePhaseVM); ok {
		value, ok := int64FromBig(vmPhase.GasFees.Nano())
		if !ok {
			return fees{}, internalError("gas fee does not fit int64")
		}
		out.GasFee = value
	}
	if desc.ActionPhase != nil && desc.ActionPhase.TotalFwdFees != nil {
		value, ok := int64FromBig(desc.ActionPhase.TotalFwdFees.Nano())
		if !ok {
			return fees{}, internalError("forward fee does not fit int64")
		}
		out.FwdFee = value
	}
	return out, nil
}

func requiredBOCCell(params requestParams, field string) (*cell.Cell, *apiError) {
	raw, apiErr := params.requiredString(field)
	if apiErr != nil {
		return nil, apiErr
	}
	return parseBOCCellParam(raw, field)
}

func optionalBOCCell(params requestParams, field string) (*cell.Cell, error) {
	raw, err := params.optionalNonEmptyString(field)
	if err != nil {
		return nil, err
	}
	root, apiErr := parseBOCCellParam(raw, field)
	if apiErr != nil {
		return nil, apiErr
	}
	return root, nil
}

func parseBOCCellParam(raw string, field string) (*cell.Cell, *apiError) {
	data, apiErr := parseBytes(raw, field)
	if apiErr != nil {
		return nil, apiErr
	}
	root, err := cell.FromBOCWithOptions(data, cell.BOCParseOptions{
		MaxCells: estimateFeeMaxCells,
	})
	if err != nil {
		return nil, validationError(fmt.Sprintf("failed to parse request: Error at path '%s': invalid bag of cells", field))
	}
	return root, nil
}

func estimateFeeMessageTailUsage(root *cell.Cell) (messageUsage, error) {
	usage, rootBits, err := estimateFeeMessageUsage(root)
	if err != nil {
		return messageUsage{}, err
	}

	usage.cells--
	usage.bits -= rootBits
	return usage, nil
}

func estimateFeeMessageUsage(root *cell.Cell) (messageUsage, uint64, error) {
	seen := map[cell.Hash]struct{}{}
	var usage messageUsage
	var rootBits uint64

	var walk func(*cell.Cell, bool) error
	walk = func(current *cell.Cell, isRoot bool) error {
		slice, err := current.BeginParseWithoutTrace()
		if err != nil {
			return err
		}
		loaded := slice.BaseCell()
		if isRoot {
			rootBits = uint64(loaded.BitsSize())
		}
		key := loaded.HashKey()
		if _, ok := seen[key]; ok {
			return nil
		}
		seen[key] = struct{}{}
		usage.cells++
		usage.bits += uint64(loaded.BitsSize())

		for slice.RefsNum() > 0 {
			ref, err := slice.LoadRefCell()
			if err != nil {
				return err
			}
			if err := walk(ref, false); err != nil {
				return err
			}
		}
		return nil
	}
	if err := walk(root, true); err != nil {
		return messageUsage{}, 0, err
	}
	return usage, rootBits, nil
}

func int64FromBig(value *big.Int) (int64, bool) {
	if value == nil {
		return 0, true
	}
	if !value.IsInt64() || value.Sign() < 0 || value.Cmp(big.NewInt(math.MaxInt64)) > 0 {
		return 0, false
	}
	return value.Int64(), true
}

func randomSeed() []byte {
	var seed [32]byte
	if _, err := rand.Read(seed[:]); err != nil {
		return nil
	}
	return seed[:]
}
