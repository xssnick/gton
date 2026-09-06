package main

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"sync"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const balanceSnapshotConcurrency = 16

// settlementPollPeriod is how often the drain re-reads the recipient aggregate.
// It is deliberately coarser than accountPollPeriod: one reading here is a
// fan-out over every recipient wallet, not a single account read.
const settlementPollPeriod = 2 * time.Second

func setResultProfile(result *commandResult, profile resolvedContractProfile) {
	result.ContractProfile = profile.name
	result.MinterCodeHash = profile.minterCodeHash
	result.WalletCodeHash = profile.walletCodeHash
}

func validateStateContract(state loadState, profile resolvedContractProfile) error {
	if state.ContractProfile != profile.name {
		return fmt.Errorf("state contract profile %q differs from requested profile %q", state.ContractProfile, profile.name)
	}
	if state.MinterCodeHash != profile.minterCodeHash {
		return fmt.Errorf("state minter code hash %q differs from profile hash %q", state.MinterCodeHash, profile.minterCodeHash)
	}
	if state.WalletCodeHash != profile.walletCodeHash {
		return fmt.Errorf("state wallet code hash %q differs from profile hash %q", state.WalletCodeHash, profile.walletCodeHash)
	}

	return nil
}

func validateJettonMasterProfile(
	ctx context.Context,
	api ton.APIClientWrapped,
	minter *address.Address,
	profile resolvedContractProfile,
) error {
	active, err := activeContract(ctx, api, minter, profile.minterCode)
	if err != nil {
		return fmt.Errorf("check jetton minter profile: %w", err)
	}
	if !active {
		return errors.New("jetton minter is not active")
	}

	master, err := api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return fmt.Errorf("get masterchain head for jetton profile: %w", err)
	}
	result, err := api.WaitForBlock(master.SeqNo).RunGetMethod(ctx, master, minter, "get_jetton_data")
	if err != nil {
		return fmt.Errorf("get jetton master data: %w", err)
	}
	walletCode, err := result.Cell(4)
	if err != nil {
		return fmt.Errorf("load jetton wallet code from master: %w", err)
	}
	if actualHash := cellHashHex(walletCode); actualHash != profile.walletCodeHash {
		return fmt.Errorf("on-chain jetton wallet code hash %s differs from profile hash %s", actualHash, profile.walletCodeHash)
	}

	return nil
}

func deriveJettonWalletAddress(
	walletCode *cell.Cell,
	minter *address.Address,
	owner *address.Address,
) (*address.Address, error) {
	data := cell.BeginCell().
		MustStoreBigCoins(new(big.Int)).
		MustStoreAddr(owner).
		MustStoreAddr(minter).
		MustStoreRef(walletCode).
		EndCell()

	return contractAddress(walletCode, data, 0)
}

func deriveRecipientJettonWallets(
	walletCode *cell.Cell,
	minter *address.Address,
	senderIndex uint64,
	runEpoch loadRunEpoch,
	recipients int,
) ([]*address.Address, error) {
	wallets := make([]*address.Address, recipients)
	for index := range wallets {
		walletAddr, err := deriveJettonWalletAddress(
			walletCode,
			minter,
			recipientAddress(senderIndex, runEpoch, uint64(index)),
		)
		if err != nil {
			return nil, fmt.Errorf("derive recipient %d jetton wallet: %w", index, err)
		}
		wallets[index] = walletAddr
	}

	return wallets, nil
}

func sumJettonWalletBalances(
	ctx context.Context,
	api ton.APIClientWrapped,
	wallets []*address.Address,
	expectedCode *cell.Cell,
) (*big.Int, error) {
	master, err := api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("get masterchain head for recipient snapshot: %w", err)
	}

	if len(wallets) == 0 {
		return new(big.Int), nil
	}

	snapshotCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	workerCount := min(balanceSnapshotConcurrency, len(wallets))
	balances := make([]*big.Int, len(wallets))
	errorsByWorker := make(chan error, workerCount)
	var workers sync.WaitGroup
	for worker := range workerCount {
		workers.Go(func() {
			for index := worker; index < len(wallets); index += workerCount {
				if err := snapshotCtx.Err(); err != nil {
					return
				}

				balance, err := jettonWalletBalanceAtBlock(
					snapshotCtx,
					api,
					master,
					wallets[index],
					expectedCode,
				)
				if err != nil {
					errorsByWorker <- fmt.Errorf("get recipient %d balance: %w", index, err)
					cancel()
					return
				}
				balances[index] = balance
			}
		})
	}
	workers.Wait()
	close(errorsByWorker)
	if err = <-errorsByWorker; err != nil {
		return nil, err
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}

	total := new(big.Int)
	for _, balance := range balances {
		total.Add(total, balance)
	}

	return total, nil
}

func jettonWalletBalance(
	ctx context.Context,
	api ton.APIClientWrapped,
	walletAddr *address.Address,
	expectedCode *cell.Cell,
) (*big.Int, error) {
	master, err := api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return nil, err
	}

	return jettonWalletBalanceAtBlock(ctx, api, master, walletAddr, expectedCode)
}

func jettonWalletBalanceAtBlock(
	ctx context.Context,
	api ton.APIClientWrapped,
	master *ton.BlockIDExt,
	walletAddr *address.Address,
	expectedCode *cell.Cell,
) (*big.Int, error) {
	account, err := api.WaitForBlock(master.SeqNo).GetAccount(ctx, master, walletAddr)
	if err != nil {
		return nil, err
	}
	if !account.IsActive || account.State.Status != tlb.AccountStatusActive {
		return new(big.Int), nil
	}
	if account.Code == nil || account.Code.HashKey() != expectedCode.HashKey() {
		return nil, errors.New("recipient account code differs from the contract profile")
	}
	if account.Data == nil {
		return nil, errors.New("active recipient jetton wallet has no data")
	}

	slice, err := account.Data.BeginParse()
	if err != nil {
		return nil, fmt.Errorf("parse recipient jetton wallet data: %w", err)
	}
	balance, err := slice.LoadBigCoins()
	if err != nil {
		return nil, fmt.Errorf("load recipient jetton balance: %w", err)
	}

	return balance, nil
}

func waitJettonWalletIncrease(
	ctx context.Context,
	api ton.APIClientWrapped,
	walletAddr *address.Address,
	expectedCode *cell.Cell,
	before *big.Int,
	minimumIncrease *big.Int,
	timeout time.Duration,
) error {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(accountPollPeriod)
	defer ticker.Stop()

	after := new(big.Int).Set(before)
	var lastErr error
	for {
		balance, err := jettonWalletBalance(ctx, api, walletAddr, expectedCode)
		if err == nil {
			after = balance
			increase := new(big.Int).Sub(after, before)
			if increase.Cmp(minimumIncrease) >= 0 {
				return nil
			}
		} else {
			// Liteserver reads can fail while a new shard block is propagating;
			// preserve the last error and retry until the explicit canary deadline.
			lastErr = err
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-deadline.C:
			if lastErr != nil {
				return fmt.Errorf("destination balance stayed at %s after %s: %w", after, timeout, lastErr)
			}
			return fmt.Errorf("destination balance increased from %s to %s, need at least %s after %s", before, after, minimumIncrease, timeout)
		case <-ticker.C:
		}
	}
}

// waitRecipientsSettled waits until the recipient wallets account for every
// submitted transfer, and returns the aggregate balance it last read.
//
// timeout is an upper bound and not a fixed cost. The lab passes a drain sized
// for the worst case — ten minutes against a ninety-second load — and a run
// whose transfers land in seconds must not idle for the rest of it: the node
// under test receives no load during that wait, and every later phase of the
// scenario is pushed back by it.
//
// A timeout is not an error here. The caller derives its verdict from the
// balances themselves, and "so many of so many transfers arrived" is a more
// precise report than "the wait expired". A failed reading is tolerated for the
// same reason the canary wait tolerates one — a liteserver read can fail while
// a shard block propagates — and so is a delivery count that does not add up
// yet; only the caller's final evaluation is authoritative.
//
// snapshot is taken rather than derived so this can be tested without a
// network: it is the recipient aggregate at one masterchain block.
func waitRecipientsSettled(
	ctx context.Context,
	snapshot func(context.Context) (*big.Int, error),
	before *big.Int,
	amount *big.Int,
	submitted int,
	timeout time.Duration,
	poll time.Duration,
) (*big.Int, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(poll)
	defer ticker.Stop()

	var last *big.Int
	for {
		after, err := snapshot(ctx)
		if err == nil {
			last = after
			if delivered, countErr := deliveredTransfers(before, after, amount, submitted); countErr == nil &&
				delivered >= submitted {
				return after, nil
			}
		} else if ctx.Err() != nil {
			return nil, err
		}

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-deadline.C:
			if last != nil {
				return last, nil
			}

			return snapshot(ctx)
		case <-ticker.C:
		}
	}
}

func deliveredTransfers(before, after, amount *big.Int, submitted int) (int, error) {
	if amount.Sign() <= 0 {
		return 0, errors.New("transfer amount must be positive")
	}
	if after.Cmp(before) < 0 {
		return 0, fmt.Errorf("recipient aggregate balance decreased from %s to %s", before, after)
	}

	increase := new(big.Int).Sub(after, before)
	delivered, remainder := new(big.Int).QuoRem(increase, amount, new(big.Int))
	if remainder.Sign() != 0 {
		return 0, fmt.Errorf("recipient balance increase %s is not divisible by transfer amount %s", increase, amount)
	}
	if !delivered.IsInt64() || delivered.Int64() > int64(submitted) {
		return 0, fmt.Errorf("recipient balances account for %s transfers, but only %d were submitted", delivered, submitted)
	}

	return int(delivered.Int64()), nil
}
