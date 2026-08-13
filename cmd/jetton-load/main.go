package main

import (
	"context"
	"crypto/ed25519"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/rs/zerolog"
	"github.com/xssnick/gton/cmd/node/config"
	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/ton/jetton"
	"github.com/xssnick/tonutils-go/ton/wallet"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	stateFormatVersion    = 4
	previousStateFormat   = 3
	legacyStateFormat     = 2
	highloadKeyDomain     = "gton-jetton-load/highload/v2"
	highloadQueryLimit    = 1 << 23
	highloadQueryLowBits  = 10
	highloadQueryLowMask  = 1<<highloadQueryLowBits - 1
	highloadValidQueries  = (highloadQueryLimit >> highloadQueryLowBits) * highloadQueryLowMask
	highloadTTL           = 30 * 60
	previousHighloadTTL   = 120
	highloadInitPoll      = time.Second
	highloadInitResubmit  = 10 * time.Second
	defaultSetupTimeout   = 3 * time.Minute
	accountPollPeriod     = 200 * time.Millisecond
	batchPeriod           = time.Second
	submissionRetryPeriod = 500 * time.Millisecond
	submissionRetryWindow = 15 * time.Second
	fundingFeeReserveNano = int64(10_000_000)
)

var logger = zerolog.New(zerolog.ConsoleWriter{
	Out:        os.Stderr,
	TimeFormat: time.RFC3339,
}).With().Timestamp().Logger()

var errHighloadInitializationTimeout = errors.New("highload wallet initialization timeout")

type commonOptions struct {
	nodeConfig  string
	liteAddr    string
	statePath   string
	fundingKey  string
	senderIndex uint64
}

type setupOptions struct {
	commonOptions
	minterCode string
	walletCode string
	fundTON    string
	mintJetton string
	timeout    time.Duration
}

type runOptions struct {
	commonOptions
	rate       int
	duration   time.Duration
	drain      time.Duration
	recipients int
	msgTON     string
	jettons    string
}

type loadState struct {
	FormatVersion             int    `json:"format_version"`
	SenderIndex               uint64 `json:"sender_index"`
	Minter                    string `json:"minter"`
	HighloadWallet            string `json:"highload_wallet"`
	SourceJettonWallet        string `json:"source_jetton_wallet"`
	NextHighloadQueryID       uint32 `json:"next_highload_query_id"`
	HighloadQueryIDReservedAt int64  `json:"highload_query_id_reserved_at"`
	HighloadMessageTTL        uint32 `json:"highload_message_ttl"`
}

type networkClient struct {
	pool    *liteclient.ConnectionPool
	api     ton.APIClientWrapped
	funding *fundingWallet
	seed    []byte
}

type fundingWallet struct {
	api  ton.APIClientWrapped
	addr *address.Address
	key  ed25519.PrivateKey
}

type highloadTimeAPI interface {
	CurrentMasterchainInfo(context.Context) (*ton.BlockIDExt, error)
	GetBlockShardsInfo(context.Context, *ton.BlockIDExt) ([]*ton.BlockIDExt, error)
	GetBlockHeader(context.Context, *ton.BlockIDExt) (*tlb.BlockHeader, error)
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		if errors.Is(err, flag.ErrHelp) {
			return
		}

		logger.Error().Err(err).Msg("jetton load failed")
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: gton-jetton-load <setup|run> [flags]")
	}

	switch args[0] {
	case "setup":
		opts, err := parseSetupOptions(args[1:])
		if err != nil {
			return err
		}
		return setup(opts)
	case "run":
		opts, err := parseRunOptions(args[1:])
		if err != nil {
			return err
		}
		return runLoad(opts)
	default:
		return fmt.Errorf("unknown command %q; expected setup or run", args[0])
	}
}

func parseSetupOptions(args []string) (setupOptions, error) {
	var opts setupOptions
	set := flag.NewFlagSet("setup", flag.ContinueOnError)
	addCommonFlags(set, &opts.commonOptions)
	set.StringVar(&opts.minterCode, "minter-code", "cppnode/ton/benchmark/contracts/jetton-minter.code.boc", "jetton minter code BOC")
	set.StringVar(&opts.walletCode, "wallet-code", "cppnode/ton/benchmark/contracts/jetton-wallet.code.boc", "jetton wallet code BOC")
	set.StringVar(&opts.fundTON, "fund-ton", "10000", "TON transferred to the load wallet")
	set.StringVar(&opts.mintJetton, "mint-jetton", "1000000000", "jettons minted to the load wallet")
	set.DurationVar(&opts.timeout, "timeout", defaultSetupTimeout, "maximum duration for one sender setup")
	if err := set.Parse(args); err != nil {
		return setupOptions{}, err
	}
	if set.NArg() != 0 {
		return setupOptions{}, fmt.Errorf("unexpected setup arguments: %v", set.Args())
	}
	if opts.timeout <= 0 {
		return setupOptions{}, errors.New("setup timeout must be positive")
	}
	return opts, nil
}

func parseRunOptions(args []string) (runOptions, error) {
	var opts runOptions
	set := flag.NewFlagSet("run", flag.ContinueOnError)
	addCommonFlags(set, &opts.commonOptions)
	set.IntVar(&opts.rate, "rate", 30, "jetton transfers per second")
	set.DurationVar(&opts.duration, "duration", 30*time.Second, "load duration")
	set.DurationVar(&opts.drain, "drain", 30*time.Second, "maximum wait for submitted transfers")
	set.IntVar(&opts.recipients, "recipients", 256, "deterministic recipient owner addresses")
	set.StringVar(&opts.msgTON, "message-ton", "0.05", "TON attached to each jetton transfer")
	set.StringVar(&opts.jettons, "jettons", "1", "jettons sent by each transfer")
	if err := set.Parse(args); err != nil {
		return runOptions{}, err
	}
	if set.NArg() != 0 {
		return runOptions{}, fmt.Errorf("unexpected run arguments: %v", set.Args())
	}
	if opts.rate <= 0 || opts.rate > 254*254 {
		return runOptions{}, fmt.Errorf("rate must be between 1 and %d", 254*254)
	}
	if opts.duration <= 0 || opts.duration%batchPeriod != 0 {
		return runOptions{}, errors.New("duration must be a positive whole number of seconds")
	}
	if opts.drain < 0 {
		return runOptions{}, errors.New("drain cannot be negative")
	}
	if opts.recipients <= 0 {
		return runOptions{}, errors.New("recipients must be positive")
	}
	return opts, nil
}

func addCommonFlags(set *flag.FlagSet, opts *commonOptions) {
	set.StringVar(&opts.nodeConfig, "node-config", "config.json", "node config containing the liteserver key")
	set.StringVar(&opts.liteAddr, "lite", "127.0.0.1:7445", "liteserver address")
	set.StringVar(&opts.statePath, "state", "jetton-load.state.json", "load state file")
	set.StringVar(&opts.fundingKey, "funding-key", "", "base64 Ed25519 seed controlling the genesis funding wallet")
	set.Uint64Var(&opts.senderIndex, "sender-index", 0, "independent load sender index")
}

func connect(ctx context.Context, opts commonOptions) (*networkClient, error) {
	cfg, err := config.Load(opts.nodeConfig)
	if err != nil {
		return nil, fmt.Errorf("load node config: %w", err)
	}
	fundingSeed, err := base64.StdEncoding.DecodeString(strings.TrimSpace(opts.fundingKey))
	if err != nil {
		return nil, fmt.Errorf("decode funding key: %w", err)
	}
	if len(fundingSeed) != ed25519.SeedSize {
		return nil, fmt.Errorf("funding key has %d bytes, expected %d", len(fundingSeed), ed25519.SeedSize)
	}
	if len(cfg.Lite.Key) != ed25519.SeedSize {
		return nil, fmt.Errorf("liteserver key has %d bytes, expected %d", len(cfg.Lite.Key), ed25519.SeedSize)
	}

	pool := liteclient.NewConnectionPool()
	liteKey := ed25519.NewKeyFromSeed(cfg.Lite.Key).Public().(ed25519.PublicKey)
	if err = pool.AddConnection(ctx, opts.liteAddr, base64.StdEncoding.EncodeToString(liteKey)); err != nil {
		pool.Stop()
		return nil, fmt.Errorf("connect to liteserver: %w", err)
	}

	api := ton.NewAPIClient(pool).WithRetryTimeout(2, 3*time.Second)
	fundingKey := ed25519.NewKeyFromSeed(fundingSeed)
	funding := &fundingWallet{
		api:  api,
		addr: address.NewAddress(0, 0xff, make([]byte, 32)),
		key:  fundingKey,
	}

	client := &networkClient{pool: pool, api: api, funding: funding, seed: fundingSeed}
	master, err := api.CurrentMasterchainInfo(ctx)
	if err != nil {
		client.close()
		return nil, fmt.Errorf("get masterchain head: %w", err)
	}
	account, err := api.WaitForBlock(master.SeqNo).GetAccount(ctx, master, funding.addr)
	if err != nil {
		client.close()
		return nil, fmt.Errorf("get genesis wallet: %w", err)
	}
	if err = verifyFundingAccount(account, fundingKey.Public().(ed25519.PublicKey)); err != nil {
		client.close()
		return nil, err
	}
	seqno, err := fundingSeqno(ctx, api, funding.addr)
	if err != nil {
		client.close()
		return nil, fmt.Errorf("validator key 0 does not control the active genesis wallet at %s: %w", displayAddress(funding.addr), err)
	}

	logger.Info().
		Uint32("master_seqno", master.SeqNo).
		Uint32("funding_seqno", seqno).
		Str("funding_wallet", displayAddress(funding.addr)).
		Str("funding_balance", account.State.Balance.String()).
		Msg("connected to test network")
	return client, nil
}

func (c *networkClient) close() {
	clear(c.seed)
	c.pool.Stop()
}

func verifyFundingAccount(account *tlb.Account, publicKey ed25519.PublicKey) error {
	if !account.IsActive || account.State.Status != tlb.AccountStatusActive || account.Data == nil {
		return errors.New("genesis funding wallet is not active")
	}
	slice, err := account.Data.BeginParse()
	if err != nil {
		return err
	}
	if _, err = slice.LoadUInt(32); err != nil {
		return fmt.Errorf("load funding wallet seqno: %w", err)
	}
	key, err := slice.LoadSlice(256)
	if err != nil {
		return fmt.Errorf("load funding wallet public key: %w", err)
	}
	if !ed25519.PublicKey(key).Equal(publicKey) {
		return errors.New("validator key 0 does not match the genesis funding wallet public key")
	}
	return nil
}

func (s *fundingWallet) buildMessage(ctx context.Context, messages []*wallet.Message) (*cell.Cell, error) {
	if len(messages) != 1 {
		return nil, fmt.Errorf("legacy funding wallet requires exactly one message, got %d", len(messages))
	}
	master, err := s.api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return nil, fmt.Errorf("get funding wallet block: %w", err)
	}
	result, err := s.api.WaitForBlock(master.SeqNo).RunGetMethod(ctx, master, s.addr, "seqno")
	if err != nil {
		return nil, fmt.Errorf("get funding wallet seqno: %w", err)
	}
	seqno, err := result.Int(0)
	if err != nil {
		return nil, fmt.Errorf("parse funding wallet seqno: %w", err)
	}
	internal, err := tlb.ToCell(messages[0].InternalMessage)
	if err != nil {
		return nil, fmt.Errorf("serialize funding wallet message: %w", err)
	}
	payload := cell.BeginCell().
		MustStoreUInt(seqno.Uint64(), 32).
		MustStoreUInt(uint64(messages[0].Mode), 8).
		MustStoreRef(internal)
	signed := payload.EndCell()
	signature := signed.Sign(s.key)
	return cell.BeginCell().
		MustStoreSlice(signature, 512).
		MustStoreBuilder(payload).
		EndCell(), nil
}

func setup(opts setupOptions) error {
	var previous *loadState
	stored, err := readState(opts.statePath)
	if err == nil {
		if err = validateStateSender(stored, opts.senderIndex); err != nil {
			return err
		}
		previous = &stored
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.timeout)
	defer cancel()

	client, err := connect(ctx, opts.commonOptions)
	if err != nil {
		return err
	}
	defer client.close()

	minterCode, err := loadBOC(opts.minterCode)
	if err != nil {
		return fmt.Errorf("load minter code: %w", err)
	}
	jettonWalletCode, err := loadBOC(opts.walletCode)
	if err != nil {
		return fmt.Errorf("load jetton wallet code: %w", err)
	}

	highloadKey := deriveHighloadKey(client.seed, opts.senderIndex)
	highload, err := newHighloadWallet(client.api, highloadKey, nil)
	if err != nil {
		return err
	}
	defer clear(highloadKey)
	reprovisionHighload := previous != nil && previous.HighloadMessageTTL != highloadTTL
	if previous != nil && previous.HighloadWallet != displayAddress(highload.WalletAddress()) && !reprovisionHighload {
		return errors.New("load wallet derived from node config differs from existing state file")
	}
	if reprovisionHighload {
		logger.Info().
			Uint64("sender_index", opts.senderIndex).
			Uint32("previous_message_ttl", previous.HighloadMessageTTL).
			Uint32("message_ttl", highloadTTL).
			Msg("reprovisioning highload wallet for updated message TTL")
	}
	setupNow := time.Now()
	initialNextHighloadQueryID := currentHighloadQueryID(setupNow)
	highloadStateInit, err := wallet.GetStateInit(
		highload.PrivateKey().Public().(ed25519.PublicKey),
		wallet.ConfigHighloadV3{MessageTTL: highloadTTL},
		highload.GetSubwalletID(),
	)
	if err != nil {
		return fmt.Errorf("build load wallet state init: %w", err)
	}

	content := cell.BeginCell().MustStoreUInt(0, 8).MustStoreDict(nil).EndCell()
	minterData := cell.BeginCell().
		MustStoreBigCoins(big.NewInt(0)).
		MustStoreAddr(client.funding.addr).
		MustStoreRef(content).
		MustStoreRef(jettonWalletCode).
		EndCell()
	minter, err := contractAddress(minterCode, minterData, 0)
	if err != nil {
		return err
	}
	if previous != nil && previous.Minter != displayAddress(minter) {
		return errors.New("jetton minter derived from code differs from existing state file")
	}

	minterActive := contractMethodReady(ctx, client.api, minter, "get_jetton_data")
	if !minterActive {
		logger.Info().Str("minter", displayAddress(minter)).Msg("deploying jetton minter")
		deployMessage := &wallet.Message{
			Mode: wallet.PayGasSeparately,
			InternalMessage: &tlb.InternalMessage{
				IHRDisabled: true,
				Bounce:      false,
				DstAddr:     minter,
				Amount:      tlb.MustFromTON("10"),
				Body:        cell.BeginCell().EndCell(),
				StateInit:   &tlb.StateInit{Code: minterCode, Data: minterData},
			},
		}
		if err = sendFundingAndWait(ctx, client.api, client.funding, deployMessage); err != nil {
			return fmt.Errorf("deploy minter: %w", err)
		}
		if err = waitContractMethod(ctx, client.api, minter, "get_jetton_data"); err != nil {
			return fmt.Errorf("wait for minter deployment: %w", err)
		}
	}

	fundAmount, err := tlb.FromTON(opts.fundTON)
	if err != nil {
		return fmt.Errorf("parse fund-ton: %w", err)
	}
	highloadActive, err := activeContract(ctx, client.api, highload.WalletAddress(), highloadStateInit.Code)
	if err != nil {
		return fmt.Errorf("check load wallet: %w", err)
	}
	balance, err := accountBalance(ctx, client.api, highload.WalletAddress())
	if err != nil {
		return fmt.Errorf("get load wallet balance: %w", err)
	}
	if balance.Cmp(fundAmount.Nano()) < 0 {
		topUp := loadWalletTopUp(fundAmount.Nano(), balance)
		logger.Info().
			Str("wallet", displayAddress(highload.WalletAddress())).
			Str("amount", tlb.FromNanoTON(topUp).String()).
			Str("minimum_balance", fundAmount.String()).
			Msg("funding load wallet")
		comment, buildErr := wallet.CreateCommentCell("gton jetton load")
		if buildErr != nil {
			return buildErr
		}
		message := wallet.SimpleMessage(highload.WalletAddress(), tlb.FromNanoTON(topUp), comment)
		message.InternalMessage.Bounce = false
		if err = sendFundingAndWait(ctx, client.api, client.funding, message); err != nil {
			return fmt.Errorf("fund load wallet: %w", err)
		}
		if err = waitAccountBalance(ctx, client.api, highload.WalletAddress(), fundAmount.Nano()); err != nil {
			return fmt.Errorf("wait for load wallet funding: %w", err)
		}
	}

	if !highloadActive {
		initialQueryID := initialNextHighloadQueryID
		initialNextHighloadQueryID = advanceHighloadQueryID(initialNextHighloadQueryID)
		initialCreatedAt, timeErr := highloadCreatedAt(ctx, client.api, highload.WalletAddress())
		if timeErr != nil {
			return fmt.Errorf("resolve load wallet shard time: %w", timeErr)
		}
		highload, err = newHighloadWallet(client.api, highloadKey, func(context.Context, uint32) (uint32, int64, error) {
			return initialQueryID, initialCreatedAt, nil
		})
		if err != nil {
			return err
		}
		initMessage := wallet.SimpleMessage(client.funding.addr, tlb.MustFromTON("0.01"), cell.BeginCell().EndCell())
		// Build once so every retry has identical query ID, creation time, and signature.
		initExternal, buildErr := buildHighloadExternal(ctx, highload, []*wallet.Message{initMessage}, true)
		if buildErr != nil {
			return fmt.Errorf("build load wallet initialization: %w", buildErr)
		}
		err = initializeHighloadWallet(
			ctx,
			opts.timeout,
			func(ctx context.Context) error {
				return client.api.SendExternalMessage(ctx, initExternal)
			},
			func(ctx context.Context) (bool, error) {
				return activeContract(ctx, client.api, highload.WalletAddress(), highloadStateInit.Code)
			},
		)
		if err != nil {
			return fmt.Errorf("initialize load wallet: %w", err)
		}
	}

	jettonClient := jetton.NewJettonMasterClient(client.api, minter)
	sourceJetton, err := jettonClient.GetJettonWallet(ctx, highload.WalletAddress())
	if err != nil {
		return fmt.Errorf("derive source jetton wallet: %w", err)
	}
	if previous != nil && previous.SourceJettonWallet != displayAddress(sourceJetton.Address()) && !reprovisionHighload {
		return errors.New("source jetton wallet differs from existing state file")
	}
	mintAmount, err := tlb.FromTON(opts.mintJetton)
	if err != nil {
		return fmt.Errorf("parse mint-jetton: %w", err)
	}
	jettonBalance := new(big.Int)
	jettonWalletActive, err := activeContract(ctx, client.api, sourceJetton.Address(), jettonWalletCode)
	if err != nil {
		return fmt.Errorf("check source jetton wallet: %w", err)
	}
	if jettonWalletActive {
		jettonBalance, err = sourceJetton.GetBalance(ctx)
		if err != nil {
			return fmt.Errorf("get source jetton balance: %w", err)
		}
	}
	if jettonBalance.Cmp(mintAmount.Nano()) < 0 {
		mintDelta := new(big.Int).Sub(mintAmount.Nano(), jettonBalance)
		logger.Info().
			Str("wallet", displayAddress(sourceJetton.Address())).
			Str("amount", tlb.FromNanoTON(mintDelta).String()).
			Msg("minting test jettons")
		body, buildErr := buildMintPayload(highload.WalletAddress(), client.funding.addr, tlb.FromNanoTON(mintDelta))
		if buildErr != nil {
			return buildErr
		}
		if err = sendFundingAndWait(ctx, client.api, client.funding, wallet.SimpleMessage(minter, tlb.MustFromTON("3"), body)); err != nil {
			return fmt.Errorf("send mint message: %w", err)
		}
		if err = waitJettonBalance(ctx, sourceJetton, mintAmount.Nano()); err != nil {
			return fmt.Errorf("wait for minted jettons: %w", err)
		}
	}

	state := loadState{
		FormatVersion:             stateFormatVersion,
		SenderIndex:               opts.senderIndex,
		Minter:                    displayAddress(minter),
		HighloadWallet:            displayAddress(highload.WalletAddress()),
		SourceJettonWallet:        displayAddress(sourceJetton.Address()),
		HighloadQueryIDReservedAt: setupNow.Unix(),
		HighloadMessageTTL:        highloadTTL,
	}
	if previous != nil && !reprovisionHighload {
		if previous.NextHighloadQueryID >= highloadQueryLimit {
			return fmt.Errorf("stored highload query id %d is outside the 23-bit range", previous.NextHighloadQueryID)
		}
		var rebased bool
		state.NextHighloadQueryID, rebased = nextHighloadQueryID(*previous, setupNow)
		if rebased {
			logger.Info().
				Uint64("sender_index", opts.senderIndex).
				Uint32("previous_query_id", previous.NextHighloadQueryID).
				Uint32("query_id", state.NextHighloadQueryID).
				Msg("rebasing expired highload query IDs")
		}
	} else {
		state.NextHighloadQueryID = initialNextHighloadQueryID
	}
	if err = writeState(opts.statePath, state); err != nil {
		return err
	}

	logger.Info().
		Uint64("sender_index", state.SenderIndex).
		Str("minter", state.Minter).
		Str("load_wallet", state.HighloadWallet).
		Str("source_jetton_wallet", state.SourceJettonWallet).
		Str("state", opts.statePath).
		Msg("jetton load setup is ready")
	return nil
}

func loadWalletTopUp(minimum, balance *big.Int) *big.Int {
	// The internal transfer value pays its delivery fees, so sending the exact
	// deficit can never make the recipient reach the requested minimum.
	amount := new(big.Int).Sub(minimum, balance)
	return amount.Add(amount, big.NewInt(fundingFeeReserveNano))
}

func runLoad(opts runOptions) error {
	state, err := readState(opts.statePath)
	if err != nil {
		return err
	}
	if err = validateStateSender(state, opts.senderIndex); err != nil {
		return err
	}

	ctx, cancel := context.WithTimeout(context.Background(), opts.duration+opts.drain+defaultSetupTimeout)
	defer cancel()
	client, err := connect(ctx, opts.commonOptions)
	if err != nil {
		return err
	}
	defer client.close()

	highloadKey := deriveHighloadKey(client.seed, opts.senderIndex)
	defer clear(highloadKey)
	batches64 := uint64(opts.duration / batchPeriod)
	if batches64 > highloadValidQueries {
		return fmt.Errorf("load requires %d highload query IDs, maximum is %d", batches64, highloadValidQueries)
	}
	if state.NextHighloadQueryID >= highloadQueryLimit {
		return fmt.Errorf("stored highload query id %d is outside the 23-bit range", state.NextHighloadQueryID)
	}
	batches := uint32(batches64)
	runNow := time.Now()
	queryID, rebased := nextHighloadQueryID(state, runNow)
	if rebased {
		logger.Info().
			Uint64("sender_index", opts.senderIndex).
			Uint32("previous_query_id", state.NextHighloadQueryID).
			Uint32("query_id", queryID).
			Msg("rebasing expired highload query IDs")
	}
	reservedNext := queryID
	for range batches {
		reservedNext = advanceHighloadQueryID(reservedNext)
	}
	highload, err := newHighloadWallet(client.api, highloadKey, nil)
	if err != nil {
		return err
	}
	if displayAddress(highload.WalletAddress()) != state.HighloadWallet {
		return errors.New("load wallet derived from node config differs from state file")
	}
	createdAt, err := highloadCreatedAt(ctx, client.api, highload.WalletAddress())
	if err != nil {
		return fmt.Errorf("resolve load wallet shard time: %w", err)
	}
	highload, err = newHighloadWallet(client.api, highloadKey, func(context.Context, uint32) (uint32, int64, error) {
		id := queryID
		queryID = advanceHighloadQueryID(queryID)
		return id, createdAt, nil
	})
	if err != nil {
		return err
	}

	sourceAddr, err := address.ParseAddr(state.SourceJettonWallet)
	if err != nil {
		return fmt.Errorf("parse source jetton wallet: %w", err)
	}
	minterAddr, err := address.ParseAddr(state.Minter)
	if err != nil {
		return fmt.Errorf("parse minter: %w", err)
	}
	jettonClient := jetton.NewJettonMasterClient(client.api, minterAddr)
	sourceWallet, err := jettonClient.GetJettonWallet(ctx, highload.WalletAddress())
	if err != nil {
		return fmt.Errorf("resolve source jetton wallet: %w", err)
	}
	if !sourceWallet.Address().Equals(sourceAddr) {
		return errors.New("source jetton wallet from contract differs from state file")
	}
	before, err := sourceWallet.GetBalance(ctx)
	if err != nil {
		return fmt.Errorf("get initial jetton balance: %w", err)
	}
	jettonAmount, err := tlb.FromTON(opts.jettons)
	if err != nil {
		return fmt.Errorf("parse jettons: %w", err)
	}
	messageAmount, err := tlb.FromTON(opts.msgTON)
	if err != nil {
		return fmt.Errorf("parse message-ton: %w", err)
	}
	expected := new(big.Int).Mul(jettonAmount.Nano(), new(big.Int).SetUint64(uint64(opts.rate)*uint64(batches)))
	if before.Cmp(expected) < 0 {
		return fmt.Errorf("source has %s jettons, need at least %s", tlb.FromNanoTON(before).String(), tlb.FromNanoTON(expected).String())
	}
	state.NextHighloadQueryID = reservedNext
	state.HighloadQueryIDReservedAt = runNow.Unix()
	if err = writeState(opts.statePath, state); err != nil {
		return fmt.Errorf("reserve highload query IDs: %w", err)
	}

	logger.Info().
		Uint64("sender_index", opts.senderIndex).
		Int("rate", opts.rate).
		Dur("duration", opts.duration).
		Uint32("batches", batches).
		Int("transfers", opts.rate*int(batches)).
		Msg("starting jetton transfer load")

	ticker := time.NewTicker(batchPeriod)
	defer ticker.Stop()
	start := time.Now()
	sent := 0
	failedBatches := 0
	for batch := uint32(0); batch < batches; batch++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case scheduled := <-ticker.C:
			messages, buildErr := buildTransferBatch(sourceAddr, opts.rate, opts.recipients, uint64(batch)*uint64(opts.rate), jettonAmount, messageAmount)
			if buildErr != nil {
				return buildErr
			}
			attempts, sendErr := sendHighloadExternal(ctx, client.api, highload, messages, false)
			if sendErr != nil {
				failedBatches++
				logger.Error().
					Err(sendErr).
					Uint32("batch", batch).
					Int("attempts", attempts).
					Msg("submit batch failed")
				continue
			}
			if attempts > 1 {
				logger.Warn().
					Uint32("batch", batch).
					Int("attempts", attempts).
					Msg("batch accepted after retry")
			}
			sent += len(messages)
			if lag := time.Since(scheduled); lag > 100*time.Millisecond {
				logger.Warn().Dur("lag", lag).Uint32("batch", batch).Msg("load sender missed schedule")
			}
		}
	}
	submissionElapsed := time.Since(start)

	accepted, after, waitErr := waitTransferred(ctx, sourceWallet, before, jettonAmount.Nano(), sent, opts.drain)
	logger.Info().
		Int("submitted", sent).
		Int("accepted", accepted).
		Int("failed_batches", failedBatches).
		Float64("submitted_tps", float64(sent)/submissionElapsed.Seconds()).
		Str("balance_before", tlb.FromNanoTON(before).String()).
		Str("balance_after", tlb.FromNanoTON(after).String()).
		Msg("jetton load completed")
	if waitErr != nil {
		return waitErr
	}
	if failedBatches != 0 || accepted != sent {
		return fmt.Errorf("only %d of %d submitted transfers were observed", accepted, sent)
	}
	return nil
}

func newHighloadWallet(api ton.APIClientWrapped, key ed25519.PrivateKey, builder func(context.Context, uint32) (uint32, int64, error)) (*wallet.Wallet, error) {
	result, err := wallet.FromPrivateKeyWithOptions(
		key,
		wallet.ConfigHighloadV3{MessageTTL: highloadTTL, MessageBuilder: builder},
		wallet.WithAPI(api),
		wallet.WithWorkchain(0),
	)
	if err != nil {
		return nil, fmt.Errorf("create highload wallet: %w", err)
	}
	return result, nil
}

// Highload Wallet v3 splits the 23-bit query ID into a 13-bit shift and a
// 10-bit dictionary index. Index 1023 is reserved by the contract, so a plain
// modulo counter periodically produces an external message that must fail.
func normalizeHighloadQueryID(id uint32) uint32 {
	if id&highloadQueryLowMask == highloadQueryLowMask {
		return advanceHighloadQueryID(id)
	}

	return id
}

func currentHighloadQueryID(now time.Time) uint32 {
	return normalizeHighloadQueryID(uint32(now.Unix() % int64(highloadQueryLimit)))
}

func nextHighloadQueryID(state loadState, now time.Time) (uint32, bool) {
	if state.HighloadQueryIDReservedAt <= 0 {
		return currentHighloadQueryID(now), true
	}

	age := now.Unix() - state.HighloadQueryIDReservedAt
	if age < 0 || age > int64(highloadTTL) {
		return currentHighloadQueryID(now), true
	}

	return normalizeHighloadQueryID(state.NextHighloadQueryID), false
}

func advanceHighloadQueryID(id uint32) uint32 {
	id = (id + 1) % highloadQueryLimit
	if id&highloadQueryLowMask == highloadQueryLowMask {
		id = (id + 1) % highloadQueryLimit
	}

	return id
}

// highloadCreatedAt anchors every external in one run to the latest registered
// block of the wallet's shard. Wall-clock time is not a valid substitute on a
// loaded network: the shard can lag behind it, and Highload V3 rejects a
// created_at that is still in the future for the account state used by the
// liteserver admission check.
func highloadCreatedAt(ctx context.Context, api highloadTimeAPI, addr *address.Address) (int64, error) {
	master, err := api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return 0, fmt.Errorf("get masterchain head: %w", err)
	}
	shards, err := api.GetBlockShardsInfo(ctx, master)
	if err != nil {
		return 0, fmt.Errorf("get shard heads: %w", err)
	}
	for _, shardBlock := range shards {
		if shardBlock.Workchain != addr.Workchain() ||
			!tlb.ShardID(uint64(shardBlock.Shard)).ContainsAddress(addr) {
			continue
		}
		header, headerErr := api.GetBlockHeader(ctx, shardBlock)
		if headerErr != nil {
			return 0, fmt.Errorf("get wallet shard header: %w", headerErr)
		}
		if header.GenUtime <= 1 {
			return 0, fmt.Errorf("wallet shard block has invalid generation time %d", header.GenUtime)
		}

		return int64(header.GenUtime) - 1, nil
	}

	return 0, errors.New("wallet shard is absent from the masterchain view")
}

func sendFundingAndWait(ctx context.Context, api ton.APIClientWrapped, source *fundingWallet, message *wallet.Message) error {
	before, err := fundingSeqno(ctx, api, source.addr)
	if err != nil {
		return err
	}
	body, err := source.buildMessage(ctx, []*wallet.Message{message})
	if err != nil {
		return err
	}
	if err = api.SendExternalMessage(ctx, &tlb.ExternalMessage{DstAddr: source.addr, Body: body}); err != nil {
		return err
	}
	ticker := time.NewTicker(accountPollPeriod)
	defer ticker.Stop()
	for {
		seqno, seqErr := fundingSeqno(ctx, api, source.addr)
		if seqErr == nil && seqno > before {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func fundingSeqno(ctx context.Context, api ton.APIClientWrapped, addr *address.Address) (uint32, error) {
	master, err := api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return 0, err
	}
	result, err := api.WaitForBlock(master.SeqNo).RunGetMethod(ctx, master, addr, "seqno")
	if err != nil {
		return 0, err
	}
	seqno, err := result.Int(0)
	if err != nil {
		return 0, err
	}
	return uint32(seqno.Uint64()), nil
}

func accountBalance(ctx context.Context, api ton.APIClientWrapped, addr *address.Address) (*big.Int, error) {
	master, err := api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return nil, err
	}
	account, err := api.WaitForBlock(master.SeqNo).GetAccount(ctx, master, addr)
	if err != nil {
		return nil, err
	}
	if account.State == nil {
		return new(big.Int), nil
	}
	return new(big.Int).Set(account.State.Balance.Nano()), nil
}

func waitAccountBalance(
	ctx context.Context,
	api ton.APIClientWrapped,
	addr *address.Address,
	minimum *big.Int,
) error {
	ticker := time.NewTicker(accountPollPeriod)
	defer ticker.Stop()
	for {
		balance, err := accountBalance(ctx, api, addr)
		if err == nil && balance.Cmp(minimum) >= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func sendHighloadExternal(ctx context.Context, api ton.APIClientWrapped, source *wallet.Wallet, messages []*wallet.Message, withStateInit bool) (int, error) {
	external, err := buildHighloadExternal(ctx, source, messages, withStateInit)
	if err != nil {
		return 0, err
	}

	return submitExternalWithRetry(
		ctx,
		external,
		api.SendExternalMessage,
		waitContext,
		submissionRetryPeriod,
		submissionRetryWindow,
	)
}

func submitExternalWithRetry(
	ctx context.Context,
	external *tlb.ExternalMessage,
	send func(context.Context, *tlb.ExternalMessage) error,
	wait func(context.Context, time.Duration) error,
	retryPeriod time.Duration,
	retryWindow time.Duration,
) (int, error) {
	attempts := 0
	elapsed := time.Duration(0)

	for {
		attempts++
		err := send(ctx, external)
		if err == nil {
			return attempts, nil
		}
		if ctxErr := ctx.Err(); ctxErr != nil {
			return attempts, ctxErr
		}
		if elapsed >= retryWindow {
			return attempts, err
		}

		delay := min(retryPeriod, retryWindow-elapsed)
		if err = wait(ctx, delay); err != nil {
			return attempts, err
		}
		elapsed += delay
	}
}

func buildHighloadExternal(ctx context.Context, source *wallet.Wallet, messages []*wallet.Message, withStateInit bool) (*tlb.ExternalMessage, error) {
	spec := source.GetSpec().(*wallet.SpecHighloadV3)
	body, err := spec.BuildMessage(ctx, messages)
	if err != nil {
		return nil, err
	}
	var stateInit *tlb.StateInit
	if withStateInit {
		stateInit, err = wallet.GetStateInit(
			source.PrivateKey().Public().(ed25519.PublicKey),
			wallet.ConfigHighloadV3{MessageTTL: highloadTTL},
			source.GetSubwalletID(),
		)
		if err != nil {
			return nil, err
		}
	}
	return &tlb.ExternalMessage{
		DstAddr:   source.WalletAddress(),
		StateInit: stateInit,
		Body:      body,
	}, nil
}

func initializeHighloadWallet(
	ctx context.Context,
	timeout time.Duration,
	send func(context.Context) error,
	active func(context.Context) (bool, error),
) error {
	initCtx, cancel := context.WithTimeoutCause(ctx, timeout, errHighloadInitializationTimeout)
	defer cancel()

	return initializeHighloadWalletWithWait(initCtx, timeout, waitContext, send, active)
}

func initializeHighloadWalletWithWait(
	ctx context.Context,
	timeout time.Duration,
	wait func(context.Context, time.Duration) error,
	send func(context.Context) error,
	active func(context.Context) (bool, error),
) error {
	elapsed := time.Duration(0)
	nextResubmit := highloadInitResubmit
	var lastErr error

	if err := ctx.Err(); err != nil {
		return err
	}
	if err := send(ctx); err != nil {
		lastErr = err
	}

	for {
		isActive, activeErr := active(ctx)
		if activeErr == nil && isActive {
			return nil
		}
		if activeErr != nil {
			lastErr = fmt.Errorf("check load wallet activation: %w", activeErr)
		}
		if err := ctx.Err(); err != nil {
			if errors.Is(context.Cause(ctx), errHighloadInitializationTimeout) {
				return highloadInitializationTimeoutError(timeout, lastErr)
			}

			return err
		}
		if elapsed >= timeout {
			return highloadInitializationTimeoutError(timeout, lastErr)
		}
		if elapsed >= nextResubmit {
			if err := send(ctx); err != nil {
				lastErr = err
			}
			nextResubmit += highloadInitResubmit
		}

		poll := min(highloadInitPoll, timeout-elapsed)
		if err := wait(ctx, poll); err != nil {
			if errors.Is(context.Cause(ctx), errHighloadInitializationTimeout) {
				return highloadInitializationTimeoutError(timeout, lastErr)
			}

			return err
		}
		elapsed += poll
	}
}

func highloadInitializationTimeoutError(timeout time.Duration, lastErr error) error {
	if lastErr == nil {
		lastErr = errors.New("load wallet did not become active")
	}

	return fmt.Errorf("load wallet remained inactive after %s: %w", timeout, lastErr)
}

func waitContext(ctx context.Context, delay time.Duration) error {
	timer := time.NewTimer(delay)
	defer timer.Stop()

	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}

func buildMintPayload(owner, admin *address.Address, amount tlb.Coins) (*cell.Cell, error) {
	rest := cell.BeginCell().
		MustStoreAddr(admin).
		MustStoreAddr(address.NewAddressNone()).
		MustStoreBigCoins(big.NewInt(0)).
		MustStoreUInt(0, 1).
		EndCell()
	payload, err := tlb.ToCell(jetton.MintPayload{
		ToAddress: owner,
		Amount:    tlb.MustFromTON("1"),
		MasterMsg: jetton.MintPayloadMasterMsg{
			Opcode:       0x178d4519,
			JettonAmount: amount,
			RestData:     rest,
		},
	})
	if err != nil {
		return nil, fmt.Errorf("build mint payload: %w", err)
	}
	return payload, nil
}

func buildTransferBatch(source *address.Address, count, recipients int, offset uint64, jettons, messageTON tlb.Coins) ([]*wallet.Message, error) {
	messages := make([]*wallet.Message, count)
	for i := range messages {
		recipient := recipientAddress((offset + uint64(i)) % uint64(recipients))
		body, err := jetton.BuildTransferPayload(
			recipient,
			address.NewAddressNone(),
			jettons,
			tlb.ZeroCoins,
			nil,
			nil,
		)
		if err != nil {
			return nil, fmt.Errorf("build transfer %d: %w", i, err)
		}
		messages[i] = wallet.SimpleMessage(source, messageTON, body)
	}
	return messages, nil
}

func recipientAddress(index uint64) *address.Address {
	var encoded [8]byte
	binary.LittleEndian.PutUint64(encoded[:], index)
	hash := sha256.New()
	_, _ = hash.Write([]byte("gton-jetton-load/recipient/v1"))
	_, _ = hash.Write(encoded[:])
	return address.NewAddress(0, 0, hash.Sum(nil))
}

func deriveHighloadKey(seed []byte, senderIndex uint64) ed25519.PrivateKey {
	var encodedIndex [8]byte
	binary.LittleEndian.PutUint64(encodedIndex[:], senderIndex)

	hash := sha256.New()
	_, _ = hash.Write([]byte(highloadKeyDomain))
	_, _ = hash.Write(encodedIndex[:])
	_, _ = hash.Write(seed)
	return ed25519.NewKeyFromSeed(hash.Sum(nil))
}

func validateStateSender(state loadState, senderIndex uint64) error {
	if state.FormatVersion != stateFormatVersion {
		return fmt.Errorf("unsupported state format %d, expected %d", state.FormatVersion, stateFormatVersion)
	}
	if state.SenderIndex != senderIndex {
		return fmt.Errorf("state sender index %d differs from requested sender index %d", state.SenderIndex, senderIndex)
	}

	return nil
}

func contractAddress(code, data *cell.Cell, workchain byte) (*address.Address, error) {
	state, err := tlb.ToCell(&tlb.StateInit{Code: code, Data: data})
	if err != nil {
		return nil, fmt.Errorf("serialize state init: %w", err)
	}
	return address.NewAddress(0, workchain, state.Hash()), nil
}

func loadBOC(path string) (*cell.Cell, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	root, err := cell.FromBOC(data)
	if err != nil {
		return nil, err
	}
	return root, nil
}

func contractMethodReady(ctx context.Context, api ton.APIClientWrapped, addr *address.Address, method string) bool {
	master, err := api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return false
	}
	_, err = api.WaitForBlock(master.SeqNo).RunGetMethod(ctx, master, addr, method)
	return err == nil
}

func waitContractMethod(ctx context.Context, api ton.APIClientWrapped, addr *address.Address, method string) error {
	ticker := time.NewTicker(accountPollPeriod)
	defer ticker.Stop()
	var lastErr error
	for {
		master, err := api.CurrentMasterchainInfo(ctx)
		if err == nil {
			_, err = api.WaitForBlock(master.SeqNo).RunGetMethod(ctx, master, addr, method)
		}
		if err == nil {
			return nil
		}
		lastErr = err
		select {
		case <-ctx.Done():
			return fmt.Errorf("%w: last method error: %v", ctx.Err(), lastErr)
		case <-ticker.C:
		}
	}
}

func activeContract(
	ctx context.Context,
	api ton.APIClientWrapped,
	addr *address.Address,
	expectedCode *cell.Cell,
) (bool, error) {
	master, err := api.CurrentMasterchainInfo(ctx)
	if err != nil {
		return false, err
	}
	account, err := api.WaitForBlock(master.SeqNo).GetAccount(ctx, master, addr)
	if err != nil {
		return false, err
	}
	if !account.IsActive || account.State.Status != tlb.AccountStatusActive {
		return false, nil
	}
	if account.Code == nil || account.Code.HashKey() != expectedCode.HashKey() {
		return false, errors.New("active account code differs from the expected contract")
	}

	return true, nil
}

func waitJettonBalance(ctx context.Context, wallet *jetton.WalletClient, minimum *big.Int) error {
	ticker := time.NewTicker(accountPollPeriod)
	defer ticker.Stop()
	for {
		balance, err := wallet.GetBalance(ctx)
		if err == nil && balance.Cmp(minimum) >= 0 {
			return nil
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}
	}
}

func waitTransferred(ctx context.Context, source *jetton.WalletClient, before, amount *big.Int, submitted int, timeout time.Duration) (int, *big.Int, error) {
	deadline := time.NewTimer(timeout)
	defer deadline.Stop()
	ticker := time.NewTicker(accountPollPeriod)
	defer ticker.Stop()
	after := new(big.Int).Set(before)
	for {
		balance, err := source.GetBalance(ctx)
		if err == nil {
			after = balance
			spent := new(big.Int).Sub(before, after)
			accepted := int(new(big.Int).Quo(spent, amount).Int64())
			if accepted >= submitted {
				return accepted, after, nil
			}
		}
		select {
		case <-ctx.Done():
			return 0, after, ctx.Err()
		case <-deadline.C:
			spent := new(big.Int).Sub(before, after)
			accepted := int(new(big.Int).Quo(spent, amount).Int64())
			return accepted, after, fmt.Errorf("drain timeout after observing %d of %d transfers", accepted, submitted)
		case <-ticker.C:
		}
	}
}

func readState(path string) (loadState, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return loadState{}, fmt.Errorf("read state %s: %w", path, err)
	}
	var state loadState
	if err = json.Unmarshal(data, &state); err != nil {
		return loadState{}, fmt.Errorf("parse state %s: %w", path, err)
	}
	switch state.FormatVersion {
	case legacyStateFormat:
		state.FormatVersion = stateFormatVersion
		state.HighloadQueryIDReservedAt = 0
		state.HighloadMessageTTL = previousHighloadTTL
	case previousStateFormat:
		state.FormatVersion = stateFormatVersion
		state.HighloadMessageTTL = previousHighloadTTL
	}
	return state, nil
}

func writeState(path string, state loadState) error {
	data, err := json.MarshalIndent(state, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	file, err := os.CreateTemp(dir, ".jetton-load-*.tmp")
	if err != nil {
		return fmt.Errorf("create state temp file: %w", err)
	}
	tmp := file.Name()
	defer func() { _ = os.Remove(tmp) }()
	if err = file.Chmod(0o600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	if closeErr := file.Close(); err == nil {
		err = closeErr
	}
	if err != nil {
		return fmt.Errorf("write state: %w", err)
	}
	if err = os.Rename(tmp, path); err != nil {
		return fmt.Errorf("replace state %s: %w", path, err)
	}
	return nil
}

func displayAddress(addr *address.Address) string {
	return addr.Bounce(false).Testnet(true).String()
}
