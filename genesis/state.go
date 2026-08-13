package genesis

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"math/big"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	rootShard         = int64(-1 << 63)
	walletBalanceNano = uint64(4_999_990_000_000_000_000)
	configBalanceNano = uint64(10_000_000_000)
	walletV1CodeHex   = "b5ee9c7201010101004e000098ff0020dd2082014c97ba9730ed44d0d70b1fe0a4f260810200d71820d70b1fed44d0d31fd3ffd15112baf2a122f901541044f910f2a2f80001d31f31d307d4d101fb00a4c8cb1fcbffc9ed54"
)

type builtGenesis struct {
	master stateArtifact
	base   stateArtifact
	wallet *address.Address
}

type stateArtifact struct {
	block ton.BlockIDExt
	root  *cell.Cell
	boc   []byte
}

func buildStates(spec validatedSpec, genesisTime uint32) (builtGenesis, error) {
	base, err := buildBaseState(spec.spec.GlobalID, genesisTime)
	if err != nil {
		return builtGenesis{}, err
	}

	walletState, wallet, err := buildWalletAccount(spec.validators[0].publicKey, genesisTime)
	if err != nil {
		return builtGenesis{}, err
	}

	config, validatorHash, err := buildConfigRoot(spec, genesisTime, base.block.RootHash, base.block.FileHash)
	if err != nil {
		return builtGenesis{}, err
	}
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		return builtGenesis{}, err
	}
	shardAccount, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       walletState,
		LastTransHash: make([]byte, 32),
	})
	if err != nil {
		return builtGenesis{}, err
	}
	if err = accounts.SetIntKey(new(big.Int).SetBytes(walletAddress[:]), shardAccount); err != nil {
		return builtGenesis{}, fmt.Errorf("insert genesis wallet: %w", err)
	}
	configAccount, err := buildConfigAccount(config, genesisTime)
	if err != nil {
		return builtGenesis{}, err
	}
	configShardAccount, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       configAccount,
		LastTransHash: make([]byte, 32),
	})
	if err != nil {
		return builtGenesis{}, err
	}
	if err = accounts.SetIntKey(new(big.Int).SetBytes(configAddress[:]), configShardAccount); err != nil {
		return builtGenesis{}, fmt.Errorf("insert genesis config account: %w", err)
	}

	totalBalance := walletBalanceNano + configBalanceNano
	stats, err := buildStateStats(totalBalance)
	if err != nil {
		return builtGenesis{}, err
	}
	queue, err := buildEmptyOutQueue()
	if err != nil {
		return builtGenesis{}, err
	}
	previous, err := cell.NewAugDict(32, keyMaxLTAugmentation{})
	if err != nil {
		return builtGenesis{}, err
	}
	info, err := tlb.ToCell(&tlb.McStateExtraBlockInfo{
		ValidatorInfo: tlb.ValidatorInfo{
			ValidatorListHashShort: validatorHash,
			CatchainSeqno:          0,
			NextCCUpdated:          true,
		},
		PrevBlocks:    &tlb.OldMcBlocksInfoAugDict{AugmentedDictionary: previous},
		AfterKeyBlock: true,
	})
	if err != nil {
		return builtGenesis{}, err
	}
	extra, err := tlb.ToCell(&tlb.McStateExtra{
		ShardHashes: cell.NewDict(32),
		ConfigParams: tlb.ConfigParams{
			ConfigAddr: configAddress[:],
			Config: struct {
				Params *cell.Dictionary `tlb:"dict inline 32"`
			}{Params: config},
		},
		Info:          info,
		GlobalBalance: currency(totalBalance),
	})
	if err != nil {
		return builtGenesis{}, err
	}

	masterRoot, err := tlb.ToCell(&tlb.ShardStateUnsplit{
		GlobalID:        spec.spec.GlobalID,
		ShardIdent:      tlb.ShardIdent{PrefixBits: 0, WorkchainID: -1, ShardPrefix: 0},
		GenUTime:        genesisTime,
		MinRefMCSeqno:   ^uint32(0),
		OutMsgQueueInfo: queue,
		Accounts: struct {
			ShardAccounts *tlb.ShardAccountsAugDict `tlb:"."`
		}{ShardAccounts: accounts},
		Stats:        stats,
		McStateExtra: extra,
	})
	if err != nil {
		return builtGenesis{}, fmt.Errorf("serialize masterchain zerostate: %w", err)
	}
	master := makeStateArtifact(-1, masterRoot)

	return builtGenesis{master: master, base: base, wallet: wallet}, nil
}

func buildBaseState(globalID int32, genesisTime uint32) (stateArtifact, error) {
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		return stateArtifact{}, err
	}
	stats, err := buildStateStats(0)
	if err != nil {
		return stateArtifact{}, err
	}
	queue, err := buildEmptyOutQueue()
	if err != nil {
		return stateArtifact{}, err
	}

	root, err := tlb.ToCell(&tlb.ShardStateUnsplit{
		GlobalID: globalID,
		// ShardID's top marker bit belongs to the external BlockIDExt encoding.
		// A root ShardIdent has an empty prefix, so its stored prefix is zero.
		ShardIdent:      tlb.ShardIdent{PrefixBits: 0, WorkchainID: 0, ShardPrefix: 0},
		GenUTime:        genesisTime,
		MinRefMCSeqno:   ^uint32(0),
		OutMsgQueueInfo: queue,
		Accounts: struct {
			ShardAccounts *tlb.ShardAccountsAugDict `tlb:"."`
		}{ShardAccounts: accounts},
		Stats: stats,
	})
	if err != nil {
		return stateArtifact{}, fmt.Errorf("serialize basechain zerostate: %w", err)
	}
	return makeStateArtifact(0, root), nil
}

func buildWalletAccount(publicKey [32]byte, genesisTime uint32) (*cell.Cell, *address.Address, error) {
	codeBOC, err := hex.DecodeString(walletV1CodeHex)
	if err != nil {
		return nil, nil, err
	}
	code, err := cell.FromBOC(codeBOC)
	if err != nil {
		return nil, nil, fmt.Errorf("parse embedded Wallet V1 code: %w", err)
	}
	data := cell.BeginCell().MustStoreUInt(0, 32).MustStoreSlice(publicKey[:], 256).EndCell()
	stateInit := &tlb.StateInit{Code: code, Data: data, Lib: cell.NewDict(256)}
	storage := tlb.AccountStorage{
		Status:          tlb.AccountStatusActive,
		Balance:         nanoCoins(walletBalanceNano),
		ExtraCurrencies: cell.NewDict(32),
		StateInit:       stateInit,
	}
	storageCell, err := storage.ToCell()
	if err != nil {
		return nil, nil, err
	}
	cells, bits := cellTreeSize(storageCell)
	wallet := address.NewAddress(0, 0xff, walletAddress[:]).Bounce(true).Testnet(true)
	account, err := (tlb.AccountState{
		IsValid: true,
		Address: wallet,
		StorageInfo: tlb.StorageInfo{
			StorageUsed:  tlb.StorageUsed{CellsUsed: new(big.Int).SetUint64(cells), BitsUsed: new(big.Int).SetUint64(bits)},
			StorageExtra: tlb.StorageExtraNone{},
			LastPaid:     genesisTime,
		},
		AccountStorage: storage,
	}).ToCell()
	if err != nil {
		return nil, nil, err
	}
	return account, wallet, nil
}

func buildConfigAccount(config *cell.Dictionary, genesisTime uint32) (*cell.Cell, error) {
	data := cell.BeginCell().MustStoreRef(config.AsCell()).EndCell()
	storage := tlb.AccountStorage{
		Status:          tlb.AccountStatusActive,
		Balance:         nanoCoins(configBalanceNano),
		ExtraCurrencies: cell.NewDict(32),
		StateInit:       &tlb.StateInit{Data: data, Lib: cell.NewDict(256)},
	}
	storageCell, err := storage.ToCell()
	if err != nil {
		return nil, err
	}
	cells, bits := cellTreeSize(storageCell)

	return (tlb.AccountState{
		IsValid: true,
		Address: address.NewAddress(0, 0xff, configAddress[:]),
		StorageInfo: tlb.StorageInfo{
			StorageUsed:  tlb.StorageUsed{CellsUsed: new(big.Int).SetUint64(cells), BitsUsed: new(big.Int).SetUint64(bits)},
			StorageExtra: tlb.StorageExtraNone{},
			LastPaid:     genesisTime,
		},
		AccountStorage: storage,
	}).ToCell()
}

func buildEmptyOutQueue() (*cell.Cell, error) {
	out, err := tlb.NewOutMsgQueueAugDict()
	if err != nil {
		return nil, err
	}
	return (tlb.OutMsgQueueInfo{OutQueue: out, ProcInfo: cell.NewDict(96)}).ToCell()
}

func buildStateStats(balance uint64) (*cell.Cell, error) {
	return (tlb.ShardStateStats{
		TotalBalance:       currency(balance),
		TotalValidatorFees: currency(0),
		Libraries:          cell.NewDict(256),
	}).ToCell()
}

func currency(coins uint64) tlb.CurrencyCollection {
	return tlb.CurrencyCollection{Coins: nanoCoins(coins), ExtraCurrencies: cell.NewDict(32)}
}

func makeStateArtifact(workchain int32, root *cell.Cell) stateArtifact {
	boc := root.ToBOCWithOptions(cell.BOCSerializeOptions{WithCRC32C: false})
	fileHash := sha256.Sum256(boc)
	return stateArtifact{
		block: ton.BlockIDExt{
			Workchain: workchain,
			Shard:     rootShard,
			SeqNo:     0,
			RootHash:  bytes.Clone(root.Hash()),
			FileHash:  bytes.Clone(fileHash[:]),
		},
		root: root,
		boc:  boc,
	}
}

func cellTreeSize(root *cell.Cell) (uint64, uint64) {
	seen := make(map[cell.Hash]struct{})
	stack := []*cell.Cell{root}
	var cells uint64
	var bits uint64
	for len(stack) > 0 {
		current := stack[len(stack)-1]
		stack = stack[:len(stack)-1]
		hash := current.HashKey()
		if _, exists := seen[hash]; exists {
			continue
		}
		seen[hash] = struct{}{}
		cells++
		bits += uint64(current.BitsSize())
		slice := current.MustBeginParse()
		for slice.RefsNum() > 0 {
			stack = append(stack, slice.MustLoadRef().MustToCell())
		}
	}
	return cells, bits
}

type keyMaxLTAugmentation struct{}

func (keyMaxLTAugmentation) SkipExtra(loader *cell.Slice) error {
	return loader.SkipBits(65)
}

func (keyMaxLTAugmentation) EmptyExtra(dst *cell.Builder) error {
	return dst.StoreUInt(0, 65)
}

func (keyMaxLTAugmentation) LeafExtra(*cell.Slice, *cell.Builder) error {
	return fmt.Errorf("old masterchain blocks augmentation is empty-only")
}

func (keyMaxLTAugmentation) CombineExtra(*cell.Slice, *cell.Slice, *cell.Builder) error {
	return fmt.Errorf("old masterchain blocks augmentation is empty-only")
}
