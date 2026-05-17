package liteserver_test

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/liteclient"
	"github.com/xssnick/tonutils-go/tl"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/ton"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

const (
	liteServerE2EDefaultAddr    = "88.99.58.187:7445"
	liteServerE2EDefaultKey     = "xc95u573l7/BnAJYEyRHKXarl1iAvBw/ucC3GE2XnZY="
	liteServerE2EReferenceAddr  = "5.9.10.47:19949"
	liteServerE2EReferenceKey   = "n4VDnSCUuSpjnCyUk9e3QOOd6o0ItSWYbTnW3Wnn8wk="
	globalConfigURL             = "https://ton-blockchain.github.io/global.config.json"
	liteServerE2ERequestTimeout = 20 * time.Second

	masterchainID    int32 = -1
	masterchainShard int64 = -1 << 63
)

var (
	liteServerE2EAccountAddr        = address.MustParseAddr("EQCxE6mUtQJKFnGfaROTKOt1lZbDiiX1kCixRv7Nw2Id_sDs")
	liteServerE2EMasterAccountAddr  = address.MustParseAddr("Ef9VVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVVbxn")
	liteServerE2ERunMethodAddr      = address.MustParseAddr("EQBL2_3lMiyywU17g-or8N7v9hDmPCpttzBPE2isF2GTzpK4")
	liteServerE2EMissingLibraryHash = make([]byte, 32)
)

func TestLiveLiteServerE2E(t *testing.T) {
	if os.Getenv("RUN_LITESERVER_E2E") == "" && os.Getenv("RUN_TON_INTEGRATION") == "" {
		t.Skip("set RUN_LITESERVER_E2E=1 or RUN_TON_INTEGRATION=1 to run live liteserver e2e")
	}

	addr := envOrDefault("LITESERVER_E2E_ADDR", liteServerE2EDefaultAddr)
	key := envOrDefault("LITESERVER_E2E_KEY", liteServerE2EDefaultKey)
	refAddr := envOrDefault("LITESERVER_E2E_REFERENCE_ADDR", liteServerE2EReferenceAddr)
	refKey := envOrDefault("LITESERVER_E2E_REFERENCE_KEY", liteServerE2EReferenceKey)

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Minute)
	defer cancel()

	cfg, err := loadGlobalConfig(ctx)
	if err != nil {
		t.Fatalf("load global config for trusted block: %v", err)
	}

	refClient := liteclient.NewConnectionPool()
	defer refClient.Stop()

	refConnectCtx, refConnectCancel := context.WithTimeout(ctx, 30*time.Second)
	err = refClient.AddConnection(refConnectCtx, refAddr, refKey)
	refConnectCancel()
	if err != nil {
		t.Fatalf("connect to reference liteserver %s: %v", refAddr, err)
	}

	client := liteclient.NewConnectionPool()
	defer client.Stop()

	connectCtx, connectCancel := context.WithTimeout(ctx, 20*time.Second)
	err = client.AddConnection(connectCtx, addr, key)
	connectCancel()
	if err != nil {
		t.Fatalf("connect to liteserver %s: %v", addr, err)
	}

	refCtx := refClient.StickyContext(ctx)
	ctx = client.StickyContext(ctx)
	api := ton.NewAPIClient(client, ton.ProofCheckPolicyFast).WithRetryTimeout(3, 5*time.Second)
	refAPI := ton.NewAPIClient(refClient, ton.ProofCheckPolicyFast).WithRetryTimeout(3, 5*time.Second)
	suite := newLiteServerE2ESuite(t, ctx, refCtx, client, refClient, api, refAPI, cfg)

	version := stepValue(suite, "getVersion raw", func() (ton.Version, error) {
		refVersion, err := queryAs[ton.Version](refCtx, refAPI.Client(), ton.GetVersion{})
		if err != nil {
			return ton.Version{}, fmt.Errorf("reference getVersion: %w", err)
		}

		version, err := queryAs[ton.Version](ctx, api.Client(), ton.GetVersion{})
		if err != nil {
			return ton.Version{}, err
		}
		if version.Version == 0 || version.Now == 0 {
			return ton.Version{}, fmt.Errorf("invalid version response: %+v", version)
		}
		if refVersion.Version == 0 || refVersion.Now == 0 {
			return ton.Version{}, fmt.Errorf("invalid reference version response: %+v", refVersion)
		}
		return version, nil
	})
	t.Logf("liteserver version=%d capabilities=%d now=%d", version.Version, version.Capabilities, version.Now)

	suite.step("getTime", func() error {
		refNow, err := refAPI.GetTime(refCtx)
		if err != nil {
			return fmt.Errorf("reference get time: %w", err)
		}

		now, err := api.GetTime(ctx)
		if err != nil {
			return err
		}
		if now == 0 {
			return fmt.Errorf("empty liteserver time")
		}
		if refNow > now+60 || now > refNow+60 {
			return fmt.Errorf("time mismatch with reference: reference=%d got=%d", refNow, now)
		}
		return nil
	})

	masterExt := stepValue(suite, "getMasterchainInfoExt raw", func() (ton.MasterchainInfoExt, error) {
		refInfo, err := queryAs[ton.MasterchainInfoExt](refCtx, refAPI.Client(), ton.GetMasterchainInfoExt{Mode: 0})
		if err != nil {
			return ton.MasterchainInfoExt{}, fmt.Errorf("reference getMasterchainInfoExt: %w", err)
		}

		info, err := queryAs[ton.MasterchainInfoExt](ctx, api.Client(), ton.GetMasterchainInfoExt{Mode: 0})
		if err != nil {
			return ton.MasterchainInfoExt{}, err
		}
		if info.Last == nil || info.Last.SeqNo == 0 {
			return ton.MasterchainInfoExt{}, fmt.Errorf("invalid masterchain ext response: %+v", info)
		}
		if len(info.StateRootHash) != 32 || info.Init == nil {
			return ton.MasterchainInfoExt{}, fmt.Errorf("missing state root or zerostate in masterchain ext response")
		}
		if refInfo.Init == nil || !bytes.Equal(refInfo.Init.RootHash, info.Init.RootHash) || !bytes.Equal(refInfo.Init.FileHash, info.Init.FileHash) {
			return ton.MasterchainInfoExt{}, fmt.Errorf("zerostate mismatch with reference")
		}
		if refInfo.Last != nil && info.Last.SeqNo <= refInfo.Last.SeqNo {
			refLookup, err := refAPI.LookupBlock(refCtx, info.Last.Workchain, info.Last.Shard, info.Last.SeqNo)
			if err != nil {
				return ton.MasterchainInfoExt{}, fmt.Errorf("reference lookup of our ext master block: %w", err)
			}
			if err := compareBlockID("masterchain info ext last", refLookup, info.Last); err != nil {
				return ton.MasterchainInfoExt{}, err
			}
		}
		return info, nil
	})

	master := stepValue(suite, "getMasterchainInfo", func() (*ton.BlockIDExt, error) {
		refCurrent, err := refAPI.GetMasterchainInfo(refCtx)
		if err != nil {
			return nil, fmt.Errorf("reference getMasterchainInfo: %w", err)
		}

		master, err := api.GetMasterchainInfo(ctx)
		if err != nil {
			return nil, err
		}
		if !isFullBlock(master) {
			return nil, fmt.Errorf("invalid master block id: %+v", master)
		}
		if master.SeqNo <= refCurrent.SeqNo {
			refLookup, err := refAPI.LookupBlock(refCtx, master.Workchain, master.Shard, master.SeqNo)
			if err != nil {
				return nil, fmt.Errorf("reference lookup of our master block: %w", err)
			}
			if err := compareBlockID("masterchain info", refLookup, master); err != nil {
				return nil, err
			}
		}
		return master, nil
	})
	if master == nil {
		suite.finish()
		return
	}
	if masterExt.Last != nil {
		t.Logf("masterchain latest=%s ext_latest=%s", blockRef(master), blockRef(masterExt.Last))
	}

	if lag := envUint32("LITESERVER_E2E_MASTER_LAG", 32); lag > 0 && master.SeqNo > lag {
		targetSeq := master.SeqNo - lag
		master = stepValue(suite, "lookupBlock lagged master by seqno against reference", func() (*ton.BlockIDExt, error) {
			return compareReferenceValue(
				func() (*ton.BlockIDExt, error) {
					return refAPI.LookupBlock(refCtx, masterchainID, masterchainShard, targetSeq)
				},
				func() (*ton.BlockIDExt, error) {
					return api.LookupBlock(ctx, masterchainID, masterchainShard, targetSeq)
				},
				func(ref *ton.BlockIDExt, got *ton.BlockIDExt) error {
					return compareBlockID("lagged master", ref, got)
				},
			)
		})
		if master == nil {
			suite.finish()
			return
		}
		t.Logf("using lagged masterchain block=%s", blockRef(master))
	}

	suite.step("waitMasterchainSeqno + getMasterchainInfo", func() error {
		waited, err := api.WaitForBlock(master.SeqNo).GetMasterchainInfo(ctx)
		if err != nil {
			return err
		}
		if waited.SeqNo < master.SeqNo {
			return fmt.Errorf("waited master seqno %d is lower than requested %d", waited.SeqNo, master.SeqNo)
		}
		refLookup, err := refAPI.LookupBlock(refCtx, waited.Workchain, waited.Shard, waited.SeqNo)
		if err != nil {
			return fmt.Errorf("reference lookup of waited block: %w", err)
		}
		if err := compareBlockID("waited master", refLookup, waited); err != nil {
			return err
		}
		return nil
	})

	suite.step("getBlockHeader master", func() error {
		return compareReference(
			func() (*tlb.BlockHeader, error) {
				return refAPI.WaitForBlock(master.SeqNo).GetBlockHeader(refCtx, master)
			},
			func() (*tlb.BlockHeader, error) {
				return api.WaitForBlock(master.SeqNo).GetBlockHeader(ctx, master)
			},
			func(ref *tlb.BlockHeader, got *tlb.BlockHeader) error {
				if ref.SeqNo != got.SeqNo || got.SeqNo != master.SeqNo {
					return fmt.Errorf("header seqno mismatch: reference=%d got=%d want=%d", ref.SeqNo, got.SeqNo, master.SeqNo)
				}
				return nil
			},
		)
	})

	suite.step("getBlock master", func() error {
		return compareReference(
			func() (*cell.Cell, error) {
				return refAPI.WaitForBlock(master.SeqNo).GetBlockDataAsCell(refCtx, master)
			},
			func() (*cell.Cell, error) {
				return api.WaitForBlock(master.SeqNo).GetBlockDataAsCell(ctx, master)
			},
			func(ref *cell.Cell, got *cell.Cell) error {
				if !bytes.Equal(ref.Hash(), got.Hash()) {
					return fmt.Errorf("master block root mismatch: reference=%x got=%x", ref.Hash(), got.Hash())
				}
				return nil
			},
		)
	})

	suite.step("lookupBlock master by seqno", func() error {
		return compareReference(
			func() (*ton.BlockIDExt, error) {
				return refAPI.LookupBlock(refCtx, masterchainID, masterchainShard, master.SeqNo)
			},
			func() (*ton.BlockIDExt, error) {
				return api.LookupBlock(ctx, masterchainID, masterchainShard, master.SeqNo)
			},
			func(ref *ton.BlockIDExt, got *ton.BlockIDExt) error {
				if err := compareBlockID("lookup master reference", ref, got); err != nil {
					return err
				}
				return compareBlockID("lookup master target", master, got)
			},
		)
	})

	suite.step("getState zerostate raw", func() error {
		return compareReference(
			func() (ton.BlockState, error) {
				return queryAs[ton.BlockState](refCtx, refAPI.Client(), ton.GetState{ID: zeroStateBlock(masterExt.Init)})
			},
			func() (ton.BlockState, error) {
				return queryAs[ton.BlockState](ctx, api.Client(), ton.GetState{ID: zeroStateBlock(masterExt.Init)})
			},
			func(ref ton.BlockState, got ton.BlockState) error {
				if err := compareBlockID("zerostate id", ref.ID, got.ID); err != nil {
					return err
				}
				if !bytes.Equal(ref.RootHash, got.RootHash) || !bytes.Equal(ref.FileHash, got.FileHash) || !bytes.Equal(ref.Data, got.Data) {
					return fmt.Errorf("zerostate mismatch with reference")
				}
				return nil
			},
		)
	})

	suite.step("getBlockProof master self", func() error {
		return compareReference(
			func() (*ton.PartialBlockProof, error) {
				return refAPI.GetBlockProof(refCtx, master, master)
			},
			func() (*ton.PartialBlockProof, error) {
				return api.GetBlockProof(ctx, master, master)
			},
			func(ref *ton.PartialBlockProof, got *ton.PartialBlockProof) error {
				if ref.Complete != got.Complete || len(ref.Steps) != len(got.Steps) {
					return fmt.Errorf("block proof shape mismatch: reference complete=%v steps=%d got complete=%v steps=%d", ref.Complete, len(ref.Steps), got.Complete, len(got.Steps))
				}
				if err := compareBlockID("block proof from", ref.From, got.From); err != nil {
					return err
				}
				return compareBlockID("block proof to", ref.To, got.To)
			},
		)
	})

	suite.step("getBlockchainConfig all", func() error {
		return compareReference(
			func() (*tlb.BlockchainConfig, error) {
				return refAPI.WaitForBlock(master.SeqNo).GetBlockchainConfig(refCtx, master)
			},
			func() (*tlb.BlockchainConfig, error) {
				return api.WaitForBlock(master.SeqNo).GetBlockchainConfig(ctx, master)
			},
			compareConfig,
		)
	})

	suite.step("getBlockchainConfig param 8", func() error {
		return compareReference(
			func() (*tlb.BlockchainConfig, error) {
				return refAPI.WaitForBlock(master.SeqNo).GetBlockchainConfig(refCtx, master, 8)
			},
			func() (*tlb.BlockchainConfig, error) {
				return api.WaitForBlock(master.SeqNo).GetBlockchainConfig(ctx, master, 8)
			},
			compareConfig,
		)
	})

	shards := stepValue(suite, "getAllShardsInfo", func() ([]*ton.BlockIDExt, error) {
		return compareReferenceValue(
			func() ([]*ton.BlockIDExt, error) {
				return refAPI.WaitForBlock(master.SeqNo).GetBlockShardsInfo(refCtx, master)
			},
			func() ([]*ton.BlockIDExt, error) {
				return api.WaitForBlock(master.SeqNo).GetBlockShardsInfo(ctx, master)
			},
			func(ref []*ton.BlockIDExt, got []*ton.BlockIDExt) error {
				if len(ref) == 0 {
					return fmt.Errorf("reference master block has no shard refs")
				}
				return compareBlockList("all shards info", ref, got)
			},
		)
	})

	shard := pickShard(shards)
	if shard != nil {
		t.Logf("selected shard block=%s", blockRef(shard))

		suite.step("getBlockHeader shard", func() error {
			return compareReference(
				func() (*tlb.BlockHeader, error) {
					return refAPI.GetBlockHeader(refCtx, shard)
				},
				func() (*tlb.BlockHeader, error) {
					return api.GetBlockHeader(ctx, shard)
				},
				func(ref *tlb.BlockHeader, got *tlb.BlockHeader) error {
					if ref.SeqNo != got.SeqNo || got.SeqNo != shard.SeqNo {
						return fmt.Errorf("shard header seqno mismatch: reference=%d got=%d want=%d", ref.SeqNo, got.SeqNo, shard.SeqNo)
					}
					return nil
				},
			)
		})

		suite.step("getBlock shard", func() error {
			return compareReference(
				func() (*cell.Cell, error) {
					return refAPI.GetBlockDataAsCell(refCtx, shard)
				},
				func() (*cell.Cell, error) {
					return api.GetBlockDataAsCell(ctx, shard)
				},
				func(ref *cell.Cell, got *cell.Cell) error {
					if !bytes.Equal(ref.Hash(), got.Hash()) {
						return fmt.Errorf("shard block root mismatch: reference=%x got=%x", ref.Hash(), got.Hash())
					}
					return nil
				},
			)
		})

		suite.step("getShardBlockProof raw", func() error {
			return compareReference(
				func() (ton.ShardBlockProof, error) {
					return queryAs[ton.ShardBlockProof](refCtx, refAPI.Client(), ton.GetShardBlockProof{ID: shard})
				},
				func() (ton.ShardBlockProof, error) {
					return queryAs[ton.ShardBlockProof](ctx, api.Client(), ton.GetShardBlockProof{ID: shard})
				},
				func(ref ton.ShardBlockProof, got ton.ShardBlockProof) error {
					if err := compareBlockID("shard proof master", ref.MasterchainID, got.MasterchainID); err != nil {
						return err
					}
					if len(ref.Links) != len(got.Links) {
						return fmt.Errorf("shard proof links mismatch: reference=%d got=%d", len(ref.Links), len(got.Links))
					}
					return nil
				},
			)
		})

		suite.step("getShardInfo raw", func() error {
			req := ton.GetShardInfo{
				ID:        master,
				Workchain: shard.Workchain,
				Shard:     shard.Shard,
				Exact:     true,
			}
			return compareReference(
				func() (ton.ShardInfo, error) {
					return queryAs[ton.ShardInfo](refCtx, refAPI.Client(), req)
				},
				func() (ton.ShardInfo, error) {
					return queryAs[ton.ShardInfo](ctx, api.Client(), req)
				},
				func(ref ton.ShardInfo, got ton.ShardInfo) error {
					if err := compareBlockID("shard info block", ref.ShardBlock, got.ShardBlock); err != nil {
						return err
					}
					if !bytes.Equal(cellHash(ref.ShardDescription), cellHash(got.ShardDescription)) {
						return fmt.Errorf("shard description hash mismatch")
					}
					return nil
				},
			)
		})

		suite.step("lookupBlockWithProof shard by seqno raw", func() error {
			req := ton.LookupBlockWithProof{
				Mode: 1,
				ID: &ton.BlockInfoShort{
					Workchain: shard.Workchain,
					Shard:     shard.Shard,
					Seqno:     int32(shard.SeqNo),
				},
				MCBlockID: master,
			}
			return compareReference(
				func() (ton.LookupBlockResult, error) {
					return queryAs[ton.LookupBlockResult](refCtx, refAPI.Client(), req)
				},
				func() (ton.LookupBlockResult, error) {
					return queryAs[ton.LookupBlockResult](ctx, api.Client(), req)
				},
				func(ref ton.LookupBlockResult, got ton.LookupBlockResult) error {
					if err := compareBlockID("lookup with proof id", ref.ID, got.ID); err != nil {
						return err
					}
					if len(ref.Header) > 0 && len(got.Header) == 0 {
						return fmt.Errorf("missing lookup header proof")
					}
					return nil
				},
			)
		})
	}

	referenceAccount := referenceValue(t, "basechain account state", func() (*tlb.Account, error) {
		return refAPI.WaitForBlock(master.SeqNo).GetAccount(refCtx, master, liteServerE2EAccountAddr)
	})

	referenceTxList := referenceValue(t, "basechain account transactions", func() (ton.TransactionList, error) {
		return queryAs[ton.TransactionList](refCtx, refAPI.Client(), ton.GetTransactions{
			Limit: 3,
			AccID: &ton.AccountID{
				Workchain: liteServerE2EAccountAddr.Workchain(),
				ID:        liteServerE2EAccountAddr.Data(),
			},
			LT:     int64(referenceAccount.LastTxLT),
			TxHash: referenceAccount.LastTxHash,
		})
	})
	var referenceTxBlock *ton.BlockIDExt
	if len(referenceTxList.IDs) > 0 {
		referenceTxBlock = referenceTxList.IDs[0]
	}

	account := stepValue(suite, "getAccountState basechain", func() (*tlb.Account, error) {
		account, err := api.WaitForBlock(master.SeqNo).GetAccount(ctx, master, liteServerE2EAccountAddr)
		if err != nil {
			return nil, err
		}
		if err := compareAccounts(referenceAccount, account); err != nil {
			return nil, err
		}
		return account, nil
	})
	if account == nil {
		account = referenceAccount
	}

	suite.step("getAccountState masterchain", func() error {
		return compareReference(
			func() (*tlb.Account, error) {
				return refAPI.WaitForBlock(master.SeqNo).GetAccount(refCtx, master, liteServerE2EMasterAccountAddr)
			},
			func() (*tlb.Account, error) {
				return api.WaitForBlock(master.SeqNo).GetAccount(ctx, master, liteServerE2EMasterAccountAddr)
			},
			compareAccounts,
		)
	})

	suite.step("getAccountStatePruned raw", func() error {
		req := ton.GetAccountStatePruned{
			ID: master,
			Account: ton.AccountID{
				Workchain: liteServerE2EAccountAddr.Workchain(),
				ID:        liteServerE2EAccountAddr.Data(),
			},
		}
		return compareReference(
			func() (ton.AccountState, error) {
				return queryAs[ton.AccountState](refCtx, refAPI.Client(), req)
			},
			func() (ton.AccountState, error) {
				return queryAs[ton.AccountState](ctx, api.Client(), req)
			},
			compareAccountState,
		)
	})

	txBlock := stepValue(suite, "getTransactions account raw", func() (*ton.BlockIDExt, error) {
		if account == nil {
			return nil, fmt.Errorf("account flow did not produce an account")
		}
		req := ton.GetTransactions{
			Limit: 3,
			AccID: &ton.AccountID{
				Workchain: liteServerE2EAccountAddr.Workchain(),
				ID:        liteServerE2EAccountAddr.Data(),
			},
			LT:     int64(account.LastTxLT),
			TxHash: account.LastTxHash,
		}
		list, err := queryAs[ton.TransactionList](ctx, api.Client(), req)
		if err != nil {
			return nil, err
		}
		if err := compareTransactionList(referenceTxList, list); err != nil {
			return nil, err
		}
		return list.IDs[0], nil
	})
	if txBlock == nil {
		txBlock = referenceTxBlock
	}

	suite.step("listTransactions verified", func() error {
		if account == nil {
			return fmt.Errorf("account flow did not produce an account")
		}
		return compareReference(
			func() ([]*tlb.Transaction, error) {
				return refAPI.ListTransactions(refCtx, liteServerE2EAccountAddr, 3, account.LastTxLT, account.LastTxHash)
			},
			func() ([]*tlb.Transaction, error) {
				return api.ListTransactions(ctx, liteServerE2EAccountAddr, 3, account.LastTxLT, account.LastTxHash)
			},
			compareTransactions,
		)
	})

	suite.step("getOneTransaction verified", func() error {
		if account == nil || txBlock == nil {
			return fmt.Errorf("account transaction flow did not produce transaction block")
		}
		return compareReference(
			func() (*tlb.Transaction, error) {
				return refAPI.GetTransaction(refCtx, txBlock, liteServerE2EAccountAddr, account.LastTxLT)
			},
			func() (*tlb.Transaction, error) {
				return api.GetTransaction(ctx, txBlock, liteServerE2EAccountAddr, account.LastTxLT)
			},
			func(ref *tlb.Transaction, got *tlb.Transaction) error {
				if ref.LT != got.LT || !bytes.Equal(ref.Hash, got.Hash) || !bytes.Equal(ref.AccountAddr, got.AccountAddr) {
					return fmt.Errorf("transaction mismatch")
				}
				return nil
			},
		)
	})

	if txBlock != nil {
		suite.step("listBlockTransactions verified", func() error {
			refTxs, refMore, refErr := refAPI.GetBlockTransactionsV2(refCtx, txBlock, 4)
			gotTxs, gotMore, gotErr := api.GetBlockTransactionsV2(ctx, txBlock, 4)
			if err := compareCallOutcome(refErr, gotErr); err != nil {
				return err
			}
			if refErr != nil {
				return nil
			}
			if refMore != gotMore {
				return fmt.Errorf("block transaction pagination flag mismatch: reference=%v got=%v", refMore, gotMore)
			}
			return compareShortTransactions(refTxs, gotTxs)
		})

		suite.step("listBlockTransactionsExt raw", func() error {
			req := ton.ListBlockTransactionsExt{
				ID:        txBlock,
				Mode:      1 << 5,
				Count:     4,
				WantProof: &ton.True{},
			}
			return compareReference(
				func() (ton.BlockTransactionsExt, error) {
					return queryAs[ton.BlockTransactionsExt](refCtx, refAPI.Client(), req)
				},
				func() (ton.BlockTransactionsExt, error) {
					return queryAs[ton.BlockTransactionsExt](ctx, api.Client(), req)
				},
				compareBlockTransactionsExt,
			)
		})
	}

	suite.step("runSmcMethod verified", func() error {
		c1 := cell.BeginCell().MustStoreUInt(0xAA, 8).EndCell().MustBeginParse()
		c2 := cell.BeginCell().MustStoreUInt(0xBB, 8).EndCell()

		return compareReference(
			func() (*ton.ExecutionResult, error) {
				return refAPI.WaitForBlock(master.SeqNo).RunGetMethod(refCtx, master, liteServerE2ERunMethodAddr, "clltst2", c1, c2)
			},
			func() (*ton.ExecutionResult, error) {
				return api.WaitForBlock(master.SeqNo).RunGetMethod(ctx, master, liteServerE2ERunMethodAddr, "clltst2", c1, c2)
			},
			func(ref *ton.ExecutionResult, got *ton.ExecutionResult) error {
				refSlice, err := ref.Slice(0)
				if err != nil {
					return fmt.Errorf("reference first stack value: %w", err)
				}
				gotSlice, err := got.Slice(0)
				if err != nil {
					return err
				}
				if !bytes.Equal(refSlice.MustToCell().Hash(), gotSlice.MustToCell().Hash()) {
					return fmt.Errorf("first returned stack value mismatch")
				}

				refCell, err := ref.Cell(1)
				if err != nil {
					return fmt.Errorf("reference second stack value: %w", err)
				}
				gotCell, err := got.Cell(1)
				if err != nil {
					return err
				}
				if !bytes.Equal(refCell.Hash(), gotCell.Hash()) {
					return fmt.Errorf("second returned stack value mismatch")
				}
				return nil
			},
		)
	})

	suite.step("getLibraries missing hash", func() error {
		return compareReference(
			func() ([]*cell.Cell, error) {
				return refAPI.GetLibraries(refCtx, liteServerE2EMissingLibraryHash)
			},
			func() ([]*cell.Cell, error) {
				return api.GetLibraries(ctx, liteServerE2EMissingLibraryHash)
			},
			compareLibraries,
		)
	})

	suite.step("getLibrariesWithProof raw", func() error {
		req := ton.GetLibrariesWithProof{
			ID:          master,
			Mode:        0,
			LibraryList: [][]byte{liteServerE2EMissingLibraryHash},
		}
		return compareReference(
			func() (ton.LibraryResultWithProof, error) {
				return queryAs[ton.LibraryResultWithProof](refCtx, refAPI.Client(), req)
			},
			func() (ton.LibraryResultWithProof, error) {
				return queryAs[ton.LibraryResultWithProof](ctx, api.Client(), req)
			},
			compareLibraryResultWithProof,
		)
	})

	suite.step("getValidatorStats raw", func() error {
		req := ton.GetValidatorStats{
			Mode:  0,
			ID:    master,
			Limit: 1,
		}
		return compareReference(
			func() (ton.ValidatorStats, error) {
				return queryAs[ton.ValidatorStats](refCtx, refAPI.Client(), req)
			},
			func() (ton.ValidatorStats, error) {
				return queryAs[ton.ValidatorStats](ctx, api.Client(), req)
			},
			compareValidatorStats,
		)
	})

	suite.step("getOutMsgQueueSizes", func() error {
		return compareReference(
			func() (*ton.OutMsgQueueSizes, error) {
				return refAPI.GetOutMsgQueueSizes(refCtx, nil, nil)
			},
			func() (*ton.OutMsgQueueSizes, error) {
				return api.GetOutMsgQueueSizes(ctx, nil, nil)
			},
			compareOutMsgQueueSizes,
		)
	})

	if shard != nil {
		suite.step("getOutMsgQueueSizes filtered", func() error {
			wc := shard.Workchain
			shardID := shard.Shard
			return compareReference(
				func() (*ton.OutMsgQueueSizes, error) {
					return refAPI.GetOutMsgQueueSizes(refCtx, &wc, &shardID)
				},
				func() (*ton.OutMsgQueueSizes, error) {
					return api.GetOutMsgQueueSizes(ctx, &wc, &shardID)
				},
				compareOutMsgQueueSizes,
			)
		})
	}

	suite.step("getBlockOutMsgQueueSize verified", func() error {
		return compareReference(
			func() (*ton.BlockOutMsgQueueSize, error) {
				return refAPI.GetBlockOutMsgQueueSize(refCtx, master)
			},
			func() (*ton.BlockOutMsgQueueSize, error) {
				return api.GetBlockOutMsgQueueSize(ctx, master)
			},
			compareBlockOutMsgQueueSize,
		)
	})

	suite.step("sendMessage invalid external message is rejected", func() error {
		req := ton.SendMessage{Body: cell.BeginCell().EndCell().ToBOCWithFlags(false)}
		_, refErr := queryAs[ton.SendMessageStatus](refCtx, refAPI.Client(), req)
		_, gotErr := queryAs[ton.SendMessageStatus](ctx, api.Client(), req)
		return compareCallOutcome(refErr, gotErr)
	})

	suite.step("secure proof policy getMasterchainInfo from config", func() error {
		refSecure := ton.NewAPIClient(refClient, ton.ProofCheckPolicySecure).WithRetryTimeout(3, 5*time.Second)
		refSecure.SetTrustedBlockFromConfig(cfg)
		secure := ton.NewAPIClient(client, ton.ProofCheckPolicySecure).WithRetryTimeout(3, 5*time.Second)
		secure.SetTrustedBlockFromConfig(cfg)

		refBlock, refErr := refSecure.GetMasterchainInfo(refCtx)
		block, err := secure.GetMasterchainInfo(ctx)
		if cmpErr := compareCallOutcome(refErr, err); cmpErr != nil {
			return cmpErr
		}
		if refErr != nil {
			return nil
		}
		if !isFullBlock(block) {
			return fmt.Errorf("secure master block is invalid: %+v", block)
		}
		if refBlock != nil && block.SeqNo <= refBlock.SeqNo {
			refLookup, err := refAPI.LookupBlock(refCtx, block.Workchain, block.Shard, block.SeqNo)
			if err != nil {
				return fmt.Errorf("reference lookup of secure master block: %w", err)
			}
			return compareBlockID("secure master", refLookup, block)
		}
		return nil
	})

	suite.step("getDispatchQueueInfo compared with reference", func() error {
		return compareReference(
			func() (*ton.DispatchQueueInfo, error) {
				return refAPI.GetDispatchQueueInfo(refCtx, master, nil, 1)
			},
			func() (*ton.DispatchQueueInfo, error) {
				return api.GetDispatchQueueInfo(ctx, master, nil, 1)
			},
			func(ref *ton.DispatchQueueInfo, got *ton.DispatchQueueInfo) error {
				if ref.Complete != got.Complete || len(ref.AccountDispatchQueues) != len(got.AccountDispatchQueues) {
					return fmt.Errorf("dispatch queue info mismatch: reference complete=%v count=%d got complete=%v count=%d", ref.Complete, len(ref.AccountDispatchQueues), got.Complete, len(got.AccountDispatchQueues))
				}
				return compareBlockID("dispatch queue info id", ref.ID, got.ID)
			},
		)
	})

	suite.step("getDispatchQueueMessages compared with reference", func() error {
		return compareReference(
			func() (*ton.DispatchQueueMessages, error) {
				return refAPI.GetDispatchQueueMessages(refCtx, master, liteServerE2EAccountAddr, 0, 1)
			},
			func() (*ton.DispatchQueueMessages, error) {
				return api.GetDispatchQueueMessages(ctx, master, liteServerE2EAccountAddr, 0, 1)
			},
			func(ref *ton.DispatchQueueMessages, got *ton.DispatchQueueMessages) error {
				if ref.Complete != got.Complete || len(ref.Messages) != len(got.Messages) {
					return fmt.Errorf("dispatch queue messages mismatch: reference complete=%v count=%d got complete=%v count=%d", ref.Complete, len(ref.Messages), got.Complete, len(got.Messages))
				}
				return compareBlockID("dispatch queue messages id", ref.ID, got.ID)
			},
		)
	})

	suite.step("nonfinal methods compared with reference", func() error {
		refGroups, refErr := refAPI.GetAllNonfinalValidatorGroups(refCtx)
		gotGroups, gotErr := api.GetAllNonfinalValidatorGroups(ctx)
		if err := compareCallOutcome(refErr, gotErr); err != nil {
			return fmt.Errorf("getAllNonfinalValidatorGroups: %w", err)
		}
		if refErr == nil && len(refGroups.Groups) != len(gotGroups.Groups) {
			return fmt.Errorf("nonfinal validator groups mismatch: reference=%d got=%d", len(refGroups.Groups), len(gotGroups.Groups))
		}

		refPending, refErr := refAPI.GetAllNonfinalPendingShardBlocks(refCtx)
		gotPending, gotErr := api.GetAllNonfinalPendingShardBlocks(ctx)
		if err := compareCallOutcome(refErr, gotErr); err != nil {
			return fmt.Errorf("getAllNonfinalPendingShardBlocks: %w", err)
		}
		if refErr == nil && (len(refPending.SignedBlocks) != len(gotPending.SignedBlocks) || len(refPending.Candidates) != len(gotPending.Candidates)) {
			return fmt.Errorf("nonfinal pending shard blocks mismatch")
		}
		return nil
	})

	suite.finish()
}

type liteServerE2ESuite struct {
	t         *testing.T
	ctx       context.Context
	refCtx    context.Context
	client    ton.LiteClient
	refClient ton.LiteClient
	api       ton.APIClientWrapped
	refAPI    ton.APIClientWrapped
	cfg       *liteclient.GlobalConfig
	failures  []string
}

func newLiteServerE2ESuite(
	t *testing.T,
	ctx context.Context,
	refCtx context.Context,
	client ton.LiteClient,
	refClient ton.LiteClient,
	api ton.APIClientWrapped,
	refAPI ton.APIClientWrapped,
	cfg *liteclient.GlobalConfig,
) *liteServerE2ESuite {
	return &liteServerE2ESuite{
		t:         t,
		ctx:       ctx,
		refCtx:    refCtx,
		client:    client,
		refClient: refClient,
		api:       api,
		refAPI:    refAPI,
		cfg:       cfg,
	}
}

func (s *liteServerE2ESuite) step(name string, fn func() error) {
	s.t.Run(name, func(t *testing.T) {
		if err := fn(); err != nil {
			s.failures = append(s.failures, fmt.Sprintf("%s: %v", name, err))
			t.Errorf("%s: %v", name, err)
		}
	})
}

func stepValue[T any](s *liteServerE2ESuite, name string, fn func() (T, error)) T {
	var value T
	s.step(name, func() error {
		got, err := fn()
		if err != nil {
			return err
		}
		value = got
		return nil
	})
	return value
}

func referenceValue[T any](t *testing.T, name string, fn func() (T, error)) T {
	t.Helper()

	value, err := fn()
	if err != nil {
		var zero T
		t.Fatalf("reference %s failed: %v", name, err)
		return zero
	}
	return value
}

func compareReference[T any](refFn func() (T, error), gotFn func() (T, error), cmp func(ref T, got T) error) error {
	_, err := compareReferenceValue(refFn, gotFn, cmp)
	return err
}

func compareReferenceValue[T any](refFn func() (T, error), gotFn func() (T, error), cmp func(ref T, got T) error) (T, error) {
	var zero T

	ref, refErr := refFn()
	got, gotErr := gotFn()
	if err := compareCallOutcome(refErr, gotErr); err != nil {
		return zero, err
	}
	if refErr != nil {
		return got, nil
	}
	if err := cmp(ref, got); err != nil {
		return zero, err
	}
	return got, nil
}

func compareCallOutcome(refErr error, gotErr error) error {
	if refErr != nil && isReferenceUnavailable(refErr) {
		return nil
	}

	switch {
	case refErr == nil && gotErr == nil:
		return nil
	case refErr == nil:
		return fmt.Errorf("reference succeeded, our liteserver failed: %w", gotErr)
	case gotErr == nil:
		return fmt.Errorf("reference failed (%v), our liteserver succeeded", refErr)
	}

	refCode, refIsLSError := liteServerErrorCode(refErr)
	gotCode, gotIsLSError := liteServerErrorCode(gotErr)
	if refIsLSError && gotIsLSError && refCode == gotCode {
		return nil
	}

	return fmt.Errorf("reference error %v, our liteserver error %v", refErr, gotErr)
}

func isReferenceUnavailable(err error) bool {
	if errors.Is(err, context.DeadlineExceeded) {
		return true
	}

	text := err.Error()
	if strings.Contains(text, "deadline exceeded") ||
		strings.Contains(text, "timeout error received") ||
		strings.Contains(text, "backend node timeout") ||
		strings.Contains(text, "not in db") ||
		strings.Contains(text, "no more active nodes left") ||
		strings.Contains(text, "no active connections") ||
		strings.Contains(text, "unknown request type") ||
		strings.Contains(text, "unknown query") ||
		strings.Contains(text, "query is not allowed") {
		return true
	}
	return false
}

func liteServerErrorCode(err error) (int32, bool) {
	var lsErr ton.LSError
	if errors.As(err, &lsErr) {
		return lsErr.Code, true
	}
	return 0, false
}

func (s *liteServerE2ESuite) finish() {
	if len(s.failures) == 0 {
		return
	}

	s.t.Fatalf("non-working liteserver e2e checks:\n - %s", strings.Join(s.failures, "\n - "))
}

func queryAs[T any](ctx context.Context, client ton.LiteClient, request tl.Serializable) (T, error) {
	var zero T

	ctx, cancel := context.WithTimeout(ctx, liteServerE2ERequestTimeout)
	defer cancel()

	var response tl.Serializable
	if err := client.QueryLiteserver(ctx, request, &response); err != nil {
		return zero, err
	}

	if lsErr, ok := response.(ton.LSError); ok {
		return zero, lsErr
	}

	got, ok := response.(T)
	if !ok {
		return zero, fmt.Errorf("unexpected response type %T", response)
	}
	return got, nil
}

func compareBlockID(name string, ref *ton.BlockIDExt, got *ton.BlockIDExt) error {
	if ref == nil || got == nil {
		if ref == got {
			return nil
		}
		return fmt.Errorf("%s block mismatch: reference=%s got=%s", name, blockRef(ref), blockRef(got))
	}
	if !ref.Equals(got) {
		return fmt.Errorf("%s block mismatch: reference=%s got=%s", name, blockRef(ref), blockRef(got))
	}
	return nil
}

func compareBlockList(name string, ref []*ton.BlockIDExt, got []*ton.BlockIDExt) error {
	if len(ref) != len(got) {
		return fmt.Errorf("%s block count mismatch: reference=%d got=%d", name, len(ref), len(got))
	}

	seen := map[string]struct{}{}
	for _, block := range ref {
		seen[blockKey(block)] = struct{}{}
	}
	for _, block := range got {
		if _, ok := seen[blockKey(block)]; !ok {
			return fmt.Errorf("%s contains unexpected block %s", name, blockRef(block))
		}
	}
	return nil
}

func compareConfig(ref *tlb.BlockchainConfig, got *tlb.BlockchainConfig) error {
	refHash, err := configHash(ref)
	if err != nil {
		return fmt.Errorf("reference config hash: %w", err)
	}
	gotHash, err := configHash(got)
	if err != nil {
		return fmt.Errorf("our config hash: %w", err)
	}
	if !bytes.Equal(refHash, gotHash) {
		return fmt.Errorf("config root hash mismatch: reference=%x got=%x", refHash, gotHash)
	}
	return nil
}

func configHash(cfg *tlb.BlockchainConfig) ([]byte, error) {
	if cfg == nil {
		return nil, fmt.Errorf("nil config")
	}

	root, err := cfg.ToCell()
	if err != nil {
		return nil, err
	}
	return root.Hash(), nil
}

func compareAccounts(ref *tlb.Account, got *tlb.Account) error {
	if ref == nil || got == nil {
		if ref == got {
			return nil
		}
		return fmt.Errorf("account mismatch: reference_nil=%v got_nil=%v", ref == nil, got == nil)
	}
	if ref.IsActive != got.IsActive {
		return fmt.Errorf("account active mismatch: reference=%v got=%v", ref.IsActive, got.IsActive)
	}
	if ref.LastTxLT != got.LastTxLT || !bytes.Equal(ref.LastTxHash, got.LastTxHash) {
		return fmt.Errorf("account last tx mismatch: reference=%d/%x got=%d/%x", ref.LastTxLT, ref.LastTxHash, got.LastTxLT, got.LastTxHash)
	}
	if ref.State != nil && got.State != nil && ref.State.Status != got.State.Status {
		return fmt.Errorf("account status mismatch: reference=%s got=%s", ref.State.Status, got.State.Status)
	}
	if !bytes.Equal(cellHash(ref.Code), cellHash(got.Code)) {
		return fmt.Errorf("account code hash mismatch")
	}
	if !bytes.Equal(cellHash(ref.Data), cellHash(got.Data)) {
		return fmt.Errorf("account data hash mismatch")
	}
	return nil
}

func compareAccountState(ref ton.AccountState, got ton.AccountState) error {
	if err := compareBlockID("account state master", ref.ID, got.ID); err != nil {
		return err
	}
	if err := compareBlockID("account state shard", ref.Shard, got.Shard); err != nil {
		return err
	}
	if !bytes.Equal(cellHash(ref.State), cellHash(got.State)) {
		return fmt.Errorf("account state hash mismatch")
	}
	if len(ref.Proof) > 0 && len(got.Proof) == 0 {
		return fmt.Errorf("missing account proof")
	}
	if len(ref.ShardProof) > 0 && len(got.ShardProof) == 0 {
		return fmt.Errorf("missing shard proof")
	}
	return nil
}

func compareTransactionList(ref ton.TransactionList, got ton.TransactionList) error {
	if err := compareBlockList("transaction list block ids", ref.IDs, got.IDs); err != nil {
		return err
	}

	refHashes, err := bocRootHashes(ref.Transactions)
	if err != nil {
		return fmt.Errorf("reference transaction boc: %w", err)
	}
	gotHashes, err := bocRootHashes(got.Transactions)
	if err != nil {
		return fmt.Errorf("our transaction boc: %w", err)
	}
	return compareHashList("transaction boc roots", refHashes, gotHashes)
}

func compareTransactions(ref []*tlb.Transaction, got []*tlb.Transaction) error {
	if len(ref) != len(got) {
		return fmt.Errorf("transaction count mismatch: reference=%d got=%d", len(ref), len(got))
	}
	for i := range ref {
		if ref[i].LT != got[i].LT || !bytes.Equal(ref[i].Hash, got[i].Hash) || !bytes.Equal(ref[i].AccountAddr, got[i].AccountAddr) {
			return fmt.Errorf("transaction %d mismatch", i)
		}
	}
	return nil
}

func compareShortTransactions(ref []ton.TransactionShortInfo, got []ton.TransactionShortInfo) error {
	if len(ref) != len(got) {
		return fmt.Errorf("short transaction count mismatch: reference=%d got=%d", len(ref), len(got))
	}
	for i := range ref {
		if ref[i].LT != got[i].LT || !bytes.Equal(ref[i].Hash, got[i].Hash) || !bytes.Equal(ref[i].Account, got[i].Account) {
			return fmt.Errorf("short transaction %d mismatch", i)
		}
	}
	return nil
}

func compareBlockTransactionsExt(ref ton.BlockTransactionsExt, got ton.BlockTransactionsExt) error {
	if err := compareBlockID("block transactions ext", ref.ID, got.ID); err != nil {
		return err
	}
	if ref.Incomplete != got.Incomplete {
		return fmt.Errorf("block transactions ext incomplete mismatch: reference=%v got=%v", ref.Incomplete, got.Incomplete)
	}
	refHashes := cellsHashes(ref.Transactions)
	gotHashes := cellsHashes(got.Transactions)
	return compareHashList("block transactions ext roots", refHashes, gotHashes)
}

func compareLibraries(ref []*cell.Cell, got []*cell.Cell) error {
	if len(ref) != len(got) {
		return fmt.Errorf("library count mismatch: reference=%d got=%d", len(ref), len(got))
	}
	for i := range ref {
		if !bytes.Equal(cellHash(ref[i]), cellHash(got[i])) {
			return fmt.Errorf("library %d hash mismatch", i)
		}
	}
	return nil
}

func compareLibraryResultWithProof(ref ton.LibraryResultWithProof, got ton.LibraryResultWithProof) error {
	if err := compareBlockID("library proof id", ref.ID, got.ID); err != nil {
		return err
	}
	if len(ref.Result) != len(got.Result) {
		return fmt.Errorf("library proof result count mismatch: reference=%d got=%d", len(ref.Result), len(got.Result))
	}
	if len(ref.StateProof) > 0 && len(got.StateProof) == 0 {
		return fmt.Errorf("missing library state proof")
	}
	if len(ref.DataProof) > 0 && len(got.DataProof) == 0 {
		return fmt.Errorf("missing library data proof")
	}
	return nil
}

func compareValidatorStats(ref ton.ValidatorStats, got ton.ValidatorStats) error {
	if err := compareBlockID("validator stats id", ref.ID, got.ID); err != nil {
		return err
	}
	if ref.Count != got.Count || ref.Complete != got.Complete {
		return fmt.Errorf("validator stats mismatch: reference=%d/%v got=%d/%v", ref.Count, ref.Complete, got.Count, got.Complete)
	}
	if len(ref.StateProof) > 0 && len(got.StateProof) == 0 {
		return fmt.Errorf("missing validator stats state proof")
	}
	if len(ref.DataProof) > 0 && len(got.DataProof) == 0 {
		return fmt.Errorf("missing validator stats data proof")
	}
	return nil
}

func compareOutMsgQueueSizes(ref *ton.OutMsgQueueSizes, got *ton.OutMsgQueueSizes) error {
	if ref == nil || got == nil {
		return fmt.Errorf("nil out msg queue sizes response")
	}
	if ref.ExtMsgQueueSizeLimit != got.ExtMsgQueueSizeLimit {
		return fmt.Errorf("out msg queue size limit mismatch: reference=%d got=%d", ref.ExtMsgQueueSizeLimit, got.ExtMsgQueueSizeLimit)
	}
	if len(ref.Shards) > 0 && len(got.Shards) == 0 {
		return fmt.Errorf("reference returned queue shards, our response is empty")
	}
	for _, shard := range got.Shards {
		if !isFullBlock(shard.ID) {
			return fmt.Errorf("invalid out msg queue block id: %s", blockRef(shard.ID))
		}
		if shard.Size < 0 {
			return fmt.Errorf("negative out msg queue size: %d", shard.Size)
		}
	}
	return nil
}

func compareBlockOutMsgQueueSize(ref *ton.BlockOutMsgQueueSize, got *ton.BlockOutMsgQueueSize) error {
	if ref == nil || got == nil {
		return fmt.Errorf("nil block out msg queue size response")
	}
	if err := compareBlockID("block out msg queue id", ref.ID, got.ID); err != nil {
		return err
	}
	if ref.Size != got.Size {
		return fmt.Errorf("block out msg queue size mismatch: reference=%d got=%d", ref.Size, got.Size)
	}
	if len(ref.Proof) > 0 && len(got.Proof) == 0 {
		return fmt.Errorf("missing block out msg queue proof")
	}
	return nil
}

func compareHashList(name string, ref [][]byte, got [][]byte) error {
	if len(ref) != len(got) {
		return fmt.Errorf("%s count mismatch: reference=%d got=%d", name, len(ref), len(got))
	}
	for i := range ref {
		if !bytes.Equal(ref[i], got[i]) {
			return fmt.Errorf("%s[%d] mismatch: reference=%x got=%x", name, i, ref[i], got[i])
		}
	}
	return nil
}

func bocRootHashes(data []byte) ([][]byte, error) {
	if len(data) == 0 {
		return nil, nil
	}

	roots, err := cell.FromBOCMultiRoot(data)
	if err != nil {
		return nil, err
	}
	return cellsHashes(roots), nil
}

func cellsHashes(cells []*cell.Cell) [][]byte {
	hashes := make([][]byte, 0, len(cells))
	for _, c := range cells {
		hashes = append(hashes, cellHash(c))
	}
	return hashes
}

func cellHash(c *cell.Cell) []byte {
	if c == nil {
		return nil
	}
	return c.Hash()
}

func blockKey(block *ton.BlockIDExt) string {
	if block == nil {
		return "<nil>"
	}
	return fmt.Sprintf("%d:%d:%d:%x:%x", block.Workchain, block.Shard, block.SeqNo, block.RootHash, block.FileHash)
}

func expectLSError(fn func() error) error {
	err := fn()
	if err == nil {
		return fmt.Errorf("query succeeded")
	}

	var lsErr ton.LSError
	if !errors.As(err, &lsErr) {
		return err
	}
	return nil
}

func loadGlobalConfig(ctx context.Context) (*liteclient.GlobalConfig, error) {
	if path := os.Getenv("LITESERVER_E2E_GLOBAL_CONFIG"); path != "" {
		return liteclient.GetConfigFromFile(path)
	}

	url := envOrDefault("LITESERVER_E2E_GLOBAL_CONFIG_URL", globalConfigURL)
	return liteclient.GetConfigFromUrl(ctx, url)
}

func zeroStateBlock(zero *ton.ZeroStateIDExt) *ton.BlockIDExt {
	if zero == nil {
		return nil
	}

	return &ton.BlockIDExt{
		Workchain: zero.Workchain,
		Shard:     masterchainShard,
		SeqNo:     0,
		RootHash:  append([]byte(nil), zero.RootHash...),
		FileHash:  append([]byte(nil), zero.FileHash...),
	}
}

func pickShard(shards []*ton.BlockIDExt) *ton.BlockIDExt {
	for _, shard := range shards {
		if shard != nil && shard.Workchain == 0 {
			return shard
		}
	}
	for _, shard := range shards {
		if shard != nil {
			return shard
		}
	}
	return nil
}

func isFullBlock(block *ton.BlockIDExt) bool {
	return block != nil && block.Shard != 0 && len(block.RootHash) == 32 && len(block.FileHash) == 32
}

func blockRef(block *ton.BlockIDExt) string {
	if block == nil {
		return "<nil>"
	}
	return fmt.Sprintf("(%d,%016x,%d):%x:%x", block.Workchain, uint64(block.Shard), block.SeqNo, block.RootHash, block.FileHash)
}

func envOrDefault(name string, fallback string) string {
	if value := os.Getenv(name); value != "" {
		return value
	}
	return fallback
}

func envUint32(name string, fallback uint32) uint32 {
	value := os.Getenv(name)
	if value == "" {
		return fallback
	}

	parsed, err := strconv.ParseUint(value, 10, 32)
	if err != nil {
		return fallback
	}
	return uint32(parsed)
}
