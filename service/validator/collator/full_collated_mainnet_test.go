package collator

import (
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// The collated round trip over real mainnet traffic, as a test rather than only
// as a benchmark.
//
// This is the workload that caught a produce-side defect no synthetic profile
// could: the accounts in the fixture carry storage-stat dictionaries and several
// of them transact many times in one block, while every hand-built profile in
// this package makes accounts with no storage extra at all — for those,
// bindInitialStorageStat returns immediately on the verify side and the whole
// account-storage half of full collated data is never exercised. The collator
// emitted an account storage proof covering only what the account's first
// transaction read, and a replay of its second transaction walked into a pruned
// branch: "dict has special cells in tree structure".
//
// Which is to say: a validator — including this one — could not verify the
// blocks this node produced. The round trip below is what makes that a failing
// test instead of a production incident, so the assertions guarding the shape of
// the fixture matter as much as the verification itself. A fixture that quietly
// stopped containing multi-transaction storage-stat accounts would pass while
// proving nothing.
func TestFullCollatedMainnetCandidateVerifies(t *testing.T) {
	// Runs in the shared-fixture parallel batch: it reads the cached mainnet
	// workload and keeps every mutation on its own copy, holds no package-level
	// counter and derives nothing from wall-clock timing.
	t.Parallel()
	for _, tc := range []struct {
		name   string
		repeat int
	}{
		{name: "single", repeat: 1},
		{name: "heavy", repeat: benchMainnetHeavyRepeat},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.repeat > 1 && testing.Short() {
				t.Skip("heavy mainnet collation is slow")
			}
			req := benchMainnetCollatedRequest(t, tc.repeat)
			candidate, err := testBuilder().BuildShard(context.Background(), req)
			if err != nil {
				t.Fatalf("collate mainnet candidate: %v", err)
			}
			assertBenchMainnetCollated(t, candidate)
			assertMainnetStorageStatWorkload(t, req, candidate)

			verification := shardVerificationRequest(req, candidate)
			verification.NeighborShardEndLT = req.NeighborShardEndLT
			verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
			if err = VerifyShardCandidate(context.Background(), verification); err != nil {
				t.Fatalf("verify collated mainnet candidate: %v", err)
			}
		})
	}
}

// assertMainnetStorageStatWorkload fails unless the candidate really contains
// the shape the bug needed: an account that carries a storage-stat dictionary
// and runs more than one transaction in the block. It also counts the storage
// proofs the collated data carries, because an account proof that stopped being
// emitted would make the replay compute the dictionary itself and pass — a green
// test over an untested path.
func assertMainnetStorageStatWorkload(tb testing.TB, req ShardRequest, candidate *Candidate) {
	tb.Helper()

	blockRoot, err := cell.FromBOC(candidate.BlockBOC)
	if err != nil {
		tb.Fatalf("parse candidate block: %v", err)
	}
	var block tlb.Block
	if err = parseExact(&block, blockRoot); err != nil {
		tb.Fatalf("decode candidate block: %v", err)
	}
	transactions, err := block.ListTransactions()
	if err != nil {
		tb.Fatalf("list candidate transactions: %v", err)
	}
	counts := make(map[[32]byte]int, len(transactions))
	for _, transaction := range transactions {
		if len(transaction.AccountAddr) != 32 {
			tb.Fatalf("transaction %x has a malformed account address", transaction.Hash)
		}
		var key [32]byte
		copy(key[:], transaction.AccountAddr)
		counts[key]++
	}

	previous := loadPreviousShardState(tb, req)
	var withStorageStat, multiTransaction int
	for key, count := range counts {
		account, exists, loadErr := semanticLoadAccount(previous.Accounts.ShardAccounts, key)
		if loadErr != nil {
			tb.Fatalf("load predecessor account %x: %v", key, loadErr)
		}
		if !exists {
			// Created by this very block, so it had no storage stat to prove.
			continue
		}
		prepared, prepareErr := tvm.PrepareAccount(account, address.NewAddress(0, 0, key[:]))
		if prepareErr != nil {
			tb.Fatalf("prepare predecessor account %x: %v", key, prepareErr)
		}
		if _, ok := prepared.State().StorageInfo.StorageExtra.(tlb.StorageExtraInfo); !ok {
			continue
		}
		withStorageStat++
		if count > 1 {
			multiTransaction++
		}
	}
	if multiTransaction == 0 {
		tb.Fatalf("fixture no longer exercises the defect: %d accounts carry a storage stat, "+
			"none of them transacts more than once", withStorageStat)
	}

	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		tb.Fatalf("parse collated data: %v", err)
	}
	var proofs int
	for _, root := range roots {
		loader := root.MustBeginParse()
		if loader.BitsLeft() < 32 {
			continue
		}
		if tag, tagErr := loader.LoadUInt(32); tagErr == nil && tag == accountStorageDictProofTag {
			proofs++
		}
	}
	if proofs != withStorageStat {
		tb.Fatalf("collated data carries %d account storage proofs, want one per storage-stat account (%d)",
			proofs, withStorageStat)
	}
	tb.Logf("mainnet collated workload: %d accounts with a storage stat, %d of them transact more than once, "+
		"%d storage proofs in %d collated bytes",
		withStorageStat, multiTransaction, proofs, len(candidate.CollatedData))
}
