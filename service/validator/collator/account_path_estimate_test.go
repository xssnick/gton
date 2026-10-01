package collator

import (
	"errors"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

func TestAccountPathSizeEstimatorUnionsSharedSpines(t *testing.T) {
	left := cell.BeginCell().MustStoreUInt(1, 8).EndCell()
	right := cell.BeginCell().MustStoreUInt(2, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(3, 8).MustStoreRef(left).MustStoreRef(right).EndCell()

	var estimate accountPathSizeEstimator
	estimate.addLoadedCell(root)
	estimate.addLoadedCell(left)
	first := estimate.bytes
	wantFirst := uint64(2*(12+8/8+3) + 40)
	if first != wantFirst {
		t.Fatalf("first path estimate = %d, want %d", first, wantFirst)
	}

	estimate.addLoadedCell(root)
	estimate.addLoadedCell(left)
	if estimate.bytes != first {
		t.Fatalf("repeated path estimate = %d, want unchanged %d", estimate.bytes, first)
	}

	estimate.addLoadedCell(root)
	estimate.addLoadedCell(right)
	wantUnion := uint64(3 * (12 + 8/8 + 3))
	if estimate.bytes != wantUnion {
		t.Fatalf("two-path union estimate = %d, want %d", estimate.bytes, wantUnion)
	}

	estimate.reset()
	estimate.addLoadedCell(root)
	estimate.addLoadedCell(right)
	if estimate.bytes != wantFirst {
		t.Fatalf("next-window path estimate = %d, want %d", estimate.bytes, wantFirst)
	}
}

func TestAccountPathEstimateCoversFinalDictionaryProof(t *testing.T) {
	code := externalAcceptCode(t)
	contracts := make([]activeContract, 32)
	for i := range contracts {
		contracts[i] = activeContract{
			address: benchAddress("path-proof", i),
			code:    code,
			balance: 1_000_000_000 + uint64(i),
		}
	}

	tests := []struct {
		name    string
		addr    *activeContract
		value   *cell.Cell
		deleted bool
	}{
		{
			name: "replace",
			addr: &activeContract{
				address: contracts[0].address,
				code:    code,
				balance: contracts[0].balance + 1_000_000,
			},
		},
		{
			name: "insert",
			addr: &activeContract{
				address: benchAddress("path-proof-new", 0),
				code:    code,
				balance: 1_000_000_000,
			},
		},
		{
			name:    "delete",
			addr:    &contracts[1],
			deleted: true,
		},
	}
	for i := range tests {
		if !tests[i].deleted {
			tests[i].value = activeShardAccount(t, *tests[i].addr, 1)
		}
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			accounts := activeContracts(t, 1, contracts...)
			usage := cell.NewReadSet(accounts.RootCell())
			traced := &tlb.ShardAccountsAugDict{
				AugmentedDictionary: accounts.Copy().SetTrace(usage.Trace()),
			}
			// usage is both the trace on the predecessor dictionary and the
			// collation's record, as it is in a real build: the lane tracer
			// forwards to c.usage, so the two must be the same recorder for the
			// descent to land where the proof is measured.
			c := &collation{
				shard: msgpool.ShardIdent{Workchain: 0, Shard: 1 << 63},
				usage: usage,
				accountSources: [2]predecessorAccountSource{{
					shard:    tlb.ShardIdent{WorkchainID: 0},
					accounts: traced,
				}},
			}
			c.prepareAccountPathRecorder()

			var key [32]byte
			copy(key[:], test.addr.address.Data())
			lane := &accountLane{key: key}
			lane.tracer = newLaneTracer(c, lane)
			value, path, err := c.loadPredecessorAccount(lane.tracer, key)
			existed := err == nil
			if err != nil && !errors.Is(err, cell.ErrNoSuchKeyInDict) {
				t.Fatalf("load account path: %v", err)
			}

			status := newBlockLimitStatus(blockLimits{}, 0, usage, 0, 0)
			var oldAccount *cell.Cell
			var newAccount *cell.Cell
			if existed {
				var old tlb.ShardAccount
				if err = loadExactSlice(&old, &value); err != nil {
					t.Fatalf("decode old account: %v", err)
				}
				oldAccount = old.Account
			}
			if test.value != nil {
				var next tlb.ShardAccount
				if err = parseExact(&next, test.value); err != nil {
					t.Fatalf("decode new account: %v", err)
				}
				newAccount = next.Account
				if err = status.addProof(newAccount); err != nil {
					t.Fatalf("charge new account: %v", err)
				}
			}
			status.addAccountPath(path, existed, test.deleted, oldAccount)
			status.commitAccountPaths()
			estimated := status.estimatedBytes() - 2000

			updated := accounts.Copy()
			keyCell := cell.BeginCell().MustStoreSlice(key[:], 256).EndCell()
			if test.deleted {
				err = updated.Delete(keyCell)
			} else {
				err = updated.Set(keyCell, test.value)
			}
			if err != nil {
				t.Fatalf("apply final dictionary update: %v", err)
			}
			exactStorage := cell.NewCellStorageStat()
			if err = exactStorage.AddProof(newAccount, usage); err != nil {
				t.Fatalf("measure new account proof: %v", err)
			}
			if err = exactStorage.AddProof(updated.RootCell(), usage); err != nil {
				t.Fatalf("measure final dictionary proof: %v", err)
			}
			exact := exactStorage.TotalStat()
			exactBytes := exact.Bits/8 + exact.Cells*12 + exact.InternalRefs*3 + exact.ExternalRefs*40
			// A replacement leaves two 3-byte internal edges to the already
			// charged Account outside the path component. The block estimator's
			// existing per-transaction term owns that fixed wrapper cost.
			const replacementWrapperRefs = 2 * 3
			if estimated+replacementWrapperRefs < exactBytes {
				t.Fatalf("account path estimate = %d, final proof = %d", estimated, exactBytes)
			}
		})
	}
}

func TestAccountPathRecorderStopsAtDictionaryValue(t *testing.T) {
	addr := benchAddress("account-path", 0)
	accounts := activeContracts(t, 1, activeContract{
		address: addr,
		code:    externalAcceptCode(t),
		balance: 1_000_000_000,
	})
	usage := cell.NewReadSet(accounts.RootCell())
	traced := &tlb.ShardAccountsAugDict{
		AugmentedDictionary: accounts.Copy().SetTrace(usage.Trace()),
	}
	c := &collation{
		shard: msgpool.ShardIdent{Workchain: 0, Shard: 1 << 63},
		usage: usage,
		accountSources: [2]predecessorAccountSource{{
			shard:    tlb.ShardIdent{WorkchainID: 0},
			accounts: traced,
		}},
	}
	c.prepareAccountPathRecorder()

	var key [32]byte
	copy(key[:], addr.Data())
	lane := &accountLane{key: key}
	lane.tracer = newLaneTracer(c, lane)
	value, path, err := c.loadPredecessorAccount(lane.tracer, key)
	if err != nil {
		t.Fatalf("load predecessor account: %v", err)
	}
	if len(path) == 0 {
		t.Fatal("account lookup recorded no Patricia cells")
	}
	if value.Trace() == nil {
		t.Fatal("account value lost the predecessor read trace")
	}
	// The spine recorder is per lookup and is stripped off the value, so the
	// payload must not extend the path: the slice handed back is final.
	recorded := len(path)
	var account tlb.ShardAccount
	if err = loadExactSlice(&account, &value); err != nil {
		t.Fatalf("decode account value: %v", err)
	}
	if len(path) != recorded {
		t.Fatalf("account payload extended Patricia path from %d to %d cells", recorded, len(path))
	}
	for _, loaded := range path {
		if loaded.HashKey() == account.Account.HashKey() {
			t.Fatal("account payload cell was recorded as a Patricia path cell")
		}
	}
	// The value's remaining trace is the lane's, and the lane is in
	// pass-through, so the shared record saw the descent as it happened —
	// exactly the cells of the spine, delivered through the tracer.
	if got, want := value.Trace(), lane.tracer.trace; got != want {
		t.Fatalf("account value carries trace %p, want the lane tracer %p", got, want)
	}
	if usage.Size() != len(path) {
		t.Fatalf("shared record holds %d cells, want the %d cells of the spine", usage.Size(), len(path))
	}
}
