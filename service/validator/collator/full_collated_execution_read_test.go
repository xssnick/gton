package collator

import (
	"bytes"
	"context"
	"math/big"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	cellsliceop "github.com/xssnick/tonutils-go/tvm/op/cellslice"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
	stackop "github.com/xssnick/tonutils-go/tvm/op/stack"
)

// TestFullCollatedProofCoversCellsReachedOnlyThroughTheMachine pins the union of
// the traversal record and the machine's own load reports at the level it is
// wired in.
//
// The fixture puts one subtree of an account's data into the inbound message as
// well, so the executing contract reaches it through the message rather than
// through the account: the same cell by hash, but a copy that never rode the
// recording trace. Reading the data root keeps the root itself in the record and
// nothing below it, which the untouched sibling confirms by staying pruned — so
// the shared subtree is in the collated proof only if the machine's report of
// what it loaded put it there.
//
// What this does not claim: the replay of this particular block would survive
// the cell being pruned, because it reads it out of the message too. The union
// is a safety net over routes nobody enumerated, and this is the cheapest
// reachable one — the property under test is that the collated proof covers what
// execution read, not that this fixture is the failure it prevents.
func TestFullCollatedProofCoversCellsReachedOnlyThroughTheMachine(t *testing.T) {
	addr := address.NewAddress(0, 0, bytes.Repeat([]byte{0x51}, 32))
	// Both branches are non-leaf on purpose: an omitted leaf reference is
	// materialized as a proof boundary rather than pruned, so a leaf would make
	// the pruned/present distinction unreadable.
	shared := cell.BeginCell().
		MustStoreUInt(0xDD, 8).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0xEE, 8).EndCell()).
		EndCell()
	untouched := cell.BeginCell().
		MustStoreUInt(0xAA, 8).
		MustStoreRef(cell.BeginCell().MustStoreUInt(0xBB, 8).EndCell()).
		EndCell()
	data := cell.BeginCell().MustStoreRef(shared).MustStoreRef(untouched).EndCell()

	req := emptyCandidateRequest(t)
	accounts, err := tlb.NewShardAccountsAugDict()
	if err != nil {
		t.Fatal(err)
	}
	key := cell.BeginCell().MustStoreSlice(addr.Data(), 256).EndCell()
	if err = accounts.Set(key, executionReadShardAccount(t, addr, executionReadCode(t), data, req.Header.GenUtime)); err != nil {
		t.Fatal(err)
	}
	req.Previous.State = stateWithAccounts(t, req.Previous.State, accounts)
	// The account has to be part of a real predecessor before the block that
	// transacts on it: full collated data binds the state to the previous block,
	// so it cannot be edited in place under the block being collated.
	req = advanceCandidateRequest(t, req)
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	message, err := tlb.ToCell(&tlb.ExternalMessage{
		DstAddr: addr,
		Body:    cell.BeginCell().MustStoreRef(shared).EndCell(),
	})
	if err != nil {
		t.Fatal(err)
	}
	req.Externals = []ExternalInput{externalInput(t, message)}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Transactions != 1 || candidate.Stats.ExternalIncluded != 1 {
		t.Fatalf("fixture did not run the transaction under test: %+v", candidate.Stats)
	}

	provenData := executionReadProvenAccountData(t, req, candidate, addr)
	if provenData.GetType() == cell.PrunedCellType {
		t.Fatal("account data root is pruned, so the fixture proves nothing about what is below it")
	}
	provenShared, err := provenData.PeekRef(0)
	if err != nil {
		t.Fatalf("peek shared data branch: %v", err)
	}
	if provenShared.GetType() == cell.PrunedCellType {
		t.Fatal("collated proof pruned a cell the machine loaded; a peer replaying this block " +
			"reports it back as a pruned branch")
	}
	// The sibling is the control inside the fixture: reading the data root does
	// not put its references in the record, so a proof that kept this one would
	// be keeping everything and the assertion above would mean nothing.
	provenUntouched, err := provenData.PeekRef(1)
	if err != nil {
		t.Fatalf("peek untouched data branch: %v", err)
	}
	if provenUntouched.GetType() != cell.PrunedCellType {
		t.Fatal("untouched data branch survived into the collated proof, so the proof is not the read record")
	}

	// A candidate nobody can verify would make the proof assertions above a
	// statement about a shape that never ships.
	verification := shardVerificationRequest(req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	verification.Semantics = NewSemanticVerifier(tvm.NewTVM())
	verification.Neighbors = collatedNeighborQueues(t, req, candidate)
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify candidate: %v", err)
	}
}

// executionReadCode drops the is-external flag, accepts, opens c4 without
// descending into it, and then reads the reference the inbound message body
// carries. The stack below the flag is (balance, msg balance, msg cell, body),
// so the body slice is what LDREFRTOS finds once c4 is off the stack again.
func executionReadCode(t *testing.T) *cell.Cell {
	t.Helper()

	code := cell.BeginCell()
	for _, op := range []*cell.Builder{
		stackop.DROP().Serialize(),
		funcsop.ACCEPT().Serialize(),
		stackop.PUSHCTR(4).Serialize(),
		cellsliceop.CTOS().Serialize(),
		stackop.DROP().Serialize(),
		cellsliceop.LDREFRTOS().Serialize(),
	} {
		if err := code.StoreBuilder(op); err != nil {
			t.Fatal(err)
		}
	}
	return code.EndCell()
}

// executionReadShardAccount is activeShardAccount with the data cell under the
// caller's control; the fixture needs an account whose data has branches.
func executionReadShardAccount(t *testing.T, addr *address.Address, code, data *cell.Cell, lastPaid uint32) *cell.Cell {
	t.Helper()

	stateInit, err := tlb.ToCell(&tlb.StateInit{Code: code, Data: data})
	if err != nil {
		t.Fatal(err)
	}
	storage, err := tlb.ToCell(&tlb.StorageInfo{
		StorageUsed: tlb.StorageUsed{
			CellsUsed: big.NewInt(0),
			BitsUsed:  big.NewInt(0),
		},
		StorageExtra: tlb.StorageExtraNone{},
		LastPaid:     lastPaid,
	})
	if err != nil {
		t.Fatal(err)
	}
	account := cell.BeginCell().
		MustStoreBoolBit(true).
		MustStoreAddr(addr).
		MustStoreBuilder(storage.ToBuilder()).
		MustStoreUInt(0, 64).
		MustStoreBigCoins(new(big.Int).SetUint64(100_000_000_000)).
		MustStoreDict(nil).
		MustStoreBoolBit(true).
		MustStoreBuilder(stateInit.ToBuilder()).
		EndCell()
	shardAccount, err := tlb.ToCell(&tlb.ShardAccount{
		Account:       account,
		LastTransHash: make([]byte, 32),
	})
	if err != nil {
		t.Fatal(err)
	}
	return shardAccount
}

// executionReadProvenAccountData reads the account's data root out of the
// predecessor state proof the candidate ships, which is the only view of the
// predecessor a validator running on collated data ever gets.
func executionReadProvenAccountData(t *testing.T, req ShardRequest, candidate *Candidate, addr *address.Address) *cell.Cell {
	t.Helper()

	verified, err := verifyCollatedData(candidate, req.Header.GenUtime)
	if err != nil {
		t.Fatalf("decode collated data: %v", err)
	}
	if !verified.full {
		t.Fatal("candidate carries no full collated data")
	}
	stateRoot, err := collatedStateRoot(&verified, req.Previous.ID)
	if err != nil {
		t.Fatalf("load collated predecessor state: %v", err)
	}
	var state tlb.ShardStateUnsplit
	if err = parseProofExact(&state, stateRoot); err != nil {
		t.Fatalf("decode collated predecessor state: %v", err)
	}

	var key [32]byte
	copy(key[:], addr.Data())
	proven, exists, err := semanticLoadAccount(state.Accounts.ShardAccounts, key)
	if err != nil {
		t.Fatalf("load proven account: %v", err)
	}
	if !exists {
		t.Fatal("account is missing from the collated predecessor proof")
	}
	var account tlb.AccountState
	if err = tlb.LoadFromCell(&account, proven.Account.MustBeginParse()); err != nil {
		t.Fatalf("decode proven account: %v", err)
	}
	if account.StateInit == nil || account.StateInit.Data == nil {
		t.Fatal("proven account lost its state init")
	}
	return account.StateInit.Data
}
