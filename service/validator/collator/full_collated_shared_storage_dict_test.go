package collator

import (
	"bytes"
	"context"
	"testing"

	"github.com/xssnick/tonutils-go/address"
	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm"
	"github.com/xssnick/tonutils-go/tvm/cell"
	cellsliceop "github.com/xssnick/tonutils-go/tvm/op/cellslice"
	execop "github.com/xssnick/tonutils-go/tvm/op/exec"
	funcsop "github.com/xssnick/tonutils-go/tvm/op/funcs"
	stackop "github.com/xssnick/tonutils-go/tvm/op/stack"
)

// Two accounts can commit to the SAME storage-stat dictionary hash: the
// dictionary is a function of the account storage cell's references — its code
// and data trees — and the address is not among them, so identical contracts
// hash to identical dictionaries. Collated data carries one
// account_storage_dict_proof per dictionary hash, never one per account
// (verifyCollatedRoots rejects a second root with the same virtual hash), and a
// validator binds that single proof to every account whose state commits to the
// hash and replays each of their update walks against it.
//
// So the one shipped proof has to cover the union of those walks. It used to
// cover only the last account the collator happened to visit — the emitted map
// was keyed by dictionary hash but assigned from a per-account recorder, so the
// last assignment won. Every other account's branches reached the validator
// pruned. Our own replay recovers from that (the executor recomputes the stat
// from state when a bound dict is short, and only counts it), but the reference
// validator does not: validate-query.cpp raises VmVirtError on the pruned cell
// and votes REJECT. An insufficient proof is a consensus hazard, not a
// nuisance.
//
// Every hand-built account profile in this package has no storage extra at all,
// and the mainnet fixture has no two accounts sharing a dictionary, so nothing
// else in the suite can reach this shape.
func TestFullCollatedSharedStorageDictProvesEveryAccount(t *testing.T) {
	// The two accounts run identical code over identical data, so their initial
	// storage-stat dictionaries — and therefore their dictionary hashes — are
	// the same. The ballast under the code carries the state over the
	// storage-dict threshold, which is what makes the accounts keep a dictionary
	// at all.
	ballast := sharedStorageDictBallast(t, 40)
	code := sharedStorageDictContract(t, ballast[len(ballast)-1])
	first := address.NewAddress(0, 0, bytes.Repeat([]byte{0x71}, 32))
	second := address.NewAddress(0, 0, bytes.Repeat([]byte{0x72}, 32))

	req := emptyCandidateRequest(t)
	req.Previous.State = stateWithAccounts(t, req.Previous.State, activeContracts(t, req.Header.GenUtime,
		activeContract{address: first, code: code, balance: 100_000_000_000},
		activeContract{address: second, code: code, balance: 100_000_000_000},
	))

	// A block whose only job is to let the executor write both accounts' real
	// storage usage and dictionary hash: a hand-built ShardAccount carries
	// neither, and below the threshold no dictionary is kept. Both accounts
	// receive the same body, so they come out of it in identical states.
	req.Externals = []ExternalInput{
		sharedStorageDictExternal(t, first, nil),
		sharedStorageDictExternal(t, second, nil),
	}
	req = advanceCandidateRequest(t, req)

	dictHash := assertSharedStorageDict(t, req, first, second)

	// capFullCollatedData is OFF in the default fixture, and with it off the
	// collated data degenerates to a marker carrying no proofs at all — every
	// assertion below would then hold vacuously.
	req.Masterchain.Config.capabilities |= capFullCollatedData
	attachFullCollatedTestNeighbors(t, &req)

	// Now the two accounts diverge. Each stores a reference to a DIFFERENT cell
	// that already lives in its state, which makes the storage-stat update bump
	// that cell's refcount — an existing key, so a deep walk into the shared
	// dictionary, and a different branch for each account. Anything shallower
	// (a fresh key, which terminates at the first label mismatch near the root)
	// would be served by either account's proof and prove nothing.
	touchedByFirst, touchedBySecond := ballast[3], ballast[30]
	req.Externals = []ExternalInput{
		sharedStorageDictExternal(t, first, touchedByFirst),
		sharedStorageDictExternal(t, second, touchedBySecond),
	}

	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatalf("collate shared storage dict candidate: %v", err)
	}
	if candidate.Stats.Transactions != 2 {
		t.Fatalf("candidate has %d transactions, want one per account", candidate.Stats.Transactions)
	}

	// The wire shape that forces the union: one proof for both accounts.
	proven := collatedAccountStorageProofs(t, candidate)
	if len(proven) != 1 {
		t.Fatalf("collated data carries %d account storage proofs, want one shared by both accounts", len(proven))
	}
	shared := proven[dictHash]
	if shared == nil {
		t.Fatalf("collated data has no account storage proof for the shared dictionary %x", dictHash)
	}

	// The direct assertion, on the shipped bytes rather than on how our own
	// replay reacts to them: both accounts' walks resolve through the one proof.
	// A proof built from a single account's reads answers the other account's
	// key with ErrDictHasSpecialCells — it walks into a pruned boundary — which
	// is the exact cell the reference validator rejects the block over.
	for _, touched := range []struct {
		label string
		value *cell.Cell
	}{
		{label: "first", value: touchedByFirst},
		{label: "second", value: touchedBySecond},
	} {
		if _, err = shared.AsDict(256).LoadValueByBytesKey(touched.value.Hash()); err != nil {
			t.Fatalf("shared storage dict proof cannot serve the %s account's walk (key %x): %v",
				touched.label, touched.value.Hash()[:8], err)
		}
	}

	// And end to end through the production verifier. The recompute observer is
	// the second half of it: our executor silently recomputes a stat whose bound
	// dictionary falls short, so a candidate with a short proof still verifies
	// here while the reference rejects it. Zero recomputes is the assertion that
	// the shipped proof really carried the replay.
	verification := shardVerificationRequest(req, candidate)
	verification.Neighbors = collatedNeighborQueues(t, req, candidate)
	verification.NeighborShardEndLT = req.NeighborShardEndLT
	semantics := NewSemanticVerifier(tvm.NewTVM())
	var recomputed [][32]byte
	semantics.SetStorageStatRecomputeObserver(func(_ MetricChain, key [32]byte) {
		recomputed = append(recomputed, key)
	})
	verification.Semantics = semantics
	if err = VerifyShardCandidate(context.Background(), verification); err != nil {
		t.Fatalf("verify shared storage dict candidate: %v", err)
	}
	for _, key := range recomputed {
		t.Errorf("account %x fell back to recomputing its storage stat: the shipped proof was short", key)
	}
}

// Accounts sharing a dictionary are emitted as one proof, so the collated size
// estimate must be told about that one proof once. Charging each of them the
// whole proof would bill the block for bytes it never carries — safe, but it
// tightens admission against nothing — while charging only the first would stop
// tracking growth the later accounts add.
//
// The exact assertion is available here and worth making: with nothing else
// contributing, the fixed estimate must equal the serialized size of the proof
// the collation would actually emit at that moment.
func TestTrackAccountStorageProofChargesSharedDictionaryOnce(t *testing.T) {
	root := storageStatTestDict(t)
	c := &collation{fullCollated: true, collatedProofEstimate: newProofSizeEstimator(0)}

	builder := c.sharedAccountStorageProof(root)
	if again := c.sharedAccountStorageProof(root); again != builder {
		t.Fatal("two accounts on one storage dictionary were given different proof recorders")
	}

	charged := func(index int) uint64 {
		t.Helper()

		if _, err := builder.Root().AsDict(256).LoadValueByBytesKey(storageStatTestKeys[index][:]); err != nil {
			t.Fatalf("read storage dict entry %d: %v", index, err)
		}
		lane := &accountLane{initialStorageStat: root, storageProof: builder}
		if err := c.trackAccountStorageProof(lane); err != nil {
			t.Fatalf("track account storage proof: %v", err)
		}
		if lane.initialStorageProof == nil {
			t.Fatal("account was not marked as charged")
		}

		return c.collatedFixedEstimate
	}
	// Two accounts on the one dictionary, reading branches far enough apart that
	// neither proof subsumes the other.
	first := charged(0)
	if first == 0 {
		t.Fatal("the first account on a storage dictionary charged nothing")
	}
	second := charged(4)
	if second <= first {
		t.Fatalf("estimate did not grow with the second account's reads: %d then %d", first, second)
	}
	if second >= 2*first {
		t.Fatalf("shared storage dict proof was charged twice: %d then %d", first, second)
	}

	proof, err := builder.CreateProof()
	if err != nil {
		t.Fatalf("create shared storage dict proof: %v", err)
	}
	boc, err := wrapAccountStorageProof(proof).ToBOCWithOptionsErr(cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatalf("serialize shared storage dict proof: %v", err)
	}
	if uint64(len(boc)) != second {
		t.Fatalf("estimate charges %d bytes for a shared storage dict proof that serializes to %d",
			second, len(boc))
	}
}

// assertSharedStorageDict fails unless both accounts really reached the shape
// the defect needs — a storage-stat dictionary, and the same one — and returns
// the hash they share. Without this the test would keep passing after a change
// that quietly stopped producing dictionaries here, proving nothing.
func assertSharedStorageDict(tb testing.TB, req ShardRequest, addrs ...*address.Address) cell.Hash {
	tb.Helper()

	state := loadPreviousShardState(tb, req)
	var shared cell.Hash
	for i, addr := range addrs {
		var key [32]byte
		copy(key[:], addr.Data())
		account, exists, err := semanticLoadAccount(state.Accounts.ShardAccounts, key)
		if err != nil {
			tb.Fatalf("load account %x: %v", key, err)
		}
		if !exists {
			tb.Fatalf("account %x is absent from the predecessor state", key)
		}
		prepared, err := tvm.PrepareAccount(account, addr)
		if err != nil {
			tb.Fatalf("prepare account %x: %v", key, err)
		}
		extra, ok := prepared.State().StorageInfo.StorageExtra.(tlb.StorageExtraInfo)
		if !ok || len(extra.DictHash) != len(shared) {
			tb.Fatalf("account %x carries no storage-stat dictionary; the fixture no longer "+
				"reaches the storage-dict threshold", key)
		}
		var hash cell.Hash
		copy(hash[:], extra.DictHash)
		if i == 0 {
			shared = hash
			continue
		}
		if hash != shared {
			tb.Fatalf("accounts do not share an initial storage dictionary: %x vs %x", shared, hash)
		}
	}

	return shared
}

// collatedAccountStorageProofs returns the virtual dictionary root of every
// account storage proof the candidate ships, keyed by the dictionary hash it
// proves.
func collatedAccountStorageProofs(tb testing.TB, candidate *Candidate) map[cell.Hash]*cell.Cell {
	tb.Helper()

	roots, err := cell.FromBOCMultiRoot(candidate.CollatedData)
	if err != nil {
		tb.Fatalf("parse collated data: %v", err)
	}
	proofs := map[cell.Hash]*cell.Cell{}
	for _, root := range roots {
		loader := root.MustBeginParse()
		if loader.BitsLeft() < 32 {
			continue
		}
		tag, tagErr := loader.LoadUInt(32)
		if tagErr != nil || tag != accountStorageDictProofTag {
			continue
		}
		proof, refErr := loader.LoadRefCell()
		if refErr != nil {
			tb.Fatalf("load account storage proof: %v", refErr)
		}
		virtual, unwrapErr := unwrapCollatedProof(proof)
		if unwrapErr != nil {
			tb.Fatalf("unwrap account storage proof: %v", unwrapErr)
		}
		// Level zero explicitly: the proof body carries pruned branches, so its
		// top level is not the one the proven dictionary is addressed by.
		proofs[virtual.HashKey(0)] = virtual
	}

	return proofs
}

// sharedStorageDictContract builds a contract that replaces its data with the
// inbound message body: `c4 := ENDC(NEWC + body)`. Both test accounts run this
// same code, so they start from the same dictionary, and each ends up storing
// whatever its own message carried — including a reference into a cell the
// account's state already holds, which is what sends the two storage-stat
// updates down different branches of that one dictionary.
//
// The trailing ballast reference is never executed. It is there to carry the
// account state over accStateCellsForStorageDict, below which no storage-stat
// dictionary is kept at all.
func sharedStorageDictContract(tb testing.TB, ballast *cell.Cell) *cell.Cell {
	tb.Helper()

	code := cell.BeginCell()
	// The entry stack is balance, msg_balance, msg_cell, body_slice, selector.
	for _, op := range []*cell.Builder{
		stackop.DROP().Serialize(),
		funcsop.ACCEPT().Serialize(),
		cellsliceop.NEWC().Serialize(),
		cellsliceop.STSLICE().Serialize(),
		cellsliceop.ENDC().Serialize(),
		execop.POPCTR(4).Serialize(),
		stackop.PUSHREF(ballast).Serialize(),
	} {
		if err := code.StoreBuilder(op); err != nil {
			tb.Fatal(err)
		}
	}

	return code.EndCell()
}

// sharedStorageDictBallast builds a chain of n distinct cells and returns every
// link, deepest last. The chain hangs off the contract code, so each of its
// cells is an entry of the account's storage-stat dictionary and any of them can
// be referenced by a message to force a deep lookup of an existing key.
func sharedStorageDictBallast(tb testing.TB, n int) []*cell.Cell {
	tb.Helper()

	chain := make([]*cell.Cell, 0, n)
	current := cell.BeginCell().MustStoreUInt(0x5a, 8).EndCell()
	chain = append(chain, current)
	for i := 1; i < n; i++ {
		current = cell.BeginCell().
			MustStoreUInt(uint64(uint32(i)*0x9e3779b9), 32).
			MustStoreRef(current).
			EndCell()
		chain = append(chain, current)
	}

	return chain
}

// sharedStorageDictExternal addresses one external message to addr, carrying a
// reference to touch when one is given. The contract stores the body verbatim,
// so the reference lands in the account's new data.
//
// The body's bits are the same for every account on purpose: the seeding block
// must leave both accounts in identical states, or they would not share a
// dictionary at all. Only the reference — and only in the block under test —
// tells the two accounts apart. The messages still differ, by destination.
func sharedStorageDictExternal(tb testing.TB, addr *address.Address, touch *cell.Cell) ExternalInput {
	tb.Helper()

	body := cell.BeginCell().MustStoreUInt(0x5f0d1c7d, 32)
	if touch != nil {
		body.MustStoreRef(touch)
	}
	message, err := tlb.ToCell(&tlb.ExternalMessage{DstAddr: addr, Body: body.EndCell()})
	if err != nil {
		tb.Fatal(err)
	}

	return externalInput(tb, message)
}
