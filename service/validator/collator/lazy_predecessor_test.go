package collator

import (
	"fmt"
	"testing"
	"time"

	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/msgpool"
)

// A state read out of the node's own store is lazy: its references are
// pruned-branch stubs until something materializes them. That is the same shape
// a Merkle proof presents, and it is why the two decodes must not be
// interchangeable — proof-aware decoding treats such a reference as a proven
// absence and skips the field.
//
// It skipped ShardAccounts, so a freshly bootstrapped network stopped dead:
// every validator refused to activate its first session on the zerostate with
// "shard accounts dictionary is absent", nothing was collated or validated, and
// the masterchain never left seqno 0.
func TestVerifyPredecessorKeepsAccountsOfALazyResidentState(t *testing.T) {
	req := emptyCandidateRequest(t)
	previous := req.Previous
	previous.State = lazyRefState(t, req.Previous.State)

	// The shape has to be the real one, or the test proves nothing: the accounts
	// reference must still be an unmaterialized stub at this point.
	accounts, err := previous.State.PeekRef(1)
	if err != nil {
		t.Fatal(err)
	}
	if accounts.GetType() != cell.PrunedCellType {
		t.Fatalf("accounts reference is %v, so the fixture is not lazy", accounts.GetType())
	}

	state, err := verifyPredecessor("acquired", &previous)
	if err != nil {
		t.Fatalf("lazy resident predecessor rejected: %v", err)
	}
	if state.Accounts.ShardAccounts == nil || state.Accounts.ShardAccounts.AugmentedDictionary == nil {
		t.Fatal("decoding a lazy resident state dropped its accounts dictionary")
	}
	if _, err = state.Accounts.ShardAccounts.Count(); err != nil {
		t.Fatalf("accounts dictionary did not materialize on use: %v", err)
	}
}

// The other half of the split: a genuine proof must still be decoded as one.
// Reading straight through a pruned boundary would not fail — the payload
// starts with the 0x01 type byte, so fields come back as plausible zeroes — so
// this pins that the proven form still reaches the proof decode.
func TestVerifyPredecessorStateDecodesAProvenStateAsProof(t *testing.T) {
	req := benchMainnetCollatedRequest(t, 1)
	candidate, err := testBuilder().BuildShard(t.Context(), req)
	if err != nil {
		t.Fatalf("collate mainnet candidate: %v", err)
	}
	collated, err := verifyCollatedData(candidate, req.Header.GenUtime)
	if err != nil {
		t.Fatal(err)
	}
	proven, err := collatedStateRoot(&collated, req.Previous.ID)
	if err != nil {
		t.Fatal(err)
	}

	previous := req.Previous
	previous.State = proven
	if _, err = verifyPredecessorState(provenPredecessor, "first", &previous); err != nil {
		t.Fatalf("proven predecessor rejected by the proof decode: %v", err)
	}
}

// The bootstrap path itself: the applied-block hook publishes the masterchain
// state straight out of the cell store, and that publish runs the same
// predecessor decode. On a fresh network this is the zerostate, so a decode that
// drops its accounts is the difference between the first session activating and
// the whole network sitting at seqno 0.
func TestPublishMasterchainViewAcceptsALazyResidentState(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	pool := msgpool.New(msgpool.Config{})
	defer pool.Close()
	acquisition, err := NewLocalAcquisition(LocalAcquisitionOptions{
		Builder:   testBuilder(),
		Store:     &localValidationStore{},
		Groups:    &localAcquisitionTestGroups{snapshot: fixture.request.Groups},
		Messages:  pool,
		Semantics: testCandidateTransitionVerifier,
	})
	if err != nil {
		t.Fatal(err)
	}

	lazy := lazyRefState(t, fixture.request.Previous.State)
	if err = acquisition.PublishMasterchainView(
		t.Context(),
		fixture.request.Groups,
		fixture.request.Previous.Block,
		lazy,
	); err != nil {
		t.Fatalf("publish a lazy resident masterchain view: %v", err)
	}

	// The store is empty, so reaching a view at all proves the published one was
	// used — and that it decoded well enough to serve as a predecessor.
	projected, err := acquisition.projectedMasterView(t.Context(), fixture.request.Previous.ID, time.Now())
	if err != nil {
		t.Fatalf("project the lazy resident view: %v", err)
	}
	if projected.state.Accounts.ShardAccounts == nil ||
		projected.state.Accounts.ShardAccounts.AugmentedDictionary == nil {
		t.Fatal("the projected masterchain view lost its accounts dictionary")
	}
}

// lazyRefState rebuilds a state root the way the cell store hands it back: the
// root cell itself is present and every reference below it is an unresolved
// stub that loads on demand.
func lazyRefState(tb testing.TB, state *cell.Cell) *cell.Cell {
	tb.Helper()

	slice, err := state.BeginParse()
	if err != nil {
		tb.Fatal(err)
	}
	bits := slice.BitsLeft()
	payload, err := slice.LoadSlice(bits)
	if err != nil {
		tb.Fatal(err)
	}

	builder := cell.BeginCell().MustStoreSlice(payload, bits)
	for i := 0; i < int(state.RefsNum()); i++ {
		ref, refErr := state.PeekRef(i)
		if refErr != nil {
			tb.Fatal(refErr)
		}
		if err = builder.StoreRef(lazyStub(tb, ref)); err != nil {
			tb.Fatal(err)
		}
	}
	rebuilt := builder.EndCell()
	if rebuilt.HashKey() != state.HashKey() {
		tb.Fatal("rebuilt lazy state is not the same state")
	}

	return rebuilt
}

// lazyStub returns an unresolved reference to target: a pruned-branch cell that
// materializes the real subtree when it is loaded.
func lazyStub(tb testing.TB, target *cell.Cell) *cell.Cell {
	tb.Helper()

	carrier := cell.BeginCell().MustStoreRef(target).EndCell()
	lazy, err := cell.CreateWithLazyRefsUnsafe(
		0x0100,
		nil,
		carrier.Hash(),
		[]uint16{carrier.Depth()},
		[]cell.LazyRef{{
			LevelMask: target.LevelMask(),
			Hashes:    target.Hash(),
			Depths:    []uint16{target.Depth()},
		}},
		func(hash cell.Hash) (*cell.Cell, error) {
			if hash != target.HashKey() {
				return nil, fmt.Errorf("unexpected lazy load %x", hash)
			}
			return target, nil
		},
	)
	if err != nil {
		tb.Fatal(err)
	}

	return lazy.MustPeekRef(0)
}
