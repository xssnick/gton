package collator

import (
	"encoding/binary"
	"errors"
	"fmt"
	"math/rand/v2"
	"slices"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

// minimumDispatchAccountByPrefixCuts is the selection as it was written before
// the direct walk: the C++ get_dispatch_queue_min_lt_account loop of prefix cuts
// on dictionary copies. It is kept as the reference for the account chosen and
// for the cells the choice records into the collated proof.
func minimumDispatchAccountByPrefixCuts(queue *tlb.DispatchQueueAugDict) (DispatchAccount, error) {
	if queue == nil || queue.IsEmpty() {
		return DispatchAccount{}, fmt.Errorf("%w: dispatch queue has no minimum account", ErrInvalidInput)
	}

	current := queue.AugmentedDictionary.Copy()
	var rootExtra cell.Slice
	if err := current.LoadRootExtraInto(&rootExtra); err != nil {
		return DispatchAccount{}, fmt.Errorf("%w: load dispatch queue root augmentation: %v", ErrInvalidInput, err)
	}
	minimumLT, err := dispatchMinimumLT(&rootExtra)
	if err != nil {
		return DispatchAccount{}, fmt.Errorf("%w: dispatch queue root augmentation: %v", ErrInvalidInput, err)
	}

	accountKey := cell.BeginCell()
	for {
		common, commonErr := current.GetCommonPrefix()
		if commonErr != nil {
			return DispatchAccount{}, fmt.Errorf("%w: load dispatch queue common prefix: %v", ErrInvalidInput, commonErr)
		}
		remaining := current.GetKeySize()
		if common.BitsSize() > remaining || common.RefsNum() != 0 {
			return DispatchAccount{}, fmt.Errorf("%w: invalid dispatch queue common prefix", ErrInvalidInput)
		}
		var commonBuilder cell.Builder
		common.ToBuilderInto(&commonBuilder)
		if err = accountKey.StoreBuilder(&commonBuilder); err != nil {
			return DispatchAccount{}, fmt.Errorf("%w: append dispatch account prefix: %v", ErrInvalidInput, err)
		}
		if common.BitsSize() == remaining {
			if accountKey.BitsUsed() != 256 {
				return DispatchAccount{}, fmt.Errorf("%w: dispatch account key has %d bits", ErrInvalidInput, accountKey.BitsUsed())
			}
			var key cell.Slice
			loadErr := accountKey.EndCell().BeginParseInto(&key)
			var selected DispatchAccount
			if loadErr == nil {
				loadErr = key.LoadSliceInto(selected.AccountID[:], 256)
			}
			if loadErr != nil || key.BitsLeft() != 0 || key.RefsNum() != 0 {
				return DispatchAccount{}, fmt.Errorf("%w: invalid dispatch account key", ErrInvalidInput)
			}
			return selected, nil
		}

		common.ToBuilderInto(&commonBuilder)
		leftPrefix := commonBuilder.MustStoreUInt(0, 1).EndCell()
		left := current.Copy()
		ok, cutErr := left.CutPrefixSubdict(leftPrefix, true)
		if cutErr != nil {
			return DispatchAccount{}, fmt.Errorf("%w: cut left dispatch queue branch: %v", ErrInvalidInput, cutErr)
		}
		if !ok || left.IsEmpty() {
			return DispatchAccount{}, fmt.Errorf("%w: left dispatch queue branch is absent", ErrInvalidInput)
		}
		var leftExtra cell.Slice
		loadErr := left.LoadRootExtraInto(&leftExtra)
		if loadErr != nil {
			return DispatchAccount{}, fmt.Errorf("%w: load left dispatch queue augmentation: %v", ErrInvalidInput, loadErr)
		}
		leftMinimum, loadErr := dispatchMinimumLT(&leftExtra)
		if loadErr != nil {
			return DispatchAccount{}, fmt.Errorf("%w: left dispatch queue augmentation: %v", ErrInvalidInput, loadErr)
		}

		branch := uint64(0)
		if leftMinimum == minimumLT {
			current = left
		} else {
			branch = 1
			common.ToBuilderInto(&commonBuilder)
			rightPrefix := commonBuilder.MustStoreUInt(1, 1).EndCell()
			ok, cutErr = current.CutPrefixSubdict(rightPrefix, true)
			if cutErr != nil {
				return DispatchAccount{}, fmt.Errorf("%w: cut right dispatch queue branch: %v", ErrInvalidInput, cutErr)
			}
			if !ok || current.IsEmpty() {
				return DispatchAccount{}, fmt.Errorf("%w: right dispatch queue branch is absent", ErrInvalidInput)
			}
			var rightExtra cell.Slice
			rightErr := current.LoadRootExtraInto(&rightExtra)
			if rightErr != nil {
				return DispatchAccount{}, fmt.Errorf("%w: load right dispatch queue augmentation: %v", ErrInvalidInput, rightErr)
			}
			rightMinimum, rightErr := dispatchMinimumLT(&rightExtra)
			if rightErr != nil {
				return DispatchAccount{}, fmt.Errorf("%w: right dispatch queue augmentation: %v", ErrInvalidInput, rightErr)
			}
			if rightMinimum != minimumLT {
				return DispatchAccount{}, fmt.Errorf("%w: right dispatch queue branch does not contain the root minimum", ErrInvalidInput)
			}
		}
		if err = accountKey.StoreUInt(branch, 1); err != nil {
			return DispatchAccount{}, fmt.Errorf("%w: append dispatch account branch: %v", ErrInvalidInput, err)
		}
	}
}

type minimumDispatchSelector func(*tlb.DispatchQueueAugDict) (DispatchAccount, error)

// dispatchSelectionTrace is what one selector did to a traced queue: the
// accounts it chose and every cell the read set recorded, in first-read order.
type dispatchSelectionTrace struct {
	accounts []DispatchAccount
	errs     []error
	reads    []cell.Hash
}

// TestMinimumDispatchAccountMatchesPrefixCuts drains random queues the way a
// dispatch phase does — select the minimum, then drop the account or keep it
// with a later message — through a read set attached as a collation attaches
// it, and requires the same accounts and the same recorded cells, in the same
// order, from the direct walk and from the prefix cuts.
func TestMinimumDispatchAccountMatchesPrefixCuts(t *testing.T) {
	rng := rand.New(rand.NewPCG(0x6469737061746368, 0x6d696e6c74))
	for _, accountCount := range []int{1, 2, 3, 5, 16, 33, 256, 1000, 4096} {
		rounds := 16
		if accountCount >= 1000 {
			rounds = 3
		}
		for round := range rounds {
			wrapper := randomDispatchQueueCell(t, rng, accountCount)
			steps := min(accountCount, 64)
			seed := rng.Uint64()

			want := drainMinimumDispatchAccounts(t, wrapper, steps, seed, minimumDispatchAccountByPrefixCuts)
			got := drainMinimumDispatchAccounts(t, wrapper, steps, seed, minimumDispatchAccount)
			if !slices.Equal(got.accounts, want.accounts) {
				t.Fatalf("accounts=%d round=%d: selected %x, want %x", accountCount, round, got.accounts, want.accounts)
			}
			if !slices.Equal(got.reads, want.reads) {
				t.Fatalf("accounts=%d round=%d: recorded %d cells, want %d in the same order", accountCount, round, len(got.reads), len(want.reads))
			}
		}
	}
}

// TestMinimumDispatchAccountForgedAugmentationMatchesPrefixCuts forges the fork
// augmentations of a four-account queue. Where the extras still lead to a leaf
// both selectors pick it; where a branch claims a minimum it does not hold both
// reject the queue, after reading the same cells.
func TestMinimumDispatchAccountForgedAugmentationMatchesPrefixCuts(t *testing.T) {
	accounts := [4][32]byte{{0x00}, {0x40}, {0x80}, {0xc0}}
	queue := makeDispatchQueue(t,
		dispatchFixtureAccount{accountID: accounts[0], lts: []uint64{5}},
		dispatchFixtureAccount{accountID: accounts[1], lts: []uint64{6}},
		dispatchFixtureAccount{accountID: accounts[2], lts: []uint64{2}},
		dispatchFixtureAccount{accountID: accounts[3], lts: []uint64{3}},
	)
	root := queue.RootCell()
	leftFork := mustDispatchRef(t, root, 0)
	rightFork := mustDispatchRef(t, root, 1)

	for _, test := range []struct {
		name                  string
		rootLT, leftLT, right uint64
		want                  int
	}{
		{name: "canonical", rootLT: 2, leftLT: 5, right: 2, want: 2},
		{name: "fork above its children picks the matching child", rootLT: 5, leftLT: 5, right: 2, want: 0},
		{name: "left branch without the minimum", rootLT: 2, leftLT: 2, right: 2, want: -1},
		{name: "neither branch holds the minimum", rootLT: 1, leftLT: 5, right: 2, want: -1},
		{name: "right branch without the minimum", rootLT: 1, leftLT: 4, right: 1, want: -1},
	} {
		t.Run(test.name, func(t *testing.T) {
			left := reaugmentedDispatchFork(t, leftFork, test.leftLT, mustDispatchRef(t, leftFork, 0), mustDispatchRef(t, leftFork, 1))
			right := reaugmentedDispatchFork(t, rightFork, test.right, mustDispatchRef(t, rightFork, 0), mustDispatchRef(t, rightFork, 1))
			forgedRoot := reaugmentedDispatchFork(t, root, test.rootLT, left, right)
			wrapper := cell.BeginCell().
				MustStoreUInt(1, 1).
				MustStoreRef(forgedRoot).
				MustStoreUInt(test.rootLT, 64).
				EndCell()

			want := drainMinimumDispatchAccounts(t, wrapper, 1, 0, minimumDispatchAccountByPrefixCuts)
			got := drainMinimumDispatchAccounts(t, wrapper, 1, 0, minimumDispatchAccount)
			if test.want < 0 {
				if !errors.Is(want.errs[0], ErrInvalidInput) || !errors.Is(got.errs[0], ErrInvalidInput) {
					t.Fatalf("errors = %v / %v, want ErrInvalidInput from both", got.errs[0], want.errs[0])
				}
			} else {
				if got.errs[0] != nil || want.errs[0] != nil {
					t.Fatalf("errors = %v / %v", got.errs[0], want.errs[0])
				}
				if got.accounts[0].AccountID != accounts[test.want] || want.accounts[0].AccountID != accounts[test.want] {
					t.Fatalf("selected %x / %x, want %x", got.accounts[0].AccountID, want.accounts[0].AccountID, accounts[test.want])
				}
			}
			if !slices.Equal(got.reads, want.reads) {
				t.Fatalf("recorded %d cells, want %d in the same order", len(got.reads), len(want.reads))
			}
		})
	}
}

// drainMinimumDispatchAccounts loads wrapper under a fresh read set, copies it
// into a scan view and runs steps selections on it. After each one the account
// is removed or given a later message, as seed decides, so later selections
// walk roots rebuilt by mutation. A selection error ends the drain.
func drainMinimumDispatchAccounts(
	t *testing.T,
	wrapper *cell.Cell,
	steps int,
	seed uint64,
	selector minimumDispatchSelector,
) dispatchSelectionTrace {
	t.Helper()
	var trace dispatchSelectionTrace
	usage := cell.NewReadSet(wrapper)
	usage.SetRecordCallback(func(loaded *cell.Cell) {
		trace.reads = append(trace.reads, loaded.HashKey())
	})
	var loaded tlb.DispatchQueueAugDict
	if err := loaded.LoadFromCell(usage.Root().MustBeginParse()); err != nil {
		t.Fatal(err)
	}
	view := &tlb.DispatchQueueAugDict{AugmentedDictionary: loaded.Copy()}

	rng := rand.New(rand.NewPCG(seed, 0))
	for step := 0; step < steps && !view.IsEmpty(); step++ {
		account, err := selector(view)
		trace.accounts = append(trace.accounts, account)
		trace.errs = append(trace.errs, err)
		if err != nil {
			break
		}

		var value *cell.Cell
		if rng.IntN(2) == 0 {
			value = dispatchAccountQueueValue(t, []uint64{2_000_000 + rng.Uint64N(64)})
		}
		if err = storeDispatchAccountValue(view.AugmentedDictionary, account.AccountID, value); err != nil {
			t.Fatal(err)
		}
	}
	return trace
}

// randomDispatchQueueCell builds a HashmapAugE dispatch queue. Keys mix random
// accounts, a cluster sharing a random prefix of random length and small
// integers, so the trie has long labels and deep one-sided paths. LTs come from
// a span that is sometimes tiny, which fills the queue with equal minimums.
func randomDispatchQueueCell(t *testing.T, rng *rand.Rand, accountCount int) *cell.Cell {
	t.Helper()
	queue, err := tlb.NewDispatchQueueAugDict()
	if err != nil {
		t.Fatal(err)
	}
	ltSpan := []uint64{1, 3, 1 << 40}[rng.IntN(3)]
	cluster := randomDispatchAccountID(rng)
	seen := make(map[[32]byte]struct{}, accountCount)
	for len(seen) < accountCount {
		var accountID [32]byte
		switch rng.IntN(3) {
		case 0:
			accountID = randomDispatchAccountID(rng)
		case 1:
			accountID = cluster
			tail := randomDispatchAccountID(rng)
			shared := rng.IntN(32)
			copy(accountID[shared:], tail[shared:])
		default:
			binary.BigEndian.PutUint64(accountID[24:], rng.Uint64N(uint64(accountCount)*4))
		}
		if _, ok := seen[accountID]; ok {
			continue
		}
		seen[accountID] = struct{}{}

		lts := make([]uint64, 1+rng.IntN(3))
		for i := range lts {
			lts[i] = 1_000 + rng.Uint64N(ltSpan)
		}
		if err = queue.Set(dispatchAccountKey(accountID), dispatchAccountQueueValue(t, lts)); err != nil {
			t.Fatal(err)
		}
	}
	wrapper, err := queue.ToCell()
	if err != nil {
		t.Fatal(err)
	}
	return wrapper
}

func randomDispatchAccountID(rng *rand.Rand) [32]byte {
	var accountID [32]byte
	for i := 0; i < len(accountID); i += 8 {
		binary.BigEndian.PutUint64(accountID[i:], rng.Uint64())
	}
	return accountID
}

// dispatchAccountQueueValue is an AccountDispatchQueue whose messages are empty
// cells: the queue augmentation reads only the message keys.
func dispatchAccountQueueValue(t *testing.T, lts []uint64) *cell.Cell {
	t.Helper()
	messages := cell.NewDict(64)
	for _, lt := range lts {
		if err := messages.Set(dispatchLTKey(lt), cell.BeginCell().EndCell()); err != nil {
			t.Fatal(err)
		}
	}
	value, err := (tlb.AccountDispatchQueue{Messages: messages, Count: uint64(len(lts))}).ToCell()
	if err != nil {
		t.Fatal(err)
	}
	return value
}

func mustDispatchRef(t *testing.T, node *cell.Cell, index int) *cell.Cell {
	t.Helper()
	ref, err := node.MustBeginParse().PeekRefCellAt(index)
	if err != nil {
		t.Fatal(err)
	}
	return ref
}

// reaugmentedDispatchFork rebuilds a queue fork with its label kept bit for bit,
// the given children and a forged minimum lt.
func reaugmentedDispatchFork(t *testing.T, fork *cell.Cell, lt uint64, left, right *cell.Cell) *cell.Cell {
	t.Helper()
	loader := fork.MustBeginParse()
	labelBits := loader.BitsLeft() - 64
	label := loader.MustLoadSlice(labelBits)
	return cell.BeginCell().
		MustStoreSlice(label, labelBits).
		MustStoreRef(left).
		MustStoreRef(right).
		MustStoreUInt(lt, 64).
		EndCell()
}
