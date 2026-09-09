package collator

import (
	"fmt"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
)

func TestDispatchOptionalPhasesPreservePredecessorAccounts(t *testing.T) {
	for _, total := range []uint32{2, 20} {
		t.Run(fmt.Sprint(total), func(t *testing.T) {
			a, b := repeatedDispatchAccount(0x81), repeatedDispatchAccount(0x82)
			queue := makeDispatchQueue(t,
				dispatchFixtureAccount{accountID: a, lts: []uint64{10, 30, 50, 70}},
				dispatchFixtureAccount{accountID: b, lts: []uint64{20, 40, 60, 80}},
			)
			original := queue.RootCell().HashKey()
			policy := DispatchPolicy{
				DeferMessagesAfter: 100, Phase2MaxTotal: total, Phase2MaxPerInitiator: 100,
			}
			for range 2 {
				c := dispatchTestCollation(t, &tlb.DispatchQueueAugDict{AugmentedDictionary: queue.Copy()}, policy)
				if err := c.processDispatchQueue(); err != nil {
					t.Fatal(err)
				}
				if got := c.stats.DispatchedMessages; got != min(total+2, 8) {
					t.Fatalf("dispatched %d messages", got)
				}
				for id, account := range c.oldDispatchAccounts {
					if account.Count != 4 {
						t.Fatalf("predecessor account %x count changed to %d", id, account.Count)
					}
					messages, err := account.Messages.LoadAll()
					if err != nil || len(messages) != 4 {
						t.Fatalf("predecessor account %x messages changed: count=%d err=%v", id, len(messages), err)
					}
				}
				if queue.RootCell().HashKey() != original || c.oldDispatchQueue.RootCell().HashKey() != original {
					t.Fatal("dispatch processing changed the shared predecessor")
				}
				if total == 2 {
					assertDispatchLTs(t, c.dispatchQueue, a, []uint64{50, 70})
					assertDispatchLTs(t, c.dispatchQueue, b, []uint64{60, 80})
				} else if !c.dispatchQueue.IsEmpty() {
					t.Fatal("dispatch queue was not drained")
				}
			}
		})
	}
}

// Includes account selection, message preparation, queue mutations and proof
// checkpoints. The immutable predecessor is built outside the timed loop.
func BenchmarkProcessDispatchQueue(b *testing.B) {
	for _, accountCount := range []int{4, 32} {
		for _, optional := range []bool{false, true} {
			b.Run(fmt.Sprintf("accounts=%d/optional=%t", accountCount, optional), func(b *testing.B) {
				accounts := make([]dispatchFixtureAccount, accountCount)
				for i := range accounts {
					accounts[i].accountID = benchmarkDispatchAccount(i)
					for j := range 16 {
						accounts[i].lts = append(accounts[i].lts, uint64(j*accountCount+i+1))
					}
				}
				queue := makeDispatchQueue(b, accounts...)
				policy := DispatchPolicy{DeferMessagesAfter: 100}
				want := uint32(accountCount)
				if optional {
					policy.Phase2MaxTotal = uint32(accountCount * 7)
					policy.Phase2MaxPerInitiator = 1000
					policy.Phase3MaxTotal = uint32(accountCount * 8)
					policy.Phase3MaxPerInitiator = 1000
					want *= 16
				}

				b.ReportAllocs()
				for b.Loop() {
					c := dispatchTestCollation(b, &tlb.DispatchQueueAugDict{AugmentedDictionary: queue.Copy()}, policy)
					if err := c.processDispatchQueue(); err != nil {
						b.Fatal(err)
					}
					if c.stats.DispatchedMessages != want {
						b.Fatalf("dispatched %d messages, want %d", c.stats.DispatchedMessages, want)
					}
				}
				b.ReportMetric(float64(want), "messages/op")
			})
		}
	}
}
