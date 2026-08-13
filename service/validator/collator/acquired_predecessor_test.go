package collator

import (
	"strings"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
)

// Recovering a durable session re-reads its predecessor out of storage and puts
// it through verifyPredecessor, and a failure there is fatal: the runtime aborts
// startup rather than joining consensus. The shard-prefix check inside it is the
// one that fired on the stand, and the shapes below are the ones a session
// recovered across a split or a merge actually hands it — a parent block whose
// state still holds both halves, and a child block holding one.
//
// They pass, which is the useful part: the check does not reject these on
// account of their topology, so a rejection in the field is about the state the
// store returned, not about the block being historical.
func TestVerifyPredecessorAcceptsHistoricalSplitAndMergeStates(t *testing.T) {
	t.Run("parent still holding both halves", func(t *testing.T) {
		for _, right := range []bool{false, true} {
			fixture := newSplitPredecessorFixture(t, right)
			if _, err := verifyPredecessor("acquired", &fixture.req.Previous); err != nil {
				t.Fatalf("acquired split parent rejected (right=%t): %v", right, err)
			}
		}
	})

	t.Run("children of a merge", func(t *testing.T) {
		fixture := newMergePredecessorFixture(t)
		if _, err := verifyPredecessor("acquired", &fixture.req.Previous); err != nil {
			t.Fatalf("acquired left merge child rejected: %v", err)
		}
		if fixture.req.Previous2 == nil {
			t.Fatal("merge fixture has no second predecessor")
		}
		if _, err := verifyPredecessor("acquired", fixture.req.Previous2); err != nil {
			t.Fatalf("acquired right merge child rejected: %v", err)
		}
	})
}

// The three ways the prefix check fails need three different responses — drop
// the session, wait for the state, or reject the block — and the runtime only
// ever sees the message. It used to say "wrong shard prefix" for all of them,
// which is why a stand failure could not be classified from the log.
func TestVerifyPredecessorNamesWhyTheAccountsFailed(t *testing.T) {
	fixture := newSplitPredecessorFixture(t, false)

	// A state whose accounts dictionary cannot be walked: the shape a narrow
	// proof or a partially available tree presents.
	previous := fixture.req.Previous
	previous.State = narrowedStateRoot(t, previous.State)
	_, err := verifyPredecessor("acquired", &previous)
	if err == nil {
		t.Fatal("a predecessor with no readable accounts was accepted")
	}
	// The decode fails before the prefix walk on a root-only proof, so the
	// message only has to name the state rather than the shard; what matters is
	// that it is not the bare prefix verdict.
	if strings.Contains(err.Error(), "do not match shard") && !strings.Contains(err.Error(), ":") {
		t.Fatalf("error does not carry a cause: %v", err)
	}

	// The two absences are kept apart on purpose. A masterchain predecessor
	// reaches this branch with the prefix comparison already vacuous — no prefix
	// bits — so the message is the only thing that says what happened, and
	// "never decoded" and "dictionary absent" point at different halves of the
	// system: one at whoever assembled the state, one at the decode.
	if err = verifyAccountsShardPrefix(mustPredecessorIdent(t, fixture.req.Shard), nil); err == nil ||
		!strings.Contains(err.Error(), "never decoded") {
		t.Fatalf("nil accounts field error = %v", err)
	}
	if err = verifyAccountsShardPrefix(
		mustPredecessorIdent(t, fixture.req.Shard),
		new(tlb.ShardAccountsAugDict),
	); err == nil || !strings.Contains(err.Error(), "dictionary is absent") {
		t.Fatalf("absent dictionary error = %v", err)
	}
}
