package collator

import "time"

// collationPace carries the wall-clock spans that Collator::check_block_overload
// consults before it decides whether a block was overloaded because collation
// itself took too long. A shard whose collation is CPU-bound has to split even
// when every block-limit axis stayed below its soft threshold: the limits
// measure the block that was produced, not the time it took to produce it, so a
// shard that can no longer collate within its slot looks "normally loaded"
// to them forever.
//
// The spans are wall time in the reference node as well — the CPU timers it
// keeps elsewhere do not feed this decision — so a collation stalled on a lock
// or on storage counts exactly like one stalled on the executor. That is the
// intent: both mean the shard is not keeping up.
//
// A zero value is inert, which is what every deterministic entry point (a
// direct BuildShard, every test that constructs a collation by hand) leaves in
// place. Only the live schedule that can actually wait for external messages
// installs one, mirroring the params_.wait_externals_until gate around the
// whole heuristic in C++.
type collationPace struct {
	// started is the instant the node began assembling this candidate, taken
	// before acquisition rather than before collation: collator_started_at_ is
	// a member initializer, so it runs when the actor is constructed and the
	// span it opens includes waiting for the predecessor and masterchain
	// states.
	started time.Time
	// body is the instant deterministic collation began, after acquisition
	// returned — do_collate_started_at_, assigned at the head of
	// do_collate_inner. A size-limit retry re-enters that function, so this is
	// per attempt while started is not.
	body time.Time
	// externalWait reports the time spent blocked on ready external messages so
	// far, mirroring wait_externals_total_time_. It is a function because the
	// live schedule keeps adding to the accumulator until the last batch is
	// taken, and this is read at finish.
	externalWait func() time.Duration
}

// collationPaceMinTotal is the C++ total_time > 0.1 guard. Below it the ratios
// are noise: a fast collation can spend most of a millisecond anywhere.
const collationPaceMinTotal = 100 * time.Millisecond

// longCollation reports the two independent C++ predicates. They are not
// symmetric and must not be collapsed into one: overload demands that external
// waiting was under a fifth of the slot, while the underload suppression only
// demands it was under seven tenths. Between those two ratios a block is
// neither declared overloaded nor allowed to claim it is idle.
func (p collationPace) longCollation(now time.Time) (overload bool, underload bool) {
	if p.started.IsZero() || p.body.IsZero() || p.externalWait == nil {
		return false, false
	}
	total := now.Sub(p.started)
	if total <= collationPaceMinTotal {
		return false, false
	}
	// do_collate_time > total_time * 0.6, in exact integer arithmetic.
	if body := now.Sub(p.body); body*5 <= total*3 {
		return false, false
	}
	wait := p.externalWait()

	return wait*5 < total, wait*10 < total*7
}
