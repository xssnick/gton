package simplex

import (
	"math"
	"testing"
	"time"
)

func TestZeroFirstBlockTimeoutKeepsTargetRateDeadline(t *testing.T) {
	for _, update := range []bool{false, true} {
		name := "startup"
		if update {
			name = "update"
		}

		t.Run(name, func(t *testing.T) {
			params := DefaultParams()
			params.FirstBlockTimeout = 0
			initial := params
			if update {
				initial = DefaultParams()
			}
			env := newTestEnv(t, withLocal(1), withParams(initial))
			env.start()
			if update {
				if err := env.eng.UpdateParams(params); err != nil {
					t.Fatal(err)
				}
				// Enter a fresh window without locally timing out the old one,
				// so it takes the newly configured base timeout.
				for slot := uint32(0); slot < env.spw; slot++ {
					env.eng.HandleMessage(peer(2), 2, env.buildCert(SkipVote(slot), 0, 2, 3).Serialize())
				}
			}

			want := env.clock.Now().Add(params.TargetRate)
			requireEqual(t, env.eng.voter.alarmAt.Equal(want), true, "zero extra grace retains target rate")
			env.clock.set(want.Add(-time.Nanosecond))
			env.eng.Advance()
			requireEqual(t, env.trans.countVotes(VoteSkip), 0, "no premature skip")
			env.clock.set(want)
			env.eng.Advance()
			requireEqual(t, env.trans.countVotes(VoteSkip), int(env.spw), "window skipped at target rate")
			env.requireNoFatal()
		})
	}
}

func TestFirstBlockTimeoutScalingMatchesReference(t *testing.T) {
	timeouts := map[float64]time.Duration{
		0:               0,
		1e12:            100 * time.Second,
		math.MaxFloat32: 100 * time.Second,
	}
	for multiplier, wantTimeout := range timeouts {
		params := DefaultParams()
		params.FirstBlockTimeoutMultiplier = multiplier
		params.FirstBlockTimeoutCap = 100 * time.Second
		env := newTestEnv(t, withLocal(1), withParams(params))
		env.start()

		env.clock.set(env.eng.NextWakeup())
		env.eng.Advance()
		for slot := uint32(0); slot < env.spw; slot++ {
			env.deliverVote(2, SkipVote(slot))
			env.deliverVote(3, SkipVote(slot))
		}

		if got := env.eng.voter.firstBlockTimeout; got != wantTimeout {
			t.Fatalf("multiplier %g: first block timeout = %v, want %v", multiplier, got, wantTimeout)
		}
		want := env.clock.Now().Add(wantTimeout + params.TargetRate)
		requireEqual(t, env.eng.voter.alarmAt.Equal(want), true, "scaled deadline")
		env.requireNoFatal()
	}
}
