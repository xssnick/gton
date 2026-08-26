package hooks

import (
	"os"
	"runtime/debug"

	"github.com/rs/zerolog"
)

// validatorGCPercent is the garbage-collection target the validator and
// collator roles run at. The node's CPU gap against the reference validator is
// dominated by collection frequency, not by the live heap — the live heap was
// measured identical to 0.01% across GC settings — and raising the target from
// the Go default of 100 to 400 measured at −27% process CPU on the collation
// benchmark, at the price of the heap goal growing to roughly five times the
// live heap. That price is why this is raised by the roles that earn it and
// not by every node: a plain full node keeps the runtime default.
//
// Static on purpose. Flipping GOGC per slot was measured strictly worse than
// never raising it at all — 185.5 ms CPU per slot against 183.2, with the
// post-slot p90 degrading up to 26 ms — so the raise happens once, when the
// role is composed, and stays.
const validatorGCPercent = 400

// RaiseGCPercent lifts the runtime GC target to validatorGCPercent unless the
// operator has set GOGC in the environment — an explicit GOGC always wins,
// because that is what anyone debugging memory with standard Go tooling
// expects. Idempotent, so the validator and collator roles may both call it.
func RaiseGCPercent(log zerolog.Logger) {
	if env := os.Getenv("GOGC"); env != "" {
		log.Info().Str("gogc", env).Msg("keeping operator GOGC for the validator role")
		return
	}
	previous := debug.SetGCPercent(validatorGCPercent)
	if previous == validatorGCPercent {
		return
	}
	log.Info().
		Int("previous", previous).
		Int("gc_percent", validatorGCPercent).
		Msg("raised runtime GC target for the validator role")
}
