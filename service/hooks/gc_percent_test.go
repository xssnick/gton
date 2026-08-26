package hooks

import (
	"runtime/debug"
	"testing"

	"github.com/rs/zerolog"
)

// The raise must not fight the operator: an explicit GOGC in the environment
// is kept exactly as set, because it is the one knob every Go memory
// investigation reaches for first.
func TestRaiseGCPercentKeepsOperatorGOGC(t *testing.T) {
	restore := debug.SetGCPercent(123)
	defer debug.SetGCPercent(restore)
	t.Setenv("GOGC", "250")

	RaiseGCPercent(zerolog.Nop())

	if got := debug.SetGCPercent(-1); got != 123 {
		t.Fatalf("GC percent = %d after RaiseGCPercent under operator GOGC, want the untouched 123", got)
	}
	debug.SetGCPercent(123)
}

func TestRaiseGCPercentLiftsTheDefault(t *testing.T) {
	restore := debug.SetGCPercent(100)
	defer debug.SetGCPercent(restore)
	t.Setenv("GOGC", "")

	RaiseGCPercent(zerolog.Nop())

	if got := debug.SetGCPercent(-1); got != validatorGCPercent {
		t.Fatalf("GC percent = %d, want %d", got, validatorGCPercent)
	}
	debug.SetGCPercent(validatorGCPercent)
}
