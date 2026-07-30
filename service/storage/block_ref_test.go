package storage

import (
	"bytes"
	"testing"

	"github.com/rs/zerolog"
	"github.com/xssnick/tonutils-go/ton"
)

func testBlockRefID() ton.BlockIDExt {
	return ton.BlockIDExt{
		Workchain: -1,
		Shard:     -1 << 63,
		SeqNo:     4242,
		RootHash:  bytes.Repeat([]byte{0x11}, 32),
		FileHash:  bytes.Repeat([]byte{0x22}, 32),
	}
}

type countingStringer struct {
	calls *int
	value string
}

func (c countingStringer) String() string {
	*c.calls++
	return c.value
}

func TestBlockRefStringMatchesFormatBlockRef(t *testing.T) {
	block := testBlockRefID()
	if got, want := BlockRef(block).String(), FormatBlockRef(block); got != want {
		t.Fatalf("BlockRef.String() = %q, want %q", got, want)
	}
}

func TestBlockRefIsFormattedForEnabledEvent(t *testing.T) {
	var out bytes.Buffer
	logger := zerolog.New(&out).Level(zerolog.DebugLevel)
	block := testBlockRefID()

	logger.Debug().Stringer("block", BlockRef(block)).Msg("enabled")
	if !bytes.Contains(out.Bytes(), []byte(FormatBlockRef(block))) {
		t.Fatalf("enabled debug event = %q, want it to contain %q", out.String(), FormatBlockRef(block))
	}
}

// Hot paths log block refs through Event.Stringer instead of a pre-formatted
// Str value, which only pays off because zerolog returns early on a disabled
// event and never calls String(). Pin that contract so a logger upgrade cannot
// silently put the formatting back on the block ingest path.
func TestDisabledEventDoesNotInvokeStringer(t *testing.T) {
	logger := zerolog.New(zerolog.Nop()).Level(zerolog.InfoLevel)

	calls := 0
	logger.Debug().Stringer("block", countingStringer{calls: &calls, value: "x"}).Msg("disabled")
	if calls != 0 {
		t.Fatalf("disabled debug event called String() %d times, want 0", calls)
	}

	var out bytes.Buffer
	enabled := zerolog.New(&out).Level(zerolog.DebugLevel)
	enabled.Debug().Stringer("block", countingStringer{calls: &calls, value: "x"}).Msg("enabled")
	if calls != 1 {
		t.Fatalf("enabled debug event called String() %d times, want 1", calls)
	}
}

func TestBlockRefAllocatesLessThanEagerFormatting(t *testing.T) {
	logger := zerolog.New(zerolog.Nop()).Level(zerolog.InfoLevel)
	block := testBlockRefID()

	lazy := testing.AllocsPerRun(200, func() {
		logger.Debug().Stringer("block", BlockRef(block)).Msg("disabled")
	})
	eager := testing.AllocsPerRun(200, func() {
		logger.Debug().Str("block", FormatBlockRef(block)).Msg("disabled")
	})
	if lazy >= eager {
		t.Fatalf("lazy block ref allocated %.0f times per disabled event, eager formatting %.0f; want fewer", lazy, eager)
	}
}
