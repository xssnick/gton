package collator

import (
	"bytes"
	"errors"
	"maps"
	"math"
	"math/rand"
	"testing"

	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestDiscountedCounterValidation(t *testing.T) {
	tests := []struct {
		name    string
		counter discountedCounter
		wantErr bool
	}{
		{
			name: "zero",
		},
		{
			name:    "zero with update time",
			counter: discountedCounter{lastUpdated: 17},
		},
		{
			name:    "zero total with short component",
			counter: discountedCounter{lastUpdated: 17, count2048: 1},
			wantErr: true,
		},
		{
			name:    "zero total with long component",
			counter: discountedCounter{lastUpdated: 17, count65536: 1},
			wantErr: true,
		},
		{
			name:    "non-zero total without update time",
			counter: discountedCounter{total: 1},
			wantErr: true,
		},
		{
			name: "non-zero total",
			counter: discountedCounter{
				lastUpdated: 17,
				total:       3,
				count2048:   2 << 32,
				count65536:  3 << 32,
			},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			err := test.counter.validate()
			if (err != nil) != test.wantErr {
				t.Fatalf("validate error = %v, want error %v", err, test.wantErr)
			}
		})
	}
}

func TestDiscountedCounterIncrease(t *testing.T) {
	t.Run("initialize", func(t *testing.T) {
		var counter discountedCounter
		if err := counter.increaseBy(3, 100); err != nil {
			t.Fatal(err)
		}
		want := discountedCounter{
			lastUpdated: 100,
			total:       3,
			count2048:   3 << 32,
			count65536:  3 << 32,
		}
		if counter != want {
			t.Fatalf("counter = %+v, want %+v", counter, want)
		}
	})

	t.Run("same time", func(t *testing.T) {
		counter := discountedCounter{lastUpdated: 100, total: 4, count2048: 8 << 32, count65536: 9 << 32}
		if err := counter.increaseBy(2, 100); err != nil {
			t.Fatal(err)
		}
		if counter.lastUpdated != 100 || counter.total != 6 ||
			counter.count2048 != 10<<32 || counter.count65536 != 11<<32 {
			t.Fatalf("counter = %+v", counter)
		}
	})

	t.Run("time reversal clamps decay", func(t *testing.T) {
		counter := discountedCounter{lastUpdated: 100, total: 4, count2048: 8 << 32, count65536: 9 << 32}
		if err := counter.increaseBy(2, 90); err != nil {
			t.Fatal(err)
		}
		if counter.lastUpdated != 90 || counter.total != 6 ||
			counter.count2048 != 10<<32 || counter.count65536 != 11<<32 {
			t.Fatalf("counter = %+v", counter)
		}
	})

	t.Run("decay", func(t *testing.T) {
		counter := discountedCounter{lastUpdated: 100, total: 4, count2048: 8 << 32, count65536: 9 << 32}
		if err := counter.increaseBy(2, 125); err != nil {
			t.Fatal(err)
		}
		scaled := uint64(2) << 32
		if counter.lastUpdated != 125 || counter.total != 6 ||
			counter.count2048 != decayDiscountedValue(8<<32, 25<<5)+scaled ||
			counter.count65536 != decayDiscountedValue(9<<32, 25)+scaled {
			t.Fatalf("counter = %+v", counter)
		}
	})

	t.Run("short window cutoff", func(t *testing.T) {
		counter := discountedCounter{lastUpdated: 1, total: 1, count2048: 1 << 60, count65536: 1 << 60}
		now := uint32(1 + 48*2048)
		if err := counter.increaseBy(0, now); err != nil {
			t.Fatal(err)
		}
		if counter.count2048 != 0 || counter.count65536 != decayDiscountedValue(1<<60, now-1) {
			t.Fatalf("counter = %+v", counter)
		}
	})

	t.Run("zero relaxation", func(t *testing.T) {
		var counter discountedCounter
		if err := counter.increaseBy(0, 77); err != nil {
			t.Fatal(err)
		}
		if counter != (discountedCounter{lastUpdated: 77}) {
			t.Fatalf("counter = %+v", counter)
		}
	})

	t.Run("unix epoch would invalidate result", func(t *testing.T) {
		var counter discountedCounter
		if err := counter.increaseBy(1, 0); err == nil {
			t.Fatal("non-zero counter accepted unix time zero")
		}
	})
}

func TestDiscountedCounterOverflow(t *testing.T) {
	tests := []struct {
		name    string
		counter discountedCounter
	}{
		{
			name:    "total",
			counter: discountedCounter{lastUpdated: 1, total: math.MaxUint64},
		},
		{
			name:    "short component",
			counter: discountedCounter{lastUpdated: 1, total: 1, count2048: math.MaxUint64},
		},
		{
			name:    "long component",
			counter: discountedCounter{lastUpdated: 1, total: 1, count65536: math.MaxUint64},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			before := test.counter
			if err := test.counter.increaseBy(1, 100_000); err == nil {
				t.Fatal("overflowing increment succeeded")
			}
			if test.counter != before {
				t.Fatalf("failed increment mutated counter: %+v", test.counter)
			}
		})
	}
}

func TestDecayDiscountedValueMatchesCPP(t *testing.T) {
	// Golden values of the canonical fixed-point exponential.
	tests := []struct {
		value    uint64
		exponent uint32
		want     uint64
	}{
		{value: 3_167_801_306_015_831_286, exponent: 4_003, want: 2_980_099_890_648_636_481},
		{value: 1_583_900_653_007_915_643, exponent: 4_003, want: 1_490_049_945_324_318_240},
		{value: 9_094_494_907_266_047_891, exponent: 17_239, want: 6_990_995_826_652_297_465},
		{value: 5_487_867_407_433_215_099, exponent: 239_017, want: 143_048_684_491_504_152},
		{value: 46_462_010_749_955_243, exponent: 239_017, want: 1_211_095_134_625_318},
		{value: 390_263_500_024_095_125, exponent: 2_700_001, want: 1},
	}

	for _, test := range tests {
		got := decayDiscountedValue(test.value, test.exponent)
		if !uint64DistanceAtMostOne(got, test.want) {
			t.Fatalf("decay(%d, %d) = %d, want %d", test.value, test.exponent, got, test.want)
		}
	}
	if got := decayDiscountedValue(math.MaxUint64, 46<<16); got != 0 {
		t.Fatalf("decay past the table range = %d, want 0", got)
	}
}

func TestDiscountedCounterApproximationPredicates(t *testing.T) {
	base := discountedCounter{lastUpdated: 10, total: 3, count2048: math.MaxUint64, count65536: 20}
	neighbor := base
	neighbor.count2048--
	neighbor.count65536++
	if !base.almostEqual(neighbor) {
		t.Fatal("neighboring discounted components are not almost equal")
	}
	neighbor.count65536++
	if base.almostEqual(neighbor) {
		t.Fatal("components differing by two are almost equal")
	}
	if !(discountedCounter{count2048: 1, count65536: 1}).almostZero() {
		t.Fatal("unit discounted components are not almost zero")
	}
	if (discountedCounter{count2048: 2}).almostZero() {
		t.Fatal("non-trivial discounted component is almost zero")
	}
}

func TestBlockCreateStatsRoundTrip(t *testing.T) {
	firstKey := creatorStatsTestKey(0x11)
	secondKey := creatorStatsTestKey(0x22)
	entries := map[[32]byte]creatorStats{
		secondKey: {
			masterchain: discountedCounter{lastUpdated: 900, total: 2, count2048: 3 << 32, count65536: 4 << 32},
			shardchain:  discountedCounter{lastUpdated: 901, total: 5, count2048: 6 << 32, count65536: 7 << 32},
		},
		firstKey: {
			masterchain: discountedCounter{lastUpdated: 77},
		},
	}
	root := blockCreateStatsTestCell(t, entries)
	loader := root.MustBeginParse()
	if tag := loader.MustLoadUInt(8); tag != blockCreateStatsTag {
		t.Fatalf("tag = %x", tag)
	}
	if dict := loader.MustLoadDict(256); !dict.ValidateAll() || loader.BitsLeft() != 0 || loader.RefsNum() != 0 {
		t.Fatal("malformed serialized block creator statistics")
	}
	parsed, err := openBlockCreateStats(root)
	if err != nil {
		t.Fatal(err)
	}
	if got := blockCreateStatsTestEntries(t, parsed); !maps.Equal(got, entries) {
		t.Fatalf("round trip = %+v, want %+v", got, entries)
	}

	reversed := make(map[[32]byte]creatorStats, len(entries))
	reversed[firstKey] = entries[firstKey]
	reversed[secondKey] = entries[secondKey]
	secondRoot := blockCreateStatsTestCell(t, reversed)
	firstBOC, err := root.ToBOCWithOptionsErr(cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	secondBOC, err := secondRoot.ToBOCWithOptionsErr(cell.BOCSerializeOptions{WithCRC32C: true})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(firstBOC, secondBOC) {
		t.Fatal("map insertion order changed BlockCreateStats BOC")
	}
}

// TestOpenBlockCreateStatsRejectsMalformedHeader covers what the cheap open
// still decides on its own: the wrapper around the dictionary.
func TestOpenBlockCreateStatsRejectsMalformedHeader(t *testing.T) {
	zero := discountedCounter{}
	validCreator := creatorStatsTestCell(t, creatorStatsTag, zero, zero, false)
	validRoot := blockCreateStatsTestRoot(t, blockCreateStatsTag, validCreator)
	tests := []struct {
		name string
		root *cell.Cell
	}{
		{
			name: "wrong outer tag",
			root: blockCreateStatsTestRoot(t, 0x34, validCreator),
		},
		{
			name: "outer trailing data",
			root: cell.BeginCell().MustStoreBuilder(validRoot.ToBuilder()).MustStoreBoolBit(true).EndCell(),
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			if _, err := openBlockCreateStats(test.root); !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

// TestVerifyBlockCreateStatsUpdateRejectsMalformedEntries pins where entry level
// validation moved to. Opening the dictionary no longer walks it, so a malformed
// creator entry has to be rejected by the pass that actually reads it — the diff
// against the predecessor, which is the only place a candidate can introduce one.
func TestVerifyBlockCreateStatsUpdateRejectsMalformedEntries(t *testing.T) {
	zero := discountedCounter{}
	tests := []struct {
		name  string
		value *cell.Cell
	}{
		{
			name:  "wrong creator tag",
			value: creatorStatsTestCell(t, 0x5, zero, zero, false),
		},
		{
			name:  "creator trailing data",
			value: creatorStatsTestCell(t, creatorStatsTag, zero, zero, true),
		},
		{
			name: "zero total with component",
			value: creatorStatsTestCell(t, creatorStatsTag,
				discountedCounter{lastUpdated: 1, count2048: 1}, zero, false),
		},
		{
			name:  "non-zero total without time",
			value: creatorStatsTestCell(t, creatorStatsTag, zero, discountedCounter{total: 1}, false),
		},
	}

	previous := blockCreateStatsTestStats(t, nil)
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			next, err := openBlockCreateStats(blockCreateStatsTestRoot(t, blockCreateStatsTag, test.value))
			if err != nil {
				t.Fatal(err)
			}
			err = verifyBlockCreateStatsUpdate(previous, next, 1_000, nil, [32]byte{})
			if !errors.Is(err, ErrInvalidInput) {
				t.Fatalf("error = %v, want ErrInvalidInput", err)
			}
		})
	}
}

// TestVerifyBlockCreateStatsUpdateCatchesSkippedIncrement is the converse of the
// diff pass: an entry the candidate left untouched is skipped by hash, so the
// explicit increment sweep is what has to notice it was required to move.
func TestVerifyBlockCreateStatsUpdateCatchesSkippedIncrement(t *testing.T) {
	creator := creatorStatsTestKey(0x61)
	other := creatorStatsTestKey(0x62)
	entry := creatorStats{
		masterchain: discountedCounter{lastUpdated: 100, total: 3, count2048: 3 << 32, count65536: 3 << 32},
	}
	aggregate := creatorStats{
		masterchain: discountedCounter{lastUpdated: 100, total: 7, count2048: 7 << 32, count65536: 7 << 32},
		shardchain:  discountedCounter{lastUpdated: 100, total: 9, count2048: 9 << 32, count65536: 9 << 32},
	}
	entries := map[[32]byte]creatorStats{creator: entry, other: entry, [32]byte{}: aggregate}
	previous := blockCreateStatsTestStats(t, entries)

	// The candidate moved the aggregate but silently left the creator's own
	// counter alone, so the diff never reports that key.
	next := maps.Clone(entries)
	moved := entries[[32]byte{}]
	if err := moved.masterchain.increaseBy(1, 150); err != nil {
		t.Fatal(err)
	}
	next[[32]byte{}] = moved
	if err := verifyBlockCreateStatsUpdate(
		previous, blockCreateStatsTestStats(t, next), 150, nil, creator,
	); err == nil {
		t.Fatal("a creator that skipped its own increment was accepted")
	}

	// The same candidate with the creator's counter moved as well is valid.
	updated := entries[creator]
	if err := updated.masterchain.increaseBy(1, 150); err != nil {
		t.Fatal(err)
	}
	next[creator] = updated
	if err := verifyBlockCreateStatsUpdate(
		previous, blockCreateStatsTestStats(t, next), 150, nil, creator,
	); err != nil {
		t.Fatalf("valid update rejected: %v", err)
	}
}

func TestUpdateBlockCreateStats(t *testing.T) {
	creatorA := creatorStatsTestKey(0x11)
	creatorB := creatorStatsTestKey(0x22)
	creatorC := creatorStatsTestKey(0x33)
	unrelated := creatorStatsTestKey(0x44)
	oldA := creatorStats{
		masterchain: discountedCounter{lastUpdated: 700, total: 10, count2048: 12 << 32, count65536: 13 << 32},
		shardchain:  discountedCounter{lastUpdated: 900, total: 5, count2048: 6 << 32, count65536: 7 << 32},
	}
	oldUnrelated := creatorStats{
		masterchain: discountedCounter{lastUpdated: 800, total: 2, count2048: 2 << 32, count65536: 2 << 32},
	}
	previous := blockCreateStatsTestStats(t, map[[32]byte]creatorStats{
		creatorA:  oldA,
		unrelated: oldUnrelated,
	})

	root, err := updateBlockCreateStats(blockCreateStatsInput{
		enabled:  true,
		previous: previous,
		now:      1_000,
		shardBlockCreators: map[[32]byte]uint32{
			creatorA:   2,
			creatorB:   1,
			creatorC:   0,
			[32]byte{}: 99,
		},
		masterchainCreator: creatorB,
	})
	if err != nil {
		t.Fatal(err)
	}
	opened, err := openBlockCreateStats(root)
	if err != nil {
		t.Fatal(err)
	}
	updated := blockCreateStatsTestEntries(t, opened)
	if len(updated) != 4 {
		t.Fatalf("entries = %d, want creators A/B, aggregate, and unrelated", len(updated))
	}

	entryA := updated[creatorA]
	if entryA.masterchain != oldA.masterchain || entryA.shardchain.lastUpdated != 1_000 ||
		entryA.shardchain.total != oldA.shardchain.total+2 ||
		entryA.shardchain.count2048 != decayDiscountedValue(oldA.shardchain.count2048, 100<<5)+2<<32 ||
		entryA.shardchain.count65536 != decayDiscountedValue(oldA.shardchain.count65536, 100)+2<<32 {
		t.Fatalf("creator A = %+v", entryA)
	}
	entryB := updated[creatorB]
	wantUnit := uint64(1) << 32
	if entryB.masterchain != (discountedCounter{lastUpdated: 1_000, total: 1, count2048: wantUnit, count65536: wantUnit}) ||
		entryB.shardchain != (discountedCounter{lastUpdated: 1_000, total: 1, count2048: wantUnit, count65536: wantUnit}) {
		t.Fatalf("creator B = %+v", entryB)
	}
	aggregate := updated[[32]byte{}]
	if aggregate.masterchain.total != 1 || aggregate.shardchain.total != 3 ||
		aggregate.masterchain.count2048 != wantUnit || aggregate.shardchain.count2048 != 3<<32 {
		t.Fatalf("aggregate = %+v", aggregate)
	}
	if updated[unrelated] != oldUnrelated {
		t.Fatal("unrelated creator statistics changed")
	}
	if _, exists := updated[creatorC]; exists {
		t.Fatal("zero-count creator was inserted")
	}
}

// TestUpdateBlockCreateStatsMatchesFullRebuild is the bit-exactness guard for
// the incremental collation path. Mutating the predecessor dictionary in place
// keeps its untouched subtrees, while the reference serializes every entry from
// scratch; a canonical Hashmap is determined by its key set, so the two must
// agree on the root hash for every shape of update — insert, modify, and a
// predecessor whose keys share deep prefixes with the new ones.
func TestUpdateBlockCreateStatsMatchesFullRebuild(t *testing.T) {
	rnd := rand.New(rand.NewSource(20260814))

	for round := range 200 {
		previousEntries := map[[32]byte]creatorStats{}
		for range rnd.Intn(40) {
			var key [32]byte
			rnd.Read(key[:])
			if rnd.Intn(3) == 0 {
				// Share a long prefix with a sibling so the update lands inside
				// an existing edge label rather than always at the root.
				key[0], key[1], key[2] = 0xaa, 0xbb, byte(rnd.Intn(4))
			}
			previousEntries[key] = creatorStats{
				masterchain: discountedCounter{
					lastUpdated: uint32(1_000 + rnd.Intn(500)),
					total:       uint64(1 + rnd.Intn(50)),
					count2048:   uint64(1+rnd.Intn(50)) << 32,
					count65536:  uint64(1+rnd.Intn(50)) << 32,
				},
			}
		}

		shardCreators := map[[32]byte]uint32{}
		existing := sortedCreatorKeys(previousEntries)
		for i := 0; i < rnd.Intn(6) && i < len(existing); i++ {
			shardCreators[existing[rnd.Intn(len(existing))]] = uint32(1 + rnd.Intn(4))
		}
		for range rnd.Intn(4) {
			var key [32]byte
			rnd.Read(key[:])
			if rnd.Intn(2) == 0 {
				key[0], key[1], key[2] = 0xaa, 0xbb, byte(rnd.Intn(8))
			}
			shardCreators[key] = uint32(1 + rnd.Intn(4))
		}
		var masterCreator [32]byte
		if rnd.Intn(2) == 0 && len(existing) != 0 {
			masterCreator = existing[rnd.Intn(len(existing))]
		} else {
			rnd.Read(masterCreator[:])
		}
		now := uint32(2_000 + rnd.Intn(1_000))

		root, err := updateBlockCreateStats(blockCreateStatsInput{
			enabled:            true,
			previous:           blockCreateStatsTestStats(t, previousEntries),
			now:                now,
			shardBlockCreators: shardCreators,
			masterchainCreator: masterCreator,
		})
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}

		// Reference: apply the same increments to a plain map and serialize the
		// whole dictionary from nothing.
		wantEntries := maps.Clone(previousEntries)
		increments, err := creatorStatsIncrements(shardCreators, masterCreator)
		if err != nil {
			t.Fatalf("round %d: %v", round, err)
		}
		for _, creator := range sortedCreatorKeys(increments) {
			increment := increments[creator]
			entry := wantEntries[creator]
			if increment.masterchain != 0 {
				if err = entry.masterchain.increaseBy(increment.masterchain, now); err != nil {
					t.Fatalf("round %d: %v", round, err)
				}
			}
			if increment.shardchain != 0 {
				if err = entry.shardchain.increaseBy(increment.shardchain, now); err != nil {
					t.Fatalf("round %d: %v", round, err)
				}
			}
			wantEntries[creator] = entry
		}

		if got, want := root.HashKey(), blockCreateStatsTestCell(t, wantEntries).HashKey(); got != want {
			t.Fatalf("round %d: incremental update root %x, full rebuild root %x", round, got, want)
		}
	}
}

func TestUpdateBlockCreateStatsZeroCreatorAndOrdering(t *testing.T) {
	creatorA := creatorStatsTestKey(0x11)
	creatorB := creatorStatsTestKey(0x22)
	first := map[[32]byte]uint32{creatorA: 2, creatorB: 3, [32]byte{}: 100}
	second := make(map[[32]byte]uint32, len(first))
	second[[32]byte{}] = 100
	second[creatorB] = 3
	second[creatorA] = 2

	build := func(t *testing.T, creators map[[32]byte]uint32) *cell.Cell {
		t.Helper()
		root, err := updateBlockCreateStats(blockCreateStatsInput{
			enabled:            true,
			now:                500,
			shardBlockCreators: creators,
		})
		if err != nil {
			t.Fatal(err)
		}
		return root
	}
	firstRoot := build(t, first)
	secondRoot := build(t, second)
	if firstRoot.HashKey() != secondRoot.HashKey() {
		t.Fatal("creator map insertion order changed output root")
	}
	stats, err := openBlockCreateStats(firstRoot)
	if err != nil {
		t.Fatal(err)
	}
	aggregate := blockCreateStatsTestEntries(t, stats)[[32]byte{}]
	if aggregate.masterchain.total != 0 || aggregate.shardchain.total != 5 {
		t.Fatalf("aggregate = %+v, zero creator must be ignored", aggregate)
	}
}

func TestVerifyBlockCreateStatsUpdateCppTolerance(t *testing.T) {
	creator := creatorStatsTestKey(0x51)
	previousEntry := creatorStats{
		masterchain: discountedCounter{
			lastUpdated: 100,
			total:       3,
			count2048:   4 << 32,
			count65536:  5 << 32,
		},
	}
	previous := blockCreateStatsTestStats(t, map[[32]byte]creatorStats{
		creator: previousEntry,
	})
	expected := previousEntry
	if err := expected.masterchain.increaseBy(1, 150); err != nil {
		t.Fatal(err)
	}
	expected.masterchain.count2048++
	expected.masterchain.count65536--
	nextEntries := map[[32]byte]creatorStats{
		creator: expected,
		[32]byte{}: {
			masterchain: discountedCounter{
				lastUpdated: 150,
				total:       1,
				count2048:   1 << 32,
				count65536:  1 << 32,
			},
		},
	}
	next := blockCreateStatsTestStats(t, nextEntries)
	if err := verifyBlockCreateStatsUpdate(previous, next, 150, nil, creator); err != nil {
		t.Fatalf("+/-1 result rejected: %v", err)
	}

	invalidEntries := maps.Clone(nextEntries)
	entry := invalidEntries[creator]
	entry.masterchain.count2048++
	entry.masterchain.count2048++
	invalidEntries[creator] = entry
	invalid := blockCreateStatsTestStats(t, invalidEntries)
	if err := verifyBlockCreateStatsUpdate(previous, invalid, 150, nil, creator); err == nil {
		t.Fatal("discounted counter outside the +/-1 tolerance was accepted")
	}
}

func TestVerifyBlockCreateStatsUpdateCppPruning(t *testing.T) {
	creator := creatorStatsTestKey(0x52)
	previous := blockCreateStatsTestStats(t, map[[32]byte]creatorStats{
		creator: {
			masterchain: discountedCounter{
				lastUpdated: 1,
				total:       1,
				count2048:   1 << 32,
				count65536:  1 << 32,
			},
		},
	})
	empty := blockCreateStatsTestStats(t, nil)
	if err := verifyBlockCreateStatsUpdate(previous, empty, 4_000_000, nil, [32]byte{}); err != nil {
		t.Fatalf("stale creator pruning rejected: %v", err)
	}
	if err := verifyBlockCreateStatsUpdate(previous, empty, 2, nil, [32]byte{}); err == nil {
		t.Fatal("non-stale creator pruning was accepted")
	}
	// Absent statistics stay distinguishable from empty ones.
	if err := verifyBlockCreateStatsUpdate(previous, blockCreateStats{}, 4_000_000, nil, [32]byte{}); err == nil {
		t.Fatal("absent resulting statistics were accepted")
	}
}

func TestUpdateBlockCreateStatsDisabled(t *testing.T) {
	creatorA := creatorStatsTestKey(0x11)
	creatorB := creatorStatsTestKey(0x22)
	root, err := updateBlockCreateStats(blockCreateStatsInput{
		enabled: false,
		previous: blockCreateStatsTestStats(t, map[[32]byte]creatorStats{
			creatorA: {masterchain: discountedCounter{lastUpdated: 1, total: 1}},
		}),
		shardBlockCreators: map[[32]byte]uint32{
			creatorA: math.MaxUint32,
			creatorB: 1,
		},
	})
	if err != nil || root != nil {
		t.Fatalf("disabled update = (%v, %v), want nil, nil", root, err)
	}
}

func TestUpdateBlockCreateStatsRejectsInvalidInput(t *testing.T) {
	creatorA := creatorStatsTestKey(0x11)
	creatorB := creatorStatsTestKey(0x22)
	t.Run("aggregate overflow", func(t *testing.T) {
		_, err := updateBlockCreateStats(blockCreateStatsInput{
			enabled: true,
			now:     1,
			shardBlockCreators: map[[32]byte]uint32{
				creatorA: math.MaxUint32,
				creatorB: 1,
			},
		})
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("error = %v, want ErrInvalidInput", err)
		}
	})

	t.Run("counter overflow", func(t *testing.T) {
		previous := blockCreateStatsTestStats(t, map[[32]byte]creatorStats{
			creatorA: {
				masterchain: discountedCounter{lastUpdated: 1, total: math.MaxUint64},
			},
		})
		_, err := updateBlockCreateStats(blockCreateStatsInput{
			enabled:            true,
			previous:           previous,
			now:                100,
			masterchainCreator: creatorA,
		})
		if !errors.Is(err, ErrInvalidInput) {
			t.Fatalf("error = %v, want ErrInvalidInput", err)
		}
	})
}

func creatorStatsTestKey(fill byte) [32]byte {
	var key [32]byte
	for i := range key {
		key[i] = fill
	}
	return key
}

func creatorStatsTestCell(
	t *testing.T,
	tag uint64,
	masterchain discountedCounter,
	shardchain discountedCounter,
	trailing bool,
) *cell.Cell {
	t.Helper()

	builder := cell.BeginCell().MustStoreUInt(tag, 4).
		MustStoreUInt(uint64(masterchain.lastUpdated), 32).
		MustStoreUInt(masterchain.total, 64).
		MustStoreUInt(masterchain.count2048, 64).
		MustStoreUInt(masterchain.count65536, 64).
		MustStoreUInt(uint64(shardchain.lastUpdated), 32).
		MustStoreUInt(shardchain.total, 64).
		MustStoreUInt(shardchain.count2048, 64).
		MustStoreUInt(shardchain.count65536, 64)
	if trailing {
		builder.MustStoreBoolBit(true)
	}
	return builder.EndCell()
}

// blockCreateStatsTestCell serializes a whole creator dictionary from scratch.
// Production no longer does this — it mutates the predecessor in place — so the
// helper doubles as the independent reference the incremental update is
// compared against.
func blockCreateStatsTestCell(t *testing.T, entries map[[32]byte]creatorStats) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(creatorStatsKeyBits)
	for _, creator := range sortedCreatorKeys(entries) {
		value, err := entries[creator].toCell()
		if err != nil {
			t.Fatalf("serialize creator %x: %v", creator, err)
		}
		key := cell.BeginCell().MustStoreSlice(creator[:], 256).EndCell()
		if err = dict.Set(key, value); err != nil {
			t.Fatalf("store creator %x: %v", creator, err)
		}
	}
	builder := cell.BeginCell().MustStoreUInt(blockCreateStatsTag, 8)
	if err := builder.StoreDict(dict); err != nil {
		t.Fatal(err)
	}
	return builder.EndCell()
}

func blockCreateStatsTestStats(t *testing.T, entries map[[32]byte]creatorStats) blockCreateStats {
	t.Helper()

	stats, err := openBlockCreateStats(blockCreateStatsTestCell(t, entries))
	if err != nil {
		t.Fatal(err)
	}
	return stats
}

// blockCreateStatsTestEntries materializes a dictionary for assertions, which is
// exactly the traversal production stopped doing.
func blockCreateStatsTestEntries(t *testing.T, stats blockCreateStats) map[[32]byte]creatorStats {
	t.Helper()

	entries := map[[32]byte]creatorStats{}
	if stats.dict == nil {
		return entries
	}
	items, err := stats.dict.LoadAll()
	if err != nil {
		t.Fatal(err)
	}
	for i := range items {
		key, err := items[i].Key.LoadSlice(256)
		if err != nil {
			t.Fatal(err)
		}
		var creator [32]byte
		copy(creator[:], key)
		entry, err := loadCreatorStats(items[i].Value)
		if err != nil {
			t.Fatalf("creator %x: %v", creator, err)
		}
		entries[creator] = entry
	}
	return entries
}

func blockCreateStatsTestRoot(t *testing.T, tag uint64, value *cell.Cell) *cell.Cell {
	t.Helper()

	dict := cell.NewDict(256)
	key := creatorStatsTestKey(0x11)
	if err := dict.Set(cell.BeginCell().MustStoreSlice(key[:], 256).EndCell(), value); err != nil {
		t.Fatal(err)
	}
	builder := cell.BeginCell().MustStoreUInt(tag, 8)
	if err := builder.StoreDict(dict); err != nil {
		t.Fatal(err)
	}
	return builder.EndCell()
}

// masterCreatorStatsBenchEntries approximates a mainnet BlockCreateStats: one
// entry per recently active validator.
const masterCreatorStatsBenchEntries = 300

func benchBlockCreateStatsEntries(n int) map[[32]byte]creatorStats {
	entries := make(map[[32]byte]creatorStats, n)
	for i := range n {
		var key [32]byte
		key[0] = byte(i)
		key[1] = byte(i >> 8)
		key[2] = 0xa5
		entries[key] = creatorStats{
			masterchain: discountedCounter{
				lastUpdated: uint32(1_000 + i),
				total:       uint64(i + 1),
				count2048:   uint64(i+1) << 32,
				count65536:  uint64(i+1) << 32,
			},
			shardchain: discountedCounter{
				lastUpdated: uint32(2_000 + i),
				total:       uint64(i + 2),
				count2048:   uint64(i+2) << 32,
				count65536:  uint64(i+2) << 32,
			},
		}
	}
	return entries
}

func benchBlockCreateStatsCell(tb testing.TB, n int) *cell.Cell {
	tb.Helper()

	dict := cell.NewDict(creatorStatsKeyBits)
	entries := benchBlockCreateStatsEntries(n)
	for _, creator := range sortedCreatorKeys(entries) {
		value, err := entries[creator].toCell()
		if err != nil {
			tb.Fatal(err)
		}
		if err = dict.SetBuilderByBytesKey(creator[:], value.ToBuilder()); err != nil {
			tb.Fatal(err)
		}
	}
	builder := cell.BeginCell().MustStoreUInt(blockCreateStatsTag, 8)
	if err := builder.StoreDict(dict); err != nil {
		tb.Fatal(err)
	}
	return builder.EndCell()
}

func benchBlockCreateStats(tb testing.TB, n int) blockCreateStats {
	tb.Helper()

	stats, err := openBlockCreateStats(benchBlockCreateStatsCell(tb, n))
	if err != nil {
		tb.Fatal(err)
	}
	return stats
}

// BenchmarkUpdateBlockCreateStats covers the collation-side update of a
// realistically sized creator dictionary. It touches only the keys the block
// names, so its cost is flat in the number of recently active validators.
func BenchmarkUpdateBlockCreateStats(b *testing.B) {
	previous := benchBlockCreateStats(b, masterCreatorStatsBenchEntries)
	creator := creatorStatsTestKey(0x7e)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := updateBlockCreateStats(blockCreateStatsInput{
			enabled:            true,
			previous:           previous,
			now:                5_000,
			masterchainCreator: creator,
		}); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkOpenBlockCreateStats measures what decoding the statistics now costs
// on the paths that only need the dictionary handle: the header, and nothing
// else. It replaces a full traversal removed once per masterchain collation,
// twice per masterchain validation and once per shard validation.
func BenchmarkOpenBlockCreateStats(b *testing.B) {
	root := benchBlockCreateStatsCell(b, masterCreatorStatsBenchEntries)
	b.ReportAllocs()
	for b.Loop() {
		if _, err := openBlockCreateStats(root); err != nil {
			b.Fatal(err)
		}
	}
}

// BenchmarkVerifyBlockCreateStatsUpdate measures the validation pass over a
// realistically sized dictionary whose block touched a handful of creators.
func BenchmarkVerifyBlockCreateStatsUpdate(b *testing.B) {
	const shardCreators = 20
	previous := benchBlockCreateStats(b, masterCreatorStatsBenchEntries)
	creator := creatorStatsTestKey(0x7e)
	creators := make(map[[32]byte]uint32, shardCreators)
	for i := range shardCreators {
		var key [32]byte
		key[0] = byte(i)
		key[2] = 0xa5
		creators[key] = 1
	}
	root, err := updateBlockCreateStats(blockCreateStatsInput{
		enabled:            true,
		previous:           previous,
		now:                5_000,
		shardBlockCreators: creators,
		masterchainCreator: creator,
	})
	if err != nil {
		b.Fatal(err)
	}
	next, err := openBlockCreateStats(root)
	if err != nil {
		b.Fatal(err)
	}

	b.ReportAllocs()
	for b.Loop() {
		if err := verifyBlockCreateStatsUpdate(previous, next, 5_000, creators, creator); err != nil {
			b.Fatal(err)
		}
	}
}
