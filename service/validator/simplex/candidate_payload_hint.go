package simplex

// The combined broadcast BOC is the third serialization of one candidate: the
// block BOC and the collated data are written first, and the payload writes the
// union of the two over the same roots. The first two are already presized by
// the collator; this one had no hint at all and grew its dedup index and cell
// list from nothing on every broadcast.
//
// The hint below is not an estimate. The combined bag holds exactly the union of
// the cell sets of the two BOCs the caller is holding, so the sum of the two
// declared counts is an upper bound on it by construction — no proportional
// factor, no headroom, and no dependence on a previous build. It over-shoots
// only by the overlap between block and collated cells.
//
// The structural bound on that over-shoot, which is what holds for ANY input
// rather than for one fixture: with B and C the two cell sets, the hint is
// |B| + |C| and the truth is |B ∪ C| ≥ max(|B|, |C|) ≥ (|B| + |C|) / 2, so
//
//	|B ∪ C|  ≤  hint  ≤  2 · |B ∪ C|
//
// for every pair of BOCs, with the upper end reached only when the two sets are
// identical. A hint can therefore never presize more than twice the table the
// serializer ends up needing, whatever the traffic looks like. On the mainnet
// fixture the actual over-shoot is 3.88% to 6.27% (TestL3HintOnMainnetFixture
// logs it per arm), i.e. far inside that bound — but the 2x is the guarantee and
// the percentages are one workload's observation of it.
//
// It is bounded anyway, because a declared count is a number read out of a
// buffer and this project has already shipped one presize that reached 58.7 MB
// from a hint nobody bounded:
//
//   - factor 1: the hint is the sum itself. Growing it would only enlarge a
//     table that is already an over-estimate.
//   - byte-consistency clamp: a cell costs at least two descriptor bytes in the
//     BOC payload it was declared in, so a count above len(boc)/2 is impossible
//     for that buffer and is clamped to it. This is what makes a forged or
//     truncated header harmless: the ceiling that matters scales with bytes the
//     caller is already holding.
//   - ceiling payloadHintMaxPresizedCells: the absolute cap, a quarter of the
//     parser's own MaxBOCCells, so even two headers each declaring four billion
//     cells presize a bounded table — about 12.6 MB of scratch, against the
//     58.7 MB the unbounded presize reached. On the mainnet fixture the combined
//     payload runs 20,894-28,231 cells at ~32 bytes per cell, so the ceiling
//     stands for a payload of roughly 8 MB of BOC and cannot bind on a candidate
//     that passed the consensus size limits. If it ever did, the hint would be
//     dropped to the ceiling and the serializer would grow the rest of the way,
//     which is the pre-hint behaviour and not a failure.
//   - floor payloadHintFloorCells: below the serializer's own initial capacity a
//     presize cannot beat the default sizing it replaces, so the hint is dropped
//     and the serializer starts where it always did. An empty or malformed
//     header lands here too.
//
// Correctness never depends on any of this: a hint only sizes scratch
// structures, and the bytes the serializer emits are identical for every value
// of it, including zero. TestL3HintIsUpperBound and
// TestL3HintDegradesUnderWrongHints hold both halves.
const (
	payloadHintFloorCells       = 64
	payloadHintMaxPresizedCells = 1 << 18
	bocHeaderCellsOffset        = 6
	bocMinPayloadBytesPerCell   = 2
)

// bocSerializedMagic is the standard BOC magic, b5ee9c72, the only one anything
// in this project emits: cell.ToBOCWithOptions writes it unconditionally.
var bocSerializedMagic = [4]byte{0xB5, 0xEE, 0x9C, 0x72}

// PayloadCellHint sizes the combined broadcast BOC from the two BOCs the
// producer already holds. Both callers have them: the collator has just written
// them, and the codec has just re-serialized them from the roots it decoded.
func PayloadCellHint(blockBOC, collatedData []byte) int {
	return PayloadCellHintFromCounts(bocDeclaredCells(blockBOC), bocDeclaredCells(collatedData))
}

// PayloadCellHintFromCounts is the same hint for a producer that starts the
// payload before either BOC exists — the collator, which launches it the moment
// its roots are final — and so has no header to read a count out of. It takes
// instead the two counts those component serializations are themselves presized
// with, which is the only pair of numbers describing these same two cell sets
// available that early.
//
// The |B| + |C| construction and both clamps carry over unchanged. What does not
// carry over is exactness: a declared count is what a finished serializer wrote,
// while these two are the producer's own sizing estimates, so the sum bounds the
// union only as well as they bound their own halves. That costs nothing, because
// a hint sizes scratch structures and the payload bytes are identical for every
// value of it, including zero. The byte-consistency clamp has no counterpart
// here and needs none: these numbers are the producer's own, not a length read
// out of a buffer it did not write.
func PayloadCellHintFromCounts(blockCells, collatedCells int) int {
	if blockCells < 0 || collatedCells < 0 {
		return 0
	}

	// Each half is clamped to the ceiling before the two are summed. Clamping
	// only the sum would be enough for the two declared counts PayloadCellHint
	// derives, which the byte clamp has already bounded by the buffers they came
	// out of, but not for two counts a producer computed: near the top of the int
	// range their sum wraps negative, drops under the floor and yields "no hint"
	// where the ceiling was meant. Clamped first, the sum is at most twice the
	// ceiling and cannot wrap.
	hint := min(blockCells, payloadHintMaxPresizedCells) + min(collatedCells, payloadHintMaxPresizedCells)
	if hint > payloadHintMaxPresizedCells {
		return payloadHintMaxPresizedCells
	}
	if hint < payloadHintFloorCells {
		return 0
	}

	return hint
}

// bocDeclaredCells reads the cells counter out of a BOC header — magic(4),
// flags(1), off_bytes(1), then cells over ref_size bytes big-endian — and clamps
// it to what the buffer it was read from could physically hold. It parses
// nothing else and allocates nothing.
//
// The magic is checked first, because this is an exported entry point taking a
// []byte and everything after offset 4 is meaningless without it: a buffer that
// is not a BOC at all would otherwise yield a number read out of arbitrary bytes,
// bounded only by the clamp below. The two legacy magics the parser still accepts
// — 68ff65f3 and acc3a728 (cell/parse.go:19-21) — keep the cells counter at this
// same offset, so refusing them is not about relocating the read; it is that
// nothing in this project writes them, and a hint is a pure optimization that is
// never worth trusting a header we do not otherwise validate. A rejected header
// yields no hint and the serializer starts where it always did.
func bocDeclaredCells(boc []byte) int {
	if len(boc) < bocHeaderCellsOffset || [4]byte(boc[:4]) != bocSerializedMagic {
		return 0
	}
	sizeBytes := int(boc[4] & 0b111)
	if sizeBytes < 1 || sizeBytes > 4 || len(boc) < bocHeaderCellsOffset+sizeBytes {
		return 0
	}

	var cells uint64
	for _, b := range boc[bocHeaderCellsOffset : bocHeaderCellsOffset+sizeBytes] {
		cells = cells<<8 | uint64(b)
	}
	if limit := uint64(len(boc) / bocMinPayloadBytesPerCell); cells > limit {
		cells = limit
	}

	return int(cells)
}
