package collator

import (
	"context"
	"math"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"
)

func TestLimitThresholdsBoundaries(t *testing.T) {
	limits, err := newLimitThresholds(tlb.ParamLimits{Underload: 10, SoftLimit: 20, HardLimit: 31})
	if err != nil {
		t.Fatal(err)
	}

	want := limitThresholds{10, 20, 25, 31}
	if limits != want {
		t.Fatalf("thresholds = %v, want %v", limits, want)
	}

	cases := []struct {
		value uint64
		class LoadClass
	}{
		{9, LoadUnderload},
		{10, LoadNormal},
		{19, LoadNormal},
		{20, LoadSoft},
		{24, LoadSoft},
		{25, LoadMedium},
		{30, LoadMedium},
		{31, LoadHard},
	}
	for _, tc := range cases {
		if got := limits.classify(tc.value); got != tc.class {
			t.Fatalf("classify(%d) = %d, want %d", tc.value, got, tc.class)
		}
	}
}

func TestLimitThresholdsMidpointDoesNotOverflow(t *testing.T) {
	limits, err := newLimitThresholds(tlb.ParamLimits{
		Underload: math.MaxUint32 - 20,
		SoftLimit: math.MaxUint32 - 10,
		HardLimit: math.MaxUint32,
	})
	if err != nil {
		t.Fatal(err)
	}
	if limits[2] != math.MaxUint32-5 {
		t.Fatalf("midpoint = %d", limits[2])
	}
}

func TestBlockLimitsV1UsesByteLimitsForCollatedData(t *testing.T) {
	bytes := tlb.ParamLimits{Underload: 10, SoftLimit: 20, HardLimit: 30}
	limits, err := parseBlockLimits(&tlb.BlockLimits{Limits: tlb.BlockLimitsV1{
		Bytes:   bytes,
		Gas:     tlb.ParamLimits{Underload: 40, SoftLimit: 50, HardLimit: 60},
		LTDelta: tlb.ParamLimits{Underload: 70, SoftLimit: 80, HardLimit: 90},
	}})
	if err != nil {
		t.Fatal(err)
	}

	want, err := newLimitThresholds(bytes)
	if err != nil {
		t.Fatal(err)
	}
	if limits.collatedData != want {
		t.Fatalf("collated data limits = %v, want %v", limits.collatedData, want)
	}
}

func TestCandidateLoadIncludesActualCollatedData(t *testing.T) {
	req := emptyCandidateRequest(t)
	candidate, err := testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	size := uint64(len(candidate.CollatedData))
	loose := limitThresholds{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}
	req.Masterchain.Config.basechain.limits = blockLimits{
		bytes:        loose,
		gas:          loose,
		ltDelta:      loose,
		collatedData: limitThresholds{size, size + 1, size + 2, size + 3},
	}
	candidate, err = testBuilder().BuildShard(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if candidate.Stats.Load != LoadNormal {
		t.Fatalf("candidate load = %d, want %d at the exact underload boundary", candidate.Stats.Load, LoadNormal)
	}
}

func TestProofSizeEstimateParticipatesInAdmission(t *testing.T) {
	estimator := newProofSizeEstimator(0)
	leaf := cell.BeginCell().MustStoreUInt(1, 8).EndCell()
	root := cell.BeginCell().MustStoreUInt(2, 8).MustStoreRef(leaf).EndCell()
	estimator.addLoadedCell(root)
	withPrunedLeaf := estimator.size()
	if withPrunedLeaf != 50 {
		t.Fatalf("root proof estimate = %d, want 50", withPrunedLeaf)
	}
	estimator.addLoadedCell(leaf)
	if loaded := estimator.size(); loaded != 15 {
		t.Fatalf("loaded proof estimate = %d, want 15", loaded)
	}

	loose := limitThresholds{math.MaxUint64, math.MaxUint64, math.MaxUint64, math.MaxUint64}
	status := newBlockLimitStatus(blockLimits{
		bytes:        loose,
		gas:          loose,
		ltDelta:      loose,
		collatedData: limitThresholds{1, 2, 3, 4},
	}, 1, cell.NewReadSet(nil), 0, 0)
	c := collation{limits: status, fullCollated: true, collatedProofEstimate: estimator}
	c.updateCollatedEstimate()
	if status.collatedData != estimator.size() {
		t.Fatalf("admission estimate = %d, want %d", status.collatedData, estimator.size())
	}
	if status.fits(LoadSoft) {
		t.Fatal("soft admission accepted an oversized collated-data estimate")
	}
}

func TestProofSizeEstimatorTracksLoadedHashesSeparatelyFromReferences(t *testing.T) {
	child := cell.BeginCell().MustStoreUInt(7, 3).EndCell()
	root := cell.BeginCell().MustStoreRef(child).EndCell()
	estimator := newProofSizeEstimator(0)
	estimator.addLoadedCell(root)
	loaded := estimator.loadedHashes()
	if !loaded.loaded(root.HashKey()) {
		t.Fatal("loaded root hash is absent")
	}
	if loaded.loaded(child.HashKey()) {
		t.Fatal("referenced-only child is marked loaded")
	}
	// A second estimator for the promote, because loadedHashes seals the one it
	// was asked of: what the selector holds cannot move, so the same estimator
	// cannot be fed again to show the child turning loaded.
	promoted := newProofSizeEstimator(0)
	promoted.addLoadedCell(root)
	promoted.addLoadedCell(child)
	if !promoted.loadedHashes().loaded(child.HashKey()) {
		t.Fatal("loaded child hash is absent")
	}
}

func TestCollatedHashUsageProofKeepsStateUpdateSourceRoot(t *testing.T) {
	oldLeaf := cell.BeginCell().MustStoreUInt(0, 1).EndCell()
	newLeaf := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	oldBranch := cell.BeginCell().MustStoreUInt(7, 3).MustStoreRef(oldLeaf).EndCell()
	newBranch := cell.BeginCell().MustStoreUInt(7, 3).MustStoreRef(newLeaf).EndCell()
	unchanged := cell.BeginCell().MustStoreUInt(5, 3).EndCell()
	oldRoot := cell.BeginCell().MustStoreRef(oldBranch).MustStoreRef(unchanged).EndCell()
	newRoot := cell.BeginCell().MustStoreRef(newBranch).MustStoreRef(unchanged).EndCell()
	update, err := cell.CreateMerkleUpdate(oldRoot, newRoot)
	if err != nil {
		t.Fatal(err)
	}

	// Semantic validation did not independently read the changed branch. The
	// state update source still carries it as an ordinary non-leaf, so applying
	// the update requires the full-collated predecessor proof to retain it too.
	estimator := newProofSizeEstimator(0)
	estimator.addLoadedCell(unchanged)
	updateSource, err := merkleUpdateSourceShape(update)
	if err != nil {
		t.Fatal(err)
	}
	if !updateSource.emitted(oldBranch.HashKey()) {
		t.Fatal("state update source shape omitted the logical level-zero branch hash")
	}
	loaded := estimator.loadedHashes()
	proof, err := oldRoot.CreateHashUsageProof(func(hash cell.Hash) bool {
		return loaded.loaded(hash) || updateSource.emitted(hash)
	})
	if err != nil {
		t.Fatal(err)
	}
	provenOld, err := cell.UnwrapProofVirtualized(proof, oldRoot.Hash())
	if err != nil {
		t.Fatal(err)
	}
	applied, err := cell.ApplyMerkleUpdate(provenOld, update)
	if err != nil {
		t.Fatalf("apply update to full-collated predecessor proof: %v", err)
	}
	if applied.HashKey() != newRoot.HashKey() {
		t.Fatalf("applied root = %x, want %x", applied.HashKey(), newRoot.HashKey())
	}
}

func TestMerkleUpdateSourceShapeUsesPredecessorVisibleHashes(t *testing.T) {
	leaf := cell.BeginCell().MustStoreUInt(1, 1).EndCell()
	deep := cell.BeginCell().MustStoreUInt(3, 2).MustStoreRef(leaf).EndCell()
	prunedDeep, err := cell.CreatePrunedBranch(deep, 1, 3)
	if err != nil {
		t.Fatal(err)
	}
	branch := cell.BeginCell().MustStoreUInt(7, 3).MustStoreRef(deep).EndCell()
	sourceBranch := cell.BeginCell().MustStoreUInt(7, 3).MustStoreRef(prunedDeep).EndCell()
	oldRoot := cell.BeginCell().MustStoreRef(branch).EndCell()
	sourceRoot := cell.BeginCell().MustStoreRef(sourceBranch).EndCell()
	if sourceBranch.Level() == 0 || sourceBranch.HashKeyAt(0) != branch.HashKey() {
		t.Fatal("test source branch does not carry a level-shifted view of the predecessor")
	}
	if sourceRoot.HashKeyAt(0) != oldRoot.HashKey() {
		t.Fatal("test source root does not represent the predecessor")
	}
	update, err := cell.CreateMerkleUpdate(sourceRoot, sourceRoot)
	if err != nil {
		t.Fatal(err)
	}

	shape, err := merkleUpdateSourceShape(update)
	if err != nil {
		t.Fatal(err)
	}
	if !shape.emitted(branch.HashKey()) {
		t.Fatal("source shape omitted the predecessor-visible level-zero branch hash")
	}

	predecessorProof, err := oldRoot.CreateHashUsageProof(shape.emitted)
	if err != nil {
		t.Fatal(err)
	}
	provenOld, err := cell.UnwrapProofVirtualized(predecessorProof, oldRoot.Hash())
	if err != nil {
		t.Fatal(err)
	}
	if _, err = cell.ApplyMerkleUpdate(provenOld, update); err != nil {
		t.Fatalf("apply level-shifted update source to collated predecessor proof: %v", err)
	}
}

// proofSeenHashSet decides the admission estimate, so it is tested against the
// map it replaced rather than against a hand-listed set of expectations: the
// reference below is literally the map[cell.Hash]bool the estimator used to
// keep, and any disagreement — a lost loaded bit, a demotion, a rehash losing
// an entry — separates the two. It cannot reach a fingerprint collision, which
// needs hand-built hashes; TestProofSeenHashSetComparesEveryByte covers that.
func TestProofSeenHashSetMatchesTheMapItReplaced(t *testing.T) {
	var set proofSeenHashSet
	reference := map[cell.Hash]bool{}

	// Enough entries to force several doublings past the initial 64 slots, and a
	// deliberate re-observation stream so promotions and repeats are exercised.
	const entries = 4000
	hashes := make([]cell.Hash, entries)
	for i := range hashes {
		hashes[i] = cell.BeginCell().MustStoreUInt(uint64(i), 32).EndCell().HashKey()
	}
	for step := 0; step < 3; step++ {
		for i, hash := range hashes {
			loaded := (i+step)%3 == 0
			seen, wasLoaded := set.observeLegacy(hash, loaded)
			refLoaded, refSeen := reference[hash]
			if seen != refSeen || wasLoaded != refLoaded {
				t.Fatalf("observe(%x, %v) = (%v, %v), want (%v, %v)",
					hash[:4], loaded, seen, wasLoaded, refSeen, refLoaded)
			}
			if loaded || !refSeen {
				reference[hash] = refLoaded || loaded
			}
		}
	}

	// Everything that went in is still findable, with the bit it ended on.
	for hash, want := range reference {
		seen, wasLoaded := set.observeLegacy(hash, false)
		if !seen || wasLoaded != want {
			t.Fatalf("re-observe %x = (%v, %v), want (true, %v)", hash[:4], seen, wasLoaded, want)
		}
	}
	// A hash that never went in is absent even though the table is full of
	// neighbours — the fingerprint alone must not answer.
	absent := cell.BeginCell().MustStoreUInt(entries, 32).EndCell().HashKey()
	if seen, _ := set.observeLegacy(absent, false); seen {
		t.Fatal("an unobserved hash was reported as seen")
	}
}

// The fingerprint is four bytes of the hash and it only selects the slot; the
// full 32-byte compare is what decides identity. Nothing in the random-hash
// test above can reach that distinction — at 4000 sha256 keys a fingerprint
// collision has a ~0.2% chance of ever occurring — so a set that compared the
// fingerprint alone would pass it. These two hashes agree on exactly the four
// bytes the fingerprint reads and differ immediately after, which is the shape
// the comment on proofSeenHashSet calls consensus-critical: charging the second
// cell as already-seen would change how many bytes the block is charged and
// therefore which messages it admits.
func TestProofSeenHashSetComparesEveryByte(t *testing.T) {
	var first, second cell.Hash
	for i := range 4 {
		first[i], second[i] = byte(0xA0+i), byte(0xA0+i)
	}
	first[4], second[4] = 0x01, 0x02

	if proofCellFingerprint(first) != proofCellFingerprint(second) {
		t.Fatal("fixture is wrong: the two hashes must share a fingerprint")
	}

	var set proofSeenHashSet
	if seen, wasLoaded := set.observeLegacy(first, true); seen || wasLoaded {
		t.Fatalf("first observe = (%v, %v), want (false, false)", seen, wasLoaded)
	}
	seen, wasLoaded := set.observeLegacy(second, false)
	if seen {
		t.Fatal("a different hash sharing the fingerprint was reported as already seen")
	}
	if wasLoaded {
		t.Fatal("a different hash sharing the fingerprint inherited the loaded bit")
	}
	// Both are now present and keep their own bits.
	if seen, wasLoaded := set.observeLegacy(first, false); !seen || !wasLoaded {
		t.Fatalf("re-observe first = (%v, %v), want (true, true)", seen, wasLoaded)
	}
	if seen, wasLoaded := set.observeLegacy(second, false); !seen || wasLoaded {
		t.Fatalf("re-observe second = (%v, %v), want (true, false)", seen, wasLoaded)
	}
}

func TestBlockLimitsAtTimeAppliesSlowCollatorPolicy(t *testing.T) {
	limits := blockLimits{ltDelta: limitThresholds{10, 100, 150, 1_000}}
	unchanged, err := blockLimitsAtTime(limits, 100, 115)
	if err != nil {
		t.Fatal(err)
	}
	if unchanged.ltDelta != limits.ltDelta {
		t.Fatalf("15-second limits = %v, want %v", unchanged.ltDelta, limits.ltDelta)
	}

	adjusted, err := blockLimitsAtTime(limits, 100, 116)
	if err != nil {
		t.Fatal(err)
	}
	want := limitThresholds{20, 180, 190, 200}
	if adjusted.ltDelta != want {
		t.Fatalf("slow-block LT limits = %v, want %v", adjusted.ltDelta, want)
	}
	if adjusted.bytes != limits.bytes || adjusted.gas != limits.gas || adjusted.collatedData != limits.collatedData {
		t.Fatal("slow-block policy changed a non-LT limit")
	}
}

func TestPrepareUsesAdjustedSlowBlockHardLTDelta(t *testing.T) {
	shardRequest := emptyCandidateRequest(t)
	shardState := loadPreviousShardState(t, shardRequest)
	shardRequest.Header.GenUtime = max(shardState.GenUTime+16, shardRequest.Masterchain.GenUtime+1)
	shardRequest.Header.GenUtimeMS = uint64(shardRequest.Header.GenUtime) * 1_000
	shardCollation, err := testBuilder().prepare(context.Background(), shardRequest)
	if err != nil {
		t.Fatal(err)
	}
	if shardCollation.hardLTDelta != 200 || shardCollation.limits.limits.ltDelta[3] != 200 {
		t.Fatalf("shard slow-block hard LT limits = (%d, %d), want (200, 200)",
			shardCollation.hardLTDelta, shardCollation.limits.limits.ltDelta[3])
	}

	masterFixture := newMasterBuildFixture(t, false)
	var masterState tlb.ShardStateUnsplit
	if err = parseExact(&masterState, masterFixture.request.Previous.State); err != nil {
		t.Fatal(err)
	}
	masterFixture.request.Header.GenUtime = masterState.GenUTime + 16
	masterFixture.request.Header.GenUtimeMS = uint64(masterFixture.request.Header.GenUtime) * 1_000
	masterCollation, err := testBuilder().prepareMaster(context.Background(), masterFixture.request)
	if err != nil {
		t.Fatal(err)
	}
	if masterCollation.hardLTDelta != 200 || masterCollation.limits.limits.ltDelta[3] != 200 {
		t.Fatalf("master slow-block hard LT limits = (%d, %d), want (200, 200)",
			masterCollation.hardLTDelta, masterCollation.limits.limits.ltDelta[3])
	}
}

// The estimate has to include the finished state, not only the dictionary roots
// as they stood at the last periodic sample. Collator adds this proof at the
// same point — the end of create_shard_state, right after the Merkle update
// (collator.cpp:5819) — and without it every estimate a collation reports is
// short by whatever changed since the last sample. Measured on the mainnet
// fixture that is 4.1% of a 345-transaction block and 13.1% of a
// thousand-transaction one: the gap widens as the block does, which is the
// worst shape for a number the size-limit rebuild aims with and the load class
// behind split and merge history reads.
//
// Adding the proof a second time must therefore find every cell already
// counted. Cells and Bits are the halves of the stat that answer that question:
// AddProof charges an internal ref per traversal whether or not the cell is new,
// but counts a cell once.
func TestCollationEstimateIncludesFinishedState(t *testing.T) {
	req, _ := benchMainnetRequest(t, benchMainnetFiller)
	c, err := testBuilder().prepareShardPhases(context.Background(), req, collationAttempt{})
	if err != nil {
		t.Fatal(err)
	}
	if err = c.processExternals(); err != nil {
		t.Fatal(err)
	}
	if err = c.processNewMessages(c.blockFull || c.haveUnprocessedDispatchQueue || req.internalsIncomplete()); err != nil {
		t.Fatal(err)
	}
	candidate, err := c.finishShard()
	if err != nil {
		t.Fatal(err)
	}

	before := c.limits.storage.TotalStat()
	if err = c.limits.addProof(candidate.State); err != nil {
		t.Fatal(err)
	}
	after := c.limits.storage.TotalStat()
	if after.Cells != before.Cells || after.Bits != before.Bits {
		t.Fatalf("re-proving the finished state found %d uncounted cells and %d uncounted bits; "+
			"collation.finish no longer proves it, so the estimate understates every block",
			after.Cells-before.Cells, after.Bits-before.Bits)
	}
}
