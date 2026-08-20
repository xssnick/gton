package collator

import (
	"bytes"
	"context"
	"slices"
	"testing"

	"github.com/xssnick/tonutils-go/tlb"
	"github.com/xssnick/tonutils-go/tvm/cell"

	"github.com/xssnick/gton/service/validator/groups"
)

func sortedHashes(hashes []cell.Hash) []cell.Hash {
	slices.SortFunc(hashes, func(left, right cell.Hash) int {
		return bytes.Compare(left[:], right[:])
	})
	return hashes
}

func footprintHashes(footprint *configFootprint) []cell.Hash {
	hashes := make([]cell.Hash, 0, len(footprint.cells))
	for _, c := range footprint.cells {
		hashes = append(hashes, c.HashKey())
	}
	return sortedHashes(hashes)
}

// withFootprint returns config with a different footprint bound to the same
// configuration root, which is what every mutation below is.
func withFootprint(config *Config, cells []*cell.Cell) *Config {
	replaced := *config
	if cells == nil {
		replaced.footprint = nil
		return &replaced
	}
	footprint := *config.footprint
	footprint.cells = cells
	replaced.footprint = &footprint
	return &replaced
}

// withRequestConfig returns req with a different prepared configuration.
func withRequestConfig(req MasterRequest, config *Config) MasterRequest {
	req.Config = config
	return req
}

// foreignCells are cells no configuration contains, for the mutation that makes
// the footprint a superset rather than a prefix.
func foreignCells(count int) []*cell.Cell {
	cells := make([]*cell.Cell, 0, count)
	for i := range count {
		cells = append(cells, cell.BeginCell().MustStoreUInt(uint64(i)|1<<40, 64).EndCell())
	}
	return cells
}

// masterConfigTransitionReads runs deriveMasterConfigTransition over a recorder
// of its own and returns what that recorder holds afterwards.
//
// The recorded set is the whole safety argument for reuse: on the collation path
// this runs under the block's read set, the Merkle update descends only through
// cells that set recorded, and the collated-size estimate answers membership out
// of the same record. Every gate in this file is a statement about this set.
func masterConfigTransitionReads(
	t *testing.T,
	fixture masterBuildFixture,
	config *Config,
	groupConfig *groups.Config,
) (masterConfigTransition, []cell.Hash) {
	t.Helper()

	usage := cell.NewReadSet(fixture.request.Previous.State)
	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, usage.Root()); err != nil {
		t.Fatal(err)
	}
	var extra tlb.McStateExtra
	if err := parseExact(&extra, state.McStateExtra); err != nil {
		t.Fatal(err)
	}
	transition, err := deriveMasterConfigTransition(
		state.Accounts.ShardAccounts,
		&extra,
		masterConfigPredecessor{config: config, groups: groupConfig, usage: usage},
	)
	if err != nil {
		t.Fatal(err)
	}
	return transition, sortedHashes(usage.Hashes())
}

// buildMasterReadSetSize returns how many cells a whole master collation
// recorded. The builder keeps the last build's size to presize the next
// recorder, which makes it the one end-to-end view of the block's read set a
// test can take without a hook of its own.
func buildMasterReadSetSize(t *testing.T, req MasterRequest) int {
	t.Helper()

	builder := testBuilder()
	if _, err := builder.BuildMaster(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	size := builder.readSetHint()
	if size == 0 {
		t.Fatal("the builder did not record a read set size")
	}
	return size
}

// The footprint stands in for the parses master collation skips, so it has to
// be the set those parses read — not a superset, not a prefix — and every cell
// in it has to be admissible as a predecessor body: materialized, level zero,
// unvirtualized and carrying no trace of the tree it was captured from.
func TestCaptureConfigFootprintMatchesParseReads(t *testing.T) {
	root := loadMainnetConfig(t).execution.Root()

	usage := cell.NewReadSet(root)
	if _, err := parseMasterConfigEpoch(usage.Root()); err != nil {
		t.Fatal(err)
	}
	want := sortedHashes(usage.Hashes())

	resident, footprint := captureConfigFootprint(root)
	if footprint == nil {
		t.Fatal("mainnet configuration produced no footprint")
	}
	if resident == nil || resident.HashKey() != root.HashKey() {
		t.Fatal("the capture did not hand back the configuration it materialized")
	}
	if footprint.root != root.HashKey() || !footprint.covers(root) {
		t.Fatal("footprint is not bound to the configuration it was captured from")
	}
	if got := footprintHashes(footprint); !slices.Equal(want, got) {
		t.Fatalf("footprint holds %d cells, the parse reads %d", len(got), len(want))
	}
	for _, c := range footprint.cells {
		if c.IsLazy() || c.IsVirtualized() || c.Level() != 0 || c.Trace() != nil {
			t.Fatal("footprint holds a cell that must never stand in for a predecessor body")
		}
	}
}

// countLazyReferences reports how many references of these cells are still
// unresolved placeholders.
func countLazyReferences(t *testing.T, cells []*cell.Cell) int {
	t.Helper()

	lazy := 0
	for _, c := range cells {
		for i := range int(c.RefsNum()) {
			ref, err := c.PeekRef(i)
			if err != nil {
				t.Fatal(err)
			}
			if ref.IsLazy() {
				lazy++
			}
		}
	}
	return lazy
}

// A capture taken over a configuration that is still in CellDB must come back
// detached from it.
//
// Recording alone does not do that. The recorder keeps the resolved body of
// every cell the parse opened, so the hashes come out right, but the references
// those bodies were never asked to follow stay placeholders bound to the
// snapshot's loader — and the footprint, together with the prepared
// configuration built beside it, outlives that snapshot by a whole epoch. This
// is the test that fails if the capture stops materializing first; a set
// comparison alone passes either way.
func TestCaptureConfigFootprintFromLazyStateIsDetachedFromTheLoader(t *testing.T) {
	root := loadMainnetConfig(t).execution.Root()

	residentRoot, resident := captureConfigFootprint(root)
	if resident == nil || residentRoot == nil {
		t.Fatal("resident configuration produced no footprint")
	}

	pages := newAdvLazifier()
	lazyRoot := pages.root(t, root)
	if !lazyRoot.IsLazy() && countLazyReferences(t, []*cell.Cell{lazyRoot}) == 0 {
		t.Fatal("the fixture root is not paged in, so this proves nothing")
	}
	materialized, lazy := captureConfigFootprint(lazyRoot)
	if lazy == nil || materialized == nil {
		t.Fatal("paged-in configuration produced no footprint")
	}
	if lazy.root != resident.root {
		t.Fatal("the paged-in capture bound itself to a different configuration root")
	}
	if !slices.Equal(footprintHashes(resident), footprintHashes(lazy)) {
		t.Fatalf("paged-in capture holds %d cells, resident capture %d", len(lazy.cells), len(resident.cells))
	}
	for _, c := range lazy.cells {
		if c.IsLazy() {
			t.Fatal("paged-in capture stored a placeholder")
		}
	}
	if dangling := countLazyReferences(t, lazy.cells); dangling != 0 {
		t.Fatalf("%d references of the footprint still resolve through the state snapshot's loader", dangling)
	}
	if dangling := countLazyReferences(t, []*cell.Cell{materialized}); dangling != 0 {
		t.Fatal("the tree handed back for the caller's parse still resolves through the loader")
	}
}

// prepare is reached inline from candidate validation, so what it costs there is
// what a validator pays inside a consensus slot on the first block after a
// configuration change. It must page the configuration in once, not once per
// parse: the capture materializes and the parse beside it reads that
// materialization, never the paged-in root.
func TestPrepareConfigMaterializesBeforeParsing(t *testing.T) {
	root := loadMainnetConfig(t).execution.Root()

	pages := newAdvLazifier()
	lazyRoot := pages.root(t, root)

	cache := localConfigCache{entries: make(map[cell.Hash]localPreparedConfig)}
	prepared, err := cache.prepare(lazyRoot)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.config.footprint == nil {
		t.Fatal("prepare published a configuration without a footprint")
	}

	pages.mu.Lock()
	calls, distinct := pages.calls, len(pages.seen)
	pages.mu.Unlock()

	// One pass resolves every cell once, plus the handful reached through two
	// parents. A second pass over the same tree doubles this, which is what the
	// bound rejects.
	if calls > distinct+distinct/10 {
		t.Fatalf("preparing one configuration took %d loads for %d distinct cells: "+
			"the configuration is being paged in more than once", calls, distinct)
	}
	t.Logf("preparing one configuration: %d loads, %d distinct cells", calls, distinct)

	// Everything prepare publishes is read again by every later block, so none of
	// it may still reach for the snapshot this was built from.
	if prepared.execution.Root().IsLazy() ||
		countLazyReferences(t, []*cell.Cell{prepared.execution.Root()}) != 0 {
		t.Fatal("the published configuration still resolves through the state snapshot's loader")
	}
	if dangling := countLazyReferences(t, prepared.config.footprint.cells); dangling != 0 {
		t.Fatalf("%d references of the published footprint still resolve through the loader", dangling)
	}
}

func TestProofBackedConfigCannotPopulateEpochCache(t *testing.T) {
	root := loadMainnetConfig(t).execution.Root()
	usage := cell.NewReadSet(root)
	if _, err := parseMasterConfigEpoch(usage.Root()); err != nil {
		t.Fatal(err)
	}
	read := make(map[cell.Hash]struct{}, len(usage.Hashes()))
	for _, hash := range usage.Hashes() {
		read[hash] = struct{}{}
	}
	proof, err := root.CreateHashUsageProof(func(hash cell.Hash) bool {
		_, ok := read[hash]
		return ok
	})
	if err != nil {
		t.Fatal(err)
	}
	narrow, err := cell.UnwrapProofVirtualized(proof, root.Hash())
	if err != nil {
		t.Fatal(err)
	}
	if !narrow.IsVirtualized() {
		t.Fatal("configuration proof did not produce a virtualized root")
	}

	cache := localConfigCache{entries: make(map[cell.Hash]localPreparedConfig)}
	proofPrepared, err := cache.prepare(narrow)
	if err != nil {
		t.Fatal(err)
	}
	if proofPrepared.execution.Root() != narrow {
		t.Fatal("proof-backed preparation did not stay bound to its validation root")
	}
	if len(cache.entries) != 0 {
		t.Fatal("proof-backed configuration poisoned the epoch cache")
	}

	resident, err := cache.prepare(root)
	if err != nil {
		t.Fatal(err)
	}
	if resident.execution.Root().IsVirtualized() || len(cache.entries) != 1 {
		t.Fatal("resident configuration was not published into the epoch cache")
	}
	reused, err := cache.prepare(narrow)
	if err != nil {
		t.Fatal(err)
	}
	if reused.execution != resident.execution {
		t.Fatal("authenticated proof root did not reuse the resident epoch context")
	}
}

// TestMasterConfigFootprintMutationsAreDetected is the gate.
//
// It is a read-set comparison and not a byte comparison because the bytes this
// fixture produces cannot see a wrong footprint:
// TestMasterConfigFootprintDoesNotReachTheProducedBlock below shows why, and
// why that is a property of the fixture rather than of the mechanism. Reuse
// promises that skipping the parses records what the parses would have
// recorded, and that promise is checkable against the record on every state,
// so this is where a wrong footprint has to be caught.
func TestMasterConfigFootprintMutationsAreDetected(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	captured := fixture.request.Config.footprint.cells
	if len(captured) < 1001 {
		t.Fatalf("the fixture footprint holds %d cells, too few to truncate", len(captured))
	}

	_, fresh := masterConfigTransitionReads(t, fixture, nil, nil)
	freshBuild := buildMasterReadSetSize(t, withRequestConfig(fixture.request, withFootprint(fixture.request.Config, nil)))

	for _, mutation := range []struct {
		name  string
		cells []*cell.Cell
	}{
		{"one cell short", captured[:len(captured)-1]},
		{"ten cells short", captured[:len(captured)-10]},
		{"a hundred cells short", captured[:len(captured)-100]},
		{"a thousand cells short", captured[:len(captured)-1000]},
		{"empty", captured[:0]},
		{"complete plus eight foreign cells", append(slices.Clone(captured), foreignCells(8)...)},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			config := withFootprint(fixture.request.Config, mutation.cells)

			transition, reads := masterConfigTransitionReads(t, fixture, config, fixture.request.Groups.Config)
			if transition.config != config {
				t.Fatal("the reuse gate did not fire, so the mutated footprint was never replayed")
			}
			if slices.Equal(fresh, reads) {
				t.Fatal("a wrong footprint recorded exactly what a fresh parse records")
			}

			// The same difference has to survive a whole collation, where the
			// replay competes with every other read the block takes.
			if size := buildMasterReadSetSize(t, withRequestConfig(fixture.request, config)); size == freshBuild {
				t.Fatalf("a whole master collation recorded %d cells either way", size)
			}
		})
	}

	// The complete footprint is the one case that must record the fresh set
	// exactly, or the table above would pass for the wrong reason.
	transition, reads := masterConfigTransitionReads(t, fixture, fixture.request.Config, fixture.request.Groups.Config)
	if transition.config != fixture.request.Config {
		t.Fatal("the reuse gate did not fire for an unchanged configuration")
	}
	if !slices.Equal(fresh, reads) {
		t.Fatalf("replaying the complete footprint recorded %d cells, parsing records %d", len(reads), len(fresh))
	}
	if size := buildMasterReadSetSize(t, fixture.request); size != freshBuild {
		t.Fatalf("a whole master collation recorded %d cells replaying and %d parsing", size, freshBuild)
	}
}

// TestMasterConfigFootprintDoesNotReachTheProducedBlock records why the byte
// comparisons in this file cannot be the gate on this fixture, and fails the
// day that stops holding.
//
// The reuse gate only fires when the configuration the candidate installs is the
// very same root the predecessor state already holds. The candidate state
// therefore references that root unchanged, the predecessor's own McStateExtra
// is read on every masterchain block, and a state update prunes at the highest
// hash it knows — so on this fixture the configuration leaves the block as one
// boundary and nothing the replay contributed below it is emitted. The size
// estimate stops at the same boundary for the same reason.
//
// A footprint that is short, empty or padded with foreign cells therefore
// produces byte-identical output here. That is a property of this fixture, whose
// entire non-configuration state is twenty cells, and not a structural one: the
// read set answers membership by hash wherever that hash occurs, so a
// configuration cell that also occurs elsewhere in the state is named by the
// destination and moves the produced bytes. This test is the canary for both
// ways that can start happening on the state we do have.
func TestMasterConfigFootprintDoesNotReachTheProducedBlock(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	configRoot := fixture.request.Config.execution.Root()
	captured := fixture.request.Config.footprint.cells

	// What the block reads without the replay: the raw validation of the
	// configuration runs on every masterchain block and reaches the dictionary's
	// top nodes whatever the footprint holds, so those are not what reuse skips.
	// Everything else in the footprint is, and that is the set under test.
	_, otherwise := masterConfigTransitionReads(t, fixture,
		withFootprint(fixture.request.Config, captured[:0]), fixture.request.Groups.Config)
	read := make(map[cell.Hash]struct{}, len(otherwise))
	for _, hash := range otherwise {
		read[hash] = struct{}{}
	}
	skipped := make(map[cell.Hash]struct{}, len(captured))
	for _, c := range captured {
		if _, anyway := read[c.HashKey()]; !anyway {
			skipped[c.HashKey()] = struct{}{}
		}
	}
	if len(skipped) < len(captured)/2 {
		t.Fatalf("only %d of %d footprint cells are exclusive to the skipped parses, "+
			"so this proves close to nothing", len(skipped), len(captured))
	}

	candidate, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}

	boundary := false
	for side := range 2 {
		bodies, pruned := updateSideCells(t, candidate.StateUpdate.MustPeekRef(side))
		for _, hash := range append(bodies, pruned...) {
			if _, replayed := skipped[hash]; replayed {
				t.Fatalf("side %d of the state update names a configuration cell only the "+
					"replay read: either a new consumer now selects cells out of the read "+
					"set, or this fixture holds a configuration cell whose hash occurs "+
					"outside the configuration as well. Either way the footprint decides "+
					"the produced block and the byte comparisons in this file have become "+
					"the gate", side)
			}
		}
		if slices.Contains(pruned, configRoot.HashKey()) {
			boundary = true
		}
	}
	if !boundary {
		t.Fatal("the configuration root is not a pruned boundary of the state update")
	}
	t.Logf("%d of the footprint's %d cells are read only by the parses reuse skips, "+
		"and none of them reaches either side of the state update", len(skipped), len(captured))

	// The other consumer of the record: a proof over the configuration stops at
	// the same boundary, so the size estimate cannot see the footprint either.
	for _, cells := range [][]*cell.Cell{fixture.request.Config.footprint.cells, captured[:0]} {
		usage := cell.NewReadSet(fixture.request.Previous.State)
		var state tlb.ShardStateUnsplit
		if err = parseExact(&state, usage.Root()); err != nil {
			t.Fatal(err)
		}
		var extra tlb.McStateExtra
		if err = parseExact(&extra, state.McStateExtra); err != nil {
			t.Fatal(err)
		}
		if _, err = deriveMasterConfigTransition(state.Accounts.ShardAccounts, &extra,
			masterConfigPredecessor{
				config: withFootprint(fixture.request.Config, cells),
				groups: fixture.request.Groups.Config,
				usage:  usage,
			}); err != nil {
			t.Fatal(err)
		}
		stat := cell.NewCellStorageStat()
		if err = stat.AddProof(extra.ConfigParams.Config.Params.AsCell(), usage); err != nil {
			t.Fatal(err)
		}
		if total := stat.TotalStat(); total.Cells != 0 {
			t.Fatalf("a proof over the configuration charges %d cells, so the size estimate "+
				"now depends on the footprint", total.Cells)
		}
	}
}

// updateSideCells splits one side of a Merkle update into the predecessor cells
// it carries in full and the ones it stands for with a pruned branch. Both are
// named by the level-zero hash, which is the identity of the cell in the
// predecessor tree rather than of its proof-side instance: a body that carries a
// boundary below it is a level-one cell and its own hash would name nothing.
func updateSideCells(t *testing.T, root *cell.Cell) ([]cell.Hash, []cell.Hash) {
	t.Helper()

	var bodies, pruned []cell.Hash
	seen := make(map[cell.Hash]struct{})
	var walk func(*cell.Cell, int)
	walk = func(c *cell.Cell, depth int) {
		if c == nil || depth > 64 {
			return
		}
		if _, visited := seen[c.HashKey()]; visited {
			return
		}
		seen[c.HashKey()] = struct{}{}
		if c.GetType() == cell.PrunedCellType {
			pruned = append(pruned, c.HashKeyAt(0))
			return
		}
		bodies = append(bodies, c.HashKeyAt(0))
		for i := range int(c.RefsNum()) {
			walk(c.MustPeekRef(i), depth+1)
		}
	}
	walk(root, 0)
	return bodies, pruned
}

func assertIdenticalMasterCandidates(t *testing.T, first, second *Candidate) {
	t.Helper()

	if !bytes.Equal(first.BlockBOC, second.BlockBOC) {
		t.Fatal("replaying the configuration footprint changed the block")
	}
	if !bytes.Equal(first.CollatedData, second.CollatedData) {
		t.Fatal("replaying the configuration footprint changed the collated data")
	}
	if first.State.HashKey() != second.State.HashKey() {
		t.Fatal("replaying the configuration footprint changed the resulting state")
	}
	if first.StateUpdate.HashKey() != second.StateUpdate.HashKey() {
		t.Fatal("replaying the configuration footprint changed the state update")
	}
}

// The end-to-end run of the reuse gate: the candidate a reused configuration
// produces has to be the candidate a fresh parse produces, and it has to verify.
//
// This does not check the footprint's contents — see
// TestMasterConfigFootprintDoesNotReachTheProducedBlock for why nothing about
// the bytes this fixture produces can. It checks the other half: that skipping three parses
// and installing a configuration object built a block earlier yields the same
// block, the same state, and an update that really applies to the predecessor.
func TestBuildMasterConfigFootprintReuseProducesIdenticalCandidate(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	if fixture.request.Config.footprint == nil {
		t.Fatal("the fixture configuration carries no footprint")
	}

	replayed, err := testBuilder().BuildMaster(context.Background(), fixture.request)
	if err != nil {
		t.Fatal(err)
	}

	parsedRequest := withRequestConfig(fixture.request, withFootprint(fixture.request.Config, nil))
	parsed, err := testBuilder().BuildMaster(context.Background(), parsedRequest)
	if err != nil {
		t.Fatal(err)
	}

	assertIdenticalMasterCandidates(t, parsed, replayed)

	applied, err := cell.ApplyMerkleUpdate(fixture.request.Previous.State, replayed.StateUpdate)
	if err != nil {
		t.Fatalf("apply masterchain state update: %v", err)
	}
	if applied.HashKey() != replayed.State.HashKey() {
		t.Fatal("the replayed candidate's update does not produce its state")
	}
	if err = VerifyMasterCandidate(context.Background(), MasterVerificationRequest{
		Previous:  fixture.request.Previous,
		Config:    fixture.request.Config,
		Groups:    fixture.request.Groups,
		ShardTops: fixture.request.ShardTops,
		Semantics: testCandidateTransitionVerifier,
		Candidate: replayed,
	}); err != nil {
		t.Fatalf("verify the replayed masterchain candidate: %v", err)
	}
}

// With a paged-in predecessor the recorded instance is what the update hands
// back as the applied boundary body, so this is where a footprint cell bound to
// a stale loader — or one that is simply the wrong instance — would show.
func TestBuildMasterFromLazyPredecessorWithFootprintIsIdentical(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)

	boc, err := fixture.request.Previous.State.ToBOCWithOptionsErr(cell.BOCSerializeOptions{
		WithIndex:     true,
		WithCacheBits: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	lazyState, err := cell.FromBOCWithOptions(boc, cell.BOCParseOptions{Lazy: true})
	if err != nil {
		t.Fatal(err)
	}

	req := fixture.request
	req.Previous.State = lazyState
	replayed, err := testBuilder().BuildMaster(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}

	lazyState, err = cell.FromBOCWithOptions(boc, cell.BOCParseOptions{Lazy: true})
	if err != nil {
		t.Fatal(err)
	}
	parsedRequest := withRequestConfig(fixture.request, withFootprint(fixture.request.Config, nil))
	parsedRequest.Previous.State = lazyState
	parsed, err := testBuilder().BuildMaster(context.Background(), parsedRequest)
	if err != nil {
		t.Fatal(err)
	}

	// Only the two lazy runs are compared. A lazy predecessor legitimately
	// produces a larger update than a resident one — an unresolved reference is
	// pruned on the spot rather than paged in to find out whether a boundary
	// would have cost a few bytes too many — so the resident candidate is a
	// different block by design and is not the comparison this proves.
	assertIdenticalMasterCandidates(t, parsed, replayed)
}

// Verification records nothing, so it reuses without a footprint and the replay
// must be a no-op there rather than a nil dereference.
func TestDeriveMasterConfigTransitionReplaysNothingOnVerification(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)

	var state tlb.ShardStateUnsplit
	if err := parseExact(&state, fixture.request.Previous.State); err != nil {
		t.Fatal(err)
	}
	var extra tlb.McStateExtra
	if err := parseExact(&extra, state.McStateExtra); err != nil {
		t.Fatal(err)
	}

	stripped := withFootprint(fixture.request.Config, nil)
	transition, err := deriveMasterConfigTransition(
		state.Accounts.ShardAccounts,
		&extra,
		masterConfigPredecessor{config: stripped, groups: fixture.request.Groups.Config},
	)
	if err != nil {
		t.Fatal(err)
	}
	if transition.config != stripped || transition.groups != fixture.request.Groups.Config {
		t.Fatal("verification did not reuse the predecessor's prepared configuration")
	}
}
