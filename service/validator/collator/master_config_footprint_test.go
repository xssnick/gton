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
	if _, err := parseMasterConfigEpoch(usage.Root(), testConfigAddress(t, root)); err != nil {
		t.Fatal(err)
	}
	want := sortedHashes(usage.Hashes())

	resident, footprint := captureConfigFootprint(root, testConfigAddress(t, root))
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

	residentRoot, resident := captureConfigFootprint(root, testConfigAddress(t, root))
	if resident == nil || residentRoot == nil {
		t.Fatal("resident configuration produced no footprint")
	}

	pages := newAdvLazifier()
	lazyRoot := pages.root(t, root)
	if !lazyRoot.IsLazy() && countLazyReferences(t, []*cell.Cell{lazyRoot}) == 0 {
		t.Fatal("the fixture root is not paged in, so this proves nothing")
	}
	materialized, lazy := captureConfigFootprint(lazyRoot, testConfigAddress(t, root))
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

	cache := localConfigCache{entries: make(map[localConfigKey]localPreparedConfig)}
	prepared, err := cache.prepare(lazyRoot, testConfigAddress(t, root))
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
	if _, err := parseMasterConfigEpoch(usage.Root(), testConfigAddress(t, root)); err != nil {
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

	cache := localConfigCache{entries: make(map[localConfigKey]localPreparedConfig)}
	proofPrepared, err := cache.prepare(narrow, testConfigAddress(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if proofPrepared.execution.Root() != narrow {
		t.Fatal("proof-backed preparation did not stay bound to its validation root")
	}
	if len(cache.entries) != 0 {
		t.Fatal("proof-backed configuration poisoned the epoch cache")
	}

	resident, err := cache.prepare(root, testConfigAddress(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if resident.execution.Root().IsVirtualized() || len(cache.entries) != 1 {
		t.Fatal("resident configuration was not published into the epoch cache")
	}
	reused, err := cache.prepare(narrow, testConfigAddress(t, root))
	if err != nil {
		t.Fatal(err)
	}
	if reused.execution != resident.execution {
		t.Fatal("authenticated proof root did not reuse the resident epoch context")
	}
}

// Compare the replay in isolation: strict config validation can already read
// the same cells, which would mask an incomplete replay in a whole transition.
func TestMasterConfigFootprintMutationsAreDetected(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	captured := fixture.request.Config.footprint.cells
	if len(captured) < 1001 {
		t.Fatalf("the fixture footprint holds %d cells, too few to truncate", len(captured))
	}

	root := fixture.request.Config.execution.Root()
	usage := cell.NewReadSet(root)
	if _, err := parseMasterConfigEpoch(usage.Root(), testConfigAddress(t, root)); err != nil {
		t.Fatal(err)
	}
	fresh := sortedHashes(usage.Hashes())

	for _, mutation := range []struct {
		name    string
		cells   []*cell.Cell
		matches bool
	}{
		{"one cell short", captured[:len(captured)-1], false},
		{"ten cells short", captured[:len(captured)-10], false},
		{"a hundred cells short", captured[:len(captured)-100], false},
		{"a thousand cells short", captured[:len(captured)-1000], false},
		{"empty", captured[:0], false},
		{"complete plus eight foreign cells", append(slices.Clone(captured), foreignCells(8)...), false},
		{"complete", captured, true},
	} {
		t.Run(mutation.name, func(t *testing.T) {
			config := withFootprint(fixture.request.Config, mutation.cells)
			replayed := cell.NewReadSet(root)
			config.footprint.replay(replayed)
			if equal := slices.Equal(fresh, sortedHashes(replayed.Hashes())); equal != mutation.matches {
				t.Fatalf("replayed footprint matches fresh parse = %t, want %t", equal, mutation.matches)
			}
		})
	}
}

func TestMasterConfigValidationCoversParseReads(t *testing.T) {
	fixture := newMasterBuildFixture(t, false)
	_, fresh := masterConfigTransitionReads(t, fixture, nil, nil)
	withoutReplay := withFootprint(fixture.request.Config, fixture.request.Config.footprint.cells[:0])
	transition, validated := masterConfigTransitionReads(t, fixture, withoutReplay, fixture.request.Groups.Config)
	if transition.config != withoutReplay {
		t.Fatal("configuration was parsed instead of reused")
	}
	// Full TL-B validation now reaches every cell the epoch parsers read in
	// this fixture, even when no recorded reads are replayed.
	if !slices.Equal(fresh, validated) {
		t.Fatalf("validation recorded %d cells, fresh transition recorded %d", len(validated), len(fresh))
	}
	if complete, empty := buildMasterReadSetSize(t, fixture.request),
		buildMasterReadSetSize(t, withRequestConfig(fixture.request, withoutReplay)); complete != empty {
		t.Fatalf("whole collation recorded %d cells with replay and %d without it", complete, empty)
	}
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

// Reusing an epoch must preserve the block, collated data and applied state.
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
	if err = verifyMasterCandidateForTest(context.Background(), MasterVerificationRequest{
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
